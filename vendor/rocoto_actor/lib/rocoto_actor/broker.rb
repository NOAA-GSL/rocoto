# frozen_string_literal: true

require "securerandom"

module RocotoActor
  # The only way to create an actor, and the owner of all cross-actor policy:
  # the node graph (paths, parents, children, states, generations), brokered
  # calls and tells, route limits and deadlines, the restart policy, watches
  # and events, actor-owned timers, and the process-limit preflight.
  #
  # Threads. The application's own thread runs the public API. Three broker
  # threads, all created by #initialize and alive until #stop, run the rest:
  # the deadline scheduler (route and boot expirations, timer firing, restart
  # backoff), the event dispatcher (on_event and watcher delivery), and a
  # lifecycle pool (spawn and stop work, which blocks on processes). Each actor
  # has a reader, a writer, and a reaper thread of its own in Reference, so a
  # request from an actor is served on that actor's reader thread and must not
  # block there.
  #
  # Locks. One mutex guards every instance variable here. Two rules keep it
  # deadlock-free, and every method below obeys them:
  #
  #   1. Never hold @mutex while calling a Reference method that takes the
  #      reference mutex (ask, tell, stop, kill, alive?). Collect what is
  #      needed under the lock; act after releasing it.
  #   2. Never run an application or actor callback under @mutex.
  #
  # "Caller holds @mutex" on a method means it must be called with the lock
  # already held.
  #
  # Reading order. The sections below are marked with "# ===" banners:
  #
  #   The application API           what an application calls; every entry is short
  #   Requests from actors          dispatch: the index of what an actor can ask for
  #   Construction                  the broker's own threads, started by initialize
  #   Serving one actor request     one handler per operation, none of which waits
  #   Request capacity and replies  the bounds on requests in flight, and writing answers back
  #   Actor-owned timers            schedule, cancel, fire
  #   Watches and lifecycle events  who is told when an actor's state changes
  #   Launching an actor            launch_node through settle: the boot path, first and relaunch
  #   Exit, restart, and stopping   what happens when a process ends, by policy or by request
  #   The node graph                lookups and derived state, under @mutex
  #   Small helpers                 validation and thin wrappers over the executors
  #
  # An actor's whole life is in two of those: "Launching an actor" starts it,
  # "Exit, restart, and stopping" ends it. docs/architecture.md has the process
  # and component picture around them, and REVIEW.md the invariants to preserve.
  class ActorBroker
    DEFAULT_MAX_ROUTES = 1_000
    DEFAULT_MAX_ROUTES_PER_ACTOR = 100
    DEFAULT_ROUTE_TIMEOUT = 30
    DEFAULT_MAX_LIFECYCLE_WORKERS = 2
    DEFAULT_MAX_PENDING_LIFECYCLE_REQUESTS = 100
    MAX_TIMERS_PER_ACTOR = 100
    MIN_TIMER_INTERVAL = 0.01
    DEFAULT_PROCESS_MARGIN = 32
    DEFAULT_ERROR_HANDLER = lambda do |error, context|
      warn "rocoto_actor: #{context}: #{error.class}: #{error.message}"
    end

    TimerRecord = Struct.new(:id, :message, :every)

    # max_routes bounds brokered requests awaiting a target actor across the broker.
    # max_routes_per_actor bounds the broker responses owed to one source actor that
    # have not yet been written to its socket; a source at this limit is not read
    # until a response drains. route_timeout applies when a request has no timeout.
    # Spawn and stop requests from actors run on up to max_lifecycle_workers threads
    # with at most max_pending_lifecycle_requests waiting for one. error_handler
    # receives (error, context) for failures on the broker's own threads, which
    # are reported rather than allowed to kill the thread; it must not raise.
    # Raises ResourceLimitError when the broker's own threads cannot be created.
    def initialize(max_routes: DEFAULT_MAX_ROUTES, max_routes_per_actor: DEFAULT_MAX_ROUTES_PER_ACTOR,
                   route_timeout: DEFAULT_ROUTE_TIMEOUT, max_lifecycle_workers: DEFAULT_MAX_LIFECYCLE_WORKERS,
                   max_pending_lifecycle_requests: DEFAULT_MAX_PENDING_LIFECYCLE_REQUESTS,
                   error_handler: DEFAULT_ERROR_HANDLER, on_event: nil, process_margin: DEFAULT_PROCESS_MARGIN)
      unless process_margin.nil? || (process_margin.is_a?(Integer) && process_margin >= 0)
        raise ArgumentError, "process_margin must be a non-negative integer or nil"
      end

      raise ArgumentError, "on_event must respond to call" unless on_event.nil? || on_event.respond_to?(:call)

      raise ArgumentError, "error_handler must respond to call" unless error_handler.respond_to?(:call)

      raise ArgumentError, "max_routes must be positive" unless max_routes.positive?
      raise ArgumentError, "max_routes_per_actor must be positive" unless max_routes_per_actor.positive?
      raise ArgumentError, "route_timeout must be positive" unless valid_timeout?(route_timeout)
      raise ArgumentError, "max_lifecycle_workers must be positive" unless max_lifecycle_workers.positive?
      unless max_pending_lifecycle_requests.positive?
        raise ArgumentError, "max_pending_lifecycle_requests must be positive"
      end

      @max_routes = max_routes
      @max_routes_per_actor = max_routes_per_actor
      @route_timeout = route_timeout
      @max_lifecycle_workers = max_lifecycle_workers
      @max_pending_lifecycle_requests = max_pending_lifecycle_requests
      @error_handler = error_handler
      @on_event = on_event
      @process_margin = process_margin
      @waiting = {} # node id => id of the node it is blocked on (a call or a child's boot)
      @mutex = Mutex.new
      @capacity_condition = ConditionVariable.new
      @nodes = {}
      @routes = 0
      @responses_by_source = Hash.new(0).compare_by_identity
      @stopped = false
      # References a failed boot killed without waiting: pruned as their exits
      # are observed; stop confirms any still alive.
      @killed_references = {}.compare_by_identity
      @scheduler = DeadlineScheduler.new(error_handler: @error_handler)
      @lifecycle_executor = LifecycleExecutor.new(
        max_workers: @max_lifecycle_workers,
        max_pending_requests: @max_pending_lifecycle_requests,
        error_handler: @error_handler,
        request_error: lambda { |source, request, error, release|
          respond_error(source, request[:request_id], Error.new("#{error.class}: #{error.message}"), release)
        }
      ) { |source, request, release| perform_lifecycle(source, request, release) }
      @event_dispatcher = EventDispatcher.new(error_handler: @error_handler) do |*event|
        deliver_event(*event)
      end
      start_executors
    end

    # === The application API =================================================

    # parent: is a handle owned by this broker; the new actor becomes its logical
    # child and is stopped whenever the parent stops or fails. name: must be
    # unique among the parent's live children and forms the actor's path. The
    # remaining options are those of SpawnOptions: start_timeout, source,
    # mailbox_size, mailbox_bytes, and the restart policy (restart: :never by
    # default, or :on_failure with max_restarts, restart_window, restart_backoff).
    def spawn(actor_class, *arguments, name: nil, parent: nil, **options)
      spawn_options = SpawnOptions.parse(options)
      node, boot = launch_node(actor_class, arguments, parent_id: parent&.id, name: name, options: spawn_options)
      settled = Queue.new
      await_boot(node, boot, spawn_options.start_timeout) { |error| settled << error }
      error = settled.pop
      raise error if error

      ActorHandle.new(node.id, broker: self)
    end

    def stop(timeout: Reference::DEFAULT_STOP_TIMEOUT, force: false)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      nodes, threads = @mutex.synchronize do
        @stopped = true
        @capacity_condition.broadcast
        event_thread = @event_dispatcher.stop
        scheduler_thread = @scheduler.stop
        [@nodes.values, [scheduler_thread, event_thread].compact]
      end
      abandoned = @lifecycle_executor.stop
      abandoned.each { |job| respond_error(job.source, job.request[:request_id], stopped_error, job.release_response) }
      stopped = stop_subtrees(nodes, timeout: timeout, force: force)
      # Processes failed boots killed without waiting, listed by now since the
      # sweep is done: confirm them gone, with the grace a forced stop allows
      # a KILL, once for the batch.
      killed = @mutex.synchronize { @killed_references.keys.tap { @killed_references.clear } }
      confirm_by = Reference.kill_deadline(deadline)
      killed.each(&:kill) # idempotent; a settle may have listed one before killing it
      confirmed = killed.select(&:alive?).map { |reference| reference.wait_for_exit(confirm_by) }.all?
      threads.each { |thread| thread.join unless thread == Thread.current } # stop may be called from error_handler
      stopped && confirmed
    end

    def ask(id, message)
      @mutex.synchronize { checked_node(id).reference }.ask(message)
    end

    def tell(id, message)
      @mutex.synchronize { checked_node(id).reference }.tell(message)
    end

    # Stops the actor's live descendants first, deepest first, then the actor.
    # The timeout is one deadline shared by the whole subtree.
    def stop_actor(id, timeout: Reference::DEFAULT_STOP_TIMEOUT, force: false)
      node = @mutex.synchronize { fetch_node(id) }
      stop_subtrees([node], timeout: timeout, force: force)
    end

    # A plain-data snapshot of every actor the broker knows, for operators.
    def describe
      process_limit = @process_margin && ProcessBudget.snapshot(@process_margin) # scans /proc; keep it off the mutex
      @mutex.synchronize do
        {
          stopped: @stopped,
          routes_in_flight: @routes,
          timers: @nodes.values.sum { |node| node.timers.size },
          lifecycle_queue: @lifecycle_executor.pending_requests,
          process_limit: process_limit,
          actors: @nodes.values.map { |node| describe_node(node) }
        }
      end
    end

    # Handles of top-level actors that have not stopped or failed.
    def roots
      @mutex.synchronize do
        @nodes.values.select { |node| node.parent_id.nil? && !node.terminal? }
              .map { |node| ActorHandle.new(node.id, broker: self) }
      end
    end

    def alive?(id)
      node, reference = @mutex.synchronize { fetch_node(id).then { |found| [found, found.reference] } }
      !node.terminal? && !reference.nil? && reference.alive?
    end

    def state(id)
      @mutex.synchronize { fetch_node(id).state }
    end

    def path(id)
      @mutex.synchronize { fetch_node(id).path }
    end

    def generation(id)
      @mutex.synchronize { fetch_node(id).generation }
    end

    def parent(id)
      parent_id = @mutex.synchronize do
        node = fetch_node(id)
        node.parent_id if node.parent_id && @nodes.key?(node.parent_id)
      end
      parent_id && ActorHandle.new(parent_id, broker: self)
    end

    # Handles of the actor's children that have not stopped or failed.
    def children(id)
      @mutex.synchronize do
        live_children(fetch_node(id)).map { |child| ActorHandle.new(child.id, broker: self) }
      end
    end

    # RemoteError for the most recent unhandled exception in a told message
    # that ended one of this actor's incarnations, or nil.
    def last_failure(id)
      @mutex.synchronize do
        node = fetch_node(id)
        node.reference&.exit_error || node.failure
      end
    end

    # ExitStatus of the most recent incarnation whose process ended on its own
    # (crash, signal, or unhandled exception), or nil.
    def last_exit(id)
      @mutex.synchronize do
        node = fetch_node(id)
        node.reference&.exit_status || node.exit
      end
    end

    # === Requests from actors ================================================
    # The worker side of the socket sends these; dispatch is the index.

    # Entry point for every broker request read from an actor's socket. Runs on
    # that actor's reader thread and must not block on other actors.
    def dispatch(source, request)
      request_id = request[:request_id]
      return unless request_id.is_a?(Integer)

      return unless acquire_response_slot(source)

      release_response = release_once { release_response_slot(source) }
      case request[:op]
      when :broker_request then route(source, request, release_response)
      when :broker_tell then relay_tell(source, request, release_response)
      when :broker_schedule then schedule_timer(source, request, release_response)
      when :broker_watch then watch(source, request, release_response)
      when :broker_unwatch then unwatch(source, request, release_response)
      when :broker_cancel then cancel_timer(source, request, release_response)
      when :broker_spawn, :broker_stop then enqueue_lifecycle(source, request, release_response)
      else respond_error(source, request_id, Error.new("unknown broker operation"), release_response)
      end
    end

    private

    # === Construction ========================================================

    # Starts the scheduler and event threads and the first lifecycle worker.
    # Without them the broker cannot run, and a lifecycle worker that exists
    # from the start means internal work (stopping a failed actor's children,
    # a relaunch) can always be queued; so a thread that cannot be created
    # fails construction, and nothing later needs a thread the broker may not
    # get.
    def start_executors
      @scheduler.start
      @event_dispatcher.start
      @lifecycle_executor.start
    rescue ResourceLimitError
      @scheduler.stop
      @event_dispatcher.stop
      @lifecycle_executor.stop
      raise
    end

    # === Serving one actor request ===========================================
    # Each handler answers its requester exactly once, and none of them waits
    # on another actor: blocking work goes to the lifecycle pool.

    # Never blocks on the target actor; the response is sent when the target
    # future resolves or its route reaches its expiration.
    def route(source, request, release_response)
      request_id = request[:request_id]
      timeout = request.fetch(:timeout, nil) || @route_timeout
      unless valid_timeout?(timeout)
        return respond_error(source, request_id, ArgumentError.new("invalid broker timeout"), release_response)
      end

      reference, sender, rejection = acquire_route(source, request[:handle_id])
      return respond_error(source, request_id, rejection, release_response) if rejection

      source_id = @mutex.synchronize { node_for(source)&.id }
      release_route = release_once { release_route_slot(source_id) }
      begin
        future = reference.ask(request[:message], sender)
      rescue StandardError => error
        release_route.call
        return respond_error(source, request_id, error, release_response)
      end

      if (error = failure_for(schedule_expiration(future, timeout)))
        future.reject(error)
      end
      future.on_resolve do |result, error|
        cancel_expiration(future)
        release_route.call
        if error
          respond_error(source, request_id, error, release_response)
        else
          respond(source, request_id, result, release_response)
        end
      end
    rescue StandardError => error
      release_route&.call
      respond_error(source, request_id, error, release_response)
    end

    # Enqueues the message in the target's mailbox and acknowledges that, or
    # reports why it was not enqueued. Never waits for the target.
    def relay_tell(source, request, release_response)
      reference, sender, rejection = @mutex.synchronize do
        node = @nodes[request[:handle_id]]
        error = node_error(node)
        next [nil, nil, error] if error

        [node.reference, sender_handle(source), nil]
      end
      return respond_error(source, request[:request_id], rejection, release_response) if rejection

      reference.tell(request[:message], sender)
      respond(source, request[:request_id], nil, release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    # Spawn and stop requests block for up to their timeouts, so they run on
    # the bounded pool rather than on the requesting actor's reader thread.
    def enqueue_lifecycle(source, request, release_response)
      result = @mutex.synchronize do
        @stopped ? :stopped : @lifecycle_executor.enqueue_request(source, request, release_response)
      end
      return if result == true

      respond_error(source, request[:request_id], failure_for(result) || result, release_response)
    end

    def perform_lifecycle(source, request, release_response)
      case request[:op]
      when :broker_spawn then spawn_child(source, request, release_response)
      when :broker_stop then respond(source, request[:request_id], stop_child(source, request), release_response)
      end
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    # Occupies the lifecycle thread only for process creation; the response is
    # sent when the child's boot future resolves, so a child that itself spawns
    # from initialize does not hold a thread per nesting level.
    def spawn_child(source, request, release_response)
      parent = @mutex.synchronize { source_node(source) }
      actor_class = request[:actor_class]
      path = request[:source]
      arguments = request[:arguments]
      options = request[:options]
      raise ArgumentError, "actor_class must be a string" unless actor_class.is_a?(String)
      raise ArgumentError, "source must be an absolute path" unless path.is_a?(String) && path.start_with?("/")
      raise ArgumentError, "arguments must be an array" unless arguments.is_a?(Array)
      raise ArgumentError, "options must be a hash" unless options.is_a?(Hash)

      # The worker parsed these too; the broker is the trust boundary and parses again.
      spawn_options = SpawnOptions.parse(options.merge(source: path))

      node, boot = launch_node(actor_class, arguments, parent_id: parent.id, name: request[:name],
                                                       options: spawn_options)
      @mutex.synchronize { @waiting[parent.id] = node.id } # the parent blocks in context.spawn until the boot settles
      await_boot(node, boot, spawn_options.start_timeout) do |error|
        @mutex.synchronize { @waiting.delete(parent.id) if @waiting[parent.id] == node.id }
        if error
          respond_error(source, request[:request_id], error, release_response)
        else
          respond(source, request[:request_id], ActorHandle.new(node.id, broker: self), release_response)
        end
      end
    end

    def stop_child(source, request)
      timeout = request.fetch(:timeout, nil) || Reference::DEFAULT_STOP_TIMEOUT
      raise ArgumentError, "invalid stop timeout" unless valid_timeout?(timeout)

      node = @mutex.synchronize do
        requester = source_node(source)
        target = @nodes[request[:handle_id]] or raise Error, "unknown actor handle"
        raise Error, "actor #{target.path} is not a descendant of #{requester.path}" unless descendant?(target,
                                                                                                        requester.id)

        target
      end
      stop_subtrees([node], timeout: timeout, force: request[:force] ? true : false)
    end

    # Queues internal blocking work (stops, relaunches) on the lifecycle pool;
    # such jobs are bounded by the number of nodes, not by the request queue.
    # Nothing is queued once the broker is stopping: its sweep owns every node
    # from then on. Runs on reaper and scheduler threads, so it never raises.
    def enqueue_lifecycle_job(&block)
      @mutex.synchronize { @lifecycle_executor.enqueue_job(&block) unless @stopped }
      nil
    end

    # === Request capacity and replies ========================================
    # What bounds the requests in flight and what writes the answers back.

    def acquire_route(source, handle_id)
      @mutex.synchronize do
        next [nil, nil, stopped_error] if @stopped

        node = @nodes[handle_id]
        error = node_error(node)
        next [nil, nil, error] if error
        if @routes >= @max_routes
          next [nil, nil, BrokerBusyError.new("actor broker has #{@max_routes} routes in flight")]
        end

        source_id = node_for(source)&.id
        cycle = source_id && wait_cycle(source_id, node.id)
        next [nil, nil, DeadlockError.new("call would deadlock: #{cycle.join(' -> ')}")] if cycle

        @routes += 1
        @waiting[source_id] = node.id if source_id
        [node.reference, sender_handle(source), nil]
      end
    end

    # Caller holds @mutex. Returns the path of actors that would wait on each
    # other if source blocked on target, or nil. Assumes one outstanding call per
    # actor; a multithreaded actor may evade detection and still times out.
    def wait_cycle(source_id, target_id)
      path = [source_id, target_id]
      current = target_id
      @nodes.size.times do
        return path.map { |id| @nodes[id]&.path || id } if current == source_id

        current = @waiting[current] or return nil
        path << current
      end
      nil
    end

    def release_route_slot(source_id)
      @mutex.synchronize do
        @routes -= 1
        @waiting.delete(source_id) if source_id
      end
    end

    def acquire_response_slot(source)
      @mutex.synchronize do
        while @responses_by_source[source] >= @max_routes_per_actor
          return false if @stopped

          @capacity_condition.wait(@mutex)
        end
        return false if @stopped

        @responses_by_source[source] += 1
        true
      end
    end

    def release_response_slot(source)
      @mutex.synchronize do
        remaining = @responses_by_source[source] - 1
        if remaining.positive?
          @responses_by_source[source] = remaining
        else
          @responses_by_source.delete(source)
        end
        @capacity_condition.broadcast
      end
    end

    def release_once(&block)
      released = false
      mutex = Mutex.new
      lambda do
        run = mutex.synchronize do
          next false if released

          released = true
        end
        block.call if run
      end
    end

    def respond(source, request_id, result, on_done)
      source.send_broker_response(request_id, result: result, on_done: on_done)
    rescue SerializationError => error
      respond_error(source, request_id, error, on_done)
    end

    def respond_error(source, request_id, error, on_done)
      source.send_broker_response(request_id, error: error, on_done: on_done)
    rescue StandardError
      on_done.call
    end

    # === Actor-owned timers ==================================================

    # Registers a self-addressed timer for the requesting actor and answers
    # with its Timer. Runs inline on the reader thread; never waits.
    def schedule_timer(source, request, release_response)
      after = request[:after]
      every = request[:every]
      raise ArgumentError, "schedule needs after: or every:" if after.nil? && every.nil?
      raise ArgumentError, "after must be a non-negative number" unless after.nil? || non_negative_number?(after)
      unless every.nil? || (valid_timeout?(every) && every >= MIN_TIMER_INTERVAL)
        raise ArgumentError, "every must be at least #{MIN_TIMER_INTERVAL} seconds"
      end

      node, timer = @mutex.synchronize do
        node = source_node(source)
        if node.timers.size >= MAX_TIMERS_PER_ACTOR
          raise Error,
                "actor #{node.path} already has #{MAX_TIMERS_PER_ACTOR} timers"
        end

        record = TimerRecord.new(SecureRandom.hex(16), request[:message], every)
        node.timers[record.id] = record
        [node, record]
      end
      if (error = failure_for(enqueue_task(delay: after || every) { fire_timer(node, timer.id) }))
        @mutex.synchronize { node.timers.delete(timer.id) }
        raise error
      end
      respond(source, request[:request_id], Timer.new(timer.id), release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    def cancel_timer(source, request, release_response)
      cancelled = @mutex.synchronize do
        node = node_for(source)
        node ? !node.timers.delete(request[:timer_id]).nil? : false
      end
      respond(source, request[:request_id], cancelled, release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    # Runs on the service thread. Delivers the timer's message as a tell from
    # the actor to itself, then re-arms a recurring timer. A timer whose
    # incarnation has ended is no longer in node.timers and does nothing. A
    # tell that fails is reported, except when the actor has just stopped,
    # which its lifecycle event already reports; a recurring timer tries again
    # next interval either way, unless the broker has stopped meanwhile.
    def fire_timer(node, id)
      record, reference = @mutex.synchronize do
        record = node.timers[id]
        next [nil, nil] unless record && node.active? && node.reference # none while a failed relaunch boot settles

        node.timers.delete(id) unless record.every
        [record, node.reference]
      end
      return unless record

      begin
        reference.tell(record.message, ActorHandle.new(node.id, broker: self))
      rescue ActorStoppedError
        nil
      rescue StandardError => error
        ErrorReporting.report(@error_handler, error, "scheduled tell to #{node.id}")
      end
      return unless record.every

      return if enqueue_task(delay: record.every) { fire_timer(node, id) } == true

      @mutex.synchronize { node.timers.delete(id) } # the broker is stopping
    end

    # === Watches and lifecycle events ========================================

    # Subscribes the requesting actor to the watched actor's lifecycle events.
    # Watching an actor that is already terminal delivers that event at once.
    def watch(source, request, release_response)
      @mutex.synchronize do
        watcher = source_node(source)
        watched = @nodes[request[:handle_id]] or raise Error, "unknown actor handle"
        if watched.terminal?
          detail = { reason: watched.failure&.message || watched.exit&.to_s, generation: watched.generation }
          @event_dispatcher.emit(watched.id, watched.state, detail, [watcher.id], notify_application: false)
        else
          watched.watchers[watcher.id] = true
        end
      end
      respond(source, request[:request_id], true, release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    def unwatch(source, request, release_response)
      removed = @mutex.synchronize do
        watcher = node_for(source)
        watched = @nodes[request[:handle_id]]
        watcher && watched ? !watched.watchers.delete(watcher.id).nil? : false
      end
      respond(source, request[:request_id], removed, release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    # Caller holds @mutex. Queues delivery of a lifecycle event to the
    # application's on_event and to every watcher of the node. Delivery runs on
    # the event thread, outside every lock.
    def emit(node, event, reason, watcher_ids = node.watchers.keys)
      @event_dispatcher.emit(node.id, event, { reason: reason, generation: node.generation }, watcher_ids)
    end

    def deliver_event(node_id, event, detail, watcher_ids, notify_application)
      handle = ActorHandle.new(node_id, broker: self)
      ErrorReporting.guard(@error_handler, "on_event") { @on_event&.call(event, handle, detail) } if notify_application
      watcher_ids.each do |watcher_id|
        reference = @mutex.synchronize do
          watcher = @nodes[watcher_id]
          watcher&.active? ? watcher.reference : nil
        end
        next unless reference

        begin
          reference.tell(Protocol.request(:actor_event, event: event, actor: handle, reason: detail[:reason],
                                                        generation: detail[:generation]), nil)
        rescue StandardError => error
          ErrorReporting.report(@error_handler, error, "event to #{watcher_id}")
        end
      end
    end

    # Caller holds @mutex. Why the incarnation behind this reference ended, or
    # nil if it has not reported anything.
    def exit_reason(reference)
      reference&.exit_error&.message || reference&.exit_status&.to_s
    end

    # === Launching an actor and settling its boot ============================

    # Starts the process and registers it as :starting. Returns [node, boot];
    # the caller must settle the boot future exactly once.
    def launch_node(actor_class, arguments, parent_id:, name:, options:)
      name = validate_name(name)
      @mutex.synchronize { check_placement(parent_id, name) } # cheap rejection before a /proc scan and a process
      id = SecureRandom.hex(16)
      reference, boot = launch_process(actor_class, arguments, id, options.launch)
      spec = { actor_class: actor_class, arguments: arguments, options: options.launch,
               start_timeout: options.start_timeout }
      node = register(id, reference, parent_id, name, spec, options.policy)
      reference.attach_broker(self)
      reference.on_exit { actor_exited(node, reference) }
      [node, boot]
    rescue Exception # rubocop:disable Lint/RescueException
      reference&.stop(force: true, timeout: 0)
      unregister(node) if node
      raise
    end

    # Every actor process starts here. Refuses, before paying for a process,
    # when the user's process limit leaves no room for one more actor.
    # A connection whose threads cannot be created (the preflight can only
    # estimate) fails this launch the same way, with ResourceLimitError; the
    # actors already running keep their threads.
    def launch_process(actor_class, arguments, actor_id, launch_options)
      ProcessBudget.check!(@process_margin) if @process_margin
      Launcher.launch(actor_class, *arguments, context: { actor_id: actor_id }, **launch_options)
    end

    def register(id, reference, parent_id, name, spec, policy)
      @mutex.synchronize do
        parent = check_placement(parent_id, name)
        name ||= id
        path = parent ? "#{parent.path}/#{name}" : name
        node = ActorNode.new(
          id: id, name: name, path: path, parent_id: parent_id, reference: reference,
          spec: spec, policy: policy
        )
        @nodes[id] = node
        reference.actor_id = id
        parent&.children&.push(id)
        node
      end
    end

    # Caller holds @mutex. Returns the parent node, or nil for a root.
    def check_placement(parent_id, name)
      raise stopped_error if @stopped

      parent = nil
      if parent_id
        parent = @nodes[parent_id] or raise Error, "unknown parent actor handle"
        raise ActorStoppedError, "parent actor #{parent.path} is #{parent.state}" unless parent.active?
      end
      siblings = parent ? parent.children : @nodes.values.select { |node| node.parent_id.nil? }.map(&:id)
      taken = name && siblings.any? { |id| (sibling = @nodes[id]) && !sibling.terminal? && sibling.name == name }
      raise ArgumentError, "actor name #{name.inspect} is already in use" if taken

      parent
    end

    def validate_name(name)
      return nil if name.nil?

      name = name.to_s
      raise ArgumentError, "actor name must not be empty" if name.empty?
      raise ArgumentError, "actor name must not contain '/'" if name.include?("/")

      name
    end

    # Arms the boot deadline and settles the node exactly once when its boot
    # future resolves, on whichever thread resolves it. The block receives the
    # error to raise or send, or nil on success.
    def await_boot(node, boot, start_timeout)
      if (error = failure_for(schedule_expiration(boot, start_timeout)))
        boot.reject(error)
      end
      boot.on_resolve do |_result, error|
        cancel_expiration(boot)
        yield settle(node, error)
      end
    end

    # Ends a boot, first or relaunch. Success moves the node to :running (and
    # announces a relaunch). Failure kills the process; a first boot is then
    # unregistered with any children it spawned while booting, while a failed
    # relaunch counts as another failure under the restart policy. Returns the
    # error a spawner should see, or nil.
    def settle(node, error)
      first_boot, children, exited, reference, overtaken = @mutex.synchronize do
        next [nil, [], false, nil, false] unless node.booting

        first_boot = node.first_boot?
        # A stop that reached a boot that failed is the outcome the spawner
        # learns. A boot that succeeds while a stop is in flight is left to
        # that stop: boot_succeeded declines it and the spawner gets its
        # handle, as when the stop lands a moment later.
        overtaken = error && (node.state == :stopping || node.terminal?)
        if error
          # Detaching the reference first means the reaper, seeing an exit from
          # a reference the node no longer holds, leaves this failure to us. A
          # first boot is unregistered in the same step, so no sweep can find a
          # node without a reference.
          exit_observed = node.boot_exit.equal?(node.reference)
          dead = node.boot_failed
          @killed_references[dead] = true if dead && !exit_observed # killed below without waiting
          children = live_children(node)
          if first_boot
            unregister_locked(node)
          elsif error.is_a?(RemoteError)
            node.record_failure(error)
          end
          [first_boot, children, false, dead, overtaken]
        else
          if node.boot_succeeded(Process.clock_gettime(Process::CLOCK_MONOTONIC)) && !first_boot
            emit(node, :restarted,
                 nil)
          end
          [first_boot, [], node.boot_exit&.equal?(node.reference), nil, false]
        end
      end
      # The process died after replying ready but before we settled: the actor
      # is registered and running from the caller's view, so treat it as a crash.
      actor_failed(node) if exited
      return nil unless error

      reference&.kill # without waiting: this may be the scheduler thread
      if first_boot
        stop_later(children)
        overtaken ? node_error_for(node) : Launcher.startup_error(reference, error)
      else
        actor_failed(node)
        nil
      end
    end

    def unregister(node)
      @mutex.synchronize { unregister_locked(node) }
    end

    # Caller holds @mutex. Removes a node that never finished booting; its id
    # is known only to the dead process, so no handle can refer to it. It ends
    # :stopped when a stop had reached it (that stop would have retired it),
    # :failed otherwise.
    def unregister_locked(node)
      retire(node, node.state == :stopping ? :stopped : :failed) unless node.terminal?
      @nodes.delete(node.id)
      @nodes[node.parent_id]&.children&.delete(node.id)
    end

    # === Exit, restart, and stopping =========================================

    # Called from the actor's reaper thread once its process has exited. Exits
    # during a boot are settled by the boot future's owner instead.
    def actor_exited(node, reference)
      @mutex.synchronize do
        @killed_references.delete(reference) # its exit is observed
        return if node.terminal? || !node.reference.equal?(reference)

        if node.booting
          node.record_boot_exit(reference)
          return
        end
        if node.state == :stopping
          retire(node, :stopped)
          return
        end
      end
      actor_failed(node)
    end

    # Applies the restart policy to a running or restarting actor whose process
    # is gone: schedules a relaunch, or marks it :failed. Either way its live
    # descendants are stopped; a restarted actor recreates them in initialize.
    def actor_failed(node)
      delay, children = @mutex.synchronize do
        next [nil, []] if node.terminal? || node.state == :stopping

        # Mark the whole live subtree now so it rejects messages before the
        # service thread gets to it; stop_subtrees re-derives the order itself.
        live_postorder(node).each { |descendant| descendant.begin_stopping unless descendant.equal?(node) }
        live = live_children(node)
        delay = restart_delay(node)
        release_incarnation(node)
        if delay
          node.begin_restarting
          emit(node, :restarting, exit_reason(node.reference))
        else
          retire(node, :failed)
        end
        [delay, live]
      end
      stop_later(children)
      return unless delay

      # :stopped only when the broker is stopping, and then its sweep has retired the node.
      enqueue_task(delay: delay) { enqueue_lifecycle_job { relaunch(node) } }
      nil
    end

    # Caller holds @mutex. Records a restart attempt and returns its backoff
    # delay, or nil when the policy forbids restarting now. The count is of
    # consecutive failures and resets only after the actor has run for
    # restart_window seconds, so backoff delays (time not running) can never
    # prune failures out of the window and defeat max_restarts.
    def restart_delay(node)
      return nil if @stopped || node.policy[:restart] == :never

      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      attempt = node.record_restart_attempt(now, node.policy[:restart_window])
      return nil unless attempt

      node.policy[:restart_backoff] * (2**(attempt - 1))
    end

    # Runs on a lifecycle thread. Replaces the node's dead reference with a new
    # process under the same id, path, and name, and bumps the generation.
    def relaunch(node)
      spec = @mutex.synchronize { node.state == :restarting ? node.spec : nil }
      return unless spec

      reference, boot = launch_process(spec[:actor_class], spec[:arguments], node.id, spec[:options])
      installed = @mutex.synchronize do
        next false unless node.state == :restarting

        node.install_restarted_reference(reference)
        reference.actor_id = node.id
        true
      end
      unless installed
        reference.stop(force: true, timeout: 0)
        return
      end

      reference.attach_broker(self)
      reference.on_exit { actor_exited(node, reference) }
      await_boot(node, boot, spec[:start_timeout]) { |_error| nil }
    rescue Exception => error # rubocop:disable Lint/RescueException -- the node must not be left half-relaunched
      ErrorReporting.report(@error_handler, error, "relaunch of #{node.path}")
      actor_failed(node)
    end

    # Force-stops nodes on the lifecycle pool: stopping waits on processes, and
    # the scheduler thread must stay free to run route expirations on time.
    def stop_later(nodes)
      return if nodes.empty?

      enqueue_lifecycle_job { stop_subtrees(nodes, timeout: Reference::DEFAULT_STOP_TIMEOUT, force: true) }
    end

    def stop_subtrees(roots, timeout:, force:)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      nodes = @mutex.synchronize do
        roots.flat_map { |root| live_postorder(root) }.uniq.map do |node|
          node.begin_stopping
          [node, node.reference]
        end
      end
      nodes.map do |node, reference|
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        # A relaunch whose boot just failed has no reference until actor_failed
        # applies the policy; being stopped first is a valid outcome for it.
        stopped = reference.nil? || reference.stop(timeout: [remaining, 0].max, force: force || remaining <= 0)
        @mutex.synchronize { retire(node, :stopped) if node.state == :stopping }
        stopped
      end.all?
    end

    # Caller holds @mutex. Moves a node to a terminal state and releases what
    # only a live actor needs: the reference (threads, socket) and the relaunch
    # spec. The node itself stays so its handle keeps answering with its state.
    def retire(node, state)
      release_incarnation(node)
      reason = exit_reason(node.reference)
      watcher_ids = node.watchers.keys
      node.retire(state) # clears the node's own watchers: a terminal event ends them
      emit(node, state, reason, watcher_ids)
    end

    # === The node graph ======================================================
    # Every method here is called with @mutex held unless it says otherwise.

    # Caller holds @mutex.
    def fetch_node(id)
      @nodes[id] or raise Error, "unknown actor handle"
    end

    # Caller holds @mutex. Returns the node only when it accepts messages.
    def checked_node(id)
      node = @nodes[id]
      error = node_error(node)
      raise error if error

      node
    end

    # Caller holds @mutex. Why a node cannot take a message now, or nil.
    def node_error(node)
      return Error.new("unknown actor handle") unless node

      case node.state
      when :starting, :running then nil
      when :restarting then ActorRestartingError.new("actor #{node.path} is restarting")
      when :failed then ActorFailedError.new("actor #{node.path} failed")
      else ActorStoppedError.new("actor #{node.path} is #{node.state}")
      end
    end

    def node_error_for(node)
      @mutex.synchronize { node_error(node) }
    end

    # Caller holds @mutex. The node a source reference currently belongs to, or
    # nil when the reference is not (or no longer) a node's live connection.
    def node_for(source)
      node = source.actor_id && @nodes[source.actor_id]
      node if node&.reference.equal?(source)
    end

    # Caller holds @mutex.
    def source_node(source)
      node = node_for(source) or raise Error, "unknown source actor"
      raise ActorStoppedError, "actor #{node.path} is #{node.state}" unless node.active?

      node
    end

    # Caller holds @mutex. The handle of the actor behind a source reference.
    def sender_handle(source)
      id = node_for(source)&.id
      id && ActorHandle.new(id, broker: self)
    end

    # Caller holds @mutex. The node's children that are not terminal. A child
    # unregistered after its parent was leaves its id behind.
    def live_children(node)
      node.children.filter_map { |child_id| @nodes[child_id] }.reject(&:terminal?)
    end

    # Caller holds @mutex. Live descendants first, deepest first, then the node.
    def live_postorder(node)
      live_children(node).flat_map { |child| live_postorder(child) } + (node.terminal? ? [] : [node])
    end

    # Caller holds @mutex.
    def descendant?(node, ancestor_id)
      while (parent_id = node.parent_id)
        return true if parent_id == ancestor_id

        node = @nodes[parent_id] or return false
      end
      false
    end

    # Caller holds @mutex.
    def describe_node(node)
      {
        id: node.id, path: node.path, name: node.name, state: node.state, generation: node.generation,
        parent_id: node.parent_id, children: node.children.dup, pid: node.reference&.pid,
        restarts: node.restarts, waiting_on: @waiting[node.id], watchers: node.watchers.keys,
        timers: node.timers.size,
        last_exit: (node.reference&.exit_status || node.exit)&.to_s,
        last_failure: (node.reference&.exit_error || node.failure)&.message
      }
    end

    # Caller holds @mutex. Ends what only this incarnation of the node holds:
    # its timers, the watches it placed on others, and any call it was blocked
    # in. Others' watches on the node itself persist across a restart.
    def release_incarnation(node)
      node.timers.clear
      @nodes.each_value { |other| other.watchers.delete(node.id) }
      @waiting.delete(node.id)
    end

    # === Small helpers =======================================================

    def valid_timeout?(timeout)
      timeout.is_a?(Numeric) && timeout.positive? && timeout.to_f.finite?
    end

    def non_negative_number?(value)
      value.is_a?(Numeric) && value >= 0 && value.to_f.finite?
    end

    def stopped_error
      ActorStoppedError.new("actor broker is stopped")
    end

    # An executor's answer as the error to fail the work with, or nil when
    # the work was taken: a stopped executor means the broker is stopping.
    def failure_for(result)
      result == :stopped ? stopped_error : nil
    end

    def schedule_expiration(future, timeout)
      @scheduler.schedule_expiration(future, timeout) { |expiration_timeout| future.expire(expiration_timeout) }
    end

    def cancel_expiration(future)
      @scheduler.cancel_expiration(future)
    end

    def enqueue_task(delay: 0, &)
      @scheduler.enqueue(delay: delay, &)
    end
  end
end
