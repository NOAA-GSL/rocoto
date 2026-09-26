# frozen_string_literal: true

require "securerandom"

module RocotoActor
  class ActorBroker
    DEFAULT_MAX_ROUTES = 1_000
    DEFAULT_MAX_ROUTES_PER_ACTOR = 100
    DEFAULT_ROUTE_TIMEOUT = 30
    DEFAULT_MAX_LIFECYCLE_WORKERS = 2
    DEFAULT_MAX_PENDING_LIFECYCLE_REQUESTS = 100
    REQUEST_OPS = %i[broker_request broker_tell broker_spawn broker_stop broker_schedule broker_cancel
                     broker_watch broker_unwatch].freeze
    EVENTS = %i[failed restarting restarted stopped].freeze
    MAX_TIMERS_PER_ACTOR = 100
    MIN_TIMER_INTERVAL = 0.01
    SPAWN_OPTIONS = ActorContext::SPAWN_OPTIONS
    STATES = %i[starting running restarting stopping stopped failed].freeze
    TERMINAL_STATES = %i[stopped failed].freeze
    # States in which the actor's process may issue broker requests; a
    # :restarting node is only reachable while its relaunch is booting.
    ACTIVE_STATES = %i[starting running restarting].freeze
    RESTART_POLICIES = %i[never on_failure].freeze
    DEFAULT_MAX_RESTARTS = 3
    DEFAULT_RESTART_WINDOW = 60
    DEFAULT_RESTART_BACKOFF = 0.1
    POLICY_OPTIONS = %i[restart max_restarts restart_window restart_backoff].freeze
    DEFAULT_ERROR_HANDLER = lambda do |error, context|
      warn "rocoto_actor: #{context}: #{error.class}: #{error.message}"
    end

    TimerRecord = Struct.new(:id, :node_id, :generation, :message, :every)

    # Logical lifecycle record for one brokered actor. Every actor process is a
    # direct child of the application; parent/child structure exists only here.
    # A node is registered while its actor boots (:starting) so the actor can
    # spawn children from initialize; a failed boot unregisters it. spec holds
    # what is needed to relaunch the actor; restarts records recent restart times.
    Node = Struct.new(:id, :name, :path, :generation, :parent_id, :children, :state, :reference, :booting,
                      :spec, :policy, :restarts, :failure, :exit, :boot_exit, :started_at) do
      def terminal?
        TERMINAL_STATES.include?(state)
      end

      def active?
        ACTIVE_STATES.include?(state)
      end
    end

    # max_routes bounds brokered requests awaiting a target actor across the broker.
    # max_routes_per_actor bounds the broker responses owed to one source actor that
    # have not yet been written to its socket; a source at this limit is not read
    # until a response drains. route_timeout applies when a request has no timeout.
    # Spawn and stop requests from actors run on up to max_lifecycle_workers threads
    # with at most max_pending_lifecycle_requests waiting for one. error_handler
    # receives (error, context) for failures on the broker's own threads, which
    # are reported rather than allowed to kill the thread; it must not raise.
    def initialize(max_routes: DEFAULT_MAX_ROUTES, max_routes_per_actor: DEFAULT_MAX_ROUTES_PER_ACTOR,
                   route_timeout: DEFAULT_ROUTE_TIMEOUT, max_lifecycle_workers: DEFAULT_MAX_LIFECYCLE_WORKERS,
                   max_pending_lifecycle_requests: DEFAULT_MAX_PENDING_LIFECYCLE_REQUESTS,
                   error_handler: DEFAULT_ERROR_HANDLER, on_event: nil)
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
      @watchers = {} # watched node id => { watcher node id => true }
      @events = Queue.new # delivered in order on one thread that never runs anything else
      @event_thread = nil
      @waiting = {} # node id => id of the node it is blocked on (a call or a child's boot)
      @lifecycle_queue = [] # [source, request, release_response] or an internal callable
      @lifecycle_workers = []
      @idle_lifecycle_workers = 0
      @lifecycle_condition = ConditionVariable.new
      @node_ids_by_reference = {}.compare_by_identity
      @timers = {} # id => TimerRecord; an actor's timers die with its incarnation
      @mutex = Mutex.new
      @capacity_condition = ConditionVariable.new
      @nodes = {}
      @routes = 0
      @responses_by_source = Hash.new(0).compare_by_identity
      @expiries = {}.compare_by_identity
      @tasks = []
      @service_condition = ConditionVariable.new
      @service = nil
      @stopped = false
    end

    # parent: is a handle owned by this broker; the new actor becomes its logical
    # child and is stopped whenever the parent stops or fails. name: must be
    # unique among the parent's live children and forms the actor's path.
    # restart: :never (default) leaves a crashed actor :failed. :on_failure
    # relaunches it with the same handle, path, and name and a new generation,
    # after restart_backoff seconds doubling per consecutive restart, at most
    # max_restarts times within restart_window seconds; beyond that it fails.
    def spawn(actor_class, *arguments, name: nil, parent: nil, start_timeout: START_TIMEOUT,
              restart: :never, max_restarts: DEFAULT_MAX_RESTARTS, restart_window: DEFAULT_RESTART_WINDOW,
              restart_backoff: DEFAULT_RESTART_BACKOFF, **options)
      raise ArgumentError, "start_timeout must be positive" unless valid_timeout?(start_timeout)

      policy = validate_policy(restart: restart, max_restarts: max_restarts, restart_window: restart_window,
                               restart_backoff: restart_backoff)
      node, boot = launch_node(actor_class, arguments, parent_id: parent&.id, name: name, options: options,
                                                       start_timeout: start_timeout, policy: policy)
      begin
        boot.value(timeout: start_timeout)
      rescue StandardError => error
        raise settle_boot(node, error)
      end
      settle_boot(node, nil)
      ActorHandle.new(node.id, broker: self)
    end

    # Handles of top-level actors that have not stopped or failed.
    # A plain-data snapshot of every actor the broker knows, for operators.
    def describe
      @mutex.synchronize do
        {
          stopped: @stopped,
          routes_in_flight: @routes,
          timers: @timers.size,
          lifecycle_queue: @lifecycle_queue.size,
          actors: @nodes.values.map { |node| describe_node(node) }
        }
      end
    end

    def roots
      @mutex.synchronize do
        @nodes.values.select { |node| node.parent_id.nil? && !node.terminal? }
              .map { |node| ActorHandle.new(node.id, broker: self) }
      end
    end

    def stop(timeout: Reference::DEFAULT_STOP_TIMEOUT, force: false)
      nodes, threads, abandoned = @mutex.synchronize do
        @stopped = true
        @capacity_condition.broadcast
        @service_condition.broadcast
        @lifecycle_condition.broadcast
        queued = @lifecycle_queue
        @lifecycle_queue = []
        @events.close
        [@nodes.values, [@service, @event_thread, *@lifecycle_workers].compact, queued]
      end
      abandoned.each do |job|
        next unless job.is_a?(Array)

        source, request, release = job
        respond_error(source, request[:request_id], ActorStoppedError.new("actor broker is stopped"), release)
      end
      stopped = stop_subtrees(nodes, timeout: timeout, force: force)
      threads.each { |thread| thread.join unless thread == Thread.current } # stop may be called from error_handler
      stopped
    end

    def ask(id, message)
      @mutex.synchronize { checked_node(id).reference }.ask(message)
    end

    def tell(id, message)
      @mutex.synchronize { checked_node(id).reference }.tell(message)
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

    # Stops the actor's live descendants first, deepest first, then the actor.
    # The timeout is one deadline shared by the whole subtree.
    def stop_actor(id, timeout: Reference::DEFAULT_STOP_TIMEOUT, force: false)
      node = @mutex.synchronize { fetch_node(id) }
      stop_subtrees([node], timeout: timeout, force: force)
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
        fetch_node(id).children.map { |child_id| @nodes[child_id] }
                      .reject(&:terminal?)
                      .map { |child| ActorHandle.new(child.id, broker: self) }
      end
    end

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

    # Subscribes the requesting actor to the watched actor's lifecycle events.
    # Watching an actor that is already terminal delivers that event at once.
    def watch(source, request, release_response)
      @mutex.synchronize do
        watcher = source_node(source)
        watched = @nodes[request[:handle_id]] or raise Error, "unknown actor handle"
        if watched.terminal?
          detail = { reason: watched.failure&.message || watched.exit&.to_s, generation: watched.generation }
          queue_event(watched.id, watched.state, detail, [watcher.id], notify_application: false)
        else
          (@watchers[watched.id] ||= {})[watcher.id] = true
        end
      end
      respond(source, request[:request_id], true, release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    def unwatch(source, request, release_response)
      removed = @mutex.synchronize do
        watcher = @nodes[@node_ids_by_reference[source]]
        watcher && !@watchers[request[:handle_id]]&.delete(watcher.id).nil?
      end
      respond(source, request[:request_id], removed, release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    # Caller holds @mutex. Queues delivery of a lifecycle event to the
    # application's on_event and to every watcher; a terminal event ends the
    # watches. Delivery runs on the event thread, outside every lock.
    def emit(node, event, reason)
      watcher_ids = @watchers.fetch(node.id, {}).keys
      @watchers.delete(node.id) if node.terminal?
      queue_event(node.id, event, { reason: reason, generation: node.generation }, watcher_ids)
    end

    # Caller holds @mutex. Events go to one dedicated thread so that a slow
    # on_event or a blocked watcher tell delays later events only, never route
    # expiries, timers, or lifecycle work, and so that events stay ordered.
    def queue_event(node_id, event, detail, watcher_ids, notify_application: true)
      return if @stopped

      @events << [node_id, event, detail, watcher_ids, notify_application]
      return if @event_thread&.alive?

      @event_thread = Thread.new do
        Thread.current.report_on_exception = false
        while (queued = @events.pop)
          guarded("event delivery") { deliver_event(*queued) }
        end
      end
      @event_thread.name = "rocoto-actor-broker-events" if @event_thread.respond_to?(:name=)
    end

    # Caller holds @mutex. Why the incarnation behind this reference ended, or
    # nil if it has not reported anything.
    def exit_reason(reference)
      reference&.exit_error&.message || reference&.exit_status&.to_s
    end

    def deliver_event(node_id, event, detail, watcher_ids, notify_application)
      handle = ActorHandle.new(node_id, broker: self)
      guarded("on_event") { @on_event&.call(event, handle, detail) } if notify_application
      watcher_ids.each do |watcher_id|
        reference = @mutex.synchronize do
          watcher = @nodes[watcher_id]
          watcher&.active? ? watcher.reference : nil
        end
        next unless reference

        begin
          reference.tell({ op: :actor_event, event: event, actor: handle, reason: detail[:reason],
                           generation: detail[:generation] }, nil)
        rescue StandardError => error
          report_error(error, "event to #{watcher_id}")
        end
      end
    end

    # Caller holds @mutex. Ends every watch held by this incarnation.
    def purge_watches(node)
      @watchers.each_value { |watchers| watchers.delete(node.id) }
      @waiting.delete(node.id)
    end

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

      timer = @mutex.synchronize do
        node = source_node(source)
        if @timers.count { |_id, record| record.node_id == node.id } >= MAX_TIMERS_PER_ACTOR
          raise Error, "actor #{node.path} already has #{MAX_TIMERS_PER_ACTOR} timers"
        end

        record = TimerRecord.new(SecureRandom.hex(16), node.id, node.generation, request[:message], every)
        @timers[record.id] = record
        record
      end
      enqueue_task(delay: after || every) { fire_timer(timer.id) }
      respond(source, request[:request_id], Timer.new(timer.id), release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    def cancel_timer(source, request, release_response)
      cancelled = @mutex.synchronize do
        node = @nodes[@node_ids_by_reference[source]]
        record = @timers[request[:timer_id]]
        next false unless node && record && record.node_id == node.id

        !@timers.delete(record.id).nil?
      end
      respond(source, request[:request_id], cancelled, release_response)
    rescue StandardError => error
      respond_error(source, request[:request_id], error, release_response)
    end

    # Runs on the service thread. Delivers the timer's message as a tell from
    # the actor to itself, then re-arms a recurring timer. A timer whose actor
    # incarnation is gone has already been purged; a tell that fails is
    # reported and, for a recurring timer, tried again next interval.
    def fire_timer(id)
      record, reference, sender = @mutex.synchronize do
        record = @timers[id]
        next [nil, nil, nil] unless record

        node = @nodes[record.node_id]
        unless node && node.generation == record.generation && node.active?
          @timers.delete(id)
          next [nil, nil, nil]
        end
        @timers.delete(id) unless record.every
        [record, node.reference, ActorHandle.new(node.id, broker: self)]
      end
      return unless record

      begin
        reference.tell(record.message, sender)
      rescue StandardError => error
        report_error(error, "scheduled tell to #{sender.id}")
      end
      enqueue_task(delay: record.every) { fire_timer(id) } if record.every
    end

    # Caller holds @mutex. Drops every timer of the node's current incarnation.
    def purge_timers(node)
      @timers.delete_if { |_id, record| record.node_id == node.id }
    end

    def non_negative_number?(value)
      value.is_a?(Numeric) && value >= 0 && value.to_f.finite?
    end

    # Caller holds @mutex. The handle of the actor behind a source reference.
    def sender_handle(source)
      id = @node_ids_by_reference[source]
      id && ActorHandle.new(id, broker: self)
    end

    # Never blocks on the target actor; the response is sent when the target
    # future resolves or its route expires.
    def route(source, request, release_response)
      request_id = request[:request_id]
      timeout = request.fetch(:timeout, nil) || @route_timeout
      unless valid_timeout?(timeout)
        return respond_error(source, request_id, ArgumentError.new("invalid broker timeout"), release_response)
      end

      reference, sender, rejection = acquire_route(source, request[:handle_id])
      return respond_error(source, request_id, rejection, release_response) if rejection

      source_id = @mutex.synchronize { @node_ids_by_reference[source] }
      release_route = release_once { release_route_slot(source_id) }
      begin
        future = reference.ask(request[:message], sender)
      rescue StandardError => error
        release_route.call
        return respond_error(source, request_id, error, release_response)
      end

      schedule_expiry(future, timeout)
      future.on_resolve do |result, error|
        cancel_expiry(future)
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

    def validate_policy(restart:, max_restarts:, restart_window:, restart_backoff:)
      unless RESTART_POLICIES.include?(restart)
        raise ArgumentError,
              "restart must be one of #{RESTART_POLICIES.join(', ')}"
      end
      unless max_restarts.is_a?(Integer) && max_restarts.positive?
        raise ArgumentError,
              "max_restarts must be a positive integer"
      end
      raise ArgumentError, "restart_window must be positive" unless valid_timeout?(restart_window)
      unless restart_backoff.is_a?(Numeric) && restart_backoff >= 0 && restart_backoff.to_f.finite?
        raise ArgumentError, "restart_backoff must be a non-negative number"
      end

      { restart: restart, max_restarts: max_restarts, restart_window: restart_window, restart_backoff: restart_backoff }
    end

    # Starts the process and registers it as :starting. Returns [node, boot];
    # the caller must settle the boot future exactly once.
    def launch_node(actor_class, arguments, parent_id:, name:, options:, start_timeout:, policy:)
      name = validate_name(name)
      @mutex.synchronize { check_placement(parent_id, name) }

      id = SecureRandom.hex(16)
      reference, boot = Launcher.launch(actor_class, *arguments, context: { actor_id: id }, **options)
      spec = { actor_class: actor_class, arguments: arguments, options: options, start_timeout: start_timeout }
      node = register(id, reference, parent_id, name, spec, policy)
      reference.attach_broker(self)
      reference.on_exit { actor_exited(node, reference) }
      [node, boot]
    rescue Exception # rubocop:disable Lint/RescueException
      reference&.stop(force: true, timeout: 0)
      unregister(node) if node
      raise
    end

    # Moves a booted node to :running, or on failure kills the process,
    # unregisters the node, and stops any children it spawned while booting.
    # Returns the error to raise or send.
    def settle_boot(node, error)
      children, exited, reference = @mutex.synchronize do
        next [[], false, nil] unless node.booting

        node.booting = false
        if error
          [node.children.map { |child_id| @nodes[child_id] }.reject(&:terminal?), false, node.reference]
        else
          if node.state == :starting
            node.state = :running
            node.started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          end
          [[], node.boot_exit.equal?(node.reference), nil]
        end
      end
      # The process died after replying ready but before we settled: the actor
      # is registered and running from the caller's view, so treat it as a crash.
      actor_failed(node) if exited
      return nil unless error

      reference&.stop(force: true, timeout: 0)
      unregister(node)
      stop_later(children)
      Launcher.startup_error(reference, error)
    end

    # Removes a node that never finished booting; its id is known only to the
    # dead process, so no handle can refer to it.
    def unregister(node)
      @mutex.synchronize do
        retire(node, :failed)
        @nodes.delete(node.id)
        @nodes[node.parent_id]&.children&.delete(node.id)
      end
    end

    # Spawn and stop requests block for up to their timeouts, so they run on a
    # bounded pool rather than on the requesting actor's reader thread.
    # Internal jobs are bounded by the number of nodes, not by the request queue.
    def enqueue_lifecycle_job(&block)
      @mutex.synchronize do
        next if @stopped

        @lifecycle_queue << block
        start_lifecycle_worker_if_needed
        @lifecycle_condition.signal
      end
    end

    # Caller holds @mutex.
    def start_lifecycle_worker_if_needed
      return unless @idle_lifecycle_workers.zero? && @lifecycle_workers.size < @max_lifecycle_workers

      @lifecycle_workers << start_lifecycle_worker
    end

    def enqueue_lifecycle(source, request, release_response)
      rejection = @mutex.synchronize do
        next ActorStoppedError.new("actor broker is stopped") if @stopped
        if @lifecycle_queue.count { |job| job.is_a?(Array) } >= @max_pending_lifecycle_requests
          next BrokerBusyError.new("actor broker has #{@max_pending_lifecycle_requests} lifecycle requests waiting")
        end

        @lifecycle_queue << [source, request, release_response]
        start_lifecycle_worker_if_needed
        @lifecycle_condition.signal
        nil
      end
      respond_error(source, request[:request_id], rejection, release_response) if rejection
    end

    # Broker threads report failures through error_handler, so Ruby's own
    # thread-death trace would only duplicate that on stderr.
    def start_lifecycle_worker
      worker = Thread.new do
        Thread.current.report_on_exception = false
        run_lifecycle_worker
      end
      worker.name = "rocoto-actor-broker-lifecycle" if worker.respond_to?(:name=)
      worker
    end

    # A job that raises anything is reported and, for an actor request, answered
    # with an error; the worker keeps running. Should the thread die anyway, it
    # frees its slot so the next request starts a replacement.
    def run_lifecycle_worker
      loop do
        job = @mutex.synchronize do
          @idle_lifecycle_workers += 1
          @lifecycle_condition.wait(@mutex) while @lifecycle_queue.empty? && !@stopped
          @idle_lifecycle_workers -= 1
          @stopped ? nil : @lifecycle_queue.shift
        end
        return unless job

        begin
          if job.is_a?(Array)
            perform_lifecycle(*job)
          else
            job.call
          end
        rescue Exception => error # rubocop:disable Lint/RescueException
          report_error(error, "lifecycle job")
          if job.is_a?(Array)
            source, request, release_response = job
            respond_error(source, request[:request_id], Error.new("#{error.class}: #{error.message}"), release_response)
          end
          raise unless error.is_a?(StandardError)
        end
      end
    ensure
      @mutex.synchronize { @lifecycle_workers.delete(Thread.current) }
    end

    def report_error(error, context)
      @error_handler.call(error, context)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
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

      unknown = options.keys - SPAWN_OPTIONS
      raise ArgumentError, "unsupported spawn options: #{unknown.join(', ')}" unless unknown.empty?

      options = options.dup
      start_timeout = options.delete(:start_timeout) || START_TIMEOUT
      raise ArgumentError, "start_timeout must be positive" unless valid_timeout?(start_timeout)

      policy = validate_policy(
        restart: options.delete(:restart) || :never,
        max_restarts: options.delete(:max_restarts) || DEFAULT_MAX_RESTARTS,
        restart_window: options.delete(:restart_window) || DEFAULT_RESTART_WINDOW,
        restart_backoff: options.key?(:restart_backoff) ? options.delete(:restart_backoff) : DEFAULT_RESTART_BACKOFF
      )
      raise ArgumentError, "spawn options must be numeric" unless options.values.all?(Numeric)

      options[:source] = path

      node, boot = launch_node(actor_class, arguments, parent_id: parent.id, name: request[:name], options: options,
                                                       start_timeout: start_timeout, policy: policy)
      @mutex.synchronize { @waiting[parent.id] = node.id } # the parent blocks in context.spawn until the boot settles
      schedule_expiry(boot, start_timeout)
      boot.on_resolve do |_result, error|
        cancel_expiry(boot)
        @mutex.synchronize { @waiting.delete(parent.id) if @waiting[parent.id] == node.id }
        error = settle_boot(node, error)
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

    # Caller holds @mutex.
    def source_node(source)
      node = @nodes[@node_ids_by_reference[source]] or raise Error, "unknown source actor"
      raise ActorStoppedError, "actor #{node.path} is #{node.state}" unless node.active?

      node
    end

    # Caller holds @mutex.
    def descendant?(node, ancestor_id)
      while (parent_id = node.parent_id)
        return true if parent_id == ancestor_id

        node = @nodes[parent_id] or return false
      end
      false
    end

    def valid_timeout?(timeout)
      timeout.is_a?(Numeric) && timeout.positive? && timeout.to_f.finite?
    end

    def validate_name(name)
      return nil if name.nil?

      name = name.to_s
      raise ArgumentError, "actor name must not be empty" if name.empty?
      raise ArgumentError, "actor name must not contain '/'" if name.include?("/")

      name
    end

    # Caller holds @mutex. Returns the parent node, or nil for a root.
    def check_placement(parent_id, name)
      raise ActorStoppedError, "actor broker is stopped" if @stopped

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

    def register(id, reference, parent_id, name, spec, policy)
      @mutex.synchronize do
        parent = check_placement(parent_id, name)
        name ||= id
        path = parent ? "#{parent.path}/#{name}" : name
        node = Node.new(id, name, path, 1, parent_id, [], :starting, reference, true, spec, policy, 0)
        @nodes[id] = node
        @node_ids_by_reference[reference] = id
        parent&.children&.push(id)
        node
      end
    end

    # Caller holds @mutex.
    def fetch_node(id)
      @nodes[id] or raise Error, "unknown actor handle"
    end

    # Caller holds @mutex. Moves a node to a terminal state and releases what
    # only a live actor needs: the reference (threads, socket) and the relaunch
    # spec. The node itself stays so its handle keeps answering with its state.
    def retire(node, state)
      node.state = state
      purge_timers(node)
      purge_watches(node)
      reason = exit_reason(node.reference)
      if (reference = node.reference)
        node.failure = reference.exit_error || node.failure
        node.exit = reference.exit_status || node.exit
        @node_ids_by_reference.delete(reference)
        node.reference = nil
        node.spec = nil
        node.restarts = 0
      end
      emit(node, state, reason)
    end

    # Caller holds @mutex.
    def describe_node(node)
      {
        id: node.id, path: node.path, name: node.name, state: node.state, generation: node.generation,
        parent_id: node.parent_id, children: node.children.dup, pid: node.reference&.pid,
        restarts: node.restarts, waiting_on: @waiting[node.id], watchers: @watchers.fetch(node.id, {}).keys,
        timers: @timers.count { |_id, record| record.node_id == node.id },
        last_exit: (node.reference&.exit_status || node.exit)&.to_s,
        last_failure: (node.reference&.exit_error || node.failure)&.message
      }
    end

    # Caller holds @mutex. Returns the node only when it accepts messages.
    def checked_node(id)
      node = @nodes[id]
      error = node_error(node)
      raise error if error

      node
    end

    def node_error(node)
      return Error.new("unknown actor handle") unless node

      case node.state
      when :starting, :running then nil
      when :restarting then ActorRestartingError.new("actor #{node.path} is restarting")
      when :failed then ActorFailedError.new("actor #{node.path} failed")
      else ActorStoppedError.new("actor #{node.path} is #{node.state}")
      end
    end

    # Caller holds @mutex. Live descendants first, deepest first, then the node.
    def live_postorder(node)
      node.children.flat_map { |child_id| live_postorder(@nodes[child_id]) } + (node.terminal? ? [] : [node])
    end

    def stop_subtrees(roots, timeout:, force:)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      nodes = @mutex.synchronize do
        roots.flat_map { |root| live_postorder(root) }.uniq.map do |node|
          node.state = :stopping
          [node, node.reference]
        end
      end
      nodes.map do |node, reference|
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        stopped = reference.stop(timeout: [remaining, 0].max, force: force || remaining <= 0)
        @mutex.synchronize { retire(node, :stopped) if node.state == :stopping }
        stopped
      end.all?
    end

    # Called from the actor's reaper thread once its process has exited. Exits
    # during a boot are settled by the boot future's owner instead.
    def actor_exited(node, reference)
      @mutex.synchronize do
        return if node.terminal? || !node.reference.equal?(reference)

        if node.booting
          node.boot_exit = reference
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
        live_postorder(node).each { |descendant| descendant.state = :stopping unless descendant.equal?(node) }
        live = node.children.map { |child_id| @nodes[child_id] }.reject(&:terminal?)
        delay = restart_delay(node)
        purge_timers(node)
        purge_watches(node)
        if delay
          node.state = :restarting
          emit(node, :restarting, exit_reason(node.reference))
        else
          retire(node, :failed)
        end
        [delay, live]
      end
      stop_later(children)
      enqueue_task(delay: delay) { enqueue_lifecycle_job { relaunch(node) } } if delay
    end

    # Force-stops nodes on the lifecycle pool: stopping waits on processes, and
    # the service thread must stay free to run route expiries on time.
    def stop_later(nodes)
      return if nodes.empty?

      enqueue_lifecycle_job { stop_subtrees(nodes, timeout: Reference::DEFAULT_STOP_TIMEOUT, force: true) }
    end

    # Caller holds @mutex. Records a restart attempt and returns its backoff
    # delay, or nil when the policy forbids restarting now. The count is of
    # consecutive failures and resets only after the actor has run for
    # restart_window seconds, so backoff delays (time not running) can never
    # prune failures out of the window and defeat max_restarts.
    def restart_delay(node)
      return nil if @stopped || node.policy[:restart] == :never

      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      node.restarts = 0 if node.started_at && now - node.started_at > node.policy[:restart_window]
      node.started_at = nil
      return nil if node.restarts >= node.policy[:max_restarts]

      node.restarts += 1
      node.policy[:restart_backoff] * (2**(node.restarts - 1))
    end

    # Runs on a lifecycle thread. Replaces the node's dead reference with a new
    # process under the same id, path, and name, and bumps the generation.
    def relaunch(node)
      spec = @mutex.synchronize { node.state == :restarting ? node.spec : nil }
      return unless spec

      reference, boot = Launcher.launch(spec[:actor_class], *spec[:arguments], context: { actor_id: node.id },
                                                                               **spec[:options])
      installed = @mutex.synchronize do
        next false unless node.state == :restarting

        @node_ids_by_reference.delete(node.reference)
        node.failure = node.reference.exit_error || node.failure
        node.exit = node.reference.exit_status || node.exit
        purge_watches(node)
        node.reference = reference
        @node_ids_by_reference[reference] = node.id
        node.generation += 1
        node.booting = true
        true
      end
      unless installed
        reference.stop(force: true, timeout: 0)
        return
      end

      reference.attach_broker(self)
      reference.on_exit { actor_exited(node, reference) }
      schedule_expiry(boot, spec[:start_timeout])
      boot.on_resolve do |_result, error|
        cancel_expiry(boot)
        settle_restart(node, error)
      end
    rescue StandardError
      actor_failed(node)
    end

    # A failed relaunch counts as another failure under the same policy.
    def settle_restart(node, error)
      kill, exited, reference = @mutex.synchronize do
        next [false, false, nil] unless node.booting

        node.booting = false
        if node.state == :restarting && error.nil?
          node.state = :running
          node.started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          emit(node, :restarted, nil)
        end
        [!error.nil?, error.nil? && node.boot_exit.equal?(node.reference), node.reference]
      end
      return actor_failed(node) if exited
      return unless kill

      reference&.stop(force: true, timeout: 0)
      actor_failed(node)
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

    def acquire_route(source, handle_id)
      @mutex.synchronize do
        next [nil, nil, ActorStoppedError.new("actor broker is stopped")] if @stopped

        node = @nodes[handle_id]
        error = node_error(node)
        next [nil, nil, error] if error
        if @routes >= @max_routes
          next [nil, nil, BrokerBusyError.new("actor broker has #{@max_routes} routes in flight")]
        end

        source_id = @node_ids_by_reference[source]
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
      if error.is_a?(RemoteError)
        source.send_broker_response(
          request_id,
          error: error,
          error_class: error.remote_class,
          message: error.remote_message,
          backtrace: error.remote_backtrace,
          on_done: on_done
        )
      else
        source.send_broker_response(request_id, error: error, on_done: on_done)
      end
    rescue StandardError
      on_done.call
    end

    def schedule_expiry(future, timeout)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      @mutex.synchronize do
        @expiries[future] = [deadline, timeout]
        wake_service
      end
    end

    def cancel_expiry(future)
      @mutex.synchronize { @expiries.delete(future) }
    end

    # Caller holds @mutex.
    def push_task_locked(delay: 0, &block)
      return if @stopped

      @tasks << [Process.clock_gettime(Process::CLOCK_MONOTONIC) + delay, block]
      wake_service
    end

    def enqueue_task(delay: 0, &block)
      @mutex.synchronize do
        next if @stopped

        @tasks << [Process.clock_gettime(Process::CLOCK_MONOTONIC) + delay, block]
        wake_service
      end
    end

    # Caller holds @mutex. One thread per broker runs route expiries and
    # lifecycle tasks so no actor thread blocks on another actor's shutdown.
    def wake_service
      @service = start_service unless @service&.alive?
      @service_condition.signal
    end

    def start_service
      service = Thread.new do
        Thread.current.report_on_exception = false
        run_service
      end
      service.name = "rocoto-actor-broker" if service.respond_to?(:name=)
      service
    end

    def run_service
      loop do
        expired, tasks = @mutex.synchronize do
          loop do
            break if @stopped

            now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            due = @expiries.select { |_future, (deadline, _timeout)| deadline <= now }
            ready, @tasks = @tasks.partition { |run_at, _block| run_at <= now }
            unless due.empty? && ready.empty?
              due.each_key { |future| @expiries.delete(future) }
              break [due, ready.map(&:last)]
            end

            next_deadline = (@expiries.each_value.map(&:first) + @tasks.map(&:first)).min
            @service_condition.wait(@mutex, next_deadline && (next_deadline - now))
          end
        end
        return unless expired

        expired.each { |future, (_deadline, timeout)| guarded("route expiry") { future.expire(timeout) } }
        tasks.each { |task| guarded("service task") { task.call } }
      end
    end

    # Runs one unit of service work; a StandardError is reported and the thread
    # continues. Anything worse still ends the thread, which wake_service replaces.
    def guarded(context)
      yield
    rescue StandardError => error
      report_error(error, context)
    end
  end
end
