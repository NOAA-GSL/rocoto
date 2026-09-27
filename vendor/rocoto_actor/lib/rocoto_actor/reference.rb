# frozen_string_literal: true

module RocotoActor
  # How an actor's worker process ended, as reported by its watchdog: exited
  # with exitstatus, or killed by termsig.
  ExitStatus = Struct.new(:exitstatus, :termsig) do
    # Fields come off the wire from the watchdog; anything but an Integer is
    # treated as unknown rather than trusted.
    def self.from_reply(reply)
      new(reply[:exitstatus].is_a?(Integer) ? reply[:exitstatus] : nil,
          reply[:termsig].is_a?(Integer) ? reply[:termsig] : nil)
    end

    def signaled?
      !termsig.nil?
    end

    def to_s
      return "exited with status #{exitstatus.nil? ? 'unknown' : exitstatus}" unless signaled?

      name = begin
        Signal.signame(termsig)
      rescue ArgumentError
        nil
      end
      "killed by signal #{termsig}#{" (#{name})" if name}"
    end
  end

  class Reference
    # A connection moves forward only: open (accepts requests) -> draining (a
    # graceful stop is in flight, nothing new accepted) -> terminating (no more
    # writes; pending futures rejected) -> exited (the watchdog process was
    # reaped) -> gone (the whole process group is confirmed absent).
    PHASES = %i[open draining terminating exited gone].freeze
    DEFAULT_STOP_TIMEOUT = 5
    # After a deadline forces a KILL, stop waits this much longer to confirm the
    # group is gone, so false means "still present after KILL" (for example a
    # process in uninterruptible sleep), not merely "the deadline passed".
    KILL_CONFIRMATION_GRACE = 0.5

    attr_reader :pid

    # The logical actor this connection belongs to, set once by the broker at
    # registration. A reference outlived by its node (a dead incarnation) keeps
    # the id; the broker checks identity against the node's current reference.
    attr_accessor :actor_id

    # RemoteError describing an unhandled exception in a told message, reported
    # by the actor just before it exited; nil otherwise.
    attr_reader :exit_error

    # ExitStatus of the worker process, reported by the watchdog; nil while the
    # actor runs or when the group was killed by the application.
    attr_reader :exit_status

    def initialize(socket, pid, mailbox_size:, mailbox_bytes:)
      unless mailbox_size.is_a?(Integer) && mailbox_size.positive?
        raise ArgumentError,
              "mailbox_size must be a positive integer"
      end
      unless mailbox_bytes.is_a?(Integer) && mailbox_bytes.positive?
        raise ArgumentError,
              "mailbox_bytes must be a positive integer"
      end

      @socket = socket
      @pid = pid
      @mailbox_size = mailbox_size
      @mailbox_bytes = mailbox_bytes
      @next_id = 0
      @pending = {}
      @pending_mutex = Mutex.new
      @outbox = []
      @control_outbox = []
      @outbox_bytes = 0
      @writing_bytes = 0
      @outbox_condition = ConditionVariable.new
      @phase = :open
      @exit_condition = ConditionVariable.new
      @reaper = nil
      @reaper_mutex = Mutex.new
      @broker = nil
      @boot_id = nil
      @exit_error = nil
      @exit_status = nil
      @exit_callbacks = []
      @decode_bindings = DecodeBindings.new
      @reader = Threads.start("reader-#{pid}") { read_replies }
      @writer = Threads.start("writer-#{pid}") { write_requests }
      start_reaper # last, so a half-built reference never leaves a thread that owns the process
    rescue ResourceLimitError
      # Half-built: the launcher reaps the process. Killing here means a
      # started reader's ensure has nothing left to signal, whenever it ends.
      kill
      [@reader, @writer].compact.each { |thread| thread.kill.join(1) }
      raise
    end

    def attach_broker(broker)
      @pending_mutex.synchronize { @broker = broker }
      @decode_bindings.attach_broker(broker)
    end

    # Runs the block once on the reaper thread after the actor process has
    # exited, or immediately if it already has.
    def on_exit(&block)
      exited = @pending_mutex.synchronize do
        @exit_callbacks << block unless reached?(:exited)
        reached?(:exited)
      end
      block.call if exited
    end

    # Queues a broker response ahead of ordinary asks. on_done is called once the
    # response is written to the actor or discarded because the actor stopped.
    def send_broker_response(request_id, result: nil, error: nil, on_done: nil)
      response = Protocol.broker_response(request_id, result: result, error: error)
      payload = Transport.dump(response)
      queued = @pending_mutex.synchronize do
        next false if reached?(:terminating)

        @control_outbox << [payload, on_done]
        @outbox_condition.signal
        true
      end
      on_done&.call unless queued
    end

    # sender is the handle of the actor that sent the message, or nil from the
    # application; the receiving actor sees it as RocotoActor.context.sender.
    # It is positional so that a bare hash message is never taken as keywords.
    def ask(message, sender = nil)
      enqueue(Protocol.request(:ask, message: message, sender: sender)).last
    end

    # Enqueues a message that expects no reply. Returns once it is in the
    # mailbox; raises MailboxFullError or ActorStoppedError if it is not.
    def tell(message, sender = nil)
      enqueue(Protocol.request(:tell, message: message, sender: sender), reply: false)
      nil
    end

    # Sends the boot message; the future resolves once the actor's initialize
    # has returned. Called once by the launcher before any ask.
    def boot(arguments, context)
      @boot_id, future = enqueue(Protocol.request(:boot, arguments: arguments, context: context), limit: false)
      future
    end

    def stop(timeout: DEFAULT_STOP_TIMEOUT, force: false)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      return kill_and_confirm(deadline) if force

      shutdown = @pending_mutex.synchronize do
        next unless @phase == :open

        @phase = :draining
        @next_id += 1
        id = @next_id
        future = Future.new { remove_pending(id) }
        payload = Transport.dump(Protocol.request(:stop, id: id))
        wait_for_mailbox_space(deadline, payload.bytesize)
        @pending[id] = future
        @outbox << payload
        @outbox_bytes += payload.bytesize
        @outbox_condition.signal
        future
      end
      return wait_for_exit(deadline) unless shutdown

      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise AskTimeoutError if remaining <= 0

      shutdown.value(timeout: remaining)
      close_and_reap
      wait_for_exit(deadline)
    rescue ActorStoppedError, AskTimeoutError, IOError, SystemCallError
      kill_and_confirm(deadline)
    end

    # A query only; the reaper thread observes the exit.
    def alive?
      @pending_mutex.synchronize do
        return false if reached?(:gone)

        if reached?(:exited) && !Launcher.process_group_alive?(@pid)
          @phase = :gone
          return false
        end
        true
      end
    end

    # How long a KILL is given to take effect once deadline has passed.
    def self.kill_deadline(deadline)
      [deadline, Process.clock_gettime(Process::CLOCK_MONOTONIC) + KILL_CONFIRMATION_GRACE].max
    end

    # Kills the process group now, without waiting for it to be gone, and
    # rejects pending requests as stopped. Idempotent.
    def kill
      pending = enter(:terminating)
      return unless pending

      close_socket
      Launcher.signal_process_group(@pid, "KILL")
      pending.each { |future| future.reject(ActorStoppedError.new("actor stopped")) }
      nil
    end

    # True once the process group is gone, waiting until deadline for the
    # exit; a process that is not gone by then leaves the reference as is.
    def wait_for_exit(deadline)
      @pending_mutex.synchronize do
        until reached?(:exited)
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          return false if remaining <= 0

          @exit_condition.wait(@pending_mutex, remaining)
        end
      end
      while Launcher.process_group_alive?(@pid)
        return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

        sleep 0.01
      end
      @pending_mutex.synchronize { @phase = :gone }
      true
    end

    private

    # Queues one request and returns [id, future], or [nil, nil] when no reply
    # is expected. limit: false bypasses the mailbox bound for the boot message.
    def enqueue(fields, limit: true, reply: true)
      @pending_mutex.synchronize do
        raise ActorStoppedError, "actor is stopped" unless @phase == :open

        id = future = nil
        if reply
          @next_id += 1
          id = @next_id
          future = Future.new { remove_pending(id) }
          fields = fields.merge(id: id)
        end
        payload = Transport.dump(fields)
        raise MailboxFullError, "actor mailbox is full" if limit && !mailbox_has_space?(payload.bytesize)

        @pending[id] = future if reply
        @outbox << payload
        @outbox_bytes += payload.bytesize
        @outbox_condition.signal
        [id, future]
      end
    end

    # Caller holds @pending_mutex. True once the connection has reached the
    # phase or a later one.
    def reached?(phase)
      PHASES.index(@phase) >= PHASES.index(phase)
    end

    # Advances to `phase` (:terminating or :exited) unless already there or
    # beyond: nothing more is written, waiters are woken, and the futures that
    # can no longer be answered are returned for the caller to reject outside
    # the lock. Returns nil when there was nothing to do.
    def enter(phase)
      @pending_mutex.synchronize do
        return nil if reached?(phase)

        @phase = phase
        @outbox.clear
        @outbox_bytes = 0
        @outbox_condition.broadcast
        @exit_condition.broadcast if phase == :exited
        values = @pending.values
        @pending.clear
        values
      end
    end

    def close_socket
      @socket.close unless @socket.closed?
    rescue IOError
      nil
    end

    def kill_and_confirm(deadline)
      kill
      wait_for_exit(Reference.kill_deadline(deadline))
    end

    def write_requests
      loop do
        payload, on_done, stop_writer = @pending_mutex.synchronize do
          until reached?(:terminating) || !@outbox.empty? || !@control_outbox.empty?
            @outbox_condition.wait(@pending_mutex)
          end
          if reached?(:terminating)
            [nil, nil, true]
          elsif !@control_outbox.empty?
            [*@control_outbox.shift, false]
          else
            next_payload = @outbox.shift
            @outbox_bytes -= next_payload.bytesize
            @writing_bytes = next_payload.bytesize
            [next_payload, nil, false].tap { @outbox_condition.broadcast }
          end
        end
        break if stop_writer

        begin
          Transport.write_payload(@socket, payload)
        ensure
          on_done&.call
        end
        @pending_mutex.synchronize do
          @writing_bytes = 0
          @outbox_condition.broadcast
        end
      end
    rescue IOError, SystemCallError => error
      fail_pending(ActorStoppedError.new(error.message))
    rescue StandardError => error
      # Nothing may end the writer quietly. Without this the thread dies with
      # the outbox undrained: pending requests never resolve, later ones queue
      # behind a thread that is gone, and the actor still reports :running and
      # alive? with no failure recorded. read_replies guards the receiving side
      # the same way; this is its mirror. The cause is recorded as the exit
      # reason so last_failure explains it and the restart policy can act.
      @pending_mutex.synchronize do
        @exit_error ||= RemoteError.new(error.class.name, "writer failed: #{error.message}", error.backtrace)
      end
      fail_pending(ActorStoppedError.new("actor writer failed: #{error.message}"))
    ensure
      kill
      discard_control_outbox
    end

    def discard_control_outbox
      discarded = @pending_mutex.synchronize do
        values = @control_outbox
        @control_outbox = []
        values
      end
      # discarded is an Array of [payload, on_done] pairs, not a Hash.
      run_teardown_callbacks(discarded.map(&:last), "discarded response callback")
    end

    # A teardown callback must not stop the rest from running: this thread is
    # the only observer of the process's exit, so one raising callback must not
    # strand the node (REVIEW.md invariant 10). These callbacks belong to the
    # broker and the application, and reporting cannot go through the broker's
    # error_handler, which is what they are already reaching into.
    def run_teardown_callbacks(callbacks, context)
      callbacks.each do |callback|
        callback&.call
      rescue StandardError => error
        warn "rocoto_actor: #{context} raised #{error.class}: #{error.message}"
      end
    end

    def read_replies
      while (reply = Transport.read(@socket, bindings: @decode_bindings))
        if Protocol.broker_request?(reply)
          broker = @pending_mutex.synchronize { @broker }
          if broker
            broker.dispatch(self, reply)
          else
            send_broker_response(reply[:request_id], error: Error.new("actor broker is unavailable"))
          end
          next
        end

        case reply[:op]
        when :actor_error
          @exit_error = RemoteError.new(reply[:error_class], reply[:message], reply[:backtrace])
          next
        when :actor_exit
          @exit_status = ExitStatus.from_reply(reply)
          next
        end

        # A watchdog that fails before reading the boot message cannot echo its id.
        future = remove_pending(reply[:op] == :boot_error ? @boot_id : reply.fetch(:id))
        next unless future

        if reply[:ok]
          future.fulfill(reply[:result])
        else
          future.reject(RemoteError.new(reply[:error_class], reply[:message], reply[:backtrace]))
        end
      end
    rescue IOError, SystemCallError => error
      fail_pending(ActorStoppedError.new(error.message))
    rescue StandardError => error
      # A frame the actor should never send (malformed, wrong types, missing
      # fields): treat it as the actor breaking the protocol and stop it.
      @pending_mutex.synchronize do
        @exit_error ||= RemoteError.new(error.class.name, "malformed reply: #{error.message}", [])
      end
      fail_pending(ActorStoppedError.new("actor sent a malformed reply: #{error.message}"))
    ensure
      kill
    end

    def remove_pending(id)
      @pending_mutex.synchronize { @pending.delete(id) }
    end

    def fail_pending(error)
      pending = @pending_mutex.synchronize do
        values = @pending.values
        @pending.clear
        values
      end
      pending.each { |future| future.reject(error) }
    end

    # The actor confirmed a graceful stop: nothing is left to write, so close
    # and let the watchdog end the group on its own.
    def close_and_reap
      enter(:terminating)&.each { |future| future.reject(ActorStoppedError.new("actor stopped")) }
      close_socket
    end

    def wait_for_mailbox_space(deadline, payload_bytes)
      until mailbox_has_space?(payload_bytes)
        raise ActorStoppedError, "actor is stopped" if reached?(:terminating)

        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise AskTimeoutError if remaining <= 0

        @outbox_condition.wait(@pending_mutex, remaining)
      end
      raise ActorStoppedError, "actor is stopped" if reached?(:terminating)
    end

    def mailbox_has_space?(payload_bytes)
      @outbox.length < @mailbox_size &&
        @outbox_bytes + @writing_bytes + payload_bytes <= @mailbox_bytes
    end

    def start_reaper
      @reaper_mutex.synchronize do
        return if @reaper || !@pid

        @reaper = Threads.start("reaper-#{@pid}") do
          Process.waitpid(@pid)
          actor_exited
        rescue Errno::ECHILD
          actor_exited
        rescue StandardError => error
          # waitpid failed for something other than "no such child". This thread
          # is the only observer of the process, so record why and take the
          # normal exit path anyway: a child that cannot be reaped is unusable,
          # and a dead reaper would leave the node :running with nothing
          # recorded for the broker's lifetime (REVIEW.md invariant 10).
          @pending_mutex.synchronize do
            @exit_error ||= RemoteError.new(error.class.name, "reaper failed: #{error.message}", error.backtrace)
          end
          actor_exited
        end
      end
    end

    # Runs once on the reaper thread after the watchdog process is reaped.
    def actor_exited
      # The process is gone, but its last frames may still be unread: a boot
      # error, an actor_error, the watchdog's exit report. Killing the group
      # closes every remaining copy of the socket, so the reader reaches EOF.
      # Draining it before anything is rejected is what makes a request that
      # failed for a real reason report that reason instead of this method's
      # generic one; the reader's own shutdown rejects whatever is left.
      Launcher.signal_process_group(@pid, "KILL")
      discard_control_outbox
      join_reader
      pending = enter(:exited) or return

      pending.each { |future| future.reject(ActorStoppedError.new("actor process exited")) }
      close_socket
      # The broker learns of the exit here; nothing above may prevent it, and no
      # single callback may prevent another.
      run_teardown_callbacks(@pending_mutex.synchronize { @exit_callbacks }, "exit callback")
    end

    # Join re-raises whatever ended the reader; nothing here may propagate.
    def join_reader
      return if Thread.current == @reader

      @reader&.join(1)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
    end
  end
  private_constant :Reference
end
