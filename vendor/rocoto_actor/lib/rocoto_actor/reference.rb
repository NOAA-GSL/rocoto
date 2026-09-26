# frozen_string_literal: true

module RocotoActor
  # How an actor's worker process ended, as reported by its watchdog: exited
  # with exitstatus, or killed by termsig.
  ExitStatus = Struct.new(:exitstatus, :termsig) do
    def signaled?
      !termsig.nil?
    end

    def to_s
      signaled? ? "killed by signal #{termsig} (#{Signal.signame(termsig)})" : "exited with status #{exitstatus}"
    end
  end

  class Reference
    DEFAULT_STOP_TIMEOUT = 5
    # After a deadline forces a KILL, stop waits this much longer to confirm the
    # group is gone, so false means "still present after KILL" (for example a
    # process in uninterruptible sleep), not merely "the deadline passed".
    KILL_CONFIRMATION_GRACE = 0.5

    attr_reader :pid

    # RemoteError describing an unhandled exception in a told message, reported
    # by the actor just before it exited; nil otherwise.
    attr_reader :exit_error

    # ExitStatus of the worker process, reported by the watchdog; nil while the
    # actor runs or when the group was killed by the application.
    attr_reader :exit_status

    def initialize(socket, pid, mailbox_size:, mailbox_bytes:)
      raise ArgumentError, "mailbox_size must be positive" unless mailbox_size.positive?
      raise ArgumentError, "mailbox_bytes must be positive" unless mailbox_bytes.positive?

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
      @writer_stopped = false
      @stopped = false
      @termination_started = false
      @process_exited = false
      @group_exited = false
      @exit_condition = ConditionVariable.new
      @reaper = nil
      @reaper_mutex = Mutex.new
      @broker = nil
      @boot_id = nil
      @exit_error = nil
      @exit_status = nil
      @exit_callbacks = []
      start_reaper
      @reader = Thread.new { read_replies }
      @reader.name = "rocoto-actor-reader-#{pid}" if @reader.respond_to?(:name=)
      @writer = Thread.new { write_requests }
      @writer.name = "rocoto-actor-writer-#{pid}" if @writer.respond_to?(:name=)
    end

    def attach_broker(broker)
      @pending_mutex.synchronize { @broker = broker }
      # Handles decoded from this actor's replies bind to the owning broker.
      @reader[:rocoto_actor_broker] = broker
    end

    # Runs the block once on the reaper thread after the actor process has
    # exited, or immediately if it already has.
    def on_exit(&block)
      exited = @pending_mutex.synchronize do
        @exit_callbacks << block unless @process_exited
        @process_exited
      end
      block.call if exited
    end

    # Queues a broker response ahead of ordinary asks. on_done is called once the
    # response is written to the actor or discarded because the actor stopped.
    def send_broker_response(request_id, result: nil, error: nil, error_class: nil, message: nil, backtrace: nil,
                             on_done: nil)
      response = if error
                   {
                     op: :broker_response,
                     request_id: request_id,
                     ok: false,
                     error_class: error_class || error.class.name,
                     message: message || error.message,
                     backtrace: backtrace || error.backtrace || []
                   }
                 else
                   { op: :broker_response, request_id: request_id, ok: true, result: result }
                 end
      payload = Transport.dump(response)
      queued = @pending_mutex.synchronize do
        next false if @writer_stopped

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
      enqueue({ op: :ask, message: message, sender: sender }).last
    end

    # Enqueues a message that expects no reply. Returns once it is in the
    # mailbox; raises MailboxFullError or ActorStoppedError if it is not.
    def tell(message, sender = nil)
      enqueue({ op: :tell, message: message, sender: sender }, reply: false)
      nil
    end

    # Sends the boot message; the future resolves once the actor's initialize
    # has returned. Called once by the launcher before any ask.
    def boot(arguments, context)
      @boot_id, future = enqueue({ op: :boot, arguments: arguments, context: context }, limit: false)
      future
    end

    def stop(timeout: DEFAULT_STOP_TIMEOUT, force: false)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      if force
        force_stop
        return wait_for_exit(kill_deadline(deadline))
      end

      shutdown = @pending_mutex.synchronize do
        next if @stopped

        @stopped = true
        @next_id += 1
        id = @next_id
        future = Future.new { remove_pending(id) }
        payload = Transport.dump(op: :stop, id: id)
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
      force_stop
      wait_for_exit(kill_deadline(deadline))
    end

    def alive?
      @pending_mutex.synchronize do
        return false if @group_exited

        if @process_exited && !Launcher.process_group_alive?(@pid)
          @group_exited = true
          return false
        end
        true
      end
    end

    private

    # Queues one request and returns [id, future], or [nil, nil] when no reply
    # is expected. limit: false bypasses the mailbox bound for the boot message.
    def enqueue(fields, limit: true, reply: true)
      @pending_mutex.synchronize do
        raise ActorStoppedError, "actor is stopped" if @stopped

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

    def force_stop
      pending = @pending_mutex.synchronize do
        return if @termination_started

        @termination_started = true
        @stopped = true
        @writer_stopped = true
        @outbox.clear
        @outbox_bytes = 0
        @outbox_condition.broadcast
        values = @pending.values
        @pending.clear
        values
      end
      pending.each { |future| future.reject(ActorStoppedError.new("actor stopped")) }
      @socket.close unless @socket.closed?
      Launcher.signal_process_group(@pid, "KILL")
      nil
    rescue Errno::ESRCH, IOError
      nil
    ensure
      start_reaper
    end

    def write_requests
      loop do
        payload, on_done, stop_writer = @pending_mutex.synchronize do
          @outbox_condition.wait(@pending_mutex) while @outbox.empty? && @control_outbox.empty? && !@writer_stopped
          if @writer_stopped
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
      force_stop
    ensure
      discard_control_outbox
    end

    def discard_control_outbox
      discarded = @pending_mutex.synchronize do
        values = @control_outbox
        @control_outbox = []
        values
      end
      # discarded is an Array of [payload, on_done] pairs, not a Hash.
      discarded.map(&:last).each { |on_done| on_done&.call }
    end

    def read_replies
      while (reply = Transport.read(@socket))
        if ActorBroker::REQUEST_OPS.include?(reply[:op])
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
          @exit_status = ExitStatus.new(reply[:exitstatus], reply[:termsig])
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
      force_stop
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

    def close_and_reap
      @pending_mutex.synchronize do
        @writer_stopped = true
        @outbox_condition.broadcast
      end
      @socket.close unless @socket.closed?
    rescue IOError
      nil
    ensure
      start_reaper
    end

    def wait_for_mailbox_space(deadline, payload_bytes)
      until mailbox_has_space?(payload_bytes)
        raise ActorStoppedError, "actor is stopped" if @termination_started

        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise AskTimeoutError if remaining <= 0

        @outbox_condition.wait(@pending_mutex, remaining)
      end
      raise ActorStoppedError, "actor is stopped" if @termination_started
    end

    def mailbox_has_space?(payload_bytes)
      @outbox.length < @mailbox_size &&
        @outbox_bytes + @writing_bytes + payload_bytes <= @mailbox_bytes
    end

    def start_reaper
      @reaper_mutex.synchronize do
        return if @reaper || !@pid

        @reaper = Thread.new do
          Process.waitpid(@pid)
          actor_exited
        rescue Errno::ECHILD
          actor_exited
        end
        @reaper.name = "rocoto-actor-reaper-#{@pid}" if @reaper.respond_to?(:name=)
      end
    end

    def actor_exited
      pending, callbacks = @pending_mutex.synchronize do
        return if @process_exited

        @process_exited = true
        @stopped = true
        @termination_started = true
        @writer_stopped = true
        @outbox.clear
        @outbox_bytes = 0
        @outbox_condition.broadcast
        @exit_condition.broadcast
        values = @pending.values
        @pending.clear
        [values, @exit_callbacks]
      end
      pending.each { |future| future.reject(ActorStoppedError.new("actor process exited")) }
      discard_control_outbox
      # Killing the group closes every remaining copy of the socket, so the
      # reader reaches EOF; let it consume the watchdog's exit report first.
      Launcher.signal_process_group(@pid, "KILL")
      join_reader
      begin
        @socket.close unless @socket.closed?
      rescue IOError
        nil
      end
      # The broker learns of the exit here; nothing above may prevent it.
      callbacks.each(&:call)
    end

    def kill_deadline(deadline)
      [deadline, Process.clock_gettime(Process::CLOCK_MONOTONIC) + KILL_CONFIRMATION_GRACE].max
    end

    # Join re-raises whatever ended the reader; nothing here may propagate.
    def join_reader
      return if Thread.current == @reader

      @reader&.join(1)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
    end

    def wait_for_exit(deadline)
      @pending_mutex.synchronize do
        until @process_exited
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          return false if remaining <= 0

          @exit_condition.wait(@pending_mutex, remaining)
        end
      end
      while Launcher.process_group_alive?(@pid)
        return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

        sleep 0.01
      end
      @pending_mutex.synchronize { @group_exited = true }
      true
    end
  end
  private_constant :Reference
end
