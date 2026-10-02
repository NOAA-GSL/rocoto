# frozen_string_literal: true

module RocotoActor
  # The pending result of work sent to an actor, resolved on whichever thread
  # learns the outcome. It has both a blocking and a callback interface because
  # two very different callers use it: the application blocks on value(timeout:),
  # while the broker -- relaying one actor's ask to another -- cannot afford a
  # blocked thread per request, so it registers on_resolve and lets the
  # DeadlineScheduler expire the future if no reply arrives.
  class Future
    # The optional block runs once if the future expires, before its callbacks.
    # Reference passes { remove_pending(id) } so that a reply arriving after the
    # deadline is not matched to a future nobody is waiting for any more.
    def initialize(&on_timeout)
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @on_timeout = on_timeout
      @resolved = false
      @callbacks = []
    end

    # Blocks until the future resolves, returning the result or raising whatever
    # the actor failed with. With a timeout the deadline belongs to this caller:
    # it raises AskTimeoutError and resolves the future permanently, so a result
    # arriving afterwards is discarded rather than delivered late. This is the
    # application's path -- ActorHandle#call and Reference#stop both land here.
    def value(timeout: nil)
      deadline = timeout && (Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout)

      loop do
        timed_out = @mutex.synchronize do
          if @resolved
            raise @error if @error

            return @result
          end

          remaining = deadline && (deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC))
          if remaining && remaining <= 0
            @error = AskTimeoutError.new("actor did not reply within #{timeout} seconds")
            @resolved = true
            @condition.broadcast
            true
          else
            @condition.wait(@mutex, remaining)
            false
          end
        end
        next unless timed_out

        @on_timeout&.call
        run_callbacks
        raise @error
      end
    end

    # Whether the outcome is known, for a caller that wants to poll rather than
    # block. Nothing in the library uses it; it is part of the public surface.
    def ready?
      @mutex.synchronize { @resolved }
    end

    # Registers a block called once with (result, error) when the future resolves.
    # The block runs on the resolving thread, or immediately if already resolved.
    # It must not raise: an exception is reported to Future.callback_error_handler
    # (by default one line on standard error) and neither reaches the resolving
    # thread nor prevents later callbacks.
    #
    # This is how the broker reacts to a resolution it is not blocking on.
    # ActorBroker#route sends the broker_response back to the actor that asked,
    # and await_boot settles the node once its boot reply lands.
    def on_resolve(&block)
      resolved = @mutex.synchronize do
        @callbacks << block unless @resolved
        @resolved
      end
      invoke(block) if resolved
      self
    end

    # Resolves the future with AskTimeoutError, as if value(timeout:) had expired.
    # Returns false if it was already resolved, so a caller can tell whether it
    # won that race. This exists because a future the broker is relaying from one
    # actor to another has no thread blocked on it -- the asking actor is in
    # another process -- so the deadline has to be driven from outside:
    # ActorBroker#schedule_expiration hands the future to the DeadlineScheduler,
    # whose service thread calls this once the deadline passes.
    def expire(timeout)
      expired = @mutex.synchronize do
        next false if @resolved

        @error = AskTimeoutError.new("actor did not reply within #{timeout} seconds")
        @resolved = true
        @condition.broadcast
        true
      end
      return false unless expired

      @on_timeout&.call
      run_callbacks
      true
    end

    # Resolves with a result. The only caller is Reference#read_replies, on the
    # reader thread that is the one thing seeing an actor's replies; the broker
    # never fulfils a future itself.
    def fulfill(result)
      resolve(result, nil)
    end

    # Resolves with an error. Reference#read_replies uses it for a reply carrying
    # ok: false, and kill, fail_pending, close_and_reap and actor_exited use it to
    # fail every still-pending future when the connection dies rather than
    # answers. ActorBroker#route and await_boot reject directly only when they
    # cannot even arm an expiration. The first resolution wins, whichever path
    # reaches it first; later ones are ignored.
    def reject(error)
      resolve(nil, error)
    end

    private

    def resolve(result, error)
      @mutex.synchronize do
        return if @resolved

        @result = result
        @error = error
        @resolved = true
        @condition.broadcast
      end
      run_callbacks
    end

    def run_callbacks
      callbacks = @mutex.synchronize do
        values = @callbacks
        @callbacks = []
        values
      end
      callbacks.each { |callback| invoke(callback) }
    end

    def invoke(callback)
      callback.call(@result, @error)
    rescue StandardError, ScriptError => error
      Future.report_callback_error(error)
    end

    class << self
      # A callable receiving an exception raised by an on_resolve block. Process
      # global, so anything that replaces it has to restore it afterwards or it
      # leaks into unrelated code.
      attr_writer :callback_error_handler

      def callback_error_handler
        @callback_error_handler ||= lambda { |error|
          warn "rocoto_actor: future callback raised #{error.class}: #{error.message}"
        }
      end

      def report_callback_error(error)
        callback_error_handler.call(error)
      rescue StandardError
        nil
      end
    end
  end
end
