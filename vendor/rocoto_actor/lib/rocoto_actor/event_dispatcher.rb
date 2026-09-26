# frozen_string_literal: true

module RocotoActor
  # Internal ordered delivery of application lifecycle events and watcher
  # notifications on one thread, started when the broker is created. Actor
  # state, including who watches whom, is owned by ActorBroker.
  class EventDispatcher
    def initialize(error_handler:, &deliver)
      @error_handler = error_handler
      @deliver = deliver
      @events = Queue.new
      @thread = nil
    end

    # Raises ResourceLimitError if the thread cannot be created.
    def start
      @thread = Threads.start("broker-events", quiet: true) do
        while (queued = @events.pop)
          ErrorReporting.guard(@error_handler, "event delivery") { @deliver.call(*queued) }
        end
      end
    end

    # Callers hold the broker mutex, which is also where stop closes the queue.
    def emit(node_id, event, detail, watcher_ids, notify_application: true)
      @events << [node_id, event, detail, watcher_ids, notify_application] unless @events.closed?
    end

    def stop
      @events.close
      @thread
    end
  end
  private_constant :EventDispatcher
end
