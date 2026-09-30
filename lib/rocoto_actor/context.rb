# frozen_string_literal: true

module RocotoActor
  # Broker-facing capability available to an actor as RocotoActor.context. It
  # carries no socket, reference, or process object of any other actor.
  class ActorContext
    # The actor's own handle, or nil when the actor was not spawned by a broker.
    attr_reader :handle

    # During receive, the handle of the actor that sent the current message, or
    # nil when it came from the application. Reply with sender.tell.
    attr_accessor :sender

    def initialize(socket, actor_id)
      @client = RocotoActor.broker_client(socket)
      @handle = actor_id && ActorHandle.new(actor_id, socket: socket)
    end

    # Asks the broker to tell this actor { op: :actor_event, event:, actor:,
    # reason:, generation: } whenever the watched actor fails, restarts, is
    # restarted, or stops. The watch ends when the watched actor stops or
    # fails for good, or when this incarnation of the watcher ends.
    def watch(handle)
      @client.request(Protocol.request(:broker_watch, handle_id: handle.id))
    end

    def unwatch(handle)
      @client.request(Protocol.request(:broker_unwatch, handle_id: handle.id))
    end

    # Schedules a tell from this actor to itself: once after `after:` seconds,
    # or every `every:` seconds (starting after `after:` when both are given).
    # The message arrives in receive with sender == handle. The timer dies with
    # this incarnation of the actor; a restarted actor schedules afresh in
    # initialize. A tick that cannot be delivered is dropped and reported to the
    # broker's error_handler.
    def schedule(message, after: nil, every: nil)
      @client.request(Protocol.request(:broker_schedule, message: message, after: after, every: every))
    end

    # Asks the broker to spawn a logical child of this actor and returns its
    # handle. The child process is owned by the application like any other.
    def spawn(actor_class, *arguments, name: nil, source: nil, **options)
      actor_name, source = Launcher.resolve(actor_class, source)
      SpawnOptions.parse(options) # fail here, with the same message the broker would give

      request = Protocol.request(
        :broker_spawn,
        actor_class: actor_name,
        source: source,
        arguments: arguments,
        name: name,
        options: options
      )
      @client.request(request)
    end
  end
end
