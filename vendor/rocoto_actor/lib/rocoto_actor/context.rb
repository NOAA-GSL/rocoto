# frozen_string_literal: true

module RocotoActor
  # Broker-facing capability available to an actor as RocotoActor.context. It
  # carries no socket, reference, or process object of any other actor.
  class ActorContext
    SPAWN_OPTIONS = %i[start_timeout mailbox_size mailbox_bytes restart max_restarts restart_window
                       restart_backoff].freeze

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
      @client.request(op: :broker_watch, handle_id: handle.id)
    end

    def unwatch(handle)
      @client.request(op: :broker_unwatch, handle_id: handle.id)
    end

    # Schedules a tell from this actor to itself: once after `after:` seconds,
    # or every `every:` seconds (starting after `after:` when both are given).
    # The message arrives in receive with sender == handle. The timer dies with
    # this incarnation of the actor; a restarted actor schedules afresh in
    # initialize. A tick that cannot be delivered is dropped and reported to the
    # broker's error_handler.
    def schedule(message, after: nil, every: nil)
      @client.request(op: :broker_schedule, message: message, after: after, every: every)
    end

    # Asks the broker to spawn a logical child of this actor and returns its
    # handle. The child process is owned by the application like any other.
    def spawn(actor_class, *arguments, name: nil, source: nil, **options)
      actor_name = actor_class.is_a?(String) ? actor_class : actor_class.name
      raise ArgumentError, "actor class must have a name" if actor_name.nil? || actor_name.empty?

      unknown = options.keys - SPAWN_OPTIONS
      raise ArgumentError, "unsupported spawn options: #{unknown.join(', ')}" unless unknown.empty?

      source ||= Object.const_source_location(actor_name)&.first
      raise ArgumentError, "cannot locate source for #{actor_name}; pass source:" unless source

      @client.request(
        op: :broker_spawn,
        actor_class: actor_name,
        source: File.expand_path(source),
        arguments: arguments,
        name: name,
        options: options
      )
    end
  end
end
