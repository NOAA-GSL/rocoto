# frozen_string_literal: true

module RocotoActor
  # The object used to interact with an actor via ask, tell, call, stop, etc.
  # Every method here is forwarded to the owning broker in the application, or
  # over the actor's own socket from inside another actor. Only the id travels
  # between processes.
  #
  # That id names the actor itself, not the process currently running it, so a
  # handle stays valid across restarts -- generation counts them. Reference is the
  # lower-level object underneath it: one connection to one running process, with
  # its own lifecycle, replaced each time the actor is relaunched.
  class ActorHandle
    attr_reader :id

    def initialize(id, broker: nil, socket: nil)
      @id = id
      @broker = broker
      @client = socket && RocotoActor.broker_client(socket)
    end

    # Application side only: inside an actor use call, which blocks for the reply.
    def ask(message)
      local!(:ask, "use call inside an actor")
      @broker.ask(@id, message)
    end

    # Blocks the caller until the broker answers. Two actors that call each other
    # synchronously do not hang: the broker tracks who is waiting on whom, so it
    # finds the cycle and refuses the second call at once with DeadlockError,
    # naming the path it found.
    def call(message, timeout: nil)
      return ask(message).value(timeout: timeout) if @broker

      remote!
      @client.request(Protocol.request(:broker_ask, handle_id: @id, message: message, timeout: timeout))
    end

    # Sends a message that expects no reply. Returns once the broker has placed
    # it in the target's mailbox, without waiting for it to be processed; raises
    # if it could not be enqueued. The receiver sees the sender in
    # RocotoActor.context.sender.
    def tell(message)
      return @broker.tell(@id, message) if @broker

      remote!
      @client.request(Protocol.request(:broker_tell, handle_id: @id, message: message))
      nil
    end

    # In an actor, only handles of the actor's own descendants can be stopped.
    def stop(timeout: Reference::DEFAULT_STOP_TIMEOUT, force: false)
      return @broker.stop_actor(@id, timeout: timeout, force: force) if @broker

      remote!
      @client.request(Protocol.request(:broker_stop, handle_id: @id, timeout: timeout, force: force))
    end

    def alive?
      local!(:alive?)
      @broker.alive?(@id)
    end

    def state
      local!(:state)
      @broker.state(@id)
    end

    def path
      local!(:path)
      @broker.path(@id)
    end

    # RemoteError for the most recent unhandled exception in a told message
    # that ended the actor (or a previous incarnation of it), or nil.
    def last_failure
      local!(:last_failure)
      @broker.last_failure(@id)
    end

    # How the most recent incarnation's process ended, as a
    # RocotoActor::ExitStatus, or nil. Set for crashes and signal deaths;
    # nil while running or after an orderly stop.
    def last_exit
      local!(:last_exit)
      @broker.last_exit(@id)
    end

    # Incremented each time the actor is restarted; the handle stays valid.
    def generation
      local!(:generation)
      @broker.generation(@id)
    end

    def parent
      local!(:parent)
      @broker.parent(@id)
    end

    def children
      local!(:children)
      @broker.children(@id)
    end

    def ==(other)
      other.is_a?(ActorHandle) && other.id == id
    end
    alias eql? ==

    def hash
      [ActorHandle, id].hash
    end

    private

    def local!(method, hint = "handles inside an actor support call and stop")
      raise Error, "ActorHandle##{method} is only available in the application process; #{hint}" unless @broker
    end

    def remote!
      raise Error, "actor handle is not bound to a broker" unless @client
    end
  end
end
