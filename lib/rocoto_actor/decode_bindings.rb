# frozen_string_literal: true

module RocotoActor
  # Internal binding policy for capability values decoded from the wire.
  # Worker bindings are permanently socket-bound. A Reference owns one set of
  # application bindings and attaches its broker once after registration.
  class DecodeBindings
    def initialize(socket: nil)
      @socket = socket
      @broker = nil
      @mutex = Mutex.new
    end

    def attach_broker(broker)
      raise Error, "worker decode bindings cannot attach a broker" if @socket

      @mutex.synchronize do
        raise Error, "decode bindings are already attached to another broker" if @broker && !@broker.equal?(broker)

        @broker = broker
      end
    end

    def actor_handle(id)
      return ActorHandle.new(id, socket: @socket) if @socket

      ActorHandle.new(id, broker: @mutex.synchronize { @broker })
    end

    def timer(id)
      Timer.new(id, socket: @socket)
    end
  end
  private_constant :DecodeBindings
end
