# frozen_string_literal: true

module RocotoActor
  # Returned by ActorContext#schedule when an actor calls it to schedule a one-time or recurring
  # tell to itself for some future time. It provides a reference id for the scheduled tell(s) so
  # that the actor can cancel the scheduled tell(s) later. Only the actor that created it can
  # cancel it.

  class Timer
    attr_reader :id

    # socket is provided by DecodeBindings when it arrives in the actor process.
    def initialize(id, socket: nil)
      @id = id
      @client = socket && RocotoActor.broker_client(socket)
    end

    # Cancels the scheduled tell(s) associated with this timer. Only the actor that created it can cancel it.
    # Returns true if the timer was still active and false if it had already fired, been
    # cancelled, or the actor that created it has exited.
    def cancel
      raise Error, "a timer can only be cancelled by the actor that created it" unless @client

      @client.request(Protocol.request(:broker_cancel, timer_id: @id))
    end

    def ==(other)
      other.is_a?(Timer) && other.id == id
    end
    alias eql? ==

    def hash
      [Timer, id].hash
    end
  end
end
