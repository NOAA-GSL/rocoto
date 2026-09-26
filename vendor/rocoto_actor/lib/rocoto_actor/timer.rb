# frozen_string_literal: true

module RocotoActor
  # A message an actor scheduled for itself. Only the creating actor can cancel
  # it; the value is opaque outside that actor.
  class Timer
    attr_reader :id

    def initialize(id, socket: nil)
      @id = id
      @client = socket && RocotoActor.broker_client(socket)
    end

    # Cancels the timer. Returns true if it was still scheduled. A message the
    # timer already placed in the mailbox is delivered regardless.
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
