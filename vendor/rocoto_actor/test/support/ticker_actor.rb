# frozen_string_literal: true

# Exercises RocotoActor.context.schedule: records every tick with whether it
# came from itself and when it arrived.
class TickerActor
  def initialize(mode = nil)
    @ticks = []
    @timers = {}
    RocotoActor.context.schedule({ op: :tick, name: :boot }, after: 0.05) if mode == :schedule_in_init
  end

  def receive(message)
    case message.fetch(:op)
    when :schedule
      @timers[message[:name]] = RocotoActor.context.schedule({ op: :tick, name: message[:name] },
                                                             after: message[:after], every: message[:every])
      message[:name]
    when :cancel then @timers.fetch(message[:name]).cancel
    when :timer then @timers.fetch(message[:name])
    when :tick
      @ticks << [message[:name], RocotoActor.context.sender == RocotoActor.context.handle,
                 Process.clock_gettime(Process::CLOCK_MONOTONIC)]
      nil
    when :ticks then @ticks
    when :clock then Process.clock_gettime(Process::CLOCK_MONOTONIC)
    when :crash then exit! 3
    when :hang then sleep 60
    when :fill then nil
    end
  end
end
