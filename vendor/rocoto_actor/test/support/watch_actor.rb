# frozen_string_literal: true

require_relative "example_actor"

# Watches other actors and records the lifecycle events it is told about.
class WatcherActor
  def initialize
    @events = []
  end

  def receive(message)
    case message.fetch(:op)
    when :watch then RocotoActor.context.watch(message.fetch(:handle))
    when :unwatch then RocotoActor.context.unwatch(message.fetch(:handle))
    when :actor_event
      @events << { event: message[:event], actor: message[:actor], reason: message[:reason],
                   generation: message[:generation], from_system: RocotoActor.context.sender.nil? }
      nil
    when :events then @events
    when :crash then exit! 3
    end
  end
end

# Records that shutdown ran by writing a file the test can read.
class ShutdownActor
  def initialize(path, mode = :clean)
    @path = path
    @mode = mode
  end

  def receive(_message)
    :ok
  end

  def shutdown
    case @mode
    when :raise then raise "shutdown failed"
    when :hang then sleep 60
    else File.write(@path, "shutdown ran")
    end
  end
end

# Calls its own handle synchronously: the simplest deadlock.
class SelfCallActor
  def receive(_message)
    RocotoActor.context.handle.call(:again, timeout: 30)
  end
end

# Spawns a child that calls back into this actor while it is still waiting for
# the child's boot to finish.
class CallParentInInitActor
  def initialize(parent)
    parent.call(:hello, timeout: 30)
  end

  def receive(_message)
    :ok
  end
end

class BootCyclerActor
  def receive(_message)
    RocotoActor.context.spawn(CallParentInInitActor, RocotoActor.context.handle, name: "child",
                                                                                 start_timeout: 30)
    :spawned
  end
end

# Calls whatever target it is told to, with whatever message: lets a test build
# a call cycle between two actors.
class CallerActor
  def receive(message)
    message.fetch(:target).call(message.fetch(:message), timeout: 30)
  end
end
