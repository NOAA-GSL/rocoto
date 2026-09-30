# frozen_string_literal: true

require_relative "example_actor"
require_relative "process_actor"

# Exercises RocotoActor.context from inside an actor.
class SupervisorActor
  def initialize
    @children = {}
  end

  def receive(message)
    exit! 3 if message == :crash

    return "supervisor: #{message}" unless message.is_a?(Hash)

    case message.fetch(:op)
    when :spawn
      @children[message[:name]] = RocotoActor.context.spawn(
        message.fetch(:actor_class, "ExampleActor"),
        *message.fetch(:arguments, []),
        name: message[:name],
        **message.fetch(:options, {})
      )
    when :spawn_forwarder_to_self
      @children[message[:name]] =
        RocotoActor.context.spawn(ForwardingActor, RocotoActor.context.handle, name: message[:name])
    when :call then @children.fetch(message[:name]).call(message[:message], timeout: 1)
    when :stop then @children.fetch(message[:name]).stop(timeout: 2)
    when :stop_with then @children.fetch(message[:name]).stop(timeout: 5, force: message.fetch(:force))
    when :stop_handle then message.fetch(:handle).stop(timeout: 1)
    when :handle then RocotoActor.context.handle
    when :ask_child then @children.fetch(message[:name]).ask("no")
    end
  end
end

# Builds a tree of children from initialize: depth 1 spawns ExampleActor
# leaves, deeper levels spawn InitSpawnActor children which do the same.
class InitSpawnActor
  def initialize(depth, width)
    @children = Array.new(width) do |index|
      if depth > 1
        RocotoActor.context.spawn(InitSpawnActor, depth - 1, width, name: "c#{index}")
      else
        RocotoActor.context.spawn(ExampleActor, "leaf", name: "c#{index}")
      end
    end
  end

  def receive(message)
    return @children if message == :children

    exit! 3 if message == :crash

    @children.map { |child| child.call(message, timeout: 5) }
  end
end

# Replies ready, then dies moments later from a thread started in initialize.
class DiesAfterBootActor
  def initialize
    Thread.new do
      sleep 0.05
      exit! 5
    end
  end

  def receive(_message)
    nil
  end
end

# Boots normally until a flag file appears, then fails every initialize.
class FlakyBootActor
  def initialize(flag_path)
    raise "boot refused while #{flag_path} exists" if File.exist?(flag_path)
  end

  def receive(message)
    exit! 3 if message == :crash

    :ok
  end
end

# Spawns one child successfully, then fails its own initialize.
class FailingInitSpawnActor
  def initialize
    RocotoActor.context.spawn(ExampleActor, "kept", name: "kept")
    RocotoActor.context.spawn(ExampleActor, :fail_boot, name: "broken")
  end

  def receive(_message)
    nil
  end
end
