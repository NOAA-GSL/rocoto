# frozen_string_literal: true

class ExampleActor
  def initialize(prefix)
    raise ArgumentError, "invalid actor configuration" if prefix == :fail_boot

    @prefix = prefix
  end

  def receive(message)
    case message
    when :fail then raise ArgumentError, "requested failure"
    when :hang then sleep 10
    when :crash then exit! 3
    when :pid then Process.pid
    when :not_implemented then raise NotImplementedError, "unsupported message"
    when :binary_boom then raise "bad input: \xFF".b
    when :exit_gracefully then exit 4
    when :slow
      sleep 0.05
      "#{@prefix}: slow"
    when :parent_runtime_marker
      !!$rocoto_actor_parent_runtime_marker
    when :unsupported_result
      Object.new
    when :invalid_utf8_result
      "\xFF".b
    when :oversized_result
      "x" * (RocotoActor.const_get(:Transport)::MAX_FRAME_SIZE + 1)
    when :stdio_isolated
      null = File.stat(File::NULL)
      [$stdin, $stdout, $stderr].all? { |io| io.stat.rdev == null.rdev }
    else "#{@prefix}: #{message}"
    end
  end
end
