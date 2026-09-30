# frozen_string_literal: true

require "rbconfig"

class ProcessActor
  def initialize(pid_file)
    @pid_file = pid_file
  end

  def receive(message)
    child_program = "Signal.trap('TERM', 'IGNORE'); sleep #{Integer(message.fetch(:seconds))}"
    pid = Process.spawn(RbConfig.ruby, "-e", child_program)
    File.write(@pid_file, pid.to_s)
    Process.waitpid(pid)
  end
end

class StubbornBootActor
  def initialize
    Signal.trap("TERM", "IGNORE")
    sleep 30
  end
end

class ForkThenExitActor
  def initialize(pid_file)
    @pid_file = pid_file
  end

  def receive(_message)
    child_pid = fork { sleep 30 }
    File.write(@pid_file, child_pid.to_s)
    exit! 7
  end
end

class BackgroundActor
  def initialize(pid_file)
    @pid_file = pid_file
  end

  def receive(_message)
    child_pid = fork do
      Signal.trap("TERM", "IGNORE")
      sleep 30
    end
    File.write(@pid_file, child_pid.to_s)
    :started
  end
end

# Returns a strictly increasing sequence number per message received.
class CountingActor
  def initialize
    @count = 0
  end

  def receive(_message)
    @count += 1
  end
end

class ForwardingActor
  def initialize(target)
    @target = target
  end

  def receive(message)
    if message.is_a?(Hash) && message.key?(:timeout)
      @target.call(message[:message], timeout: message[:timeout])
    else
      @target.call(message, timeout: 1)
    end
  end
end
