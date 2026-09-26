# frozen_string_literal: true

require "minitest/autorun"
require_relative "../../lib/rocoto_actor"
require_relative "example_actor"
require_relative "process_actor"
require_relative "supervisor_actor"
require_relative "tell_actor"
require_relative "../validation/support"
require_relative "ticker_actor"
require_relative "watch_actor"
require "tmpdir"

class BrokerTestCase < Minitest::Test
  # True once the process no longer exists or is a zombie awaiting its parent.
  def process_gone?(pid)
    Process.kill(0, pid)
    File.read("/proc/#{pid}/status").match?(/^State:\s+Z/)
  rescue Errno::ESRCH, Errno::ENOENT # ENOENT: reaped between the signal and the read, or no /proc
    true
  rescue Errno::EPERM
    false
  end

  # Stands in for a Reference that is not registered with the broker.
  class FakeSource
    attr_reader :responses
    attr_accessor :actor_id

    def initialize
      @responses = []
    end

    def send_broker_response(request_id, on_done:, **fields)
      @responses << fields.merge(request_id: request_id, on_done: on_done)
    end
  end

  def setup
    @broker = RocotoActor::ActorBroker.new
    @database = @broker.spawn(ExampleActor, "database")
    @worker = @broker.spawn(ForwardingActor, @database)
  end

  def teardown
    @broker&.stop(timeout: 1)
  end

  private

  def wait_until(timeout: 3)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      flunk "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.02
    end
  end
end
