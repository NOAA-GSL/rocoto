# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/rocoto_actor"

class FutureTest < Minitest::Test
  def test_timeout_wakes_other_waiters
    future = RocotoActor::Future.new
    waiter = Thread.new do
      Thread.current.report_on_exception = false # the timeout error below is expected
      future.value
    end

    assert_raises(RocotoActor::AskTimeoutError) { future.value(timeout: 0.05) }

    # join re-raises the waiter's exception; a timeout here means it never woke.
    assert_raises(RocotoActor::AskTimeoutError) { waiter.join(1) }
    assert_nil waiter.status
  end

  def test_timeout_is_terminal
    timeout_started = Queue.new
    release_timeout = Queue.new
    future = RocotoActor::Future.new do
      timeout_started << true
      release_timeout.pop
    end
    waiter = Thread.new do
      future.value(timeout: 0)
    rescue RocotoActor::AskTimeoutError => error
      error
    end
    timeout_started.pop

    future.fulfill(:late)
    release_timeout << true

    assert_instance_of RocotoActor::AskTimeoutError, waiter.value
    assert future.ready?
    assert_raises(RocotoActor::AskTimeoutError) { future.value(timeout: 0) }
  end
end
