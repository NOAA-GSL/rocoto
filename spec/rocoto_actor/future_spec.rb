# frozen_string_literal: true

require 'spec_helper'
require 'rocoto_actor'

RSpec.describe RocotoActor::Future do
  describe 'callback error handling' do
    # callback_error_handler is class-level state shared by the whole suite, and
    # specs run in random order, so it is restored in an around hook rather than
    # at the end of the example: that way it runs even if the example fails.
    around do |example|
      original = described_class.callback_error_handler
      example.run
    ensure
      described_class.callback_error_handler = original
    end

    it 'guards a callback registered after the future already resolved' do
      reported = []
      described_class.callback_error_handler = ->(error) { reported << error.message }
      future = described_class.new
      future.fulfill(:done)

      future.on_resolve { |_result, _error| raise 'late callback bug' }

      expect(reported).to eq(['late callback bug'])
    end
  end

  it 'wakes other waiters when one of them times out' do
    future = described_class.new
    waiter = Thread.new do
      Thread.current.report_on_exception = false # the timeout error below is expected
      future.value
    end

    expect { future.value(timeout: 0.05) }.to raise_error(RocotoActor::AskTimeoutError)

    # join re-raises the waiter's exception; a timeout here means it never woke.
    expect { waiter.join(1) }.to raise_error(RocotoActor::AskTimeoutError)
    expect(waiter.status).to be_nil
  end

  it 'treats a timeout as terminal, even when a result arrives during the callback' do
    timeout_started = Queue.new
    release_timeout = Queue.new
    future = described_class.new do
      timeout_started << true
      release_timeout.pop
    end
    waiter = Thread.new do
      future.value(timeout: 0)
    rescue RocotoActor::AskTimeoutError => e
      e
    end
    timeout_started.pop

    future.fulfill(:late)
    release_timeout << true

    expect(waiter.value).to be_an_instance_of(RocotoActor::AskTimeoutError)
    expect(future).to be_ready
    expect { future.value(timeout: 0) }.to raise_error(RocotoActor::AskTimeoutError)
  end
end
