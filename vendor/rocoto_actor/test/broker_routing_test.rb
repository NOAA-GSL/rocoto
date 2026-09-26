# frozen_string_literal: true

require_relative "support/broker_test_case"
class BrokerRoutingTest < BrokerTestCase
  def test_worker_can_call_shared_actor_through_serialized_handle
    assert_equal "database: write", @worker.ask("write").value(timeout: 1)
    assert_equal "database: read", @database.ask("read").value(timeout: 1)
  end

  def test_worker_receives_target_actor_errors
    error = assert_raises(RocotoActor::RemoteError) do
      @worker.ask(:fail).value(timeout: 1)
    end

    assert_equal "ArgumentError", error.remote_class
    assert_equal "requested failure", error.remote_message
    assert_equal "ArgumentError: requested failure", error.message
  end

  def test_handle_has_an_opaque_serialized_form
    payload = RocotoActor.const_get(:Transport).dump(@database)

    assert_operator payload.bytesize, :<, 200
    refute_includes payload, "Mutex"
    refute_includes payload, "Thread"
  end

  def test_stopped_target_returns_actor_stopped_error
    @database.stop(force: true)

    error = assert_raises(RocotoActor::RemoteError) do
      @worker.ask("after stop").value(timeout: 1)
    end

    assert_equal "RocotoActor::ActorStoppedError", error.remote_class
  end

  def test_unknown_handle_returns_a_typed_error
    stranger = RocotoActor::ActorBroker.new
    foreign = stranger.spawn(ExampleActor, "foreign")
    worker = @broker.spawn(ForwardingActor, foreign)

    error = assert_raises(RocotoActor::RemoteError) do
      worker.ask("hello").value(timeout: 1)
    end

    assert_equal "RocotoActor::Error", error.remote_class
    assert_equal "unknown actor handle", error.remote_message
  ensure
    stranger&.stop(timeout: 1, force: true)
  end

  def test_malformed_broker_requests_fail_closed
    error = assert_raises(RocotoActor::RemoteError) do
      @worker.ask(message: "bad", timeout: -1).value(timeout: 1)
    end

    assert_equal "ArgumentError", error.remote_class
    assert_equal "database: still alive", @worker.ask("still alive").value(timeout: 1)
  end

  def test_target_stopping_during_a_brokered_call_resolves_the_caller
    pending = @worker.ask(message: :hang, timeout: 5)
    sleep 0.2
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    @database.stop(force: true)
    error = assert_raises(RocotoActor::RemoteError) { pending.value(timeout: 3) }

    assert_equal "RocotoActor::ActorStoppedError", error.remote_class
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 2
    assert_equal :running, @worker.state
  end

  def test_concurrent_brokered_requests_from_many_actors_are_serialized_by_the_target
    counter = @broker.spawn(CountingActor, name: "counter")
    workers = Array.new(5) { |i| @broker.spawn(ForwardingActor, counter, name: "w#{i}") }

    futures = workers.flat_map { |worker| Array.new(8) { worker.ask(message: :tick, timeout: 5) } }
    results = futures.map { |future| future.value(timeout: 10) }

    assert_equal (1..40).to_a, results.sort
    assert_equal 41, counter.ask(:tick).value(timeout: 1)
  end

  def test_malformed_replies_stop_the_actor_and_reject_the_request
    %i[missing_id unknown_tag].each do |kind|
      actor = @broker.spawn(MalformedReplyActor, name: "malformed-#{kind}")

      future = actor.ask(kind: kind)

      assert_raises(RocotoActor::ActorStoppedError, kind.to_s) { future.value(timeout: 3) }
      wait_until { actor.state == :failed }
      assert_match(/malformed reply/, actor.last_failure.remote_message, kind.to_s)
    end
    assert_equal "database: fine", @database.ask("fine").value(timeout: 1)
  end

  def test_error_reply_with_wrong_field_types_is_still_a_remote_error
    actor = @broker.spawn(MalformedReplyActor, name: "malformed-fields")

    error = assert_raises(RocotoActor::RemoteError) { actor.ask(kind: :bad_backtrace).value(timeout: 3) }

    assert_equal "7", error.remote_class
    assert_equal ["1"], error.remote_backtrace
    assert_equal :running, actor.state
  end

  def test_brokered_call_timeout_rejects_a_hanging_target
    error = assert_raises(RocotoActor::RemoteError) do
      @worker.ask(message: :hang, timeout: 0.05).value(timeout: 1)
    end

    assert_equal "RocotoActor::AskTimeoutError", error.remote_class
  end

  def test_broker_stop_is_idempotent_and_rejects_new_actors_and_routes
    assert @broker.stop(force: true)
    assert @broker.stop(force: true)

    assert_raises(RocotoActor::ActorStoppedError) do
      @broker.spawn(ExampleActor, "too late")
    end
    source = FakeSource.new
    @broker.dispatch(source, op: :broker_request, request_id: 1, handle_id: @database.id)
    assert_empty source.responses
    assert_equal :stopped, @database.state
  end

  def test_mutual_calls_are_refused_as_a_deadlock
    a = @broker.spawn(CallerActor, name: "a")
    b = @broker.spawn(CallerActor, name: "b")
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    error = assert_raises(RocotoActor::RemoteError) do
      a.ask(target: b, message: { target: a, message: "ping" }).value(timeout: 5)
    end

    assert_equal "RocotoActor::DeadlockError", error.remote_class
    assert_match(/b -> a/, error.remote_message)
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 2
    assert_equal :running, a.state
    assert_equal :running, b.state
  end

  def test_self_call_is_refused_as_a_deadlock
    actor = @broker.spawn(SelfCallActor, name: "selfish")

    error = assert_raises(RocotoActor::RemoteError) { actor.ask(:go).value(timeout: 5) }

    assert_equal "RocotoActor::DeadlockError", error.remote_class
  end

  def test_calling_the_parent_during_its_wait_for_the_boot_is_refused
    parent = @broker.spawn(BootCyclerActor, name: "parent")
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    error = assert_raises(RocotoActor::RemoteError) { parent.ask(:go).value(timeout: 10) }

    assert_equal "RocotoActor::DeadlockError", error.remote_class
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 5
    assert_empty parent.children
  end

  def test_asks_sent_during_a_brokered_call_are_not_dropped
    first = @worker.ask(message: :slow, timeout: 1)
    second = @worker.ask("second")
    third = @worker.ask("third")

    assert_equal "database: slow", first.value(timeout: 2)
    assert_equal "database: second", second.value(timeout: 2)
    assert_equal "database: third", third.value(timeout: 2)
  end

  def test_pending_routes_do_not_consume_a_thread_each
    workers = Array.new(4) { @broker.spawn(ForwardingActor, @database) }
    threads_before = Thread.list.size
    futures = workers.map { |worker| worker.ask(message: :hang, timeout: 0.5) }
    sleep 0.2

    assert_operator Thread.list.size - threads_before, :<=, 1
    futures.each do |future|
      error = assert_raises(RocotoActor::RemoteError) { future.value(timeout: 2) }
      assert_equal "RocotoActor::AskTimeoutError", error.remote_class
    end
  end

  def test_routes_beyond_broker_capacity_are_rejected
    broker = RocotoActor::ActorBroker.new(max_routes: 1)
    target = broker.spawn(ExampleActor, "target")
    first = broker.spawn(ForwardingActor, target)
    second = broker.spawn(ForwardingActor, target)

    hanging = first.ask(message: :hang, timeout: 1)
    sleep 0.2
    error = assert_raises(RocotoActor::RemoteError) do
      second.ask(message: "busy", timeout: 1).value(timeout: 2)
    end

    assert_equal "RocotoActor::BrokerBusyError", error.remote_class
    assert_raises(RocotoActor::RemoteError) { hanging.value(timeout: 2) }
  ensure
    broker&.stop(timeout: 1, force: true)
  end

  def test_route_without_timeout_uses_broker_route_timeout
    broker = RocotoActor::ActorBroker.new(route_timeout: 0.1)
    target = broker.spawn(ExampleActor, "target")
    worker = broker.spawn(ForwardingActor, target)

    error = assert_raises(RocotoActor::RemoteError) do
      worker.ask(message: :hang, timeout: nil).value(timeout: 2)
    end

    assert_equal "RocotoActor::AskTimeoutError", error.remote_class
  ensure
    broker&.stop(timeout: 1, force: true)
  end

  def test_stopped_source_releases_its_route
    broker = RocotoActor::ActorBroker.new(max_routes: 1)
    hanging_target = broker.spawn(ExampleActor, "hanging")
    other_target = broker.spawn(ExampleActor, "other")
    source = broker.spawn(ForwardingActor, hanging_target)
    worker = broker.spawn(ForwardingActor, other_target)

    source.ask(message: :hang, timeout: 0.2)
    sleep 0.1
    source.stop(force: true)
    sleep 0.3

    assert_equal "other: after", worker.ask(message: "after", timeout: 1).value(timeout: 2)
  ensure
    broker&.stop(timeout: 1, force: true)
  end

  def test_source_with_unwritten_responses_is_back_pressured
    broker = RocotoActor::ActorBroker.new(max_routes_per_actor: 2)
    source = FakeSource.new
    2.times { |id| broker.dispatch(source, op: :broker_request, request_id: id, handle_id: "missing") }
    third = Thread.new { broker.dispatch(source, op: :broker_request, request_id: 2, handle_id: "missing") }

    assert_nil third.join(0.1)
    assert_equal 2, source.responses.size

    source.responses.first.fetch(:on_done).call
    assert third.join(1)
    assert_equal 3, source.responses.size
    assert_equal "RocotoActor::Error", source.responses.last.fetch(:error).class.name
  ensure
    broker&.stop
  end
end
