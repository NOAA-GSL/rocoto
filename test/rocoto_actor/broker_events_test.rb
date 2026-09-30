# frozen_string_literal: true

require_relative "support/broker_test_case"
class BrokerEventsTest < BrokerTestCase
  def test_raising_future_callback_does_not_hide_an_actor_exit
    reported = Queue.new
    original = RocotoActor::Future.callback_error_handler
    RocotoActor::Future.callback_error_handler = ->(error) { reported << error }
    actor = @broker.spawn(ExampleActor, "x", name: "x")
    pid = actor.ask(:pid).value(timeout: 2)
    future = actor.ask(:hang)
    future.on_resolve { |_result, _error| raise "application callback bug" }

    Process.kill("KILL", pid)
    wait_until { actor.state == :failed }

    assert_equal "application callback bug", reported.pop(timeout: 2).message
    assert_raises(RocotoActor::ActorStoppedError) { future.value(timeout: 0) }
  ensure
    RocotoActor::Future.callback_error_handler = original
  end

  def test_slow_on_event_does_not_delay_route_expirations
    broker = RocotoActor::ActorBroker.new(on_event: ->(*) { sleep 1.5 })
    target = broker.spawn(ExampleActor, "target")
    worker = broker.spawn(ForwardingActor, target)
    victim = broker.spawn(ExampleActor, "victim", name: "victim")
    assert_raises(RocotoActor::ActorStoppedError) { victim.ask(:crash).value(timeout: 2) }
    wait_until { victim.state == :failed } # on_event is now sleeping on the event thread
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    error = assert_raises(RocotoActor::RemoteError) { worker.ask(message: :hang, timeout: 0.1).value(timeout: 3) }

    assert_equal "RocotoActor::AskTimeoutError", error.remote_class
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 1
  ensure
    broker&.stop(timeout: 3, force: true)
  end

  def test_watching_a_terminal_actor_does_not_replay_the_event_to_the_application
    events = Queue.new
    broker = RocotoActor::ActorBroker.new(on_event: ->(event, _handle, _detail) { events << event })
    target = broker.spawn(ExampleActor, "t", name: "target")
    watcher = broker.spawn(WatcherActor, name: "watcher")
    target.stop(timeout: 2)
    assert_equal :stopped, events.pop(timeout: 2)

    watcher.ask(op: :watch, handle: target).value(timeout: 2)
    wait_until { watcher.ask(op: :events).value(timeout: 2).size == 1 }

    assert_nil events.pop(timeout: 0.3), "the catch-up delivery must reach only the late watcher"
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_watcher_is_told_when_a_watched_actor_fails
    watcher = @broker.spawn(WatcherActor, name: "watcher")
    target = @broker.spawn(ExampleActor, "t", name: "target")
    assert_equal true, watcher.ask(op: :watch, handle: target).value(timeout: 2)

    assert_raises(RocotoActor::ActorStoppedError) { target.ask(:crash).value(timeout: 2) }
    wait_until { watcher.ask(op: :events).value(timeout: 2).size == 1 }

    event = watcher.ask(op: :events).value(timeout: 2).first
    assert_equal :failed, event[:event]
    assert_equal target, event[:actor]
    assert_match(/exited with status 3/, event[:reason])
    assert_equal 1, event[:generation]
    assert event[:from_system], "lifecycle events come from the broker, with no sender"
    assert_equal false, watcher.ask(op: :unwatch, handle: target).value(timeout: 2), "watch ends with the actor"
  end

  def test_watcher_sees_restart_and_stop_events
    watcher = @broker.spawn(WatcherActor, name: "watcher")
    target = @broker.spawn(ExampleActor, "t", name: "target", restart: :on_failure, restart_backoff: 0.01)
    watcher.ask(op: :watch, handle: target).value(timeout: 2)

    assert_raises(RocotoActor::ActorStoppedError) { target.ask(:crash).value(timeout: 2) }
    wait_until { watcher.ask(op: :events).value(timeout: 2).map { |e| e[:event] } == %i[restarting restarted] }
    assert target.stop(timeout: 2)
    wait_until { watcher.ask(op: :events).value(timeout: 2).size == 3 }

    events = watcher.ask(op: :events).value(timeout: 2)
    assert_equal(%i[restarting restarted stopped], events.map { |e| e[:event] })
    assert_equal([1, 2, 2], events.map { |e| e[:generation] })
  end

  def test_watching_a_terminal_actor_delivers_its_event_immediately
    watcher = @broker.spawn(WatcherActor, name: "watcher")
    target = @broker.spawn(ExampleActor, "t", name: "target")
    target.stop(timeout: 2)

    watcher.ask(op: :watch, handle: target).value(timeout: 2)
    wait_until { watcher.ask(op: :events).value(timeout: 2).size == 1 }

    assert_equal :stopped, watcher.ask(op: :events).value(timeout: 2).first[:event]
  end

  def test_application_on_event_hook_receives_lifecycle_events
    events = Queue.new
    broker = RocotoActor::ActorBroker.new(on_event: ->(event, handle, detail) { events << [event, handle, detail] })
    actor = broker.spawn(ExampleActor, "t", name: "target", restart: :on_failure, restart_backoff: 0.01)

    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    restarting = events.pop(timeout: 5)
    restarted = events.pop(timeout: 5)
    actor.stop(timeout: 2)
    stopped = events.pop(timeout: 5)

    assert_equal [:restarting, actor], restarting.first(2)
    assert_match(/exited with status 3/, restarting.last[:reason])
    assert_equal [:restarted, actor, { reason: nil, generation: 2 }], restarted
    assert_equal [:stopped, actor], stopped.first(2)
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_watches_end_with_the_watcher_incarnation
    watcher = @broker.spawn(WatcherActor, name: "watcher", restart: :on_failure, restart_backoff: 0.01)
    target = @broker.spawn(ExampleActor, "t", name: "target")
    watcher.ask(op: :watch, handle: target).value(timeout: 2)

    assert_raises(RocotoActor::ActorStoppedError) { watcher.ask(op: :crash).value(timeout: 2) }
    wait_until { watcher.state == :running && watcher.generation == 2 }
    assert_raises(RocotoActor::ActorStoppedError) { target.ask(:crash).value(timeout: 2) }
    wait_until { target.state == :failed }
    sleep 0.3

    assert_empty watcher.ask(op: :events).value(timeout: 2), "a restarted watcher does not inherit old watches"
  end
end
