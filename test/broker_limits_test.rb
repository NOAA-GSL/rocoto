# frozen_string_literal: true

require_relative "support/broker_test_case"
require_relative "support/supervisor_actor"
require_relative "support/ticker_actor"
require_relative "support/process_actor"

# Behaviour near the user's process limit (RLIMIT_NPROC): refusing to spawn
# before the limit is hit, and failing only the launch whose threads cannot
# be created anyway; the broker's own threads exist from construction.
class BrokerLimitsTest < BrokerTestCase
  PROCESS_BUDGET = RocotoActor.const_get(:ProcessBudget)
  NEAR_LIMIT = { limit: 100, in_use: 70, margin: 32 }.freeze # 70 + 7 + 32 > 100
  NO_THREADS = ->(*) { raise ThreadError, "simulated" }

  def test_spawn_is_refused_when_one_more_actor_would_not_fit
    supervisor = @broker.spawn(SupervisorActor, name: "sup")

    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do
      error = assert_raises(RocotoActor::ResourceLimitError) { @broker.spawn(ExampleActor, "x", name: "x") }
      assert_match(/70 of the user's 100/, error.message)

      remote = assert_raises(RocotoActor::RemoteError) do
        supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)
      end
      assert_equal "RocotoActor::ResourceLimitError", remote.remote_class
    end

    assert_empty supervisor.children
    assert_equal "database: fine", @database.ask("fine").value(timeout: 1)
  end

  def test_preflight_is_skipped_when_disabled_or_unmeasurable
    unchecked = RocotoActor::ActorBroker.new(process_margin: nil)
    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do
      assert_equal "x: ok", unchecked.spawn(ExampleActor, "x").ask("ok").value(timeout: 2)
    end
    PROCESS_BUDGET.stub(:snapshot, nil) do
      assert_equal "y: ok", @broker.spawn(ExampleActor, "y").ask("ok").value(timeout: 2)
    end
  ensure
    unchecked&.stop(timeout: 2, force: true)
  end

  def test_describe_reports_process_headroom
    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do
      assert_equal NEAR_LIMIT, @broker.describe[:process_limit]
    end
    unchecked = RocotoActor::ActorBroker.new(process_margin: nil)
    assert_nil unchecked.describe[:process_limit]
  ensure
    unchecked&.stop(timeout: 2, force: true)
  end

  def test_relaunch_refused_by_the_limit_is_reported_and_counts_as_a_failure
    reported = Queue.new
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    actor = broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 1, restart_backoff: 0.01)

    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do
      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      wait_until { actor.state == :failed }
    end

    assert_equal [RocotoActor::ResourceLimitError, "relaunch of x"], reported.pop(timeout: 2)
    assert_equal 1, actor.generation
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_broker_stop_from_an_error_handler_on_a_lifecycle_thread_does_not_deadlock
    result = Queue.new
    broker = RocotoActor::ActorBroker.new(error_handler: lambda { |_error, context|
      result << broker.stop(timeout: 2, force: true) if context.start_with?("relaunch of")
    })
    actor = broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, restart_backoff: 0.01)

    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do # the relaunch is refused on a lifecycle thread and reported there
      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      assert_equal true, result.pop(timeout: 5)
    end

    assert_empty broker.roots
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_root_is_exempt_from_the_preflight
    Process.stub(:uid, 0) { assert_nil PROCESS_BUDGET.snapshot(32) }
    Process.stub(:euid, 0) { assert_nil PROCESS_BUDGET.snapshot(32) }
  end

  def test_work_racing_broker_stop_fails_as_stopped_not_as_a_resource_limit
    reported = []
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    target = broker.spawn(ExampleActor, "target")
    worker = broker.spawn(ForwardingActor, target)
    ticker = broker.spawn(TickerActor, name: "ticker")
    ticker.ask(op: :schedule, name: :beat, every: 0.02).value(timeout: 2)

    broker.instance_variable_get(:@scheduler).stop # as broker.stop does, before the actors are retired
    remote = assert_raises(RocotoActor::RemoteError) { worker.ask("hello").value(timeout: 2) }
    assert_equal "RocotoActor::ActorStoppedError", remote.remote_class
    timer = assert_raises(RocotoActor::RemoteError) do
      ticker.ask(op: :schedule, name: :late, after: 1).value(timeout: 2)
    end
    assert_equal "RocotoActor::ActorStoppedError", timer.remote_class
    sleep 0.1

    assert_empty reported, "a timer caught by shutdown is not a resource-limit failure"
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_an_actor_spawn_whose_threads_cannot_be_created_is_refused_only
    reported = []
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    supervisor = broker.spawn(SupervisorActor, name: "sup")

    # The lifecycle worker exists from the start; only the new actor's own threads are refused.
    Thread.stub(:new, NO_THREADS) do
      remote = assert_raises(RocotoActor::RemoteError) do
        supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)
      end
      assert_equal "RocotoActor::ResourceLimitError", remote.remote_class
    end

    assert_empty reported
    assert_equal :running, supervisor.state
    child = supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5) # served once a thread fits
    assert_equal "c: ok", child.ask("ok").value(timeout: 2)
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_a_launch_whose_threads_cannot_be_created_fails_that_spawn_only
    reported = []
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    actor = broker.spawn(ExampleActor, "x", name: "x")

    Thread.stub(:new, NO_THREADS) do
      error = assert_raises(RocotoActor::ResourceLimitError) { broker.spawn(ExampleActor, "y", name: "y") }
      assert_match(/reader-\d+ thread: simulated/, error.message)
    end

    assert_empty reported
    assert_equal "x: ok", actor.ask("ok").value(timeout: 2)
    assert_equal [actor], broker.roots
    assert_equal 1, broker.describe[:actors].size, "the refused actor is not registered"
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_a_relaunch_whose_threads_cannot_be_created_counts_as_a_failure
    reported = []
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    actor = broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 1, restart_backoff: 0.01)
    other = broker.spawn(ExampleActor, "other", name: "other")

    Thread.stub(:new, NO_THREADS) do # the relaunch runs on the worker that exists; its launch is what fails
      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      wait_until { actor.state == :failed }
    end

    assert_equal [[RocotoActor::ResourceLimitError, "relaunch of x"]], reported
    assert_equal 1, actor.generation
    assert_equal "other: ok", other.ask("ok").value(timeout: 2), "the other actors are unaffected"
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_broker_construction_fails_when_its_threads_cannot_be_created
    Thread.stub(:new, NO_THREADS) do
      assert_raises(RocotoActor::ResourceLimitError) { RocotoActor::ActorBroker.new }
    end
  end
end
