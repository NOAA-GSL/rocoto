# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/rocoto_actor"
require_relative "support/example_actor"
require_relative "support/process_actor"
require_relative "support/supervisor_actor"
require_relative "support/tell_actor"
require_relative "validation/support"
require_relative "support/ticker_actor"
require_relative "support/watch_actor"
require "tmpdir"

class ActorBrokerTest < Minitest::Test
  def setup
    @broker = RocotoActor::ActorBroker.new
    @database = @broker.spawn(ExampleActor, "database")
    @worker = @broker.spawn(ForwardingActor, @database)
  end

  def teardown
    @broker&.stop(timeout: 1)
  end

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

  def test_spawn_records_logical_hierarchy
    workers = @broker.spawn(ExampleActor, "workers", name: "workers")
    a = @broker.spawn(ExampleActor, "a", name: :a, parent: workers)
    cache = @broker.spawn(ExampleActor, "cache", name: "cache", parent: a)

    assert_equal "workers", workers.path
    assert_equal "workers/a", a.path
    assert_equal "workers/a/cache", cache.path
    assert_nil workers.parent
    assert_equal workers, a.parent
    assert_equal [a], workers.children
    assert_equal :running, cache.state
    assert_equal @database.id, @database.path
    assert_equal "cache: hi", cache.ask("hi").value(timeout: 1)
  end

  def test_names_are_validated_and_unique_among_live_siblings
    @broker.spawn(ExampleActor, "one", name: "shared")
    assert_raises(ArgumentError) { @broker.spawn(ExampleActor, "two", name: "shared") }
    assert_raises(ArgumentError) { @broker.spawn(ExampleActor, "two", name: "") }
    assert_raises(ArgumentError) { @broker.spawn(ExampleActor, "two", name: "a/b") }
    assert_raises(RocotoActor::Error) do
      @broker.spawn(ExampleActor, "two", parent: RocotoActor::ActorHandle.new("missing"))
    end
  end

  def test_stopping_a_parent_stops_its_descendants
    parent = @broker.spawn(ExampleActor, "parent", name: "parent")
    child = @broker.spawn(ExampleActor, "child", name: "child", parent: parent)
    grandchild = @broker.spawn(ExampleActor, "grandchild", name: "grandchild", parent: child)
    sibling = @broker.spawn(ExampleActor, "sibling", name: "sibling")

    assert parent.stop(timeout: 2)

    [parent, child, grandchild].each do |handle|
      assert_equal :stopped, handle.state
      refute handle.alive?
      assert_raises(RocotoActor::ActorStoppedError) { handle.ask("gone") }
    end
    assert_empty parent.children
    assert_equal :running, sibling.state
    assert_equal "sibling: still here", sibling.ask("still here").value(timeout: 1)
  end

  def test_stopping_a_child_leaves_its_parent_running
    parent = @broker.spawn(ExampleActor, "parent", name: "parent")
    child = @broker.spawn(ExampleActor, "child", name: "child", parent: parent)

    assert child.stop(timeout: 2)

    assert_equal :stopped, child.state
    assert_equal :running, parent.state
    assert_empty parent.children
    assert_equal "parent: ok", parent.ask("ok").value(timeout: 1)
    replacement = @broker.spawn(ExampleActor, "child2", name: "child", parent: parent)
    assert_equal [replacement], parent.children
  end

  def test_failed_parent_fails_its_node_and_stops_descendants
    parent = @broker.spawn(ExampleActor, "parent", name: "parent")
    child = @broker.spawn(ExampleActor, "child", name: "child", parent: parent)
    grandchild = @broker.spawn(ExampleActor, "grandchild", name: "grandchild", parent: child)

    assert_raises(RocotoActor::ActorStoppedError) { parent.ask(:crash).value(timeout: 2) }
    # Descendants are retired deepest first; wait for the last one, not the first.
    wait_until { child.state == :stopped && grandchild.state == :stopped }

    assert_equal :failed, parent.state
    assert_equal 1, parent.generation
    assert_equal :stopped, child.state
    refute grandchild.alive?
    error = assert_raises(RocotoActor::ActorFailedError) { parent.ask("after") }
    assert_kind_of RocotoActor::ActorStoppedError, error
    assert_raises(RocotoActor::ActorStoppedError) { @broker.spawn(ExampleActor, "x", parent: parent) }
    assert_equal "database: unaffected", @database.ask("unaffected").value(timeout: 1)
  end

  def test_failed_target_reports_failure_to_brokered_callers
    target = @broker.spawn(ExampleActor, "target")
    worker = @broker.spawn(ForwardingActor, target)
    assert_raises(RocotoActor::ActorStoppedError) { target.ask(:crash).value(timeout: 2) }
    wait_until { target.state == :failed }

    error = assert_raises(RocotoActor::RemoteError) { worker.ask("after").value(timeout: 1) }

    assert_equal "RocotoActor::ActorFailedError", error.remote_class
  end

  def test_broker_stop_stops_every_subtree
    parent = @broker.spawn(ExampleActor, "parent", name: "parent")
    child = @broker.spawn(ExampleActor, "child", name: "child", parent: parent)

    assert @broker.stop(timeout: 2)

    assert_equal :stopped, parent.state
    assert_equal :stopped, child.state
    assert_equal :stopped, @worker.state
  end

  def test_subtree_stop_shares_one_deadline
    parent = @broker.spawn(ExampleActor, "parent", name: "parent")
    children = Array.new(3) { |i| @broker.spawn(ExampleActor, "c#{i}", name: "c#{i}", parent: parent) }
    children.each { |child| child.ask(:hang) }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    stopped = parent.stop(timeout: 0.5)

    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 2
    assert_equal [:stopped] * 4, ([parent] + children).map(&:state)
    ([parent] + children).each { |handle| refute handle.alive? } if stopped
  end

  def test_actor_spawns_a_child_through_its_context
    supervisor = @broker.spawn(SupervisorActor, name: "sup")

    child = supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)

    assert_kind_of RocotoActor::ActorHandle, child
    assert_equal "sup/child", child.path
    assert_equal supervisor, child.parent
    assert_equal [child], supervisor.children
    assert_equal :running, child.state
    assert_equal "c: from app", child.ask("from app").value(timeout: 1)
    assert_equal "c: from parent", supervisor.ask(op: :call, name: "child", message: "from parent").value(timeout: 2)
  end

  def test_child_reaches_its_parent_through_the_context_handle
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    assert_equal supervisor, supervisor.ask(op: :handle).value(timeout: 1)

    forwarder = supervisor.ask(op: :spawn_forwarder_to_self, name: "fwd").value(timeout: 5)

    assert_equal "supervisor: ping", forwarder.ask("ping").value(timeout: 2)
  end

  def test_stopping_the_parent_stops_children_spawned_by_the_actor
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    child = supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)

    assert supervisor.stop(timeout: 2)

    assert_equal :stopped, child.state
    refute child.alive?
  end

  def test_actor_stops_its_own_child
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    child = supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)

    assert_equal true, supervisor.ask(op: :stop, name: "child").value(timeout: 5)

    assert_equal :stopped, child.state
    assert_equal :running, supervisor.state
    assert_empty supervisor.children
  end

  def test_ask_inside_an_actor_is_rejected_with_guidance
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)

    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :ask_child, name: "child").value(timeout: 2)
    end

    assert_equal "RocotoActor::Error", error.remote_class
    assert_match(/ActorHandle#ask is only available in the application process; use call inside an actor/,
                 error.remote_message)
    assert_equal "c: still fine", supervisor.ask(op: :call, name: "child", message: "still fine").value(timeout: 2)
  end

  def test_actor_cannot_stop_a_non_descendant
    supervisor = @broker.spawn(SupervisorActor, name: "sup")

    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :stop_handle, handle: @database).value(timeout: 2)
    end

    assert_equal "RocotoActor::Error", error.remote_class
    assert_match(/not a descendant/, error.remote_message)
    assert_equal :running, @database.state
  end

  def test_child_boot_failure_is_reported_to_the_actor
    supervisor = @broker.spawn(SupervisorActor, name: "sup")

    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :spawn, name: "bad", arguments: [:fail_boot]).value(timeout: 5)
    end

    assert_equal "ArgumentError", error.remote_class
    assert_equal "invalid actor configuration", error.remote_message
    assert_empty supervisor.children
  end

  def test_internals_are_not_public
    refute_respond_to RocotoActor, :spawn
    assert_raises(NameError) { RocotoActor::Launcher }
    # Module#constants omits private constants; const_defined? still sees them.
    # Runner is loaded only in actor processes, so it is not checked here.
    %i[Launcher Reference Transport BrokerClient].each do |name|
      assert RocotoActor.const_defined?(name), "#{name} should exist"
      refute_includes RocotoActor.constants, name, "#{name} should be private"
    end
    %i[ActorBroker ActorHandle ActorContext Future ExitStatus RemoteError].each do |name|
      assert_includes RocotoActor.constants, name, "#{name} should be public"
    end
  end

  def test_actor_spawns_children_during_initialization
    supervisor = @broker.spawn(InitSpawnActor, 1, 2, name: "sup")

    children = supervisor.ask(:children).value(timeout: 2)

    assert_equal %w[sup/c0 sup/c1], children.map(&:path)
    assert_equal children, supervisor.children
    assert_equal :running, supervisor.state
    assert_equal ["leaf: hi", "leaf: hi"], supervisor.ask("hi").value(timeout: 5)
  end

  def test_nested_initialization_spawning_is_not_limited_by_the_lifecycle_pool
    broker = RocotoActor::ActorBroker.new(max_lifecycle_workers: 1)

    root = broker.spawn(InitSpawnActor, 3, 1, name: "root", start_timeout: 20)

    assert_equal [[["leaf: deep"]]], root.ask("deep").value(timeout: 10)
    assert_equal "root/c0/c0/c0", root.children.first.children.first.children.first.path
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_failed_initialization_unregisters_the_actor_and_its_children
    roots_before = @broker.roots

    error = assert_raises(RocotoActor::RemoteError) { @broker.spawn(FailingInitSpawnActor, name: "sup") }

    assert_equal "ArgumentError", error.remote_class
    assert_equal roots_before, @broker.roots
    replacement = @broker.spawn(ExampleActor, "again", name: "sup")
    assert_equal "again: ok", replacement.ask("ok").value(timeout: 1)
  end

  def test_failed_initialization_is_reported_to_a_spawning_actor
    supervisor = @broker.spawn(SupervisorActor, name: "sup")

    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :spawn, name: "bad", actor_class: "FailingInitSpawnActor").value(timeout: 5)
    end

    assert_equal "ArgumentError", error.remote_class
    assert_empty supervisor.children
  end

  def test_malformed_spawn_requests_fail_closed
    supervisor = @broker.spawn(SupervisorActor, name: "sup")

    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :spawn, name: "bad", options: { mailbox_size: "many" }).value(timeout: 5)
    end
    assert_equal "ArgumentError", error.remote_class

    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :spawn, name: "bad", options: { pgroup: false }).value(timeout: 5)
    end
    assert_equal "ArgumentError", error.remote_class

    source = FakeSource.new
    @broker.dispatch(source, op: :broker_spawn, request_id: 1, actor_class: "ExampleActor", source: "/x",
                             arguments: [], options: {})
    wait_until { source.responses.size == 1 }
    assert_equal "unknown source actor", source.responses.first.fetch(:error).message
    assert_empty supervisor.children
  end

  def test_lifecycle_requests_are_bounded
    broker = RocotoActor::ActorBroker.new(max_lifecycle_workers: 1, max_pending_lifecycle_requests: 1)
    supervisor = broker.spawn(SupervisorActor, name: "sup")
    child = supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)
    child.ask(:hang)
    # A graceful stop of the hanging child occupies the only lifecycle worker until its deadline.
    stopping = supervisor.ask(op: :stop, name: "child")
    wait_until { child.state == :stopping }

    queued = FakeSource.new
    rejected = FakeSource.new
    spawn_request = { op: :broker_spawn, actor_class: "ExampleActor", source: "/x", arguments: [], options: {} }
    broker.dispatch(queued, spawn_request.merge(request_id: 1))
    broker.dispatch(rejected, spawn_request.merge(request_id: 2))

    assert_equal 1, rejected.responses.size
    assert_instance_of RocotoActor::BrokerBusyError, rejected.responses.first.fetch(:error)
    assert_empty queued.responses
    stopping.value(timeout: 5)
    wait_until(timeout: 5) { queued.responses.size == 1 }
    assert_equal "unknown source actor", queued.responses.first.fetch(:error).message
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_child_boot_timeout_is_reported_to_the_spawning_actor
    supervisor = @broker.spawn(SupervisorActor, name: "sup")

    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :spawn, name: "slow", actor_class: "StubbornBootActor",
                     options: { start_timeout: 0.2 }).value(timeout: 5)
    end

    assert_equal "RocotoActor::TransportTimeoutError", error.remote_class
    assert_empty supervisor.children
  end

  def test_crashed_actor_restarts_with_the_same_handle_and_a_new_generation
    actor = @broker.spawn(ExampleActor, "phoenix", name: "phoenix", restart: :on_failure, restart_backoff: 0.01)
    assert_equal 1, actor.generation

    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :running && actor.generation == 2 }

    assert_equal "phoenix", actor.path
    assert_equal [actor], @broker.roots - [@database, @worker]
    assert_equal "phoenix: back", actor.ask("back").value(timeout: 2)
  end

  def test_actor_killed_from_outside_is_restarted_under_its_policy
    actor = @broker.spawn(ExampleActor, "victim", name: "victim", restart: :on_failure, restart_backoff: 0.01)
    old_pid = actor.ask(:pid).value(timeout: 2)

    Process.kill("KILL", old_pid)
    wait_until { actor.state == :running && actor.generation == 2 }

    refute_equal old_pid, actor.ask(:pid).value(timeout: 2)
    assert_nil actor.last_failure
    wait_until { actor.last_exit }
    assert_equal 9, actor.last_exit.termsig
    assert actor.last_exit.signaled?
  end

  def test_exit_reasons_are_reported_for_signals_crashes_and_told_exceptions
    old_pid = @database.ask(:pid).value(timeout: 2)
    Process.kill("TERM", old_pid)
    wait_until { @database.state == :failed && @database.last_exit }
    assert_equal "killed by signal 15 (TERM)", @database.last_exit.to_s
    assert_nil @database.last_exit.exitstatus

    crasher = @broker.spawn(ExampleActor, "crasher", name: "crasher")
    assert_raises(RocotoActor::ActorStoppedError) { crasher.ask(:crash).value(timeout: 2) }
    wait_until { crasher.last_exit }
    assert_equal 3, crasher.last_exit.exitstatus
    refute crasher.last_exit.signaled?
    assert_nil crasher.last_failure

    collector = @broker.spawn(CollectorActor, name: "collector")
    collector.tell(op: :boom)
    wait_until { collector.last_exit && collector.last_failure }
    assert_equal 1, collector.last_exit.exitstatus
    assert_equal "RuntimeError", collector.last_failure.remote_class

    assert_nil @worker.last_exit
    @worker.stop(timeout: 2)
    assert_nil @worker.last_exit
  end

  def test_actor_killed_from_outside_fails_under_the_default_policy
    old_pid = @database.ask(:pid).value(timeout: 2)

    Process.kill("KILL", old_pid)
    wait_until { @database.state == :failed }

    assert_raises(RocotoActor::ActorFailedError) { @database.ask("gone") }
    error = assert_raises(RocotoActor::RemoteError) { @worker.ask("gone").value(timeout: 2) }
    assert_equal "RocotoActor::ActorFailedError", error.remote_class
  end

  def test_restarted_actor_recreates_its_children
    supervisor = @broker.spawn(InitSpawnActor, 1, 2, name: "sup", restart: :on_failure, restart_backoff: 0.01)
    old_children = supervisor.children

    assert_raises(RocotoActor::ActorStoppedError) { supervisor.ask(:crash).value(timeout: 2) }
    wait_until(timeout: 10) { supervisor.state == :running && supervisor.generation == 2 }

    new_children = supervisor.children
    assert_equal %w[sup/c0 sup/c1], new_children.map(&:path)
    assert_empty new_children & old_children
    # The old children are stopped on the lifecycle pool, concurrently with the relaunch.
    wait_until { old_children.all? { |child| child.state == :stopped } }
    assert_equal ["leaf: hi", "leaf: hi"], supervisor.ask("hi").value(timeout: 5)
  end

  def test_requests_during_restart_fail_fast
    actor = @broker.spawn(ExampleActor, "slow", name: "slow", restart: :on_failure, restart_backoff: 1)
    caller = @broker.spawn(ForwardingActor, actor)

    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :restarting }

    assert_raises(RocotoActor::ActorRestartingError) { actor.ask("now") }
    error = assert_raises(RocotoActor::RemoteError) { caller.ask("now").value(timeout: 2) }
    assert_equal "RocotoActor::ActorRestartingError", error.remote_class
    refute actor.alive?

    wait_until(timeout: 5) { actor.state == :running }
    assert_equal "slow: later", caller.ask("later").value(timeout: 2)
  end

  def test_backoff_longer_than_the_window_cannot_defeat_the_restart_limit
    # Backoff delays (0.3s, 0.6s) exceed the window (0.5s); under timestamp
    # pruning the count would never reach max_restarts.
    actor = @broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 2,
                                             restart_window: 0.5, restart_backoff: 0.3)

    3.times do |attempt|
      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      wait_until(timeout: 5) do
        actor.state == :failed || (actor.state == :running && actor.generation == attempt + 2)
      end
    end

    assert_equal :failed, actor.state
    assert_equal 3, actor.generation
  end

  def test_healthy_uptime_resets_the_restart_count
    actor = @broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 1,
                                             restart_window: 0.2, restart_backoff: 0.01)

    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :running && actor.generation == 2 }
    sleep 0.4 # a full window of healthy running resets the count
    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :running && actor.generation == 3 }

    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :failed }
    assert_equal 3, actor.generation
  end

  def test_relaunch_jobs_do_not_consume_the_lifecycle_request_budget
    broker = RocotoActor::ActorBroker.new(max_lifecycle_workers: 1, max_pending_lifecycle_requests: 1)
    supervisor = broker.spawn(SupervisorActor, name: "sup")
    crashers = Array.new(3) do |index|
      broker.spawn(ExampleActor, "c", name: "c#{index}", restart: :on_failure, restart_backoff: 0.3)
    end
    crashers.each do |crasher|
      crasher.ask(:crash).value(timeout: 2)
    rescue StandardError
      nil
    end
    wait_until { crashers.all? { |crasher| crasher.state == :restarting } }

    child = supervisor.ask(op: :spawn, name: "child", arguments: ["ok"]).value(timeout: 5)

    assert_equal "ok: hi", child.ask("hi").value(timeout: 2)
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_unserializable_crash_messages_are_still_reported
    collector = @broker.spawn(CollectorActor, name: "collector")
    collector.tell(op: :binary_boom)
    wait_until { collector.state == :failed && collector.last_failure }

    assert_equal "RocotoActor::SerializationError", collector.last_failure.remote_class

    error = assert_raises(RocotoActor::RemoteError) { @database.ask(:binary_boom).value(timeout: 2) }
    assert_equal "RocotoActor::SerializationError", error.remote_class
  end

  def test_broker_stop_from_its_own_error_handler_does_not_deadlock
    stopped = Queue.new
    broker = RocotoActor::ActorBroker.new(error_handler: lambda { |_error, _context|
      stopped << broker.stop(timeout: 2, force: true)
    })
    broker.spawn(ExampleActor, "x")
    broker.send(:enqueue_task) { raise "boom" } # reported on the service thread, which then stops the broker

    assert_equal true, stopped.pop(timeout: 5)
    assert_empty broker.roots
  end

  def test_child_cleanup_after_a_failure_does_not_delay_route_expiries
    target = @broker.spawn(ExampleActor, "target")
    worker = @broker.spawn(ForwardingActor, target)
    parent = @broker.spawn(InitSpawnActor, 1, 2, name: "parent")
    parent.children.each { |child| child.ask(:hang) } # children will need a KILL and its confirmation grace

    assert_raises(RocotoActor::ActorStoppedError) { parent.ask(:crash).value(timeout: 2) }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    error = assert_raises(RocotoActor::RemoteError) { worker.ask(message: :hang, timeout: 0.1).value(timeout: 3) }

    assert_equal "RocotoActor::AskTimeoutError", error.remote_class
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 0.6
  end

  def test_service_thread_survives_a_failing_task_and_reports_it
    reported = []
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    target = broker.spawn(ExampleActor, "target")
    worker = broker.spawn(ForwardingActor, target)
    worker.ask(message: "warm", timeout: 1).value(timeout: 2) # starts the service thread

    broker.send(:enqueue_task) { raise "task exploded" }
    wait_until { reported.any? }

    assert_equal [[RuntimeError, "service task"]], reported
    error = assert_raises(RocotoActor::RemoteError) { worker.ask(message: :hang, timeout: 0.1).value(timeout: 2) }
    assert_equal "RocotoActor::AskTimeoutError", error.remote_class # expiries still run
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_lifecycle_worker_recovers_from_a_fatal_launch_error
    reported = []
    broker = RocotoActor::ActorBroker.new(max_lifecycle_workers: 1,
                                          error_handler: ->(error, context) { reported << [error.class, context] })
    supervisor = broker.spawn(SupervisorActor, name: "sup")
    launcher = RocotoActor.const_get(:Launcher)
    original = launcher.method(:launch)
    calls = 0
    flaky = lambda do |*args, **options|
      calls += 1
      raise NoMemoryError, "simulated" if calls == 1

      original.call(*args, **options)
    end

    launcher.stub(:launch, flaky) do
      error = assert_raises(RocotoActor::RemoteError) do
        supervisor.ask(op: :spawn, name: "first", arguments: ["a"]).value(timeout: 5)
      end
      assert_match(/NoMemoryError/, error.remote_message)
      child = supervisor.ask(op: :spawn, name: "second", arguments: ["b"]).value(timeout: 5)
      assert_equal "b: ok", child.ask("ok").value(timeout: 2)
    end

    assert_equal [[NoMemoryError, "lifecycle job"]], reported
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_remote_stop_treats_force_by_truthiness
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    child = supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)
    child.ask(:hang)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    supervisor.ask(op: :stop_with, name: "child", force: 1).value(timeout: 5)

    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 3
    assert_equal :stopped, child.state
  end

  def test_restart_limit_marks_the_actor_failed
    actor = @broker.spawn(ExampleActor, "flaky", name: "flaky", restart: :on_failure, max_restarts: 2,
                                                 restart_backoff: 0.01)

    2.times do |attempt|
      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      wait_until { actor.state == :running && actor.generation == attempt + 2 }
    end
    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :failed }

    assert_equal 3, actor.generation
    assert_raises(RocotoActor::ActorFailedError) { actor.ask("gone") }
    assert_equal "database: fine", @database.ask("fine").value(timeout: 1)
  end

  def test_stop_during_restart_backoff_prevents_the_relaunch
    actor = @broker.spawn(ExampleActor, "paused", name: "paused", restart: :on_failure, restart_backoff: 0.5)
    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :restarting }

    assert actor.stop(timeout: 1)
    sleep 0.8

    assert_equal :stopped, actor.state
    assert_equal 1, actor.generation
    refute actor.alive?
    refute_includes @broker.roots, actor
  end

  def test_actor_spawned_from_context_can_restart
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    child = supervisor.ask(op: :spawn, name: "child", arguments: ["c"],
                           options: { restart: :on_failure, restart_backoff: 0.01 }).value(timeout: 5)

    assert_raises(RocotoActor::ActorStoppedError) { child.ask(:crash).value(timeout: 2) }
    wait_until { child.state == :running && child.generation == 2 }

    assert_equal "sup/child", child.path
    assert_equal [child], supervisor.children
    assert_equal "c: again", supervisor.ask(op: :call, name: "child", message: "again").value(timeout: 2)
  end

  def test_restart_policy_is_validated
    assert_raises(ArgumentError) { @broker.spawn(ExampleActor, "x", restart: :always) }
    assert_raises(ArgumentError) { @broker.spawn(ExampleActor, "x", restart: :on_failure, max_restarts: 0) }
    assert_raises(ArgumentError) { @broker.spawn(ExampleActor, "x", restart_backoff: -1) }
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :spawn, name: "bad", options: { restart: :always }).value(timeout: 5)
    end
    assert_equal "ArgumentError", error.remote_class
  end

  def test_application_tell_is_delivered_in_order_with_asks_and_has_no_sender
    collector = @broker.spawn(CollectorActor, name: "collector")

    assert_nil collector.tell(op: :record, value: 1)
    collector.tell(op: :record, value: 2)

    assert_equal [[1, nil], [2, nil]], collector.ask(:messages).value(timeout: 2)
  end

  def test_actors_fan_out_with_tell_and_reply_through_the_sender_handle
    coordinator = @broker.spawn(CoordinatorActor, 3, name: "coordinator")

    assert_equal 3, coordinator.ask(:start).value(timeout: 2)
    wait_until { coordinator.ask(:results).value(timeout: 2).size == 3 }

    assert_equal coordinator.children.sort_by(&:id), coordinator.ask(:results).value(timeout: 2).sort_by(&:id)
  end

  def test_routed_ask_carries_the_sender_handle
    coordinator = @broker.spawn(CoordinatorActor, 1, name: "coordinator")
    worker = coordinator.children.first

    assert_nil coordinator.ask(:call_record).value(timeout: 3)

    assert_equal [[:via_call, coordinator.id]], worker.ask(:messages).value(timeout: 2)
  end

  def test_tell_and_call_from_one_actor_arrive_in_order
    sender = @broker.spawn(CollectorActor, name: "sender")
    target = @broker.spawn(CollectorActor, name: "target")

    assert_nil sender.ask(op: :tell_then_call, target: target).value(timeout: 3)

    assert_equal [[:told, sender.id], [:called, sender.id]], target.ask(:messages).value(timeout: 2)
  end

  def test_tell_to_a_stopped_actor_is_rejected_for_application_and_actors
    sender = @broker.spawn(CollectorActor, name: "sender")
    target = @broker.spawn(CollectorActor, name: "target")
    target.stop(force: true)

    assert_raises(RocotoActor::ActorStoppedError) { target.tell(op: :record, value: 1) }
    error = assert_raises(RocotoActor::RemoteError) do
      sender.ask(op: :tell_to, target: target, message: { op: :record, value: 1 }).value(timeout: 2)
    end
    assert_equal "RocotoActor::ActorStoppedError", error.remote_class
    assert_equal [], sender.ask(:messages).value(timeout: 2)
  end

  def test_exception_in_a_told_message_fails_the_actor_and_is_recorded
    collector = @broker.spawn(CollectorActor, name: "collector")

    collector.tell(op: :boom)
    wait_until { collector.state == :failed }
    wait_until { collector.last_failure }

    assert_equal "RuntimeError", collector.last_failure.remote_class
    assert_equal "told to fail", collector.last_failure.remote_message
    assert_raises(RocotoActor::ActorFailedError) { collector.tell(op: :record, value: 1) }
  end

  def test_exception_in_a_told_message_triggers_the_restart_policy
    collector = @broker.spawn(CollectorActor, name: "collector", restart: :on_failure, restart_backoff: 0.01)
    collector.tell(op: :record, value: :before)

    collector.tell(op: :boom)
    wait_until { collector.state == :running && collector.generation == 2 }

    assert_equal "RuntimeError", collector.last_failure.remote_class
    collector.tell(op: :record, value: :after)
    assert_equal [[:after, nil]], collector.ask(:messages).value(timeout: 2)
  end

  def test_relaunch_that_cannot_launch_counts_as_a_failure
    actor = @broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 1, restart_backoff: 0.01)
    launcher = RocotoActor.const_get(:Launcher)

    launcher.stub(:launch, ->(*) { raise Errno::EMFILE, "too many open files" }) do
      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      wait_until { actor.state == :failed }
    end

    assert_equal 1, actor.generation
    assert_raises(RocotoActor::ActorFailedError) { actor.ask("gone") }
  end

  def test_diagnostics_report_the_most_recent_incarnation
    collector = @broker.spawn(CollectorActor, name: "c", restart: :on_failure, max_restarts: 1, restart_backoff: 0.01)
    collector.tell(op: :boom)
    wait_until { collector.generation == 2 && collector.state == :running }
    assert_equal "told to fail", collector.last_failure.remote_message

    pid = @broker.spawn(ExampleActor, "probe", name: "probe").ask(:pid).value(timeout: 2)
    Process.kill("KILL", pid) # unrelated actor; keeps the collector's second death distinct below
    collector.tell(op: :record, value: 1)
    collector.tell(op: :boom)
    wait_until { collector.state == :failed && collector.last_exit && collector.last_exit.exitstatus == 1 }

    assert_equal "told to fail", collector.last_failure.remote_message
    assert_equal 1, collector.last_exit.exitstatus
    assert_equal 2, collector.generation
  end

  def test_non_standard_errors_in_receive_are_replies_and_exit_is_honored
    error = assert_raises(RocotoActor::RemoteError) { @database.ask(:not_implemented).value(timeout: 2) }
    assert_equal "NotImplementedError", error.remote_class
    assert_equal :running, @database.state

    assert_raises(RocotoActor::ActorStoppedError) { @database.ask(:exit_gracefully).value(timeout: 2) }
    wait_until { @database.last_exit }
    assert_equal 4, @database.last_exit.exitstatus
  end

  def test_actor_dying_right_after_boot_is_treated_as_a_failure
    actor = @broker.spawn(DiesAfterBootActor, name: "brief")

    wait_until { actor.state == :failed }

    refute actor.alive?
    assert_equal 5, actor.last_exit.exitstatus
    restarting = @broker.spawn(DiesAfterBootActor, name: "brief2", restart: :on_failure, max_restarts: 1,
                                                   restart_backoff: 0.01)
    wait_until { restarting.state == :failed }
    assert_equal 2, restarting.generation
  end

  def test_invalid_mailbox_options_are_rejected_before_spawning
    assert_raises(ArgumentError) { @broker.spawn(ExampleActor, "x", mailbox_size: 0) }
    assert_raises(ArgumentError) { @broker.spawn(ExampleActor, "x", mailbox_bytes: -1) }
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :spawn, name: "bad", options: { mailbox_size: 0 }).value(timeout: 5)
    end
    assert_equal "ArgumentError", error.remote_class
  end

  def test_broker_stop_reaches_children_of_a_failed_boot
    stopped_broker = RocotoActor::ActorBroker.new
    assert_raises(RocotoActor::RemoteError) { stopped_broker.spawn(FailingInitSpawnActor, name: "sup") }
    assert stopped_broker.stop(timeout: 3)
  end

  def test_actor_schedules_a_one_shot_message_to_itself
    ticker = @broker.spawn(TickerActor, name: "ticker")
    scheduled_at = ticker.ask(op: :clock).value(timeout: 2)

    ticker.ask(op: :schedule, name: :once, after: 0.2).value(timeout: 2)

    assert_empty ticker.ask(op: :ticks).value(timeout: 2)
    wait_until { ticker.ask(op: :ticks).value(timeout: 2).size == 1 }
    name, from_self, at = ticker.ask(op: :ticks).value(timeout: 2).first
    assert_equal :once, name
    assert from_self, "tick should arrive as a tell from the actor itself"
    assert_operator at - scheduled_at, :>=, 0.19
    sleep 0.3
    assert_equal 1, ticker.ask(op: :ticks).value(timeout: 2).size
  end

  def test_recurring_timer_fires_until_cancelled
    ticker = @broker.spawn(TickerActor, name: "ticker")
    ticker.ask(op: :schedule, name: :beat, every: 0.05).value(timeout: 2)

    wait_until { ticker.ask(op: :ticks).value(timeout: 2).size >= 4 }
    assert_equal true, ticker.ask(op: :cancel, name: :beat).value(timeout: 2)
    count = ticker.ask(op: :ticks).value(timeout: 2).size
    sleep 0.3

    assert_operator ticker.ask(op: :ticks).value(timeout: 2).size, :<=, count + 1 # at most one tick already in flight
    assert_equal false, ticker.ask(op: :cancel, name: :beat).value(timeout: 2)
  end

  def test_cancelled_before_firing_never_arrives
    ticker = @broker.spawn(TickerActor, name: "ticker")
    ticker.ask(op: :schedule, name: :later, after: 0.2).value(timeout: 2)

    assert_equal true, ticker.ask(op: :cancel, name: :later).value(timeout: 2)
    sleep 0.35

    assert_empty ticker.ask(op: :ticks).value(timeout: 2)
  end

  def test_timers_die_with_the_incarnation_and_initialize_can_reschedule
    ticker = @broker.spawn(TickerActor, :schedule_in_init, name: "ticker", restart: :on_failure,
                                                           restart_backoff: 0.01)
    ticker.ask(op: :schedule, name: :beat, every: 0.05).value(timeout: 2)
    wait_until { ticker.ask(op: :ticks).value(timeout: 2).map(&:first).include?(:boot) }

    assert_raises(RocotoActor::ActorStoppedError) { ticker.ask(op: :crash).value(timeout: 2) }
    wait_until { ticker.state == :running && ticker.generation == 2 }
    sleep 0.3
    ticks = ticker.ask(op: :ticks).value(timeout: 2).map(&:first)

    assert_equal [:boot], ticks, "only the new incarnation's own schedule should fire"
  end

  def test_undeliverable_ticks_are_reported_and_recurrence_continues
    reported = Queue.new
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    ticker = broker.spawn(TickerActor, name: "ticker", mailbox_size: 2)
    ticker.ask(op: :schedule, name: :beat, every: 0.05).value(timeout: 2)
    ticker.ask(op: :hang)
    ticker.ask(op: :fill, payload: "x" * (2 * 1024 * 1024)) # blocks the writer; later ticks fill the mailbox

    first = reported.pop(timeout: 5)
    assert_equal [RocotoActor::MailboxFullError, "scheduled tell to #{ticker.id}"], first
    second = reported.pop(timeout: 5)
    assert_equal first, second, "the recurring timer should keep trying"
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_schedule_arguments_are_validated_and_bounded
    ticker = @broker.spawn(TickerActor, name: "ticker")
    [{ after: nil, every: nil }, { after: -1, every: nil }, { after: nil, every: 0.001 }].each do |bad|
      error = assert_raises(RocotoActor::RemoteError, bad.inspect) do
        ticker.ask(op: :schedule, name: :bad, **bad).value(timeout: 2)
      end
      assert_equal "ArgumentError", error.remote_class, bad.inspect
    end
    RocotoActor::ActorBroker::MAX_TIMERS_PER_ACTOR.times do |index|
      ticker.ask(op: :schedule, name: :"t#{index}", after: 60).value(timeout: 2)
    end
    error = assert_raises(RocotoActor::RemoteError) { ticker.ask(op: :schedule, name: :extra, after: 60).value(timeout: 2) }
    assert_equal "RocotoActor::Error", error.remote_class
    assert_match(/100 timers/, error.remote_message)
  end

  def test_timer_returned_to_the_application_cannot_be_cancelled_there
    ticker = @broker.spawn(TickerActor, name: "ticker")
    ticker.ask(op: :schedule, name: :beat, every: 1).value(timeout: 2)

    timer = ticker.ask(op: :timer, name: :beat).value(timeout: 2)

    assert_kind_of RocotoActor::Timer, timer
    assert_raises(RocotoActor::Error) { timer.cancel }
    assert @broker.stop(timeout: 2)
  end

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

  def test_slow_on_event_does_not_delay_route_expiries
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

  def test_shutdown_runs_on_graceful_stop_only
    Dir.mktmpdir do |dir|
      path = File.join(dir, "graceful")
      actor = @broker.spawn(ShutdownActor, path, name: "clean")
      assert actor.stop(timeout: 2)
      assert_equal "shutdown ran", File.read(path)

      forced = @broker.spawn(ShutdownActor, File.join(dir, "forced"), name: "forced")
      forced.stop(force: true)
      refute File.exist?(File.join(dir, "forced"))
    end
  end

  def test_shutdown_failure_is_reported_and_stop_completes
    actor = @broker.spawn(ShutdownActor, "/nonexistent", :raise, name: "raises")

    assert actor.stop(timeout: 2)

    assert_equal :stopped, actor.state
    assert_equal "shutdown failed", actor.last_failure.remote_message
  end

  def test_hanging_shutdown_is_killed_at_the_deadline
    actor = @broker.spawn(ShutdownActor, "/nonexistent", :hang, name: "hangs")
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    assert actor.stop(timeout: 0.3)

    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 2
    refute actor.alive?
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

  def test_describe_reports_every_actor
    watcher = @broker.spawn(WatcherActor, name: "watcher")
    watcher.ask(op: :watch, handle: @database).value(timeout: 2)
    crasher = @broker.spawn(ExampleActor, "c", name: "crasher")
    assert_raises(RocotoActor::ActorStoppedError) { crasher.ask(:crash).value(timeout: 2) }
    wait_until { crasher.state == :failed && crasher.last_exit }

    snapshot = @broker.describe

    assert_equal false, snapshot[:stopped]
    assert_equal 0, snapshot[:routes_in_flight]
    database = snapshot[:actors].find { |actor| actor[:id] == @database.id }
    assert_equal :running, database[:state]
    assert_kind_of Integer, database[:pid]
    assert_equal [watcher.id], database[:watchers]
    crashed = snapshot[:actors].find { |actor| actor[:id] == crasher.id }
    assert_equal :failed, crashed[:state]
    assert_nil crashed[:pid]
    assert_equal "exited with status 3", crashed[:last_exit]
    assert_equal %i[id path name state generation parent_id children pid restarts waiting_on watchers timers
                    last_exit last_failure], crashed.keys
  end

  private

  def wait_until(timeout: 3)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      flunk "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.02
    end
  end

  public

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

  class FakeSource
    attr_reader :responses

    def initialize
      @responses = []
    end

    def send_broker_response(request_id, on_done:, **fields)
      @responses << fields.merge(request_id: request_id, on_done: on_done)
    end
  end
end
