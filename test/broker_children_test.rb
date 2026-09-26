# frozen_string_literal: true

require_relative "support/broker_test_case"
class BrokerChildrenTest < BrokerTestCase
  # Delays a reference's reader so the reaper observes the process's exit
  # before the frames it left behind are read. Prepended once; inert unless a
  # test sets a delay, and it only affects references created while it is set.
  module SlowReader
    class << self
      attr_accessor :delay
    end

    def read_replies
      sleep SlowReader.delay if SlowReader.delay
      super
    end
  end
  RocotoActor.const_get(:Reference).prepend(SlowReader)

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

  # The boot error and the process's exit race each other. When the reaper wins,
  # the actor's own reason must still be what the spawner sees: rejecting the
  # boot future first would drop the unread error frame and report the generic
  # "closed during startup" instead. macOS CI hit this interleaving; the delay
  # makes it deterministic here.
  def test_a_failed_boot_reports_its_own_error_when_the_exit_is_observed_first
    supervisor = @broker.spawn(SupervisorActor, name: "sup")
    SlowReader.delay = 1.0

    error = assert_raises(RocotoActor::RemoteError) do
      supervisor.ask(op: :spawn, name: "bad", actor_class: "FailingInitSpawnActor").value(timeout: 10)
    end

    assert_equal "ArgumentError", error.remote_class
  ensure
    SlowReader.delay = nil
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
end
