# frozen_string_literal: true

require_relative "support/broker_test_case"
class BrokerLifecycleTest < BrokerTestCase
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
end
