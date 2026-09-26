# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/rocoto_actor"

class ActorNodeTest < Minitest::Test
  ACTOR_NODE = RocotoActor.const_get(:ActorNode)

  def setup
    @reference = Struct.new(:exit_error, :exit_status).new(nil, nil)
    @node = ACTOR_NODE.new(
      id: "id", name: "name", path: "name", parent_id: nil, reference: @reference,
      spec: { actor: :spec }, policy: { max_restarts: 2, restart_window: 10, restart_backoff: 0.1 }
    )
  end

  def test_boot_success_moves_starting_node_to_running
    @node.boot_succeeded(10.0)

    assert_equal :running, @node.state
    assert_equal 10.0, @node.started_at
    refute @node.booting
  end

  def test_restart_install_replaces_reference_and_increments_generation
    @node.begin_restarting
    replacement = Object.new

    assert @node.install_restarted_reference(replacement)
    assert_same replacement, @node.reference
    assert_equal 2, @node.generation
    assert @node.booting
  end

  def test_a_relaunch_boots_back_to_running_unless_a_stop_won
    fake_reference = -> { Struct.new(:exit_error, :exit_status).new(nil, nil) }
    @node.boot_succeeded(0)
    @node.begin_restarting
    @node.install_restarted_reference(fake_reference.call)
    assert @node.boot_succeeded(5.0)
    assert_equal :running, @node.state
    refute @node.first_boot?

    @node.begin_restarting
    @node.install_restarted_reference(fake_reference.call)
    @node.begin_stopping
    refute @node.boot_succeeded(6.0), "a stop that raced the boot keeps the node stopping"
    assert_equal :stopping, @node.state
  end

  def test_restarting_drops_timers_but_keeps_watchers
    @node.boot_succeeded(0)
    @node.timers["t"] = :record
    @node.watchers["w"] = true

    @node.begin_restarting

    assert_empty @node.timers
    assert_equal ["w"], @node.watchers.keys
  end

  def test_retirement_releases_live_resources_and_preserves_diagnostics
    error = RuntimeError.new("failed")
    status = Object.new
    @reference.exit_error = error
    @reference.exit_status = status

    @node.timers["t"] = :record
    @node.watchers["w"] = true

    @node.retire(:failed)

    assert_equal :failed, @node.state
    assert_empty @node.timers, "timers die with the incarnation"
    assert_empty @node.watchers, "a terminal node has no watchers left to notify"
    assert_nil @node.reference
    assert_nil @node.spec
    assert_same error, @node.failure
    assert_same status, @node.exit
  end
end
