# frozen_string_literal: true

require_relative "support/broker_test_case"
require "tmpdir"
require_relative "support/supervisor_actor"
class BrokerRestartTest < BrokerTestCase
  def test_a_relaunch_whose_initialize_fails_consumes_exactly_one_restart_attempt
    Dir.mktmpdir do |dir|
      flag = File.join(dir, "refuse-boot")
      actor = @broker.spawn(FlakyBootActor, flag, name: "flaky", restart: :on_failure, max_restarts: 3,
                                                  restart_backoff: 0.01)
      File.write(flag, "")

      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      wait_until(timeout: 10) { actor.state == :failed }

      # Three attempts, each installing a reference: generations 2, 3, 4. A
      # double-counted failure would exhaust the budget after fewer attempts.
      assert_equal 4, actor.generation
      assert_match(/boot refused/, actor.last_failure&.remote_message.to_s)
    end
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

  def test_handle_returned_by_a_restarted_actor_binds_to_the_broker
    actor = @broker.spawn(SupervisorActor, name: "supervisor", restart: :on_failure, restart_backoff: 0.01)
    first_handle = actor.ask(op: :handle).value(timeout: 2)
    assert_equal actor, first_handle

    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :running && actor.generation == 2 }

    restarted_handle = actor.ask(op: :handle).value(timeout: 2)

    assert_equal actor, restarted_handle
    assert_equal "supervisor: usable", restarted_handle.ask("usable").value(timeout: 2)
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
    # Backoff delays (0.6s, 1.2s) exceed the window (1s); under timestamp pruning
    # the count would never reach max_restarts. As above, the window also has to
    # outlast the gap between :running and the next crash on a slow machine.
    actor = @broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 2,
                                             restart_window: 1, restart_backoff: 0.6)

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
    # The window must comfortably exceed the time a loaded machine takes between
    # an incarnation becoming :running and its next crash being recorded: if the
    # last crash lands outside it the count resets by design, the actor restarts
    # again, and :failed never arrives. macOS CI failed here with 0.2s.
    actor = @broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 1,
                                             restart_window: 1, restart_backoff: 0.01)

    assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
    wait_until { actor.state == :running && actor.generation == 2 }
    sleep 1.2 # a full window of healthy running resets the count
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

  def test_child_cleanup_after_a_failure_does_not_delay_route_expirations
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
    assert_equal "RocotoActor::AskTimeoutError", error.remote_class # expirations still run
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
    # Its own broker, so the simulated failure is captured rather than warned to
    # stderr by the default handler, where it reads like real descriptor
    # exhaustion in a CI log and sends the next reader chasing it.
    reported = []
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    actor = broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 1, restart_backoff: 0.01)
    launcher = RocotoActor.const_get(:Launcher)

    launcher.stub(:launch, ->(*) { raise Errno::EMFILE, "too many open files" }) do
      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      wait_until { actor.state == :failed }
    end

    assert_equal [[Errno::EMFILE, "relaunch of x"]], reported
    assert_equal 1, actor.generation
    assert_raises(RocotoActor::ActorFailedError) { actor.ask("gone") }
  ensure
    broker&.stop(timeout: 2, force: true)
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
end
