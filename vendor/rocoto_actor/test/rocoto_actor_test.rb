# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/rocoto_actor"
require_relative "support/example_actor"
require_relative "support/process_actor"
require "tmpdir"

# Exercises the internal process launcher and Reference directly; applications
# go through RocotoActor::ActorBroker.
class RocotoActorTest < Minitest::Test
  LAUNCHER = RocotoActor.const_get(:Launcher)

  def setup
    @actor = LAUNCHER.spawn(ExampleActor, "reply")
  end

  def teardown
    @actor&.stop(force: true)
  end

  def test_ask_returns_a_future_and_delivers_result
    future = @actor.ask("hello")

    assert_instance_of RocotoActor::Future, future
    assert_equal "reply: hello", future.value(timeout: 1)
  end

  def test_actor_runs_in_a_fresh_ruby_process
    $rocoto_actor_parent_runtime_marker = true

    refute @actor.ask(:parent_runtime_marker).value(timeout: 1)
  ensure
    $rocoto_actor_parent_runtime_marker = nil
  end

  def test_actor_startup_errors_are_reported_to_the_parent
    error = assert_raises(RocotoActor::RemoteError) do
      LAUNCHER.spawn(ExampleActor, :fail_boot)
    end

    assert_equal "ArgumentError", error.remote_class
    assert_match(/invalid actor configuration/, error.message)
  end

  def test_spawn_rejects_an_unloadable_source
    error = assert_raises(RocotoActor::RemoteError) do
      LAUNCHER.spawn(ExampleActor, source: File.join(Dir.tmpdir, "missing-rocoto-actor.rb"))
    end

    assert_equal "LoadError", error.remote_class
  end

  def test_spawn_rejects_unsupported_constructor_arguments
    assert_raises(RocotoActor::SerializationError) do
      LAUNCHER.spawn(ExampleActor, Object.new)
    end
  end

  def test_remote_errors_are_returned_without_stopping_actor
    error = assert_raises(RocotoActor::RemoteError) do
      @actor.ask(:fail).value(timeout: 1)
    end

    assert_equal "ArgumentError", error.remote_class
    assert_equal "reply: still alive", @actor.ask("still alive").value(timeout: 1)
  end

  def test_unsupported_message_is_rejected_without_stopping_actor
    assert_raises(RocotoActor::SerializationError) { @actor.ask(Object.new) }

    assert_equal "reply: still alive", @actor.ask("still alive").value(timeout: 1)
  end

  def test_unsupported_result_is_returned_as_remote_error_without_stopping_actor
    error = assert_raises(RocotoActor::RemoteError) do
      @actor.ask(:unsupported_result).value(timeout: 1)
    end

    assert_equal "RocotoActor::SerializationError", error.remote_class
    assert_equal "reply: still alive", @actor.ask("still alive").value(timeout: 1)
  end

  def test_invalid_utf8_result_is_returned_as_serialization_error
    error = assert_raises(RocotoActor::RemoteError) do
      @actor.ask(:invalid_utf8_result).value(timeout: 1)
    end

    assert_equal "RocotoActor::SerializationError", error.remote_class
  end

  def test_oversized_result_is_returned_as_serialization_error
    error = assert_raises(RocotoActor::RemoteError) do
      @actor.ask(:oversized_result).value(timeout: 2)
    end

    assert_equal "RocotoActor::SerializationError", error.remote_class
  end

  def test_actor_standard_descriptors_are_isolated
    assert @actor.ask(:stdio_isolated).value(timeout: 1)
  end

  def test_hung_actor_does_not_hang_caller
    future = @actor.ask(:hang)

    assert_raises(RocotoActor::AskTimeoutError) { future.value(timeout: 0.05) }
    assert future.ready?
    assert_raises(RocotoActor::AskTimeoutError) { future.value(timeout: 0) }
  end

  def test_socket_backpressure_does_not_block_ask_or_stop
    @actor.ask(:hang)
    large_message = "x" * (1024 * 1024)

    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    pending = @actor.ask(large_message)
    ask_elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    @actor.stop(timeout: 0.05)

    assert_operator ask_elapsed, :<, 0.2
    assert_raises(RocotoActor::ActorStoppedError) { pending.value(timeout: 0) }
  end

  def test_full_mailbox_rejects_instead_of_blocking
    actor = LAUNCHER.spawn(ExampleActor, "reply", mailbox_size: 1)
    actor.ask(:hang)
    sleep 0.05
    actor.ask("x" * (8 * 1024 * 1024))
    sleep 0.05
    actor.ask("queued")

    assert_raises(RocotoActor::MailboxFullError) { actor.ask("overflow") }
  ensure
    actor&.stop(force: true)
  end

  def test_mailbox_enforces_byte_limit
    actor = LAUNCHER.spawn(ExampleActor, "reply", mailbox_bytes: 1_024)

    assert_raises(RocotoActor::MailboxFullError) { actor.ask("x" * 1_024) }
  ensure
    actor&.stop(force: true)
  end

  def test_stop_is_idempotent
    assert @actor.stop

    assert @actor.stop
    assert_raises(RocotoActor::ActorStoppedError) { @actor.ask("too late") }
  end

  def test_concurrent_stop_waits_for_existing_shutdown
    @actor.ask(:hang)
    first_stop = Thread.new { @actor.stop(timeout: 0.2) }
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
    until @actor.instance_variable_get(:@stopped)
      raise "stop did not start" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      Thread.pass
    end

    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    second_result = @actor.stop(timeout: 1)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at

    assert second_result
    assert_operator elapsed, :>, 0.05
    first_stop.value
  ensure
    first_stop&.join
  end

  def test_graceful_stop_drains_pending_messages
    pending = @actor.ask(:slow)

    @actor.stop(timeout: 1)

    assert_equal "reply: slow", pending.value(timeout: 0)
    assert_raises(RocotoActor::ActorStoppedError) { @actor.ask("too late") }
  end

  def test_stop_confirms_a_kill_that_lands_after_the_deadline
    pid = @actor.ask(:pid).value(timeout: 2)
    Process.kill("STOP", pid) # unresponsive to the stop message, but KILL still applies
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    stopped = @actor.stop(timeout: 0.05)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at

    assert stopped, "stop reported false although the KILL removed the group"
    assert_operator elapsed, :<, 1
    refute @actor.alive?
  end

  def test_graceful_stop_forces_actor_after_timeout
    pending = @actor.ask(:hang)

    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    @actor.stop(timeout: 0.05)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at

    assert_operator elapsed, :<, 1
    assert_raises(RocotoActor::ActorStoppedError) { pending.value(timeout: 0) }
  end

  def test_forceful_stop_immediately_rejects_pending_messages
    pending = @actor.ask(:hang)

    assert @actor.stop(force: true)

    assert_raises(RocotoActor::ActorStoppedError) { pending.value(timeout: 0) }
    refute @actor.alive?
  end

  def test_unexpected_actor_exit_rejects_pending_messages
    pending = @actor.ask(:hang)

    Process.kill("KILL", @actor.pid)

    assert_raises(RocotoActor::ActorStoppedError) { pending.value(timeout: 1) }
  end

  def test_forceful_stop_terminates_actor_descendants
    Dir.mktmpdir do |directory|
      pid_file = File.join(directory, "child.pid")
      actor = LAUNCHER.spawn(ProcessActor, pid_file)
      actor.ask(seconds: 30)
      child_pid = wait_for_pid(pid_file)

      actor.stop(force: true)

      assert process_exits?(child_pid, timeout: 2), "actor descendant #{child_pid} survived"
    ensure
      actor&.stop(force: true)
      terminate_process(child_pid)
    end
  end

  def test_worker_exit_rejects_future_and_terminates_inherited_socket_holder
    Dir.mktmpdir do |directory|
      pid_file = File.join(directory, "child.pid")
      actor = LAUNCHER.spawn(ForkThenExitActor, pid_file)
      pending = actor.ask(:go)
      child_pid = wait_for_pid(pid_file)

      assert_raises(RocotoActor::ActorStoppedError) { pending.value(timeout: 1) }
      assert process_exits?(child_pid, timeout: 2), "worker descendant #{child_pid} survived"
    ensure
      actor&.stop(force: true)
      terminate_process(child_pid)
    end
  end

  def test_parent_death_terminates_actor_background_children
    Dir.mktmpdir do |directory|
      pid_file = File.join(directory, "background.pid")
      reader, writer = IO.pipe
      owner_pid = fork do
        reader.close
        actor = LAUNCHER.spawn(BackgroundActor, pid_file)
        actor.ask(:go).value(timeout: 1)
        writer.puts(actor.pid)
        writer.close
        exit! 0
      end
      writer.close
      actor_pid = Integer(reader.gets)
      child_pid = wait_for_pid(pid_file)
      Process.wait(owner_pid)

      assert process_exits?(actor_pid, timeout: 2), "actor watchdog #{actor_pid} survived"
      assert process_exits?(child_pid, timeout: 2), "actor descendant #{child_pid} survived"
    ensure
      reader&.close
      terminate_process(actor_pid)
      terminate_process(child_pid)
    end
  end

  def test_startup_timeout_is_bounded_when_actor_ignores_term
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    assert_raises(RocotoActor::Error) do
      LAUNCHER.spawn(StubbornBootActor, start_timeout: 0.05)
    end

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    assert_operator elapsed, :<, 1
  end

  def test_actor_exits_when_parent_dies_even_if_parent_socket_was_inherited
    reader, writer = IO.pipe
    owner_pid = fork do
      reader.close
      actor = LAUNCHER.spawn(ExampleActor, "orphan")
      socket_holder_pid = fork { sleep 10 }
      writer.puts("#{actor.pid} #{socket_holder_pid}")
      writer.close
      exit! 0
    end
    writer.close
    actor_pid, socket_holder_pid = reader.gets.split.map(&:to_i)
    Process.wait(owner_pid)

    assert process_exits?(actor_pid, timeout: 2), "actor #{actor_pid} outlived parent #{owner_pid}"
  ensure
    reader&.close
    terminate_process(socket_holder_pid)
    terminate_process(actor_pid)
  end

  private

  def process_exits?(pid, timeout:)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      Process.kill(0, pid)
      return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    rescue Errno::ESRCH
      return true
    end
  end

  def wait_for_pid(path)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
    loop do
      return Integer(File.read(path)) if File.exist?(path)
      raise "child PID was not written" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    end
  end

  def terminate_process(pid)
    Process.kill("KILL", pid) if pid
  rescue Errno::ESRCH
    nil
  end
end
