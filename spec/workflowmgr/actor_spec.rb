# frozen_string_literal: true

require 'spec_helper'
require 'json'
require 'socket'
require 'workflowmgr/actor'
require_relative 'support/echo_actor_test_double'
require_relative 'support/failing_actor_test_double'
require_relative 'support/load_hooks_actor_test_double'
require_relative 'support/slow_start_actor_test_double'

# Defined here rather than in support/, since spawning it never gets as far
# as loading it in another process.
class ReservedNameActorTestDouble
  def wait(seconds = nil)
    seconds
  end
end

RSpec.describe WorkflowMgr::Actor do
  # A process that has exited but has not yet been collected still answers
  # kill(0), so reap it first if it is one of ours, and otherwise ask the
  # kernel for its state. Anything left in Z has already exited.
  def alive?(pid)
    reap_if_ours(pid)
    state = process_state(pid)
    return state != 'Z' unless state.nil?

    Process.kill(0, pid)
    true
  rescue Errno::ESRCH
    false
  end

  def reap_if_ours(pid)
    Process.waitpid(pid, Process::WNOHANG)
  rescue Errno::ECHILD, Errno::ESRCH
    nil
  end

  def process_state(pid)
    File.read("/proc/#{pid}/stat")[/\)\s+(\S)/, 1]
  rescue SystemCallError
    nil
  end

  # Waits for a process to disappear entirely, rather than merely stop
  # running. Deliberately never reaps: a handle that kills without
  # collecting leaves a zombie behind, and a helper that collected it first
  # would hide that for good.
  def wait_until_collected(pid, within:)
    deadline = Time.now + within
    sleep 0.05 while process_state(pid) && Time.now < deadline
    process_state(pid).nil?
  end

  def wait_until_dead(pid, within:)
    deadline = Time.now + within
    sleep 0.1 while alive?(pid) && Time.now < deadline
    !alive?(pid)
  end

  it 'forwards method calls to the real object running in its own process' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 're-raises the same exception class the served object raised' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect { actor.boom }.to raise_error(ArgumentError, 'kaboom')
  ensure
    actor&.stop!
  end

  it 'rejects methods not defined on the served class' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect { actor.not_a_real_method }.to raise_error(NoMethodError)
  ensure
    actor&.stop!
  end

  it 'passes along system errors the served object raises without treating the actor as failed' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect { actor.read_missing_file }.to raise_error(Errno::ENOENT)
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'reports an error whose message holds bytes JSON cannot write, as the error it was' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)

    # The message travels approximated, but the error must still arrive as
    # what happened. Losing the reply to one bad byte told the caller that
    # its reply could not be encoded -- an error about the transport, in
    # place of the missing file it had actually asked about.
    expect { actor.read_file_named_with_bad_bytes }.to raise_error(Errno::ENOENT, /rocoto-actor-spec/)
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'leaves a slow actor running on timeout, so the caller can decide to keep waiting' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    pid = actor.instance_variable_get(:@pid)

    expect { actor.nap(3) }.to raise_error(WorkflowMgr::Actor::ActorTimeout)
    expect(alive?(pid)).to be true

    # The same reply, still outstanding rather than lost: waiting longer
    # turns an unknown outcome back into a known one.
    expect(actor.wait(10)).to eq('awake')
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'can give up on one outstanding reply without giving up on the actor' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    pid = actor.instance_variable_get(:@pid)
    Process.kill('STOP', pid)

    expect { actor.greet }.to raise_error(WorkflowMgr::Actor::ActorTimeout)

    # Without this the handle refuses every later call with ActorBusy until
    # somebody calls wait, and nothing in rocoto ever does. It matters most
    # for a handle that several callers share: one of them quietly swallowing
    # a timeout would end the run for all the others, somewhere else
    # entirely and with a message about the wrong thing.
    expect(actor.discard_pending!).to be true
    expect(actor.discard_pending!).to be false

    Process.kill('CONT', pid)
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'refuses other calls while a reply is outstanding, rather than queueing behind it' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    expect { actor.nap(3) }.to raise_error(WorkflowMgr::Actor::ActorTimeout)

    started_at = Time.now
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorBusy)
    expect(Time.now - started_at).to be < 0.5
  ensure
    actor&.stop!
  end

  it 'kills an actor whose caller gives up on it' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    pid = actor.instance_variable_get(:@pid)
    socket_dir = File.dirname(actor.instance_variable_get(:@socket_path))
    expect { actor.nap(30) }.to raise_error(WorkflowMgr::Actor::ActorTimeout)

    actor.stop!

    # Collected, not merely dead: a process left as a zombie is a leak that
    # lasts as long as this one lives. Asserted with a helper that does not
    # reap, since reaping is exactly what would paper over such a leak.
    expect(wait_until_collected(pid, within: 5)).to be true
    expect(File.exist?(socket_dir)).to be false
  end

  it 'tells the caller why it could not start, instead of just vanishing' do
    actor = described_class.spawn(FailingActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)

    expect { actor.greet }.to raise_error(ArgumentError, /could not be started: cannot be constructed/)
    expect(wait_until_dead(pid, within: 3)).to be true
  ensure
    actor&.stop!
  end

  it 'reports a programming error to the caller and then exits, since its code cannot be trusted' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)

    expect { actor.bad_require }.to raise_error(LoadError)
    expect(wait_until_dead(pid, within: 3)).to be true
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorUnavailable)
  ensure
    actor&.stop!
  end

  it 'does not relaunch an actor that died, since a fresh one would lack its state' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    Process.kill('KILL', actor.instance_variable_get(:@pid))
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorUnavailable)
  ensure
    actor&.stop!
  end

  it 'says so when served code exits mid-request, rather than leaving the caller guessing' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)

    expect { actor.die }.to raise_error(RuntimeError, /called exit with status 1/)
    expect(wait_until_dead(pid, within: 3)).to be true
  ensure
    actor&.stop!
  end

  it 'keeps its socket in a private directory under /tmp and removes both when stopped' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    path = actor.instance_variable_get(:@socket_path)
    dir = File.dirname(path)

    expect(dir).to start_with('/tmp/rocoto-actor-EchoActorTestDouble-')
    expect(File.stat(dir).uid).to eq(Process.uid)
    expect(File.stat(dir).mode & 0o777).to eq(0o700)
    expect(File.stat(path).mode & 0o777).to eq(0o600)

    actor.stop!
    expect(File.exist?(path)).to be false
    expect(File.exist?(dir)).to be false
  ensure
    actor&.stop!
  end

  it 'stays responsive even if the actor is completely frozen, simulating an unkillable hang' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    pid = actor.instance_variable_get(:@pid)
    socket_dir = File.dirname(actor.instance_variable_get(:@socket_path))

    # SIGSTOP can't be trapped, blocked, or ignored -- it freezes the process
    # at the kernel level. A real D-state hang can't be conjured on demand,
    # but this produces the exact property that matters here: the actor
    # cannot respond to anything, no matter what, until SIGCONT.
    Process.kill('STOP', pid)

    started_at = Time.now
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorTimeout)

    # Bounded by the actor's own timeout, never by however long the freeze
    # lasts -- this is what proves the main process can't be hung by it.
    expect(Time.now - started_at).to be < 3

    # And nothing about the frozen actor stops a separate, healthy actor from
    # working normally -- true isolation, not just "this one call gave up".
    other_actor = described_class.spawn(EchoActorTestDouble, 'someone else', timeout: 5)
    expect(other_actor.greet('!')).to eq('hello someone else!')

    # SIGKILL works even on a stopped process, so giving up gets rid of the
    # frozen actor and its socket, without ever having waited on it.
    actor.stop!
    expect(wait_until_dead(pid, within: 5)).to be true
    expect(File.exist?(socket_dir)).to be false
  ensure
    other_actor&.stop!
    actor&.stop!
  end

  it 'exits its own process once stopped, without leaving a zombie behind' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)
    actor.stop!
    expect(alive?(pid)).to be false
  end

  it 'self-terminates if its parent disappears, even via SIGKILL with no chance to send stop!' do
    rd, wr = IO.pipe
    helper_pid = fork do
      rd.close
      helper_actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
      wr.puts(helper_actor.instance_variable_get(:@pid))
      wr.close
      sleep 30
    end
    wr.close
    actor_pid = rd.gets.to_i
    rd.close

    Process.kill('KILL', helper_pid)
    Process.wait(helper_pid)

    expect(wait_until_dead(actor_pid, within: 15)).to be true
  end

  it 'refuses requests sent after its parent died, instead of acting on them' do
    rd, wr = IO.pipe
    helper_pid = fork do
      rd.close
      helper_actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
      helper_actor.greet # the actor is up and serving before its parent dies
      wr.puts(helper_actor.instance_variable_get(:@pid))
      wr.puts(helper_actor.instance_variable_get(:@socket_path))
      wr.close
      sleep 30
    end
    wr.close
    actor_pid = rd.gets.to_i
    socket_path = rd.gets.chomp
    rd.close

    Process.kill('KILL', helper_pid)
    Process.wait(helper_pid)

    # Two things can happen here, and the test must not care which: the
    # actor refuses the request, or it has already noticed its parent is
    # gone and exited, since the watchdog polls every two seconds. Refusing
    # shows up as EOF or a reset connection; having already gone shows up as
    # a broken pipe or no socket at all. What matters, and what is asserted,
    # is that nothing ever answers.
    reply = begin
      conn = UNIXSocket.new(socket_path)
      conn.puts(JSON.generate({ 'method' => 'greet', 'args' => ['!'] }))
      conn.gets
    rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ECONNREFUSED, Errno::ENOENT
      nil
    ensure
      conn&.close
    end

    expect(reply).to be_nil
    expect(wait_until_dead(actor_pid, within: 5)).to be true
  end

  it 'cannot be orphaned while it is still loading, before it can serve anything' do
    rd, wr = IO.pipe
    helper_pid = fork do
      rd.close
      ENV['ROCOTO_SPEC_SLOW_LOAD'] = '10'
      helper_actor = described_class.spawn(LoadHooksActorTestDouble, timeout: 5)
      wr.puts(helper_actor.instance_variable_get(:@pid))
      wr.close
      sleep 30
    end
    wr.close
    actor_pid = rd.gets.to_i
    rd.close

    Process.kill('KILL', helper_pid)
    Process.wait(helper_pid)

    # Well before that 10 second load finishes: noticing that it has been
    # orphaned must not depend on getting as far as the serve loop.
    expect(wait_until_dead(actor_pid, within: 6)).to be true
  end

  it 'reports a failure that happened while loading, not only one in the constructor' do
    ENV['ROCOTO_SPEC_FAIL_LOAD'] = 'exploded while loading'
    actor = described_class.spawn(LoadHooksActorTestDouble, timeout: 5)

    expect { actor.greet }.to raise_error(RuntimeError, /could not be started: exploded while loading/)
  ensure
    ENV.delete('ROCOTO_SPEC_FAIL_LOAD')
    actor&.stop!
  end

  it 'gives up quickly on an actor stuck starting up that was never called' do
    actor = described_class.spawn(SlowStartActorTestDouble, timeout: 150)
    pid = actor.instance_variable_get(:@pid)

    started_at = Time.now
    actor.stop!

    # Bounded by the short stop timeout, not by the call timeout, even
    # though no call was ever made and there is nothing there to answer.
    expect(Time.now - started_at).to be < 8
    expect(wait_until_dead(pid, within: 5)).to be true
  end

  it 'brings back a result holding bytes that are not valid UTF-8' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)

    # Scheduler output and file contents are not always text; these are
    # carried rather than refused, and byte-for-byte at that.
    expect(actor.bad_bytes.bytes).to eq("abc\xC3\x28".dup.force_encoding('UTF-8').bytes)

    expect(alive?(pid)).to be true
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'answers with an error when a result truly cannot be carried, rather than falling silent' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)

    expect { actor.uncarryable }.to raise_error(WorkflowMgr::Actor::Codec::Unsupported, /cannot be carried/)

    # One bad result says nothing about the actor's health.
    expect(alive?(pid)).to be true
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'hands back a reply far larger than a socket buffer' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 30)
    expect(actor.big_payload(4_000_000).bytesize).to eq(4_000_000)
  ensure
    actor&.stop!
  end

  it 'carries a large non-ASCII payload without losing any of it, in either direction' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 30)
    expected = "café " * 200_000

    # Sockets deal in bytes while Ruby strings deal in characters, and a
    # payload this size cannot be written in one go, so any confusion
    # between the two silently eats part of it.
    expect(actor.big_utf8_payload(200_000)).to eq(expected)
    expect(actor.byte_count(expected)).to eq(expected.bytesize)
  ensure
    actor&.stop!
  end

  it 'reports an error whose class has no name without losing the message' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)

    expect { actor.anonymous_boom }.to raise_error(RuntimeError, 'anonymous boom')
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'refuses calls carrying a block or keyword arguments, which cannot be sent' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)

    expect { actor.greet('!') { :ignored } }.to raise_error(WorkflowMgr::Actor::ActorError, /block/)
    expect { actor.greet(loud: true) }.to raise_error(WorkflowMgr::Actor::ActorError, /keyword/)
  ensure
    actor&.stop!
  end

  it 'refuses to spawn a class whose methods an Actor handle already defines' do
    expect { described_class.spawn(ReservedNameActorTestDouble) }
      .to raise_error(ArgumentError, /defines wait/)
  end

  it 'can only be created by spawning a process for it' do
    expect { described_class.new(EchoActorTestDouble, ['world']) }.to raise_error(NoMethodError)
  end
end
