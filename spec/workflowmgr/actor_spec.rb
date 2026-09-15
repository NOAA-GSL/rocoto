# frozen_string_literal: true

require 'spec_helper'
require 'workflowmgr/actor'
require_relative 'support/echo_actor_test_double'

RSpec.describe WorkflowMgr::Actor do
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

  it 'gives up with ActorTimeout if the actor does not respond in time' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    expect { actor.nap(5) }.to raise_error(WorkflowMgr::Actor::ActorTimeout)
    sleep 4.5 # let the still in-flight nap finish before stop! below
  ensure
    actor&.stop!
  end

  it 'stays responsive even if the actor is completely frozen, simulating an unkillable hang' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    pid = actor.instance_variable_get(:@pid)

    # SIGSTOP can't be trapped, blocked, or ignored -- it freezes the process
    # at the kernel level. A real D-state hang can't be conjured on demand,
    # but this produces the exact property that matters here: the actor
    # cannot respond to anything, no matter what, until SIGCONT.
    Process.kill('STOP', pid)

    started_at = Time.now
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorTimeout)
    elapsed = Time.now - started_at

    # Bounded by the actor's own timeout, never by however long the freeze
    # lasts -- this is what proves the main process can't be hung by it.
    expect(elapsed).to be < 3

    # And nothing about the frozen actor stops a separate, healthy actor from
    # working normally -- true isolation, not just "this one call gave up".
    other_actor = described_class.spawn(EchoActorTestDouble, 'someone else', timeout: 5)
    expect(other_actor.greet('!')).to eq('hello someone else!')
  ensure
    other_actor&.stop!
    # stop! can't be used on the frozen actor -- it would just time out the
    # same way, leaving it frozen forever. It must be killed directly instead.
    if pid
      begin
        Process.kill('CONT', pid)
      rescue Errno::ESRCH, Errno::EINVAL
        nil
      end
      begin
        Process.kill('KILL', pid)
      rescue Errno::ESRCH
        nil
      end
      begin
        Process.waitpid(pid)
      rescue Errno::ECHILD
        nil
      end
    end
  end

  it 'exits its own process once stopped, without leaving a zombie behind' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)
    actor.stop!
    expect { Process.kill(0, pid) }.to raise_error(Errno::ESRCH)
  end

  it 'self-terminates if its parent disappears, even via SIGKILL with no chance to send stop!' do
    rd, wr = IO.pipe
    helper_pid = fork do
      rd.close
      require 'workflowmgr/actor'
      require_relative 'support/echo_actor_test_double'
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

    deadline = Time.now + 15
    alive = true
    while alive && Time.now < deadline
      sleep 0.5
      alive = begin
        Process.kill(0, actor_pid)
        true
      rescue Errno::ESRCH
        false
      end
    end
    expect(alive).to be false
  end
end
