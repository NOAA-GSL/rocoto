# frozen_string_literal: true

require 'spec_helper'
require 'workflowmgr/actor'
require 'workflowmgr/utilities'
require_relative 'support/talkative_actor_test_double'
require_relative 'support/noisy_startup_actor_test_double'

RSpec.describe 'messages from inside an actor' do
  before do
    allow(WorkflowMgr).to receive(:stderr)
    allow(WorkflowMgr).to receive(:log)
  end

  it 'are written by the parent, which has the terminal and the verbosity' do
    actor = WorkflowMgr::Actor.spawn(TalkativeActorTestDouble, timeout: 10)

    expect(actor.announce('workflow is locked by pid 123', 3)).to eq('done')

    expect(WorkflowMgr).to have_received(:stderr).with('workflow is locked by pid 123', 3)
    expect(WorkflowMgr).to have_received(:log).with('logged: workflow is locked by pid 123')
  ensure
    actor&.stop!
  end

  it 'arrive even when the call goes on to fail, since they usually explain why' do
    actor = WorkflowMgr::Actor.spawn(TalkativeActorTestDouble, timeout: 10)

    expect { actor.complain_then_fail('about to fail') }
      .to raise_error(ArgumentError, 'it went wrong')

    expect(WorkflowMgr).to have_received(:stderr).with('about to fail', 1)
  ensure
    actor&.stop!
  end

  it 'are absent when the actor had nothing to say' do
    actor = WorkflowMgr::Actor.spawn(TalkativeActorTestDouble, timeout: 10)

    expect(actor.say_nothing).to eq('quiet')

    expect(WorkflowMgr).not_to have_received(:stderr)
    expect(WorkflowMgr).not_to have_received(:log)
  ensure
    actor&.stop!
  end

  it 'keep their level, so the parent can filter them as the user asked' do
    actor = WorkflowMgr::Actor.spawn(TalkativeActorTestDouble, timeout: 10)

    actor.announce('chatty detail', 9)

    expect(WorkflowMgr).to have_received(:stderr).with('chatty detail', 9)
    expect(WorkflowMgr).to have_received(:log).with('logged: chatty detail')
  ensure
    actor&.stop!
  end

  it 'never cost the caller its result, however badly encoded they are' do
    actor = WorkflowMgr::Actor.spawn(TalkativeActorTestDouble, timeout: 10)

    # Scheduler output is logged verbatim in nine places in rocoto and is
    # not always valid UTF-8, which JSON cannot write at all.
    expect(actor.log_raw_bytes({ 'jobs' => 42 })).to eq({ 'jobs' => 42 })

    expect(WorkflowMgr).to have_received(:stderr).with(/qstat said:.*and then stopped/, 1)
  ensure
    actor&.stop!
  end

  it 'keep text that is already good UTF-8 exactly as it was written' do
    actor = WorkflowMgr::Actor.spawn(TalkativeActorTestDouble, timeout: 10)

    # Approximating what JSON cannot write is one thing. A name with an
    # accent in it is perfectly writable, and replacing each of its
    # characters twice over makes the message harder to act on than
    # whatever it was reporting.
    expect(actor.log_accented_text('ok')).to eq('ok')

    expect(WorkflowMgr).to have_received(:stderr).with('submitted job for task café_naïve', 1)
  ensure
    actor&.stop!
  end

  it 'say so when there were too many to carry, rather than truncating quietly' do
    actor = WorkflowMgr::Actor.spawn(TalkativeActorTestDouble, timeout: 30)

    expect(actor.flood(WorkflowMgr::Actor::MESSAGE_LIMIT + 200)).to eq('flooded')

    expect(WorkflowMgr).to have_received(:stderr).with(/further message\(s\) from this actor were dropped/, 0)
  ensure
    actor&.stop!
  end

  it 'are never captured in this process, where they would simply vanish' do
    actor = WorkflowMgr::Actor.spawn(TalkativeActorTestDouble, timeout: 10)
    actor.announce('something worth saying', 1)

    # Only an actor may capture. A sink installed here would silently
    # discard everything rocotorun logs for the rest of its life.
    expect(WorkflowMgr.message_sink).to be_nil
  ensure
    actor&.stop!
  end

  it 'explain a failure to start, not just report that one happened' do
    actor = WorkflowMgr::Actor.spawn(NoisyStartupActorTestDouble, timeout: 10)

    expect { actor.greet }
      .to raise_error(ArgumentError, /could not be started: gave up while starting/)

    expect(WorkflowMgr).to have_received(:stderr).with('could not reach the database file', 1)
  ensure
    actor&.stop!
  end
end
