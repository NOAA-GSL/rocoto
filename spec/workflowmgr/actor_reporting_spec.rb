# frozen_string_literal: true

require 'spec_helper'
require 'workflowmgr/utilities'

# The two callbacks rocoto hands to an ActorBroker. They are plain module
# methods so that they can be checked without starting a broker at all.
RSpec.describe 'reporting from the actor system' do
  before do
    allow(WorkflowMgr).to receive(:stderr)
    allow(WorkflowMgr).to receive(:log)
  end

  it 'reports a failure on a broker thread to the terminal and the log' do
    WorkflowMgr.report_actor_error(ArgumentError.new('no such node'), 'relaunch of database')

    expect(WorkflowMgr).to have_received(:stderr).with(/relaunch of database: ArgumentError: no such node/, 1)
    expect(WorkflowMgr).to have_received(:log).with(/relaunch of database/)
  end

  it 'tells the user when an actor dies, and keeps an orderly stop to the log' do
    handle = double(path: 'database')

    WorkflowMgr.report_actor_event(:failed, handle, { reason: 'killed by signal 9 (KILL)', generation: 2 })
    WorkflowMgr.report_actor_event(:stopped, handle, {})

    expect(WorkflowMgr).to have_received(:stderr)
      .with('rocoto actor database failed (generation 2): killed by signal 9 (KILL)', 1)
    expect(WorkflowMgr).to have_received(:stderr).with('rocoto actor database stopped', 3)
  end

  it 'names an actor it cannot ask by its id rather than failing to report it' do
    unreachable = double(id: 'abc123')
    allow(unreachable).to receive(:path).and_raise(RuntimeError, 'unknown actor handle')

    WorkflowMgr.report_actor_event(:failed, unreachable, {})

    expect(WorkflowMgr).to have_received(:stderr).with('rocoto actor abc123 failed', 1)
  end

  it 'never lets its own failure escape onto a broker thread' do
    # The broker calls these from its service and event threads. An exception
    # here would be raised somewhere no caller can see it.
    allow(WorkflowMgr).to receive(:stderr).and_raise(IOError, 'stderr is gone')

    expect { WorkflowMgr.report_actor_error(ArgumentError.new('boom'), 'a job') }.not_to raise_error
    expect { WorkflowMgr.report_actor_event(:failed, double(path: 'io'), {}) }.not_to raise_error
  end
end
