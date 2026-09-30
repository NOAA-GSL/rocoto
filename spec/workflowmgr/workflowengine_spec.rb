# frozen_string_literal: true

require 'spec_helper'
require 'workflowmgr/workflowengine'

# Records what the sweep asks of the database. A verifying double is no use
# here: the real database proxy serves delete_jobs through method_missing,
# which instance_double cannot see.
class RecordingDatabase
  attr_reader :deleted

  def initialize
    @deleted = []
  end

  def delete_jobs(jobs)
    @deleted.concat(jobs)
  end
end

class RecordingLog
  attr_reader :messages

  def initialize
    @messages = []
  end

  def log(cycle, message)
    @messages << [cycle, message]
  end
end

# The sweep that decides what to do about a submission nobody can account
# for. It deletes job rows, so it is worth pinning on its own: the engine is
# never constructed here, only the three things the sweep touches are set.
RSpec.describe WorkflowMgr::WorkflowEngine do
  let(:cycle) { Time.at(1_700_000_000).getgm }
  let(:engine) { described_class.allocate }
  let(:db_server) { RecordingDatabase.new }
  let(:log_server) { RecordingLog.new }

  def job(state, id, at = cycle)
    WorkflowMgr::Job.new(id, 'forecast', at, 1, state, state.downcase, 0, 0, 0, 0.0)
  end

  def sweep(jobs)
    active = {}
    jobs.each { |one| (active[one.task] ||= {})[one.cycle] = one }
    engine.instance_variable_set(:@active_jobs, active)
    engine.instance_variable_set(:@db_server, db_server)
    engine.instance_variable_set(:@log_server, log_server)
    engine.send(:harvest_pending_jobids)
    active
  end

  before do
    allow(WorkflowMgr).to receive(:dryrun_mode?).and_return(false)
    allow(WorkflowMgr).to receive(:stderr)
  end

  it 'drops a submission that no run ever accounted for, so it can be tried again' do
    orphan = job('SUBMITTING', 'b2f1c0de-0000-4000-8000-000000000001')

    remaining = sweep([orphan])

    # The row is the only evidence the submission was started. Nothing can
    # say now whether the scheduler took it, so rocoto does what it has
    # always done when it could not find out: drops it and tries again.
    expect(db_server.deleted).to eq([orphan])
    expect(remaining).to be_empty
  end

  it 'says out loud that the job may in fact be queued' do
    sweep([job('SUBMITTING', 'b2f1c0de-0000-4000-8000-000000000002')])

    # A user reading the log needs to know this can double-submit, because
    # until the scheduler can be searched for the marker, it can.
    expect(log_server.messages.map(&:last)).to include(/probably, but not necessarily, failed/)
  end

  it 'leaves alone every job whose submission was confirmed' do
    # Deliberately different cycles: active jobs are keyed by task and then
    # by cycle, so two jobs sharing both would be one entry rather than two.
    queued = job('QUEUED', '12345')
    running = job('RUNNING', '12346', cycle + 3600)

    remaining = sweep([queued, running])

    expect(db_server.deleted).to be_empty
    expect(remaining['forecast'].values).to contain_exactly(queued, running)
  end

  it 'does nothing at all in a dryrun, where no submission was ever made' do
    allow(WorkflowMgr).to receive(:dryrun_mode?).and_return(true)

    sweep([job('SUBMITTING', 'b2f1c0de-0000-4000-8000-000000000003')])

    expect(db_server.deleted).to be_empty
  end
end
