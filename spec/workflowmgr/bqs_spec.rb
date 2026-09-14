# frozen_string_literal: true

require 'spec_helper'
require 'workflowmgr/bqs'
require 'timeout'

RSpec.describe WorkflowMgr::BQS do
  let(:batch_system) do
    Class.new do
      def submit(_task)
        sleep 0.05
        ['12345', 'submitted']
      end
    end.new
  end

  let(:config) { double('config', SubmitThreads: 4) }

  def fake_task(name)
    double('task', attributes: { name: name })
  end

  subject(:bqs) { described_class.new(batch_system, 'fake.db', config) }

  # Regression test for a deadlock where BatchQueueServer=false submissions
  # were joined via Thread.list, which blocks forever on the pool's idle
  # worker threads. shutdown must return promptly instead of hanging.
  it 'shuts down the pool without hanging, leaving it nil for reuse' do
    3.times { |i| bqs.submit(fake_task("task#{i}"), 0) }

    Timeout.timeout(5) { bqs.shutdown }

    expect(bqs.instance_variable_get(:@pool)).to be_nil
  end

  it 'allows submit to spawn a fresh pool after shutdown' do
    bqs.submit(fake_task('task1'), 0)
    Timeout.timeout(5) { bqs.shutdown }

    expect { bqs.submit(fake_task('task2'), 0) }.not_to raise_error

    Timeout.timeout(5) { bqs.shutdown }
  end

  it 'harvests submit status for every submitted task after shutdown' do
    bqs.submit(fake_task('task1'), 0)

    Timeout.timeout(5) { bqs.shutdown }

    jobid, output = bqs.get_submit_status('task1', 0)
    expect(jobid).to eq('12345')
    expect(output).to eq('submitted')
  end
end
