# frozen_string_literal: true

require 'spec_helper'
require 'fileutils'
require 'tmpdir'
require 'timeout'
require 'sqlite3'
require 'workflowmgr/utilities'
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowsubsetoptions'

# These specs submit real jobs to a live PBS Professional cluster (this dev
# container runs one) instead of using a fake/dryrun batch system. They are
# excluded by default; opt in with:
#   ROCOTO_RUN_PBS_SPECS=1 bundle exec rspec --tag pbs
RSpec.describe 'PBS Professional integration', :pbs do
  before do
    skip 'qsub not found on PATH; these specs require a real PBS cluster' unless system('which qsub > /dev/null 2>&1')
  end

  let(:work_dir) { Dir.mktmpdir('rocoto_pbs_spec') }

  around do |example|
    saved_home = ENV.fetch('HOME', nil)
    ENV['HOME'] = File.join(work_dir, 'home')
    FileUtils.mkdir_p(ENV['HOME'])
    example.run
    ENV['HOME'] = saved_home
  end

  after { FileUtils.remove_entry(work_dir) }

  def write_config(home_dir, batch_queue_server:, submit_threads:)
    config_dir = "#{home_dir}/.rocoto/#{WorkflowMgr.version}"
    FileUtils.mkdir_p(config_dir)
    File.write("#{config_dir}/rocotorc", <<~YAML)
      :DatabaseType: SQLite3
      :WorkflowDocType: XML
      :DatabaseServer: false
      :BatchQueueServer: #{batch_queue_server}
      :WorkflowIOServer: false
      :MaxUnknowns: 3
      :MaxLogDays: 7
      :AutoVacuum: false
      :VacuumPurgeDays: 30
      :SubmitThreads: #{submit_threads}
      :JobQueueTimeout: 45
      :JobAcctTimeout: 45
    YAML
  end

  # A minimal single-cycle workflow with N independent, dependency-free tasks,
  # so they all get submitted concurrently by the BQS thread pool.
  def write_workflow(path, log_dir, ntasks)
    tasks = (1..ntasks).map do |i|
      <<~TASK
        <task name="pbs_task_#{i}" maxtries="1">
          <command>sleep 3; exit 0</command>
          <nodes>1:ppn=1</nodes>
          <queue>workq</queue>
          <walltime>2:00</walltime>
          <jobname>pbs_task_#{i}</jobname>
        </task>
      TASK
    end.join

    File.write(path, <<~XML)
      <?xml version="1.0"?>
      <!DOCTYPE workflow []>
      <workflow realtime="f" scheduler="pbspro" cyclethrottle="1" corethrottle="10" taskthrottle="#{ntasks}">
        <log verbosity="2"><cyclestr>#{log_dir}/workflow_@Y@m@d@H@M.log</cyclestr></log>
        <cycledef group="g">202001010000 202001020000 1:00:00:00</cycledef>
      #{tasks}
      </workflow>
    XML
  end

  def run_engine(workflow_xml, db_path)
    opt = WorkflowMgr::WorkflowSubsetOptions.new(
      ['-w', workflow_xml, '-d', db_path, '-v', '0'], 'rocotorun', 'run', default_all: true
    )
    WorkflowMgr::WorkflowEngine.new(opt).run
  end

  def job_states(db_path)
    db = SQLite3::Database.new(db_path)
    rows = db.execute('SELECT taskname, state FROM jobs')
    db.close
    rows.to_h
  end

  it 'submits several tasks concurrently with BatchQueueServer=false and they all succeed' do
    ntasks = 3
    log_dir = File.join(work_dir, 'log')
    FileUtils.mkdir_p(log_dir)
    workflow_xml = File.join(work_dir, 'workflow.xml')
    db_path = File.join(work_dir, 'rocoto.db')

    write_workflow(workflow_xml, log_dir, ntasks)
    write_config(ENV.fetch('HOME'), batch_queue_server: false, submit_threads: ntasks)

    # A hang here means the thread-pool shutdown deadlock (BatchQueueServer=false,
    # multiple concurrent submissions) has regressed.
    Timeout.timeout(30) { run_engine(workflow_xml, db_path) }

    states = {}
    begin
      Timeout.timeout(240) do
        loop do
          run_engine(workflow_xml, db_path)
          states = job_states(db_path)
          break if states.size == ntasks && states.values.all? { |s| %w[SUCCEEDED FAILED DEAD].include?(s) }

          sleep 3
        end
      end
    rescue Timeout::Error
      warn "Timed out waiting for jobs to reach a terminal state. states=#{states.inspect}"
      warn `qstat -f 2>&1`
      warn `pbsnodes -a 2>&1`
      raise
    end

    expect(states.values).to all(eq('SUCCEEDED'))
  end
end
