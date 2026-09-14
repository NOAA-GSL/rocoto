# frozen_string_literal: true

require 'spec_helper'
require 'fileutils'
require 'tmpdir'
require 'timeout'
require 'sqlite3'
require 'workflowmgr/utilities'
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowsubsetoptions'

# These specs submit real jobs to a live Slurm cluster instead of using a
# fake/dryrun batch system. They are excluded by default; opt in with:
#   ROCOTO_RUN_SLURM_SPECS=1 bundle exec rspec --tag slurm
#
# Partition/account are auto-detected (like test/run_smoke.sh) but can be
# overridden with ROCOTO_TEST_PARTITION / ROCOTO_TEST_ACCOUNT if detection
# picks the wrong one for your cluster.
RSpec.describe 'Slurm integration', :slurm do
  before do
    unless system('which sbatch > /dev/null 2>&1')
      skip 'sbatch not found on PATH; these specs require a real Slurm cluster'
    end
  end

  let(:work_dir) { Dir.mktmpdir('rocoto_slurm_spec') }

  around do |example|
    saved_home = ENV.fetch('HOME', nil)
    ENV['HOME'] = File.join(work_dir, 'home')
    FileUtils.mkdir_p(ENV['HOME'])
    example.run
    ENV['HOME'] = saved_home
  end

  after { FileUtils.remove_entry(work_dir) }

  def detect_partition
    return ENV['ROCOTO_TEST_PARTITION'] if ENV['ROCOTO_TEST_PARTITION']

    if system('which scontrol > /dev/null 2>&1')
      detected = `scontrol show partition 2>/dev/null`[/PartitionName=(\S+)/, 1]
    end

    # Falls back to the partition name used by docker/docker-compose.yml's Slurm cluster
    detected || 'slurmpar'
  end

  def detect_account
    return ENV['ROCOTO_TEST_ACCOUNT'] if ENV['ROCOTO_TEST_ACCOUNT']
    return nil unless system('which sacctmgr > /dev/null 2>&1')

    line = `sacctmgr -nP show account format=account 2>/dev/null`.lines.first
    line&.split('|')&.first&.strip
  end

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
  def write_workflow(path, log_dir, ntasks, partition:, account:)
    tasks = (1..ntasks).map do |i|
      <<~TASK
        <task name="slurm_task_#{i}" maxtries="1">
          <command>sleep 3; exit 0</command>
          <cores>1</cores>
          #{"<partition>#{partition}</partition>" if partition}
          #{"<account>#{account}</account>" if account}
          <walltime>2:00</walltime>
          <jobname>slurm_task_#{i}</jobname>
        </task>
      TASK
    end.join

    File.write(path, <<~XML)
      <?xml version="1.0"?>
      <!DOCTYPE workflow []>
      <workflow realtime="f" scheduler="slurm" cyclethrottle="1" corethrottle="10" taskthrottle="#{ntasks}">
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

    write_workflow(workflow_xml, log_dir, ntasks, partition: detect_partition, account: detect_account)
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
      warn `squeue 2>&1`
      warn `sinfo 2>&1`
      raise
    end

    expect(states.values).to all(eq('SUCCEEDED'))
  end
end
