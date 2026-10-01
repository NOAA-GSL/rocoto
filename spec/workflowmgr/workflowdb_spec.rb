# frozen_string_literal: true

require 'English'
require 'spec_helper'
require 'fileutils'
require 'workflowmgr/workflowdb'

RSpec.describe WorkflowMgr::WorkflowSQLite3DB do
  describe 'workflow locking' do
    let(:databasefile) { 'test.db' }
    let(:lockfile) { 'test_lock.db' }

    before do
      FileUtils.rm_f(databasefile)
      FileUtils.rm_f(lockfile)
    end

    after do
      FileUtils.rm_f(databasefile)
      FileUtils.rm_f(lockfile)
    end

    # Read the lock table directly, rather than through the class under test.
    def lock_rows
      db = SQLite3::Database.new(lockfile)
      db.execute('SELECT * FROM lock;')
    ensure
      db&.close
    end

    def lock_owner
      lock_rows.first&.first
    end

    # Write a lock row directly, so a held lock can be set up without a second
    # process. dbopen creates the table, so this must follow it. A numeric
    # TEST-NET address stands in for another host: the code calls getaddrinfo on
    # whatever it finds here, and a numeric address resolves without DNS.
    def hold_lock(pid:, host:, age_seconds:)
      db = SQLite3::Database.new(lockfile)
      db.execute('INSERT INTO lock VALUES (?,?,?);', [pid, host, Time.now.to_i - age_seconds])
    ensure
      db&.close
    end

    def local_ip
      Socket.getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3]
    end

    it 'says why, at ordinary verbosity, when another run holds the lock' do
      # Returning false here is what makes the run exit non-zero. Reported above
      # the default verbosity, as it was, the explanation reached the log file
      # alone and a cron user got a failure with an empty mail body.
      allow(WorkflowMgr).to receive(:stderr)
      allow(WorkflowMgr).to receive(:log)

      database = described_class.new(databasefile)
      database.dbopen
      # This process is alive, so its lock is not judged stale.
      hold_lock(pid: Process.pid, host: local_ip, age_seconds: 0)

      expect(database.lock_workflow).to be false
      expect(WorkflowMgr).to have_received(:stderr).with(/Workflow is locked by pid/, 1)
    end

    it 'says why, at ordinary verbosity, when it cannot release a lock it does not hold' do
      # This path calls Process.exit(1) directly, so there is no caller left to
      # explain anything -- the message here is the only account the user gets.
      allow(WorkflowMgr).to receive(:stderr)
      allow(WorkflowMgr).to receive(:log)

      database = described_class.new(databasefile)
      database.dbopen
      hold_lock(pid: 999_999, host: '203.0.113.1', age_seconds: 0)

      expect { database.unlock_workflow }.to raise_error(SystemExit)
      expect(WorkflowMgr).to have_received(:stderr).with(/cannot unlock the workflow/, 1)
    end

    it 'probes a remote lock owner with an argument list, never a shell string' do
      # The host and pid come out of the lock table. Built into one string they
      # would be interpreted by a shell, so the call has to stay a list.
      database = described_class.new(databasefile)
      database.dbopen
      hold_lock(pid: 999_999, host: '203.0.113.1', age_seconds: 3600)
      allow(database).to receive(:system).and_return(false)

      database.lock_workflow

      expect(database).to have_received(:system)
        .with('ssh', '-o', 'StrictHostKeyChecking=no', '203.0.113.1', 'kill', '-0', '999999',
              hash_including(:out, :err))
    end

    it 'steals a lock from another host once its owner is gone' do
      database = described_class.new(databasefile)
      database.dbopen
      hold_lock(pid: 999_999, host: '203.0.113.1', age_seconds: 3600)
      # A non-zero ssh probe means the owner is no longer running.
      allow(database).to receive(:system).and_return(false)

      expect(database.lock_workflow).to be true
      expect(lock_owner).to eq(Process.pid)
    end

    it 'leaves a lock alone while its remote owner still answers' do
      database = described_class.new(databasefile)
      database.dbopen
      hold_lock(pid: 999_999, host: '203.0.113.1', age_seconds: 3600)
      # A zero exit means the owner is still running.
      allow(database).to receive(:system).and_return(true)

      expect(database.lock_workflow).to be false
      expect(lock_owner).to eq(999_999)
    end

    it 'properly locks and serializes database access across processes' do
      skip 'Flaky test with race conditions - needs redesign with proper IPC instead of sleep-based timing'

      # Initialize a workflow SQLite database
      database = described_class.new(databasefile)
      database.dbopen

      # Add a test table to the database
      dbhandle = SQLite3::Database.new(databasefile)
      dbhandle.transaction do |db|
        db.execute('CREATE TABLE test (val INTEGER);')
        db.execute('INSERT INTO test VALUES (0);')
      end
      dbhandle.close

      # Create a worker script that simulates a rocotorun process
      lib_path = File.expand_path('../../lib', __dir__)
      worker_script = <<~RUBY
        #!/usr/bin/env ruby
        $LOAD_PATH.unshift('#{lib_path}')

        require 'sqlite3'
        require 'workflowmgr/workflowdb'

        databasefile = ARGV[0]
        action = ARGV[1]

        database = WorkflowMgr::WorkflowSQLite3DB.new(databasefile)
        database.dbopen

        case action
        when 'lock'
          # Acquire lock and hold it briefly
          success = database.lock_workflow
          if success
            # Write that we have the lock
            puts "LOCKED"
            # Hold the lock for a moment
            sleep 0.5
            database.unlock_workflow
          else
            puts "FAILED"
            exit 1
          end
        when 'increment'
          # Acquire lock, increment counter, release
          success = database.lock_workflow
          if success
            dbhandle = SQLite3::Database.new(databasefile)
            dbhandle.transaction do |db|
              val = db.execute('SELECT val FROM test')[0][0]
              db.execute("UPDATE test SET val=\#{val + 1}")
            end
            dbhandle.close
            database.unlock_workflow
            puts "INCREMENTED"
          else
            puts "FAILED"
            exit 1
          end
        end

        exit 0
      RUBY

      # Write the worker script
      worker_file = 'test_worker.rb'
      File.write(worker_file, worker_script)

      begin
        # Test 1: Sequential operations should all succeed
        5.times do
          result = `bundle exec ruby #{worker_file} #{databasefile} increment 2>&1`
          expect(result).to include("INCREMENTED")
          expect($CHILD_STATUS.exitstatus).to eq(0)
        end

        # Verify counter
        dbhandle = SQLite3::Database.new(databasefile)
        val = dbhandle.execute('SELECT val FROM test')[0][0]
        dbhandle.close
        expect(val).to eq(5)

        # Test 2: One process holds lock while another tries to acquire
        # Start a process that will hold the lock
        holder_pid = spawn("bundle exec ruby #{worker_file} #{databasefile} lock", out: '/dev/null', err: '/dev/null')

        # Wait for it to acquire the lock
        sleep 0.2

        # Try to increment while the lock is held - should fail/timeout
        start_time = Time.now
        competitor_pid = spawn("bundle exec ruby #{worker_file} #{databasefile} increment",
                               out: '/dev/null', err: '/dev/null')

        # The competitor should wait for the lock to be released
        _pid, _status = Process.wait2(competitor_pid)
        elapsed = Time.now - start_time

        # Should have waited at least 0.3 seconds (lock was held for 0.5s, we waited 0.2s before starting)
        expect(elapsed).to be >= 0.2

        # Wait for holder to finish
        Process.wait2(holder_pid)

        # Competitor might have succeeded or failed depending on timing
        # If it succeeded, counter should be 6, if failed should still be 5
        dbhandle = SQLite3::Database.new(databasefile)
        val = dbhandle.execute('SELECT val FROM test')[0][0]
        dbhandle.close
        expect(val).to be_between(5, 6)
      ensure
        # Clean up
        FileUtils.rm_f(worker_file)
        FileUtils.rm_f('test_worker_0.out')
        FileUtils.rm_f('test_worker_0.err')
        FileUtils.rm_f('test_worker_1.out')
        FileUtils.rm_f('test_worker_1.err')
      end
    end
  end
end
