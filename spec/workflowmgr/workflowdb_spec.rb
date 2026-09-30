# frozen_string_literal: true

require 'English'
require 'spec_helper'
require 'fileutils'
require 'tmpdir'
require 'sqlite3'
require 'rocoto_actor'
require 'workflowmgr/database_actor'
require 'workflowmgr/workflowdb'

RSpec.describe WorkflowMgr::WorkflowSQLite3DB do
  describe 'workflow locking' do
    let(:dir) { Dir.mktmpdir('rocoto-lock-spec-') }
    let(:databasefile) { File.join(dir, 'workflow.db') }
    let(:lockfile) { File.join(dir, 'workflow_lock.db') }

    # Created before anything forks. These are lazily memoized, so a child
    # that mentioned them first would make its own temporary directory and
    # lock a database the parent never sees.
    before do
      dir
    end

    after do
      FileUtils.remove_entry(dir, true)
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

    it 'refuses the lock while another live process holds it, and grants it once released' do
      told_locked, report_locked = IO.pipe
      may_release, release_now = IO.pipe

      holder = fork do
        told_locked.close
        release_now.close
        held = described_class.new(databasefile)
        held.dbopen
        report_locked.puts(held.lock_workflow ? 'LOCKED' : 'REFUSED')
        report_locked.close
        may_release.gets # hold the workflow until told to let go
        held.unlock_workflow
        exit!(0)
      end
      report_locked.close
      may_release.close

      expect(told_locked.gets.chomp).to eq('LOCKED')

      ours = described_class.new(databasefile)
      ours.dbopen
      expect(ours.lock_workflow).to be false

      release_now.puts('go')
      release_now.close
      Process.wait(holder)

      expect(ours.lock_workflow).to be true
      ours.unlock_workflow
    ensure
      told_locked&.close
      release_now&.close
    end

    it 'reports whether there was a lock of ours to release' do
      database = described_class.new(databasefile)
      database.dbopen
      database.lock_workflow

      expect(database.unlock_workflow).to be true

      # Nothing left to give back means someone judged our lock stale and
      # took it, so for a while two runs may have been advancing the same
      # workflow. The caller has to be able to tell that from a clean
      # release, rather than reporting the run as a success.
      expect(database.unlock_workflow).to be false
    end

    it 'says why, at ordinary verbosity, when there was no lock of ours to release' do
      # The run ends non-zero because of this. Reported above the default
      # verbosity it would reach nobody, leaving a cron user with a failure
      # and an empty message.
      allow(WorkflowMgr).to receive(:stderr)
      allow(WorkflowMgr).to receive(:log)

      database = described_class.new(databasefile)
      database.dbopen
      database.lock_workflow
      database.unlock_workflow

      expect(database.unlock_workflow).to be false
      expect(WorkflowMgr).to have_received(:stderr).with(/no workflow lock to release/, 1)
    end

    it 'does not retry taking the lock, since a retry reads our own row as someone else\'s' do
      # Every other database call is retried when SQLite reports the file
      # busy. Taking the lock must not be: by the time it can fail, its row
      # may already be committed, and a second attempt would read that row
      # as another process's lock and report failure -- leaving the caller
      # to abandon a workflow it actually holds, without unlocking it.
      database = described_class.new(databasefile)
      database.dbopen

      calls = 0
      allow(database).to receive(:open_workflow_db).and_wrap_original do |original|
        calls += 1
        raise WorkflowMgr::WorkflowDBLockedException, 'database is locked' if calls == 1

        original.call
      end

      expect { database.lock_workflow }.to raise_error(WorkflowMgr::WorkflowDBLockedException)
      expect(calls).to eq(1)
    end

    it 'retries the calls that are safe to repeat' do
      database = described_class.new(databasefile)
      database.dbopen
      database.lock_workflow

      # The failure is injected into the SQLite handle rather than into
      # load_cycles itself: stubbing the method would put the stub in front
      # of the prepended retry module, so no retry could ever run and the
      # example would pass whether or not one exists.
      handle = database.instance_variable_get(:@database)
      calls = 0
      allow(handle).to receive(:execute).and_wrap_original do |original, *args|
        calls += 1
        raise SQLite3::BusyException, 'database is locked' if calls == 1

        original.call(*args)
      end

      expect(database.load_cycles).to eq([])
      expect(calls).to eq(2)

      database.unlock_workflow
    end

    it 'can be served from a process that loaded nothing but this file' do
      # Run somewhere clean on purpose. Every other example here has already
      # loaded the actor code for its own reasons, which would hide a
      # missing require in the file under test -- exactly the kind of break
      # that only shows up the first time rocotorun is run for real.
      script = <<~RUBY
        $LOAD_PATH.unshift(#{File.expand_path('../../lib', __dir__).inspect})
        require "rocoto_actor"
        require "workflowmgr/workflowdb"

        config = Struct.new(:DatabaseType, :DatabaseServer).new("SQLite3", true)
        options = Struct.new(:database).new(#{databasefile.inspect})

        broker = RocotoActor::ActorBroker.new
        database = WorkflowMgr.workflow_database(config, options, broker)
        database.dbopen
        database.stop!
        broker.stop
        puts "SERVED"
      RUBY

      script_file = File.join(dir, 'fresh_load.rb')
      File.write(script_file, script)

      expect(`#{RbConfig.ruby} #{script_file} 2>&1`).to include('SERVED')
    end

    # The database served from its own process, the way rocoto runs it.
    # Rocoto's configuration exposes these exact names, so the struct has to
    # use them whatever Ruby style guides say about method names.
    def actor_backed_database(broker)
      config = Struct.new(:DatabaseType, :DatabaseServer) # rubocop:disable Naming/MethodName
                     .new('SQLite3', true)
      options = Struct.new(:database).new(databasefile)
      WorkflowMgr.workflow_database(config, options, broker)
    end

    it 'records the pid of the rocoto process, not of the actor that writes it' do
      broker = RocotoActor::ActorBroker.new
      database = actor_backed_database(broker)
      database.dbopen

      expect(database.lock_workflow).to be true
      expect(lock_owner).to eq(Process.pid)

      # The lock belongs to the run. Recording the actor's pid instead would
      # make the lock look abandoned the moment the actor was replaced.
      actor_pid = broker.describe[:actors].first[:pid]
      expect(actor_pid).not_to be_nil
      expect(lock_owner).not_to eq(actor_pid)

      database.unlock_workflow
    ensure
      broker&.stop
    end

    it 'keeps the lock when its actor dies but the rocoto process holding it is still alive' do
      told_actor_pid, report_actor_pid = IO.pipe
      may_finish, finish_now = IO.pipe

      owner = fork do
        told_actor_pid.close
        finish_now.close
        broker = RocotoActor::ActorBroker.new
        database = actor_backed_database(broker)
        database.dbopen
        database.lock_workflow
        report_actor_pid.puts(broker.describe[:actors].first[:pid])
        report_actor_pid.close
        may_finish.gets # stay alive, still owning the workflow
        exit!(0)
      end
      report_actor_pid.close
      may_finish.close

      # The actor that took the lock is gone; the run that owns it is not.
      Process.kill('KILL', told_actor_pid.gets.to_i)

      other = described_class.new(databasefile)
      other.dbopen
      expect(other.lock_workflow).to be false

      finish_now.puts('done')
      finish_now.close
      Process.wait(owner)

      # Now that the owner is gone too, the lock really is abandoned.
      expect(other.lock_workflow).to be true
      other.unlock_workflow
    ensure
      told_actor_pid&.close
      finish_now&.close
    end
  end
end
