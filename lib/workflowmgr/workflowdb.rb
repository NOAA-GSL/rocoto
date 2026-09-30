##########################################
#
# module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowLockedException
  #
  ##########################################
  class WorkflowLockedException < RuntimeError
  end

  ##########################################
  #
  # Class WorkflowDBLockedException
  #
  ##########################################
  class WorkflowDBLockedException < RuntimeError
  end

  ##########################################
  #
  # Class WorkflowDBAccessException
  #
  ##########################################
  class WorkflowDBAccessException < RuntimeError
  end

  ##########################################
  #
  # Class WorkflowSQLite3DB
  #
  ##########################################

  # How long to wait on the database when it runs in its own process. The
  # same patience the DRb-based proxy allowed before it was replaced.
  DATABASE_TIMEOUT = 150

  ##########################################
  #
  # WorkflowMgr.workflow_database
  #
  # The workflow database: in a process of its own when isolation is wanted,
  # in this one when it is not. Both answer the same calls, so nothing above
  # this needs to know which it was handed.
  #
  # An actor is told this process's pid, because the workflow lock belongs
  # to rocoto and has to outlive any actor that happens to write it.
  #
  ##########################################
  def self.workflow_database(config, options, broker = nil)
    database_class = const_get("Workflow#{config.DatabaseType}DB")

    # DatabaseActor serves a WorkflowSQLite3DB by name. That is not a
    # narrowing: SQLite3 is the only database rocoto has, and const_get above
    # would already fail on any other DatabaseType.
    if config.DatabaseServer && !dryrun_mode? && !broker.nil?
      require 'workflowmgr/database_actor'
      handle = broker.spawn(DatabaseActor, options.database, Process.pid, config.StaleLockTimeout,
                            name: 'database', source: File.expand_path('database_actor.rb', __dir__))
      DatabaseProxy.new(handle)
    else
      # Process.pid is stated rather than left to the default, so that the
      # timeout after it can be passed at all. It is the same value the
      # default supplied.
      database_class.new(options.database, Process.pid, config.StaleLockTimeout)
    end
  end

  class WorkflowSQLite3DB
    require "sqlite3"
    require "socket"
    require "workflowmgr/cycle"
    require "workflowmgr/job"
    require "workflowmgr/workflowoption"
    require "workflowmgr/workflowconfig"
    require "workflowmgr/utilities"

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(database_file, owner_pid = Process.pid, stale_lock_timeout = 300)
      # Set name of database file
      @database_file = database_file

      # How old another host's lock must be before we probe its owner. The
      # default matches the value this was hardcoded to before it became
      # configurable, so a caller that does not pass one is unchanged.
      @stale_lock_timeout = stale_lock_timeout

      # Whose lock this is. Not necessarily the process running this code:
      # when the database runs as an actor, the workflow belongs to the
      # rocoto process that spawned it, and outlives the actor itself.
      @owner_pid = owner_pid

      # Calculate the name of the corresponding workflow lock database
      @database_lock_file = if @database_file =~ /^(\S+)(\.[^.]+)$/
                              "#{::Regexp.last_match(1)}_lock#{::Regexp.last_match(2)}"
                            else
                              "#{@database_file}_lock"
                            end

      # Initialize access mode
      @mode = { readonly: false }
    end

    ##########################################
    #
    # dbopen
    #
    ##########################################
    def dbopen(mode = { readonly: false })
      # Set the database access mode
      @mode = mode

      # Verify permissions
      verify_permissions

      if mode[:readonly]
        # Open the workflow database
        open_workflow_db
      else
        # Open the lock database
        open_lock_db
      end
    end

    ##########################################
    #
    # lock_workflow
    #
    ##########################################
    def lock_workflow
      tries = 0

      begin
        # Make sure write access is enabled
        verify_write_access

        # Start a transaction and immediately acquire an exclusive lock
        @database_lock.transaction(:exclusive) do |db|
          # Access the workflow lock maintained by the workflow manager
          lock = db.execute("SELECT * FROM lock;")

          # If no lock is present, we have acquired the lock.  Write the WFM's pid and host into the lock table
          if lock.empty?
            db.execute("INSERT INTO lock VALUES (?,?,?);",
                       [@owner_pid,
                        Socket.getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3],
                        Time.now.to_i])

          # Otherwise, we didn't get the lock, but we need to check to make sure the lock is not stale
          else

            stale = false

            # If we are on the same host as the owner of the lock, run a simple local check to
            # see if the owner is still running
            if Socket.getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3] == lock[0][1]
              begin
                Process.getpgid(lock[0][0])
              rescue Errno::ESRCH
                stale = true
              end

            # Otherwise probe the owner over ssh, but only once the lock is old
            # enough to be worth suspecting. StaleLockTimeout in rocotorc.
            elsif Time.now - Time.at(lock[0][2]) > @stale_lock_timeout
              begin
                WorkflowMgr.timeout(10) do
                  # An argument list, not a command string: the host and the pid
                  # are read back from the lock table, and a shell would treat
                  # metacharacters in either as code. This is also the reason
                  # the redirections moved into keyword form -- the string
                  # version said `2>&1 > /dev/null`, which sent stderr to the
                  # inherited stdout and only then silenced stdout.
                  ok = system('ssh', '-o', 'StrictHostKeyChecking=no',
                              lock[0][1].to_s, 'kill', '-0', lock[0][0].to_s,
                              out: File::NULL, err: File::NULL)
                  # nil means ssh itself could not be run; treat that the same
                  # as a failed probe, exactly as the old exitstatus check did.
                  stale = !ok
                end
              rescue Timeout::Error
                stale = true
              end
            end

            # If the lock is stale, steal the lock
            localhostinfo = Socket.getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0]
            lockhostinfo = Socket.getaddrinfo(lock[0][1], nil)[0]
            if stale
              db.execute("DELETE FROM lock;")
              db.execute("INSERT INTO lock VALUES (?,?,?);",
                         [@owner_pid, localhostinfo[3], Time.now.to_i])
              msg = "WARNING: Rocoto pid #{@owner_pid} on host #{localhostinfo[2]} (#{localhostinfo[3]}) " \
                    "stole stale lock from Rocoto pid #{lock[0][0]} on host " \
                    "#{lockhostinfo[2]} (#{lockhostinfo[3]})."
              WorkflowMgr.stderr(msg, 3)
              WorkflowMgr.log(msg)
            else
              msg = "WARNING: Workflow is locked by pid #{lock[0][0]} on host " \
                    "#{lockhostinfo[2]} (#{lockhostinfo[3]}) since #{Time.at(lock[0][2])}."
              raise WorkflowMgr::WorkflowLockedException, msg
            end
          end
        end

        # If an exception wasn't thrown, we got the lock
        # So, open the workflow database and, if that
        # doesn't throw an exception, return true
        open_workflow_db
        true
      rescue WorkflowMgr::WorkflowLockedException
        # Level 1, not 3: returning false here is what makes the run exit
        # non-zero, and default verbosity is 1. At level 3 this explanation
        # reached the log file and nothing else, so a cron user got a failed
        # run and an empty mail body with no way to tell why.
        WorkflowMgr.stderr($ERROR_INFO.to_s, 1)
        WorkflowMgr.log($ERROR_INFO.to_s)
        false
      rescue SQLite3::BusyException
        if tries < 3
          tries += 1
          sleep rand
          retry
        else
          msg = "WARNING: WorkflowSQLite3DB.lock_workflow: Could not open workflow database " \
                "file '#{@database_lock_file}' because it is locked by SQLite."
          raise WorkflowMgr::WorkflowDBLockedException, msg
        end
      end
    end

    ##########################################
    #
    # unlock_workflow
    #
    ##########################################
    def unlock_workflow
      tries = 0

      begin
        # Make sure write access is enabled
        verify_write_access

        # Whether there was a lock of ours to give back. Reported rather
        # than assumed: finding none means someone judged ours stale and
        # took it, so for a while two runs may have been advancing the same
        # workflow, and the caller needs to be able to say so.
        released = false

        # Start a transaction so that the database will be locked
        @database_lock.transaction do |db|
          lock = db.execute("SELECT * FROM lock;")

          # Nothing to release, and indexing an empty table would raise.
          # Said out loud once the transaction has closed: writing a log
          # entry can take seconds, and every other rocoto process trying
          # to reach this database would be waiting on it.
          next if lock.empty?

          if @owner_pid == lock[0][0] && Socket.getaddrinfo(Socket.gethostname, nil, nil,
                                                            Socket::SOCK_STREAM)[0][3] == lock[0][1]
            db.execute("DELETE FROM lock;")
            released = true
          else
            msg = "ERROR: Process #{@owner_pid} cannot unlock the workflow because it is " \
                  "locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{Time.at(lock[0][2])}."
            raise WorkflowMgr::WorkflowLockedException, msg
          end
        end

        # At ordinary verbosity, not above it: this is the reason the run is
        # about to end non-zero, and a cron user reading the mail should not
        # be left with an unexplained failure.
        unless released
          msg = "WARNING: Process #{@owner_pid} found no workflow lock to release; " \
                "it was removed by someone else."
          WorkflowMgr.stderr(msg, 1)
          WorkflowMgr.log(msg)
        end

        released
      rescue WorkflowMgr::WorkflowLockedException
        # Raised rather than exited: this may be running inside an actor,
        # where exiting kills the database out from under its caller instead
        # of ending rocoto. The caller decides what to do about it.
        WorkflowMgr.stderr($ERROR_INFO.to_s, 3)
        WorkflowMgr.log($ERROR_INFO.to_s)
        raise
      rescue SQLite3::BusyException
        if tries < 3
          tries += 1
          sleep rand
          retry
        else
          msg = "ERROR: WorkflowSQLite3DB.unlock_workflow: Could not open workflow database " \
                "file '#{@database_lock_file}' because it is locked by SQLite"
          raise WorkflowMgr::WorkflowDBLockedException, msg
        end
      end
    end

    ##########################################
    #
    # load_cycledefs
    #
    ##########################################
    def load_cycledefs
      # Retrieve the cycle definitions from the database
      dbcycledefs = @database.execute("SELECT groupname,activation_offset,cycledef,dirty FROM cycledef;")

      # Return the array of cycledefs
      dbcycledefs.collect do |cycledef|
        if cycledef[3].nil?
          { group: cycledef[0], activation_offset: cycledef[1] || 0, cycledef: cycledef[2],
            position: nil }
        else
          { group: cycledef[0], activation_offset: cycledef[1] || 0, cycledef: cycledef[2],
            position: Time.at(cycledef[3]).getgm }
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_cycledefs: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # store_cycledefs
    #
    ##########################################
    def store_cycledefs(cycledefs)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Delete all current cycledefs from the database
        db.execute("DELETE FROM cycledef;")

        @database.prepare("INSERT INTO cycledef VALUES (NULL,?,?,?,?)") do |stmt|
          # Add new cycledefs to the database
          cycledefs.each do |cycledef|
            if cycledef[:position].nil?
              stmt.execute(cycledef[:group].to_s, cycledef[:cycledef].to_s, nil, cycledef[:activation_offset])
            else
              stmt.execute(cycledef[:group].to_s, cycledef[:cycledef].to_s, cycledef[:position].to_i,
                           cycledef[:activation_offset])
            end
          end
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.store_cycledefs: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # load_cycle
    #
    ##########################################
    def load_cycle(reftime = Time.gm(1900, 1, 1, 0, 0))
      # Retrieve all cycles from the cycle table
      dbcycles = @database.execute(
        "SELECT cycle,activated,expired,done,draining FROM cycles WHERE cycle == #{reftime.getgm.to_i};"
      )

      # Return an array of Cycles
      dbcycles.collect do |cycle|
        Cycle.new(Time.at(cycle[0]).getgm,
                  { activated: Time.at(cycle[1]).getgm, expired: Time.at(cycle[2]).getgm, done: Time.at(cycle[3]).getgm,
                    draining: Time.at(cycle[4] || 0).getgm })
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_cycle: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # load_cycles
    #
    ##########################################
    def load_cycles(reftime = { start: Time.gm(1900, 1, 1, 0, 0), end: Time.gm(9999, 12, 31, 23, 59) })
      # Get the starting and ending cycle times
      startcycle = reftime[:start].nil? ? Time.gm(1900, 1, 1, 0, 0) : reftime[:start].getgm
      endcycle = reftime[:end].nil? ? Time.gm(9999, 12, 31, 23, 59) : reftime[:end].getgm + 1

      # Retrieve all cycles from the cycle table
      dbcycles = @database.execute(
        "SELECT cycle,activated,expired,done,draining FROM cycles " \
        "WHERE cycle >= #{startcycle.getgm.to_i} and cycle <= #{endcycle.getgm.to_i};"
      )

      # Return an array of Cycles
      dbcycles.collect do |cycle|
        Cycle.new(Time.at(cycle[0]).getgm,
                  { activated: Time.at(cycle[1]).getgm, expired: Time.at(cycle[2]).getgm, done: Time.at(cycle[3]).getgm,
                    draining: Time.at(cycle[4] || 0).getgm })
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_cycles: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # load_last_cycle
    #
    ##########################################
    def load_last_cycle
      dbcycles = []

      # Get the maximum cycle time from the database
      max_cycle = @database.execute("SELECT MAX(cycle) FROM cycles")[0][0]
      unless max_cycle.nil?
        dbcycles = @database.execute(
          "SELECT cycle,activated,expired,done,draining FROM cycles WHERE cycle=#{max_cycle}"
        )
      end

      # Return the last Cycle
      last_cycle = dbcycles.collect do |cycle|
        Cycle.new(Time.at(cycle[0]).getgm,
                  { activated: Time.at(cycle[1]).getgm, expired: Time.at(cycle[2]).getgm, done: Time.at(cycle[3]).getgm,
                    draining: Time.at(cycle[4] || 0).getgm })
      end

      last_cycle.first
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_last_cycle: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # load_active_cycles
    #
    ##########################################
    def load_active_cycles
      # Get the cycles that are neither done nor expired
      dbcycles = @database.execute(
        "SELECT cycle,activated,expired,done,draining FROM cycles WHERE done=0 and expired=0;"
      )

      # Return the array of Cycles
      dbcycles.collect do |cycle|
        Cycle.new(Time.at(cycle[0]).getgm,
                  { activated: Time.at(cycle[1]).getgm, expired: Time.at(cycle[2]).getgm, done: Time.at(cycle[3]).getgm,
                    draining: Time.at(cycle[4] || 0).getgm })
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_active_cycles: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # update_cycles
    #
    ##########################################
    def update_cycles(cycles)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Update each cycle in the database
        cycles.each do |newcycle|
          db.execute(
            "UPDATE cycles SET activated=#{newcycle.activated.to_i},expired=#{newcycle.expired.to_i}," \
            "done=#{newcycle.done.to_i},draining=#{newcycle.draining.to_i} WHERE cycle=#{newcycle.cycle.to_i};"
          )
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.update_cycles: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # remove_cycle
    #
    ##########################################
    def remove_cycle(cycletime)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        db.execute("DELETE FROM cycles WHERE cycle=#{cycletime.to_i};")
        db.execute("DELETE FROM jobs WHERE cycle=#{cycletime.to_i};")
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.update_cycles: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # add_cycles
    #
    ##########################################
    def add_cycles(cycles)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Add each cycle to the database
        cycles.each do |newcycle|
          db.execute(
            "INSERT INTO cycles VALUES (NULL,#{newcycle.cycle.to_i},#{newcycle.activated.to_i}," \
            "#{newcycle.expired.to_i},#{newcycle.done.to_i},#{newcycle.draining.to_i});"
          )
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.add_cycles: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # load_jobs
    #
    ##########################################
    def load_jobs(cycles = nil)
      jobs = {}
      dbjobs = []

      # Start a transaction so that the database will be locked
      @database.results_as_hash = true
      if cycles.nil?

        # Retrieve all jobs from the job table
        dbjobs = @database.execute("SELECT * FROM jobs;")

      else

        # Retrieve all jobs from the job table that match the cycles provided
        cycles.uniq.each do |cycle|
          dbjobs += @database.execute("SELECT * FROM jobs WHERE cycle = #{cycle.to_i};")
        end

      end

      # jobid,taskname,cycle,cores,state,native_state,exit_status,tries,nunknowns,duration
      dbjobs.each do |job|
        task = job['taskname']
        cycle = Time.at(job['cycle']).getgm
        jobs[task] = {} if jobs[task].nil?
        jobs[task][cycle] = Job.new(job['jobid'],             # jobid
                                    task,                     # taskname
                                    cycle,                    # cycle
                                    job['cores'].to_i,        # cores
                                    job['state'],             # state
                                    job['native_state'],      # native state
                                    job['exit_status'].to_i,  # exit_status
                                    job['tries'].to_i,        # tries
                                    job['nunknowns'].to_i,    # nunknowns
                                    job['duration'].to_f)     # duration
      end

      # Return jobs hash
      jobs
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_jobs: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
    ensure
      @database.results_as_hash = false
      # begin
    end

    ##########################################
    #
    # add_jobs
    #
    ##########################################
    def add_jobs(jobs)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Add or update each job in the database
        jobs.each do |job|
          db.execute(
            "INSERT INTO jobs VALUES (NULL,'#{job.id}','#{job.task}',#{job.cycle.to_i},#{job.cores}," \
            "'#{job.state}','#{job.native_state}',#{job.exit_status},#{job.tries}," \
            "#{job.nunknowns},#{job.duration});"
          )
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.add_jobs: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # update_jobs
    #
    ##########################################
    def update_jobs(jobs)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Add or update each job in the database
        jobs.each do |job|
          db.execute(
            "UPDATE jobs SET jobid='#{job.id}',state='#{job.state}',native_state='#{job.native_state}'," \
            "exit_status=#{job.exit_status},tries=#{job.tries},nunknowns=#{job.nunknowns}," \
            "duration=#{job.duration} WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';"
          )
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.update_jobs: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # update_jobids
    #
    ##########################################
    def update_jobids(jobs)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Add or update each job in the database
        jobs.each do |job|
          db.execute("UPDATE jobs SET jobid='#{job.jobid}' WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';")
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.update_jobids: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # delete_jobs
    #
    ##########################################
    def delete_jobs(jobs)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Delete each job from the database
        jobs.each do |job|
          db.execute("DELETE FROM jobs WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';")
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.delete_jobs: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # load_bqservers
    #
    ##########################################
    def load_bqservers
      dbbqservers = @database.execute("SELECT uri FROM bqservers;")

      # Return jobs hash
      dbbqservers.flatten
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_bqservers: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # add_bqservers
    #
    ##########################################
    def add_bqservers(bqservers)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Add or update each bqserver in the database
        bqservers.each do |bqserver|
          db.execute("INSERT INTO bqservers VALUES (NULL,'#{bqserver}');")
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.add_bqservers: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # delete_bqservers
    #
    ##########################################
    def delete_bqservers(bqservers)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Delete each job from the database
        bqservers.each do |bqserver|
          db.execute("DELETE FROM bqservers WHERE uri='#{bqserver}';")
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.delete_bqservers: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # load_downpaths
    #
    ##########################################
    def load_downpaths
      # Retrieve all downpaths from the job table
      dbdownpaths = @database.execute("SELECT path,downdate,host,pid FROM downpaths;")

      # Return an array of downpaths
      dbdownpaths.collect do |downpath|
        { path: downpath[0], downtime: Time.at(downpath[1]).getgm, host: downpath[2], pid: downpath[3] }
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_downpaths: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # add_downpaths
    #
    ##########################################
    def add_downpaths(downpaths)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Add or update each job in the database
        downpaths.each do |downpath|
          # Bound rather than interpolated: these paths come from the
          # workflow document, and a directory with an apostrophe in its
          # name -- somebody's home directory, usually -- made this raise a
          # syntax error, so the hang went unrecorded and the path was
          # retried for the rest of the run.
          db.execute(
            "INSERT INTO downpaths VALUES (NULL,?,?,?,?);",
            [downpath[:path], downpath[:downtime].to_i, downpath[:host], downpath[:pid]]
          )
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.add_downpaths: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # delete_downpaths
    #
    ##########################################
    def delete_downpaths(downpaths)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Delete each downpath from the database
        downpaths.each do |downpath|
          # Bound for the same reason as the insert, and with more at stake
          # here: interpolated, a path containing a quote deleted rows it was
          # never asked to. `x' OR 1=1 --` emptied the table outright.
          db.execute("DELETE FROM downpaths WHERE path=?;", [downpath[:path]])
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.delete_downpaths: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # load_vacuum_time
    #
    ##########################################
    def load_vacuum_time
      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Access the vacuum table
        vacuum = db.execute("SELECT * FROM vacuum;")

        # If no vacuum time is present, we have never vacuumed
        if vacuum.empty?
          return Time.at(0).getgm
        else
          # Return the last vacuum time
          return Time.at(vacuum[0][0]).getgm
        end
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_vacuum_time: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # store_vacuum_time
    #
    ##########################################
    def store_vacuum_time(vacuum_time)
      # Make sure write access is enabled
      verify_write_access

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Remove the old vacuum time
        db.execute("DELETE FROM vacuum;")
        db.execute("INSERT INTO vacuum VALUES (#{vacuum_time.to_i});")
      end
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.store_vacuum_time: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # vacuum
    #
    ##########################################
    def vacuum(age)
      vacuum_date = Time.now.to_i - age

      begin
        # Make sure write access is enabled
        verify_write_access

        # Count the number of jobs removed
        njobs_removed = 0

        # Start a transaction so that the database will be locked
        @database.transaction do |db|
          # Remove jobs from all old expired cycles
          db.execute(
            "DELETE from jobs where jobs.id in (SELECT jobs.id from jobs INNER JOIN cycles ON " \
            "jobs.cycle = cycles.cycle where (cycles.expired < #{vacuum_date} and cycles.expired > 0));"
          )

          # Get the number of jobs removed from expired cycles
          njobs_removed += db.changes

          # Remove jobs from all old completed cycles
          db.execute(
            "DELETE from jobs where jobs.id in (SELECT jobs.id from jobs INNER JOIN cycles ON " \
            "jobs.cycle = cycles.cycle where (cycles.done < #{vacuum_date} and cycles.done > 0));"
          )

          # Get the number of jobs removed from completed cycles
          njobs_removed += db.changes
        end

        # Recover the empty space
        @database.execute("VACUUM;")

        WorkflowMgr.stderr("Vacuumed database. Removed #{njobs_removed} jobs", 3)
        WorkflowMgr.log("Vacuumed database. Removed #{njobs_removed} jobs")
      rescue SQLite3::BusyException
        msg = "ERROR: WorkflowSQLite3DB.vacuum: Could not open workflow database " \
              "file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException, msg
      end
    end

    ##########################################
    #
    # load_tables
    #
    ##########################################
    def load_tables(tablelist = nil)
      tables = {}

      # Get a listing of the database tables
      dbtables = @database.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
      dbtables.flatten!

      # Create an array of rows for each table
      dbtables.each do |table|
        # Initialize the array of rows to be empty
        tables[table.to_sym] = []

        # Get all rows for the table, where first row is column names
        dbtable = @database.execute2("SELECT * FROM #{table};")

        # Get the table column names
        columns = dbtable.shift

        # Add each table row to the array of rows for this table
        dbtable.each do |row|
          rowdata = {}

          # Loop over columns, creating a hash for this row's data
          columns.each_with_index do |column, idx|
            rowdata[column.to_sym] = row[idx]
          end

          # Add the hash representing this row to the array of rows for this table
          tables[table.to_sym] << rowdata
        end
      end

      # Return the tables
      tables
    rescue SQLite3::BusyException
      msg = "ERROR: WorkflowSQLite3DB.load_tables: Could not open workflow database " \
            "file '#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end


    private


    ##########################################
    #
    # verify_permissions
    #
    ##########################################
    def verify_permissions
      if @mode[:readonly]

        # Make sure the database file exists and that we can read it
        if File.exist?(@database_file)
          unless File.readable?(@database_file)
            raise WorkflowDBAccessException, "ERROR: You do not have permission to read #{@database_file}"
          end
        elsif File.readable?(File.dirname(@database_file))
          raise WorkflowDBAccessException, "ERROR: Can not open #{@database_file} read-only because it does not exist"
        else
          raise WorkflowDBAccessException, "ERROR: You do not have permission to read #{@database_file}"
        end

      else

        # Make sure the database and database lock files are writable
        [@database_file, @database_lock_file].each do |dbfile|
          if File.exist?(dbfile)
            unless File.writable?(dbfile)
              raise WorkflowDBAccessException, "ERROR: You do not have permission to modify #{dbfile}"
            end
          else
            unless File.writable?(File.dirname(dbfile))
              raise WorkflowDBAccessException, "ERROR: You do not have permission to create #{dbfile}"
            end
          end
        end
      end
    end

    ##########################################
    #
    # verify_write_access
    #
    ##########################################
    def verify_write_access
      if @mode[:readonly]
        raise WorkflowDBAccessException,
              "ERROR: Can not lock or modify a database opened in read-only mode"
      end
    end

    ##########################################
    #
    # open_lock_db
    #
    ##########################################
    def open_lock_db
      # Make sure write access is enabled
      verify_write_access

      # Get a handle to the lock  database
      # Any earlier handle is closed first: opening the database is retried
      # when SQLite reports it busy, and each attempt would otherwise leave
      # its connection behind.
      @database_lock.close if @database_lock && !@database_lock.closed?
      @database_lock = SQLite3::Database.new(@database_lock_file, @mode)

      # Set the retry limit (milliseconds) for locked resources
      @database_lock.busy_timeout = 10_000

      # Return results as arrays
      @database_lock.results_as_hash = false

      # Start a transaction so that the database will be locked
      @database_lock.transaction do |db|
        # Get a listing of the database tables
        tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

        # Create the lock table if it doesn't exist
        unless tables.flatten.member?("lock")
          db.execute("CREATE TABLE lock (pid INTEGER, host VARCHAR(64), time DATETIME);")
        end
      end
    rescue SQLite3::BusyException
      msg = "WorkflowSQLite3DB.open_lock_db: Could not open workflow database file " \
            "'#{@database_lock_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # open_workflow_db
    #
    ##########################################
    def open_workflow_db
      # Get a handle to the database
      # Closed first for the same reason as the lock database: a retried
      # open must not leave its predecessor's connection open.
      @database.close if @database && !@database.closed?
      @database = SQLite3::Database.new(@database_file, @mode)

      # Set the retry limit (milliseconds) for locked resources
      @database.busy_timeout = 10_000

      # Return results as arrays
      @database.results_as_hash = false

      # Don't try to update the db if in readonly mode
      return if @mode[:readonly]

      # Start a transaction so that the database will be locked
      @database.transaction do |db|
        # Get a listing of the database tables
        tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

        # Create all tables that are missing from the database
        create_tables(db, tables.flatten)

        # Update the tables if needed
        update_tables(db)
      end
    rescue SQLite3::BusyException
      msg = "WorkflowSQLite3DB.dbopen: Could not open workflow database file " \
            "'#{@database_file}' because it is locked by SQLite"
      raise WorkflowMgr::WorkflowDBLockedException, msg
      # begin
    end

    ##########################################
    #
    # create_tables
    #
    ##########################################
    def create_tables(db, tables)
      # Make sure write access is enabled
      verify_write_access

      raise "WorkflowSQLite3DB::create_tables must be called inside a transaction" unless db.transaction_active?

      # Create the cycledef table
      unless tables.member?("cycledef")
        db.execute(
          "CREATE TABLE cycledef (id INTEGER PRIMARY KEY, groupname VARCHAR(64), " \
          "cycledef VARCHAR(256), dirty BOOLEAN);"
        )
      end

      # Create the cycles table
      unless tables.member?("cycles")
        db.execute(
          "CREATE TABLE cycles (id INTEGER PRIMARY KEY, cycle DATETIME, activated DATETIME, " \
          "expired DATETIME, done DATETIME, draining DATETIME);"
        )
      end

      # Create the jobs table
      unless tables.member?("jobs")
        db.execute(
          "CREATE TABLE jobs (id INTEGER PRIMARY KEY, jobid VARCHAR(64), taskname VARCHAR(64), " \
          "cycle DATETIME, cores INTEGER, state VARCHAR(64), native_state VARCHAR[64], " \
          "exit_status INTEGER, tries INTEGER, nunknowns INTEGER, duration REAL);"
        )
      end

      # Create the bqservers table
      unless tables.member?("bqservers")
        db.execute("CREATE TABLE bqservers (id INTEGER PRIMARY KEY, uri VARCHAR(1024));")
      end

      # Create the downpaths table
      unless tables.member?("downpaths")
        db.execute(
          "CREATE TABLE downpaths (id INTEGER PRIMARY KEY, path VARCHAR(1024), " \
          "downdate DATETIME, host VARCHAR(64), pid INTEGER);"
        )
      end

      # Create the vacuum table
      unless tables.member?("vacuum")
        db.execute("CREATE TABLE vacuum (last_vacuum DATETIME);")
      end
    end

    ##########################################
    #
    # update_tables
    #
    ##########################################
    def update_tables(db)
      # Make sure write access is enabled
      verify_write_access

      raise "WorkflowSQLite3DB::update_tables must be called inside a transaction" unless db.transaction_active?

      # Get the command used to create the jobs table
      jobscrt = db.execute("SELECT sql FROM sqlite_master WHERE tbl_name='jobs' AND type='table';").to_s

      # Parse the jobs command to see if the duration column is not there
      unless jobscrt =~ /duration REAL/
        db.execute("ALTER TABLE jobs ADD COLUMN duration REAL;")
      end

      # Get the command used to create the cycle table
      cyclescrt = db.execute("SELECT sql FROM sqlite_master WHERE tbl_name='cycles' AND type='table';").to_s

      # Parse the cycle command to see if the draining column is not there
      unless cyclescrt =~ /draining DATETIME/
        db.execute("ALTER TABLE cycles ADD COLUMN draining DATETIME;")
      end

      # Get the command used to create the cycle table
      cycledefscrt = db.execute("SELECT sql FROM sqlite_master WHERE tbl_name='cycledef' AND type='table';").to_s

      # Parse the cycle command to see if the draining column is not there
      unless cycledefscrt =~ /activation_offset INTEGER/
        db.execute("ALTER TABLE cycledef ADD COLUMN activation_offset INTEGER;")
      end
    end

    ##########################################
    #
    # BusyRetry
    #
    # SQLite refuses rather than waits when another process holds the file,
    # which happens routinely when rocotostat reads while rocotorun writes.
    # The DRb proxy this replaced retried every call for about five seconds
    # before giving up, so that patience is kept here instead -- in one
    # place, and for callers that use the database directly as well as for
    # those that reach it through an actor.
    #
    # Retrying is safe for these because SQLite reports the file busy only
    # when the transaction did not begin, so nothing has been half-done.
    #
    # Taking and releasing the workflow lock are deliberately left out.
    # Re-running lock_workflow after its row is committed reads that row as
    # somebody else's lock and reports failure, so the caller abandons a
    # workflow it holds and never unlocks it. Both methods already retry a
    # busy database three times of their own accord, which is what they did
    # before any of this was wrapped.
    #
    ##########################################
    BUSY_TRIES = 10
    BUSY_WAIT = 0.5

    retryable = public_instance_methods(false) - %i[lock_workflow unlock_workflow]

    BusyRetry = Module.new do
      retryable.each do |operation|
        define_method(operation) do |*args, &block|
          tries = 0
          begin
            super(*args, &block)
          rescue WorkflowMgr::WorkflowDBLockedException
            raise if tries >= BUSY_TRIES

            tries += 1
            sleep BUSY_WAIT
            retry
          end
        end
      end
    end

    prepend BusyRetry
  end
end
