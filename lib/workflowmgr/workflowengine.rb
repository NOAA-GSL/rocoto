##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowEngine
  #
  ##########################################
  class WorkflowEngine
    require 'drb'
    require 'workflowmgr/workflowconfig'
    require 'workflowmgr/workflowoption'
    require 'workflowmgr/workflowstate'
    require 'workflowmgr/launchserver'
    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/dbproxy'
    require 'workflowmgr/workflowioproxy'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/dependency'
    require 'workflowmgr/bqsproxy'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(options)
      # Disable garbage collection
      GC.disable

      # Turn on full program tracing for verbosity 1000+
      if WorkflowMgr::VERBOSE > 999
        set_trace_func proc { |event, file, line, id, binding, classname|
          printf "%<event>10s %<file>s:%<line>-2d %<id>10s %<classname>8s\n",
                 event: event, file: file, line: line, id: id, classname: classname
        }

      # Turn on program tracing for Rocoto code only for verbosity 100+
      elsif WorkflowMgr::VERBOSE > 99
        set_trace_func proc { |event, file, line, id, binding, classname|
          case event
          when "call", "return", "line"
            if file =~ /\/lib\/workflowmgr\/|\/lib\/wfmstat\//
              printf "%<event>10s %<file>s:%<line>-2d %<id>10s %<classname>8s\n",
                     event: event, file: file, line: line, id: id, classname: classname
            end
          end
        }
      end

      # Get configuration file options
      @config = WorkflowYAMLConfig.new

      # Get command line options
      @options = options

      # Set up an object to serve the workflow database (but do not open the database)
      @db_server = DBProxy.new(@config, @options)

      # Initialize the workflow lock
      @locked = false

      @subset = nil
      @selected_tasks = nil
      @selected_cycles = nil
    rescue StandardError => e
      WorkflowMgr.stderr('Workflow Manager Initialization failed.', 1)
      WorkflowMgr.stderr(e.message, 1)
      WorkflowMgr.log(e.message)
      if e.is_a?(ArgumentError) || e.is_a?(NameError) || e.is_a?(TypeError)
        WorkflowMgr.stderr('Unexpected failure: Workflow Manager Initialization failed.', 1)
        WorkflowMgr.stderr(e.backtrace.join("\n"), 1)
        WorkflowMgr.log(e.backtrace.join("\n"))
      end
      Process.exit(1)
    end

    ##########################################
    #
    # rewind!
    #
    ##########################################
    def rewind!
      # with_locked_db
      with_locked_db do
        # Build the workflow objects from the contents of the workflow document
        build_workflow

        # Get the active cycles
        load_active_cycles

        # Get the active jobs, which may include jobs from cycles that have just expired
        # as well as jobs needed for evaluating inter cycle dependencies
        fetch_active_jobs

        # Update the status of all active jobs
        update_active_jobs

        # Deactivate completed cycles
        deactivate_done_cycles

        # Expire active cycles that have exceeded the cycle life span
        expire_cycles

        # Get the list of complete cycles
        rewind_cycles = selected_cycles
        nrewind_cycles = rewind_cycles.length

        # Collect the names of tasks to complete
        rewind_tasks = selected_tasks
        nrewind_tasks = rewind_tasks.length
        all_tasks = @options.all_tasks

        if all_tasks
          puts "Rewinding all tasks will set the cycle to \"inactive\" status, as if Rocoto had never started it."
          printf "Are you sure you want to proceed? (y/n) "
          reply = $stdin.gets
          unless reply =~ /^[Yy]/
            Process.exit(0)
          end
        end

        # Ask user for confirmation if rewind tasks/cycles list is very large
        if nrewind_cycles > 10 || nrewind_tasks > 10
          printf "Preparing to rewind #{nrewind_tasks} tasks for #{nrewind_cycles} cycles.  " \
                 "A total of #{nrewind_tasks * nrewind_cycles} tasks will be rewound.\n"
          printf "Are you sure you want to proceed? (y/n) "
          reply = $stdin.gets
          unless reply =~ /^[Yy]/
            Process.exit(0)
          end
        end

        did_something = false
        do_not_reactivate = false
        rewind_cycles.each do |cycle|
          strcyc = cycle.strftime('%Y%m%d%H%M')
          # puts "#{strcyc}: consider this cycle"
          # Find the cycle, or nil if the cycle was never attempted
          rewind_cycle = find_cycle(cycle)

          unless rewind_cycle.nil?
            puts "#{strcyc}: Rewind tasks for #{rewind_cycle}"
          end

          if rewind_cycle.nil?
            puts "#{strcyc}: Cycle is inactive (unstarted).  Nothing to do."
            next
          end

          if rewind_cycle.expired?
            puts "#{strcyc} is expired.  Will rewind tasks, but will not reactivate cycle."
            do_not_reactivate = true
          end

          if all_tasks
            @active_jobs.keys
          else
            # puts "Rewind specified tasks."
            @options.tasks
          end
          did_something = false
          # puts "ACTIVE JOBS: <#{@active_jobs.keys.join('><')}>"
          rewind_tasks.each do |task_name|
            # puts "#{strcyc}: #{task_name}: consider this task"
            cycjob = @active_jobs[task_name.to_s]
            if cycjob.nil?
              # puts "#{strcyc}: #{task_name}: job has not been tried yet.  Doing nothing to this task."
              next
            end

            # puts "#{strcyc}: #{task_name}: jobs exist for <#{cycjob.keys.join(', ')}>"
            rewind_job = cycjob[cycle]
            if rewind_job.nil?
              # puts "#{strcyc}: #{task_name}: job has not been tried yet for cycle #{strcyc}. " \
              #      " Doing nothing to this task."
              next
            elsif rewind_job.dead?
              puts "#{strcyc}: #{task_name}: rewinding dead job."
            elsif rewind_job.failed?
              puts "#{strcyc}: #{task_name}: rewinding failed job."
            elsif rewind_job.done?
              puts "#{strcyc}: #{task_name}: rewinding successful job."
            else
              strid = rewind_job.id.to_s
              puts "#{strcyc}: #{task_name}: killing job #{strid}..."
              @bq_server.delete(strid)
              puts "#{strcyc}: #{task_name}: will now rewind."
            end

            task = @tasks[task_name.to_s]
            if task.nil?
              puts "#{strcyc}: #{task_name}: No entry in @tasks.  Task does not exist.  INTERNAL ERROR."
              raise
            end
            wstate = WorkflowState.new(cycle, @active_jobs, @workflow_io_server, @cycledefs, task_name, task,
                                       @tasks)
            task.rewind!(wstate)

            puts "#{strcyc}: #{task_name}: deleting all records of this job."
            rewind_job.tries = 0
            @db_server.delete_jobs([rewind_job])
            did_something = true
          end

          unless did_something
            puts "#{strcyc}: No tasks to rewind."
          end

          if do_not_reactivate
            puts "#{strcyc}: not reactivating this cycle."
          elsif all_tasks
            rewind_cycle.rewind!


            puts "#{strcyc}: Deactivate cycle: #{rewind_cycle}"

            @db_server.remove_cycle(rewind_cycle.cycle)
          elsif did_something
            if rewind_cycle.done? || rewind_cycle.draining?
              if rewind_cycle.done?
                puts "#{strcyc}: WARNING: Cycle is done.  Setting its tasks' tries to 0 may start other tasks."
              else
                puts "#{strcyc}: WARNING: Cycle is draining.  Setting its tasks' tries to 0 may start other " \
                     "tasks.  If any final tasks are succeeded, the cycle will be drained as soon " \
                     "as rocotorun is executed again."
              end
              rewind_cycle.reactivate!
              @db_server.update_cycles([rewind_cycle])
            end

            unless rewind_cycle.active?
              puts "#{strcyc}: ERROR: Unable to active cycle.  Cycle is in state #{rewind_cycle.state}."
              return # rubocop:disable Lint/NonLocalExitFromIterator
            end
          end
        end
      end
    end

    ##########################################
    #
    # run
    #
    ##########################################
    def run
      if WorkflowMgr.dryrun_mode?
        WorkflowMgr.stderr("Dryrun Mode: no new jobs would be submitted", 1)
      end
      with_locked_db do
        # Build the workflow objects from the contents of the workflow document
        build_workflow

        # Get the active cycles
        load_active_cycles

        # Get new cycles that need to be activated now
        fetch_new_cycles

        # Get the active jobs, which may include jobs from cycles that have just expired
        # as well as jobs needed for evaluating inter cycle dependencies
        fetch_active_jobs

        # Update the status of all active jobs
        update_active_jobs

        # Deactivate completed cycles
        deactivate_done_cycles

        # Expire active cycles that have exceeded the cycle life span
        expire_cycles

        # Submit new tasks where possible
        submit_new_jobs

        # Auto vacuum if necessary, but only for realtime mode
        auto_vacuum if @config.AutoVacuum && @realtime
      end
    end

    ##########################################
    #
    # find_cycle
    #
    ##########################################
    def find_cycle(cycle_time)
      # Look for the boot cycle in the active cycles
      boot_cycle = @active_cycles.find { |c| cycle_time == c.cycle }

      # If it wasn't in the active cycle list, look for it in the
      # database and add the jobs for that cycle to the active job
      # list as well
      if boot_cycle.nil?
        boot_cycle = @db_server.load_cycle(cycle_time).first
        @active_jobs.merge!(@db_server.load_jobs([cycle_time]))
      end
      boot_cycle
    end

    ##########################################
    #
    # boot
    #
    ##########################################
    def boot
      if WorkflowMgr.dryrun_mode?
        WorkflowMgr.stderr("Dryrun Mode: no new jobs would be submitted", 1)
      end

      # with_locked_db
      with_locked_db do
        # Build the workflow objects from the contents of the workflow document
        build_workflow

        # Get the active cycles
        load_active_cycles

        # Get the active jobs, which may include jobs from cycles that have just expired
        # as well as jobs needed for evaluating inter cycle dependencies
        fetch_active_jobs

        # Update the status of all active jobs
        update_active_jobs

        # Deactivate completed cycles
        deactivate_done_cycles

        # Expire active cycles that have exceeded the cycle life span
        expire_cycles

        # Initialize a task cycledef hash
        taskcycledefs = {}

        # Get the list of boot cycles
        boot_cycles = selected_cycles
        nboot_cycles = boot_cycles.length

        # Collect the names of tasks to boot
        boot_tasks = selected_tasks
        nboot_tasks = boot_tasks.length

        # Ask user for confirmation if boot tasks/cycles list is very large
        if nboot_cycles > 10 || nboot_tasks > 10
          printf "Preparing to boot #{nboot_tasks} tasks for #{nboot_cycles} cycles.  " \
                 "A total of #{nboot_tasks * nboot_cycles} tasks will be booted.  This may take a while.\n"
          printf "Are you sure you want to proceed? (y/n) "
          reply = $stdin.gets
          unless reply =~ /^[Yy]/
            Process.exit(0)
          end
        end

        # Iterate over boot cycles
        # boot_cycles.each
        boot_cycles.each do |boot_cycle_time|
          # Boot each task for this boot cycle
          # boot_tasks.each
          boot_tasks.each do |boot_task_name|
            # Get the boot task from the workflow definition
            task = @tasks[boot_task_name]

            # Reject this request if the task is not defined in the XML
            if task.nil?
              puts "Can not boot task '#{boot_task_name}' for cycle '#{boot_cycle_time.strftime('%Y%m%d%H%M')}' " \
                   "because the task is not defined in the workflow definition"
              next
            end

            # Make sure the cycle is valid for this task
            unless task.attributes[:cycledefs].nil?
              # Get the cycledefs associated with this task
              if taskcycledefs[boot_task_name].nil?
                taskcycledefs[boot_task_name] = @cycledefs.find_all do |cycledef|
                  task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
                end
              end
              # Reject this task if the cycle is not a member of the tasks cycle list
              unless taskcycledefs[boot_task_name].any? do |cycledef|
                cycledef.member?(boot_cycle_time)
              end
                puts "Can not boot task '#{boot_task_name}' for cycle '#{boot_cycle_time.strftime('%Y%m%d%H%M')}' " \
                     "because the cycle is not defined for that task"
                next
              end
            end

            # Find the requested cycle
            boot_cycle = find_cycle(boot_cycle_time)

            # Activate a new cycle if necessary and add it to the database
            if boot_cycle.nil?
              if @options.all_tasks
                puts "Booting task '#{boot_task_name}' for cycle " \
                     "'#{boot_cycle_time.strftime('%Y%m%d%H%M')}' will activate " \
                     "cycle '#{boot_cycle_time.strftime('%Y%m%d%H%M')}' for the first time."
                reply = 'y'
              else
                printf "Booting task '#{boot_task_name}' for cycle " \
                       "'#{boot_cycle_time.strftime('%Y%m%d%H%M')}' will activate " \
                       "cycle '#{boot_cycle_time.strftime('%Y%m%d%H%M')}' for the first time.\n"
                printf "This may trigger submission of other tasks for cycle " \
                       "'#{boot_cycle_time.strftime('%Y%m%d%H%M')}' " \
                       "in addition to '#{boot_task_name}'\n"
                printf "Are you sure you want to boot '#{boot_task_name}' for cycle " \
                       "'#{boot_cycle_time.strftime('%Y%m%d%H%M')}' ? (y/n) "
                reply = $stdin.gets
              end
              if reply =~ /^[Yy]/
                boot_cycle = Cycle.new(boot_cycle_time)
                boot_cycle.activate!
                unless WorkflowMgr.dryrun_mode?
                  @db_server.add_cycles([boot_cycle])
                end
                boot_job = nil
              else
                puts "task '#{boot_task_name}' for cycle '#{boot_cycle_time.strftime('%Y%m%d%H%M')}' will not be booted"
                next # boot next task
              end
            else

              # Reactivate the cycle if it is done (but not expired)
              if boot_cycle.done? || boot_cycle.draining?
                boot_cycle.reactivate!
                unless WorkflowMgr.dryrun_mode?
                  @db_server.update_cycles([boot_cycle])
                end
              end

              # Retrieve the boot job from the database
              if boot_cycle.active?
                boot_job = if @active_jobs[boot_task_name].nil?
                             nil
                           else
                             @active_jobs[boot_task_name][boot_cycle_time]
                           end
              else
                print "WARNING: Cycle #{boot_cycle_time.strftime('%Y%m%d%H%M')} state is #{boot_cycle.state}.  " \
                      "I can boot task #{boot_task_name}, but this cycle might not complete again unless you " \
                      "boot the final task.  Proceed anyway (y/n)?"
                reply = $stdin.gets
                if reply =~ /^[Yy]/
                  puts "Okay, but don't say I didn't warn you."
                  boot_job = nil
                else
                  puts "Wheew.  I really dodged a bullet there.  Task '#{boot_task_name}' for cycle " \
                       "'#{boot_cycle_time.strftime('%Y%m%d%H%M')}' will not be booted!"
                  next # boot next task
                end
              end

            end

            # Check for existing jobs that are not done or expired
            unless boot_job.nil?
              if !boot_job.done? && !boot_job.expired?
                puts "Can not boot task '#{boot_task_name}' for cycle " \
                     "'#{boot_cycle_time.strftime('%Y%m%d%H%M')}' because a job for it already " \
                     "exists in state #{boot_job.state}.  You need to rewind this task instead."
                next
              end
              if boot_job.expired?
                puts "I should not boot task '#{boot_task_name}' for cycle " \
                     "'#{boot_cycle_time.strftime('%Y%m%d%H%M')}' because the task has expired.  " \
                     "Are you sure you want to boot this task anyway? (y/n)"
                reply = $stdin.gets
                if reply =~ /^[Yy]/
                  puts "Okay, but don't say I didn't warn you."
                  boot_job = nil
                else
                  puts "I'm glad you came to your senses!  Task '#{boot_task_name}' for cycle " \
                       "'#{boot_cycle_time.strftime('%Y%m%d%H%M')}' will not be booted!"
                  next # boot next task
                end
              end
            end

            # Initialize jobid of the new job
            # In dryrun mode, no DRb server is launched, so use 0 as placeholder
            newjobid = if @config.BatchQueueServer && !WorkflowMgr.dryrun_mode?
                         @bq_server.__drburi
                       else
                         0
                       end

            # Create the job
            job = Job.new(newjobid,                            # jobid
                          boot_task_name,                      # taskname
                          boot_cycle_time,                     # cycle
                          task.attributes[:cores],             # cores
                          "SUBMITTING",                        # state
                          "SUBMITTING",                        # native state
                          0,                                   # exit_status
                          boot_job.nil? ? 0 : boot_job.tries + 1, # tries
                          0, # nunknowns
                          0.0) # duration

            # Add the new job to the database
            unless WorkflowMgr.dryrun_mode?
              @db_server.add_jobs([job])
            end

            # Localize all <cyclestr> to current cycle
            localtask = task.localize(boot_cycle_time)

            # Create output directories for <stdout>,<stderr>,<join> paths
            begin
              outdir = ""
              localtask.attributes.each do |option, value|
                case option
                when :stdout, :stderr, :join
                  if value[-1, 1] == "/"
                    outdir = value
                  else
                    outdir = value.split("/")[0..-2].join("/")
                    # Roll the log file (if it already exists)
                    @workflow_io_server.roll_log(value)
                  end
                  unless outdir.empty?
                    @workflow_io_server.mkdir_p(outdir)
                  end
                end
              end
            rescue WorkflowIOHang
              msg = "WARNING! Can not submit #{task.attributes[:name]} because output directory " \
                    "'#{outdir}' resides on an unresponsive file system!"
              @log_server.log(boot_cycle_time, msg)
              WorkflowMgr.stderr(msg, 2)
              WorkflowMgr.log(msg)
            end

            # Submit the task
            @bq_server.submit(task.localize(boot_cycle_time), boot_cycle_time)
            unless WorkflowMgr.dryrun_mode?
              @log_server.log(boot_cycle_time,
                              "Forcibly submitting #{task.attributes[:name]}")
            end

            # If we are not using a batch queue server, shut down the pool, which blocks until
            # this submission finishes running, before checking for job ids. submit() recreates
            # the pool on the next iteration.
            # Skip in dryrun mode - thread pool workers sleep indefinitely waiting for work and cause deadlock
            unless @config.BatchQueueServer || WorkflowMgr.dryrun_mode?
              @bq_server.shutdown
            end

            # Harvest job ids for submitted tasks
            job.id
            jobid, output = @bq_server.get_submit_status(job.task, job.cycle)

            # Classify submission outcome: dryrun first, then pending, failure, success
            # Dryrun returns [nil, "This is a dryrun"] so output is non-nil;
            # checking output.nil? first would mis-classify dryrun as failure.
            if WorkflowMgr.dryrun_mode?
              @log_server.log(job.cycle,
                              "Dryrun Mode: would submit #{job.task} for cycle #{job.cycle.strftime('%Y%m%d%H%M')}")
            elsif output.nil?
              @log_server.log(job.cycle,
                              "Submission status of #{job.task} is pending at #{job.id}")
            elsif jobid.nil?
              # Delete the job from the database since it failed to submit.  It will be retried next time around.
              @db_server.delete_jobs([job])
              WorkflowMgr.stderr(output, 1)
              @log_server.log(job.cycle, "Submission of #{job.task} failed!  #{output}")
            else
              job.id = jobid
              job.state = "QUEUED"
              job.native_state = "queued"
              @log_server.log(job.cycle,
                              "Submission of #{job.task} succeeded, jobid=#{job.id}")
              # Update the jobid for the job in the database
              @db_server.update_jobs([job])
            end

            if WorkflowMgr.dryrun_mode?
              puts "task '#{boot_task_name}' for cycle '#{boot_cycle_time.strftime('%Y%m%d%H%M')}' " \
                   "would be booted (dryrun mode)"
            else
              puts "task '#{boot_task_name}' for cycle '#{boot_cycle_time.strftime('%Y%m%d%H%M')}' has been booted"
            end
          end
        end
      end
    end

    ##########################################
    #
    # complete!
    #
    ##########################################
    def complete!
      # with_locked_db
      with_locked_db do
        # Build the workflow objects from the contents of the workflow document
        build_workflow

        # Get the active cycles
        load_active_cycles

        # Get the active jobs, which may include jobs from cycles that have just expired
        # as well as jobs needed for evaluating inter cycle dependencies
        fetch_active_jobs

        # Initialize a task cycledef hash
        taskcycledefs = {}

        # Get the list of complete cycles
        complete_cycles = selected_cycles
        ncomplete_cycles = complete_cycles.length

        # Collect the names of tasks to complete
        complete_tasks = selected_tasks
        ncomplete_tasks = complete_tasks.length

        if @options.all_tasks
          puts 'Will complete all tasks in specified cycles.  This mark cycles as "done" and all tasks ' \
               'within the cycles as "succeeded."  Running "rocotorun" will have no more impacts on ' \
               'cycles completed in this manner unless you "rocotorewind" them.'
          printf "Are you sure you want to proceed? (y/n) "
          reply = $stdin.gets
          unless reply =~ /^[Yy]/
            Process.exit(0)
          end
        end

        # Ask user for confirmation if complete tasks/cycles list is very large
        if ncomplete_cycles > 10 || ncomplete_tasks > 10
          printf "Preparing to complete #{ncomplete_tasks} tasks for #{ncomplete_cycles} cycles.  " \
                 "A total of #{ncomplete_tasks * ncomplete_cycles} tasks will be completed.  This may take a while.\n"
          printf "Are you sure you want to proceed? (y/n) "
          reply = $stdin.gets
          unless reply =~ /^[Yy]/
            Process.exit(0)
          end
        end

        # Iterate over cycles
        # complete_cycles.each
        complete_cycles.each do |complete_cycle_time|
          # Find the requested cycle
          complete_cycle = find_cycle(complete_cycle_time)

          # Complete each requested task for this cycle
          # complete_tasks.each
          complete_tasks.each do |complete_task_name|
            # Get the complete task from the workflow definition
            task = @tasks[complete_task_name]

            # Reject this request if the task is not defined in the XML
            if task.nil?
              puts "Can not complete task '#{complete_task_name}' for cycle " \
                   "'#{complete_cycle_time.strftime('%Y%m%d%H%M')}' because the task is not " \
                   "defined in the workflow definition"
              next
            end

            # Make sure the cycle is valid for this task
            unless task.attributes[:cycledefs].nil?
              # Get the cycledefs associated with this task
              if taskcycledefs[complete_task_name].nil?
                taskcycledefs[complete_task_name] = @cycledefs.find_all do |cycledef|
                  task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
                end
              end
              # Reject this task if the cycle is not a member of the tasks cycle list
              unless taskcycledefs[complete_task_name].any? do |cycledef|
                cycledef.member?(complete_cycle_time)
              end
                puts "Can not complete task '#{complete_task_name}' for cycle " \
                     "'#{complete_cycle_time.strftime('%Y%m%d%H%M')}' because the cycle is " \
                     "not defined for that task"
                next
              end
            end

            # Activate a new cycle if necessary and add it to the database
            if complete_cycle.nil?
              if !@options.all_tasks
                printf "Completing task '#{complete_task_name}' for cycle " \
                       "'#{complete_cycle_time.strftime('%Y%m%d%H%M')}' will activate " \
                       "cycle '#{complete_cycle_time.strftime('%Y%m%d%H%M')}' for the first time.\n"
                printf "This may trigger submission of other tasks for cycle " \
                       "'#{complete_cycle_time.strftime('%Y%m%d%H%M')}' " \
                       "in addition to '#{complete_task_name}'\n"
                printf "Are you sure you want to complete '#{complete_task_name}' for cycle " \
                       "'#{complete_cycle_time.strftime('%Y%m%d%H%M')}' ? (y/n) "
                reply = $stdin.gets
              else
                puts "Starting cycle '#{complete_cycle_time.strftime('%Y%m%d%H%M')}' so I can complete task " \
                     "'#{complete_task_name}'.  I will set the cycle to \"done\" shortly."
                reply = 'y'
              end
              if reply =~ /^[Yy]/
                complete_cycle = Cycle.new(complete_cycle_time)
                complete_cycle.activate!
                @db_server.add_cycles([complete_cycle])
                complete_job = nil
              else
                puts "task '#{complete_task_name}' for cycle '#{complete_cycle_time.strftime('%Y%m%d%H%M')}' " \
                     "will not be completed"
                next # complete next task
              end
            else

              # Reactivate the cycle if it is done (but not expired)
              if complete_cycle.done?
                complete_cycle.reactivate!
                @db_server.update_cycles([complete_cycle])
              end

              # Retrieve the complete job from the database
              if complete_cycle.active?
                complete_job = if @active_jobs[complete_task_name].nil?
                                 nil
                               else
                                 @active_jobs[complete_task_name][complete_cycle_time]
                               end
              else
                print "WARNING: Cycle #{complete_cycle_time.strftime('%Y%m%d%H%M')} state is " \
                      "#{complete_cycle.state}.  Completing task #{complete_task_name} may have " \
                      "unintended consequences. Proceed anyway (y/n)?"
                reply = $stdin.gets
                if reply =~ /^[Yy]/
                  puts "Okay, but don't say I didn't warn you."
                  complete_job = nil
                else
                  puts "Wheew.  I really dodged a bullet there.  Task '#{complete_task_name}' for cycle " \
                       "'#{complete_cycle_time.strftime('%Y%m%d%H%M')}' will not be completed now!"
                  next # complete next task
                end
              end

            end

            # Check for existing jobs that are not done or expired
            if !complete_job.nil? && complete_job.expired?
              puts "Can not complete task '#{complete_task_name}' for cycle " \
                   "'#{complete_cycle_time.strftime('%Y%m%d%H%M')}' " \
                   "because the task has expired"
              next
            end

            # Create the job if we don't have one:
            if complete_job.nil?
              job = Job.new(-1,                                  # jobid
                            complete_task_name,                  # taskname
                            complete_cycle_time,                 # cycle
                            task.attributes[:cores],             # cores
                            "SUCCEEDED",                         # state
                            "FORCED",                            # native state
                            0,                                   # exit_status
                            1,                                   # tries
                            0,                                   # nunknowns
                            0.0) # duration
              @db_server.add_jobs([job])
            else
              job = complete_job
              job.state = 'SUCCEEDED'
              job.native_state = 'FORCED'
              @db_server.update_jobs([job])
            end

            # Add the new job to the database

            puts "task '#{complete_task_name}' for cycle '#{complete_cycle_time.strftime('%Y%m%d%H%M')}' " \
                 "has been completed"
          end
        end
        # Deactivate completed cycles
        deactivate_done_cycles

        # Expire active cycles that have exceeded the cycle life span
        expire_cycles
      end
    end

    ##########################################
    #
    # vacuum!
    #
    ##########################################
    def vacuum!(seconds)
      # with_locked_db
      with_locked_db do
        @db_server.vacuum(seconds)
      end
    end

    private



    ##########################################
    #
    # with_locked_db
    #
    ##########################################
    def with_locked_db
      # This locks the database and passes control to a code block,
      # and then unlocks the database afterwards, even on error.


      # Open/Create the database
      @db_server.dbopen

      # Acquire a lock on the workflow in the database
      @locked = @db_server.lock_workflow
      Process.exit(1) unless @locked

      # Set up an object to serve file stat info
      @workflow_io_server = WorkflowIOProxy.new(@db_server, @config, @options)
      ######################################
      #
      # Pass control to the code block
      #
      yield
      #
      ######################################
    rescue StandardError => e
      WorkflowMgr.stderr(e.message, 1)
      WorkflowMgr.log(e.message)
      if e.is_a?(ArgumentError) || e.is_a?(NameError) || e.is_a?(TypeError)
        WorkflowMgr.stderr(e.backtrace.join("\n"), 1)
        WorkflowMgr.log(e.backtrace.join("\n"))
      end
      Process.exit(1)
    ensure
      # Shut down the batch queue server if it is no longer needed
      # Skip if in dryrun mode since no server was launched
      if !(@bq_server.nil? || !@config.BatchQueueServer || WorkflowMgr.dryrun_mode?) && !@bq_server.running?
        uri = @bq_server.__drburi
        @bq_server.stop!
        @db_server.delete_bqservers([uri])
      end

      # Make sure we release the workflow lock in the database and shutdown the dbserver
      # Skip server shutdown if in dryrun mode since no server was launched
      unless @db_server.nil?
        @db_server.unlock_workflow if @locked
        @db_server.stop! if @config.DatabaseServer && !WorkflowMgr.dryrun_mode?
      end

      # Make sure to shut down the workflow file stat server
      # Skip if in dryrun mode since no server was launched
      if !@workflow_io_server.nil? && @config.WorkflowIOServer && !WorkflowMgr.dryrun_mode?
        @workflow_io_server.stop!
      end
    end

    ##########################################
    #
    # auto_vacuum
    #
    ##########################################
    def auto_vacuum
      # Never vacuum unless the workflow is locked!
      unless @locked
        WorkflowMgr.stderr("ERROR: auto_vacuum cannot be called unless the workflow is locked!  Exiting...", 0)
        WorkflowMgr.log("ERROR: auto_vacuum cannot be called unless the workflow is locked! Exiting...")
        Process.exit(1)
      end

      # Get the previous vacuum time
      last_vacuum = @db_server.load_vacuum_time

      # Vacuum if we haven't done so in the last 24 hours
      if (Time.now - last_vacuum) > 24 * 3600
        @db_server.vacuum(@config.VacuumPurgeDays * 24 * 3600)
        @db_server.store_vacuum_time(Time.now)
      end
    end

    ##########################################
    #
    # build_workflow
    #
    ##########################################
    def build_workflow
      # Open the workflow document, parse it, and validate it
      workflowdoc = WorkflowMgr.const_get("Workflow#{@config.WorkflowDocType}Doc").new(@options.workflowdoc,
                                                                                       @workflow_io_server, @config)

      # Get the realtime flag
      @realtime = workflowdoc.realtime?

      # Get the cycle life span
      @cyclelifespan = workflowdoc.cyclelifespan || WorkflowMgr.ddhhmmss_to_seconds("365:00:00:00")

      # Get the cyclethrottle
      @cyclethrottle = workflowdoc.cyclethrottle || 1

      # Get the corethrottle
      @corethrottle = workflowdoc.corethrottle || 9_999_999

      # Get the taskthrottle
      @taskthrottle = workflowdoc.taskthrottle || 9_999_999

      # Get the metatask taskthrottles
      @metatask_throttles = workflowdoc.metatask_throttles

      # Get the scheduler
      @bq_server = BQSProxy.new(workflowdoc.scheduler, @config, @options)

      # Add this scheduler to the bqserver database if needed (skip in dryrun mode)
      @db_server.add_bqservers([@bq_server.__drburi]) if @config.BatchQueueServer && !WorkflowMgr.dryrun_mode?

      # Get the log parameters
      @log_server = workflowdoc.log

      # Get the cycle defs
      @cycledefs = workflowdoc.cycledefs

      # Get the tasks
      @tasks = workflowdoc.tasks

      # Get the taskdep cycle offsets
      @taskdep_cycle_offsets = workflowdoc.taskdep_cycle_offsets

      # Warn use if any unsupported features are used:
      workflowdoc.features_supported?
    end

    ##########################################
    #
    # selection
    #
    ##########################################
    def selection
      @options.selection
    end

    ##########################################
    #
    # subset
    #
    ##########################################
    def subset
      if @subset.nil?
        @subset = selection.make_subset(@tasks, @cycledefs)
      end

      @subset
    end

    ##########################################
    #
    # selected_tasks
    #
    ##########################################
    def selected_tasks
      if @selected_tasks.nil?
        @selected_tasks = subset.collect_tasks { |task| task }
      end

      @selected_tasks
    end

    ##########################################
    #
    # selected_cycles
    #
    ##########################################
    def selected_cycles
      if @selected_cycles.nil?
        @selected_cycles = subset.collect_cycles { |task| task }
      end

      @selected_cycles
    end

    ##########################################
    #
    # load_active_cycles
    #
    ##########################################
    def load_active_cycles
      # Get active cycles from the database
      @active_cycles = @db_server.load_active_cycles
    end

    ##########################################
    #
    # fetch_new_cycles
    #
    ##########################################
    def fetch_new_cycles
      # Don't look for new cycles if the cyclethrottle is already satisfied
      return unless @cyclethrottle > @active_cycles.size

      # Activate new cycles
      newcycles = if @realtime
                    fetch_new_realtime_cycle
                  else
                    fetch_new_retro_cycles
                  end

      # Add new cycles to active cycle list and database
      unless newcycles.empty?

        # Add the new cycles to the database
        @db_server.add_cycles(newcycles)

        # Add the new cycles to the list of active cycles
        @active_cycles += newcycles

      end
    end

    ##########################################
    #
    # fetch_new_realtime_cycle
    #
    ##########################################
    def fetch_new_realtime_cycle
      # For realtime workflows, find the most recent cycle less than or equal to
      # the current time and activate it if it has not already been activated

      # Get the most recent cycle time <= now from cycle specs
      now = Time.now.getgm
      latest_cycle_time = nil
      latest_activation_time = nil
      latest_cycle_candidates = @cycledefs.collect { |c| c.previous(now, true) }.compact
      unless latest_cycle_candidates.empty?
        latest_cycle_time, latest_activation_time = latest_cycle_candidates.max_by { |c| c[1] }
      end

      # Create a new cycle if a cycle <= now is defined in cycle specs
      if latest_cycle_time.nil?
        return []
      else
        latest_cycle = Cycle.new(latest_cycle_time.getgm, { activated: latest_activation_time })
      end

      # Look for the lastest cycle in the database
      db_cycle = @db_server.load_cycle(latest_cycle_time)[0]

      # Return the new cycle if it hasn't already been activated
      if db_cycle.nil?
        [latest_cycle]
      else
        []
      end
    end

    ##########################################
    #
    # fetch_new_retro_cycles
    #
    ##########################################
    def fetch_new_retro_cycles
      # For retrospective workflows, find the next N cycles in chronological
      # order that have never been activated.  If any cycledefs have changed,
      # cycles may be returned that are older than previously activated cycles.
      # N is the cyclethrottle minus the number of currently active cycles.

      # Get the cycledefs from the database so that we can get their last known positions
      dbcycledefs = @db_server.load_cycledefs.collect do |dbcycledef|
        case dbcycledef[:cycledef].split.size
        when 6
          CycleCron.new(dbcycledef[:cycledef], dbcycledef[:group], dbcycledef[:activation_offset],
                        dbcycledef[:position])
        when 3
          CycleInterval.new(dbcycledef[:cycledef], dbcycledef[:group], dbcycledef[:activation_offset],
                            dbcycledef[:position])
        end
      end

      # Update the positions of the current cycledefs (loaded from the workflowdoc)
      # with their last known positions that are stored in the database
      @cycledefs.each do |cycledef|
        dbcycledef = dbcycledefs.find do |dbcycledef|
          dbcycledef.group == cycledef.group && dbcycledef.cycledef == cycledef.cycledef
        end
        next if dbcycledef.nil?

        cycledef.seek(dbcycledef.position)
      end

      # Get the set of cycles that are >= the earliest cycledef position
      cycleset = @db_server.load_cycles({ start: @cycledefs.collect(&:position).compact.min })

      # Sort the cycleset
      cycleset.sort!

      # Find N new cycles to be added
      newcycles = []
      (@cyclethrottle - @active_cycles.size).times do
        # Initialize the pool of new cycle candidates
        cyclepool = []

        # Get the next new cycle for each cycle spec, and add it to the cycle pool
        @cycledefs.each do |cycledef|
          # Start looking for new cycles at the last known position for the cycledef
          next_cycle = cycledef.position

          # Iterate through cycles until we find a cycle that has not ever been activated
          # or we have tried all the cycles represented by the cycledef
          until next_cycle.nil?
            match = cycleset.find { |c| c.cycle == next_cycle }
            break if match.nil?

            next_cycle, = cycledef.next(next_cycle + 60, by_activation_time: false)
          end

          # If we found a new cycle, add it to the new cycle pool
          cyclepool << next_cycle unless next_cycle.nil?

          # Update cycledef position
          cycledef.seek(next_cycle)
        end

        if cyclepool.empty?

          # If we didn't find any cycles that could be added, stop looking for more
          break

        else

          # The new cycle is the earliest cycle in the cycle pool
          newcycle = Cycle.new(cyclepool.min, { activated: Time.now.getgm })

          # Add the earliest cycle in the cycle pool to the list of cycles to activate
          newcycles << newcycle

          # Add the new cycle to the cycleset so that we don't try to add it again
          cycleset << newcycle

        end
      end

      # Save the workflowdoc cycledefs with their updated positions to the database
      @db_server.store_cycledefs(@cycledefs.collect do |cycledef|
        { group: cycledef.group, cycledef: cycledef.cycledef, activation_offset: cycledef.activation_offset,
          position: cycledef.position }
      end)

      newcycles
    end

    ##########################################
    #
    # fetch_active_jobs
    #
    ##########################################
    def fetch_active_jobs
      # Initialize a list of job cycles corresponding to the set of active jobs
      job_cycles = []

      # Get jobs for each active cycle and all cycle offsets from them
      @active_cycles.each do |active_cycle|
        # Add each active cycle to the active job cycles
        job_cycles << active_cycle.cycle

        # Add cycles for each known cycle offset in task dependencies
        # but only for cycles that have not just expired
        next unless Time.now - active_cycle.activated < @cyclelifespan

        @taskdep_cycle_offsets.each do |cycle_offset|
          job_cycles << active_cycle.cycle + cycle_offset
        end
      end

      # Remove duplicates
      job_cycles.uniq!

      # Get all jobs whose cycle is in the job_cycle list
      @active_jobs = @db_server.load_jobs(job_cycles)
    end

    ##########################################
    #
    # harvest_pending_jobids
    #
    ##########################################
    def harvest_pending_jobids
      # In dryrun mode, no DRb-backed BQServers exist and no real submissions
      # were made, so skip all pending-job harvesting and DB mutations.
      return if WorkflowMgr.dryrun_mode?

      # Initialize hash of old bqserver processes from the database and establish connections to them
      bqservers = {}
      @db_server.load_bqservers.each do |uri|
        # We are only interested in old bqserver processes
        # In dryrun mode, no DRb server is launched, so skip this check
        next if !WorkflowMgr.dryrun_mode? && uri == @bq_server.__drburi

        bqservers[uri] = DRbObject.new(nil, uri) unless bqservers.key?(uri)

      # The bqserver has died!
      rescue DRb::DRbConnError
        # Remove the bqserver uri from the database
        @db_server.delete_bqservers([uri])

        # Remove the bqserver uri from the bqservers list if needed
        bqservers.delete(uri) if bqservers.key?(uri)
      end

      begin
        # Loop over active jobs looking for ones with pending submissions
        sorted_jobs = @active_jobs.values.collect(&:values).flatten.sort_by do |job|
          [job.cycle,
           @tasks[job.task].nil? ? 999_999_999 : @tasks[job.task].seq]
        end
        sorted_jobs.each do |job|
          # Skip jobs that don't have pending job ids
          next unless job.pending_submit?

          # Get the URI of the workflowbqserver that submitted the job
          uri = job.id

          begin
            # Query the workflowbqserver for the status of the job submission
            jobid, output = bqservers[uri].get_submit_status(job.task, job.cycle) if bqservers.key?(uri)

          # Catch exceptions for bqservers that have died unexpectedly
          rescue DRb::DRbConnError
            # Remove the bqserver uri from the database
            @db_server.delete_bqservers([uri])

            # Remove the bqserver uri from the bqservers list if needed
            bqservers.delete(uri) if bqservers.key?(uri)
          end

          # If the bqserver died, warn user, resubmit job
          if !bqservers.key?(uri)

            # Log the fact that the submission status could not be retrieved
            msg = "Submission status of #{job.task} for cycle #{job.cycle.strftime('%Y%m%d%H%M')} could not be " \
                  "retrieved because the server process at #{uri} died"
            WorkflowMgr.stderr(msg, 2)
            @log_server.log(job.cycle, msg)
            msg = "Submission of #{job.task} for cycle #{job.cycle.strftime('%Y%m%d%H%M')} probably, but not " \
                  "necessarily, failed.  It will be resubmitted"
            WorkflowMgr.stderr(msg, 2)
            @log_server.log(job.cycle, msg)

            # Delete the job from the database since it failed to submit.  It will be retried immediately.
            @db_server.delete_jobs([job])

            # Remove the job from the active_jobs list since it failed to submit and is not active.
            @active_jobs[job.task].delete(job.cycle)
            @active_jobs.delete(job.task) if @active_jobs[job.task].empty?

            next

          # If there is no output from the submission, it means the submission is still pending
          elsif output.nil?
            @log_server.log(job.cycle,
                            "Submission status of #{job.task} is still pending at #{uri}.  The batch system " \
                            "server may be down, unresponsive, or under heavy load.")

          # Otherwise, the submission either succeeded or failed.
          elsif jobid.nil?

            # If the job submission failed, log the output of the job submission command, and print it to stdout as well
            @db_server.delete_jobs([job])

            # Remove the job from the active_jobs list since it failed to submit and is not active.
            @active_jobs[job.task].delete(job.cycle)
            @active_jobs.delete(job.task) if @active_jobs[job.task].empty?

            WorkflowMgr.stderr(output, 1)
            @log_server.log(job.cycle, "Submission status of previously pending #{job.task} is failure!  #{output}")

            next

            # Delete the job from the database since it failed to submit.  It will be retried immediately.

            # If the job succeeded, record the jobid and log it
          else
            job.id = jobid
            @log_server.log(job.cycle,
                            "Submission status of previously pending #{job.task} is success, jobid=#{jobid}")

          end

          # Update the job in the database
          @db_server.update_jobs([job])
        end
      ensure
        # Make sure we always terminate all workflowbqservers that we no longer need
        bqservers.each do |uri, bqserver|
          unless bqserver.running?
            bqserver.stop!
            @db_server.delete_bqservers([uri])
          end
        # Catch exceptions for bqservers that have died unexpectedly
        rescue DRb::DRbConnError
          msg = "WARNING! BQS Server process at #{uri} died unexpectedly.  " \
                "Submission status of some jobs may have been lost"
          WorkflowMgr.stderr(msg, 2)
          WorkflowMgr.log(msg)
          @db_server.delete_bqservers([uri])
        end
      end
    end

    ##########################################
    #
    # update_active_jobs
    #
    ##########################################
    def update_active_jobs
      # Harvest job ids from pending job submissions
      harvest_pending_jobids

      wstate = nil

      begin
        # Initialize array of jobs whose job ids have been updated

        # Initialize counters for keeping track of active workflow parameters
        @active_task_count = 0
        @active_core_count = 0
        @active_task_instance_count = {}
        @active_metatask_instance_count = {}

        # Get a sorted list of active jobs
        active_jobs_sorted = @active_jobs.values.collect(&:values).flatten.sort_by do |job|
          [job.cycle, @tasks[job.task].nil? ? 999_999_999 : @tasks[job.task].seq]
        end

        # Reject jobs whose state is "SUCCEEDED", "FAILED", "EXPIRED", or "LOST" or are awaiting submit status results
        # No need to query or update the status of jobs that we already know are done successfully or that remain failed
        # If a job is failed at this point, it could only be because the WFM crashed before a
        # resubmit or state update could occur
        # No point in trying to update the status of jobs with pending submission status
        active_jobs_sorted.reject! do |job|
          job.state == "SUCCEEDED" || job.state == "FAILED" || job.state == "EXPIRED" ||
            job.state == "LOST" || job.pending_submit?
        end

        # Check if DEAD jobs need to be resurrected
        active_jobs_sorted.each do |job|
          # Resurrect DEAD tasks if the user increased the task maxtries sufficiently to enable more attempts,
          # but only if the task is still defined and has not expired
          next unless job.state == "DEAD"

          # Can't resurrect a task that is no longer defined in the XML
          next if @tasks[job.task].nil?

          if job.tries < @tasks[job.task].attributes[:maxtries] && !@tasks[job.task].expired?(job.cycle)

            # Reset the state to FAILED so a resubmission can occur
            job.state = "FAILED"

            # Update the state of the job in the database
            @db_server.update_jobs([job])

            # Log the fact that this job was resurrected
            @log_server.log(job.cycle,
                            "Task #{job.task} has been resurrected.  " \
                            "#{@tasks[job.task].attributes[:maxtries] - job.tries} more tries will be allowed")
          end

          # No need for more updates to this job
          next
        end

        # Reject jobs whose state was just changed from "DEAD" to "FAILED" because they were resurrected,
        # they don't require further updating here
        # Reject all "DEAD" jobs that were not resurrected
        active_jobs_sorted.reject! { |job| ["FAILED", "DEAD"].include?(job.state) }

        # Get the status of ALL active jobs from the batch system
        statuses = @bq_server.statuses(active_jobs_sorted.collect(&:id))

        # Loop over all active jobs and retrieve and update their current status
        active_jobs_sorted.each do |job|
          # Update the state of the job with its current state
          job.state = statuses[job.id][:state]
          job.native_state = statuses[job.id][:native_state]
          if ["SUCCEEDED", "FAILED"].include?(statuses[job.id][:state])
            job.exit_status = statuses[job.id][:exit_status]
            job.duration = if !statuses[job.id][:duration].nil?
                             statuses[job.id][:duration]
                           elsif statuses[job.id][:start_time].nil?
                             0
                           elsif statuses[job.id][:start_time] == Time.at(0).getgm
                             0
                           else
                             statuses[job.id][:end_time] - statuses[job.id][:start_time]
                           end
            runmsg = ", ran for #{job.duration} seconds, exit status=#{statuses[job.id][:exit_status]}"
          else
            runmsg = ""
          end

          # Check for recurring state of UNKNOWN
          if job.state == "UNKNOWN"

            # Increment unknown counter
            job.nunknowns += 1

            # Assume the job failed if too many consecutive UNKNOWNS
            unknownmsg = ""
            if job.nunknowns >= @config.MaxUnknowns
              job.state = "LOST"
              unknownmsg += ", giving up because job state could not be determined #{job.nunknowns} consecutive times"
            end

          else
            # Reset unknown counter to zero if not in UNKNOWN state
            job.nunknowns = 0
            unknownmsg = ""
          end

          # Can't check for hangs/expiration for jobs whose task is no longer defined
          unless @tasks[job.task].nil?

            # Check for job hang
            if !@tasks[job.task].hangdependency.nil? && (job.state == "RUNNING")
              wstate = WorkflowState.new(job.cycle, @active_jobs, @workflow_io_server, @cycledefs,
                                         job.task, @tasks[job.task], @tasks)
              if @tasks[job.task].hangdependency.resolved?(wstate)
                job.state = "FAILED"
                runmsg = ".  A job hang has been detected.  The job will be killed.  " \
                         "It will be resubmitted if the retry count has not been exceeded."
                @bq_server.delete(job.id)
              end
            end

            # Check for job expiration
            if job.state != "SUCCEEDED" && @tasks[job.task].expired?(job.cycle)
              job.state = "EXPIRED"
              runmsg = "#{runmsg}.  This task has expired.  It will be killed and will not be retried"
              @bq_server.delete(job.id)
            end

          end

          # Check for maxtries violation and update counters
          if ["SUCCEEDED", "FAILED", "EXPIRED", "LOST"].include?(job.state)
            job.tries += 1
            maxtries = @tasks[job.task].nil? ? job.tries : @tasks[job.task].attributes[:maxtries]
            if ["FAILED", "LOST"].include?(job.state) && (job.tries >= maxtries)
              job.state = "DEAD"
            end
            triesmsg = ", try=#{job.tries} (of #{maxtries})"
          else
            # Update counters for jobs that are still QUEUED, RUNNING, or UNKNOWN
            @active_task_count += 1
            @active_core_count += job.cores
            if @active_task_instance_count[job.task].nil?
              @active_task_instance_count[job.task] = 1
            else
              @active_task_instance_count[job.task] += 1
            end
            unless @tasks[job.task].nil? || @tasks[job.task].attributes[:metatasks].nil?
              @tasks[job.task].attributes[:metatasks].split(",").each do |metatask|
                if @active_metatask_instance_count[metatask].nil?
                  @active_metatask_instance_count[metatask] = 1
                else
                  @active_metatask_instance_count[metatask] += 1
                end
              end
            end
            triesmsg = ""
          end

          statemsg = "Task #{job.task}, jobid=#{job.id}, in state #{job.state} (#{job.native_state})"

          # Update the job state in the database
          @db_server.update_jobs([job])

          # Log the state of the job
          @log_server.log(job.cycle, statemsg + runmsg + unknownmsg + triesmsg)

          if job.dead? || job.expired?
            WorkflowMgr.stderr(
              "Cycle #{job.cycle.strftime('%Y%m%d%H%M')}, #{statemsg + runmsg + unknownmsg + triesmsg}", 1
            )
          elsif job.failed?
            WorkflowMgr.stderr(
              "Cycle #{job.cycle.strftime('%Y%m%d%H%M')}, #{statemsg + runmsg + unknownmsg + triesmsg}", 3
            )
          end
        end
      end
    end

    ##########################################
    #
    #  deactivate_done_cycles
    #
    ##########################################
    def deactivate_done_cycles
      active_cycles = []
      done_cycles = []

      # Initialize a hash of task cycledefs
      taskcycledefs = {}

      # Partition the workflow tasks by value of the final attribute and sort
      final_tasks = []
      non_final_tasks = []
      unless @active_cycles.empty?
        final_tasks, non_final_tasks = @tasks.values.partition { |t| t.attributes[:final] }

        # Sort final tasks in sequential order to increase chances of finding completed final task quickly
        final_tasks.sort! { |t1, t2| t1.seq <=> t2.seq }

        # Sort non final tasks in reverse sequential order to increase chances of finding incompleted task quickly
        non_final_tasks.sort! { |t1, t2| t2.seq <=> t1.seq }
      end

      # Loop over all active cycles
      @active_cycles.each do |cycle|
        # Drain the cycle if a "final" task has completed
        unless cycle.draining?

          final_tasks.each do |task|
            # Validate that this cycle is a member of at least one of the cycledefs specified for this task
            unless task.attributes[:cycledefs].nil?

              # Get the cycledefs associated with this task
              if taskcycledefs[task].nil?
                taskcycledefs[task] = @cycledefs.find_all do |cycledef|
                  task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
                end
              end

              # Reject this task if the cycle is not a member of the tasks cycle list
              next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle.cycle) }

            end

            # Skip to next final task if this task has not been submitted yet for any cycle
            next if @active_jobs[task.attributes[:name]].nil?

            # Skip to next final task if this task has not been submitted yet for this active cycle
            next if @active_jobs[task.attributes[:name]][cycle.cycle].nil?

            # The cycle needs to drain if this task is successful
            next unless @active_jobs[task.attributes[:name]][cycle.cycle].state == "SUCCEEDED"

            cycle.drain!
            @log_server.log(cycle.cycle, "This cycle is draining")

            # Update the draining cycle in the database
            @db_server.update_cycles([cycle])

            break
          end

        end

        # Initialize done flag to false for this cycle
        cycle_done = false
        cycle_success = true

        catch(:not_done) do
          # Loop over all final tasks
          (final_tasks + non_final_tasks).each do |task|
            # Validate that this cycle is a member of at least one of the cycledefs specified for this task
            unless task.attributes[:cycledefs].nil?

              # Get the cycledefs associated with this task
              if taskcycledefs[task].nil?
                taskcycledefs[task] = @cycledefs.find_all do |cycledef|
                  task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
                end
              end

              # Reject this task if the cycle is not a member of the tasks cycle list
              next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle.cycle) }

            end

            if cycle.draining?

              # A draining cycle is not done if any task is still running
              next if @active_jobs[task.attributes[:name]].nil?
              next if @active_jobs[task.attributes[:name]][cycle.cycle].nil?

              throw :not_done if @active_jobs[task.attributes[:name]][cycle.cycle].state == "RUNNING"

            else

              # An active cycle is not done if this task has not been submitted yet for any of the active cycles
              throw :not_done if @active_jobs[task.attributes[:name]].nil?

              # An active cycle is not done if this task has not been submitted yet for this cycle
              throw :not_done if @active_jobs[task.attributes[:name]][cycle.cycle].nil?

              # An active cycle is not done if the job for this task and cycle is not in the done state
              throw :not_done if @active_jobs[task.attributes[:name]][cycle.cycle].state != "SUCCEEDED"

              # For now, only tag cycles as done if they are done successfully, meaning that all tasks are
              # complete and have exit status = 0.
              # If we mark cycles as done when they have tasks that exceeded retries, then increasing
              # retries won't cause them to rerun again
              #
              #            # The cycle is not done if the job for this task and cycle is done, but has
              #            # crashed and has not yet exceeded the retry count
              #            if @active_jobs[task.attributes[:name]][cycle.cycle].tries >= task.attributes[:maxtries]
              #              cycle_success=false
              #            else
              #              throw :not_done if @active_jobs[task.attributes[:name]][cycle.cycle].exit_status != 0
              #            end

            end
          end

          cycle_done = true
        end

        # If the cycle is done, record the time and update active cycle list
        if cycle_done

          # Mark the cycle as done
          cycle.done!

          # Add to list of done cycles
          done_cycles << cycle

          # Log the done status of this cycle
          if cycle_success
            @log_server.log(cycle.cycle, "This cycle is complete: Success")
          else
            @log_server.log(cycle.cycle, "This cycle is complete: Failed")
          end

          # Update the done cycle in the database
          @db_server.update_cycles([cycle])

          # Otherwise add the cycle to a new list of active cycles
        else
          active_cycles << cycle
        end
      end

      # Update the active cycle list
      @active_cycles = active_cycles
    end

    ##########################################
    #
    # expire_cycles
    #
    ##########################################
    def expire_cycles
      active_cycles = []
      expired_cycles = []

      # Loop over all active cycles
      @active_cycles.each do |cycle|
        # If the cycle has expired, mark it, and delete jobs if necessary
        if Time.now.getgm - cycle.activated.getgm > @cyclelifespan

          # Set the expiration time
          cycle.expire!

          # Add to list of expired cycles
          expired_cycles << cycle

        # Otherwise add the cycle to a new list of active cycles
        else
          active_cycles << cycle
        end
      end

      # Delete any jobs for the expired cycles
      expired_cycles.each do |cycle|
        @active_jobs.each_key do |taskname|
          next if @active_jobs[taskname][cycle.cycle].nil?

          next if ["SUCCEEDED", "FAILED", "DEAD", "EXPIRED",
                   "SUBMITTING"].include?(@active_jobs[taskname][cycle.cycle].state)

          @log_server.log(cycle.cycle,
                          "Deleting #{taskname} job #{@active_jobs[taskname][cycle.cycle].id} " \
                          "because this cycle has expired!")
          @bq_server.delete(@active_jobs[taskname][cycle.cycle].id)
        end

        @log_server.log(cycle.cycle, "This cycle has expired!")

        # Update the expired cycles in the database
        @db_server.update_cycles([cycle])
      end

      # Update the active cycle list
      @active_cycles = active_cycles
    end

    ##########################################
    #
    # submit_new_jobs
    #
    ##########################################
    def submit_new_jobs
      # Initialize an array of the new jobs that have been submitted
      newjobs = []

      # Initialize a hash of task cycledefs
      taskcycledefs = {}

      # Loop over active cycles and tasks, looking for eligible tasks to submit
      @active_cycles.sort { |c1, c2| c1.cycle <=> c2.cycle }.each do |cycle|
        if !@options.all_cycles && !subset.selected?(cycle)
          WorkflowMgr.stderr("#{cycle.cycle.strftime('%Y%m%d%H%M')}: cycle is not selected by -c; skip", 4)
          next
        end

        # Don't submit jobs for draining cycles
        next if cycle.draining?

        cycletime = cycle.cycle
        @tasks.values.sort { |t1, t2| t1.seq <=> t2.seq }.each do |task|
          if !@options.all_tasks && !subset.selected?(task)
            WorkflowMgr.stderr("#{task.attributes[:name]}: task is not selected by -m, -t, or -a; skip", 9)
            next
          end

          # Make sure the task is eligible for submission
          resubmit = false
          if !@active_jobs[task.attributes[:name]].nil? && !@active_jobs[task.attributes[:name]][cycletime].nil?

            # Since this task has already been submitted at least once, reject it unless the job for it
            # has failed or was lost
            next unless ["FAILED", "LOST"].include?(@active_jobs[task.attributes[:name]][cycletime].state)

            # This task is a resubmission
            resubmit = true

          end

          # Make sure the task hasn't expired.  If it has, add a fake EXPIRED job to the DB so that
          # we won't try this again.
          if task.expired?(cycletime)
            @log_server.log(cycletime, "Cannot submit #{task.attributes[:name]}, because it has expired")
            fakejob = Job.new(0,                        # jobid
                              task.attributes[:name],   # taskname
                              cycletime,                # cycle
                              task.attributes[:cores],  # cores
                              "EXPIRED",                # state
                              "SUBMITTING",             # native state
                              0,                        # exit_status
                              0,                        # tries
                              0,                        # nunknowns
                              0.0) # duration
            @db_server.add_jobs([fakejob])
            next
          end

          # Validate that this cycle is a member of at least one of the cycledefs specified for this task
          unless task.attributes[:cycledefs].nil?

            # Get the cycledefs associated with this task
            if taskcycledefs[task].nil?
              taskcycledefs[task] = @cycledefs.find_all do |cycledef|
                task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
              end
            end

            # Reject this task if the cycle is not a member of the tasks cycle list
            next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycletime) }

          end

          # Reject this task if dependencies are not satisfied
          unless task.dependency.nil?
            wstate = WorkflowState.new(cycletime, @active_jobs, @workflow_io_server, @cycledefs,
                                       task.attributes[:name], task, @tasks)
            next unless task.dependency.resolved?(wstate)
          end

          # Reject this task if core throttle will be exceeded
          if @active_core_count + task.attributes[:cores] > @corethrottle
            @log_server.log(cycletime,
                            "Cannot submit #{task.attributes[:name]}, because maximum core throttle of " \
                            "#{@corethrottle} will be violated.", 2)
            next
          end

          # Reject this task if task throttle will be exceeded
          if @active_task_count + 1 > @taskthrottle
            @log_server.log(cycletime,
                            "Cannot submit #{task.attributes[:name]}, because maximum global task throttle of " \
                            "#{@taskthrottle} will be violated.", 2)
            next
          end

          # Reject this task if task instance throttle has been exceeded
          if @active_task_instance_count[task.attributes[:name]].nil?
            @active_task_instance_count[task.attributes[:name]] =
              0
          end
          if @active_task_instance_count[task.attributes[:name]] + 1 > task.attributes[:throttle]
            @log_server.log(cycletime,
                            "Cannot submit #{task.attributes[:name]}, because maximum task instance throttle of " \
                            "#{task.attributes[:throttle]} will be violated.", 2)
            next
          end

          # Reject this task if a metatask instance throttle will be exceeded
          unless task.attributes[:metatasks].nil?
            violation = false
            catch(:violation) do
              task.attributes[:metatasks].split(",").each do |metatask|
                @active_metatask_instance_count[metatask] = 0 if @active_metatask_instance_count[metatask].nil?
                mthrottle = @metatask_throttles[metatask]
                next unless !mthrottle.nil? && (@active_metatask_instance_count[metatask] + 1 > mthrottle)

                violation = true
                @log_server.log(cycletime,
                                "Cannot submit #{task.attributes[:name]}, because maximum metatask throttle of " \
                                "#{@metatask_throttles[metatask]} will be violated.", 2)
                throw :violation
              end
            end
            next if violation
          end

          # Reject this task if retries has been exceeded
          # This code block should never execute since state should be DEAD if retries is exceeded and
          # we should never get here for a DEAD job
          if resubmit && (@active_jobs[task.attributes[:name]][cycletime].tries >= task.attributes[:maxtries])
            @log_server.log(cycletime,
                            "Cannot resubmit #{task.attributes[:name]}, maximum retry count of " \
                            "#{task.attributes[:maxtries]} has been reached")
            next
          end

          # Increment counters
          @active_core_count += task.attributes[:cores]
          @active_task_count += 1
          @active_task_instance_count[task.attributes[:name]] += 1
          task.attributes[:metatasks]&.split(",")&.each do |metatask|
            @active_metatask_instance_count[metatask] += 1
          end

          # If we are resubmitting the job, initialize the new job to the old job
          # In dryrun mode, no DRb server is launched, so use 0 as placeholder
          newjobid = if @config.BatchQueueServer && !WorkflowMgr.dryrun_mode?
                       @bq_server.__drburi
                     else
                       0
                     end
          newjob = if resubmit
                     Job.new(newjobid, # jobid
                             task.attributes[:name],   # taskname
                             cycletime,                # cycle
                             task.attributes[:cores],  # cores
                             "SUBMITTING",             # state
                             "SUBMITTING",             # native state
                             0,                        # exit_status
                             @active_jobs[task.attributes[:name]][cycletime].tries, # tries
                             0, # nunknowns
                             0.0) # duration

                   else
                     Job.new(newjobid, # jobid
                             task.attributes[:name],   # taskname
                             cycletime,                # cycle
                             task.attributes[:cores],  # cores
                             "SUBMITTING",             # state
                             "SUBMITTING",             # native state
                             0,                        # exit_status
                             0,                        # tries
                             0,                        # nunknowns
                             0.0) # duration
                   end


          # Append the new job to the list of new jobs that were submitted
          newjobs << newjob

          # Add the new job to the database (skip in dryrun to avoid persistent side effects)
          @db_server.add_jobs([newjob]) unless WorkflowMgr.dryrun_mode?

          # Localize all <cyclestr> to current cycle
          localtask = task.localize(cycletime)

          # Cap walltime requests such that the runtime of the task won't exceed expiration deadlines
          localtask.cap_walltime(cycle.activated.getgm + @cyclelifespan)

          # Create output directories for <stdout>,<stderr>,<join> paths
          begin
            outdir = ""
            localtask.attributes.each do |option, value|
              case option
              when :stdout, :stderr, :join
                if value[-1, 1] == "/"
                  outdir = value
                else
                  outdir = value.split("/")[0..-2].join("/")
                  # Roll the log file (if it already exists)
                  @workflow_io_server.roll_log(value)
                end
                @workflow_io_server.mkdir_p(outdir)
              end
            end
          rescue WorkflowIOHang
            msg = "WARNING! Can not submit #{task.attributes[:name]} because output directory " \
                  "'#{outdir}' resides on an unresponsive file system!"
            @log_server.log(cycletime, msg)
            WorkflowMgr.stderr(msg, 2)
            WorkflowMgr.log(msg)
          end

          # Submit the task
          @bq_server.submit(localtask, cycletime)
          # In dryrun mode the per-job "Dryrun Mode: would submit ..." line below
          # is the single source of truth; suppress this "Submitting" line so the
          # workflow log is not ambiguous about whether a job was actually submitted.
          unless WorkflowMgr.dryrun_mode?
            @log_server.log(cycletime, "Submitting #{task.attributes[:name]}")
          end
        end
      end

      # If we are not using a batch queue server, shut down the pool, which blocks until all
      # queued submissions have finished running, before checking for job ids.
      # Skip in dryrun mode - thread pool workers sleep indefinitely waiting for work and cause deadlock
      unless @config.BatchQueueServer || WorkflowMgr.dryrun_mode?
        @bq_server.shutdown
      end

      # Harvest job ids for submitted tasks
      newjobs.each do |job|
        job.id
        jobid, output = @bq_server.get_submit_status(job.task, job.cycle)
        # Classify submission outcome: dryrun first, then pending, failure, success
        # Dryrun returns [nil, "This is a dryrun"] so output is non-nil;
        # checking output.nil? first would mis-classify dryrun as failure.
        if WorkflowMgr.dryrun_mode?
          @log_server.log(job.cycle, "Dryrun Mode: would submit #{job.task}")
        elsif output.nil?
          @log_server.log(job.cycle, "Submission status of #{job.task} is pending at #{job.id}")
        elsif jobid.nil?
          # Delete the job from the database since it failed to submit.  It will be retried next time around.
          @db_server.delete_jobs([job])
          msg = "Submission of #{job.task} failed!  #{output}"
          @log_server.log(job.cycle, msg)
          WorkflowMgr.stderr(msg, 1)
        else
          job.id = jobid
          job.state = "QUEUED"
          job.native_state = "queued"
          @log_server.log(job.cycle, "Submission of #{job.task} succeeded, jobid=#{job.id}")
          # Update the jobid for the job in the database
          @db_server.update_jobs([job])
        end
      end
    end
  end
end
