##########################################
#
# Module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowReport
  #
  ##########################################
  class WorkflowReport
    require 'drb'
    require 'workflowmgr/workflowconfig'
    require 'workflowmgr/reportoption'
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
    def initialize(args)
      # Get command line options
      @options = ReportOption.new(args)

      # Get configuration file options
      @config = WorkflowYAMLConfig.new

      # Get the base directory of the WFM installation
      @wfmdir = File.dirname(File.dirname(__dir__))

      # Set up an object to serve the workflow database (but do not open the database)
      @db_server = DBProxy.new(@config, @options)

      # Initialize the workflow lock
      @locked = false
    rescue StandardError
      puts $ERROR_INFO
      Process.exit(1)
    end

    ##########################################
    #
    # show
    #
    ##########################################
    def show; end

    ##########################################
    #
    # run
    #
    ##########################################
    def run
      # Open/Create the database
      @db_server.dbopen

      # Acquire a lock on the workflow in the database
      @locked = @db_server.lock_workflow
      Process.exit(0) unless @locked

      # Set up an object to serve file stat info
      @workflow_io_server = WorkflowIOProxy.new(@db_server, @config)

      # Build the workflow objects from the contents of the workflow document
      build_workflow

      # Get the active cycles, including any new ones that need to be activated now
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

      # Submit new tasks where possible
      submit_new_jobs
    rescue StandardError
      puts $ERROR_INFO
      Process.exit(1)
    ensure
      # Shut down the batch queue server if it is no longer needed
      # Skip daemon cleanup in dryrun mode — BQServer is in-process without DRb
      if !(@bq_server.nil? || !@config.BatchQueueServer || WorkflowMgr.dryrun_mode?) && !@bq_server.running?
        uri = @bq_server.__drburi
        @bq_server.stop!
        @db_server.delete_bqservers([uri])
      end

      # Make sure we release the workflow lock in the database and shutdown the dbserver
      unless @db_server.nil?
        @db_server.unlock_workflow if @locked
        @db_server.stop! if @config.DatabaseServer && !WorkflowMgr.dryrun_mode?
      end

      # Make sure to shut down the workflow file stat server
      if !@workflow_io_server.nil? && @config.WorkflowIOServer && !WorkflowMgr.dryrun_mode?
        @workflow_io_server.stop!
      end
    end

    private

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

      # Get the scheduler
      @bq_server = BQSProxy.new(workflowdoc.scheduler, @config, @options)

      # Add this scheduler to the bqserver database if needed (skip in dryrun — no DRb URI)
      @db_server.add_bqservers([@bq_server.__drburi]) if @config.BatchQueueServer && !WorkflowMgr.dryrun_mode?

      # Get the log parameters
      @log_server = workflowdoc.log

      # Get the cycle defs
      @cycledefs = workflowdoc.cycledefs

      # Get the tasks
      @tasks = workflowdoc.tasks

      # Get the taskdep cycle offsets
      @taskdep_cycle_offsets = workflowdoc.taskdep_cycle_offsets
    end

    ##########################################
    #
    # load_active_cycles
    #
    ##########################################
    def load_active_cycles
      # Get active cycles from the database
      @active_cycles = @db_server.load_active_cycles(@cyclelifespan)

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
        @active_cycles += newcycles.collect { |cycle| { cycle: cycle, activated: Time.now.getgm } }

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
      cycleset = @db_server.load_cycles(@cycledefs.collect(&:position).compact.min)

      # Sort the cycleset
      cycleset.sort { |a, b| a[:cycle] <=> b[:cycle] }

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
            match = cycleset.find { |c| c[:cycle] == next_cycle }
            break if match.nil?

            next_cycle, = cycledef.next(next_cycle + 60, false)
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

          # The new cycle is the earlies cycle in the cycle pool
          newcycle = cyclepool.min

          # Add the earliest cycle in the cycle pool to the list of cycles to activate
          newcycles << newcycle

          # Add the new cycle to the cycleset so that we don't try to add it again
          cycleset << { cycle: newcycle }

        end
      end

      # Save the workflowdoc cycledefs with their updated positions to the database
      @db_server.store_cycledefs(@cycledefs.collect do |cycledef|
        { group: cycledef.group, cycledef: cycledef.cycledef, position: cycledef.position }
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
        job_cycles << active_cycle[:cycle]

        # Add cycles for each known cycle offset in task dependencies
        # but only for cycles that have not just expired
        next unless Time.now - active_cycle[:activated] < @cyclelifespan

        @taskdep_cycle_offsets.each do |cycle_offset|
          job_cycles << active_cycle[:cycle] + cycle_offset
        end
      end

      # Get all jobs whose cycle is in the job_cycle list
      @active_jobs = @db_server.load_jobs(job_cycles)
    end

    ##########################################
    #
    # harvest_pending_jobids
    #
    ##########################################
    def harvest_pending_jobids
      # Initialize hash of old bqserver processes from the database and establish connections to them
      # Skip BQServer management in dryrun mode — no DRb-backed servers exist
      bqservers = {}
      unless WorkflowMgr.dryrun_mode?
        @db_server.load_bqservers.each do |uri|
          # We are only interested in old bqserver processes
          next if uri == @bq_server.__drburi

          bqservers[uri] = DRbObject.new(nil, uri) unless bqservers.key?(uri)

        # The bqserver has died!
        rescue DRb::DRbConnError
          # Remove the bqserver uri from the database
          @db_server.delete_bqservers([uri])

          # Remove the bqserver uri from the bqservers list if needed
          bqservers.delete(uri) if bqservers.key?(uri)
        end
      end

      begin
        # Loop over active jobs looking for ones with pending submissions
        @active_jobs.each_key do |taskname|
          @active_jobs[taskname].each_key do |cycle|
            # Skip jobs that are not in the submiting state
            next unless @active_jobs[taskname][cycle][:state] == "SUBMITTING"

            # If the jobid is a DRb URI, retrieve the job submission status from the
            # workflowbqserver process that submitted it
            next unless @active_jobs[taskname][cycle][:jobid] =~ /^druby:/

            # Get the URI of the workflowbqserver that submitted the job
            uri = @active_jobs[taskname][cycle][:jobid]

            begin
              # Query the workflowbqserver for the status of the job submission
              jobid, output = bqservers[uri].get_submit_status(taskname, cycle) if bqservers.key?(uri)

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
              puts "Submission status of #{taskname} could not be retrieved because the server process at #{uri} died"
              puts "Submission of #{taskname} probably, but not necessarily, failed.  It will be resubmitted"
              @log_server.log(cycle,
                              "Submission status of #{taskname} could not be retrieved because " \
                              "the server process at #{uri} died")
              @log_server.log(cycle,
                              "Submission of #{taskname} probably, but not necessarily, failed. " \
                              "It will be resubmitted")

            # If there is no output from the submission, it means the submission is still pending
            elsif output.nil?
              @log_server.log(cycle,
                              "Submission status of #{taskname} is still pending at #{uri}.  The batch system " \
                              "server may be down, unresponsive, or under heavy load.")

            # Otherwise, the submission either succeeded or failed.
            elsif jobid.nil?

              # If the job submission failed, log the output of the job submission command, and
              # print it to stdout as well
              @db_server.delete_jobs([@active_jobs[taskname][cycle]])

              # Remove the job from the active_jobs list since it failed to submit and is not active.
              @active_jobs[taskname].delete(cycle)

              puts output
              @log_server.log(cycle, "Submission status of previously pending #{taskname} is failure!  #{output}")

              next

              # Delete the job from the database since it failed to submit.  It will be retried immediately.

              # If the job succeeded, record the jobid and log it
            else
              @active_jobs[taskname][cycle][:jobid] = jobid
              @log_server.log(cycle,
                              "Submission status of previously pending #{taskname} is success, jobid=#{jobid}")

            end

            # Update the job in the database
            @db_server.update_jobs([@active_jobs[taskname][cycle]])

            # if jobid matches /^druby:/
          end
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
          puts "WARNING: BQS Server process at #{uri} died unexpectedly.  " \
               "Submission status of some jobs may have been lost"
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

      begin
        # Initialize array of jobs whose job ids have been updated

        # Initialize counters for keeping track of active workflow parameters
        @active_task_count = 0
        @active_core_count = 0

        # Loop over all active jobs and retrieve and update their current status
        @tasks.each do |task|
          taskname = task.attributes[:name]
          next if @active_jobs[taskname].nil?

          @active_jobs[taskname].each_key do |cycle|
            # No need to query or update the status of jobs that we already know are done
            # successfully or that remain failed
            # If a job is failed at this point, it could only be because the WFM crashed before
            # a resubmit or state update could occur
            next if ["SUCCEEDED", "FAILED"].include?(@active_jobs[taskname][cycle][:state])

            # Resurrect DEAD tasks if the user increased the task maxtries sufficiently to enable more attempts
            if @active_jobs[taskname][cycle][:state] == "DEAD"
              if @active_jobs[taskname][cycle][:tries] < task.attributes[:maxtries]

                # Reset the state to FAILED so a resubmission can occur
                @active_jobs[taskname][cycle][:state] = "FAILED"

                # Update the state of the job in the database
                @db_server.update_jobs([@active_jobs[taskname][cycle]])

                # Log the fact that this job was resurrected
                @log_server.log(cycle,
                                "Task #{taskname} has been resurrected.  " \
                                "#{task.attributes[:maxtries] - @active_jobs[taskname][cycle][:tries]} " \
                                "more tries will be allowed")
              end

              # No need for more updates to this job
              next

            end

            # Get the status of the job from the batch system
            status = @bq_server.status(@active_jobs[taskname][cycle][:jobid])

            # Update the state of the job with its current state
            @active_jobs[taskname][cycle][:state] = status[:state]
            @active_jobs[taskname][cycle][:native_state] = status[:native_state]
            if ["SUCCEEDED", "FAILED"].include?(status[:state])
              @active_jobs[taskname][cycle][:exit_status] = status[:exit_status]
              duration = if status[:start_time] == Time.at(0).getgm
                           0
                         else
                           status[:end_time] - status[:start_time]
                         end
              runmsg = ", ran for #{duration} seconds, exit status=#{status[:exit_status]}"
            else
              runmsg = ""
            end

            # Check for recurring state of UNKNOWN
            if @active_jobs[taskname][cycle][:state] == "UNKNOWN"

              # Increment unknown counter
              @active_jobs[taskname][cycle][:nunknowns] += 1

              # Assume the job failed if too many consecutive UNKNOWNS
              unknownmsg = ""
              if @active_jobs[taskname][cycle][:nunknowns] >= @config.MaxUnknowns
                @active_jobs[taskname][cycle][:state] = "FAILED"
                unknownmsg += ", giving up because job state could not be determined " \
                               "#{@active_jobs[taskname][cycle][:nunknowns]} consecutive times"
              end

            else
              # Reset unknown counter to zero if not in UNKNOWN state
              @active_jobs[taskname][cycle][:nunknowns] = 0
              unknownmsg = ""
            end

            # Check for maxtries violation and update counters
            if ["SUCCEEDED", "FAILED"].include?(@active_jobs[taskname][cycle][:state])
              @active_jobs[taskname][cycle][:tries] += 1
              if (@active_jobs[taskname][cycle][:state] == "FAILED") &&
                 (@active_jobs[taskname][cycle][:tries] >= task.attributes[:maxtries])
                @active_jobs[taskname][cycle][:state] = "DEAD"
              end
              triesmsg = ", try=#{@active_jobs[taskname][cycle][:tries]} (of #{task.attributes[:maxtries]})"
            else
              # Update counters for jobs that are still QUEUED, RUNNING, or UNKNOWN
              @active_task_count += 1
              @active_core_count += @active_jobs[taskname][cycle][:cores]
              triesmsg = ""
            end

            statemsg = "Task #{taskname}, jobid=#{@active_jobs[taskname][cycle][:jobid]}, " \
                       "in state #{@active_jobs[taskname][cycle][:state]} " \
                       "(#{@active_jobs[taskname][cycle][:native_state]})"

            # Update the job state in the database
            @db_server.update_jobs([@active_jobs[taskname][cycle]])

            # Log the state of the job
            @log_server.log(cycle, statemsg + runmsg + unknownmsg + triesmsg)
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

      # Loop over all active cycles
      @active_cycles.each do |cycle|
        # Initialize done flag to true for this cycle
        cycle_done = false
        cycle_success = true

        catch(:not_done) do
          # Loop over all tasks
          @tasks.each do |task|
            # Validate that this cycle is a member of at least one of the cycledefs specified for this task
            unless task.attributes[:cycledefs].nil?

              # Get the cycledefs associated with this task
              if taskcycledefs[task].nil?
                taskcycledefs[task] = @cycledefs.find_all do |cycledef|
                  task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
                end
              end

              # Reject this task if the cycle is not a member of the tasks cycle list
              next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle[:cycle]) }

            end

            # The cycle is not done if this task has not been submitted yet for any of the active cycles
            throw :not_done if @active_jobs[task.attributes[:name]].nil?

            # The cycle is not done if this task has not been submitted yet for this cycle
            throw :not_done if @active_jobs[task.attributes[:name]][cycle[:cycle]].nil?

            # The cycle is not done if the job for this task and cycle is not in the done state
            throw :not_done if @active_jobs[task.attributes[:name]][cycle[:cycle]][:state] != "SUCCEEDED"

            # For now, only tag cycles as done if they are done successfully, meaning that all
            # tasks are complete and have exit status = 0.
            # If we mark cycles as done when they have tasks that exceeded retries, then increasing
            # retries won't cause them to rerun again
            #
            #            # The cycle is not done if the job for this task and cycle is done, but
            #            # has crashed and has not yet exceeded the retry count
            #            if @active_jobs[task.attributes[:name]][cycle[:cycle]][:tries] >= task.attributes[:maxtries]
            #              cycle_success=false
            #            else
            #              throw :not_done if @active_jobs[task.attributes[:name]][cycle[:cycle]][:exit_status] != 0
            #            end
          end

          cycle_done = true
        end

        # If the cycle is done, record the time and update active cycle list
        if cycle_done

          # Set the expiration time
          cycle[:done] = Time.now.getgm

          # Add to list of done cycles
          done_cycles << cycle

          # Log the done status of this cycle
          if cycle_success
            @log_server.log(cycle[:cycle], "This cycle is complete: Success")
          else
            @log_server.log(cycle[:cycle], "This cycle is complete: Failed")
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
        if Time.now.getgm - cycle[:activated].getgm > @cyclelifespan

          # Set the expiration time
          cycle[:expired] = Time.now.getgm

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
          next if @active_jobs[taskname][cycle[:cycle]].nil?

          next if ["SUCCEEDED", "FAILED", "DEAD"].include?(@active_jobs[taskname][cycle[:cycle]][:state])

          @log_server.log(cycle[:cycle],
                          "Deleting #{taskname} job #{@active_jobs[taskname][cycle[:cycle]][:jobid]} " \
                          "because this cycle has expired!")
          @bq_server.delete(@active_jobs[taskname][cycle[:cycle]][:jobid])
        end

        @log_server.log(cycle[:cycle], "This cycle has expired!")

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
      @active_cycles.collect { |c| c[:cycle] }.sort.each do |cycle|
        @tasks.each do |task|
          # Mqke sure the task is eligible for submission
          resubmit = false
          if !@active_jobs[task.attributes[:name]].nil? && !@active_jobs[task.attributes[:name]][cycle].nil?

            # Since this task has already been submitted at least once, reject it unless the job for it has failed
            next unless @active_jobs[task.attributes[:name]][cycle][:state] == "FAILED"

            # This task is a resubmission
            resubmit = true

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
            next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle) }

          end

          # Reject this task if core throttle will be exceeded
          if @active_core_count + task.attributes[:cores] > @corethrottle
            @log_server.log(cycle,
                            "Cannot submit #{task.attributes[:name]}, because maximum core throttle of " \
                            "#{@corethrottle} will be violated.", 2)
            next
          end

          # Reject this task if task throttle will be exceeded
          if @active_task_count + 1 > @taskthrottle
            @log_server.log(cycle,
                            "Cannot submit #{task.attributes[:name]}, because maximum task throttle of " \
                            "#{@taskthrottle} will be violated.", 2)
            next
          end

          # Reject this task if dependencies are not satisfied
          if !task.dependency.nil? && !task.dependency.resolved?(cycle, @active_jobs, @workflow_io_server, @cycledefs)
            next
          end

          # Reject this task if retries has been exceeded
          # This code block should never execute since state should be DEAD if retries is exceeded
          # and we should never get here for a DEAD job
          if resubmit && (@active_jobs[task.attributes[:name]][cycle][:tries] >= task.attributes[:maxtries])
            @log_server.log(cycle,
                            "Cannot resubmit #{task.attributes[:name]}, maximum retry count of " \
                            "#{task.attributes[:maxtries]} has been reached")
            next
          end

          # Increment counters
          @active_core_count += task.attributes[:cores]
          @active_task_count += 1

          # If we are resubmitting the job, initialize the new job to the old job
          newjob = if resubmit
                     @active_jobs[task.attributes[:name]][cycle]
                   else
                     { taskname: task.attributes[:name], cycle: cycle, tries: 0, nunknowns: 0 }
                   end
          newjob[:state] = "SUBMITTING"
          newjob[:exit_status] = 0
          newjob[:cores] = task.attributes[:cores]
          # In dryrun mode, no DRb server is launched, so use 0 as placeholder
          newjob[:jobid] = @config.BatchQueueServer && !WorkflowMgr.dryrun_mode? ? @bq_server.__drburi : 0

          # Append the new job to the list of new jobs that were submitted
          newjobs << newjob

          # Add the new job to the database (skip in dryrun to avoid persistent side effects)
          @db_server.add_jobs([newjob]) unless WorkflowMgr.dryrun_mode?

          # Submit the task
          @bq_server.submit(task.localize(cycle), cycle)
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
        job[:jobid]
        jobid, output = @bq_server.get_submit_status(job[:taskname], job[:cycle])
        if WorkflowMgr.dryrun_mode?
          @log_server.log(job[:cycle], "Dryrun Mode: would submit #{job[:taskname]}")
        elsif output.nil?
          @log_server.log(job[:cycle], "Submitted #{job[:taskname]}.  Submission status is pending at #{job[:jobid]}")
        elsif jobid.nil?
          # Delete the job from the database since it failed to submit.  It will be retried next time around.
          @db_server.delete_jobs([job])
          puts output
          @log_server.log(job[:cycle], "Submission of #{job[:taskname]} failed!  #{output}")
        else
          job[:jobid] = jobid
          @log_server.log(job[:cycle], "Submitted #{job[:taskname]}, jobid=#{job[:jobid]}")
          # Update the jobid for the job in the database
          @db_server.update_jobs([job])
        end
      end
    end

    ##########################################
    #
    # localize_task
    #
    ##########################################
    def localize_task(task, cycle)
      # Walk the task and evaluate all CompoundTimeStrings using the input cycle time
      if task.is_a?(Hash)
        lt = {}
        task.each do |key, value|
          lkey = if key.is_a?(CompoundTimeString)
                   key.to_s(cycle)
                 else
                   key
                 end
          lvalue = if value.is_a?(CompoundTimeString)
                     value.to_s(cycle)
                   elsif value.is_a?(Hash) || value.is_a?(Array)
                     localize_task(value, cycle)
                   else
                     value
                   end
          lt[lkey] = lvalue
        end
      elsif task.is_a?(Array)
        lt = task.collect do |value|
          if value.is_a?(CompoundTimeString)
            value.to_s(cycle)
          elsif value.is_a?(Hash) || value.is_a?(Array)
            localize_task(value, cycle)
          else
            value
          end
        end
      end

      lt
    end
  end
end
