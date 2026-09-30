##########################################
#
# Module WFMStat
#
##########################################
module WFMStat
  ##########################################
  #
  # Class StatusEngine
  #
  ##########################################
  class StatusEngine
    require_relative '../../vendor/rocoto_actor/lib/rocoto_actor'
    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/workflowstate'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/cycledef'
    require "workflowmgr/cycle"
    require 'workflowmgr/dependency'
    require 'workflowmgr/workflowconfig'
    require 'workflowmgr/workflowioproxy'

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
      @config = WorkflowMgr::WorkflowYAMLConfig.new

      # Get command line options
      @options = options

      # The one broker for this invocation of rocoto. It owns every actor
      # process rocoto starts, and stopping it stops all of them.
      @broker = RocotoActor::ActorBroker.new(error_handler: WorkflowMgr.method(:report_actor_error),
                                             on_event: WorkflowMgr.method(:report_actor_event))

      # Set up an object to serve the workflow database (but do not open the database)
      @db_server = WorkflowMgr.workflow_database(@config, @options, @broker)
    rescue StandardError => e
      WorkflowMgr.stderr(e.message, 1)
      WorkflowMgr.log(e.message)
      if e.is_a?(ArgumentError) || e.is_a?(NameError) || e.is_a?(TypeError)
        WorkflowMgr.stderr(e.backtrace.join("\n"), 1)
        WorkflowMgr.log(e.backtrace.join("\n"))
      end
      Process.exit(1)
    end

    ##########################################
    #
    # wfmstat
    #
    ##########################################
    def wfmstat
      # Open/Create the database
      @db_server.dbopen({ readonly: true })

      # Set up an object to serve file stat info
      @workflow_io_server = WorkflowMgr::WorkflowIOProxy.new(@db_server, @config, @options, @broker)

      # Open the workflow document
      @workflowdoc = WorkflowMgr::WorkflowXMLDoc.new(@options.workflowdoc, @workflow_io_server, @config)

      @workflowdoc.features_supported?

      # Get the task and cycle subsets
      @subset = @options.selection.make_subset(@workflowdoc.tasks, @workflowdoc.cycledefs,
                                               @db_server)

      # Print a cycle summary report if requested
      if @options.summary
        print_summary
      else
        print_status
      end
    rescue StandardError => e
      WorkflowMgr.stderr(e.message, 1)
      WorkflowMgr.log(e.message)
      if e.is_a?(ArgumentError) || e.is_a?(NameError) || e.is_a?(TypeError)
        WorkflowMgr.stderr(e.backtrace.join("\n"), 1)
        WorkflowMgr.log(e.backtrace.join("\n"))
      end
      Process.exit(1)
    ensure
      # Make sure we release the workflow lock in the database and shutdown the db_server
      # Matches the condition the database was created under: in dryrun it
      # is an ordinary object in this process, with nothing to shut down.
      if !@db_server.nil? && @config.DatabaseServer && !WorkflowMgr.dryrun_mode?
        @db_server.stop!
      end

      # Make sure to shut down the workflow file stat server
      # Matches the condition the server was created under: in dryrun the io
      # object lives in this process and has nothing to shut down.
      if !@workflow_io_server.nil? && @config.WorkflowIOServer && !WorkflowMgr.dryrun_mode?
        @workflow_io_server.stop!
      end

      # Last, once the things that own actors have been shut down: stopping
      # the broker stops every actor process along with it.
      @broker&.stop
    end

    ##########################################
    #
    # check_one_task
    #
    ##########################################
    def check_one_task(cycletime, taskname, cycledefs)
      # Get the cycle
      cycle = @db_server.load_cycles({ start: cycletime, end: cycletime }).first || WorkflowMgr::Cycle.new(cycletime)

      # Get the task
      task = @workflowdoc.tasks[taskname]
      task = task.localize(cycletime) unless task.nil?

      # Get the job (if there is one)
      jobcycles = [cycletime]
      @workflowdoc.taskdep_cycle_offsets.each do |offset|
        jobcycles << cycletime + offset
      end
      jobs = @db_server.load_jobs(jobcycles)
      job = if jobs[taskname].nil?
              nil
            else
              jobs[taskname][cycletime]
            end

      # Print the task information
      print_taskinfo(task)

      # Query and print task dependency info
      dependencies = nil
      nil
      unless task.nil?
        unless task.dependency.nil?
          wstate = WorkflowMgr::WorkflowState.new(cycle.cycle, jobs, @workflow_io_server, @workflowdoc.cycledefs,
                                                  task.attributes[:name], task, @workflowdoc.tasks)
          dependencies = task.dependency.query(wstate)
          printf "%<indent>2s%<label>s\n", indent: "", label: "dependencies"
          print_deps(dependencies, 0)
        end
        unless task.hangdependency.nil?
          wstate = WorkflowState.new(cycle.cycle, jobs, @workflow_io_server, @workflowdoc.cycledefs,
                                     task.attributes[:name], task, @workflowdoc.tasks)
          hangdependencies = task.hangdependency.query(wstate)
          printf "%<indent>2s%<label>s\n", indent: "", label: "hang dependencies"
          print_deps(hangdependencies, 0)
        end
      end

      # Print the cycle information
      print_cycleinfo(cycle, cycledefs, task)

      # Print the job information
      print_jobinfo(job)

      # Print throttling violations
      print_violations(task, cycle, dependencies) if job.nil?
    end

    ##########################################
    #
    # check_tasks
    #
    ##########################################
    def check_tasks
      # Open/Create the database
      @db_server.dbopen({ readonly: true })

      # Set up an object to serve file stat info
      @workflow_io_server = WorkflowMgr::WorkflowIOProxy.new(@db_server, @config, @options, @broker)

      # Open the workflow document
      @workflowdoc = WorkflowMgr::WorkflowXMLDoc.new(@options.workflowdoc, @workflow_io_server, @config)

      @subset = @options.selection.make_subset(@workflowdoc.tasks, cycledefs = @workflowdoc.cycledefs,
                                               @db_server)

      cycledefs = @workflowdoc.cycledefs

      @subset.each_cycle do |cycletime|
        @subset.each_task do |taskname|
          check_one_task(cycletime, taskname, cycledefs)
        end
      end
    rescue StandardError => e
      WorkflowMgr.stderr(e.message, 1)
      WorkflowMgr.log(e.message)
      if e.is_a?(ArgumentError) || e.is_a?(NameError) || e.is_a?(TypeError)
        WorkflowMgr.stderr(e.backtrace.join("\n"), 1)
        WorkflowMgr.log(e.backtrace.join("\n"))
      end
      Process.exit(1)
    ensure
      # Make sure we release the workflow lock in the database and shutdown the db_server
      # Matches the condition the database was created under: in dryrun it
      # is an ordinary object in this process, with nothing to shut down.
      if !@db_server.nil? && @config.DatabaseServer && !WorkflowMgr.dryrun_mode?
        @db_server.stop!
      end

      # Make sure to shut down the workflow file stat server
      # Matches the condition the server was created under: in dryrun the io
      # object lives in this process and has nothing to shut down.
      if !@workflow_io_server.nil? && @config.WorkflowIOServer && !WorkflowMgr.dryrun_mode?
        @workflow_io_server.stop!
      end

      # Last, once the things that own actors have been shut down: stopping
      # the broker stops every actor process along with it.
      @broker&.stop
    end

    ##########################################
    #
    # cycle_sets
    #
    ##########################################
    def cycle_sets
      # Turn the db, xml, and undef iterators into arrays:
      dbcycles = @subset.collect_db_cycles { |c| c }
      xmlcycles = @subset.collect_xml_cycles { |c| c }
      undefcycles = @subset.collect_undef_cycles { |c| c }

      [dbcycles, xmlcycles, undefcycles]
    end

    ##########################################
    #
    # print_summary
    #
    ##########################################
    def print_summary
      # Get cycles of interest
      dbcycles, xmlcycles, = cycle_sets

      # Print the header
      printf "%<cycle>12s    %<state>8s    %<activated>20s    %<deactivated>20s\n",
             cycle: "CYCLE".center(12),
             state: "STATE".center(8),
             activated: "ACTIVATED".center(20),
             deactivated: "DEACTIVATED".center(20)

      # Print the cycle date/times
      (dbcycles + xmlcycles).sort.each do |cycle|
        printf "%<cycle_time>12s    %<state>8s    %<activated>20s    %<deactivated>20s\n",
               cycle_time: cycle.cycle.strftime('%Y%m%d%H%M').to_s,
               state: cycle.state.to_s.capitalize.to_s,
               activated: cycle.activated_time_string.center(20).to_s,
               deactivated: cycle.deactivated_time_string.center(20).to_s
      end
    end

    ##########################################
    #
    # print_status
    #
    ##########################################
    def print_status
      # Get cycles of interest
      dbcycles, xmlcycles, = cycle_sets

      # Get the jobs from the database for the cycles of interest
      jobs = @db_server.load_jobs(dbcycles.collect(&:cycle))

      # Get the list of tasks from the workflow definition
      defined_tasks = @workflowdoc.tasks

      # Get the cycle defs
      cycledefs = @workflowdoc.cycledefs

      # Initialize empty hash of task cycledefs
      taskcycledefs = {}

      # Print the job status info
      if @options.taskfirst

        format = "%20s    %12s    %24s    %16s    %16s    %6s    %10s\n"
        header = "TASK".rjust(20), "CYCLE".rjust(12), "JOBID".rjust(24),
                 "STATE".rjust(16), "EXIT STATUS".rjust(16), "TRIES".rjust(6),
                 "DURATION".rjust(10)
        puts format % header

        # Sort the task list in sequence order
        tasklist = jobs.keys | defined_tasks.values.collect { |t| t.attributes[:name] }
        tasklist = tasklist.sort_by do |t|
          [defined_tasks[t].nil? ? 999_999_999 : defined_tasks[t].seq, t.split(/(\d+)/).map do |i|
            i =~ /\d+/ ? i.to_i : i
          end].flatten
        end

        tasklist.each do |task|
          next unless @subset.selected? task

          printf "#{'=' * 128}\n"
          cyclelist = (dbcycles | xmlcycles).collect(&:cycle).sort
          cyclelist.each do |cycle|
            next unless @subset.selected? cycle

            # Only print info if the cycle is defined for this task
            unless defined_tasks[task].attributes[:cycledefs].nil?
              # Get the cycledefs associated with this task
              taskcycledefs[task] = cycledefs.find_all do |cycledef|
                defined_tasks[task].attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
              end
              # Reject this task if the cycle is not a member of the tasks cycle list
              next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle) }
            end

            jobdata = if jobs[task].nil?
                        ["-", "-", "-", "-", "-"]
                      elsif jobs[task][cycle].nil?
                        ["-", "-", "-", "-", "-"]
                      else
                        case jobs[task][cycle].state
                        when "SUCCEEDED", "DEAD", "FAILED"
                          [jobs[task][cycle].id, jobs[task][cycle].state, jobs[task][cycle].exit_status,
                           jobs[task][cycle].tries, jobs[task][cycle].duration]
                        else
                          [jobs[task][cycle].id, jobs[task][cycle].state, "-", jobs[task][cycle].tries,
                           jobs[task][cycle].duration]
                        end
                      end
            puts format % ([task, cycle.strftime("%Y%m%d%H%M")] + jobdata)
          end
        end

      else

        format = "%12s    %20s    %24s    %16s    %16s    %6s    %10s\n"
        header = "CYCLE".rjust(12), "TASK".rjust(20), "JOBID".rjust(24),
                 "STATE".rjust(16), "EXIT STATUS".rjust(16), "TRIES".rjust(6),
                 "DURATION".rjust(10)
        puts format % header

        # Print status of jobs for each cycle
        cyclelist = (dbcycles | xmlcycles).collect(&:cycle).sort
        cyclelist.each do |cycle|
          unless @subset.selected? cycle
            # puts "#{cycle.class.name} #{cycle.inspect}: not selected"
            next
          end

          printf "#{'=' * 128}\n"

          # Sort the task list in sequence order
          tasklist = jobs.keys | defined_tasks.values.collect { |t| t.attributes[:name] }
          tasklist = tasklist.sort_by do |t|
            [defined_tasks[t].nil? ? 999_999_999 : defined_tasks[t].seq, t.split(/(\d+)/).map do |i|
              i =~ /\d+/ ? i.to_i : i
            end].flatten
          end
          tasklist.each do |task|
            unless @subset.selected? task
              # puts "#{task}: not selected"
              next
            end

            # Only print info if the task is defined for this cycle
            unless defined_tasks[task].nil? || defined_tasks[task].attributes[:cycledefs].nil?
              # Get the cycledefs associated with this task
              taskcycledefs[task] = cycledefs.find_all do |cycledef|
                defined_tasks[task].attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
              end
              # Reject this task if the cycle is not a member of the tasks cycle list
              next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle) }
            end

            jobdata = if jobs[task].nil?
                        ["-", "-", "-", "-", "-"]
                      elsif jobs[task][cycle].nil?
                        ["-", "-", "-", "-", "-"]
                      else
                        case jobs[task][cycle].state
                        when "SUCCEEDED", "DEAD", "FAILED"
                          [jobs[task][cycle].id, jobs[task][cycle].state, jobs[task][cycle].exit_status,
                           jobs[task][cycle].tries, jobs[task][cycle].duration]
                        else
                          [jobs[task][cycle].id, jobs[task][cycle].state, "-", jobs[task][cycle].tries,
                           jobs[task][cycle].duration]
                        end
                      end
            puts format % ([cycle.strftime("%Y%m%d%H%M"), task] + jobdata)
          end
        end

      end
    end

    ##########################################
    #
    # print_taskinfo
    #
    ##########################################
    def print_taskinfo(task)
      puts
      if task.nil?
        puts "Task: Not defined in current workflow definition"
      else
        puts "Task: #{task.attributes[:name]}"
        task.attributes.keys.sort { |a1, a2| a1.to_s <=> a2.to_s }.each do |attr|
          puts "  #{attr}: #{task.attributes[attr]}"
        end
        puts "  environment"
        task.envars.keys.sort.each do |envar|
          puts "    #{envar} ==> #{task.envars[envar]}"
        end
      end
    end

    ##########################################
    #
    # print_cycleinfo
    #
    ##########################################
    def print_cycleinfo(cycle, cycledefs, task)
      return if task.nil? || task.attributes.nil?

      # Make sure the cycle is valid for this task
      cycle_is_valid = true
      unless task.attributes[:cycledefs].nil?
        taskcycledefs = cycledefs.find_all do |cycledef|
          task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
        end
        # Cycle is invalid for this task if the cycle is not a member of the tasks cycle list
        unless taskcycledefs.any? { |cycledef| cycledef.member?(cycle.cycle) }
          cycle_is_valid = false
        end
      end

      puts
      puts "Cycle: #{cycle.cycle.strftime('%Y%m%d%H%M')}"
      if cycle_is_valid
        puts "  Valid for this task: YES"
      else
        puts "  Valid for this task: NO"
      end
      puts "  State: #{cycle.state}"
      puts "  Activated: #{cycle.activated != Time.at(0) ? cycle.activated : '-'}"
      puts "  Completed: #{cycle.done? ? cycle.done : '-'}"
      puts "  Expired: #{cycle.expired? ? cycle.expired : '-'}"
    end

    ##########################################
    #
    # print_jobinfo
    #
    ##########################################
    def print_jobinfo(job)
      puts
      if job.nil?
        puts "Job: This task has not been submitted for this cycle"
      else
        puts "Job: #{job.id}"
        puts "  State:  #{job.state} (#{job.native_state})"
        puts "  Exit Status:  #{job.done? ? job.exit_status : '-'}"
        puts "  Tries:  #{job.tries}"
        puts "  Unknown count:  #{job.nunknowns}"
        puts "  Duration:  #{job.duration}"
      end
    end

    ##########################################
    #
    # print_violations
    #
    ##########################################
    def print_violations(task, cycle, dependencies)
      puts
      puts "Task can not be submitted because:"

      # Check for non-existent task
      if task.nil?
        puts "  The task is not defined"
        return
      end

      # Check for inactive cycle
      unless cycle.active?
        puts "  The cycle is not active"
        return
      end

      # Check for unsatisfied dependencies
      if !dependencies.nil? && !dependencies.first[:resolved]
        puts "  Dependencies are not satisfied"
        return
      end

      # Check for throttle violations
      active_cycles = @db_server.load_active_cycles
      active_jobs = @db_server.load_jobs(active_cycles.collect(&:cycle))
      ncores = 0
      ntasks = 0
      active_jobs.each_key do |jobtask|
        active_jobs[jobtask].each_key do |jobcycle|
          unless active_jobs[jobtask][jobcycle].done?
            ntasks += 1
            ncores += active_jobs[jobtask][jobcycle].cores
          end
        end
      end
      if !@workflowdoc.taskthrottle.nil? && (ntasks + 1 > @workflowdoc.taskthrottle)
        puts "  Task throttle violation (#{ntasks} of #{@workflowdoc.taskthrottle} tasks are already active)"
      end
      if !@workflowdoc.corethrottle.nil? && (ncores + task.attributes[:cores] > @workflowdoc.corethrottle)
        puts "  Core throttle violation (#{ncores} of #{@workflowdoc.corethrottle} cores are already in use)"
      end
    end

    ##########################################
    #
    # print_deps
    #
    ##########################################
    def print_deps(deps, indent)
      return if deps.nil?

      deps.each do |d|
        if d.is_a?(Array)
          print_deps(d, indent + 1) if d.is_a?(Array)
        else
          printf "%#{2 * indent + 4}s%s %s\n", "", d[:dep], d[:msg]
        end
      end
    end
  end
end
