##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class BQS
  #
  ##########################################
  class BQS
    require 'thread/pool'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/moabbatchsystem'
    require 'workflowmgr/moabtorquebatchsystem'
    require 'workflowmgr/torquebatchsystem'
    require 'workflowmgr/pbsprobatchsystem'
    require 'workflowmgr/lsfbatchsystem'
    require 'workflowmgr/slurmbatchsystem'
    require 'workflowmgr/cobaltbatchsystem'
    require 'workflowmgr/task'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(batch_system, db_file, config)
      Thread::Pool.abort_on_exception = true

      # Set the batch system
      @batchsystem = batch_system

      # We can't create a thread pool yet because the DRb server hasn't been started yet
      @pool_size = config.SubmitThreads
      @pool = nil

      # Initialize hash of job submit output
      @status = {}

      # Initialize hash of running threads
      @running = {}

      # Initialize hash to keep track of which submit outputs we've retrieved
      @harvested = {}
    end

    ##########################################
    #
    # submit
    #
    ##########################################
    def submit(task, cycle)
      # Initialize hashes for this task
      @harvested[task.attributes[:name]] = {} if @harvested[task.attributes[:name]].nil?
      @running[task.attributes[:name]] = {} if @running[task.attributes[:name]].nil?
      @status[task.attributes[:name]] = {} if @status[task.attributes[:name]].nil?

      # Dryrun: record status without spawning thread pool workers
      if WorkflowMgr.dryrun_mode?
        @harvested[task.attributes[:name]][cycle.to_i] = false
        @running[task.attributes[:name]][cycle.to_i] = false
        @status[task.attributes[:name]][cycle.to_i] = @batchsystem.submit(task)
        return
      end

      # Initialize a thread pool for multithreaded job submission if we don't have one yet
      @pool = Thread.pool(@pool_size) if @pool.nil?

      # Spawn a thread to submit the job
      @pool.process do
        # Initialize submission status to NOT harvested
        @harvested[task.attributes[:name]][cycle.to_i] = false

        # Mark this job submission in progress
        @running[task.attributes[:name]][cycle.to_i] = true

        # Submit the job
        @status[task.attributes[:name]][cycle.to_i] = @batchsystem.submit(task)

        # Mark this job submission as done
        @running[task.attributes[:name]][cycle.to_i] = false
      end
    end

    ##########################################
    #
    # get_submit_status
    #
    ##########################################
    def get_submit_status(taskid, cycle)
      # Return nil for jobid and output if the submit thread doesn't exist
      return nil, nil if @running[taskid].nil?
      return nil, nil if @running[taskid][cycle.to_i].nil?

      # Return nil for jobid and output	if the submit thread is still running
      if @running[taskid][cycle.to_i]
        [nil, nil]
      # Otherwise, get the jobid and output and return it
      else
        status = @status[taskid][cycle.to_i]

        # Mark this status as harvested
        @harvested[taskid][cycle.to_i] = true

        # Return the output of the job submission
        status
      end
    end

    ##########################################
    #
    # running?
    #
    ##########################################
    def running?
      # Check to see if any threads are still running
      @running.each_key do |taskid|
        @running[taskid].each_key do |cycle|
          return true if @running[taskid][cycle.to_i]
        end
      end

      # Check to see if all statuses have been harvested
      @harvested.each_key do |taskid|
        @harvested[taskid].each_key do |cycle|
          return true unless @harvested[taskid][cycle.to_i]
        end
      end

      false
    end

    ##########################################
    #
    # shutdown
    #
    ##########################################
    def shutdown
      return if @pool.nil?

      # Gracefully stop the thread pool, waiting for any in-flight tasks to finish
      @pool.shutdown

      # Allow submit() to spawn a fresh pool the next time it's called
      @pool = nil
    end

    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name, *args)
      @batchsystem.send(name, *args)
    end

    def respond_to_missing?(name, include_private = false)
      @batchsystem.respond_to?(name, include_private) || super
    end
  end
end
