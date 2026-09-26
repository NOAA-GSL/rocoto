# frozen_string_literal: true

module RocotoActor
  # Internal bounded executor for lifecycle operations that may block on
  # process creation or shutdown. It owns workers and queueing only; the
  # broker owns actor state and request semantics.
  class LifecycleExecutor
    Request = Struct.new(:source, :request, :release_response)

    def initialize(max_workers:, max_pending_requests:, error_handler:, request_error:, &request_handler)
      @max_workers = max_workers
      @max_pending_requests = max_pending_requests
      @error_handler = error_handler
      @request_error = request_error
      @request_handler = request_handler
      @queue = []
      @workers = []
      @idle_workers = 0
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @stopped = false
      @cap_reported = false
    end

    # Starts the first worker, which lives until stop: from then on every job
    # has a worker to wait for, whether or not more can be created. Raises
    # ResourceLimitError if it cannot be created.
    def start
      @mutex.synchronize { start_worker_locked }
    end

    # Returns true when the request is queued, :stopped when the executor has
    # been stopped, or a BrokerBusyError when the queue is full.
    def enqueue_request(source, request, release_response)
      @mutex.synchronize do
        return :stopped if @stopped

        waiting = @queue.count { |job| job.is_a?(Request) }
        if waiting >= @max_pending_requests
          return BrokerBusyError.new("actor broker has #{@max_pending_requests} lifecycle requests waiting")
        end

        queue_locked(Request.new(source, request, release_response))
        true
      end
    end

    def pending_requests
      @mutex.synchronize { @queue.count { |job| job.is_a?(Request) } }
    end

    # Returns true when the job will run, or :stopped when the executor has
    # been stopped.
    def enqueue_job(&block)
      @mutex.synchronize do
        return :stopped if @stopped

        queue_locked(block)
        true
      end
    end

    # Stops accepting work, waits for the workers, and returns the abandoned
    # requests.
    def stop
      workers, requests = @mutex.synchronize do
        @stopped = true
        @condition.broadcast
        queued = @queue
        @queue = []
        [@workers.dup, queued.grep(Request)]
      end
      workers.each { |worker| worker.join unless worker == Thread.current } # stop may run on a worker
      requests
    end

    private

    # Caller holds @mutex. Queues the job and adds a worker when none is idle
    # and the pool has room; a worker that cannot be created (RLIMIT_NPROC)
    # only leaves the pool smaller, since the first worker always exists. That
    # is reported once, from a worker, so no callback runs under the lock.
    def queue_locked(job)
      @queue << job
      begin
        start_worker_locked if @idle_workers.zero? && @workers.size < @max_workers
      rescue ResourceLimitError => error
        unless @cap_reported
          @cap_reported = true
          @queue.unshift(-> { ErrorReporting.report(@error_handler, error, "starting a lifecycle thread") })
        end
      end
      @condition.signal
    end

    # Caller holds @mutex.
    def start_worker_locked
      @workers << Threads.start("broker-lifecycle", quiet: true) { run_worker }
    end

    # A worker never ends before stop: the broker relies on one existing.
    def run_worker
      loop do
        job = @mutex.synchronize do
          @idle_workers += 1
          @condition.wait(@mutex) while @queue.empty? && !@stopped
          @idle_workers -= 1
          @stopped ? nil : @queue.shift
        end
        return unless job

        begin
          if job.is_a?(Request)
            @request_handler.call(job.source, job.request, job.release_response)
          else
            job.call
          end
        rescue Exception => error # rubocop:disable Lint/RescueException
          # Reported and, for a request, answered; nothing ends the worker.
          ErrorReporting.report(@error_handler, error, "lifecycle job")
          @request_error.call(job.source, job.request, error, job.release_response) if job.is_a?(Request)
        end
      end
    ensure
      @mutex.synchronize { @workers.delete(Thread.current) }
    end
  end
  private_constant :LifecycleExecutor
end
