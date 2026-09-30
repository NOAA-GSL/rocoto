# frozen_string_literal: true

module RocotoActor
  # Internal deadline executor for route expirations and delayed broker tasks.
  # It owns timing and a thread, but knows nothing about actor state.
  class DeadlineScheduler
    def initialize(error_handler:)
      @error_handler = error_handler
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @expirations = {}
      @tasks = []
      @stopped = false
      @thread = nil
    end

    # Starts the thread, which runs until stop: callbacks are guarded, so no
    # exception ends it. Raises ResourceLimitError if it cannot be created.
    def start
      @thread = Threads.start("broker", quiet: true) { run }
    end

    # Returns true when the expiration is armed, or :stopped when the
    # scheduler has been stopped. Nothing here calls back while the lock is held.
    def schedule_expiration(key, timeout, &block)
      deadline = monotonic_time + timeout
      @mutex.synchronize do
        return :stopped if @stopped

        @expirations[key] = [deadline, timeout, block]
        @condition.signal
        true
      end
    end

    def cancel_expiration(key)
      @mutex.synchronize { @expirations.delete(key) }
    end

    # Returns true when the task will run, or :stopped when the scheduler has
    # been stopped.
    def enqueue(delay: 0, &block)
      @mutex.synchronize do
        return :stopped if @stopped

        @tasks << [monotonic_time + delay, block]
        @condition.signal
        true
      end
    end

    def stop
      @mutex.synchronize do
        @stopped = true
        @condition.broadcast
        return @thread
      end
    end

    private

    def run
      loop do
        expirations, tasks = @mutex.synchronize do
          loop do
            break if @stopped

            now = monotonic_time
            due_expirations = @expirations.select { |_key, (deadline, _timeout, _block)| deadline <= now }
            ready, @tasks = @tasks.partition { |run_at, _block| run_at <= now }
            unless due_expirations.empty? && ready.empty?
              due_expirations.each_key { |key| @expirations.delete(key) }
              break [due_expirations.values, ready.map(&:last)]
            end

            next_deadline = (@expirations.values.map(&:first) + @tasks.map(&:first)).min
            @condition.wait(@mutex, next_deadline && (next_deadline - now))
          end
        end
        return unless expirations

        expirations.each do |_deadline, timeout, block|
          ErrorReporting.guard(@error_handler, "route expiration") { block.call(timeout) }
        end
        tasks.each { |task| ErrorReporting.guard(@error_handler, "service task") { task.call } }
      end
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
  private_constant :DeadlineScheduler
end
