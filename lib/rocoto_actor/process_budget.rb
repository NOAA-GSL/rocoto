# frozen_string_literal: true

module RocotoActor
  # Internal check that starting one more actor will not run the user into
  # RLIMIT_NPROC, which counts every process and thread the user owns on the
  # machine and, when hit, can wedge a Ruby process rather than fail cleanly.
  # The count is taken from /proc so that no process is forked to make it;
  # where /proc is absent or the limit is unlimited, nothing is checked.
  module ProcessBudget
    # Kernel tasks one actor costs: watchdog and worker processes (two threads
    # each) plus the reader, writer, and reaper threads in the application.
    TASKS_PER_ACTOR = 7

    module_function

    # { limit:, in_use:, margin: } when the limit is finite and measurable,
    # nil otherwise.
    def snapshot(margin)
      # Root (real or effective) is exempt from RLIMIT_NPROC, and /proc would count kernel threads for it.
      return nil if Process.uid.zero? || Process.euid.zero?

      limit = soft_limit
      in_use = limit && tasks_in_use
      return nil unless in_use

      { limit: limit, in_use: in_use, margin: margin }
    end

    # Raises ResourceLimitError when one more actor plus the margin would not
    # fit under the limit.
    def check!(margin)
      snapshot = snapshot(margin)
      return unless snapshot

      needed = snapshot[:in_use] + TASKS_PER_ACTOR + margin
      return if needed <= snapshot[:limit]

      raise ResourceLimitError,
            "starting an actor needs #{TASKS_PER_ACTOR} processes/threads plus a margin of #{margin}, but " \
            "#{snapshot[:in_use]} of the user's #{snapshot[:limit]} (RLIMIT_NPROC) are in use"
    end

    def soft_limit
      soft, = Process.getrlimit(:NPROC)
      soft == Process::RLIM_INFINITY ? nil : soft
    rescue NotImplementedError, ArgumentError
      nil
    end

    # Sum of the Threads field of every /proc entry whose real uid is ours.
    # Processes that exit mid-scan are skipped.
    def tasks_in_use
      return nil unless File.directory?("/proc")

      uid = Process.uid.to_s
      Dir.children("/proc").sum do |entry|
        next 0 unless entry.match?(/\A\d+\z/)

        status = File.read("/proc/#{entry}/status")
        next 0 unless status[/^Uid:\s+(\d+)/, 1] == uid

        status[/^Threads:\s+(\d+)/, 1].to_i
      rescue SystemCallError, IOError
        0
      end
    end
  end
  private_constant :ProcessBudget
end
