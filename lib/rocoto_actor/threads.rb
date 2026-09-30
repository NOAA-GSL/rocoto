# frozen_string_literal: true

module RocotoActor
  # Creates every thread the library owns. A thread that cannot be created
  # (the user's RLIMIT_NPROC is exhausted) is a ResourceLimitError, never a
  # ThreadError, which Ruby also raises for lock misuse.
  module Threads
    module_function

    def start(name, quiet: false, &)
      thread = Thread.new(&)
      thread.report_on_exception = false if quiet # its loop reports its own errors
      thread.name = "rocoto-actor-#{name}" if thread.respond_to?(:name=)
      thread
    rescue ThreadError => error
      raise ResourceLimitError, "#{name} thread: #{error.message}"
    end
  end
  private_constant :Threads
end
