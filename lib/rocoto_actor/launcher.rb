# frozen_string_literal: true

module RocotoActor
  # Internal: creates one actor process with its socket pair and returns the
  # Reference that owns it. Only ActorBroker calls this; there is no public way
  # to start an actor process outside a broker.
  module Launcher
    RUNNER_PATH = File.expand_path("runner.rb", __dir__)
    CHILD_SOCKET_FD = 3

    module_function

    # Starts the process and sends the boot message without waiting. Returns
    # [reference, boot_future]; the future resolves when the actor's initialize
    # returns, and the caller owns the deadline. actor_class may be a Class or
    # its name; a name requires source: unless the constant is defined in this
    # process. context: is delivered to the actor as RocotoActor.context.
    def launch(actor_class, *arguments, source: nil, mailbox_size: DEFAULT_MAILBOX_SIZE,
               mailbox_bytes: DEFAULT_MAILBOX_BYTES, context: nil)
      actor_name, source = resolve(actor_class, source)
      # Reject unserializable arguments before paying for a process; the
      # mailbox options were validated by SpawnOptions.
      Transport.dump(arguments: arguments, context: context)

      parent_socket, child_socket = UNIXSocket.pair
      parent_pid = Process.pid
      environment = {
        "ROCOTO_ACTOR_FD" => CHILD_SOCKET_FD.to_s,
        "ROCOTO_ACTOR_PARENT_PID" => parent_pid.to_s,
        "ROCOTO_ACTOR_CLASS" => actor_name,
        "ROCOTO_ACTOR_SOURCE" => source
      }
      pid = Process.spawn(
        environment,
        RbConfig.ruby,
        RUNNER_PATH,
        CHILD_SOCKET_FD => child_socket,
        in: File::NULL,
        out: File::NULL,
        err: File::NULL,
        pgroup: true,
        close_others: true
      )
      child_socket.close
      reference = Reference.new(parent_socket, pid, mailbox_size: mailbox_size, mailbox_bytes: mailbox_bytes)
      [reference, reference.boot(arguments, context)]
    rescue Exception # rubocop:disable Lint/RescueException
      if reference
        reference.stop(force: true, timeout: 0)
      else
        parent_socket&.close
        child_socket&.close
        terminate_failed_spawn(pid)
      end
      raise
    end

    # The actor's class name and the absolute path of the file that defines it.
    # A class name string requires source: unless the constant is defined here.
    def resolve(actor_class, source)
      actor_name = actor_class.is_a?(String) ? actor_class : actor_class.name
      raise ArgumentError, "actor class must have a name" if actor_name.nil? || actor_name.empty?

      source ||= Object.const_source_location(actor_name)&.first
      raise ArgumentError, "cannot locate source for #{actor_name}; pass source:" unless source

      [actor_name, File.expand_path(source)]
    end

    # Maps a boot future's failure to the error the spawner sees.
    def startup_error(reference, error)
      return error unless reference

      case error
      when AskTimeoutError then TransportTimeoutError.new("actor #{reference.pid} startup timed out")
      when ActorStoppedError then Error.new("actor #{reference.pid} closed during startup")
      else error
      end
    end

    def terminate_failed_spawn(pid)
      return unless pid
      return if Process.waitpid(pid, Process::WNOHANG)

      terminate_process_group(pid)
    rescue Errno::ECHILD
      nil
    end
    private_class_method :terminate_failed_spawn

    def terminate_process_group(pid)
      signal_process_group(pid, "KILL")
      Threads.start("reaper-#{pid}") do
        Process.waitpid(pid)
      rescue Errno::ECHILD
        nil
      end
    rescue ResourceLimitError
      reap_without_thread(pid)
    end

    # KILL is delivered asynchronously, so poll briefly rather than once; a
    # process that cannot die within the window stays a zombie until the
    # application exits, which is the best a caller with no threads can do.
    def reap_without_thread(pid, patience: 1.0)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + patience
      until Process.waitpid(pid, Process::WNOHANG)
        return nil if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

        sleep 0.01
      end
      nil
    rescue Errno::ECHILD
      nil
    end
    private_class_method :reap_without_thread

    def signal_process_group(pid, signal)
      Process.kill(signal, -pid)
    rescue Errno::ESRCH, Errno::EPERM
      nil
    end

    def process_group_alive?(pid)
      Process.kill(0, -pid)
      true
    rescue Errno::ESRCH
      false
    rescue Errno::EPERM
      true
    end
  end
  private_constant :Launcher
end
