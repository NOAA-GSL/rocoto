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
      actor_name = actor_class.is_a?(String) ? actor_class : actor_class.name
      raise ArgumentError, "actor class must have a name" if actor_name.nil? || actor_name.empty?

      source ||= Object.const_source_location(actor_name)&.first
      raise ArgumentError, "cannot locate source for #{actor_name}; pass source:" unless source

      # Reject bad options and unserializable arguments before paying for a process.
      raise ArgumentError, "mailbox_size must be positive" unless mailbox_size.is_a?(Integer) && mailbox_size.positive?
      unless mailbox_bytes.is_a?(Integer) && mailbox_bytes.positive?
        raise ArgumentError,
              "mailbox_bytes must be positive"
      end

      Transport.dump(arguments: arguments, context: context)

      parent_socket, child_socket = UNIXSocket.pair
      parent_pid = Process.pid
      environment = {
        "ROCOTO_ACTOR_FD" => CHILD_SOCKET_FD.to_s,
        "ROCOTO_ACTOR_PARENT_PID" => parent_pid.to_s,
        "ROCOTO_ACTOR_CLASS" => actor_name,
        "ROCOTO_ACTOR_SOURCE" => File.expand_path(source)
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

    # Launches and waits for the actor to become ready. Used by low-level tests;
    # the broker uses launch so that the actor is registered while it boots.
    def spawn(actor_class, *, start_timeout: START_TIMEOUT, **)
      reference, boot = launch(actor_class, *, **)
      boot.value(timeout: start_timeout)
      reference
    rescue StandardError => error
      reference&.stop(force: true, timeout: 0)
      raise startup_error(reference, error)
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
      Thread.new do
        Process.waitpid(pid)
      rescue Errno::ECHILD
        nil
      end
    end

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
