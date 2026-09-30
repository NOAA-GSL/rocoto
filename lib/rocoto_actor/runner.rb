# frozen_string_literal: true

require_relative "../rocoto_actor"

module RocotoActor
  module Runner
    module_function

    def run
      socket = UNIXSocket.for_fd(Integer(ENV.fetch("ROCOTO_ACTOR_FD")))
      parent_pid = Integer(ENV.fetch("ROCOTO_ACTOR_PARENT_PID"))
      worker_pid = fork { run_worker(socket) }
      watch(socket, worker_pid, parent_pid)
    rescue Exception => error # rubocop:disable Lint/RescueException
      report_boot_error(socket, error)
      socket&.close
      exit! 1
    end

    def run_worker(socket)
      RocotoActor.worker_process!
      decode_bindings = RocotoActor.broker_client(socket).decode_bindings
      require ENV.fetch("ROCOTO_ACTOR_SOURCE")
      actor_class = constantize(ENV.fetch("ROCOTO_ACTOR_CLASS"))
      boot = Transport.read(socket, bindings: decode_bindings)
      raise Error, "expected actor boot message" unless boot&.fetch(:op) == :boot

      boot_id = boot.fetch(:id)
      # The broker serves requests from here on, so initialize may spawn children.
      RocotoActor.context = ActorContext.new(socket, boot[:context]&.fetch(:actor_id, nil))
      actor = actor_class.new(*boot.fetch(:arguments))
      Transport.write(socket, Protocol.success(boot_id))
      run_actor(socket, actor)
    rescue Exception => error # rubocop:disable Lint/RescueException
      report_boot_error(socket, error, boot_id)
      socket&.close
      exit! 1
    end
    private_class_method :run_worker

    def run_actor(socket, actor)
      deferred = RocotoActor.broker_client(socket).deferred_frames
      loop do
        request = deferred.shift || Transport.read(socket, bindings: RocotoActor.broker_client(socket).decode_bindings)
        break unless request

        case request.fetch(:op)
        when :stop
          shutdown_actor(socket, actor)
          Transport.write(socket, Protocol.success(request.fetch(:id)))
          break
        when :ask
          response = begin
            Protocol.success(request.fetch(:id), deliver(actor, request))
          rescue StandardError, ScriptError => error
            Protocol.failure(request.fetch(:id), error)
          end
          begin
            Transport.write(socket, response)
          rescue SerializationError => error
            Transport.write(socket, Protocol.failure(request.fetch(:id), error))
          end
        when :tell
          # No reply can carry an exception, so a failure ends the actor and
          # the broker's restart policy decides what happens next.
          begin
            deliver(actor, request)
          rescue StandardError, ScriptError => error
            report_failure(socket, error)
            socket.close unless socket.closed?
            exit! 1
          end
        end
      end
    rescue IOError, SystemCallError
      nil
    rescue SignalException => error
      die_by_signal(socket, error.signo)
    rescue SystemExit => error
      socket.close unless socket.closed?
      exit!(error.status)
    rescue Exception => error # rubocop:disable Lint/RescueException
      # Anything else escaping the loop is a bug in the actor or the protocol;
      # report it and exit non-zero so it is not mistaken for an orderly exit.
      report_failure(socket, error)
      socket.close unless socket.closed?
      exit! 1
    ensure
      socket.close unless socket.closed?
      exit! 0
    end
    private_class_method :run_actor

    # Ruby turns a fatal signal into an exception; re-deliver it with the default
    # disposition so the watchdog reports a signal death rather than exit 0.
    def die_by_signal(socket, signo)
      socket.close unless socket.closed?
      Signal.trap(signo, "SYSTEM_DEFAULT")
      Process.kill(signo, Process.pid)
      sleep 1
      exit!(128 + signo)
    rescue ArgumentError, SystemCallError
      exit!(128 + signo)
    end
    private_class_method :die_by_signal

    # An actor that defines shutdown gets to flush and close before a graceful
    # stop completes; an exception there is reported as the actor's failure
    # but does not prevent the stop. A crash or KILL never reaches it.
    def shutdown_actor(socket, actor)
      return unless actor.respond_to?(:shutdown)

      actor.shutdown
    rescue StandardError, ScriptError => error
      report_failure(socket, error)
    end
    private_class_method :shutdown_actor

    def deliver(actor, request)
      RocotoActor.context.sender = request[:sender]
      actor.receive(request[:message])
    ensure
      RocotoActor.context.sender = nil
    end
    private_class_method :deliver

    def report_failure(socket, error)
      write_report(socket, error) { |reported| Protocol.actor_error(reported) }
    end
    private_class_method :report_failure

    # Writes the report built by the block; if the error itself cannot be
    # serialized (for example a message with invalid UTF-8), reports that
    # SerializationError instead so the parent still learns why the actor died.
    def write_report(socket, original)
      Transport.write(socket, yield(original))
    rescue SerializationError => error
      begin
        Transport.write(socket, yield(error))
      rescue IOError, SystemCallError, SerializationError
        nil
      end
    rescue IOError, SystemCallError
      nil
    end
    private_class_method :write_report

    # The watchdog owns the actor's process group and kills it when the worker
    # or the application dies. It holds no policy; the broker is the supervisor.
    # It keeps its copy of the socket so it can report how the worker exited.
    def watch(socket, worker_pid, parent_pid)
      loop do
        terminate_group unless Process.ppid == parent_pid
        break if Process.waitpid(worker_pid, Process::WNOHANG)

        sleep RocotoActor::PARENT_CHECK_INTERVAL
      end
      report_exit(socket, $?) # rubocop:disable Style/SpecialGlobalVars
      terminate_group
    rescue Errno::ECHILD
      terminate_group
    end
    private_class_method :watch

    # Tells the parent how the worker process ended, for diagnostics only; the
    # broker's restart policy does not depend on it.
    def report_exit(socket, status)
      return unless status && socket && !socket.closed?

      Transport.write(socket, Protocol.request(:actor_exit, exitstatus: status.exitstatus, termsig: status.termsig))
      socket.close
    rescue IOError, SystemCallError
      nil
    end
    private_class_method :report_exit

    def terminate_group
      Process.kill("KILL", -Process.getpgrp)
      exit! 0
    end
    private_class_method :terminate_group

    def constantize(name)
      name.split("::").reject(&:empty?).reduce(Object) do |namespace, constant_name|
        namespace.const_get(constant_name, false)
      end
    end
    private_class_method :constantize

    # Answers the boot request when its id is known; a failure before the boot
    # message was read is reported as a bare boot_error frame.
    def report_boot_error(socket, error, boot_id = nil)
      return unless socket && !socket.closed?

      write_report(socket, error) do |reported|
        response = Protocol.failure(boot_id, reported)
        response[:op] = :boot_error unless boot_id
        response
      end
    end
    private_class_method :report_boot_error
  end
  private_constant :Runner

  Runner.run
end
