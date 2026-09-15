##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class Actor
  #
  # A handle to an object running in its own, isolated OS process (spawned
  # via fork+exec), reachable only from the local machine over a Unix domain
  # socket. Method calls are transparently forwarded to the real object and
  # its result (or exception) comes back as though the call had happened
  # in-process. This is what provides fault isolation: a hang in the actor
  # -- including an uninterruptible filesystem hang -- can never block
  # whatever holds this handle past its own timeout.
  #
  ##########################################
  class Actor
    require 'json'
    require 'securerandom'
    require 'digest'
    require 'fileutils'
    require 'socket'
    require 'timeout'
    require 'workflowmgr/utilities'

    # Reserved, protocol-level message that stops the actor process itself
    # rather than being forwarded to the real served object.
    STOP_MESSAGE = "__actor_stop__".freeze

    # Bounds a single request read on the server side, so one hung/malformed
    # client can never wedge the actor's ability to serve everyone else.
    READ_TIMEOUT = 30

    RUNNER = File.expand_path("../../sbin/rocotoactor", __dir__)

    class ActorTimeout < StandardError; end
    class ActorUnavailable < StandardError; end

    class << self
      ##########################################
      #
      # spawn
      #
      ##########################################
      def spawn(klass, *args, timeout: 150)
        actor = new(klass, args, timeout: timeout)
        actor.send(:launch!)
        actor
      end

      ##########################################
      #
      # serve
      #
      # Runs forever, dispatching requests from `server` to `real_object`.
      # Called by the generic runner script (sbin/rocotoactor) after it has
      # daemonized and constructed the real object. Kept separate from all
      # of that bootstrapping so it can be exercised directly in tests
      # without forking a whole process.
      #
      ##########################################
      def serve(server, real_object)
        allowed = real_object.class.instance_methods - Object.instance_methods
        loop do
          break unless dispatch_one(server.accept, real_object, allowed)
        end
      ensure
        path = server.addr[1]
        File.delete(path) if path && File.exist?(path)
      end

      ##########################################
      #
      # watch_parent!
      #
      # Runs the given block (default: terminate this process immediately)
      # the moment parent_pid is no longer alive. This is the backstop that
      # works even if the parent was killed with SIGKILL and never got a
      # chance to tell us to stop.
      #
      ##########################################
      def watch_parent!(parent_pid, poll_interval: 10, &on_parent_gone)
        on_parent_gone ||= -> { exit!(0) }
        Thread.new do
          loop do
            begin
              Process.kill(0, parent_pid)
            rescue Errno::ESRCH
              on_parent_gone.call
              break
            end
            sleep poll_interval
          end
        end
      end

      private

      def dispatch_one(conn, real_object, allowed)
        request = Timeout.timeout(READ_TIMEOUT) { JSON.parse(conn.gets.to_s) }
        name = request["method"]

        if name == STOP_MESSAGE
          conn.puts(JSON.generate({ "result" => true }))
          return false
        end

        conn.puts(JSON.generate(build_response(real_object, allowed, name, request["args"] || [])))
        true
      rescue StandardError => e
        begin
          conn.puts(JSON.generate({ "error" => { "class" => e.class.name, "message" => e.message } }))
        rescue StandardError
          nil
        end
        true
      ensure
        conn.close
      end

      def build_response(real_object, allowed, name, args)
        unless allowed.include?(name.to_s.to_sym)
          return { "error" => { "class" => "NoMethodError", "message" => "#{name} is not permitted" } }
        end

        { "result" => real_object.public_send(name, *args) }
      rescue StandardError => e
        { "error" => { "class" => e.class.name, "message" => e.message } }
      end

      def socket_path(klass, args)
        identity = Digest::SHA256.hexdigest("#{klass.name}:#{args.inspect}")[0, 16]
        File.join(socket_dir, "#{identity}-#{Process.pid}-#{SecureRandom.hex(4)}.sock")
      end

      def socket_dir
        dir = File.join(ENV.fetch("HOME"), ".rocoto", WorkflowMgr.version, "tmp")
        FileUtils.mkdir_p(dir, mode: 0o700)
        dir
      end
    end

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(klass, args, timeout: 150)
      @klass = klass
      @args = args
      @timeout = timeout
      @allowed = klass.instance_methods - Object.instance_methods
    end

    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name, *args)
      return super unless @allowed.include?(name)

      call(name, args)
    end

    def respond_to_missing?(name, include_private = false)
      @allowed.include?(name) || super
    end

    ##########################################
    #
    # stop!
    #
    ##########################################
    def stop!
      call(STOP_MESSAGE, [], allow_relaunch: false)
    rescue ActorUnavailable, ActorTimeout
      nil
    ensure
      reap
    end

    private

    ##########################################
    #
    # reap
    #
    # A successful stop! means the actor is already exiting, not hung, so a
    # brief bounded retry (unlike anywhere we might be dealing with a wedged
    # actor) is safe here: it just gives the process a moment to actually
    # finish unwinding and become reapable before we give up.
    #
    ##########################################
    def reap
      10.times do
        _reaped_pid, status = Process.waitpid2(@pid, Process::WNOHANG)
        return status unless status.nil?

        sleep 0.1
      end
    rescue Errno::ECHILD
      nil
    end

    ##########################################
    #
    # launch!
    #
    ##########################################
    def launch!
      @socket_path = self.class.send(:socket_path, @klass, @args)
      server = UNIXServer.new(@socket_path)
      File.chmod(0o600, @socket_path)

      # The runner starts as a fresh interpreter that has never loaded the
      # served class, so it needs to know which file defines it. Since the
      # caller already had to load @klass to reference it here, we can look
      # that up ourselves instead of asking the developer for it.
      source_file = Object.const_source_location(@klass.name)&.first

      # Must be captured before forking: inside the fork block (pre-exec),
      # Process.pid is the *child's* own pid, not the real caller's.
      parent_pid = Process.pid

      @pid = fork do
        exec(RbConfig.ruby, RUNNER, @klass.name, source_file.to_s, JSON.generate(@args),
             server.fileno.to_s, parent_pid.to_s, server => server)
      end
      server.close
    end

    ##########################################
    #
    # relaunch!
    #
    ##########################################
    def relaunch!
      begin
        Process.waitpid(@pid, Process::WNOHANG)
      rescue Errno::ECHILD
        nil
      end
      File.delete(@socket_path) if File.exist?(@socket_path)
      launch!
    end

    ##########################################
    #
    # call
    #
    ##########################################
    def call(name, args, allow_relaunch: true)
      attempts = 0
      begin
        WorkflowMgr.timeout(@timeout) { send_request(name, args) }
      rescue Timeout::Error
        raise ActorTimeout, "Actor #{@klass} (pid #{@pid}) did not respond within #{@timeout} seconds"
      rescue Errno::ECONNREFUSED, Errno::ECONNRESET, Errno::ENOENT, EOFError => e
        if allow_relaunch && attempts.zero?
          attempts += 1
          relaunch!
          retry
        end
        raise ActorUnavailable, "Actor #{@klass} (pid #{@pid}) is unavailable: #{e.message}"
      end
    end

    def send_request(name, args)
      conn = UNIXSocket.new(@socket_path)
      conn.puts(JSON.generate({ "method" => name, "args" => args }))
      response = JSON.parse(conn.gets.to_s)
      conn.close
      raise_remote_error(response["error"]) if response["error"]

      response["result"]
    end

    def raise_remote_error(error)
      error_class = begin
        Object.const_get(error["class"])
      rescue NameError
        nil
      end
      error_class = RuntimeError unless error_class.is_a?(Class) && error_class <= Exception
      raise error_class, error["message"]
    end
  end
end
