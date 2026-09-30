##########################################
#
# Module WorkflowMgr
#
##########################################
require 'English'
require 'fileutils'
module WorkflowMgr
  # DRYRUN controls whether workflow operations are executed or just logged.
  # It defaults to 0 (off), but can be overridden via the WORKFLOWMGR_DRYRUN
  # or DRYRUN environment variables, or by explicit assignment in an
  # entrypoint script before this file is loaded.
  DRYRUN = (ENV['WORKFLOWMGR_DRYRUN'] || ENV['DRYRUN'] || '0').to_i unless const_defined?(:DRYRUN)

  #####################################################
  # Helper method to check if we're in dry run mode
  #####################################################
  def self.dryrun_mode?
    DRYRUN.to_i > 0
  end

  require 'timeout'
  require 'open4'

  ##########################################
  #
  # Class WorkflowIOHang
  #
  ##########################################
  class WorkflowIOHang < RuntimeError
  end

  ##########################################
  #
  # Class SchedulerDown
  #
  ##########################################
  class SchedulerDown < RuntimeError
  end

  ##########################################
  #
  # WorkflowMgr.version
  #
  ##########################################
  def self.version
    IO.readlines("#{File.expand_path('../..', __dir__)}/VERSION", nil)[0].strip
  end

  ##########################################
  #
  # WorkflowMgr.timeout
  #
  ##########################################
  def self.timeout(seconds, &block)
    Timeout.timeout(seconds, &block)
  end

  ##########################################
  #
  # WorkflowMgr.ddhhmmss_to_seconds
  #
  ##########################################
  def self.ddhhmmss_to_seconds(ddhhmmss)
    secs = 0
    unless ddhhmmss.nil?
      sign = ddhhmmss[/^-/].nil? ? 1 : -1
      ddhhmmss.split(":").reverse.each_with_index do |i, index|
        if index == 3
          secs += i.to_i.abs * 3600 * 24
        elsif index < 3
          secs += i.to_i.abs * 60**index
        else
          raise "Invalid dd:hh:mm:ss, '#{ddhhmmss}'"
        end
      end
      secs *= sign
    end
    secs
  end

  ##########################################
  #
  # WorkflowMgr.seconds_to_hhmmss
  #
  ##########################################
  def self.seconds_to_hhmmss(seconds)
    s = seconds
    hours = (s / 3600.0).floor
    s -= hours * 3600
    minutes = (s / 60.0).floor
    s -= minutes * 60
    seconds = s

    format("%<hours>0d:%<minutes>02d:%<seconds>02d", hours: hours, minutes: minutes, seconds: seconds)
  end

  ##########################################
  #
  # WorkflowMgr.seconds_to_hhmm
  #
  ##########################################
  def self.seconds_to_hhmm(seconds)
    s = seconds
    hours = (s / 3600.0).floor
    s -= hours * 3600
    minutes = (s / 60.0).ceil
    if minutes > 59
      hours += 1
      minutes = 0
    end

    format("%<hours>0d:%<minutes>02d", hours: hours, minutes: minutes)
  end

  ##########################################
  #
  # WorkflowMgr.config_set
  #
  ##########################################
  def self.config_set(config)
    WorkflowMgr.const_set("CONFIG", config)
  end

  ##########################################
  #
  # WorkflowMgr.options_set
  #
  ##########################################
  def self.options_set(options)
    WorkflowMgr.const_set("OPTIONS", options)
  end

  ##########################################
  #
  # WorkflowMgr.message_sink
  #
  # When set, messages are handed to this instead of being written out. An
  # actor installs one: its stderr goes to /dev/null and it knows neither
  # the workflow id nor how verbose the user asked things to be, so whatever
  # it has to say must travel back to the process that does know.
  #
  ##########################################
  class << self
    attr_accessor :message_sink
  end

  ##########################################
  #
  # WorkflowMgr.report_actor_error
  #
  # Handed to an ActorBroker as its error_handler. A failure on one of the
  # broker's own threads belongs to no single call and can be reported
  # nowhere else; the library's default writes a bare line to stderr, which
  # carries no workflow id and ignores the verbosity the user asked for.
  #
  ##########################################
  def self.report_actor_error(error, context)
    message = "WARNING: rocoto actor system failed while #{context}: #{error.class}: #{error.message}"
    stderr(message, 1)
    log(message)
    nil
  rescue StandardError
    # The broker undertakes never to let this handler's own failure escape
    # onto one of its threads, and this keeps that cheap to honour.
    nil
  end

  ##########################################
  #
  # WorkflowMgr.report_actor_event
  #
  # Handed to an ActorBroker as its on_event. An actor that dies while
  # nothing happens to be talking to it would otherwise go unmentioned until
  # some later call stumbled over it.
  #
  ##########################################
  def self.report_actor_event(event, handle, detail)
    # An orderly stop is routine and belongs in the log alone. Anything else
    # explains behaviour the user can see.
    level = event.to_s == 'stopped' ? 3 : 1
    message = "rocoto actor #{actor_name(handle)} #{event}"
    message += " (generation #{detail[:generation]})" unless detail[:generation].nil?
    message += ": #{detail[:reason]}" unless detail[:reason].nil?
    stderr(message, level)
    log(message)
    nil
  rescue StandardError
    nil
  end

  # The path is the name the actor was given and is what a reader will
  # recognise; the id is a fallback for a node too far gone to answer.
  def self.actor_name(handle)
    handle.path
  rescue StandardError
    handle.id
  end
  private_class_method :actor_name

  ##########################################
  #
  # WorkflowMgr.stderr
  #
  ##########################################
  def self.stderr(message, level = 0)
    return if message.nil?
    return if message.empty?
    return message_sink.call(:stderr, message, level) if message_sink

    verbose = defined?(WorkflowMgr::VERBOSE) ? WorkflowMgr::VERBOSE : 0
    workflow_id = defined?(WorkflowMgr::WORKFLOW_ID) ? WorkflowMgr::WORKFLOW_ID : 'unknown'

    if level <= verbose
      warn "#{Time.now.strftime('%x %X %Z')} :: #{workflow_id} :: #{message}"
    end
  end

  ##########################################
  #
  # WorkflowMgr.log
  #
  ##########################################
  def self.log(message)
    return if message.nil?
    return if message.empty?
    return message_sink.call(:log, message, nil) if message_sink

    # Get workflow ID, or use 'unknown' if not defined
    workflow_id = defined?(WorkflowMgr::WORKFLOW_ID) ? WorkflowMgr::WORKFLOW_ID : 'unknown'

    # Name of the current log file
    rocotolog = "#{ENV['HOME']}/.rocoto/#{WorkflowMgr.version}/#{workflow_id.sub(/.xml$/, '')}/log"

    # Create the log directory if it doesn't exist
    logdir = File.dirname(rocotolog)
    FileUtils.mkdir_p(logdir) unless File.directory?(logdir)

    # Logging requires exclusive access to the logs
    # Open the log lock file
    File.open("#{rocotolog}.lock", "w") do |lockfile|
      # Try up to three times to acquire an exclusive write lock for the lock file
      got_lock = false
      5.times do
        got_lock = lockfile.flock(File::LOCK_EX | File::LOCK_NB)
        if got_lock
          break
        end

        sleep rand
      end

      # If we get the lock, proceed with logging and rotation if needed
      if got_lock

        begin
          # Only rotate logs if they exist
          if File.exist?(rocotolog)

            # Determine if it is time to rotate the logs
            log_mod_time = File.mtime(rocotolog)
            rotate = log_mod_time.day != Time.now.day

            if rotate

              # Rotate log
              FileUtils.mv(rocotolog, rocotolog + ".#{log_mod_time.strftime('%Y%m%d')}")

              # Get the max age (in days) of the log file from the configuration
              # NOTE: This is a hack due to poor design preventing proper access to the configuration object
              rocotorc = "#{ENV['HOME']}/.rocoto/#{WorkflowMgr.version}/rocotorc"
              max_age = YAML.safe_load_file(rocotorc, permitted_classes: [Symbol], symbolize_names: true)[:MaxLogDays]

              # Remove files last modified more than MaxAge days ago
              Dir["#{rocotolog}.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"].each do |logfile|
                next unless (Time.now - File.mtime(logfile)) > (max_age * 24 * 3600)

                FileUtils.rm_f(logfile)
              end

            end

          end

          # Log the message
          File.open(rocotolog, "a") do |f|
            f.puts "#{Time.now.strftime('%x %X %Z')} :: #{workflow_id} :: #{message}"
          end
        ensure
          # Make sure the lock is released
          lockfile.flock(File::LOCK_UN)
        end

      else
        warn "#{Time.now.strftime('%x %X %Z')} :: #{workflow_id} :: " \
             "WARNING! Could not acquire lock to write log the following message"
        warn "#{Time.now.strftime('%x %X %Z')} :: #{workflow_id} ::          #{message}"
      end
    end
  end

  ##########################################
  #
  # WorkflowMgr.run4
  #
  ##########################################
  def self.run4(command, timeout = 30)
    # Turn off garbage collection to avoid possible seg faults caused by Ruby
    # trying to allocate objects during GC when calling IO.select and IO#read.
    # See: http://tickets.opscode.com/browse/CHEF-2916
    GC.disable

    begin
      pid, stdin, stdout, stderr = Open4.popen4(command)
      stdin.close
    rescue StandardError
      raise "Execution of '#{command}' unsuccessful: #{$ERROR_INFO}"
    end

    error = ""
    output = ""
    exit_status = 0
    begin
      WorkflowMgr.timeout(timeout) do
        output += stdout.read until stdout.eof?
        stdout.close

        error += stderr.read until stderr.eof?
        stderr.close

        status = Process.waitpid2(pid)
        exit_status = status[1].exitstatus
      end
    rescue Timeout::Error
      Process.kill('KILL', pid)
      stdout.close
      stderr.close
      WorkflowMgr.log("WARNING! The command '#{command}' timed out after #{timeout} seconds.")
      raise Timeout::Error
    end
    [output, error, exit_status]
  end
end
