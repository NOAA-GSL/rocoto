##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowIOProxy
  #
  # Filesystem access on behalf of the workflow, kept at arm's length.
  #
  # Rocoto checks data dependencies constantly, and on HPC systems those
  # paths live on parallel filesystems that occasionally stop answering in a
  # way no signal can interrupt. A stat on one of those leaves the calling
  # process in uninterruptible sleep, which is why the work happens in an
  # actor: a hang there costs a deadline, not the run.
  #
  # Remembering what is wedged is the other half, and it stays here rather
  # than in the actor -- the actor may be the thing that is wedged. What gets
  # remembered is the *mount*, because that is the unit that actually fails:
  # a filesystem hangs as a whole, not one directory at a time. Mounts are
  # read from /proc/self/mountinfo, which is procfs and so cannot itself hang
  # on the filesystem in question.
  #
  # A mount is a big thing to refuse, so nothing is refused forever: a block
  # older than BLOCK_TTL is retested by a throwaway actor that stats the
  # mount, and a mount that answers is unblocked. Blocks belong to the host
  # that recorded them, since a mount wedged on one node says nothing about
  # the same mount on another.
  #
  ##########################################
  class WorkflowIOProxy
    require 'socket'
    require 'workflowmgr/workflowio'
    require 'workflowmgr/actor'
    require 'workflowmgr/utilities'

    # How long to wait for a filesystem operation before calling it hung. A
    # healthy stat answers in milliseconds, so this is generous by orders of
    # magnitude -- but deciding wrongly is not free: the mount is refused for
    # the rest of this run, and the tasks that read or write there go
    # unsubmitted. A filesystem merely slow under load is the case to watch.
    IO_TIMEOUT = 30

    # How long a retest gets. This is not the same budget as IO_TIMEOUT and
    # must not be mistaken for one: a probe is a brand new process, so it
    # pays fork+exec and loading workflowmgr before it can stat anything at
    # all. A budget that only covers the stat declares a healthy mount dead
    # on a loaded machine -- the same mistake that made the hang specs fail
    # under load, with a far worse consequence here.
    PROBE_TIMEOUT = 20

    # How old a block must be before a retest is worth the process it costs.
    BLOCK_TTL = 900

    # The one mount that is never blocked. Everything is under it, so
    # refusing it refuses the workflow database, the rocoto install and every
    # dependency at once -- the whole failure this design exists to avoid. A
    # hang that resolves to / is remembered as the exact path instead.
    ROOT_MOUNT = "/".freeze

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(db_server, config, options)
      @db_server = db_server
      @config = config
      @options = options
      @stopped = false

      # Resolved once, and before anything is spawned: a sick name service
      # tends to keep company with a sick filesystem, and this must not be
      # able to fail in between starting a server and recording its identity.
      @host = Socket.getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3]

      @blocks = load_blocks
      @retested = {}

      start_server
    end

    ##########################################
    #
    # method_missing
    #
    # Everything WorkflowIO serves goes through the same two steps: refuse
    # what is known to be wedged, then hand the call to the actor.
    #
    ##########################################
    def method_missing(name, *args, &block)
      return super unless served?(name)

      # It would be dropped silently on the way across, so say so instead.
      raise ArgumentError, "#{name} was given a block, which cannot be sent to the io process" if block

      path = args.first
      refuse_blocked(path)
      forward(name, args, path)
    end

    def respond_to_missing?(name, include_private = false)
      served?(name) || super
    end

    ##########################################
    #
    # stop!
    #
    ##########################################
    def stop!
      @stopped = true
      @server.stop! if @server.respond_to?(:stop!)
      @server = nil
    end

    private

    def served?(name)
      WorkflowIO.public_instance_methods(false).include?(name)
    end

    # Asked of the server itself rather than remembered, so it cannot go
    # stale: a cached pid that outlives its process is one that gets blamed
    # for a hang after the number has been handed to somebody else.
    def server_pid
      @server.respond_to?(:actor_pid) ? @server.actor_pid : Process.pid
    end

    ##########################################
    #
    # db_quietly
    #
    # Runs a database call whose failure must not end the run.
    #
    # Two separate things are needed for that, and the second is easy to
    # miss. Rescuing stops the error escaping to callers, who rescue
    # WorkflowIOHang and nothing else. But the database is an actor shared
    # with the rest of rocoto, and a timed-out call leaves a reply
    # outstanding on its handle, so every later call by anyone raises
    # ActorBusy -- the run still ends, just later and blaming something
    # unrelated. Discarding the reply is what actually restores it.
    #
    ##########################################
    def db_quietly
      yield
      true
    rescue StandardError => e
      @db_server.discard_pending! if @db_server.respond_to?(:discard_pending!)
      WorkflowMgr.stderr(e.message, 2)
      WorkflowMgr.log(e.message)
      false
    end

    ##########################################
    #
    # load_blocks
    #
    # Blocks recorded by earlier runs. Only this host's are kept: a mount
    # wedged on another node says nothing about this one, and acting on it
    # would refuse work that would have run perfectly well. Entries with no
    # path are dropped -- older rocotos recorded an empty string for a hang
    # on a top-level file, and an empty prefix matches every path there is.
    #
    ##########################################
    def load_blocks
      entries = @db_server.load_downpaths
      entries.select { |entry| this_host?(entry) && !entry[:path].to_s.empty? }
    rescue StandardError => e
      # Losing this costs only the memory of what was wedged before, and the
      # first check to hang will record it again. Ending the run over it
      # would cost far more, and a cron-driven rocoto would keep paying it.
      @db_server.discard_pending! if @db_server.respond_to?(:discard_pending!)
      WorkflowMgr.stderr("WARNING! rocoto could not read the list of unresponsive " \
                         "filesystems: #{e.message}", 1)
      WorkflowMgr.log(e.message)
      []
    end

    def this_host?(entry)
      entry[:host].to_s == @host
    end

    ##########################################
    #
    # start_server
    #
    # In dryrun, or when the user has turned the io server off, the work
    # happens in this process: there is nothing to isolate it from, and no
    # process to pay for.
    #
    ##########################################
    def start_server
      @server = if @config.WorkflowIOServer && !WorkflowMgr.dryrun_mode?
                  Actor.spawn(WorkflowIO, timeout: IO_TIMEOUT)
                else
                  WorkflowIO.new
                end
    rescue StandardError => e
      WorkflowMgr.stderr(e.message, 1)
      WorkflowMgr.log(e.message)
      raise "Could not launch IO server process."
    end

    ##########################################
    #
    # ensure_server
    #
    ##########################################
    def ensure_server
      raise WorkflowIOHang, "WARNING! rocoto io has been shut down and cannot access the filesystem." if @stopped
      return unless @server.nil?

      begin
        start_server
      rescue StandardError => e
        # Raised as a hang, not as itself: every caller of this proxy knows
        # how to carry on without a filesystem answer, and none of them
        # rescues anything else. An io process we cannot start is no worse
        # for them than one that will not answer.
        raise WorkflowIOHang, "WARNING! rocoto io process could not be started: #{e.message}"
      end
    end

    ##########################################
    #
    # replace_wedged_server
    #
    # The old actor is abandoned rather than reused. It may be stuck in a
    # syscall that will not return, so nothing waits on it; the kernel
    # collects it if and when the filesystem recovers. This is the one place
    # an actor is deliberately replaced, and it is safe here only because
    # WorkflowIO holds no state worth carrying over.
    #
    ##########################################
    def replace_wedged_server
      old = @server
      @server = nil
      old.stop! if old.respond_to?(:stop!)
      start_server
    rescue StandardError => e
      WorkflowMgr.stderr(e.message, 1)
      WorkflowMgr.log(e.message)
    end

    ##########################################
    #
    # forward
    #
    ##########################################
    def forward(name, args, path)
      ensure_server
      retried = false

      begin
        @server.public_send(name, *args)
      rescue Actor::ActorTimeout
        # Captured before anything is replaced: this identifies the process
        # that actually hung, which is what gets recorded.
        hung_pid = server_pid

        begin
          block_for(path)
        ensure
          replace_wedged_server
        end

        raise WorkflowIOHang, "WARNING! rocoto io process #{hung_pid} on host #{@host} " \
                              "is unresponsive while accessing #{path} and is probably wedged."
      rescue Actor::ActorUnavailable => e
        if retried
          # Same reasoning as ensure_server: callers rescue WorkflowIOHang
          # and nothing else, and an io process that keeps dying should cost
          # a dependency check rather than the whole run.
          raise WorkflowIOHang, "WARNING! rocoto io could not be restarted while accessing #{path}: #{e.message}"
        end

        retried = true
        msg = "WARNING! The rocoto io process #{server_pid} on host #{@host} died. " \
              "Attempting to restart and try again."
        WorkflowMgr.stderr(msg, 2)
        WorkflowMgr.log(msg)
        replace_wedged_server
        ensure_server
        retry
      end
    end

    ##########################################
    #
    # refuse_blocked
    #
    # A path on a filesystem known to be wedged is not touched at all. If the
    # block is old enough to be worth doubting, the mount is retested first,
    # and a mount that answers is unblocked and the path tried normally.
    #
    ##########################################
    def refuse_blocked(path)
      return unless path.is_a?(String)

      blocking = @blocks.find { |block| covers?(block, path) }
      return if blocking.nil?
      return if recovered?(blocking)

      raise WorkflowIOHang, "WARNING! rocoto cannot attempt to access #{path}, because " \
                            "#{blocking[:path]} stopped responding at #{blocking[:downtime]} " \
                            "and has not answered since."
    end

    # Compared by whole path components, never as raw text: /scratch must not
    # cover /scratchX, which is somebody else's filesystem entirely.
    def covers?(block, path)
      blocked = block[:path].to_s
      return false if blocked.empty?
      return false unless path.is_a?(String)
      return true if path == blocked
      return path.start_with?("/") if blocked == ROOT_MOUNT

      path.start_with?("#{blocked}/")
    end

    ##########################################
    #
    # recovered?
    #
    # Retests a block, at most once per run and only once it is older than
    # the TTL, by asking a throwaway actor to stat the thing that stopped
    # answering. A stat that returns at all -- true or false -- means the
    # filesystem is talking again.
    #
    # Deliberately not re-armed on failure: refreshing the timestamp every
    # time a check was refused would keep a block permanently young and the
    # filesystem permanently distrusted, long after it recovered.
    #
    ##########################################
    def recovered?(block)
      target = block[:path]
      return false if @retested.key?(target)
      return false if Time.now - block[:downtime] < BLOCK_TTL

      @retested[target] = true
      return false unless answers?(target)

      msg = "#{target} is responding again, and is no longer being avoided."
      WorkflowMgr.stderr(msg, 1)
      WorkflowMgr.log(msg)
      unblock(block)
      true
    end

    # One throwaway actor, whose only job is to touch the suspect filesystem
    # so that this process never has to. If it wedges it is abandoned exactly
    # as the io actor is: it cannot be killed until its syscall returns, and
    # the kernel collects it when that happens. At most one is ever spawned
    # per mount per run, so a filesystem that stays down cannot accumulate
    # unkillable processes check after check.
    def answers?(target)
      probe = Actor.spawn(WorkflowIO, timeout: PROBE_TIMEOUT)
      begin
        probe.exist?(target)
        true
      rescue Actor::ActorTimeout
        false
      ensure
        probe.stop!
      end
    rescue StandardError => e
      WorkflowMgr.stderr("WARNING! rocoto could not test whether #{target} is responding: #{e.message}", 2)
      WorkflowMgr.log(e.message)
      false
    end

    def unblock(block)
      @blocks.delete(block)
      db_quietly { @db_server.delete_downpaths([block]) }
    end

    ##########################################
    #
    # block_for
    #
    # Remembers that a path stopped answering, as the mount it lives on.
    #
    # The mount is the honest unit: filesystems hang whole, and blaming a
    # directory means every sibling directory hangs the run again in turn.
    # Where the mount cannot be blamed -- it is /, or the path is relative
    # and so cannot be placed -- the exact path is remembered instead.
    # Remembering nothing is not an option: the path would be retried by
    # every check, for the whole run and every run after it, each one paying
    # IO_TIMEOUT before giving up.
    #
    ##########################################
    def block_for(path)
      return unless path.is_a?(String) && !path.empty?

      target = mount_for(path) || path

      # A backstop rather than a live guard: refuse_blocked declines this
      # call before it ever reaches the actor when a block already covers
      # the path, and it decides that with the same test mount_for uses to
      # choose the target. So nothing can currently arrive here twice for
      # one target. Kept because it costs nothing and a duplicate row would
      # outlive the run that wrote it.
      return if @blocks.any? { |block| block[:path] == target }

      block = { path: target, downtime: Time.now, host: @host, pid: server_pid }
      @blocks << block
      db_quietly { @db_server.add_downpaths([block]) }

      msg = "WARNING! #{target} stopped responding while accessing #{path}; " \
            "rocoto will avoid it until it answers again."
      WorkflowMgr.stderr(msg, 1)
      WorkflowMgr.log(msg)
    end

    ##########################################
    #
    # mount_for
    #
    # The mount point a path lives on, by longest match on whole components.
    # Returns nil when the answer would be useless or dangerous: a relative
    # path cannot be placed without resolving it, and resolving touches the
    # filesystem that is currently hanging; and / covers everything.
    #
    # The path is normalised only lexically, for the same reason. That is
    # wrong in the presence of symlinks, which is why a wrong answer here
    # costs only precision: the exact path is blocked instead.
    #
    ##########################################
    def mount_for(path)
      normalized = normalize(path)
      return nil if normalized.nil?

      best = mount_points.select { |point| covers?({ path: point }, normalized) }
                         .max_by(&:length)
      return nil if best.nil? || best == ROOT_MOUNT

      best
    end

    def normalize(path)
      return nil unless path.start_with?("/")

      components = []
      path.split("/").each do |component|
        next if component.empty? || component == "."

        component == ".." ? components.pop : components << component
      end
      "/#{components.join('/')}"
    end

    ##########################################
    #
    # mount_points
    #
    # Read from procfs, which answers even when the filesystems it describes
    # do not -- that is the whole reason this is how mounts are identified.
    # The kernel octal-escapes space, tab, newline and backslash in the
    # mount point field, so a mount under a directory with a space in its
    # name would otherwise never match anything.
    #
    ##########################################
    def mount_points
      File.readlines("/proc/self/mountinfo").filter_map do |line|
        field = line.split(" ")[4]
        unescape(field) unless field.nil?
      end
    rescue StandardError
      # Without mountinfo there is no mount to blame, so callers fall back to
      # blocking the exact path. That is worth no more than a quiet note.
      []
    end

    def unescape(field)
      field.gsub(/\\(\d{3})/) { Regexp.last_match(1).to_i(8).chr }
    end
  end
end
