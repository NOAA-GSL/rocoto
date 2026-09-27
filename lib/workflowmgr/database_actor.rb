##########################################
#
# module WorkflowMgr
#
# The workflow database, served from a process of its own.
#
# An actor is a fresh Ruby VM that loads exactly one file, and it inherits
# the application's environment but not its $LOAD_PATH or its bundle. So
# this file bootstraps both before requiring anything of rocoto's, the same
# way sbin/rocotorun.rb does.
#
##########################################
standalone = File.expand_path('../../bundle/bundler/setup.rb', __dir__)
require standalone if File.exist?(standalone)
rocoto_lib = File.expand_path('..', __dir__)
$LOAD_PATH.unshift(rocoto_lib) unless $LOAD_PATH.include?(rocoto_lib)

require 'workflowmgr/workflowdb'

module WorkflowMgr
  ##########################################
  #
  # Module DatabaseWire
  #
  # Turns what the database deals in into what the transport can carry, and
  # back again.
  #
  # The transport carries nil, booleans, strings, integers, floats, symbols,
  # arrays and hashes -- including symbol keys and non-string keys, which is
  # most of what a codec would otherwise be for. Only three of rocoto's
  # types are left over: Time, Job and Cycle. Job and Cycle already describe
  # themselves as plain values through to_wire/from_wire, so all that is
  # really needed is Time, and a walk that reaches it wherever it appears --
  # bare, inside a hash, or as a hash *key*, which is how load_jobs returns
  # {task => {cycle_time => job}}.
  #
  # Times are carried as epoch seconds because that is exactly what SQLite
  # stores and what every one of these values came from; none of them has
  # sub-second precision to lose. They come back as UTC, since the database
  # builds them all with getgm.
  #
  # The tag is a hash with a :wire key. None of rocoto's own hashes uses
  # that key -- they are keyed by :group, :path, :readonly, :start and the
  # like -- so tagged values can never be confused with data.
  #
  ##########################################
  module DatabaseWire
    module_function

    def encode(value)
      case value
      when Time then { wire: :time, at: value.to_i }
      when Job then { wire: :job, fields: encode(value.to_wire) }
      when Cycle then { wire: :cycle, fields: encode(value.to_wire) }
      when Array then value.map { |element| encode(element) }
      when Hash then value.to_h { |key, element| [encode(key), encode(element)] }
      else value
      end
    end

    def decode(value)
      case value
      when Array then value.map { |element| decode(element) }
      when Hash then decode_hash(value)
      else value
      end
    end

    def decode_hash(hash)
      case hash[:wire]
      when :time then Time.at(hash[:at]).getgm
      when :job then Job.from_wire(decode(hash[:fields]))
      when :cycle then Cycle.from_wire(decode(hash[:fields]))
      else hash.to_h { |key, value| [decode(key), decode(value)] }
      end
    end
  end

  ##########################################
  #
  # Class DatabaseActor
  #
  # What runs in the database process. It owns a WorkflowSQLite3DB and
  # answers one message at a time, which is the serialisation the database
  # wanted anyway.
  #
  # The operations it will serve are taken from the database class itself
  # rather than listed here, so the two cannot drift apart as the database
  # grows methods. Anything else is refused rather than forwarded.
  #
  # The owner pid is the application's, not this process's: the workflow
  # lock belongs to rocoto and has to outlive any actor that writes it.
  #
  ##########################################
  class DatabaseActor
    OPERATIONS = (WorkflowSQLite3DB.public_instance_methods(false) - Object.public_instance_methods).freeze

    def initialize(database_file, owner_pid)
      @database = WorkflowSQLite3DB.new(database_file, owner_pid)
    end

    def receive(message)
      operation = message[:op]
      raise NoMethodError, "#{operation} is not a database operation" unless OPERATIONS.include?(operation)

      DatabaseWire.encode(@database.public_send(operation, *DatabaseWire.decode(message[:args])))
    end

    # Deliberately no shutdown hook. The run releases the workflow lock in
    # its own ensure block, before the broker is stopped, and a second
    # unlock here would find nothing to release and warn about it -- on
    # every ordinary run, for no reason.
  end

  ##########################################
  #
  # Class DatabaseProxy
  #
  # The database as the rest of rocoto sees it: the same calls, answered by
  # another process.
  #
  # Every call is an ask rather than a tell. Most of them return something
  # the caller needs immediately -- lock_workflow's answer decides whether
  # the run proceeds at all -- and an actor has no way to send anything back
  # to the application except as a reply. A told message that raises would
  # also end the actor, and WorkflowDBLockedException is routine enough that
  # BusyRetry exists for it.
  #
  ##########################################
  class DatabaseProxy
    # As generous as the DRb proxy this replaces. A database call that has
    # not answered in this long is not slow, it is wedged.
    TIMEOUT = 150

    def initialize(handle)
      @handle = handle
    end

    def method_missing(name, *args, &block)
      return super unless DatabaseActor::OPERATIONS.include?(name)

      raise ArgumentError, "#{name} was given a block, which cannot be sent to the database" if block

      DatabaseWire.decode(@handle.ask(op: name, args: DatabaseWire.encode(args)).value(timeout: TIMEOUT))
    end

    def respond_to_missing?(name, include_private = false)
      DatabaseActor::OPERATIONS.include?(name) || super
    end

    # The engines stop the database in their ensure blocks, before the
    # broker itself goes, exactly as they did when it was a DRb server.
    def stop!
      @handle.stop
    end

    # The handle, for anything that needs to know which process is serving
    # the database.
    attr_reader :handle
  end
end
