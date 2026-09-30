##########################################
#
# module WorkflowMgr
#
# Filesystem access, served from a process of its own.
#
# An actor is a fresh Ruby VM that loads exactly one file, and it inherits
# the application's environment but not its $LOAD_PATH or its bundle, so
# this file bootstraps both the way sbin/rocotorun.rb does.
#
##########################################
standalone = File.expand_path('../../bundle/bundler/setup.rb', __dir__)
require standalone if File.exist?(standalone)
rocoto_lib = File.expand_path('..', __dir__)
$LOAD_PATH.unshift(rocoto_lib) unless $LOAD_PATH.include?(rocoto_lib)

require 'workflowmgr/workflowio'

module WorkflowMgr
  ##########################################
  #
  # Module IOWire
  #
  # Turns what the filesystem gives back into what the transport can carry.
  #
  # Two things need help. A Time, because mtime returns one. And file
  # contents, because the transport carries only text that is valid UTF-8 --
  # a string that is valid in some other encoding is transcoded rather than
  # carried byte for byte, and one that is invalid in its own encoding is
  # refused outright. A workflow document written in latin-1 is neither of
  # those things by accident: File.read tags what it returns with the
  # process's default external encoding, which with no locale set is
  # US-ASCII, so a single accented byte makes it invalid. Those contents
  # travel base64 and arrive byte for byte.
  #
  ##########################################
  module IOWire
    module_function

    def encode(value)
      case value
      when Time then { wire: :time, at: value.to_i }
      when String then encode_string(value)
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

    # Text that is already valid UTF-8 travels as itself, which is the
    # ordinary case and costs nothing. Everything else travels as bytes.
    def encode_string(value)
      return value if value.encoding == Encoding::UTF_8 && value.valid_encoding?

      as_utf8 = value.dup.force_encoding(Encoding::UTF_8)
      return as_utf8 if as_utf8.valid_encoding?

      { wire: :bytes, encoding: value.encoding.name, base64: [value.b].pack('m0') }
    end

    def decode_hash(hash)
      case hash[:wire]
      when :time then Time.at(hash[:at])
      when :bytes then hash[:base64].unpack1('m0').force_encoding(hash[:encoding])
      else hash.to_h { |key, value| [decode(key), decode(value)] }
      end
    end
  end

  ##########################################
  #
  # Class IOActor
  #
  # What runs in the io process. It owns a WorkflowIO and answers one
  # message at a time.
  #
  # The operations it serves are taken from WorkflowIO itself rather than
  # listed here, so the two cannot drift apart. Anything else is refused
  # rather than forwarded.
  #
  ##########################################
  class IOActor
    OPERATIONS = (WorkflowIO.public_instance_methods(false) - Object.public_instance_methods).freeze

    def initialize
      @io = WorkflowIO.new
    end

    def receive(message)
      operation = message[:op]
      raise NoMethodError, "#{operation} is not a filesystem operation" unless OPERATIONS.include?(operation)

      IOWire.encode(@io.public_send(operation, *IOWire.decode(message[:args])))
    end
  end
end
