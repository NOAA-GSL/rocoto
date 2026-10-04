# frozen_string_literal: true

require "json"

module RocotoActor
  module Transport
    MAX_FRAME_SIZE = 16 * 1024 * 1024
    MAX_NESTING = 100
    HEADER_SIZE = 4

    module_function

    def write(io, message = nil, **fields)
      message = fields unless fields.empty?
      write_payload(io, dump(message))
    end

    def dump(message)
      payload = JSON.generate(encode(message))
      raise SerializationError, "message exceeds #{MAX_FRAME_SIZE} bytes" if payload.bytesize > MAX_FRAME_SIZE

      payload
    rescue JSON::JSONError, EncodingError => error
      raise SerializationError, error.message
    end

    # Writes block until the frame is in the socket buffer; the sending side's
    # mailbox bounds what can queue behind a blocked write. The length header is
    # a binary string and the payload is UTF-8, so they are written as two
    # arguments rather than concatenated: joining them raises
    # Encoding::CompatibilityError whenever both carry a byte above 0x7F, which
    # is every multi-byte payload whose length happens to have a high byte.
    def write_payload(io, payload)
      io.write([payload.bytesize].pack("N"), payload)
    end

    # Blocks until a whole frame arrives; nil at a clean end of stream. There is
    # deliberately no read timeout: a partial frame abandoned on a deadline
    # would desynchronize the stream, so callers bound waiting elsewhere.
    def read(io, bindings: DecodeBindings.new)
      header = read_exactly(io, HEADER_SIZE)
      return if header.nil?

      size = header.unpack1("N")
      raise Error, "invalid frame size: #{size}" if size > MAX_FRAME_SIZE

      decode(JSON.parse(read_exactly(io, size)), bindings)
    rescue JSON::JSONError => error
      raise SerializationError, error.message
    end

    def encode(value, seen = {}.compare_by_identity, depth = 0)
      raise SerializationError, "value exceeds #{MAX_NESTING} nesting levels" if depth > MAX_NESTING

      case value
      when nil then ["nil"]
      when true, false then ["boolean", value]
      when String
        raise SerializationError, "string is not valid UTF-8 (#{value.encoding})" unless encodable_string?(value)

        ["string", value]
      when Integer then ["integer", value.to_s]
      when Float
        raise SerializationError, "non-finite floats are not supported" unless value.finite?

        ["float", value]
      when Symbol then ["symbol", value.to_s]
      when ActorHandle then ["actor_handle", value.id]
      when Timer then ["timer", value.id]
      when Array
        encode_container(value, seen) do
          ["array", value.map { |item| encode(item, seen, depth + 1) }]
        end
      when Hash
        encode_container(value, seen) do
          ["hash", value.map { |key, item| [encode(key, seen, depth + 1), encode(item, seen, depth + 1)] }]
        end
      else
        raise SerializationError, "unsupported value type: #{value.class}"
      end
    end
    private_class_method :encode

    # What JSON.generate accepts: anything whose bytes are valid in its own
    # encoding, except a binary string carrying a byte above 0x7F. Checked here
    # rather than left to the gem, whose wording for the failure differs
    # between versions (json 2.7 says "partial character in source", json 2.19
    # names the encodings), which made the error the caller sees depend on
    # which json the host Ruby happens to ship.
    def encodable_string?(value)
      return false unless value.valid_encoding?

      value.encoding != Encoding::BINARY || value.ascii_only?
    end

    def encode_container(value, seen)
      raise SerializationError, "cyclic values are not supported" if seen.key?(value)

      seen[value] = true
      yield
    ensure
      seen.delete(value)
    end
    private_class_method :encode_container

    def decode(value, bindings)
      raise SerializationError, "invalid encoded value" unless value.is_a?(Array)

      type, payload = value
      case type
      when "nil" then nil
      when "boolean", "string", "float" then payload
      when "integer" then Integer(payload, 10)
      when "symbol" then payload.to_sym
      when "actor_handle" then bindings.actor_handle(payload)
      when "timer" then bindings.timer(payload)
      when "array" then payload.map { |item| decode(item, bindings) }
      when "hash"
        payload.to_h { |key, item| [decode(key, bindings), decode(item, bindings)] }
      else
        raise SerializationError, "unknown encoded type: #{type.inspect}"
      end
    rescue ArgumentError, NoMethodError, TypeError => error
      raise SerializationError, error.message
    end
    private_class_method :decode

    def read_exactly(io, size)
      buffer = +""
      while buffer.bytesize < size
        chunk = io.read(size - buffer.bytesize)
        return if chunk.nil? && buffer.empty?
        raise EOFError, "socket closed during frame" if chunk.nil?

        buffer << chunk
      end
      buffer
    end
    private_class_method :read_exactly
  end
  private_constant :Transport
end
