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
    # mailbox bounds what can queue behind a blocked write.
    def write_payload(io, payload)
      io.write([payload.bytesize].pack("N") << payload)
    end

    def read(io, timeout: nil)
      deadline = timeout && (monotonic_time + timeout)
      header = read_exactly(io, HEADER_SIZE, deadline: deadline)
      return if header.nil?

      size = header.unpack1("N")
      raise Error, "invalid frame size: #{size}" if size > MAX_FRAME_SIZE

      Thread.current[:rocoto_actor_transport_socket] = io
      decode(JSON.parse(read_exactly(io, size, deadline: deadline)))
    rescue JSON::JSONError => error
      raise SerializationError, error.message
    end

    def encode(value, seen = {}.compare_by_identity, depth = 0)
      raise SerializationError, "value exceeds #{MAX_NESTING} nesting levels" if depth > MAX_NESTING

      case value
      when nil then ["nil"]
      when true, false then ["boolean", value]
      when String then ["string", value]
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

    def encode_container(value, seen)
      raise SerializationError, "cyclic values are not supported" if seen.key?(value)

      seen[value] = true
      yield
    ensure
      seen.delete(value)
    end
    private_class_method :encode_container

    def decode(value)
      raise SerializationError, "invalid encoded value" unless value.is_a?(Array)

      type, payload = value
      case type
      when "nil" then nil
      when "boolean", "string", "float" then payload
      when "integer" then Integer(payload, 10)
      when "symbol" then payload.to_sym
      when "actor_handle"
        if RocotoActor.worker_process?
          ActorHandle.new(payload, socket: Thread.current[:rocoto_actor_transport_socket])
        else
          ActorHandle.new(payload, broker: Thread.current[:rocoto_actor_broker])
        end
      when "timer"
        Timer.new(payload, socket: RocotoActor.worker_process? ? Thread.current[:rocoto_actor_transport_socket] : nil)
      when "array" then payload.map { |item| decode(item) }
      when "hash"
        payload.to_h { |key, item| [decode(key), decode(item)] }
      else
        raise SerializationError, "unknown encoded type: #{type.inspect}"
      end
    rescue ArgumentError, NoMethodError, TypeError => error
      raise SerializationError, error.message
    end
    private_class_method :decode

    def read_exactly(io, size, deadline: nil)
      buffer = +""
      while buffer.bytesize < size
        chunk = if deadline
                  io.read_nonblock(size - buffer.bytesize, exception: false)
                else
                  io.read(size - buffer.bytesize)
                end
        if chunk == :wait_readable
          wait_for(io, deadline)
          next
        end
        return if chunk.nil? && buffer.empty?
        raise EOFError, "socket closed during frame" if chunk.nil?

        buffer << chunk
      end
      buffer
    end
    private_class_method :read_exactly

    def wait_for(io, deadline)
      remaining = deadline - monotonic_time
      raise TransportTimeoutError, "transport read timed out" if remaining <= 0

      raise TransportTimeoutError, "transport read timed out" unless io.wait_readable(remaining)
    end
    private_class_method :wait_for

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    private_class_method :monotonic_time
  end
  private_constant :Transport
end
