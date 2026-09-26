# frozen_string_literal: true

require "minitest/autorun"
require "stringio"
require "socket"
require_relative "../lib/rocoto_actor"

class TransportTest < Minitest::Test
  TRANSPORT = RocotoActor.const_get(:Transport) # internal; exercised directly here

  def test_json_codec_round_trips_supported_values
    message = {
      operation: :ask,
      "payload" => [nil, true, false, "text", 42, 1.5, { nested: :value }]
    }
    io = StringIO.new

    TRANSPORT.write(io, message)
    io.rewind

    assert_equal message, TRANSPORT.read(io)
  end

  def test_json_codec_rejects_arbitrary_objects_without_writing
    io = StringIO.new

    error = assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(io, Object.new)
    end

    assert_match(/unsupported value type: Object/, error.message)
    assert_empty io.string
  end

  def test_json_codec_rejects_non_finite_numbers
    error = assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(StringIO.new, Float::INFINITY)
    end

    assert_match(/non-finite/, error.message)
  end

  def test_json_codec_normalizes_invalid_utf8_errors
    error = assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(StringIO.new, "\xFF".b)
    end

    assert_match(/UTF-8/, error.message)
  end

  def test_json_codec_rejects_cycles
    value = []
    value << value

    assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(StringIO.new, value)
    end
  end

  def test_read_timeout_covers_partial_frames
    reader, writer = UNIXSocket.pair
    writer.write("\x00\x00")

    assert_raises(RocotoActor::TransportTimeoutError) do
      TRANSPORT.read(reader, timeout: 0.05)
    end
  ensure
    reader&.close
    writer&.close
  end

  def test_read_with_timeout_returns_nil_at_frame_boundary_eof
    reader, writer = UNIXSocket.pair
    writer.close

    assert_nil TRANSPORT.read(reader, timeout: 0.05)
  ensure
    reader&.close
    writer&.close
  end

  def test_read_with_timeout_rejects_partial_frame_at_eof
    reader, writer = UNIXSocket.pair
    writer.write([4].pack("N") << "x")
    writer.close

    assert_raises(EOFError) do
      TRANSPORT.read(reader, timeout: 0.05)
    end
  ensure
    reader&.close
    writer&.close
  end
end
