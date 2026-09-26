# frozen_string_literal: true

require "minitest/autorun"
require "stringio"
require "socket"
require_relative "../lib/rocoto_actor"

class TransportTest < Minitest::Test
  TRANSPORT = RocotoActor.const_get(:Transport) # internal; exercised directly here
  DECODE_BINDINGS = RocotoActor.const_get(:DecodeBindings)

  class FakeBroker
    attr_reader :asks

    def initialize
      @asks = []
    end

    def ask(id, message)
      @asks << [id, message]
      :future
    end
  end

  def test_read_returns_nil_at_a_clean_end_of_stream_and_raises_mid_frame
    reader, writer = IO.pipe
    writer.close
    assert_nil TRANSPORT.read(reader)

    reader, writer = IO.pipe
    writer.write("#{[100].pack('N')}abc")
    writer.close
    assert_raises(EOFError) { TRANSPORT.read(reader) }
  end

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

  def test_decode_bindings_bind_handles_to_the_broker
    broker = FakeBroker.new
    bindings = DECODE_BINDINGS.new
    bindings.attach_broker(broker)
    io = StringIO.new
    TRANSPORT.write(io, [RocotoActor::ActorHandle.new("actor"), { nested: RocotoActor::ActorHandle.new("child") }])
    io.rewind

    actor, nested = TRANSPORT.read(io, bindings: bindings)

    assert_equal :future, actor.ask(:work)
    assert_equal :future, nested[:nested].ask(:child_work)
    assert_equal [["actor", :work], ["child", :child_work]], broker.asks
  end

  def test_decode_bindings_bind_worker_capabilities_to_the_socket
    reader, writer = UNIXSocket.pair
    bindings = DECODE_BINDINGS.new(socket: reader)
    io = StringIO.new
    TRANSPORT.write(io, [RocotoActor::ActorHandle.new("actor"), RocotoActor::Timer.new("timer")])
    io.rewind

    handle, timer = TRANSPORT.read(io, bindings: bindings)

    assert_same RocotoActor.broker_client(reader), handle.instance_variable_get(:@client)
    assert_same RocotoActor.broker_client(reader), timer.instance_variable_get(:@client)
  ensure
    reader&.close
    writer&.close
  end

  def test_unbound_decode_bindings_produce_unbound_capabilities
    io = StringIO.new
    TRANSPORT.write(io, [RocotoActor::ActorHandle.new("actor"), RocotoActor::Timer.new("timer")])
    io.rewind

    handle, timer = TRANSPORT.read(io)

    assert_raises(RocotoActor::Error) { handle.ask(:work) }
    assert_raises(RocotoActor::Error) { timer.cancel }
  end

  def test_decode_bindings_attach_to_only_one_broker
    bindings = DECODE_BINDINGS.new
    broker = FakeBroker.new

    bindings.attach_broker(broker)

    bindings.attach_broker(broker)
    assert_raises(RocotoActor::Error) { bindings.attach_broker(FakeBroker.new) }
    assert_raises(RocotoActor::Error) { DECODE_BINDINGS.new(socket: StringIO.new).attach_broker(broker) }
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

  # The length header is binary and a multi-byte payload is UTF-8. Concatenating
  # them raises Encoding::CompatibilityError whenever both hold a byte above
  # 0x7F, so roughly half of all payload sizes used to fail while the same
  # lengths in ASCII succeeded. The sweep has to cross the 0x80 boundaries: a
  # single short sample passes either way.
  def test_multibyte_payloads_round_trip_at_every_frame_length
    failures = (1..400).reject do |length|
      io = StringIO.new(+"".b)
      message = { op: :ask, message: "é" * length }
      TRANSPORT.write(io, message)
      io.rewind
      TRANSPORT.read(io) == message
    rescue StandardError
      false
    end

    assert_empty failures.first(10), "multi-byte payloads failed at #{failures.size} of 400 lengths"
  end

  def test_multibyte_strings_keep_their_encoding_and_bytes
    io = StringIO.new(+"".b)
    text = "café naïve — ☕ 日本語 🧪 #{'padding ünicode ' * 12}"

    TRANSPORT.write(io, { op: :ask, message: text })
    io.rewind
    decoded = TRANSPORT.read(io)[:message]

    assert_equal Encoding::UTF_8, decoded.encoding
    assert_equal text, decoded
    assert_equal text.bytes, decoded.bytes
  end

  def test_json_codec_rejects_cycles
    value = []
    value << value

    assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(StringIO.new, value)
    end
  end
end
