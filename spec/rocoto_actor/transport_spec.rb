# frozen_string_literal: true

require 'spec_helper'
require 'stringio'
require 'rocoto_actor'

# Transport and DecodeBindings are private constants, reached the way the
# library's own tests reach its internals.
#
# One case from the original suite is deliberately not converted here:
# test_decode_bindings_bind_worker_capabilities_to_the_socket asserts on
# RocotoActor.broker_client, which does not exist yet, so socket-bound decoding
# is covered when broker_client.rb lands.
RSpec.describe 'RocotoActor::Transport' do
  let(:transport) { RocotoActor.const_get(:Transport) }
  let(:bindings_class) { RocotoActor.const_get(:DecodeBindings) }
  let(:broker) do
    Class.new do
      attr_reader :asks

      def initialize
        @asks = []
      end

      def ask(id, message)
        @asks << [id, message]
        :future
      end
    end.new
  end

  describe 'framing' do
    it 'returns nil at a clean end of stream and raises mid-frame' do
      reader, writer = IO.pipe
      writer.close

      expect(transport.read(reader)).to be_nil

      reader, writer = IO.pipe
      writer.write("#{[100].pack('N')}abc")
      writer.close

      expect { transport.read(reader) }.to raise_error(EOFError)
    end

    # The length header is binary and a multi-byte payload is UTF-8. Concatenating
    # them raises Encoding::CompatibilityError whenever both hold a byte above
    # 0x7F, so roughly half of all payload sizes used to fail while the same
    # lengths in ASCII succeeded. The sweep has to cross the 0x80 boundaries: a
    # single short sample passes either way.
    it 'round trips multibyte payloads at every frame length' do
      failures = (1..400).reject do |length|
        io = StringIO.new(+''.b)
        message = { op: :ask, message: 'é' * length }
        transport.write(io, message)
        io.rewind
        transport.read(io) == message
      rescue StandardError
        false
      end

      expect(failures.first(10)).to be_empty, "multi-byte payloads failed at #{failures.size} of 400 lengths"
    end

    # Both halves of the frame-size limit. The write side refuses to build an
    # oversized frame; the read side refuses a header that merely claims one, so
    # a peer cannot make us allocate 16 MB on its word alone. Note the read side
    # raises Error rather than SerializationError: nothing was decoded.
    it 'refuses to write a frame larger than the limit' do
      max = transport.const_get(:MAX_FRAME_SIZE)

      expect { transport.write(StringIO.new, 'x' * (max + 1)) }
        .to raise_error(RocotoActor::SerializationError, /message exceeds #{max} bytes/)
    end

    it 'refuses to read a frame whose header claims more than the limit' do
      max = transport.const_get(:MAX_FRAME_SIZE)
      io = StringIO.new([max + 1].pack('N'))

      expect { transport.read(io) }.to raise_error(RocotoActor::Error, /invalid frame size: #{max + 1}/)
    end
  end

  describe 'the JSON codec' do
    it 'round trips supported values' do
      message = {
        operation: :ask,
        'payload' => [nil, true, false, 'text', 42, 1.5, { nested: :value }]
      }
      io = StringIO.new

      transport.write(io, message)
      io.rewind

      expect(transport.read(io)).to eq(message)
    end

    it 'rejects arbitrary objects without writing' do
      io = StringIO.new

      expect { transport.write(io, Object.new) }
        .to raise_error(RocotoActor::SerializationError, /unsupported value type: Object/)
      expect(io.string).to be_empty
    end

    it 'rejects non-finite numbers' do
      expect { transport.write(StringIO.new, Float::INFINITY) }
        .to raise_error(RocotoActor::SerializationError, /non-finite/)
    end

    # The message is asserted, not just the error class. Without it this example
    # passes even when the cycle guard is deleted, because the runaway recursion
    # then trips the nesting guard instead and raises the same class -- the two
    # guards mask each other.
    it 'rejects cycles' do
      value = []
      value << value

      expect { transport.write(StringIO.new, value) }
        .to raise_error(RocotoActor::SerializationError, /cyclic values are not supported/)
    end

    # The other half of that pair: a value nested past the limit without any
    # cycle, so this fails if the nesting guard goes and the cycle guard cannot
    # stand in for it. Depth is derived from the constant so the example cannot
    # drift out of step with it.
    it 'rejects values nested beyond the limit' do
      max = transport.const_get(:MAX_NESTING)
      deep = []
      (max + 2).times { deep = [deep] }

      expect { transport.write(StringIO.new, deep) }
        .to raise_error(RocotoActor::SerializationError, /value exceeds #{max} nesting levels/)
    end

    # The json gem's wording for an unencodable string differs between versions
    # (json 2.7 says "partial character in source, but hit end"; later versions
    # name the encodings), and the gem is whichever the host Ruby ships. The
    # message a caller sees must therefore come from here, not from the gem.
    it 'rejects invalid UTF-8 strings with a gem-independent message' do
      expect { transport.write(StringIO.new, "\xFF".b) }
        .to raise_error(RocotoActor::SerializationError, 'string is not valid UTF-8 (ASCII-8BIT)')

      expect { transport.write(StringIO.new, "\xFF".dup.force_encoding(Encoding::UTF_8)) }
        .to raise_error(RocotoActor::SerializationError, 'string is not valid UTF-8 (UTF-8)')
    end

    # The guard above must not be stricter than the codec it stands in front of:
    # JSON.generate transcodes these, so they have to keep working.
    it 'does not reject strings the codec accepts' do
      { 'utf-8 multibyte' => 'café', 'binary ascii-only' => 'abc'.b,
        'latin-1 high bytes' => 'café'.encode('ISO-8859-1'),
        'us-ascii' => 'abc'.encode('US-ASCII') }.each do |label, value|
        io = StringIO.new(+''.b)

        transport.write(io, value)

        expect(io.string).not_to be_empty, label
      end
    end

    it 'keeps multibyte strings\' encoding and bytes' do
      io = StringIO.new(+''.b)
      text = "café naïve — ☕ 日本語 🧪 #{'padding ünicode ' * 12}"

      transport.write(io, { op: :ask, message: text })
      io.rewind
      decoded = transport.read(io)[:message]

      expect(decoded.encoding).to eq(Encoding::UTF_8)
      expect(decoded).to eq(text)
      expect(decoded.bytes).to eq(text.bytes)
    end
  end

  describe 'decode bindings' do
    it 'binds decoded handles to the attached broker' do
      bindings = bindings_class.new
      bindings.attach_broker(broker)
      io = StringIO.new
      transport.write(io, [RocotoActor::ActorHandle.new('actor'), { nested: RocotoActor::ActorHandle.new('child') }])
      io.rewind

      actor, nested = transport.read(io, bindings: bindings)

      expect(actor.ask(:work)).to eq(:future)
      expect(nested[:nested].ask(:child_work)).to eq(:future)
      expect(broker.asks).to eq([['actor', :work], ['child', :child_work]])
    end

    it 'produces unbound capabilities when no bindings are given' do
      io = StringIO.new
      transport.write(io, [RocotoActor::ActorHandle.new('actor'), RocotoActor::Timer.new('timer')])
      io.rewind

      handle, timer = transport.read(io)

      expect { handle.ask(:work) }.to raise_error(RocotoActor::Error)
      expect { timer.cancel }.to raise_error(RocotoActor::Error)
    end
  end
end
