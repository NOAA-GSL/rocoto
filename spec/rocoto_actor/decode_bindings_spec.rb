# frozen_string_literal: true

require 'spec_helper'
require 'stringio'
require 'rocoto_actor'

# DecodeBindings is a private constant, reached the way the library's own tests
# reach its internals. The cases where a decoded value is bound to a socket need
# RocotoActor.broker_client, which does not exist until the BrokerClient phase,
# and the cases that decode values off the wire need Transport, which arrives with
# its own phase; both are covered there rather than here.
RSpec.describe 'RocotoActor::DecodeBindings' do
  let(:bindings_class) { RocotoActor.const_get(:DecodeBindings) }
  let(:broker) do
    Class.new do
      attr_reader :asks

      def initialize
        @asks = []
      end

      def ask(id, message)
        @asks << [id, message]
        :a_future
      end
    end.new
  end

  describe '#attach_broker' do
    # A Reference registers once and then attaches its broker, so attaching the
    # same broker again is harmless; attaching a different one would silently
    # re-point every handle this Reference has already decoded.
    it 'accepts the same broker repeatedly but refuses a second one' do
      bindings = bindings_class.new

      bindings.attach_broker(broker)

      expect { bindings.attach_broker(broker) }.not_to raise_error
      expect { bindings.attach_broker(broker.class.new) }
        .to raise_error(RocotoActor::Error, /already attached to another broker/)
    end

    # Actor bindings are always socket-bound: inside an actor there is no
    # broker object to attach, only a socket to reach it over.
    it 'refuses outright when the bindings are socket-bound' do
      expect { bindings_class.new(socket: StringIO.new).attach_broker(broker) }
        .to raise_error(RocotoActor::Error, /actor decode bindings cannot attach a broker/)
    end
  end

  describe '#actor_handle' do
    it 'binds the handle to the attached broker' do
      bindings = bindings_class.new
      bindings.attach_broker(broker)

      handle = bindings.actor_handle('actor-1')

      expect(handle.ask(:work)).to eq(:a_future)
      expect(broker.asks).to eq([['actor-1', :work]])
    end

    # Decoding can happen before a broker is attached. The handle is still built,
    # carrying its id, and refuses use rather than binding to something wrong.
    it 'still builds a handle when no broker has been attached, but an unusable one' do
      handle = bindings_class.new.actor_handle('actor-1')

      expect(handle.id).to eq('actor-1')
      expect { handle.ask(:work) }.to raise_error(RocotoActor::Error)
    end
  end

  describe '#timer' do
    it 'builds an unbound timer when there is no socket to cancel over' do
      timer = bindings_class.new.timer('timer-1')

      expect(timer.id).to eq('timer-1')
      expect { timer.cancel }.to raise_error(RocotoActor::Error)
    end
  end
end
