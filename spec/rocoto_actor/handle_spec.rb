# frozen_string_literal: true

require 'spec_helper'
require 'rocoto_actor'

# Only the application side of ActorHandle is covered here. Constructing one with
# socket: binds it through RocotoActor.broker_client, which does not exist until
# the BrokerClient phase, so the actor-side behaviour of call, tell and stop
# arrives with that phase rather than this one.
RSpec.describe RocotoActor::ActorHandle do
  # ActorBroker does not exist yet, so this stands in for it and records what it
  # was asked, which is what lets delegation be checked rather than assumed.
  let(:broker) do
    Class.new do
      attr_reader :calls

      def initialize
        @calls = []
      end

      def ask(id, message)
        @calls << [:ask, id, message]
        :a_future
      end

      def tell(id, message)
        @calls << [:tell, id, message]
        nil
      end

      def stop_actor(id, timeout:, force:)
        @calls << [:stop_actor, id, timeout, force]
        true
      end

      def alive?(id)
        @calls << [:alive?, id]
        true
      end

      def state(id)
        @calls << [:state, id]
        :running
      end
    end.new
  end

  describe 'in the application process, where it delegates to its broker' do
    subject(:handle) { described_class.new('actor-1', broker: broker) }

    it 'forwards each call to the broker, naming the actor it stands for' do
      expect(handle.ask(:work)).to eq(:a_future)
      expect(handle.tell(:notice)).to be_nil
      expect(handle.alive?).to be(true)
      expect(handle.state).to eq(:running)

      expect(broker.calls).to eq([[:ask, 'actor-1', :work], [:tell, 'actor-1', :notice],
                                  [:alive?, 'actor-1'], [:state, 'actor-1']])
    end

    # The timeout is always passed explicitly: stop defaults it to
    # Reference::DEFAULT_STOP_TIMEOUT, and Reference does not arrive until a later
    # phase, so stop with no argument raises NameError until then.
    it 'forwards stop with its timeout and force flag' do
      expect(handle.stop(timeout: 2, force: true)).to be(true)

      expect(broker.calls).to eq([[:stop_actor, 'actor-1', 2, true]])
    end
  end

  describe 'unbound, with neither a broker nor a socket' do
    subject(:handle) { described_class.new('actor-1') }

    # Two separate guards, and the distinction is the point: local! marks the
    # methods that only mean anything in the application process, and names the
    # method so the message says which one was misused. remote! marks the ones an
    # actor reaches over its socket.
    it 'refuses the application-only methods, naming the one that was called' do
      expect { handle.ask(:work) }.to raise_error(RocotoActor::Error, /ActorHandle#ask is only available/)
      expect { handle.state }.to raise_error(RocotoActor::Error, /ActorHandle#state is only available/)
      expect { handle.children }.to raise_error(RocotoActor::Error, /ActorHandle#children is only available/)
    end

    it 'refuses the methods that need a socket to the broker' do
      expect { handle.call(:work) }.to raise_error(RocotoActor::Error, /not bound to a broker/)
      expect { handle.tell(:work) }.to raise_error(RocotoActor::Error, /not bound to a broker/)
      expect { handle.stop(timeout: 1) }.to raise_error(RocotoActor::Error, /not bound to a broker/)
    end
  end

  describe 'identity' do
    # Only the id crosses a process boundary, so two handles naming the same actor
    # have to be interchangeable, including as a Hash key.
    it 'compares and hashes by id alone' do
      handle = described_class.new('actor-1')
      same = described_class.new('actor-1')
      other = described_class.new('actor-2')

      expect(handle).to eq(same)
      expect(handle).to eql(same)
      expect(handle.hash).to eq(same.hash)
      expect({ handle => :value }[same]).to eq(:value)
      expect(handle).not_to eq(other)
      expect(handle).not_to eq('actor-1')
    end
  end
end
