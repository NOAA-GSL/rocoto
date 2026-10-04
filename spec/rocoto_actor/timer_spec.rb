# frozen_string_literal: true

require 'spec_helper'
require 'rocoto_actor'

# As with ActorHandle, only the unbound surface is covered here: a Timer built
# with socket: binds through RocotoActor.broker_client, which does not exist until
# the BrokerClient phase, so cancelling for real arrives with that phase.
RSpec.describe RocotoActor::Timer do
  # A Timer is opaque outside the actor that scheduled it, and that is enforced
  # rather than documented: without a socket there is nobody to ask, so cancel
  # refuses instead of silently doing nothing.
  it 'refuses to be cancelled by anyone but the actor that created it' do
    expect { described_class.new('timer-1').cancel }
      .to raise_error(RocotoActor::Error, /can only be cancelled by the actor that created it/)
  end

  it 'reports the id it was given' do
    expect(described_class.new('timer-1').id).to eq('timer-1')
  end

  it 'compares and hashes by id alone' do
    timer = described_class.new('timer-1')
    same = described_class.new('timer-1')
    other = described_class.new('timer-2')

    expect(timer).to eq(same)
    expect(timer).to eql(same)
    expect(timer.hash).to eq(same.hash)
    expect({ timer => :value }[same]).to eq(:value)
    expect(timer).not_to eq(other)
    expect(timer).not_to eq('timer-1')
  end
end
