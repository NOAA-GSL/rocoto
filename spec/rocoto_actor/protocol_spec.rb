# frozen_string_literal: true

require 'spec_helper'
require 'rocoto_actor'

# Protocol is a private constant, reached the way the library's own tests reach
# its internals. Describing RocotoActor::Protocol directly would raise NameError.
RSpec.describe 'RocotoActor::Protocol' do
  let(:protocol) { RocotoActor.const_get(:Protocol) }

  it 'builds requests and correlates them' do
    request = protocol.request(:broker_tell, handle_id: 'target', message: :work)

    expect(request).to eq({ op: :broker_tell, handle_id: 'target', message: :work })
    expect(protocol.with_request_id(request, 7)).to eq(request.merge(request_id: 7))
    expect(protocol).to be_broker_request(request)
  end

  # The broker's dispatch has one arm per op in this set, and an actor emits
  # these ops from the other side of a socket. An op renamed on one side only
  # would still be accepted here and then fall through there, so the names the
  # wire actually uses are pinned rather than left implicit.
  it 'classifies every broker op, and refuses an application-to-actor one' do
    ops = protocol.const_get(:BROKER_REQUEST_OPS)

    expect(ops).to contain_exactly(:broker_ask, :broker_tell, :broker_spawn, :broker_stop,
                                   :broker_schedule, :broker_cancel, :broker_watch, :broker_unwatch)
    ops.each { |op| expect(protocol).to be_broker_request({ op: op }) }
    # :ask, :tell and :stop travel the same socket in the opposite direction,
    # built by Reference for the application. They are not broker requests.
    expect(protocol).not_to be_broker_request({ op: :ask })
    expect(protocol).not_to be_broker_request({ op: :tell })
    expect(protocol).not_to be_broker_request({ op: :stop })
  end

  it 'builds successful responses' do
    expect(protocol.success(3, :done)).to eq({ id: 3, ok: true, result: :done })
    expect(protocol.broker_response(9, result: :done))
      .to eq({ op: :broker_response, request_id: 9, ok: true, result: :done })
  end

  it 'flattens remote errors' do
    original = RocotoActor::RemoteError.new('ArgumentError', 'bad input', ['actor.rb:1'])

    expect(protocol.failure(4, original)).to eq(
      { id: 4, ok: false, error_class: 'ArgumentError', message: 'bad input', backtrace: ['actor.rb:1'] }
    )
  end

  it 'identifies only the matching broker response' do
    response = protocol.broker_response(2, result: nil)

    expect(protocol).to be_broker_response(response)
    expect(protocol).to be_response_for(response, 2)
    # not_to, rather than asserting a strict false: response_for? returns the
    # value of an &&, so a non-matching id yields nil rather than false. The
    # original asserted only that it was not truthy, and not_to be_* keeps
    # exactly that rather than claiming more than the library promises.
    expect(protocol).not_to be_response_for(response, 1)
    expect(protocol).not_to be_broker_response(protocol.request(:ask, id: 2))
  end
end
