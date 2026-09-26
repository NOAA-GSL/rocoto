# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/rocoto_actor"

class ProtocolTest < Minitest::Test
  PROTOCOL = RocotoActor.const_get(:Protocol)

  def test_builds_requests_and_correlates_them
    request = PROTOCOL.request(:broker_tell, handle_id: "target", message: :work)

    assert_equal({ op: :broker_tell, handle_id: "target", message: :work }, request)
    assert_equal request.merge(request_id: 7), PROTOCOL.with_request_id(request, 7)
    assert PROTOCOL.broker_request?(request)
  end

  def test_builds_successful_responses
    assert_equal({ id: 3, ok: true, result: :done }, PROTOCOL.success(3, :done))
    assert_equal(
      { op: :broker_response, request_id: 9, ok: true, result: :done },
      PROTOCOL.broker_response(9, result: :done)
    )
  end

  def test_flattens_remote_errors
    original = RocotoActor::RemoteError.new("ArgumentError", "bad input", ["actor.rb:1"])

    assert_equal(
      { id: 4, ok: false, error_class: "ArgumentError", message: "bad input", backtrace: ["actor.rb:1"] },
      PROTOCOL.failure(4, original)
    )
  end

  def test_identifies_only_the_matching_broker_response
    response = PROTOCOL.broker_response(2, result: nil)

    assert PROTOCOL.broker_response?(response)
    assert PROTOCOL.response_for?(response, 2)
    refute PROTOCOL.response_for?(response, 1)
    refute PROTOCOL.broker_response?(PROTOCOL.request(:ask, id: 2))
  end
end
