# frozen_string_literal: true

require 'spec_helper'
require 'rocoto_actor'

# ErrorReporting is a private constant, reached the way the library's own tests
# reach its internals. NotImplementedError stands in for an exception outside
# StandardError: it descends from ScriptError, so a plain `rescue` would miss it
# and these examples would fail if the rescues were narrowed.
RSpec.describe 'RocotoActor::ErrorReporting' do
  let(:reporting) { RocotoActor.const_get(:ErrorReporting) }
  let(:reported) { [] }
  let(:handler) { ->(error, context) { reported << [error, context] } }

  describe '.report' do
    it 'hands the error and its context to the handler' do
      error = RuntimeError.new('boom')
      reporting.report(handler, error, 'on_event')

      expect(reported).to eq([[error, 'on_event']])
    end

    # These run on the broker's own threads, where nothing may escape: a thread
    # that dies with work queued behind it leaves an actor unreachable while the
    # broker still reports it healthy, which is the failure invariant 10 forbids.
    it 'swallows anything the handler itself raises' do
      hostile = ->(_error, _context) { raise NotImplementedError, 'handler is broken' }

      expect { reporting.report(hostile, RuntimeError.new('boom'), 'ctx') }.not_to raise_error
    end
  end

  describe '.guard' do
    it 'returns the block value when nothing raises' do
      result = reporting.guard(handler, 'ctx') { :result }

      expect(result).to eq(:result)
      expect(reported).to be_empty
    end

    # Nothing is asserted about what guard returns on the error path. It happens
    # to return the handler's own return value, but all eight call sites discard
    # it, so pinning it here would only make a later refactor fail for no reason.
    it 'reports what the block raises, with its context, instead of propagating' do
      error = RuntimeError.new('boom')

      expect { reporting.guard(handler, 'event delivery') { raise error } }.not_to raise_error
      expect(reported).to eq([[error, 'event delivery']])
    end

    # Not only StandardError: an exit or interrupt raised inside a callback on a
    # non-main thread would end just that thread, silently, so it has to be
    # reported like any other failure.
    it 'catches exceptions outside StandardError' do
      expect { reporting.guard(handler, 'ctx') { raise NotImplementedError, 'fatal' } }.not_to raise_error

      expect(reported.first.first).to be_a(NotImplementedError)
      expect(reported.first.last).to eq('ctx')
    end
  end
end
