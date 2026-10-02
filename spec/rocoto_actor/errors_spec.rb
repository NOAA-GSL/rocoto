# frozen_string_literal: true

require 'spec_helper'
require 'rocoto_actor'

RSpec.describe RocotoActor do
  describe 'the error hierarchy' do
    # Callers rescue RocotoActor::Error to catch anything the library raises
    # without also swallowing unrelated StandardErrors, so every error class
    # has to descend from it.
    it 'roots every library error at RocotoActor::Error' do
      constants = described_class.constants.map { |name| described_class.const_get(name) }
      errors = constants.select { |value| value.is_a?(Class) && value <= StandardError }

      expect(errors).not_to be_empty
      expect(errors).to all(be <= RocotoActor::Error)
    end

    it 'descends from StandardError, so a bare rescue catches it' do
      expect(RocotoActor::Error).to be < StandardError
    end

    # A failed actor is a stopped actor as far as a caller is concerned: code
    # that rescues ActorStoppedError must not miss the failure case.
    it 'treats a failed actor as a stopped one' do
      expect(RocotoActor::ActorFailedError).to be < RocotoActor::ActorStoppedError
    end
  end

  describe RocotoActor::RemoteError do
    it 'reports the remote class, message and backtrace it was given' do
      error = described_class.new('ArgumentError', 'wrong number of arguments', ['a.rb:1'])

      expect(error.remote_class).to eq('ArgumentError')
      expect(error.remote_message).to eq('wrong number of arguments')
      expect(error.remote_backtrace).to eq(['a.rb:1'])
      expect(error.message).to eq('ArgumentError: wrong number of arguments')
    end

    # The fields arrive over a socket from another process, so they cannot be
    # trusted to be strings. Coercing them means a malformed error reply still
    # produces a RemoteError rather than raising while it is being built.
    it 'coerces whatever the wire supplied rather than raising' do
      error = described_class.new(:ArgumentError, 42, 'not-an-array')

      expect(error.remote_class).to eq('ArgumentError')
      expect(error.remote_message).to eq('42')
      expect(error.remote_backtrace).to eq(['not-an-array'])
    end

    it 'defaults to an empty backtrace' do
      expect(described_class.new('RuntimeError', 'boom').remote_backtrace).to eq([])
    end

    it 'sets its backtrace from the remote one, so a raise shows where it failed' do
      error = described_class.new('RuntimeError', 'boom', ['worker.rb:7'])

      expect(error.backtrace).to eq(['worker.rb:7'])
    end
  end
end
