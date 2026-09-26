# frozen_string_literal: true

module RocotoActor
  class Error < StandardError; end
  class ActorStoppedError < Error; end
  class ActorFailedError < ActorStoppedError; end
  class ActorRestartingError < Error; end
  class DeadlockError < Error; end
  class AskTimeoutError < Error; end
  class BrokerBusyError < Error; end
  class MailboxFullError < Error; end
  class SerializationError < Error; end
  class TransportTimeoutError < Error; end

  class RemoteError < Error
    attr_reader :remote_class, :remote_message, :remote_backtrace

    # Fields come off the wire from an actor process, so they are coerced to
    # strings rather than trusted: a malformed error reply must still produce
    # a RemoteError instead of raising while it is being built.
    def initialize(remote_class, message, remote_backtrace = [])
      @remote_class = remote_class.to_s
      @remote_message = message.to_s
      @remote_backtrace = Array(remote_backtrace).map(&:to_s)
      super("#{@remote_class}: #{@remote_message}")
      set_backtrace(@remote_backtrace)
    end
  end
end
