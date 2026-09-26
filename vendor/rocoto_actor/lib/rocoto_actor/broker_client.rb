# frozen_string_literal: true

module RocotoActor
  # Worker-side request/response channel to the parent broker over the actor's
  # own socket. One client exists per socket so request IDs cannot collide
  # between handles, and frames read while waiting that are not broker
  # responses are kept for the actor loop.
  class BrokerClient
    attr_reader :decode_bindings, :deferred_frames

    def initialize(socket)
      @socket = socket
      @decode_bindings = DecodeBindings.new(socket: socket)
      @mutex = Mutex.new
      @next_request_id = 0
      @deferred_frames = []
    end

    # Sends one broker request and blocks until its response arrives. The broker
    # owns the deadline and always answers, so no read timeout is applied here:
    # a timed-out partial frame read would desynchronize the stream.
    def request(fields)
      @mutex.synchronize do
        @next_request_id += 1
        request_id = @next_request_id
        Transport.write(@socket, Protocol.with_request_id(fields, request_id))

        loop do
          response = Transport.read(@socket, bindings: decode_bindings)
          raise ActorStoppedError, "broker connection closed" unless response

          unless Protocol.broker_response?(response)
            @deferred_frames << response
            next
          end
          # A response for an earlier request that is no longer awaited is discarded.
          next unless Protocol.response_for?(response, request_id)

          return response[:result] if response[:ok]

          raise RemoteError.new(response[:error_class], response[:message], response[:backtrace])
        end
      end
    end
  end
  private_constant :BrokerClient
end
