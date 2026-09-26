# frozen_string_literal: true

module RocotoActor
  # Internal wire-envelope vocabulary shared by the application and worker.
  # These helpers build plain transport values and own no state or I/O.
  module Protocol
    BROKER_REQUEST_OPS = %i[broker_request broker_tell broker_spawn broker_stop broker_schedule broker_cancel
                            broker_watch broker_unwatch].freeze

    module_function

    def request(operation, **fields)
      fields.merge(op: operation)
    end

    def with_request_id(fields, request_id)
      fields.merge(request_id: request_id)
    end

    def success(id, result = nil)
      { id: id, ok: true, result: result }
    end

    def failure(id, error)
      { id: id, ok: false, **error_fields(error) }
    end

    # An actor reporting the exception that is ending it, with no request to answer.
    def actor_error(error)
      request(:actor_error, **error_fields(error))
    end

    def broker_response(request_id, result: nil, error: nil)
      fields = error ? { ok: false, **error_fields(error) } : { ok: true, result: result }
      request(:broker_response, request_id: request_id, **fields)
    end

    def broker_request?(message)
      BROKER_REQUEST_OPS.include?(message[:op])
    end

    def broker_response?(message)
      message[:op] == :broker_response
    end

    def response_for?(message, request_id)
      broker_response?(message) && message[:request_id] == request_id
    end

    def error_fields(error)
      if error.is_a?(RemoteError)
        { error_class: error.remote_class, message: error.remote_message, backtrace: error.remote_backtrace }
      else
        { error_class: error.class.name, message: error.message, backtrace: error.backtrace || [] }
      end
    end
    private_class_method :error_fields
  end
  private_constant :Protocol
end
