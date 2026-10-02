# frozen_string_literal: true

module RocotoActor
  # Message vocabulary for communication between the application, its actors, and the broker.
  #
  # NOTE: These helpers own no state or I/O and are safe to call from any process or thread.
  #
  # Two independent counters share each socket: :id for work the application
  # sent an actor, :request_id for work an actor asked of the broker. Both start
  # at 1 in different processes, so they overlap and only the field name tells
  # them apart -- hence read_replies classifies by op first, and uses fetch(:id)
  # so a misclassified frame raises instead of matching the wrong request.

  module Protocol
    # What an actor may request of the broker.
    #
    #   :broker_ask       ask another actor and wait for its result (ActorHandle#call)
    #   :broker_tell      send another actor a message, expecting no reply (ActorHandle#tell)
    #   :broker_spawn     spawn a logical child of this actor (ActorContext#spawn)
    #   :broker_stop      stop another actor (ActorHandle#stop)
    #   :broker_schedule  schedule a tell from this actor to itself (ActorContext#schedule)
    #   :broker_cancel    cancel a tell timer this actor scheduled (Timer#cancel)
    #   :broker_watch     be told when another actor fails, restarts or stops (ActorContext#watch)
    #   :broker_unwatch   stop watching another actor (ActorContext#unwatch)
    BROKER_REQUEST_OPS = %i[broker_ask broker_tell broker_spawn broker_stop broker_schedule broker_cancel
                            broker_watch broker_unwatch].freeze

    # Everything else, grouped by the direction it travels.
    #
    # Application to actor, each carrying an :id the actor quotes back:
    #
    #   :boot             construct the actor in its new process (Reference#boot)
    #   :ask              deliver a message and return its result (Reference#ask)
    #   :tell             deliver a message, expecting no reply (Reference#tell)
    #   :stop             shut the actor down (Reference#stop)
    #
    # Actor to application:
    #
    #   (reply)           success/failure below, keyed by :id and carrying no
    #                     :op; answers an :ask, :boot or :stop, never a :tell
    #   :boot_error       a failure before the boot id was known (Runner.report_boot_error)
    #   :actor_error      an actor is dying with an exception (Runner.report_failure)
    #   :actor_exit       how the worker process ended, sent by the watchdog on
    #                     every exit, for diagnostics only (Runner.report_exit)
    #
    # Broker to actor:
    #
    #   :broker_response  the broker's answer to a broker request (Reference#send_broker_response)
    #
    # :actor_event is built here too, but travels as the message inside a :tell
    # to a watching actor rather than as an envelope op of its own.

    module_function

    # Creates a message envelope for a given operation
    def request(operation, **fields)
      fields.merge(op: operation)
    end

    # Adds a request_id to a message envelope
    def with_request_id(fields, request_id)
      fields.merge(request_id: request_id)
    end

    # An actor's answer to work sent to it
    def success(id, result = nil)
      { id: id, ok: true, result: result }
    end

    # An actor's answer to work that raised
    def failure(id, error)
      { id: id, ok: false, **error_fields(error) }
    end

    # An actor reporting the exception that is ending it
    def actor_error(error)
      request(:actor_error, **error_fields(error))
    end

    # The broker's answer to one of the BROKER_REQUEST_OPS.
    # The ok flag is always set, so a successful nil result is never confused
    # with a failure: BrokerClient returns the result when ok is truthy and
    # raises RemoteError otherwise.
    def broker_response(request_id, result: nil, error: nil)
      fields = error ? { ok: false, **error_fields(error) } : { ok: true, result: result }
      request(:broker_response, request_id: request_id, **fields)
    end

    # Is this a request to the broker?
    def broker_request?(message)
      BROKER_REQUEST_OPS.include?(message[:op])
    end

    # Is this a response from the broker?
    def broker_response?(message)
      message[:op] == :broker_response
    end

    # Whether this is the broker response a particular request is waiting for.
    def response_for?(message, request_id)
      broker_response?(message) && message[:request_id] == request_id
    end

    # Flattens an exception into the three plain fields an error message envelope
    # carries. A RemoteError is special-cased because it already holds flattened
    # fields from a previous hop: using its own class and message would report
    # "RocotoActor::RemoteError" and a doubly-prefixed string instead of the
    # original failure, losing the provenance as it is relayed on.
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
