# frozen_string_literal: true

module RocotoActor
  # Error handling on the broker's own threads. Everything a callback raises
  # is reported to the application's error_handler as (error, context), and
  # nothing ends the thread: not the callback's exception, whatever its class
  # (an exit or interrupt on a non-main thread would only end that thread),
  # and not one the handler itself raises.
  module ErrorReporting
    module_function

    def report(handler, error, context)
      handler.call(error, context)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
    end

    def guard(handler, context)
      yield
    rescue Exception => error # rubocop:disable Lint/RescueException
      report(handler, error, context)
    end
  end
  private_constant :ErrorReporting
end
