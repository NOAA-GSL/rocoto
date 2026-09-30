# frozen_string_literal: true

module RocotoActor
  # The options accepted when spawning an actor, parsed and validated in one
  # place for ActorBroker#spawn, ActorContext#spawn, and the broker_spawn
  # request. Raises ArgumentError with the same messages everywhere.
  class SpawnOptions
    KEYS = %i[start_timeout mailbox_size mailbox_bytes restart max_restarts restart_window restart_backoff
              source].freeze
    RESTART_POLICIES = %i[never on_failure].freeze
    DEFAULT_MAX_RESTARTS = 3
    DEFAULT_RESTART_WINDOW = 60
    DEFAULT_RESTART_BACKOFF = 0.1

    # Seconds allowed for the actor's initialize to return.
    attr_reader :start_timeout
    # Options for Launcher.launch: source, mailbox_size, mailbox_bytes.
    attr_reader :launch
    # Restart policy: restart, max_restarts, restart_window, restart_backoff.
    attr_reader :policy

    def self.parse(options)
      raise ArgumentError, "spawn options must be a hash" unless options.is_a?(Hash)

      unknown = options.keys - KEYS
      raise ArgumentError, "unsupported spawn options: #{unknown.join(', ')}" unless unknown.empty?

      new(options)
    end

    def initialize(options)
      @start_timeout = options.fetch(:start_timeout, START_TIMEOUT)
      raise ArgumentError, "start_timeout must be positive" unless positive_number?(@start_timeout)

      @launch = parse_launch(options)
      @policy = parse_policy(options)
    end

    private

    def parse_launch(options)
      launch = options.slice(:source, :mailbox_size, :mailbox_bytes)
      raise ArgumentError, "source must be a path" if launch.key?(:source) && !launch[:source].is_a?(String)

      %i[mailbox_size mailbox_bytes].each do |key|
        next unless launch.key?(key)
        unless launch[key].is_a?(Integer) && launch[key].positive?
          raise ArgumentError,
                "#{key} must be a positive integer"
        end
      end
      launch
    end

    def parse_policy(options)
      restart = options.fetch(:restart, :never)
      max_restarts = options.fetch(:max_restarts, DEFAULT_MAX_RESTARTS)
      window = options.fetch(:restart_window, DEFAULT_RESTART_WINDOW)
      backoff = options.fetch(:restart_backoff, DEFAULT_RESTART_BACKOFF)
      unless RESTART_POLICIES.include?(restart)
        raise ArgumentError, "restart must be one of #{RESTART_POLICIES.join(', ')}"
      end
      unless max_restarts.is_a?(Integer) && max_restarts.positive?
        raise ArgumentError, "max_restarts must be a positive integer"
      end
      raise ArgumentError, "restart_window must be positive" unless positive_number?(window)
      unless backoff.is_a?(Numeric) && backoff >= 0 && backoff.to_f.finite?
        raise ArgumentError, "restart_backoff must be a non-negative number"
      end

      { restart: restart, max_restarts: max_restarts, restart_window: window, restart_backoff: backoff }
    end

    def positive_number?(value)
      value.is_a?(Numeric) && value.positive? && value.to_f.finite?
    end
  end
  private_constant :SpawnOptions
end
