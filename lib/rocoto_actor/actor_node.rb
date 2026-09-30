# frozen_string_literal: true

module RocotoActor
  # Broker-owned record for one logical actor in the family tree. All methods
  # are called while ActorBroker's mutex is held.
  class ActorNode
    STATES = %i[starting running restarting stopping stopped failed].freeze
    TERMINAL_STATES = %i[stopped failed].freeze
    ACTIVE_STATES = %i[starting running restarting].freeze

    attr_reader :id, :name, :path, :generation, :parent_id, :children, :state, :reference,
                :booting, :spec, :policy, :restarts, :failure, :exit, :boot_exit, :started_at
    # Timers this incarnation scheduled (id => record) and the ids of actors
    # watching this node. Timers die with the incarnation; watchers persist
    # across restarts and end when the node goes terminal.
    attr_reader :timers, :watchers

    def initialize(id:, name:, path:, parent_id:, reference:, spec:, policy:)
      @id = id
      @name = name
      @path = path
      @generation = 1
      @parent_id = parent_id
      @children = []
      @state = :starting
      @reference = reference
      @booting = true
      @spec = spec
      @policy = policy
      @restarts = 0
      @failure = nil
      @exit = nil
      @boot_exit = nil
      @started_at = nil
      @timers = {}
      @watchers = {}
    end

    def terminal?
      TERMINAL_STATES.include?(@state)
    end

    def active?
      ACTIVE_STATES.include?(@state)
    end

    # Ends a boot (first or relaunch) successfully. Returns true when the node
    # moved to :running, false when a stop raced the boot and won.
    def boot_succeeded(now)
      @booting = false
      return false unless %i[starting restarting].include?(@state)

      @state = :running
      @started_at = now
      true
    end

    def first_boot?
      @generation == 1
    end

    # Ends a failed boot: the incarnation's reference is detached (its exit
    # is already accounted for by the caller) and returned to be killed.
    def boot_failed
      @booting = false
      @boot_exit = nil
      record_reference(@reference)
      reference = @reference
      @reference = nil
      reference
    end

    # A relaunch whose initialize failed reports the error as a boot reply, not
    # as an exit frame; keep it as the failure so last_failure explains it.
    def record_failure(error)
      @failure = error
    end

    def record_boot_exit(reference)
      @boot_exit = reference
    end

    def begin_stopping
      @state = :stopping
    end

    def begin_restarting
      @state = :restarting
      @timers.clear
    end

    def record_restart_attempt(now, window)
      @restarts = 0 if @started_at && now - @started_at > window
      @started_at = nil
      return false if @restarts >= @policy[:max_restarts]

      @restarts += 1
      @restarts
    end

    def install_restarted_reference(reference) # rubocop:disable Naming/PredicateMethod
      return false unless @state == :restarting

      record_reference(@reference)
      @reference = reference
      @generation += 1
      @booting = true
      @boot_exit = nil
      true
    end

    def retire(state)
      @state = state
      record_reference(@reference)
      @reference = nil
      @spec = nil
      @restarts = 0
      @timers.clear
      @watchers.clear
    end

    private

    def record_reference(reference)
      return unless reference

      @failure = reference.exit_error || @failure
      @exit = reference.exit_status || @exit
    end
  end
  private_constant :ActorNode
end
