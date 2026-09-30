##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class Job
  #
  ##########################################
  class Job
    attr_reader   :task, :cycle, :cores
    attr_accessor :id, :state, :native_state, :exit_status, :tries, :nunknowns, :duration

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(id, task, cycle, cores, state, native_state, exit_status, tries, nunknowns, duration)
      @id = id
      @task = task
      @cycle = cycle
      @cores = cores
      @state = state
      @native_state = native_state
      @exit_status = exit_status
      @tries = tries
      @nunknowns = nunknowns
      @duration = duration
    end

    #####################################################
    #
    # to_wire / from_wire
    #
    # How a Job crosses an actor boundary: its fields in constructor order.
    # The Time in @cycle is handled by the codec rather than here, so this
    # stays a plain list of values.
    #
    #####################################################
    def to_wire
      [@id, @task, @cycle, @cores, @state, @native_state, @exit_status, @tries, @nunknowns, @duration]
    end

    def self.from_wire(fields)
      new(*fields)
    end

    #####################################################
    #
    # pending_submit?
    #
    #####################################################
    # A job that was written down before its submission was confirmed. The
    # state says so on its own: a submission that succeeded becomes QUEUED,
    # so anything still SUBMITTING was either submitted moments ago by this
    # run, or abandoned by a run that died before it heard back.
    def pending_submit?
      @state == "SUBMITTING"
    end

    #####################################################
    #
    # done?
    #
    #####################################################
    def done?
      @state == "SUCCEEDED" || @state == "FAILED" || @state == "DEAD" || @state == "LOST"
    end

    #####################################################
    #
    # failed?
    #
    #####################################################
    def failed?
      @state == "FAILED" || @state == "DEAD" || @state == "LOST"
    end

    #####################################################
    #
    # dead?
    #
    #####################################################
    def dead?
      @state == "DEAD"
    end

    #####################################################
    #
    # expired?
    #
    #####################################################
    def expired?
      @state == "EXPIRED"
    end
  end
end
