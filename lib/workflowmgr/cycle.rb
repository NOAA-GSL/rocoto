##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class Cycle
  #
  ##########################################
  class Cycle
    include Comparable

    attr_reader :cycle, :activated, :expired, :draining, :done, :state

    ##########################################
    #
    # init
    #
    ##########################################
    def initialize(cycle,
                   params = { activated: Time.at(0), expired: Time.at(0), done: Time.at(0), draining: Time.at(0) })
      @cycle = cycle
      @activated = params[:activated] || Time.at(0)
      @expired = params[:expired] || Time.at(0)
      @draining = params[:draining] || Time.at(0)
      @done = params[:done] || Time.at(0)

      @state = if @done != Time.at(0)
                 :done
               elsif @expired != Time.at(0)
                 :expired
               elsif @draining != Time.at(0)
                 :draining
               elsif @activated != Time.at(0)
                 :active
               else
                 :inactive
               end
    end

    ##########################################
    #
    # to_wire / from_wire
    #
    # How a Cycle crosses an actor boundary. @state is left out on purpose:
    # the constructor derives it from the four times, so sending it would
    # risk carrying a state that contradicts them. These are also the only
    # four values update_cycles writes, so what crosses the wire is exactly
    # what the database keeps -- with one exception worth knowing:
    # reactivate! sets @state without touching @activated, so a cycle
    # reactivated in memory and then sent comes back in the state its times
    # describe, the same as it would after being saved and loaded again.
    #
    ##########################################
    def to_wire
      [@cycle, { activated: @activated, expired: @expired, done: @done, draining: @draining }]
    end

    def self.from_wire(fields)
      new(fields[0], fields[1])
    end

    ##########################################
    #
    # to_s
    #
    ##########################################
    def to_s
      strcyc = @cycle.strftime("%Y%m%d%H%M")
      if done?
        "#{strcyc} in state \"done\" since #{@done.strftime('%Y-%m-%d %H:%M:%S')}"
      elsif draining?
        "#{strcyc} in state \"drained\" since @#{@draining.strftime('%Y-%m-%d %H:%M:%S')}"
      elsif expired?
        "#{strcyc} in state \"expired\" since #{@expired.strftime('%Y-%m-%d %H:%M:%S')}"
      elsif active?
        "#{strcyc} in state \"activated\" since #{@activated.strftime('%Y-%m-%d %H:%M:%S')}"
      else
        "#{strcyc} in state \"inactive\""
      end
    end

    ##########################################
    #
    # hash
    #
    ##########################################
    def hash
      @cycle.hash
    end

    ##########################################
    #
    # <=>
    #
    ##########################################
    def <=>(other)
      @cycle.getgm.to_i <=> other.cycle.getgm.to_i
    end

    ##########################################
    #
    # inactive?
    #
    ##########################################
    def inactive?
      @state == :inactive
    end

    ##########################################
    #
    # active?
    #
    ##########################################
    def active?
      @state == :active
    end

    ##########################################
    #
    # expired?
    #
    ##########################################
    def expired?
      @state == :expired
    end

    ##########################################
    #
    # draining?
    #
    ##########################################
    def draining?
      @state == :draining
    end

    ##########################################
    #
    # done?
    #
    ##########################################
    def done?
      @state == :done
    end

    ##########################################
    #
    # activate!
    #
    ##########################################
    def activate!
      return if @state == :active
      raise "Expired cycle cannot be activated!" if @state == :expired
      raise "Done cycle cannot be activated!  Use reactivate!" if @state == :done
      raise "Draining cycle cannot be activated!" if @state == :draining

      @activated = Time.now.getgm
      @state = :active
    end

    ##########################################
    #
    # reactivate!
    #
    ##########################################
    def reactivate!
      return if @state == :active
      raise "Expired cycle cannot be reactivated!" if @state == :expired
      raise "Draining cycle cannot be reactivated!" if @state == :draining

      @done = Time.at(0)
      @draining = Time.at(0)
      @state = :active
    end

    ##########################################
    #
    # rewind!
    #
    ##########################################
    def rewind!
      @done = Time.at(0)
      @draining = Time.at(0)
      @expired = Time.at(0)
      @activated = Time.at(0)
      @state = :inactive
    end

    ##########################################
    #
    # drain!
    #
    ##########################################
    def drain!
      return if @state == :draining
      raise "Done cycle cannot be drained!" if @state == :done
      raise "Expired cycle cannot be drained!" if @state == :expired

      @draining = Time.now.getgm
      @state = :draining
    end

    ##########################################
    #
    # expire!
    #
    ##########################################
    def expire!
      return if @state == :expired
      raise "Done cycle cannot be expired!" if @state == :done

      @expired = Time.now.getgm
      @state = :expired
    end

    ##########################################
    #
    # done!
    #
    ##########################################
    def done!
      return if @state == :done
      raise "Expired cycle cannot be completed!" if @state == :expired

      @done = Time.now.getgm
      @state = :done
    end

    ##########################################
    #
    # activated_time_string
    #
    # sets activated time based on state  [mon dd, YYYY HH:MM:SS]
    #
    ##########################################
    def activated_time_string(fmt = "%b %d %Y %H:%M:%S")
      case @state
      when :inactive
        activated = "-"
      when :active, :done, :expired, :draining
        activated = @activated.strftime(fmt)
      end
      activated
    end

    ##########################################
    #
    # deactivated_time_string
    #
    # sets deactivated time based on state
    #
    ##########################################
    def deactivated_time_string(fmt = "%b %d %Y %H:%M:%S")
      case @state
      when :inactive, :active, :draining
        deactivated = "-"
      when :done
        deactivated = @done.strftime(fmt)
      when :expired
        deactivated = @expired.strftime(fmt)
      end
      deactivated
    end
  end
end
