# frozen_string_literal: true

# Tiny fixture class used only by actor_spec.rb to exercise WorkflowMgr::Actor
# without depending on any real, more complex served object.
class EchoActorTestDouble
  def initialize(name)
    @name = name
  end

  def greet(suffix = "")
    "hello #{@name}#{suffix}"
  end

  def boom
    raise ArgumentError, "kaboom"
  end

  def nap(seconds)
    sleep(seconds)
    "awake"
  end
end
