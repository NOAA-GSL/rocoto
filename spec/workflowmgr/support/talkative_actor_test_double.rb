# frozen_string_literal: true

require 'workflowmgr/utilities'

# Fixture that talks while it works, the way the real database does when it
# steals a stale lock, or tells the user the workflow is locked by someone
# else. Those messages are written from inside the actor, whose stderr goes
# to /dev/null, so they only reach the user if they ride back with the reply.
class TalkativeActorTestDouble
  def announce(text, level)
    WorkflowMgr.stderr(text, level)
    WorkflowMgr.log("logged: #{text}")
    "done"
  end

  # Messages emitted before something goes wrong still have to arrive: they
  # are usually the explanation for what went wrong.
  def complain_then_fail(text)
    WorkflowMgr.stderr(text, 1)
    raise ArgumentError, "it went wrong"
  end

  def say_nothing
    "quiet"
  end

  # Scheduler output is not always valid UTF-8, and rocoto logs it verbatim
  # in nine places. One such line must not cost the caller its result.
  def log_raw_bytes(result)
    WorkflowMgr.stderr("qstat said: \xC3\x28 and then stopped".dup.force_encoding('UTF-8'), 1)
    result
  end

  # Text that is already perfectly good UTF-8, which has no business being
  # altered on the way back.
  def log_accented_text(result)
    WorkflowMgr.stderr("submitted job for task café_naïve", 1)
    result
  end

  def flood(count)
    count.times { |i| WorkflowMgr.stderr("message #{i}", 0) }
    "flooded"
  end
end
