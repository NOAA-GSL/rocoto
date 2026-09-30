# frozen_string_literal: true

# Actors used only by the Linux fault matrix (test/validation/fault_matrix.rb).
# They deliberately do things a well-behaved actor never does.

require "rbconfig"

# Forks a child that inherits the actor socket and sleeps, then reports pids.
class SocketHolderActor
  def receive(message)
    case message
    when :fork_holder
      child = fork do
        Signal.trap("TERM", "IGNORE")
        sleep 60
      end
      { worker: Process.pid, holder: child }
    when :pid then Process.pid
    when :hang then sleep 60
    else :ok
    end
  end
end

# Writes raw bytes straight onto the actor socket instead of a proper reply.
class MalformedReplyActor
  def receive(message)
    socket = ObjectSpace.each_object(UNIXSocket).find { |io| !io.closed? }
    payload = case message.fetch(:kind)
              when :huge_header then [2**31].pack("N")
              when :bad_json then frame("{not json")
              when :unknown_tag then frame('["mystery", 1]')
              when :missing_id then frame('["hash", [[["symbol","ok"],["boolean",true]]]]')
              when :bad_backtrace
                # id 2: the boot request is id 1, so a fresh actor's first ask is 2.
                frame('["hash", [[["symbol","id"],["integer","2"]],[["symbol","ok"],["boolean",false]],' \
                      '[["symbol","error_class"],["integer","7"]],[["symbol","message"],["nil"]],' \
                      '[["symbol","backtrace"],["array",[["integer","1"]]]]]]')
              when :truncated then "#{[100].pack('N')}abc"
              end
    socket.write(payload)
    socket.flush
    sleep 60 # never send the real reply; the parent must not depend on it
  end

  def frame(json)
    [json.bytesize].pack("N") + json
  end
end

# Escapes process-group containment on purpose: a child in its own process
# group is outside the group the watchdog and the broker kill.
class EscapeActor
  def receive(_message)
    pid = Process.spawn("sleep", "60", pgroup: true)
    Process.detach(pid)
    pid
  end
end

# Blocks in receive until killed, ignoring TERM, and reports its pid.
class StubbornActor
  def receive(message)
    return Process.pid if message == :pid

    Signal.trap("TERM", "IGNORE")
    sleep 60
  end
end
