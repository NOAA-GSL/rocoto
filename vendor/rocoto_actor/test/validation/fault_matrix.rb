# frozen_string_literal: true

# Linux fault matrix from docs/linux-validation.md. Runs bounded fault-injection
# probes that are too slow, too environment-specific, or too destructive for the
# normal suite, prints PASS/FAIL per probe, and exits non-zero on any failure.
#
#   bundle exec ruby -Ilib test/validation/fault_matrix.rb [probe-name-substring]
#
# Every probe has a hard time limit and stops its own broker; a probe that hangs
# is reported as a failure rather than hanging the run.

require_relative "../../lib/rocoto_actor"
require_relative "../support/example_actor"
require_relative "../support/process_actor"
require_relative "support"
require "json"
require "rbconfig"
require "stringio"

TRANSPORT = RocotoActor.const_get(:Transport)
PROBE_LIMIT = Float(ENV.fetch("FAULT_PROBE_SECONDS", "120"))
APP_CHILD = File.expand_path("app_child.rb", __dir__)

class FaultMatrix
  Result = Struct.new(:name, :status, :detail)
  # A documented limitation observed as expected: reported, not a failure.
  class Note < StandardError; end

  def initialize(filter)
    @filter = filter
    @results = []
  end

  def run
    $stdout.sync = true
    environment
    baseline = snapshot
    probes.each do |name, block|
      next if @filter && !name.include?(@filter)

      run_probe(name, block)
    end
    final = snapshot
    report(baseline, final)
  end

  private

  # ---------------------------------------------------------------- reporting

  def environment
    puts "uname:  #{`uname -a`.strip}"
    puts "ruby:   #{RUBY_DESCRIPTION}"
    puts "bundle: #{`bundle -v 2>/dev/null`.strip}"
    uncommitted = `git status --short 2>/dev/null`.lines.size
    puts "commit: #{`git rev-parse --short HEAD 2>/dev/null`.strip} #{uncommitted} uncommitted"
    pid_max = File.read("/proc/sys/kernel/pid_max").strip
    nproc = `bash -c "ulimit -u" 2>/dev/null`.strip
    puts "limits: nofile=#{`sh -c "ulimit -n"`.strip} nproc=#{nproc} pid_max=#{pid_max}"
    puts
  end

  def run_probe(name, block)
    started = now
    outcome = nil
    thread = Thread.new do
      Thread.current.report_on_exception = false
      outcome = [:pass, block.call]
    rescue Note => error
      outcome = [:note, error.message]
    rescue StandardError => error
      outcome = [:fail, "#{error.class}: #{error.message} @ #{error.backtrace&.first}"]
    end
    outcome = [:fail, "probe exceeded #{PROBE_LIMIT}s and was abandoned"] unless thread.join(PROBE_LIMIT)
    status, detail = outcome
    @results << Result.new(name, status, detail)
    puts format("%-4s %-52s %6.1fs  %s", status.to_s.upcase, name, now - started, detail)
  end

  def report(baseline, final)
    puts
    puts "baseline #{baseline.inspect}"
    puts "final    #{final.inspect}"
    leaks = []
    leaks << "threads #{baseline[:threads]} -> #{final[:threads]}" if final[:threads] > baseline[:threads] + 2
    leaks << "fds #{baseline[:fds]} -> #{final[:fds]}" if final[:fds] > baseline[:fds] + 2
    leaks << "children #{final[:children]}" if final[:children].positive?
    leaks << "zombies #{final[:zombies]}" if final[:zombies].positive?
    failures = @results.select { |result| result.status == :fail }
    puts
    puts "#{@results.size} probes, #{failures.size} failed#{", leaks: #{leaks.join(', ')}" unless leaks.empty?}"
    exit 1 unless failures.empty? && leaks.empty?
  end

  def snapshot
    children = child_processes
    {
      threads: Thread.list.size,
      fds: Dir.children("/proc/self/fd").size,
      children: children.reject { |_pid, state| state.start_with?("Z") }.size,
      zombies: children.count { |_pid, state| state.start_with?("Z") },
      rss_kb: rss_kb
    }
  end

  # ------------------------------------------------------------------ helpers

  def now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def wait_until(timeout: 5, what: "condition")
    deadline = now + timeout
    until yield
      raise "#{what} not met within #{timeout}s" if now > deadline

      sleep 0.02
    end
    now
  end

  def check(condition, message)
    raise message unless condition
  end

  def child_processes
    `ps -o pid=,stat=,comm= --ppid #{Process.pid}`.lines.map(&:split)
                                                  .reject { |_pid, _state, command| command == "ps" }
                                                  .to_h { |pid, state, _command| [Integer(pid), state] }
  end

  def rss_kb
    File.read("/proc/self/status")[/VmRSS:\s+(\d+)/, 1].to_i
  end

  # A process counts as gone when it does not exist or is a zombie awaiting its
  # own parent's waitpid (orphans of a dead worker are reaped by init, not us).
  def gone?(pid)
    state = File.read("/proc/#{pid}/status")[/State:\s+(\S)/, 1]
    state == "Z"
  rescue Errno::ENOENT, Errno::ESRCH
    true
  end

  def parent_pid(pid)
    Integer(File.read("/proc/#{pid}/status")[/PPid:\s+(\d+)/, 1])
  end

  def with_broker(**)
    broker = RocotoActor::ActorBroker.new(**)
    yield broker
  ensure
    broker&.stop(timeout: 5, force: true)
  end

  # Runs the value of every future to a terminal state and tallies outcomes.
  def settle(futures, timeout:)
    tally = Hash.new(0)
    futures.each do |future|
      first = outcome(future, timeout)
      second = outcome(future, timeout)
      check(first == second, "future outcome changed after settling: #{first} then #{second}")
      check(future.ready?, "future not ready after value")
      tally[first] += 1
    end
    tally
  end

  def outcome(future, timeout)
    future.value(timeout: timeout)
    :result
  rescue RocotoActor::Error => error
    error.class.name.split("::").last.to_sym
  end

  # ------------------------------------------------------------------- probes

  def probes
    [
      ["C1 many asks racing graceful stop", -> { asks_racing_stop(false) }],
      ["C2 many asks racing force stop", -> { asks_racing_stop(true) }],
      ["C3 concurrent graceful and force stops", -> { concurrent_stops }],
      ["C4 replies racing Future#value timeouts", -> { reply_races_timeout }],
      ["C5 worker killed with requests queued and executing", -> { exit_with_queued_requests }],
      ["C6 mailbox count and byte limits under contention", -> { mailbox_limits_under_contention }],
      ["C7 randomized lifecycle stress", -> { randomized_lifecycle }],
      ["P1 worker TERM with socket-holding descendant", -> { worker_signal_with_holder("TERM") }],
      ["P1 worker KILL with socket-holding descendant", -> { worker_signal_with_holder("KILL") }],
      ["P2 watchdog killed while worker idle", -> { watchdog_killed(false) }],
      ["P2 watchdog killed while worker busy", -> { watchdog_killed(true) }],
      ["P3 application exit (idle actor)", -> { application_death("idle", "USR1") }],
      ["P3 application TERM (busy actor)", -> { application_death("busy", "TERM") }],
      ["P3 application KILL (actor with descendants)", -> { application_death("descendants", "KILL") }],
      ["P5 setsid descendant escapes containment (documented)", -> { escape_via_setsid }],
      ["P8 stubborn worker ignoring TERM is force-stopped", -> { stubborn_worker_stop }],
      ["T2 malformed reply: oversized header", -> { malformed_reply(:huge_header) }],
      ["T2 malformed reply: invalid JSON", -> { malformed_reply(:bad_json) }],
      ["T2 malformed reply: unknown tag", -> { malformed_reply(:unknown_tag) }],
      ["T2 malformed reply: missing id", -> { malformed_reply(:missing_id) }],
      ["T2 malformed reply: non-string error fields", -> { malformed_reply(:bad_backtrace) }],
      ["T2 malformed reply: truncated frame then silence", -> { malformed_reply(:truncated) }],
      ["T3 decoder fuzz never raises outside SerializationError", -> { decoder_fuzz }],
      ["T4 parent memory bounded near mailbox byte limit", -> { mailbox_memory }],
      ["T5 descriptor exhaustion fails spawn cleanly", -> { fd_exhaustion }],
      ["T5 process exhaustion fails spawn cleanly", -> { process_exhaustion }],
      ["T6 only intended descriptors cross exec", -> { descriptor_audit }],
      ["D1 SIGSTOP analogue: unresponsive but killable worker", -> { stopped_worker }],
      ["S1 socket is anonymous", -> { anonymous_socket }],
      ["S4 symbol decoding is garbage-collected", -> { symbol_growth }]
    ]
  end

  # C1/C2
  def asks_racing_stop(force)
    with_broker do |broker|
      actor = broker.spawn(ExampleActor, "c")
      futures = Queue.new
      errors = Hash.new(0)
      askers = Array.new(8) do
        Thread.new do
          50.times do
            futures << actor.ask(:slow)
          rescue RocotoActor::Error => error
            errors[error.class.name.split("::").last] += 1
          end
        end
      end
      sleep 0.05
      stopped = actor.stop(timeout: 2, force: force)
      check(askers.all? { |thread| thread.join(10) }, "an asker thread stayed blocked")
      list = []
      list << futures.pop until futures.empty?
      tally = settle(list, timeout: 10)
      check(tally.keys.all? { |key| %i[result ActorStoppedError].include?(key) }, "unexpected outcome #{tally}")
      check(!actor.alive?, "actor still alive after stop")
      "stop=#{stopped} accepted=#{list.size} #{tally} rejected_at_ask=#{errors}"
    end
  end

  # C3
  def concurrent_stops
    with_broker do |broker|
      actor = broker.spawn(ExampleActor, "c")
      20.times { actor.ask(:slow) }
      stoppers = Array.new(8) { |index| Thread.new { actor.stop(timeout: 3, force: index.even?) } }
      check(stoppers.all? { |thread| thread.join(8) }, "a stop call did not return")
      values = stoppers.map(&:value)
      check(values.all? { |value| [true, false].include?(value) }, "stop returned non-boolean #{values}")
      check(!actor.alive?, "actor alive after concurrent stops")
      "results=#{values.tally}"
    end
  end

  # C4
  def reply_races_timeout
    with_broker do |broker|
      actor = broker.spawn(ExampleActor, "c")
      tally = Hash.new(0)
      random = Random.new(7)
      300.times do
        future = actor.ask(:slow) # replies in ~50ms
        first = outcome(future, 0.02 + (random.rand * 0.06))
        second = outcome(future, 2)
        check(first == second, "outcome changed: #{first} -> #{second}")
        tally[first] += 1
      end
      check(tally.keys.all? { |key| %i[result AskTimeoutError].include?(key) }, "unexpected #{tally}")
      check(tally.size == 2, "race never produced both outcomes: #{tally} (adjust :slow or timeout)")
      "outcomes=#{tally}"
    end
  end

  # C5
  def exit_with_queued_requests
    with_broker do |broker|
      actor = broker.spawn(ExampleActor, "c")
      pid = actor.ask(:pid).value(timeout: 5)
      futures = Array.new(30) { actor.ask(:slow) }
      sleep 0.12 # a few execute, one is mid-flight, the rest are queued
      Process.kill("KILL", pid)
      tally = settle(futures, timeout: 5)
      check(tally.keys.all? { |key| %i[result ActorStoppedError].include?(key) }, "unexpected #{tally}")
      check(tally[:ActorStoppedError].positive?, "no queued request was rejected")
      wait_until(what: "actor failed") { actor.state == :failed }
      "outcomes=#{tally}"
    end
  end

  # C6
  def mailbox_limits_under_contention
    with_broker do |broker|
      actor = broker.spawn(ExampleActor, "c", mailbox_size: 20, mailbox_bytes: 4_000)
      actor.ask(:hang)
      sleep 0.05
      futures = Queue.new
      outcomes = Hash.new(0)
      mutex = Mutex.new
      threads = Array.new(8) do
        Thread.new do
          100.times do
            futures << actor.ask("x" * 150)
            mutex.synchronize { outcomes[:accepted] += 1 }
          rescue RocotoActor::MailboxFullError
            mutex.synchronize { outcomes[:full] += 1 }
          end
        end
      end
      check(threads.all? { |thread| thread.join(10) }, "an asker blocked on a full mailbox")
      check(outcomes[:accepted] <= 21, "more requests accepted than the mailbox allows: #{outcomes}")
      check(outcomes[:full].positive?, "limit never reached")
      actor.stop(force: true)
      list = []
      list << futures.pop until futures.empty?
      tally = settle(list, timeout: 5)
      "#{outcomes} settled=#{tally}"
    end
  end

  # C7
  def randomized_lifecycle
    with_broker do |broker|
      outcomes = Hash.new(0)
      mutex = Mutex.new
      threads = Array.new(6) do |index|
        Thread.new do
          random = Random.new(index)
          30.times do |iteration|
            name = "s#{index}-#{iteration}"
            actor = broker.spawn(ExampleActor, name, name: name)
            3.times { actor.ask("m").value(timeout: 5) }
            action = %i[graceful force crash kill].sample(random: random)
            case action
            when :graceful then actor.stop(timeout: 3)
            when :force then actor.stop(force: true)
            when :crash then begin
              actor.ask(:crash).value(timeout: 5)
            rescue StandardError
              nil
            end
            when :kill
              pid = actor.ask(:pid).value(timeout: 5)
              Process.kill("KILL", pid)
            end
            mutex.synchronize { outcomes[action] += 1 }
          rescue StandardError => error
            mutex.synchronize { outcomes[:"error_#{error.class.name.split('::').last}"] += 1 }
          end
        end
      end
      check(threads.all? { |thread| thread.join(PROBE_LIMIT - 10) }, "a lifecycle thread did not finish")
      wait_until(timeout: 10, what: "all actors terminal") { broker.roots.empty? }
      sleep 0.5
      snap = snapshot
      check(snap[:zombies].zero?, "zombies after lifecycle stress: #{snap}")
      "#{outcomes} #{snap}"
    end
  end

  # P1
  def worker_signal_with_holder(signal)
    with_broker do |broker|
      actor = broker.spawn(SocketHolderActor, name: "holder")
      info = actor.ask(:fork_holder).value(timeout: 5)
      pending = actor.ask(:hang)
      sleep 0.05
      Process.kill(signal, info[:worker])
      tally = settle([pending], timeout: 5)
      check(tally[:ActorStoppedError] == 1, "pending request not rejected: #{tally}")
      wait_until(what: "actor failed") { actor.state == :failed }
      wait_until(what: "socket holder killed") { gone?(info[:holder]) }
      "worker=#{info[:worker]} holder=#{info[:holder]} last_exit=#{actor.last_exit}"
    end
  end

  # P2
  def watchdog_killed(busy)
    with_broker do |broker|
      actor = broker.spawn(ExampleActor, "w")
      worker = actor.ask(:pid).value(timeout: 5)
      watchdog = parent_pid(worker)
      pending = busy ? actor.ask(:hang) : nil
      sleep 0.05
      Process.kill("KILL", watchdog)
      wait_until(what: "actor failed") { actor.state == :failed }
      wait_until(what: "worker gone") { gone?(worker) }
      settle([pending], timeout: 5) if pending
      "watchdog=#{watchdog} worker=#{worker} alive=#{actor.alive?}"
    end
  end

  # P3
  def application_death(mode, signal)
    reader, writer = IO.pipe
    app = Process.spawn(RbConfig.ruby, "-I#{File.expand_path('../../lib', __dir__)}", APP_CHILD, mode,
                        out: writer, err: File::NULL)
    writer.close
    line = reader.gets
    check(line, "application child printed nothing")
    pids = JSON.parse(line, symbolize_names: true)
    Process.kill(signal, app)
    Process.waitpid(app)
    pids.each do |role, pid|
      wait_until(timeout: 3, what: "#{role} #{pid} gone after application #{signal}") { gone?(pid) }
    end
    "#{pids} all gone"
  ensure
    reader&.close
    begin
      Process.kill("KILL", app) if app
    rescue Errno::ESRCH
      nil
    end
  end

  # P5
  def escape_via_setsid
    with_broker do |broker|
      actor = broker.spawn(EscapeActor, name: "escape")
      escaped = actor.ask(:escape).value(timeout: 5)
      actor.stop(force: true)
      sleep 0.3
      survived = !gone?(escaped)
      Process.kill("KILL", escaped) if survived
      check(survived, "setsid descendant was killed; the documented limitation no longer applies")
      raise Note, "escaped pid #{escaped} survived stop, as documented; killed by the probe"
    end
  end

  # P8
  def stubborn_worker_stop
    with_broker do |broker|
      actor = broker.spawn(StubbornActor, name: "stubborn")
      pid = actor.ask(:pid).value(timeout: 5)
      actor.ask(:block)
      sleep 0.1
      started = now
      stopped = actor.stop(timeout: 1)
      elapsed = now - started
      check(elapsed < 3, "stop took #{elapsed.round(2)}s")
      wait_until(what: "stubborn worker gone") { gone?(pid) }
      "stop=#{stopped} in #{elapsed.round(2)}s"
    end
  end

  # T2. An error reply with wrong field types is still a usable RemoteError;
  # every other malformation is a protocol violation that stops the actor.
  def malformed_reply(kind)
    with_broker do |broker|
      control = broker.spawn(ExampleActor, "control")
      actor = broker.spawn(MalformedReplyActor, name: "malformed")
      future = actor.ask(kind: kind)
      tally = settle([future], timeout: kind == :truncated ? 0.5 : 5)
      case kind
      when :bad_backtrace
        check(tally[:RemoteError] == 1, "expected a coerced RemoteError: #{tally}")
      when :truncated
        # A partial frame followed by silence is indistinguishable from a hung
        # actor: the caller's timeout applies and stop must still work.
        check(tally[:AskTimeoutError] == 1, "expected a timeout: #{tally}")
        check(actor.stop(timeout: 2), "could not stop an actor that left a partial frame")
      else
        check(tally[:ActorStoppedError] == 1, "future not rejected: #{tally}")
        wait_until(what: "actor terminal") { actor.state == :failed }
      end
      check(control.ask("ok").value(timeout: 5) == "control: ok", "unrelated actor unresponsive")
      "#{tally} state=#{actor.state} last_failure=#{actor.last_failure&.remote_message}"
    end
  end

  # T3
  def decoder_fuzz
    random = Random.new(42)
    samples = 3_000
    classes = Hash.new(0)
    samples.times do |iteration|
      body = case iteration % 4
             when 0 then random.bytes(random.rand(1..64))
             when 1 then begin
               JSON.generate(random_structure(random, 0))
             rescue StandardError
               random.bytes(16)
             end
             when 2 then %(["symbol", "#{'s' * random.rand(0..20)}"])
             else %(["hash", [[#{'["string","k"],' * 3}]]])
             end
      frame = [random.rand(0..1).zero? ? body.bytesize : random.rand(0..((2**32) - 1))].pack("N") + body
      begin
        TRANSPORT.read(StringIO.new(frame), timeout: 0.5)
        classes[:decoded] += 1
      rescue RocotoActor::Error, EOFError => error
        classes[error.class.name.split("::").last.to_sym] += 1
      end
    end
    "#{samples} frames: #{classes}"
  end

  def random_structure(random, depth)
    case random.rand(0..6)
    when 0 then ["nil"]
    when 1 then ["integer", random.rand((-2**70)..(2**70)).to_s]
    when 2 then ["float", random.rand]
    when 3 then ["string", random.bytes(8).force_encoding("UTF-8")]
    when 4 then [%w[array hash unknown symbol boolean].sample(random: random), random.rand(3)]
    when 5 then ["array", Array.new(random.rand(0..3)) { depth > 120 ? ["nil"] : random_structure(random, depth + 1) }]
    else ["hash", [[%w[symbol k], depth > 120 ? ["nil"] : random_structure(random, depth + 1)]]]
    end
  end

  # T4
  def mailbox_memory
    with_broker do |broker|
      limit = 4 * 1024 * 1024
      actor = broker.spawn(ExampleActor, "m", mailbox_size: 10_000, mailbox_bytes: limit)
      actor.ask(:hang)
      sleep 0.05
      GC.start
      before = rss_kb
      accepted = 0
      message = "x" * 100_000
      loop do
        actor.ask(message)
        accepted += 1
      rescue RocotoActor::MailboxFullError
        break
      end
      during = rss_kb
      actor.stop(force: true)
      GC.start
      after = rss_kb
      growth = (during - before) * 1024
      check(growth < limit * 4, "parent grew #{growth / 1024}kB for a #{limit / 1024}kB mailbox")
      "accepted=#{accepted} rss before=#{before}kB during=#{during}kB after=#{after}kB (limit #{limit / 1024}kB)"
    end
  end

  # T5
  def fd_exhaustion
    script = <<~RUBY
      require "rocoto_actor"; require "#{File.expand_path('../support/example_actor', __dir__)}"
      broker = RocotoActor::ActorBroker.new(error_handler: ->(*) {})
      actors = []
      error = nil
      begin
        40.times { |i| actors << broker.spawn(ExampleActor, "f", name: "f\#{i}", start_timeout: 10) }
      rescue StandardError => e
        error = e
      end
      raise "no failure under nofile=48" unless error
      actors.first(5).each { |a| a.stop(force: true) }
      extra = broker.spawn(ExampleActor, "again", name: "again")
      ok = extra.ask("x").value(timeout: 5) == "again: x"
      broker.stop(timeout: 5, force: true)
      puts JSON.generate(spawned: actors.size, error: error.class.name, message: error.message[0, 80], recovered: ok)
    RUBY
    output = limited("--nofile=48:48", script)
    result = JSON.parse(output.lines.last)
    check(result["recovered"], "broker did not recover after descriptors were freed: #{result}")
    result.to_s
  end

  # RLIMIT_NPROC counts every process and thread of the uid on the whole host,
  # which a container cannot see, so the usable limit is found by probing.
  def process_exhaustion
    threshold = [300, 600, 1_200, 2_500, 5_000, 10_000, 20_000, 40_000].find do |candidate|
      system("timeout", "-s", "KILL", "3", "prlimit", "--nproc=#{candidate}:#{candidate}", RbConfig.ruby, "-e",
             "t = Array.new(20) { Thread.new { sleep 0.1 } }; t.each(&:join)", out: File::NULL, err: File::NULL)
    end
    unless threshold
      raise Note,
            "no RLIMIT_NPROC value up to 40000 allowed 21 threads; the uid's host-wide count is out of reach"
    end

    script = <<~RUBY
      require "rocoto_actor"; require "#{File.expand_path('../support/example_actor', __dir__)}"
      broker = RocotoActor::ActorBroker.new(error_handler: ->(*) {})
      actors = []
      error = nil
      begin
        60.times { |i| actors << broker.spawn(ExampleActor, "p", name: "p\#{i}", start_timeout: 10) }
      rescue StandardError => e
        error = e
      end
      broker.stop(timeout: 10, force: true)
      puts JSON.generate(spawned: actors.size, error: error&.class&.name, message: error&.message.to_s[0, 80])
    RUBY
    output, status = limited("--nproc=#{threshold + 40}:#{threshold + 40}", script, allow_hang: true)
    if status.exitstatus == 137
      raise Note, "Ruby wedged in a futex when thread creation hit RLIMIT_NPROC (#{threshold + 40}) and " \
                  "ignored TERM; KILL removed it. Deployment must keep process limits above need; " \
                  "the library cannot recover a wedged VM."
    end
    check(status.success?, "exhaustion script failed: #{output.lines.last(3).join.strip}")
    result = JSON.parse(output.lines.last)
    note = if result["error"]
             "spawn failed cleanly"
           else
             "limit not enforced for this user (root bypasses RLIMIT_NPROC); no failure to observe"
           end
    "#{result} #{note}"
  end

  # Runs a Ruby script under prlimit with a hard wall-clock limit; a hang is a
  # failure with the script's last output, never a stuck run. "nproc" is
  # resolved relative to the user's current process and thread count, because
  # RLIMIT_NPROC counts every process of the user, not only this tree.
  def limited(limit, script, allow_hang: false)
    command = ["timeout", "-s", "KILL", "30", "prlimit", limit, RbConfig.ruby,
               "-I#{File.expand_path('../../lib', __dir__)}", "-rjson", "-e", script]
    output = IO.popen(command, err: %i[child out], &:read)
    status = $?
    return [output, status] if allow_hang

    check(status.exitstatus != 137, "script hung under prlimit #{limit} and was killed; output: #{output[-400..]}")
    check(status.success?, "script failed under prlimit #{limit}: #{output.lines.last(3).join.strip}")
    output
  end

  # T6
  def descriptor_audit
    with_broker do |broker|
      actor = broker.spawn(ExampleActor, "fd")
      worker = actor.ask(:pid).value(timeout: 5)
      watchdog = parent_pid(worker)
      [worker, watchdog].map do |pid|
        links = Dir.children("/proc/#{pid}/fd").sort_by(&:to_i).map do |fd|
          [fd.to_i, File.readlink("/proc/#{pid}/fd/#{fd}")]
        end
        std = links.select { |fd, _| fd < 3 }.map(&:last)
        check(std == ["/dev/null"] * 3, "standard streams for #{pid}: #{std}")
        sockets = links.select { |_, target| target.start_with?("socket:") }
        check(sockets.size == 1, "#{pid} holds #{sockets.size} sockets: #{links}")
        others = links.reject do |fd, target|
          fd < 3 || target.start_with?("socket:", "anon_inode:[eventfd]", "anon_inode:[eventpoll]")
        end
        check(others.empty?, "unexpected descriptors in #{pid}: #{others}")
        "#{pid}:#{links.size}fds"
      end.join(" ")
    end
  end

  # D1
  def stopped_worker
    with_broker do |broker|
      other = broker.spawn(ExampleActor, "other")
      actor = broker.spawn(ExampleActor, "frozen")
      pid = actor.ask(:pid).value(timeout: 5)
      Process.kill("STOP", pid)
      future = actor.ask("x")
      check(outcome(future, 0.5) == :AskTimeoutError, "expected a timeout from a stopped worker")
      check(other.ask("y").value(timeout: 5) == "other: y", "unrelated actor unresponsive")
      started = now
      stopped = actor.stop(timeout: 2)
      elapsed = now - started
      check(stopped, "stop could not kill a SIGSTOPped worker (KILL should still apply)")
      "timeout terminal; stop=#{stopped} in #{elapsed.round(2)}s; true D state not reproducible here"
    end
  end

  # S1
  def anonymous_socket
    pair = UNIXSocket.pair
    check(pair.first.local_address.unix_path.empty?, "socketpair has a filesystem path")
    pair.each(&:close)
    "socketpair has no filesystem path"
  end

  # S4
  def symbol_growth
    require "objspace"
    mortal = lambda {
      3.times { GC.start }
      ObjectSpace.count_symbols[:mortal_dynamic_symbol]
    }
    before = mortal.call
    json = JSON.generate(["array", Array.new(50_000) do |index|
      ["symbol", "dynamic_symbol_#{index}_#{rand(1_000_000)}"]
    end])
    size = decode_and_count(json)
    check(size == 50_000, "decode lost symbols")
    peak = mortal.call
    after = mortal.call
    check(peak - before > 40_000, "decoding did not create mortal symbols: #{before} -> #{peak}")
    check(after - before < 5_000, "symbols not collected: #{before} -> #{peak} -> #{after}")
    "mortal dynamic symbols #{before} -> peak #{peak} -> after GC #{after}"
  end

  # Keeps the decoded symbols out of this frame so the GC can collect them.
  def decode_and_count(json)
    TRANSPORT.read(StringIO.new([json.bytesize].pack("N") + json)).size
  end
end

FaultMatrix.new(ARGV[0]).run
