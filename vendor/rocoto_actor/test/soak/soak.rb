# frozen_string_literal: true

# Long-running soak test: continuous ask/tell traffic plus injected failures,
# restarts, stops, and respawns, while sampling threads, file descriptors,
# child processes, zombies, and RSS for leaks. Not part of the normal suite.
#
#   SOAK_SECONDS=1800 bundle exec ruby -Ilib test/soak/soak.rb
#
# Exits non-zero when a resource trends upward, a zombie persists, a child
# process outlives the broker, or an unexpected error class is observed.

require_relative "../../lib/rocoto_actor"
require_relative "../support/example_actor"
require_relative "../support/process_actor"
require_relative "../support/supervisor_actor"
require_relative "../support/tell_actor"

DURATION = Integer(ENV.fetch("SOAK_SECONDS", "600"))
SAMPLE_EVERY = Float(ENV.fetch("SOAK_SAMPLE_SECONDS", "5"))
CHAOS_EVERY = Float(ENV.fetch("SOAK_CHAOS_SECONDS", "1.5"))

EXPECTED_ERRORS = %w[
  RocotoActor::ActorStoppedError RocotoActor::ActorRestartingError RocotoActor::ActorFailedError
  RocotoActor::AskTimeoutError RocotoActor::MailboxFullError
].freeze

REFERENCE = RocotoActor.const_get(:Reference) # internal; instrumented and counted here

# Logs any Reference#stop that could not confirm the process group was gone.
module StopDiagnostics
  def stop(timeout: REFERENCE::DEFAULT_STOP_TIMEOUT, force: false)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    result = super
    unless result
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      warn format("reference %d stop(timeout: %.3f, force: %s) => false after %.3fs", pid, timeout, force, elapsed)
    end
    result
  end
end
REFERENCE.prepend(StopDiagnostics)

class Soak
  Sample = Struct.new(:at, :threads, :fds, :children, :zombies, :zombie_pids, :rss_kb, :actors, :live_slots,
                      :references, :futures, :handles)

  def initialize
    @broker = RocotoActor::ActorBroker.new
    @stats = Hash.new(0)
    @stats_mutex = Mutex.new
    @stop = false
    @samples = []
    @problems = []
    @handles = {}
  end

  def run
    baseline = sample("baseline")
    spawn_population
    threads = traffic_threads + [chaos_thread, sampler_thread]
    sleep DURATION
    @stop = true
    threads.each { |thread| thread.join(30) || problem("thread #{thread.name} did not finish") }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    stopped = @broker.stop(timeout: 10)
    puts format("broker.stop => %s in %.3fs", stopped, Process.clock_gettime(Process::CLOCK_MONOTONIC) - started)
    problem("broker.stop returned false") unless stopped
    sleep 1
    final = sample("final")
    report(baseline, final)
  end

  private

  def spawn_population
    @handles[:database] = @broker.spawn(ExampleActor, "database", name: "database", restart: :on_failure,
                                                                  max_restarts: 1_000, restart_window: 1,
                                                                  restart_backoff: 0.01)
    @handles[:counter] = @broker.spawn(CountingActor, name: "counter")
    @handles[:collector] = @broker.spawn(CollectorActor, name: "collector", restart: :on_failure,
                                                         max_restarts: 1_000, restart_window: 1,
                                                         restart_backoff: 0.01)
    @handles[:supervisor] = @broker.spawn(InitSpawnActor, 1, 2, name: "supervisor", restart: :on_failure,
                                                                max_restarts: 1_000, restart_window: 1,
                                                                restart_backoff: 0.01)
    3.times do |index|
      @handles[:"forwarder#{index}"] = @broker.spawn(ForwardingActor, @handles[:database], name: "forwarder#{index}",
                                                                                           restart: :on_failure,
                                                                                           max_restarts: 1_000,
                                                                                           restart_window: 1,
                                                                                           restart_backoff: 0.01)
    end
  end

  def traffic_threads
    askers = Array.new(3) do |index|
      named_thread("asker#{index}") do
        until @stop
          attempt(:ask_forwarder) { @handles[:"forwarder#{index}"].ask(message: "ping", timeout: 3).value(timeout: 5) }
          attempt(:ask_database) { @handles[:database].ask("direct").value(timeout: 5) }
          attempt(:ask_supervisor) { @handles[:supervisor].ask("fan").value(timeout: 10) }
          attempt(:ask_counter) { @handles[:counter].ask(:tick).value(timeout: 5) }
        end
      end
    end
    teller = named_thread("teller") do
      value = 0
      until @stop
        20.times { attempt(:tell_collector) { @handles[:collector].tell(op: :record, value: (value += 1)) } }
        attempt(:ask_collector) { @handles[:collector].ask(:messages).value(timeout: 5) }
        sleep 0.05
      end
    end
    askers + [teller]
  end

  def chaos_thread
    named_thread("chaos") do
      actions = %i[crash_database kill_database_worker boom_collector crash_supervisor stop_and_respawn_counter
                   spawn_and_stop_child]
      index = 0
      until @stop
        action = actions[index % actions.size]
        index += 1
        attempt(action) { send(action) }
        sleep CHAOS_EVERY
      end
    end
  end

  def crash_database
    @handles[:database].ask(:crash).value(timeout: 2)
  rescue RocotoActor::ActorStoppedError
    nil
  end

  # An external SIGKILL of the database worker, as an OOM killer would do.
  def kill_database_worker
    pid = @handles[:database].ask(:pid).value(timeout: 2)
    Process.kill("KILL", pid) if pid.is_a?(Integer)
  end

  def boom_collector
    @handles[:collector].tell(op: :boom)
  end

  def crash_supervisor
    @handles[:supervisor].ask(:crash).value(timeout: 2)
  rescue RocotoActor::ActorStoppedError
    nil
  end

  def stop_and_respawn_counter
    @handles[:counter].stop(timeout: 3)
    @handles[:counter] = @broker.spawn(CountingActor, name: "counter")
  end

  def spawn_and_stop_child
    child = @broker.spawn(ExampleActor, "temp", name: "temp-#{rand(1_000_000)}", parent: @handles[:database])
    child.ask("hello").value(timeout: 2)
    child.stop(timeout: 3)
  end

  def sampler_thread
    named_thread("sampler") do
      until @stop
        sample("run")
        sleep SAMPLE_EVERY
      end
    end
  end

  def sample(label)
    GC.start
    children = child_processes
    zombies = children.select { |_pid, state| state.start_with?("Z") }.keys
    sample = Sample.new(
      Process.clock_gettime(Process::CLOCK_MONOTONIC),
      Thread.list.size,
      Dir.children("/proc/self/fd").size,
      children.size,
      zombies.size,
      zombies,
      rss_kb,
      begin
        @broker.roots.size
      rescue StandardError
        0
      end,
      GC.stat(:heap_live_slots),
      ObjectSpace.each_object(REFERENCE).count,
      ObjectSpace.each_object(RocotoActor::Future).count,
      ObjectSpace.each_object(RocotoActor::ActorHandle).count
    )
    @samples << sample
    puts format("%-8s t=%5.0fs threads=%3d fds=%4d children=%3d zombies=%2d rss=%7dkB actors=%2d " \
                "slots=%8d refs=%4d futures=%4d handles=%4d",
                label, sample.at - (@samples.first&.at || sample.at), sample.threads, sample.fds,
                sample.children, sample.zombies, sample.rss_kb, sample.actors, sample.live_slots,
                sample.references, sample.futures, sample.handles)
    sample
  end

  # Direct children by pid and state, excluding the ps process doing the listing.
  def child_processes
    `ps -o pid=,stat=,comm= --ppid #{Process.pid}`.lines.map(&:split)
                                                  .reject { |_pid, _state, command| command == "ps" }
                                                  .to_h { |pid, state, _command| [Integer(pid), state] }
  rescue StandardError
    {}
  end

  def rss_kb
    File.read("/proc/self/status")[/VmRSS:\s+(\d+)/, 1].to_i
  end

  def attempt(name)
    yield
    count(:"#{name}_ok")
  rescue RocotoActor::RemoteError => error
    count(:"#{name}_remote_#{error.remote_class}")
    problem("unexpected remote error in #{name}: #{error.message}") unless EXPECTED_ERRORS.include?(error.remote_class)
  rescue RocotoActor::Error => error
    count(:"#{name}_#{error.class.name}")
    unless EXPECTED_ERRORS.include?(error.class.name)
      problem("unexpected error in #{name}: #{error.class}: #{error.message}")
    end
  rescue StandardError => error
    count(:"#{name}_#{error.class.name}")
    problem("unexpected exception in #{name}: #{error.class}: #{error.message}")
  end

  def count(key)
    @stats_mutex.synchronize { @stats[key] += 1 }
  end

  def problem(message)
    @stats_mutex.synchronize { @problems << message unless @problems.include?(message) }
  end

  def named_thread(name, &)
    thread = Thread.new(&)
    thread.name = name
    thread
  end

  def report(baseline, final)
    puts "\nOutcomes:"
    @stats.sort.each { |key, value| puts format("  %-60s %8d", key, value) }

    running = @samples.select { |sample| sample.at > baseline.at && sample.at < final.at }
    if running.size >= 4
      head = running.first(running.size / 2)
      tail = running.last(running.size / 2)
      %i[threads fds children rss_kb live_slots references futures handles].each do |metric|
        first = head.sum(&metric).fdiv(head.size)
        last = tail.sum(&metric).fdiv(tail.size)
        # Threads, descriptors, and children are deterministic and must not
        # drift at all. Futures and handles are in-flight objects whose count
        # swings with load; references and terminal nodes are retained by
        # design. A leak of futures would pin references too, so the strict
        # check on references covers it.
        limit = case metric
                when :rss_kb, :live_slots, :futures then (first * 1.25) + 20
                when :references, :handles then first + 50
                else first + 5
                end
        problem("#{metric} trended upward: #{first.round(1)} -> #{last.round(1)}") if last > limit
      end
    end
    # A child is briefly a zombie between exit and the reaper's waitpid; only a
    # pid that stays a zombie across consecutive samples is a leak.
    running.each_cons(2) do |earlier, later|
      persistent = earlier.zombie_pids & later.zombie_pids
      problem("zombie persisted across samples: #{persistent.inspect}") unless persistent.empty?
    end
    problem("#{final.children} child processes outlived the broker") if final.children.positive?
    if final.threads > baseline.threads + 1
      problem("threads did not return to baseline: #{baseline.threads} -> #{final.threads}")
    end
    problem("fds did not return to baseline: #{baseline.fds} -> #{final.fds}") if final.fds > baseline.fds + 2

    if @problems.empty?
      puts "\nSOAK PASSED (#{DURATION}s)"
    else
      puts "\nSOAK FAILED:"
      @problems.each { |message| puts "  - #{message}" }
      exit 1
    end
  end
end

Soak.new.run
