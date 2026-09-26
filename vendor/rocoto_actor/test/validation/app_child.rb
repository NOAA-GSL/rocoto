# frozen_string_literal: true

# A throwaway application for the fault matrix: spawns actors in the requested
# state, prints every relevant pid as JSON on stdout, then idles until killed.
#
#   ruby -Ilib test/validation/app_child.rb idle|busy|descendants

require_relative "../../lib/rocoto_actor"
require_relative "../support/example_actor"
require_relative "support"
require "json"

mode = ARGV.fetch(0)
Signal.trap("USR1") { exit 0 } # "normal exit" for the fault matrix, without stopping the broker first
broker = RocotoActor::ActorBroker.new
pids = { app: Process.pid }

case mode
when "idle"
  actor = broker.spawn(ExampleActor, "idle", name: "idle")
  pids[:worker] = actor.ask(:pid).value(timeout: 5)
when "busy"
  actor = broker.spawn(ExampleActor, "busy", name: "busy")
  pids[:worker] = actor.ask(:pid).value(timeout: 5)
  actor.ask(:hang)
  sleep 0.2
when "descendants"
  actor = broker.spawn(SocketHolderActor, name: "holder")
  info = actor.ask(:fork_holder).value(timeout: 5)
  pids[:worker] = info[:worker]
  pids[:holder] = info[:holder]
end

$stdout.puts(JSON.generate(pids))
$stdout.flush
sleep 300
