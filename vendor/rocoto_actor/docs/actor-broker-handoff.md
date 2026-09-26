# ActorBroker Implementation Handoff

## Purpose

This document preserves the design discussion and current implementation state for adding process-isolated actor handles and a parent-owned actor broker to RocotoActor. It is intended for a future Copilot, Claude Code, or other coding session.

## Repository and baseline

- Repository: `/home/admin/rocoto_actor`
- Runtime used for validation: Ruby 3.4.10 on Linux (linuxkit); the gem requires Ruby >= 3.2 (raised from 3.1 on 2026-09-22 because 3.1 is end-of-life)
- Existing actor model: one watchdog process group per actor, one Unix socket pair between the application and that actor, parent-side reader/writer/reaper threads, bounded outbound mailbox.
- The previously identified concurrent `Reference#stop` race was fixed in `lib/rocoto_actor/reference.rb` and covered by `test_concurrent_stop_waits_for_existing_shutdown`.
- Before the broker prototype, the suite passed with 35 runs and 70 assertions. The concurrent ask/stop and timeout/load stress probes passed.

## Core design decisions

### Process isolation remains primary

Actors must continue to run in separate operating-system processes. Do not move toward a shared-thread actor runtime. The bulkheading requirement is the reason for RocotoActor.

### Scale: a handful of actors, never thousands

Process-based actors are expensive to create and run; that cost is accepted because separate processes are the only way to bulkhead Ruby code given the GVL. The practical population is a handful of actors per application. Do not design for thousands: retained terminal nodes, per-actor threads (reader, writer, reaper), and per-restart process creation are all fine at this scale. Thread-based actors were considered and deliberately left out; a hung thread-based actor could hang the application, which defeats the purpose of the library.

### Idempotency keys and message ids are application design

Deduplication has to live in the receiver, recorded atomically with the effect (same transaction) when the effect is in a transactional store, and pushed to the downstream system's own idempotency mechanism when it is not. Message ids for correlating a `tell` reply with its request are likewise part of the application's protocol. The library carries no key or id slot in the envelope and never retries.

### Flat OS ownership, logical actor hierarchy

The application-side broker owns every actual actor process, `Reference`, socket, reader thread, writer thread, reaper, and lifecycle decision. The OS process tree should not be used as the actor hierarchy.

Logical hierarchy is broker metadata only:

```text
ActorBroker
  logical path: database
  logical path: workers/a
  logical path: workers/a/cache
```

Cross-branch communication is routed through the same broker:

```text
workers/a -> broker -> database
workers/a -> broker -> workers/b
```

### Actor handles are opaque logical capabilities

A handle passed through transport must contain only an opaque logical actor ID or path. It must not contain a `Reference`, mutex, thread, socket, file descriptor, or process object. The broker maps the logical ID to the current parent-owned `Reference`.

This indirection is intentionally stable across future actor restarts:

```text
logical actor ID: database
current binding: generation 1 -> socket A
restart:          generation 2 -> socket B
```

### No socket descriptor transfer

Each actor gets exactly one socket pair with the parent broker at spawn time. Actor-to-actor calls send broker protocol messages over the requesting actor's existing socket. The broker performs the target `Reference#ask` and sends the result back over the source socket. Do not use `SCM_RIGHTS` or pass sockets between actors.

### Delivery semantics

The default should remain at-most-once. A failure can mean that a request was never received, was executing, completed before its response was lost, or completed and the actor died. Do not automatically retry non-idempotent database operations.

Future request IDs/idempotency keys may be added so applications can implement safe deduplication, especially for SQLite writes. Exactly-once effects are not provided by transport alone.

### Synchronous calls are dangerous

`ActorHandle#call` is documented as a convenience with deadlock risk. A one-message-at-a-time actor can deadlock:

```text
A synchronously waits for B
B synchronously waits for A
```

The long-term preferred API is asynchronous `handle.ask`, returning a future that can be integrated with the actor loop. `call` should either be documented as a convenience with deadlock risk or replaced with a nonblocking continuation/message pattern.

## Current prototype files

- `lib/rocoto_actor/broker.rb`: `ActorBroker` registry and lifecycle nodes, bounded non-blocking routing, the service thread (route expiries and failure cleanup), and the lifecycle pool for actor-initiated spawn/stop.
- `lib/rocoto_actor/handle.rb`: `ActorHandle`; in the application it delegates to the broker, in an actor it sends requests through `BrokerClient`.
- `lib/rocoto_actor/broker_client.rb`: worker-side per-socket request channel with unique request IDs and deferred frames.
- `lib/rocoto_actor/context.rb`: `ActorContext`, exposed as `RocotoActor.context` inside an actor (`spawn`, `handle`, `schedule`).
- `lib/rocoto_actor/timer.rb`: `Timer`, the cancellable result of `context.schedule`.
- `lib/rocoto_actor.rb`: loads the library and holds worker-side accessors (`worker_process?`, `context`, `broker_client`); no public spawn.
- `lib/rocoto_actor/launcher.rb`: internal (`private_constant`) process launcher (`launch` returns `[reference, boot_future]`), startup error mapping, and process-group helpers used by `ActorBroker` and `Reference`.
- `lib/rocoto_actor/runner.rb`: worker entry point; marks the worker process, builds the context from the boot message, marks the broker client ready after `:ready`, and runs the actor loop (`run_actor` drains deferred frames and flattens `RemoteError` provenance in `error_response`).
- `lib/rocoto_actor/future.rb`: `on_resolve` callbacks and `expire` used by the broker to answer without a waiting thread.
- `lib/rocoto_actor/transport.rb`: encodes handles as `['actor_handle', id]` and decodes them using the current transport thread's socket.
- `lib/rocoto_actor/reference.rb`: accepts broker requests from an actor and queues broker responses on the actor's existing writer, with an `on_done` callback per response for broker accounting.
- `test/support/process_actor.rb`: contains `ForwardingActor` used by the broker test.
- `test/support/supervisor_actor.rb`: `SupervisorActor`, `InitSpawnActor`, and `FailingInitSpawnActor` exercising `RocotoActor.context`.
- `test/support/tell_actor.rb`: `CollectorActor` and `CoordinatorActor` exercising `tell`, `context.sender`, and failure in told messages.
- `test/broker_test.rb`: initial end-to-end broker tests.
- `README.md`: preliminary shared-actor documentation.

## Current protocol sketch

Actor A sends over A's existing socket:

```ruby
{
  op: :broker_request,
  request_id: integer,
  handle_id: opaque_id,
  message: supported_value
}
```

The parent broker looks up `handle_id`, calls the target reference, and queues this response to A:

```ruby
{
  op: :broker_response,
  request_id: integer,
  ok: true,
  result: supported_value
}
```

Or:

```ruby
{
  op: :broker_response,
  request_id: integer,
  ok: false,
  error_class: string,
  message: string,
  backtrace: array
}
```

The existing transport is a request/reply stream, so broker responses and ordinary actor replies need correlation and must not be confused. The worker side uses the response `op`, serializes calls under a per-handle mutex, and defers any non-response frame read during a call (`RocotoActor.defer_frame`) so the actor loop processes it next in arrival order. The `timeout` field must be sent inside one explicit hash: `Transport.write(io, timeout:)` is the write deadline, and the prototype's keyword form silently dropped it.

Error provenance is flattened rather than nested: `error_response` and the broker both forward `remote_class`, `remote_message`, and `remote_backtrace` of a `RemoteError`, so a caller two hops away still sees `remote_class == "ArgumentError"`.

## Current known status

Implementation progress:

- [x] Preserve process-based actors and parent-owned socket pairs.
- [x] Add opaque transport-serializable actor handles.
- [x] Add the first parent-side broker routing path.
- [x] Add a forwarding-actor end-to-end smoke test.
- [x] Define initial broker error semantics: errors preserve provenance through nested `RemoteError` wrappers; do not reconstruct arbitrary exception classes.
- [x] Propagate per-call broker timeouts and cover a hanging target.
- [x] Run the broker suite and full suite after the current changes.
- [x] Add an initial per-call timeout field and propagate it to the target future.
- [x] Bound broker routing work with a finite dispatcher/request capacity (`max_routes`, `max_routes_per_actor`, `route_timeout`; no thread per route; one timer thread per broker).
- [x] Flatten `RemoteError` provenance across hops; typed errors for unknown handle, stopped target, invalid timeout, and broker capacity.
- [x] Fix asks arriving during a brokered `call` being dropped by the waiting handle.
- [x] Add logical parent/child lifecycle metadata (`ActorBroker::Node`: id, name, path, generation, parent_id, children, state, reference; `spawn(parent:, name:)`; recursive stop; failure propagation; `ActorFailedError`).
- [x] Add broker-owned child spawning (`RocotoActor.context.spawn`, `:broker_spawn`/`:broker_stop`, bounded lifecycle pool, per-socket `BrokerClient` with unique request IDs).
- [x] Add `tell` (one-way messages, broker-acked enqueue, sender handle in the envelope, `context.sender`, failure on unhandled exception with `handle.last_failure`).
- [x] Add death notification (`context.watch`/`unwatch`, `:actor_event` tells; `ActorBroker.new(on_event:)`), a `shutdown` callback on graceful stop, synchronous-call deadlock detection (`DeadlockError`), and `broker.describe` (2026-09-23).
- [x] Add self-scheduled tells (`context.schedule(message, after:, every:)` → `Timer#cancel`; broker-owned timers that die with the incarnation; undeliverable ticks dropped and reported).
- [x] Add restart policies and generations (`restart: :never | :on_failure`, `max_restarts`, `restart_window`, `restart_backoff`; `:restarting` state; `ActorRestartingError`; `handle.generation`).

The broker-focused command was run:

```sh
bundle exec ruby -Itest test/broker_test.rb
```

Latest validation (2026-09-23):

```text
broker: 95 runs, 428 assertions, 0 failures, 0 errors
full suite: 132 runs, 504 assertions, 0 failures, 0 errors
```

The outer caller sees the innermost remote class: a target `ArgumentError` arrives as `RemoteError` with `remote_class == "ArgumentError"` and `remote_message == "requested failure"`; a broker deadline arrives as `remote_class == "RocotoActor::AskTimeoutError"`; a stopped target as `"RocotoActor::ActorStoppedError"`; an over-capacity broker as `"RocotoActor::BrokerBusyError"`; an unknown handle as `"RocotoActor::Error"` with message `unknown actor handle`.

Remove generated `Gemfile.lock` if it is untracked and was created only by local validation.

### Routing design (implemented)

- `ActorBroker#route` runs on the source actor's reader thread and never waits on the target: it registers `Future#on_resolve` on the target future and returns.
- The response is written on the source `Reference` writer thread via `send_broker_response(..., on_done:)`; `on_done` fires when the frame is written or discarded (source stopped), releasing the per-source response slot.
- One `rocoto-actor-broker-timer` thread per broker expires target futures at their deadline (`Future#expire`), so a hanging target costs no thread.
- `max_routes` (global in-flight routes) rejects with `BrokerBusyError`; `max_routes_per_actor` (unwritten responses per source) blocks that source's reader only, which stops reading that actor's socket until responses drain.
- A source that stops with routes pending has its responses discarded by `discard_control_outbox`, which releases the route slots; the target's work is not cancelled.
- `ActorHandle#call` applies no local read timeout: the broker always answers, and a partial frame read abandoned on timeout would desynchronize the stream.

### Lifecycle design (implemented)

- `ActorBroker::Node` holds `id, name, path, generation (always 1 until restart exists), parent_id, children (all child ids, including terminal ones), state, reference`. All node state is guarded by the broker mutex. Nodes are never removed, so a stale handle answers with a typed error and can never resolve to a recycled process.
- States: `:starting`, `:running`, `:stopping`, `:stopped`, `:failed`. A node is registered as `:starting` immediately after the process is launched and before its boot completes, so the actor can issue broker requests (including `context.spawn`) from `initialize`. `check_placement` is run before launch (cheap rejection) and again under the mutex at registration, so a parent stopped during the child's startup makes the child fail with `ActorStoppedError` and the child process is force-stopped.
- Local handles no longer hold a `Reference`; every `ask`/`stop`/`alive?`/`state`/`path`/`parent`/`children` goes through the broker (`ActorHandle.new(id, broker:)`), which checks node state first. `ActorHandle#==` compares ids.
- Stop order: descendants first, deepest first (post-order), then the node; one deadline shared across the subtree (`stop_subtrees`), falling back to `force: true` once the deadline passes. `broker.stop` applies the same to every root.
- Failure: `Reference#on_exit` fires on the reaper thread. `actor_exited` marks the node `:stopped` if it was `:stopping`, else `:failed`, marks live descendants `:stopping`, and enqueues their force-stop on the broker service thread (the former timer thread now runs expiries and lifecycle tasks). The reaper thread never waits on another actor.
- Names: unique among live siblings; may be reused after the sibling stops or fails. `children(handle)` returns only live children.
- `ActorFailedError < ActorStoppedError` distinguishes a crash from an orderly stop for both local callers and brokered callers (`remote_class`).

### Child spawning design (implemented)

- Worker side: `RocotoActor.context` (`ActorContext`, `lib/rocoto_actor/context.rb`) is created by the runner from the boot message's `context: { actor_id: }`, which `ActorBroker` passes through a new `RocotoActor.spawn(context:)` keyword. `context.spawn(Class|"Name", *args, name:, source:, start_timeout:, mailbox_size:, mailbox_bytes:)` resolves the source path in the worker and sends `{ op: :broker_spawn, actor_class:, source:, arguments:, name:, options: }`. `context.handle` is the actor's own handle.
- The process primitive is `RocotoActor::Launcher.spawn` (`lib/rocoto_actor/launcher.rb`), a `private_constant` called only by `ActorBroker#spawn_node` for both `broker.spawn` and `context.spawn`. There is no public `RocotoActor.spawn`; `ActorBroker` is the only way to create actors. The launcher accepts a class name string plus `source:`, so the broker never needs the child class loaded. `test/rocoto_actor_test.rb` reaches it through `RocotoActor.const_get(:Launcher)` to test `Reference` directly.
- `BrokerClient` (`lib/rocoto_actor/broker_client.rb`) is one per socket in the worker; all handles and the context share its mutex and request counter, which closes the request-ID collision item. It holds the deferred non-response frames that `run_actor` drains. Requests are valid from the first frame because the parent's reader thread services the socket from the moment the process is launched.
- Broker side: `Reference#read_replies` forwards every op in `ActorBroker::REQUEST_OPS` to `ActorBroker#dispatch`, which takes the per-source response slot then routes `:broker_request` inline and queues `:broker_spawn`/`:broker_stop` on a lifecycle pool (`max_lifecycle_workers`, lazily started; `max_pending_lifecycle_requests` queue bound rejecting with `BrokerBusyError`). Spawn blocks up to `start_timeout`, so it must not run on a reader thread or the service thread.
- The source node is found through `@node_ids_by_reference`; an unregistered source gets `Error("unknown source actor")`. `spawn_child` validates types and restricts `options` to `SPAWN_OPTIONS` with numeric values; `source` must be absolute.
- Worker-side `handle.stop(timeout:, force:)` sends `:broker_stop`; the broker only allows stopping descendants of the requester (`descendant?`), else `Error("... is not a descendant of ...")`.
- Handles decoded in the application bind to the owning broker (`Thread.current[:rocoto_actor_broker]`, set on the reference reader thread by `attach_broker`), so a handle returned from an actor's reply is a fully local handle. Decoding in a worker (`RocotoActor.worker_process?`) binds to the socket as before.
- `ActorBroker#stop` fails any queued lifecycle requests with `ActorStoppedError` and joins the lifecycle workers; a spawn in progress completes and is rejected at registration because the broker is stopped.

### Boot protocol (implemented)

- Boot is an ordinary correlated request. `Launcher.launch` forks, builds the `Reference` immediately, and calls `Reference#boot(arguments, context)`, which enqueues `{ op: :boot, id:, arguments:, context: }` (bypassing the mailbox bound) and returns `[reference, boot_future]`. The runner replies `{ id:, ok: true }` after `initialize` returns, or an `error_response` with the boot id on failure; a watchdog failure before the boot message is read sends a bare `op: :boot_error` frame, which the reader resolves against `@boot_id`. `Launcher.spawn` (launch + wait) remains for the low-level `Reference` tests only.
- `ActorBroker#launch_node` registers the node as `:starting` (with `booting = true`), attaches the broker, and returns `[node, boot]`. Exactly one caller settles it via `settle_boot(node, error)`: success moves `:starting` to `:running`; failure kills the process (`stop(force: true, timeout: 0)`), `unregister`s the node (removed from `@nodes` and the parent's children, since no handle can exist for it), force-stops any children it spawned while booting on the service thread, and maps the error with `Launcher.startup_error` (`AskTimeoutError` becomes `TransportTimeoutError` "startup timed out"; `ActorStoppedError` becomes `Error` "closed during startup"; `RemoteError` passes through with the constructor's original class).
- `broker.spawn(start_timeout:)` waits synchronously on the boot future. `spawn_child` (lifecycle pool) does not wait: it schedules an expiry for `start_timeout` on the service thread and responds from `boot.on_resolve`, so a chain of actors spawning in their constructors needs no thread per level (`test_nested_initialization_spawning_is_not_limited_by_the_lifecycle_pool`).
- `:starting` sources may issue broker requests and may be spawn parents (`Node#active?`); `:starting` targets accept routed messages, which queue until the actor loop starts. An actor that passes `context.handle` to a child during its own `initialize` and has that child call back synchronously deadlocks until a timeout, as documented for synchronous calls generally.
- `ActorBroker#roots` returns handles of live top-level actors.

### Tell design (implemented)

- Envelope: every delivered message is `{ op: :ask | :tell, message:, sender: }` (`id:` only for asks). `sender` is an `ActorHandle` encoded by id, or nil from the application; the runner sets `RocotoActor.context.sender` around `receive` (`Runner.deliver`) for both ops.
- Application side: `handle.tell` → `ActorBroker#tell` → `Reference#tell`, which enqueues without a pending future (`enqueue(reply: false)`) and raises `MailboxFullError`/`ActorStoppedError` if not enqueued. Ordering with asks from the same sender is preserved by the single outbox.
- Worker side: `handle.tell` sends `:broker_tell`; `ActorBroker#relay_tell` runs inline on the source's reader thread (no route slot; it never waits on the target), enqueues with `sender: sender_handle(source)`, and acks with `ok: true, result: nil` or a typed error. Routed asks now also carry the sender.
- Failure: `Runner.run_actor` rescues an exception from a told `receive`, writes `{ op: :actor_error, error_class:, message:, backtrace: }`, and exits; `Reference` stores it as `exit_error`, `ActorBroker#last_failure` returns `node.failure || reference.exit_error`, and `relaunch` copies the previous reference's `exit_error` into `node.failure` before swapping. The exit then follows the normal failure path (`actor_failed`, restart policy).
- Idempotency keys were deliberately left as an application pattern (dedup must live in the receiver, atomically with the effect); no envelope slot was added. Worker-side async `ask` was rejected: a future waited on inside `receive` either blocks or delivers results outside the message flow. `tell` plus reply-as-message is the async model.

### Events, shutdown, deadlock detection, describe (implemented, 2026-09-23)

- Chosen after comparing with Erlang/OTP and Akka: these four are what a bulkheading library with a handful of actors still lacked. Left out on purpose: supervision strategies beyond one-for-one (composable from watch + tell + stop), registry lookup by path (handles are capabilities), routers, distribution, hot code loading, become/stash.
- Events: `emit(node, event)` runs under the broker mutex at every transition (`retire` → `:stopped`/`:failed`, `actor_failed` → `:restarting`, `settle_restart` → `:restarted`) and only queues a service task (`push_task_locked`); `deliver_event` runs on the service thread outside every lock, calls `on_event` under `guarded`, and tells each watcher `{ op: :actor_event, event:, actor:, reason:, generation: }` with no sender. Watches (`@watchers`: watched id → watcher ids) end when the watched node is retired (after its final event) and when the watcher's incarnation ends (`purge_watches` in `retire`, `actor_failed`, and `relaunch`'s install). Watching a terminal actor delivers its state as an event immediately.
- Shutdown: `Runner.shutdown_actor` calls the actor's `shutdown` on the `:stop` op if defined; an exception becomes an `:actor_error` frame (`last_failure`) and the stop still completes; a hang is bounded by the caller's stop deadline and KILL.
- Deadlock detection: `@waiting` maps a node to the node it is blocked on; `acquire_route` records it and walks the chain from the target (`wait_cycle`), refusing with `DeadlockError` naming the path; `release_route_slot` clears it. `spawn_child` records the parent as waiting on the child until the boot settles, so a child calling its parent from `initialize` is refused instead of waiting out both timeouts. One outstanding call per actor is assumed.
- `describe`: plain-data snapshot under the mutex (`describe_node`).
- Test support: `test/support/watch_actor.rb` (`WatcherActor`, `ShutdownActor`, `SelfCallActor`, `CallParentInInitActor`/`BootCyclerActor`, `CallerActor`).

### Timer design (implemented, 2026-09-23)

- Scope decided with the user: only an actor schedules messages, and only to itself; the application does its own scheduling. No retry of an undeliverable tick. `Timer#cancel` is the whole timer API.
- `ActorContext#schedule` sends `:broker_schedule { message, after, every }`; the broker (`schedule_timer`, inline on the reader thread) validates (`after` ≥ 0, `every` ≥ `MIN_TIMER_INTERVAL` 0.01 s, at most `MAX_TIMERS_PER_ACTOR` 100 per actor), records a `TimerRecord { id, node_id, generation, message, every }`, arms it with `enqueue_task(delay:)`, and answers with a `Timer` (encoded `["timer", id]`; decoded socket-bound in a worker, unbound in the application where `cancel` raises).
- `fire_timer` on the service thread re-checks the record and that the node is the same generation and active, tells the message with the actor's own handle as sender, reports a failing tell to `error_handler` ("scheduled tell to <id>"), and re-arms a recurring timer with fixed delay. A timer whose incarnation is gone is dropped at the check; `purge_timers` also runs in `retire` and in `actor_failed`, so nothing fires into a restarting or terminal node.
- `:broker_cancel` succeeds only for the requesting actor's own timer. A cancelled recurring timer's already-armed task fires into a missing record and is a no-op; tasks are therefore bounded by the number of timers ever armed plus one per recurrence, never accumulating.
- Test support: `test/support/ticker_actor.rb`. Tests cover one-shot with sender, recurring until cancel, cancel before fire, timers dying with the incarnation while `initialize` reschedules, undeliverable ticks reported with recurrence continuing, validation and bounds, and a `Timer` returned to the application refusing `cancel`.

### Restart design (implemented)

- Terminology: the small per-actor process started by `runner.rb` is the watchdog (`Runner.watch`; it was called the supervisor before 2026-09-22): a process-group owner with no policy. The supervisor in the actor-system sense is the broker. An external kill of the worker is noticed by the watchdog, which kills the group and exits; the broker's reaper then runs the same failure path as a crash. Deaths without an exception (signals, `exit!`, OOM) leave `last_failure` nil but are reported through `last_exit`: the watchdog keeps its socket copy, and after `waitpid` returns for the worker it writes `{ op: :actor_exit, exitstatus:, termsig: }` before killing the group and exiting (`Runner.report_exit`); `Reference#exit_status` holds it as `RocotoActor::ExitStatus`, `ActorBroker#last_exit` returns `node.exit || reference.exit_status`, and `relaunch` carries it across generations like `last_failure`. The watchdog holding the socket delays the parent's EOF by at most one `PARENT_CHECK_INTERVAL`. Ruby converts a fatal signal into `SignalException`; `Runner.run_actor` rescues it and `die_by_signal` resets the disposition with `Signal.trap(signo, "SYSTEM_DEFAULT")` (`"DEFAULT"` would restore Ruby's own handler, which raises again) and re-delivers the signal so the status shows `termsig` instead of exit 0. An unhandled exception in a told message exits with status 1 after the `:actor_error` frame. Nothing in the restart policy depends on the exit reason; it is diagnostics only.

- Policy per node (`Node#policy`): `restart` (`:never` default, `:on_failure`), `max_restarts` (3), `restart_window` (60 s), `restart_backoff` (0.1 s, doubling per consecutive restart). `Node#spec` keeps the class, arguments, launch options, and `start_timeout` needed to relaunch. Validated by `validate_policy` for both `broker.spawn` and `context.spawn` (`POLICY_OPTIONS` are accepted in `SPAWN_OPTIONS`).
- Failure decision is centralized in `actor_failed(node)`: marks live descendants `:stopping` and force-stops them on the service thread, then `restart_delay` (under the mutex) prunes restart timestamps outside the window, records the attempt, and returns the backoff or nil. With a delay the node becomes `:restarting` and a delayed service task (`enqueue_task(delay:)`) hands `relaunch(node)` to the lifecycle pool via `enqueue_lifecycle_job` (internal jobs bypass the request bound and are bounded by the number of nodes). Without one the node becomes `:failed`.
- `relaunch` launches under the same id (`context: { actor_id: }`), then under the mutex re-checks `:restarting` before installing the new reference and bumping `generation`; if the node was stopped meanwhile the new process is killed and discarded. `stop_subtrees` snapshots each node's reference at marking time for the same reason.
- Exit callbacks are bound to the reference they came from (`actor_exited(node, reference)`) so a late reaper from a previous generation cannot fail the current one; exits during any boot (`node.booting`) are owned by the boot settler (`settle_boot` for first boots, `settle_restart` for relaunches, whose failure feeds back into `actor_failed` and counts against the limit).
- Requests to a `:restarting` node fail fast with `ActorRestartingError` (`node_error`, hence also brokered routes). "Wait" and "queue" policies from the plan were not implemented; fail-fast is the only behavior. No request is ever replayed: `Reference` rejects queued and in-flight futures with `ActorStoppedError` when the process exits.
- `Reference#alive?` is false for a `:restarting` node between generations.

## Production-readiness review (2026-09-22)

A `/code-review high lib/` pass produced ten findings. Fixed, each with a regression test:

1. A relaunch whose `Launcher.launch` raised left the node `:restarting` forever; it now goes through `actor_failed` like any other failure.
2. `last_failure`/`last_exit` preferred the previous generation's values; they now prefer the current reference's, falling back to the carried-over ones.
3. `run_actor` only rescued `StandardError`, so `NotImplementedError`/`LoadError` in `receive` (and any protocol error) killed the actor with exit 0 and no report. Per-message rescues cover `ScriptError`; the method-level path reports anything else with `:actor_error` and exits 1; `SystemExit` exits with its status.
4. `Future#value`'s timeout branch did not broadcast, leaving other waiters on the same future asleep forever.
5. The reaper closed the socket under the reader mid-frame, losing the watchdog's `:actor_exit` frame. `actor_exited` now rejects pending futures, kills the group (which produces EOF for the reader), joins the reader for up to a second, then closes.
6. A process that died after replying ready but before `settle_boot` cleared `booting` was dropped (state `:running`, never restarted). `actor_exited` records `node.boot_exit` during a boot and the settlers call `actor_failed` when they see it.
9. `unregister` left a failed boot's children with a dangling `parent_id`: `descendant?` and `parent` now tolerate it, and `broker.stop` sweeps every node rather than only roots so such children are stopped.
10. `mailbox_size`/`mailbox_bytes` were validated only after the process was spawned; the launcher validates first.

Documented, not fixed:

7. While an actor is blocked in `BrokerClient#request` it reads and defers every incoming `:ask`/`:tell` without bound; the application-side mailbox limits bound only the unsent outbox, so a caller that floods an actor during its long `call` grows that actor's memory. Acceptable at the intended scale; a credit-based flow control or a bound that fails the call would be the fix if needed.
8. Terminal nodes are retained forever and root spawns scan all nodes. Node metadata retention is accepted per the scale decision above; the soak showed each terminal node also pinned its `Reference` (thread stacks, closed socket) and `spec` (constructor arguments), so `retire(node, state)` now copies `exit_error`/`exit_status` onto the node and releases the reference and spec at every terminal transition. `ask`/`tell` and the boot settlers read the reference under the mutex because a concurrent `retire` can null it.

A `/code-review ultra` pass (2026-09-22, standard diff review; the launch note asking for lock-ordering and settlement-race focus was recorded but not delivered to the reviewers) produced four findings, all fixed with tests:

1. Exponential backoff defeated the restart cap: timestamps older than `restart_window` were pruned, and once a doubling delay exceeded the window the count could never reach `max_restarts`, so a persistently crashing actor restarted forever (defaults were safe; `restart_backoff: 1, max_restarts: 10` was not). `restart_delay` now counts consecutive failures and resets only when the previous incarnation ran for `restart_window` seconds (`node.started_at`, set when an incarnation becomes `:running`); time spent in backoff never counts. README wording updated.
2. Internal relaunch jobs shared `@lifecycle_queue` and counted against `max_pending_lifecycle_requests`; the bound now counts only client requests.
3. `report_failure`/`report_boot_error` dropped the report when the error itself could not be serialized (invalid UTF-8 in a message); `write_report` falls back to reporting the `SerializationError`, matching the ask path.
4. A local named `roots` in `ActorBroker#stop` held every node; renamed `nodes`.

A second `/code-review ultra` (whole repository, base tag `review-base` on an empty-tree commit, with `REVIEW.md` in the diff as the reviewers' brief) produced three findings, all fixed with tests:

5. `run_service` ran expiries and tasks unguarded, and `wake_service` never replaced a dead thread, so one raising callback silently ended route timeouts and restart scheduling for the broker's lifetime. Each unit of work is now `guarded`; a `StandardError` is reported and the loop continues, and `wake_service` starts a replacement whenever the thread is not alive.
6. A non-`StandardError` (e.g. `NoMemoryError`) escaping a lifecycle job killed the worker, left it counted in `@lifecycle_workers`, and never answered the requesting actor. The worker now reports any exception, answers actor requests with an error, re-raises only non-`StandardError`s, and removes itself from the pool in `ensure` so the next request starts a replacement.
7. `stop_child` compared `force == true` while the in-process path used truthiness; unified on truthiness.

These introduced the first observability hook: `ActorBroker.new(error_handler:)`, a callable `(error, context)` defaulting to a one-line `warn`; `report_error` never lets the handler's own exception propagate.

Tagging note: a bare commit hash after `ultra` is read as a focus note, and a base with no shared history triggers a whole-repository cost confirmation that only an interactive terminal session can answer; the extension's command path cannot. `review-since-initial` (tag on `22a97a2`) is the fallback base that needs no confirmation. The note passed on the command line is never delivered to the cloud reviewers. Documented channels for review guidance are the repository's `CLAUDE.md` (project instructions) and `REVIEW.md` (review-only instructions), both read by the local `/code-review`; whether the cloud sandbox reads them is undocumented but likely, since its agents are Claude Code sessions in a clone of the repository. On 2026-09-23 the review brief moved from `docs/review-focus.md` to `REVIEW.md` and a `CLAUDE.md` was added; the next ultra run will show whether findings cite the brief's invariant numbers.

A third `/code-review ultra` (2026-09-23, whole repository, after `REVIEW.md` and `CLAUDE.md` existed) returned four findings, all fixed with tests: `Future#run_callbacks` let an application `on_resolve` block that raised abort `Reference#actor_exited` before the broker's exit hook (per-callback rescue reporting to `Future.callback_error_handler`); `on_event` ran on the service thread, so a slow handler stalled expiries and timers (events now go through a dedicated ordered event thread, joined on stop); watching a terminal actor replayed the event to `on_event` (`notify_application: false` on the catch-up path); the write-with-timeout branch in `Transport.write_payload` was dead since boot became a `Reference` request (removed). None of the findings cited `REVIEW.md` invariant numbers, though all four fell inside the areas it names.

Whether the whole-repository reviewers used `REVIEW.md` is unclear: none of the findings cite its invariants by number, though finding 5 is invariant 9 in substance.

Targeted concurrency read (2026-09-22, by the author, invariant by invariant against `REVIEW.md`; not independent, recorded so the reasoning can be checked):

- Lock ordering holds: every broker call into a `Reference` method that takes `@pending_mutex` (`ask`, `tell`, `stop`, `send_broker_response`, `attach_broker`) happens after the broker mutex is released, with the reference captured under the mutex; `Reference` never calls into the broker while holding `@pending_mutex` (`read_replies` reads `@broker` under the lock and dispatches outside it); every callback (`on_resolve`, `on_exit`, `on_done`) is invoked outside `@pending_mutex` and outside `Future`'s mutex.
- Invariant 1 (one response, one release per request): each `dispatch` path ends in exactly one `respond`/`respond_error`; `release_once` guards the slot; a source that stops discards its control outbox exactly once (array swap under lock) and the writer's `ensure` covers the in-flight write.
- Invariants 3–5 (boot settlement, stale exits, stop vs relaunch): both orderings of "reader rejects the boot future" and "reaper runs `actor_exited`" converge (`boot_exit` recorded during boot; `actor_failed` after settle); `relaunch` installs only under `:restarting` and kills the new process otherwise; `stop_subtrees` snapshots the reference at marking time; a `:restarting` node in backoff is stopped through its dead reference and the delayed relaunch finds it terminal.
- Invariant 9 (anything an actor sends): a non-hash frame raises `TypeError` in `read_replies` and is now a protocol violation; a request without an integer `request_id` gets no response (the misbehaving actor's `call` blocks until it is stopped); floods are bounded by the per-source response slots, which block only that actor's reader.
- Three changes came out of the read: `Reference#actor_exited` ran the broker's exit callbacks after `@socket.close` inside a `rescue IOError`, so a close error would have silenced the exit (callbacks now run unconditionally); failure-path subtree stops ran on the service thread and could delay route expiries by the kill grace per child (`stop_later` puts them on the lifecycle pool); `broker.stop` from a broker thread (an `error_handler`) joined its own thread (`ThreadError`; now skipped). Tests: `test_broker_stop_from_its_own_error_handler_does_not_deadlock`, `test_child_cleanup_after_a_failure_does_not_delay_route_expiries`.
- Remaining review debt: this read is the author's. If an independent pass is wanted, `REVIEW.md` is the brief.

Public surface (2026-09-22): `Reference`, `Transport`, `BrokerClient`, and `Runner` joined `Launcher` as `private_constant`s, so the API is exactly `ActorBroker`, `ActorHandle`, `ActorContext`, `Future`, `ExitStatus`, the errors, and the `RocotoActor` module functions (`context`, `worker_process?`, `broker_client`). Tests that exercise internals directly reach them with `RocotoActor.const_get(:Name)` (`test/transport_test.rb`, `test/rocoto_actor_test.rb`, `test/soak/soak.rb`, and one support actor). `Runner.run` is invoked from inside the module namespace at the end of `runner.rb` because the constant is private. `test_internals_are_not_public` asserts the split using `Module#constants`, which omits private constants.

Lint and CI (2026-09-22): RuboCop 1.91 with `.rubocop.yml` (target 3.2, new cops enabled, line length 120, `Metrics` disabled on purpose, rescued exceptions named `error`, three success-reporting commands allow-listed from `Naming/PredicateMethod`); `bundle exec rake` runs rubocop then the suite. The first `rubocop -A` pass silently broke every actor-exit path: the unsafe `Style/HashEachMethods` rewrote `discarded.each { |_payload, on_done| ... }` to `each_value` on what is an Array of pairs. Prefer `rubocop -a` (safe only) and run the suite after any auto-correct. `.github/workflows/ci.yml` runs lint and the suite on Ruby 3.2–3.4 on Ubuntu plus 3.4 on macOS, and a manual `workflow_dispatch` soak job taking `soak_seconds`. First CI run: Ruby 3.3, 3.4, and macOS 3.4 passed; Ruby 3.2 failed at `bundle install` because the lockfile (resolved on 3.4) had `parallel 2.2.0`, a RuboCop dependency whose 2.x line requires Ruby >= 3.3. `Gemfile` pins `parallel "~> 1.28"` (supports 2.7+; RuboCop accepts `>= 1.10`) until the Ruby floor is raised to 3.3, at which point the pin should be removed. General rule: the committed lockfile must resolve on the oldest supported Ruby, so relock with the floor version or keep dev dependencies pinned to versions that support it. A later macOS run exposed two test races: asserting a descendant's `:stopped` state after waiting on a *different* node, while retirement proceeds deepest-first (`stop_subtrees`) or on the lifecycle pool concurrently with a relaunch. Rule for tests: wait on the exact node whose state you assert; never infer one node's terminal state from another's.

Linux fault matrix (2026-09-22): `test/validation/fault_matrix.rb` (with `support.rb` actors that misbehave on purpose and `app_child.rb`, a throwaway application) executes the cases from `docs/linux-validation.md` that the suite does not cover, each under a hard time limit, printing PASS/FAIL/NOTE per probe; results are recorded in that document. Writing and running it found:

- A real bug from the review-fix round: `Reference#actor_exited`'s `@reader.join(1)` re-raised whatever ended the reader into the reaper thread, so a malformed reply (a frame without `:id` raises `KeyError` in `read_replies`) killed the reaper before it closed the socket and ran the broker's exit callback; the node stayed `:running` forever. Fixed: `join_reader` swallows the reader's exception, and `read_replies` treats any `StandardError` from a frame as a protocol violation (records a `RemoteError` "malformed reply" as `exit_error`, rejects pending futures, force-stops). Regression: `test_malformed_replies_stop_the_actor_and_reject_the_request`.
- `RemoteError.new` raised `TypeError` from `set_backtrace` when an error reply carried non-string fields, after the future had already been removed from `@pending`, so nothing could ever resolve it. `RemoteError` now coerces its fields to strings. Regression: `test_error_reply_with_wrong_field_types_is_still_a_remote_error`.
- Ruby itself wedges under `RLIMIT_NPROC`: when thread creation fails at the limit the VM blocks in a futex and ignores `TERM` (only `KILL` removes it). This is not recoverable by the library; deployments must keep process limits above need (cgroup `pids.max` with headroom). `RLIMIT_NPROC` counts the uid's processes on the whole host, so a container cannot compute a meaningful limit; the probe finds one empirically and reports the wedge as a NOTE.
- `Reference#stop` returned `false` whenever the deadline forced a `KILL`, even when the kill landed milliseconds later, so `false` could not distinguish "killed late" from "still alive" (the D-state signal). `KILL_CONFIRMATION_GRACE` (0.5 s) after a forced kill makes `false` mean the group survived the `KILL`. README wording updated; regression `test_stop_confirms_a_kill_that_lands_after_the_deadline`. A subtree stop may exceed its deadline by up to the grace per actor that needed killing.
- Matrix result on the recorded run: 30 probes, 29 pass, 1 note (process-group escape, documented). Full table and reasoning for the two cases not reproduced (true D state, PID churn) are in `docs/linux-validation.md`, which is now a record rather than a plan.
- Harness lessons: `Method#curry[arg]` on a one-argument method calls it immediately; `ulimit` is a shell builtin; pipe `rake test` output through `head` and the chain sees `head`'s exit status; wrap every subprocess that can wedge in `timeout -s KILL`.

Soak harness: `test/soak/soak.rb` (`SOAK_SECONDS=1800 bundle exec ruby -Ilib test/soak/soak.rb`) runs continuous ask/tell traffic with injected crashes, external kills, told-message exceptions, stops, and respawns; samples threads/fds/children/zombies/RSS/live heap slots and live `Reference`/`Future`/`ActorHandle` counts after `GC.start`; and fails on upward trends, a zombie persisting across consecutive samples, children surviving `broker.stop`, `broker.stop` returning false, or unexpected error classes. It logs any `Reference#stop` that returns false with its pid and elapsed time. It is not part of `rake test`.

Soak results (2026-09-22, this container): a 10-minute run (~900k operations, ~250 injected failures) showed threads and file descriptors flat during the run and back to baseline after `broker.stop`, no surviving children, and only expected error classes. Findings: a single-sample zombie (the normal exit-to-`waitpid` window; the check now requires persistence), `Reference`/`Future` counts growing with terminal nodes (fixed by `retire`), and one `broker.stop` returning false while every process was gone a second later, which did not reproduce in later runs; `StopDiagnostics` in the harness will name the reference if it recurs. Long runs (2026-09-23, CI on ubuntu-latest): a 1800 s soak passed, and a 7200 s soak passed with ~19 million operations, ~4,500 injected failures (749 of each of the six kinds), live `Reference` count flat at 17, RSS 55 MB at the end (the same plateau seen at 10 minutes, so no slow growth), no zombies, `broker.stop => true in 0.096 s`, and no `Reference#stop` returning false in two hours, which retires the unreproduced `stop=false` observation from the first run. The CI step itself timed out because its `timeout-minutes` equalled the soak length; it is now 350 minutes, so `soak_seconds` must stay under about 20000.

After `retire`, a 3-minute run passed every check: live `Reference` count flat at 15–17 and zero after `broker.stop`, `Future` count flat, `heap_live_slots` flat, and RSS climbing only during the first minute (24→36 MB) before plateauing at ~37 MB, so the earlier steady RSS growth was terminal-node retention plus heap warm-up rather than a leak. A multi-hour run before production is still worthwhile for the `broker.stop` false return that did not reproduce.

## Immediate implementation plan

### 1. Stabilize the protocol (remaining items)

- Define protocol constants or helpers for `broker_request` and `broker_response`.
- Done: timeout field, flattened error provenance, typed errors for unknown handle and invalid timeout, request IDs unique per source socket (`BrokerClient`).

### 2. Make routing bounded (done)

See "Routing design (implemented)" above. Still open: a byte budget for control responses (they currently bypass the mailbox byte limit but are bounded in count by `max_routes_per_actor`).

### 3. Add logical lifecycle metadata (done)

See "Lifecycle design (implemented)" above. The original requirements are kept below for reference. Tracked fields:

```ruby
id
path
generation
parent_id
children
state # starting, running, stopping, stopped, failed, restarting
reference
```

Required invariants:

- The broker owns every reference.
- A logical child cannot outlive its parent unless explicit re-parenting is added.
- Stopping a parent recursively stops its descendants.
- Stopping a child revokes its handle or transitions it to a typed stopped state.
- Actor failure transitions the logical node and applies the configured subtree policy.
- Unknown/stale handles never reach an unrelated recycled process.

### 4. Add broker-owned child spawning (done)

See "Child spawning design (implemented)" above. Original sketch:

```ruby
{
  op: :broker_spawn,
  request_id: integer,
  parent_id: source_actor_id,
  actor_class: class_name,
  source: source_path,
  arguments: supported_values,
  options: constrained_spawn_options
}
```

The broker should create the child, record parent/child metadata, and return an opaque handle. Child spawn must be bounded and must not block unrelated broker traffic.

Pass an `ActorContext` or system capability to workers if desired, but keep it as an opaque broker-facing value. Do not expose the parent socket or a direct `Reference`.

### 5. Define shutdown and restart policies (done)

See "Restart design (implemented)" above. The original requirements are kept for reference:

- queued requests on actor failure: fail, do not replay by default
- in-flight request on actor failure: fail as ambiguous
- pending broker calls from a stopped source: fail and discard late response
- parent stop: stop descendants recursively, then parent or vice versa; choose and test one order
- broker stop: reject new spawns/routes and wait for all process groups
- restart: stable logical ID, increment generation, replace reference/socket internally
- new requests during restart: wait, fail with `ActorRestartingError`, or queue according to policy
- restart loops: backoff and maximum attempts

### 6. Prefer async handles

Add a worker-safe asynchronous API, likely `ActorHandle#ask`, that returns a broker future without blocking inside `receive`. This requires the actor loop to support pending broker calls or a continuation/event mechanism. Do not rely on synchronous `call` for database services in general.

## Tests to add

Normal suite:

- [x] handle encoding contains only an opaque ID and never socket/thread/mutex state
- [x] worker routes a successful request to a shared target
- [x] target remote error has defined nested/flattened semantics
- [x] unknown handle returns a typed error
- [x] stopped target returns a typed error
- [x] source actor stopping with a pending broker call does not leak a route (`test_stopped_source_releases_its_route`)
- [x] target actor stopping with a pending broker call resolves the caller promptly with `ActorStoppedError`
- [x] concurrent requests from multiple actors preserve target actor serialization (5 sources × 8 calls, unique sequence numbers)
- [x] broker stop is idempotent, rejects new actors, and stops every subtree
- [x] new routes are rejected after broker stop
- [x] logical hierarchy: path/parent/children, name validation and sibling uniqueness
- [x] stopping a parent stops descendants; stopping a child leaves the parent running
- [x] failed parent marks `:failed` and stops descendants; failed target reports `ActorFailedError` to brokered callers
- [x] subtree stop shares one deadline
- [x] actor spawns a child through its context; child reaches its parent through `context.handle`
- [x] stopping the parent stops children spawned by the actor; actor stops its own child; non-descendant stop refused
- [x] child boot failure reported to the actor; spawn during initialization rejected
- [x] malformed spawn requests and unknown sources fail closed; lifecycle requests are bounded
- [x] children spawned from `initialize`; nested constructor spawning with a pool of one; failed constructor unregisters the actor and its children, reported to both the application and a spawning actor
- [x] tell: application tell ordered with asks and without sender; actor fan-out with tell and reply via `context.sender`; routed ask carries sender; tell then call from one actor arrive in order; tell to stopped target rejected for application and actors; exception in told message fails the actor and is recorded in `last_failure`; exception in told message triggers restart policy
- [x] `ask` inside an actor raises with guidance
- [x] actor killed from outside (SIGKILL to the worker) is restarted under `:on_failure` and fails under the default policy; the per-actor watchdog process holds no policy
- [x] internals are private constants and the public constants are enumerable
- [x] backoff longer than the window cannot defeat `max_restarts`; healthy uptime resets the count; relaunch jobs do not consume the lifecycle request budget; unserializable crash messages are still reported
- [x] malformed replies (missing id, unknown tag) stop the actor with a recorded reason; wrong-typed error fields still yield a `RemoteError`; `stop` confirms a kill that lands after the deadline
- [x] exit reasons: `last_exit` reports termsig for SIGKILL and SIGTERM, exitstatus for `exit!` and told-message exceptions (with `last_failure`), nil while running and after stop, retained across restart
- [x] restart: same handle and path with new generation; restarted actor recreates children; requests during restart fail fast locally and via broker; restart limit leaves `:failed`; stop during backoff cancels relaunch; policy from `context.spawn`; policy validation; default policy does not restart
- [x] malformed broker requests fail closed (invalid timeout)
- [x] broker request limits remain bounded (`max_routes`, `max_routes_per_actor`, no thread per route)
- [x] synchronous call deadlock behavior is documented (README); asks arriving during a call are not dropped

Optional Linux/constrained job:

- run the normal suite under `ulimit -n`, cgroup memory, process, and PID limits
- separately test spawn failures from unavailable descriptors/process slots
- keep hard outer timeouts and cleanup traps

Do not make genuine Linux `D`-state fixtures part of normal CI. They require disposable, representative storage/network failure environments and are unsuitable for a portable test suite.

## Public API direction

Possible target API:

```ruby
system = RocotoActor::ActorBroker.new

database = system.spawn(DatabaseActor, "app.db")
worker = system.spawn(WorkerActor, database)

future = worker.ask(:write)
result = future.value(timeout: 5)

system.stop
```

Future child API:

```ruby
# Conceptual worker-side API; not implemented yet.
child = context.spawn(ChildActor, configuration)
child.ask(message)
```

Avoid calling this full component `ActorSystem` until it has general lifecycle, dispatch, registry, and supervision behavior. `ActorBroker` accurately describes the current focused role.

## Completion criteria for this feature

- Full existing suite passes.
- Broker tests pass without unbounded threads or blocked shutdown.
- Every accepted broker request reaches exactly one terminal caller-visible outcome.
- Parent and target actor failures are distinguishable from successful results.
- Logical parent/child lifecycle invariants are tested.
- Broker handles remain stable across any future restart implementation.
- Documentation states at-most-once behavior and retry/idempotency requirements.
- No socket descriptors cross actor boundaries.
