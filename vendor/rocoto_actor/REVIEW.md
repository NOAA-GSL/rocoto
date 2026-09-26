# Review instructions

Read this before reviewing any change in this repository. It is the review
brief that `docs/actor-broker-handoff.md` refers to as the review focus, and
it is read by Claude Code reviews as review-only instructions.

## What matters most

Correctness findings in the concurrency core (`lib/rocoto_actor/broker.rb`,
`lib/rocoto_actor/reference.rb`) outrank everything else. Style is enforced by
RuboCop and is out of scope. Please cite the invariant number below when a
finding shows one can be violated.

Status: the invariants were traced by the author on 2026-09-22 (see "Targeted
concurrency read" in `docs/actor-broker-handoff.md`); an independent trace is
still wanted.

This section directs the concurrency review. It describes the invariants the
concurrency core is supposed to uphold and the specific interleavings that have
not yet had an independent review. Findings that show one of these invariants
can be violated are the most valuable output.

## Threads that touch shared state

- Per actor (`lib/rocoto_actor/reference.rb`): a reader thread (`read_replies`),
  a writer thread (`write_requests`), and a reaper thread (`start_reaper`,
  `actor_exited`). All share `@pending_mutex`.
- Per broker (`lib/rocoto_actor/broker.rb`): a service thread (`run_service`:
  route expirations and delayed tasks) and up to `max_lifecycle_workers` lifecycle
  threads (`run_lifecycle_worker`: actor-initiated spawn/stop and relaunches).
  All share the broker `@mutex`.
- Application threads call `spawn`, `ask`, `tell`, `stop`, `stop_actor`, and the
  query methods on `ActorBroker`.
- Callbacks: `Future#on_resolve` blocks run on whichever thread resolves the
  future (a reader thread, the service thread on expiration, or an application
  thread on `value(timeout:)`). `Reference#on_exit` blocks run on the reaper
  thread. `Reference#send_broker_response(on_done:)` callbacks run on the
  writer thread, or on whichever thread discards the response.

## Lock ordering

Intended rule: the broker `@mutex` is never held while calling into a
`Reference` method that takes `@pending_mutex`, and no `Reference` method calls
back into the broker while holding `@pending_mutex`. Please look for
violations, in particular through callbacks: `on_resolve`, `on_exit`, `on_done`,
`attach_broker`, and `broker.dispatch` (called from the reader thread in
`read_replies`).

## Invariants to check

1. Every accepted broker request (`dispatch`) reaches exactly one caller-visible
  outcome: one `send_broker_response`, and its `release_response` callback runs
  exactly once, on every path including errors, expiration, source death, and
   broker stop.
2. `@routes` and `@responses_by_source` return to zero; no path leaks a slot.
3. A node's boot future is settled exactly once, by exactly one of
  `spawn` (synchronous path), `spawn_child` (`on_resolve`), or the expiration, and
   `settle_boot`/`settle_restart` are idempotent under `node.booting`.
4. `actor_exited(node, reference)` ignores exits from a reference that is no
   longer the node's current one, and an exit during a boot is not lost
   (`node.boot_exit`).
5. `stop`/`stop_actor` racing `relaunch`: a new process can never be installed
   under a node that is `:stopping`, `:stopped`, or `:failed`, and can never be
   left running unowned. See `relaunch` (`installed`) and `stop_subtrees`
   (reference snapshot under the mutex).
6. `retire` nulls `node.reference`; every read of `node.reference` outside the
   mutex must have captured it under the mutex first.
7. Restart accounting (`restart_delay`) cannot restart forever when
   `restart: :on_failure` and the actor never runs for `restart_window`.
8. `broker.stop` is idempotent, rejects new work, and joins every broker thread
   without deadlock, including while lifecycle jobs and relaunch tasks are queued
   or running.
9. Nothing an actor process can send over its socket (malformed frames, unknown
   ops, wrong types, huge frames, floods) can raise on a broker thread, block
   another actor's reader, or leave broker accounting inconsistent. The reader
   thread of the offending actor may die (that actor is then treated as failed).

## Specific interleavings worth tracing

- Reader thread rejects the boot future (EOF) while the reaper thread runs
  `actor_exited` for the same reference; then `settle_boot` runs on the
  application thread.
- `stop_subtrees` marks a `:restarting` node `:stopping` between `relaunch`'s
  `Launcher.launch` and its `installed` check.
- Broker `stop` sets `@stopped` while a lifecycle worker is inside
  `Launcher.launch`, and while the service thread is between selecting due
  tasks and running them.
- Source actor dies with routes pending: `discard_control_outbox` runs the
  `on_done` callbacks from the writer thread's `ensure` and from
  `actor_exited`; confirm each callback runs once.
- `Future#value(timeout:)` reaching its expiration on an application thread concurrently with
  the reader fulfilling the same future.
- The reaper's `@reader.join(1)` in `Reference#actor_exited` when the reader is
  itself the thread that called `kill`.

## Out of scope

Style, naming, and documentation. Scale beyond a handful of actors (terminal
nodes are retained by design). Idempotency and message ids (application
design).
