# Changelog

Notable changes to `rocoto_actor`. The current design is described in
[architecture.md](architecture.md); the chronological record of why
each decision was made is in
[actor-broker-handoff.md](actor-broker-handoff.md).

## 0.1.0 — 2026-09-26

First release. Requires Ruby >= 3.3.

### Actors

- `RocotoActor::ActorBroker` is the only way to create an actor. Each actor runs
  in its own process behind a Unix socket pair, under a watchdog that owns its
  process group, so a hung or crashed actor cannot take the application down.
- `broker.spawn` returns an `ActorHandle`: an opaque capability that stays valid
  across restarts and is safe to send through the transport. No socket, process,
  or internal object ever crosses an actor boundary.
- `handle.ask` returns a `RocotoActor::Future`; `handle.tell` is one-way and
  carries the sender's handle. Delivery is at-most-once and nothing is retried.
- Actors form a logical hierarchy with paths. A child never outlives its parent.
- Messages, constructor arguments, actor names, and error text may contain any
  valid UTF-8, including multi-byte characters, and round-trip byte for byte in
  every direction at every frame length. A value that is not valid UTF-8, or is
  otherwise not serializable, raises `SerializationError` instead of corrupting
  the stream.

### Inside an actor

- `RocotoActor.context` provides `spawn`, `watch`, `unwatch`, `schedule`,
  `handle`, and `sender`. Children may be spawned from `initialize`.
- `context.schedule(message, after:, every:)` returns a `Timer`. A timer belongs
  to the incarnation that created it and dies with it.
- An optional `shutdown` method runs on a graceful stop.

### Failure and restart

- Per-actor policy: `restart: :never | :on_failure`, with `max_restarts`,
  `restart_window`, and exponential `restart_backoff`. The consecutive-failure
  count resets only after a full window of healthy uptime, so backoff delays
  cannot defeat the limit.
- `handle.last_failure` and `handle.last_exit` report why an incarnation ended,
  including deaths by signal and exits without an exception.
- `context.watch` notifies an actor of another's lifecycle events, and
  `ActorBroker.new(on_event:)` notifies the application. A synchronous call that
  would deadlock is refused with `DeadlockError` instead of timing out.

### Bounds and operations

- Routing is bounded by `max_routes`, `max_routes_per_actor`, and
  `route_timeout`, with no thread per call in flight. Spawn and stop requests
  from actors run on a bounded pool.
- Before every launch, a preflight refuses to start an actor that would not fit
  under the user's `RLIMIT_NPROC` with a margin, raising `ResourceLimitError`.
  Configure with `ActorBroker.new(process_margin:)`; `nil` disables the check.
- `broker.describe` returns a plain-data snapshot for operators, and
  `ActorBroker.new(error_handler:)` receives `(error, context)` for failures on
  the broker's own threads.

### Public surface

`ActorBroker`, `ActorHandle`, `ActorContext`, `Timer`, `Future`, `ExitStatus`,
the error classes, the default constants (`START_TIMEOUT`,
`DEFAULT_MAILBOX_SIZE`, `DEFAULT_MAILBOX_BYTES`, `PARENT_CHECK_INTERVAL`,
`VERSION`), and the `RocotoActor` module functions. Everything else —
`Launcher`, `Reference`, `Transport`, `BrokerClient`, `Runner`, and the broker's
internal parts — is a private constant, asserted by the test suite. There is no
public `RocotoActor.spawn`; the broker is the only way to create an actor.
