# Linux Validation

## Status

Executed on 2026-09-22 (Ruby 3.4.10, Linux 7.0.12-linuxkit, Ubuntu 26.04 container under Docker Desktop). Results are recorded in "Results" below; the checklist that follows them is kept as the specification to re-run against. Rerun with:

```sh
bundle exec rake                                          # lint and suite
bundle exec ruby -Ilib test/validation/fault_matrix.rb    # fault matrix (~1 min)
SOAK_SECONDS=1800 bundle exec ruby -Ilib test/soak/soak.rb
```

## Objective

Perform a fresh adversarial review and Linux-specific validation of RocotoActor. Treat the implementation and existing tests as untrusted. Report findings before changing production code.

The library exists to keep an application responsive when an actor or a subprocess launched by an actor blocks indefinitely, including Linux uninterruptible sleep (`D` state). It is a reliability bulkhead, not a security sandbox.

## Current architecture

- The internal launcher (`RocotoActor::Launcher`, used by `ActorBroker#spawn`) creates a Unix socket pair and starts a fresh Ruby watchdog with `Process.spawn`.
- The watchdog is a normal child of the application and a process-group leader. It forks one actor worker before starting threads.
- The actor worker loads the actor's dedicated source file, constructs it from JSON-serialized arguments, and processes one message at a time.
- Only the actor socket is passed across `exec`; standard input, output, and error are mapped to `/dev/null`.
- The parent has a bounded outbound mailbox and separate reader, writer, and reaper threads.
- Mailbox limits apply to both message count and encoded bytes.
- `ask` returns a future without waiting for socket capacity. A full mailbox raises `MailboxFullError`.
- A future timeout is terminal and does not cancel queued or executing actor work.
- Graceful stop drains accepted messages. Force stop and graceful timeout send `KILL` to the actor process group.
- `stop` returns `true` when the whole process group is observed gone. After a deadline forces a `KILL`, it waits up to `Reference::KILL_CONFIRMATION_GRACE` (0.5 s) more to confirm, so `false` means the group was still present after `KILL`, not merely that the deadline passed.
- The watchdog polls for application death every 100 milliseconds and kills its process group if the application or actor worker exits.

Primary implementation files:

- `lib/rocoto_actor/broker.rb`: `ActorBroker`, the only public way to create actors; lifecycle nodes, routing, restart policy
- `lib/rocoto_actor/launcher.rb`: process creation, boot request, process-group helpers (private)
- `lib/rocoto_actor/runner.rb`: watchdog and actor worker lifecycle, exit reporting
- `lib/rocoto_actor/reference.rb`: mailbox, reader/writer/reaper threads, stopping, process-group status (private)
- `lib/rocoto_actor/future.rb`: result, error, and terminal timeout state
- `lib/rocoto_actor/transport.rb`: framed tagged-JSON codec (private)

## Accepted limitations

- `SIGTERM` and `SIGKILL` do not take effect while a process remains in `D` state. The parent application must nevertheless stay responsive and `stop` must return `false` once its deadline and the kill-confirmation grace have passed.
- A subprocess can escape process-group containment by deliberately creating a new session or process group (confirmed by probe P5).
- Ruby itself can wedge under `RLIMIT_NPROC`: when thread creation fails at the limit, the VM sometimes blocks in a futex and ignores `TERM`; only `KILL` removes it. Observed twice while writing the fault matrix and not reproduced on the recorded run, where spawn failed cleanly with `ThreadError`. The library cannot recover a wedged VM; deployments must keep process limits above need (for example a cgroup `pids.max` with headroom) and rely on `KILL`.
- `RLIMIT_NPROC` counts the uid's processes and threads on the whole host, so a container cannot compute a meaningful limit for it; the fault matrix finds one by probing.
- Actors run with the application's UID, environment, working directory, resource limits, and filesystem/network access.
- A timed-out future does not cancel work. Late responses are discarded.
- Actor source files must be independently loadable and free of application-startup side effects.

## Results (2026-09-22, Linux)

Suite: `110 runs, 412 assertions, 0 failures` with RuboCop clean, on Ruby 3.4.10 here and on Ruby 3.2, 3.3, 3.4 (Ubuntu) plus 3.4 (macOS) in CI. Fault matrix (`test/validation/fault_matrix.rb`): 30 probes, 29 passed, 1 note, no leaked threads, descriptors, children, or zombies at the end.

| Probe | Result | Evidence |
|---|---|---|
| C1/C2 400 asks from 8 threads racing graceful and force stop | pass | every future terminal (result or `ActorStoppedError`), no asker blocked, `stop=true` |
| C3 8 concurrent graceful+force stops | pass | all returned `true`, actor gone |
| C4 300 replies racing `Future#value` timeouts | pass | 289 timeouts, 11 results; every future's outcome stable on a second `value` |
| C5 worker `KILL` with 30 requests queued/executing | pass | 2 results, 28 `ActorStoppedError`, node `:failed` |
| C6 mailbox count and byte limits, 8 threads × 100 asks | pass | 13 accepted, 787 `MailboxFullError`, no blocking, all settled after force stop |
| C7 180 actors spawned/asked/stopped/crashed/killed from 6 threads | pass | back to baseline threads and fds, 0 zombies |
| P1 worker `TERM`/`KILL` while a forked descendant holds the socket | pass | pending request rejected, holder killed by the watchdog, `last_exit` shows the signal |
| P2 watchdog `KILL` while worker idle and busy | pass | node `:failed`, worker gone |
| P3 application normal exit / `TERM` / `KILL` with idle, busy, descendant-holding actors | pass | application, worker, and holder all gone within 3 s |
| P5 descendant in its own process group | note | survives stop, as documented; the probe kills it |
| P8 worker ignoring `TERM` | pass | `stop(timeout: 1)` returned `true` in 1.0 s |
| T2 malformed replies: oversized header, invalid JSON, unknown tag, missing id | pass | request rejected with `ActorStoppedError`, node `:failed`, `last_failure` "malformed reply: …", unrelated actor unaffected |
| T2 error reply with non-string fields | pass | coerced `RemoteError`, actor keeps running |
| T2 truncated frame then silence | pass | behaves as a hung actor: caller timeout, `stop` works |
| T3 3,000 fuzzed frames | pass | only `SerializationError`, `Error`, `EOFError`, or a decode; never another exception |
| T4 parent RSS while filling a 4 MiB mailbox | pass | +4.3 MB, released after stop |
| T5 `RLIMIT_NOFILE` = 48 | pass | 37 actors, then `Errno::EMFILE` raised cleanly; broker recovered after freeing descriptors |
| T5 `RLIMIT_NPROC` at probed threshold + 40 | pass (this run) | 30 actors, then `ThreadError` raised cleanly; see accepted limitations for the wedge seen on other runs |
| T6 descriptors in worker and watchdog | pass | `/dev/null` ×3, one anonymous socket, Ruby's eventfd and epoll only |
| D1 `SIGSTOP`ped worker | pass | ask times out terminally, other actors responsive, `stop` confirms the `KILL` |
| S1 socket | pass | `socketpair`, no filesystem path |
| S4 50,000 decoded symbols | pass | mortal dynamic symbols 111 → 50,111 → 111 after GC |

Cases covered by the normal suite rather than the matrix: startup timeout with a boot that ignores `TERM`; socket back-pressure not blocking `ask` or `stop`; descendants ignoring `TERM` removed by group `KILL`; worker exit with an inherited socket; application death with background children; codec rejection of unsupported values, cycles, invalid UTF-8, oversized frames, and non-finite floats; read timeouts across partial frames.

Not reproduced here, with reasoning:

- True `D` state needs a blocked-I/O fixture on disposable storage. `SIGSTOP` was used as the "unresponsive but killable" analogue; the `D`-state case differs only in that `KILL` is delayed, which is exactly what `stop` returning `false` after the grace reports.
- PID churn confusing `alive?`/`stop`: the watchdog pid is held as a zombie until this process's own reaper calls `waitpid`, so it cannot be recycled before its exit is observed. After that, `process_group_alive?` probes the pgid; a recycled pid becoming a new group leader in the few milliseconds between reap and confirmation is theoretically possible and would only make `wait_for_exit` report `false`, never signal an unrelated group (nothing signals after the reaper has run). Not worth a fixture at `pid_max` 4,194,304.

Findings made while writing the matrix, all fixed with regression tests in the suite:

1. A reply without `:id` killed the reader thread, and the reaper's `join` re-raised that exception and died before running the broker's exit callback, leaving the node `:running` forever. Malformed frames are now protocol violations that stop the actor; the join swallows reader exceptions.
2. `RemoteError.new` raised on non-string fields after the future had left the pending table, so the caller could never be answered. Fields are coerced.
3. `stop` returned `false` whenever the deadline forced a `KILL`, even when the `KILL` worked milliseconds later, so callers could not tell "killed late" from "still alive". A 0.5 s confirmation grace makes `false` mean the latter.

Security review outcome: the socket is an anonymous `socketpair`; only the socket and `/dev/null` cross `exec` (`close_others: true`, verified in `/proc`); the child environment carries only the four `ROCOTO_ACTOR_*` variables plus the inherited environment; class names are resolved with `const_get(name, false)` and never evaluated; the codec is tagged JSON with frame, nesting, and encoding limits checked before allocation, and remote error fields become strings on a `RemoteError`, never a reconstructed class. Same-uid actor code can read what the application can read and signal its processes; that is stated in `README.md`.

Earlier macOS evidence (30 runs, 66 assertions, before the broker existed) is superseded by the CI macOS job.

## Required Linux review

Start by recording:

```sh
uname -a
ruby -v
bundle -v
git rev-parse HEAD
git status --short
```

Install dependencies and run the baseline:

```sh
bundle install
bundle exec rake test
gem build rocoto_actor.gemspec
rm -f rocoto_actor-*.gem
```

Then perform a fresh code review without using the list of previously fixed bugs as a checklist. Model these state machines explicitly:

1. Request: created, serialized, admitted, queued, writing, delivered, replied, timed out, rejected.
2. Reference: running, draining, terminating, watchdog exited, process group gone.
3. Processes: application, watchdog, worker, actor descendants, escaped descendants.
4. Transport: empty, partial header, partial body, full frame, EOF, malformed frame, blocked reader/writer.

Report findings with severity, file and line, triggering interleaving, impact, and a proposed correction. Run a bounded reproducer before labeling timing-dependent findings as confirmed.

## Linux fault matrix

Exercise at least these cases with hard outer time limits and cleanup traps:

### Concurrency

- Many threads call `ask` while another calls graceful `stop`.
- Many threads call `ask` while another calls force stop.
- Multiple concurrent graceful and forceful stops.
- Replies race exactly with `Future#value` timeout.
- Actor exits while requests are queued, writing, executing, and replying.
- Mailbox count and byte limits are reached concurrently.
- Repeat randomized lifecycle stress for thousands of actor instances or until resource limits make that impractical.

For every accepted request, verify its future reaches exactly one permanent terminal state. Verify no caller remains blocked after the test deadline.

### Process lifecycle

- Kill the worker with `TERM`, `KILL`, and ordinary exit while a descendant retains the socket.
- Kill the watchdog while the worker is idle and while it is busy.
- Kill the application with normal exit, `TERM`, and `KILL` while the actor is idle, busy, and has descendants.
- Launch descendants that ignore `TERM`; verify group `KILL` removes them.
- Confirm escaped sessions/process groups are the only documented descendant escape.
- Verify actors and watchdogs are reaped and do not accumulate as zombies.
- Verify `alive?` and `stop` do not confuse unrelated recycled PIDs or process groups under PID churn.
- Verify startup timeout remains bounded when source loading or initialization hangs and ignores `TERM`.

Inspect process state with commands such as:

```sh
ps -eo pid,ppid,pgid,sid,state,wchan:32,etime,cmd
cat /proc/PID/status
cat /proc/PID/wchan
cat /proc/PID/stack
```

Reading `/proc/PID/stack` may require elevated permissions. Do not route passwords or credentials through the agent.

### Transport and resources

- Fill the Unix socket buffer while the actor is not reading; `ask` must remain bounded and force stop must remain callable.
- Inject partial headers, partial payloads, EOF, malformed JSON, unknown tags, oversized declared frames, invalid UTF-8, deep nesting, cycles, huge integers, and unsupported values.
- Verify malformed responses reject pending futures or stop the actor without blocking the application.
- Measure parent memory while filling the mailbox near both limits. Confirm retained memory is bounded to an acceptable multiple of `mailbox_bytes`.
- Test file-descriptor exhaustion, process/thread exhaustion, and failure to create the socket, watchdog, worker, reader, writer, or reaper.
- Verify no descriptors other than the intended socket and `/dev/null` standard streams cross `exec`.

### Linux `D` state

Use a disposable environment and a safe, reproducible blocked-I/O fixture if one is available. Do not intentionally disrupt host storage or production mounts.

Verify while the worker is in `D` state:

- The parent application continues executing and serving unrelated work.
- Other actors remain responsive.
- `Future#value(timeout:)` reaches a permanent `AskTimeoutError` state.
- `stop(timeout:)` returns `false` near its deadline rather than hanging.
- Reader, writer, reaper, and watchdog behavior does not block application shutdown.
- Mailbox limits still prevent parent memory growth.
- Once the kernel wait releases, pending `KILL` takes effect and the process group disappears.

An Ubuntu container under Docker Desktop uses the Docker Linux VM kernel. It is suitable for Linux process, `/proc`, signal, and socket behavior, but may not reproduce the storage-driver, block-device, or NFS failure modes of the deployment host. Repeat the `D`-state scenario on a disposable environment representative of production if those subsystems are central to the threat model.

### Security

- Confirm the socket is not filesystem-addressable and only intended descriptors cross `exec`.
- Review environment-variable and source-path handling for injection or accidental secret exposure.
- Fuzz the framed JSON decoder and verify memory, nesting, and frame limits are enforced before dangerous allocation.
- Verify symbol decoding cannot cause unacceptable memory growth on every supported Ruby version.
- Confirm malformed remote error fields cannot instantiate arbitrary classes or execute deserialization hooks.
- Document that same-UID actor code can access application-readable resources and signal application processes; this library does not defend against malicious actor code.

## Supported Ruby versions

The gem declares Ruby `>= 3.2`. CI runs lint and the suite on 3.2, 3.3, and 3.4 on Ubuntu and on 3.4 on macOS. Differences in `Process.spawn`, `fork`, JSON, `Thread`, and signal behavior are especially relevant when adding a version.

## Completion criteria

- Findings are reported before production fixes are made. (Done: recorded above and in `docs/actor-broker-handoff.md`.)
- Every confirmed serious finding has a deterministic or bounded regression test. (Done.)
- Full tests pass on the tested Linux/Ruby matrix. (Done: CI.)
- Stress runs finish without blocked Ruby threads, zombies, or leaked actor descendants. (Done: C7 and the soak harness.)
- Parent responsiveness is demonstrated during a real or representative blocked-I/O scenario. (Representative only: D1 uses `SIGSTOP`; a true `D`-state run on disposable storage remains open if storage or NFS failure is central to the deployment's threat model.)
- Any remaining limitation is explicit in `README.md` and accepted before release. (`README.md` states the `D`-state, escape, and same-uid limitations; the `RLIMIT_NPROC` wedge is stated here and should be added to `README.md` if deployments run under tight process limits.)

## Suggested prompt for the container session

```text
Read README.md and docs/linux-validation.md. Perform the fresh Linux-specific
adversarial review described there. Treat the implementation as untrusted.
Run bounded tests and fault injection, but report findings before changing
production code. Do not create a dangerous D-state fixture on shared or
production storage.
```