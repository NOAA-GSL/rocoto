# Integrating rocoto_actor into a larger codebase

Written for a session working in the **host** repository. Everything stated
here is a property of this library and was verified against the code. The
decisions it deliberately leaves open depend on facts about the host that are
not visible from this repository.

Read this file and [architecture.md](architecture.md); you should not need the
chronological history in [actor-broker-handoff.md](actor-broker-handoff.md)
unless you want to know *why* something is the way it is.

## What you are integrating

- 23 files, about 3,200 lines, all under `lib/`.
- **No runtime dependencies.** Only stdlib: `socket`, `json`, `securerandom`,
  `rbconfig`. The development dependencies (minitest, rake, rubocop) are not
  needed to run it.
- Ruby >= 3.3. Apache-2.0.
- Public surface: `ActorBroker`, `ActorHandle`, `ActorContext`, `Timer`,
  `Future`, `ExitStatus`, the error classes, the default constants
  (`START_TIMEOUT` 5, `DEFAULT_MAILBOX_SIZE` 1000, `DEFAULT_MAILBOX_BYTES` 16
  MiB, `PARENT_CHECK_INTERVAL` 0.1, `VERSION`), and the `RocotoActor` module
  functions. Everything else — `Launcher`, `Reference`, `Transport`,
  `BrokerClient`, `Runner`, and the broker's internals — is a private constant,
  asserted by `test_internals_are_not_public`.

## The property that makes integration easy

Each actor process bootstraps itself entirely by relative path:

- `Launcher::RUNNER_PATH = File.expand_path("runner.rb", __dir__)`
- `runner.rb` starts with `require_relative "../rocoto_actor"`, and every
  internal require is `require_relative` too.

So a spawned actor needs **no `$LOAD_PATH` entry, no RubyGems activation, and
no Bundler** to load the library itself. Put the tree anywhere on disk and it
works. Vendoring is a copy; there is nothing to resolve.

## The property that will bite

An actor is a **fresh Ruby VM**, and the runner loads exactly one file into it:

```ruby
require ENV.fetch("ROCOTO_ACTOR_SOURCE")   # one file, absolute path
```

A child inherits the parent's **environment** but not its in-process
`$LOAD_PATH`. If an actor's source file requires other host code, those
requires must resolve on their own. Three ways, in rough order of preference:

1. **Bundler** — a child inherits `RUBYOPT=-rbundler/setup` and
   `BUNDLE_GEMFILE`, so it resolves the same bundle with no extra work. Cost:
   Bundler's startup on every actor boot; budget it against `start_timeout:`
   (default 5 s).
2. **`RUBYLIB`** — set it before spawning; it is inherited.
3. **The actor file sets up its own load path** before its requires.

**Settle this before anything else.** It decides whether an actor can use host
code at all, and it is the most likely source of a confusing first failure.
Prove it with one actor that requires something real from the host.

## Packaging options

Both work, because of the self-loading bootstrap above. What differs is
development infrastructure, not runtime behavior.

**A. Path gem in a subdirectory.** `gem "rocoto_actor", path: "vendor/rocoto_actor"`
in the host Gemfile, with this repository's tree underneath. Keeps the gemspec,
the suite, the harnesses, `REVIEW.md`, and the version; host CI can run
`bundle exec rake` in that directory as its own job. Extraction later is
`git subtree split -P vendor/rocoto_actor`, which returns a standalone repo
with history.

**B. Plain source under the host's `lib/`.** Drop the gemspec and Gemfile and
`$LOAD_PATH.unshift` the directory (or rely on the host's existing mechanism).
Appropriate if the host does not use Bundler at runtime — common for a
self-contained tarball install. You inherit the host's RuboCop config, test
runner, and CI, which is the real cost: see the next section.

## What travels as-is, and what needs a decision

Travels unchanged: `lib/`, `LICENSE`.

Needs a decision from someone who can see the host:

| Thing | Why it needs a decision |
| --- | --- |
| `test/` (160 tests) | Its own Minitest suite with `test/support/` actors that are spawned as real processes. Keep it as a separate job, or merge it into the host's suite. |
| `.rubocop.yml` | `Metrics` disabled on purpose, line length 120, rescued exceptions named `error`, a `Naming/PredicateMethod` allow-list. If the host's config differs, reconcile deliberately rather than letting autocorrect loose — an unsafe `rubocop -A` once broke every actor-exit path here. |
| `.github/workflows/ci.yml` | Ruby 3.3 and 3.4 on Ubuntu, 3.4 on macOS, plus a manual soak job. macOS has caught two races Linux never did; keeping a macOS job is worth it. |
| `Gemfile` / gemspec | Dropped entirely under option B. |
| Soak and fault matrix | Operator harnesses, deliberately not part of `rake test`. See [linux-validation.md](linux-validation.md). |
| `LICENSE` / `NOTICE` | The host is already Apache-2.0. Check whether its `LICENSE` carries a filled-in copyright line or a `NOTICE` file; this repository ships the pristine Apache text with the appendix template unfilled. |

## Operational facts the host needs to know

- **Cost per actor:** about 7 kernel tasks — watchdog and worker processes (two
  threads each) plus reader, writer, and reaper threads in the application. The
  broker itself adds 3 threads, created in `ActorBroker.new`. Three actors is
  roughly 26 tasks in all.
- **Process limit:** before every launch a preflight refuses to start an actor
  that would not fit under the user's `RLIMIT_NPROC` with a margin (default 32),
  raising `ResourceLimitError`. `ActorBroker.new(process_margin: nil)` disables
  it. This matters on HPC login nodes, where `ulimit -u` is often 1024 and is
  counted across everything the user runs on the host. Reaching the limit can
  wedge the Ruby VM itself, which is why the preflight exists; see
  [linux-validation.md](linux-validation.md).
- **Actor stdio goes to `/dev/null`.** Diagnostics must go through the protocol
  or an explicitly configured logging destination.
- **Always call `broker.stop`.** If the application exits without it, each
  watchdog notices within `PARENT_CHECK_INTERVAL` (0.1 s) and kills its group,
  so nothing is orphaned for long — but the orderly path is `stop`.
- **`ActorBroker.new(error_handler:)`** receives `(error, context)` for failures
  on the broker's own threads. The default warns to stderr; a host with real
  logging should pass its own. It must not raise.
- **Delivery is at-most-once and nothing is ever retried.** Idempotency and
  deduplication are the application's design, by decision.

## Invariants not to break

The full list with the interleavings to trace is in [REVIEW.md](../REVIEW.md).
The ones most likely to be violated by well-meaning integration work:

- There is no public `RocotoActor.spawn`, and there should not be. `ActorBroker`
  is the only way to create an actor.
- The broker mutex is never held while calling a `Reference` method, and no
  application or actor callback runs under a lock.
- Broker threads never die: everything they run is routed through
  `ErrorReporting`.
- No retries, idempotency keys, application-side timers, actor registry, or
  thread-based actors. These are deliberate omissions, not gaps.

## Verifying an integration

1. Spawn one actor whose source file requires real host code, and confirm it
   boots. This proves the load-path decision.
2. Run this library's own suite from its new location (`bundle exec rake`, or
   the host's equivalent): 160 runs, 0 failures.
3. Confirm a clean shutdown leaves nothing behind — after `broker.stop`, no
   child processes and no zombies.
4. If actors will be long-lived, run a short soak on the host's target platform:
   `SOAK_SECONDS=600 bundle exec ruby -Ilib test/soak/soak.rb`.

## Where to read more

| Question | File |
| --- | --- |
| How do I use it? | [../README.md](../README.md) |
| How is it built? | [architecture.md](architecture.md) |
| What must a change preserve? | [../REVIEW.md](../REVIEW.md) |
| What does it do under platform faults? | [linux-validation.md](linux-validation.md) |
| Why is it this way? | [actor-broker-handoff.md](actor-broker-handoff.md) |
| What is in this version? | [../CHANGELOG.md](../CHANGELOG.md) |
