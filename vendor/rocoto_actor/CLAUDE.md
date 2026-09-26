# RocotoActor

Process-isolated actors for Ruby: each actor runs in its own OS process behind a
Unix socket pair so a hung or crashed actor cannot take the application down.
The purpose is bulkheading for a handful of long-lived actors, not throughput
with thousands of them. `RocotoActor::ActorBroker` is the only way to create
actors; everything under `RocotoActor` other than `ActorBroker`, `ActorHandle`,
`ActorContext`, `Timer`, `Future`, `ExitStatus`, and the errors is a private
constant.

## Commands

- `bundle exec rake` runs RuboCop and then the test suite; both must pass.
- `bundle exec ruby -Ilib test/soak/soak.rb` (30 minutes by default via
  `SOAK_SECONDS`) and `bundle exec ruby -Ilib test/validation/fault_matrix.rb`
  are operator harnesses, not part of the suite.
- Use `rubocop -a` (safe corrections) only; an unsafe `-A` pass once broke every
  actor-exit path. Run the suite after any auto-correction.

## Where to look

- `docs/actor-broker-handoff.md` is the design record: every decision, every
  review finding and its fix, and the validation results. Update it when
  behaviour changes.
- `REVIEW.md` is the review brief: the concurrency invariants and the
  interleavings to trace. Reviews should cite its invariant numbers.
- `docs/linux-validation.md` records the fault-matrix results and the accepted
  limitations (D state, process-group escape, `RLIMIT_NPROC`).

## Conventions that matter for review

- Rescued exceptions are named `error`. Methods that report success but have
  side effects (`stop`, `expire`, `wait_for_exit`) do not end in `?`.
- The broker mutex is never held while calling a `Reference` method that takes
  `@pending_mutex`, and no `Reference` callback runs under a lock.
- Tests wait on the exact node whose state they assert; never infer one node's
  terminal state from another's (retirement is deepest-first and may run on
  the lifecycle pool).
- No retries, no idempotency keys, no application-side timers, no actor
  registry: those are application design by decision, not omissions.
