# Testing Infrastructure Summary

## Test suites

Rocoto has two test suites, and `rake` on its own runs both:

* **RSpec specs** under `spec/` cover Rocoto itself.
* **A Minitest suite** under `test/rocoto_actor/` covers the `rocoto_actor`
  library in `lib/rocoto_actor/`. It is kept as Minitest deliberately: it
  spawns real actor processes, and a hand conversion of its concurrency
  assertions could weaken them without any test failing to say so.

The Minitest suite takes about three minutes, because it starts and kills real
processes, so `bundle exec rake spec` is the faster loop when working on Rocoto
rather than on actors.

`test/rocoto_actor/soak/` and `test/rocoto_actor/validation/` hold operator
harnesses — a long-running soak and a fault matrix — that are deliberately not
part of either suite. They are leak and fault detectors, run by hand.

Run `bundle exec rake coverage` for a current coverage report rather than
trusting a number written down here.

## Running Tests

### Locally (after installation with Ruby 3.3+)

```bash
# Run everything: both suites
bundle exec rake

# Run only the RSpec specs
bundle exec rake spec

# Run only the rocoto_actor Minitest suite
bundle exec rake test

# Run with coverage HTML report (generates coverage/index.html)
bundle exec rake coverage

# Run with coverage shown in terminal
bundle exec rake coverage_terminal

# Run specific spec
bundle exec rspec spec/workflowmgr/cycledef_spec.rb

# Run specs matching pattern
bundle exec rspec -e "exclude_hours"

# Run a single rocoto_actor test file
bundle exec rake test TEST=test/rocoto_actor/broker_restart_test.rb
```

### PBS integration tests (opt-in)

`spec/workflowmgr/pbs_integration_spec.rb` submits real jobs to a live PBS Professional cluster
(via `qsub`) instead of using a fake/dryrun batch system. Because it needs an actual scheduler, it
is tagged `:pbs` and excluded from the default run (`bundle exec rspec` / `bundle exec rake spec`
skip it automatically, whether or not `qsub` is installed).

To include it, opt in with the `ROCOTO_RUN_PBS_SPECS` environment variable and the `pbs` tag:

```bash
# Run only the PBS integration spec
ROCOTO_RUN_PBS_SPECS=1 bundle exec rspec --tag pbs spec/workflowmgr/pbs_integration_spec.rb

# Run the full suite, including PBS integration tests
ROCOTO_RUN_PBS_SPECS=1 bundle exec rspec --tag pbs
```

If `qsub` isn't on `PATH` when `ROCOTO_RUN_PBS_SPECS=1` is set, the spec skips itself with a message
instead of failing, so it's safe to leave the env var set in shells that don't have PBS available.

### Slurm integration tests (opt-in)

`spec/workflowmgr/slurm_integration_spec.rb` is the Slurm equivalent of the PBS integration spec
above, submitting real jobs via `sbatch` instead of using a fake/dryrun batch system. It is tagged
`:slurm` and excluded from the default run the same way the PBS spec is.

To include it, opt in with the `ROCOTO_RUN_SLURM_SPECS` environment variable and the `slurm` tag:

```bash
# Run only the Slurm integration spec
ROCOTO_RUN_SLURM_SPECS=1 bundle exec rspec --tag slurm spec/workflowmgr/slurm_integration_spec.rb

# Run the full suite, including Slurm integration tests
ROCOTO_RUN_SLURM_SPECS=1 bundle exec rspec --tag slurm
```

If `sbatch` isn't on `PATH` when `ROCOTO_RUN_SLURM_SPECS=1` is set, the spec skips itself with a
message instead of failing. The partition/account are auto-detected via `scontrol`/`sacctmgr`
(falling back to the `slurmpar` partition used by `docker/docker-compose.yml`), or you can force
them with the `ROCOTO_TEST_PARTITION` / `ROCOTO_TEST_ACCOUNT` environment variables.

### In CI

Tests run automatically on every push and pull request via GitHub Actions.

* The RSpec specs run inside Slurm and PBS containers against every supported
  Ruby version: 3.3.0, 3.3, 3.4.1, 3.4, 4.0.0 and 4.0. Ruby 3.4.0 is excluded
  for a nokogiri ABI incompatibility.
* The `rocoto_actor` Minitest suite runs as its own job, without a container or
  a scheduler, on Ruby 3.3 and 3.4 under Linux and on Ruby 3.4 under macOS.
  macOS is included because it has caught process races that Linux did not.
* The actor soak harness is a manual job only. Trigger the workflow with
  `workflow_dispatch` and set `soak_seconds` to something other than `0`.

## References

* [RSpec Documentation](https://rspec.info/)
* [RSpec Best Practices](https://www.betterspecs.org/)
* [Minitest Documentation](https://github.com/minitest/minitest)
* [SimpleCov](https://github.com/simplecov-ruby/simplecov)

## Linting and Style Checks (RuboCop)

RuboCop is used to enforce Ruby style and catch common issues. Run it before submitting changes:

```bash
# Lint the codebase
bundle exec rubocop

# Auto-correct safe issues
bundle exec rubocop -a
```

**Do not run `rubocop -A`** (unsafe auto-correct) across this repository without
reviewing every change it makes. An unsafe pass over the actor library once
broke every actor-exit path in it. Prefer `-a`, and run both test suites after
any auto-correction.

`lib/rocoto_actor/` and `test/rocoto_actor/` each carry their own
`.rubocop.yml`, inheriting from the root one and restoring two conventions the
library was written with: rescued exceptions are named `error`, and
`Naming/PredicateMethod` has an allow-list for commands that report success but
have side effects. Check `.rubocop.yml` for project-wide rules, and review all
auto-corrected changes before committing.
