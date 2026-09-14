# Testing Infrastructure Summary

## Current Status

✅ **RSpec infrastructure:** Fully operational  
✅ **All tests passing:** 101/101 examples (100%)  
✅ **Code coverage:** 4.82% (442/9162 lines tracked)

Coverage includes all 52 Ruby files in `lib/`. The current tests cover 8 files (cycledef, workflowoption,
workflowdb, and dependencies). Coverage will increase as you refactor and add tests for currently untested
modules.

## Running Tests

### Locally (after installation with Ruby 3.2+)

```bash
# Run all tests
bundle exec rake spec

# Run with coverage HTML report (generates coverage/index.html)
bundle exec rake coverage

# Run with coverage shown in terminal
bundle exec rake coverage_terminal

# Run specific spec
bundle exec rspec spec/workflowmgr/cycledef_spec.rb

# Run specs matching pattern
bundle exec rspec -e "exclude_hours"
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
Tests run automatically on every push/PR via GitHub Actions with Ruby 3.2.0, 3.2.x, 3.3.0, and 3.3.x.

## References

* [RSpec Documentation](https://rspec.info/)
* [RSpec Best Practices](https://www.betterspecs.org/)
* [SimpleCov](https://github.com/simplecov-ruby/simplecov)

## Linting and Style Checks (RuboCop)

RuboCop is used to enforce Ruby style and catch common issues. Run it before submitting changes:

```bash
# Lint the codebase
bundle exec rubocop

# Auto-correct safe issues
bundle exec rubocop -a

# Auto-correct all (safe and unsafe) issues
bundle exec rubocop -A
```

Check `.rubocop.yml` for project-specific rules. Review all auto-corrected changes before committing.
