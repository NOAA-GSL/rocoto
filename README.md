# Rocoto Workflow Management System

[![TestSuite](https://github.com/NOAA-GSL/rocoto/actions/workflows/test-suite.yaml/badge.svg)](https://github.com/NOAA-GSL/rocoto/actions/workflows/test-suite.yaml)

## Documentation
Detailed documentation is provided at https://noaa-gsl.github.io/rocoto/

The `rocoto_actor` library, which gives Rocoto its process-isolated actors, keeps
its own documentation in [docs/rocoto_actor/](docs/rocoto_actor/): the
architecture and concurrency model, the review brief, the Linux validation
record, and the library's own README.

## Introduction
Workflow Management is a concept that originated in the 1970's to handle business process management. Workflow management systems were developed to manage complex collections of business processes that need to be carried out in a certain way with complex interdependencies and requirements. Scientific Workflow Management is much newer, and is very much like its business counterpart, except that it is usually data oriented instead of process oriented. That is, scientific workflows are driven by the scientific data that "flows" through them. Scientific workflow tasks are usually triggered by the availability of some kind of input data, and a task's result is usually some kind of data that is fed as input to another task in the workflow. The individual tasks themselves are scientific codes that perform some kind of computation or retrieve or store some type of data for a computation. So, whereas a business workflow is comprised of a diverse set of processes that have to be completed in a certain way, sometimes carried out by a machine, sometimes carried out by a human being, a scientific workflow is usually comprised of a set of computations that are driven by the availability of input data.

## Installation

### Requirements
- Ruby 3.3.0 or higher (Ruby 3.2 and earlier are no longer supported)

### Quick Start
1. Clone or download Rocoto to your desired installation directory
2. Run the installation script:
   ```bash
   ./INSTALL
   ```
3. Add Rocoto's bin directory to your PATH:
   ```bash
   export PATH="/path/to/rocoto/bin:${PATH}"
   ```

### Installation Options

If you need to specify a custom Ruby installation, use this option:

```bash
./INSTALL --with-ruby=/path/to/ruby
```

Available options:
- `--with-ruby=/path/to/ruby` - Specify Ruby installation directory
- `--local` - Install from cached gems in vendor/cache/ (air-gapped mode)

The installation script will:
1. Verify Ruby version (≥ 3.3.0)
2. Install Bundler if not already available
3. Update script shebangs to use the specified Ruby
4. Install all gem dependencies to `vendor/bundle`

### Managing Dependencies

Rocoto uses Bundler to manage gem dependencies. The required gems are specified in the `Gemfile`. If you need to update or add dependencies
in the future, you can use standard Bundler commands:

```bash
# Update all gems to latest compatible versions
bundle update

# Install a specific gem
bundle add <gem-name>

# Check for outdated gems
bundle outdated
```

## Testing

Rocoto has two test suites: RSpec specs under `spec/` covering Rocoto itself,
and a Minitest suite under `test/rocoto_actor/` covering the `rocoto_actor`
library in `lib/rocoto_actor/`. `rake` runs both. Either can be run locally or
in CI; see `TESTING.md` for the full picture.

### Running Tests Locally

After installation, run the test suite:

```bash
# Run everything: the RSpec specs and the rocoto_actor Minitest suite
bundle exec rake

# Run all specs
bundle exec rake spec

# Run the rocoto_actor suite on its own. It spawns real actor processes and
# takes about three minutes, so rake spec is the faster loop for Rocoto work.
bundle exec rake test

# Run specs with coverage report
bundle exec rake coverage

# Run with coverage shown in terminal
bundle exec rake coverage_terminal

# Run a specific spec file
bundle exec rspec spec/workflowmgr/cycledef_spec.rb

# Run specs matching a pattern
bundle exec rspec spec/workflowmgr/cycledef_spec.rb -e "exclude_hours"

# Run a single rocoto_actor test file
bundle exec rake test TEST=test/rocoto_actor/broker_restart_test.rb
```

### Continuous Integration

The project uses GitHub Actions. The Rocoto specs run inside Slurm and PBS
containers against every supported Ruby version:
- Ruby 3.3.0 (minimum supported)
- Ruby 3.3 (latest patch)
- Ruby 3.4.1 (second release; 3.4.0 is excluded for a nokogiri ABI incompatibility)
- Ruby 3.4 (latest patch)
- Ruby 4.0.0 (first release)
- Ruby 4.0 (latest patch)

The `rocoto_actor` suite runs as its own job, without a container or a
scheduler, on Ruby 3.3 and 3.4 under Linux and on Ruby 3.4 under macOS. macOS
is included because it has caught process races that Linux did not. That
matrix is narrower than the one above, which is a known gap rather than a
decision to keep it narrow.

Tests run on every push and pull request.

### Air-Gapped Installation

For systems without internet access, Rocoto supports installation from cached gem files using the `--local` option.

**For maintainers:** To create/update the gem cache:
```bash
# First, install normally (requires internet)
./INSTALL

# Configure bundler to cache all dependencies
bundle config set cache_all true

# Package the gems into vendor/cache/
bundle cache

# Commit the cache to the repository
git add vendor/cache/
git commit -m "Update gem cache"
```

**For users on air-gapped systems:**
```bash
# Use the --local option to install from cached gems
./INSTALL --local
```

The cached gems are stored in `vendor/cache/` and can be committed to version control.

## Why Workflow Management?
The day when a scientist could conduct his or her numerical modeling and simulation research by writing, running, and monitoring the progress of a modest Fortran code or two, is quickly becoming a distant memory. It is a fact that researchers now often have to make hundreds or thousands of runs of a numerical model to get a single result. In addition, each end-to-end "run" of the model often entails running many different codes for pre- and post-processing in addition to the model itself. And, in some cases, multiple models and their associated pre- and post-processing tasks are coupled together to build a larger, more complex model. The codes that comprise the end-to-end modeling systems often have complex interdependencies that dictate the order in which they can be run. And, in order to run the end-to-end system efficiently, concurrency must be used when dependencies allow it. The problem of scale and complexity is exacerbated by the fact that these codes are usually run on high performance machines that are notoriously difficult for scientists to use, and which tend to exhibit frequent failures. As machines get larger and larger, the failure rate of hardware and software components increases commensurately. Ad-hoc management of the execution of a complex modeling system is often difficult even for a single end-to-end run on a machine that never fails. Multiply that by the thousands of runs needed to perform a scientific experiment, in a hostile computing environment where hardware and facility outages are not uncommon, and you have a very challenging situation. For simulations that must run reliably in realtime, the situation is almost hopeless. The traditional ad-hoc techniques for automating the execution of modeling systems (e.g. driver scripts, batch job chains or trees) do not provide sufficient fault tolerance for the scale and complexity of current and future workflows, nor are they reusable; each modeling system requires a custom automation system.

A Workflow Management System addresses the problems of complexity, scale, reliability, and reusability by providing two things:

* A high-level means by which to describe the various codes that need to be run, along with their runtime requirements and interdependencies.
* An automation engine for reliably managing the execution of the workflow

## Prerequisites
Depending on how the components of a modeling system are designed and how existing software for running them is designed, some changes may be necessary to make use of a workflow management system. In order to take full advantage of the features offered by a workflow management system, the model system components must be well designed. In particular the following best practices should be followed:

Each workflow task must correctly check for its successful completion, and must return a non-zero exit status upon failure. An exit status of 0 means success, regardless of what actually happened. No workflow task should contain automation features. Automation is the workflow management system's responsibility. A workflow management system cannot manage tasks or jobs that it is not aware of. Enable reuse of workflow tasks by using principles of modular design to build autonomous model components with well-defined interfaces for input and output that can be run stand-alone. Prefer the construction of small model components that do only one thing. It is easy to combine several small, well-designed, components together to build a larger, more complex workflow task. It is generally much more difficult to divide large, complex, model components into smaller ones to form multiple workflow tasks. Avoid combining serial and parallel processing in the same workflow task unless the serial processing is very short in duration.

## Code Linting and Style (RuboCop)

Rocoto uses [RuboCop](https://github.com/rubocop/rubocop) for Ruby code linting and style enforcement. RuboCop checks for code quality, formatting, and common errors.

### Running RuboCop

To check the codebase for style and lint issues:

```bash
# Lint all Ruby files in lib/ and bin/
bundle exec rubocop

# Lint a specific file
bundle exec rubocop lib/workflowmgr/workflowengine.rb
```

### Auto-correcting Offenses

RuboCop can automatically fix many issues:

```bash
# Safe auto-corrections only
bundle exec rubocop -a

# Safe and unsafe auto-corrections (use with caution)
bundle exec rubocop -A
```

See `.rubocop.yml` for the current configuration. Some legacy code may not be fully auto-correctable; review changes before committing.

For more details, see the [RuboCop documentation](https://docs.rubocop.org/rubocop/).
