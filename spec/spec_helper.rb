# frozen_string_literal: true

# Configure SimpleCov for code coverage
if ENV['COVERAGE']
  require 'simplecov'

  # Custom formatter for terminal output
  module SimpleCov
    module Formatter
      class TerminalFormatter
        def format(result)
          puts "\n#{'=' * 80}"
          puts "COVERAGE REPORT BY FILE"
          puts "=" * 80

          result.groups.each do |group_name, files|
            # Only show our code, not gems
            project_files = files.select { |f| !f.filename.include?('/bundle/') && !f.filename.include?('/vendor/') }
            next if project_files.empty?

            puts "\n#{group_name}:"
            puts "-" * 80
            project_files.sort_by(&:covered_percent).reverse.each do |file|
              printf "  %-<file>60s %<percent>6.2f%% (%<covered>d/%<total>d)\n",
                     file: file.filename.sub("#{SimpleCov.root}/", ''),
                     percent: file.covered_percent,
                     covered: file.covered_lines.count,
                     total: file.lines_of_code
            end
          end

          puts "\n#{'=' * 80}"
          printf "TOTAL COVERAGE: %<percent>.2f%% (%<covered>d/%<total>d lines)\n",
                 percent: result.covered_percent,
                 covered: result.covered_lines,
                 total: result.total_lines
          puts "=" * 80
        end
      end
    end
  end

  SimpleCov.start do
    add_filter '/spec/'
    add_filter '/test/'
    add_filter '/vendor/'
    add_filter '/bundle/'
    add_group 'Libraries', 'lib'

    # Track all .rb files in lib/, even if not loaded by tests
    track_files 'lib/**/*.rb'

    # Minimum coverage lowered because we now track ALL files (not just loaded ones)
    minimum_coverage 4.5 # Will increase as we add more tests

    # Use terminal formatter if requested, otherwise just HTML
    if ENV['COVERAGE_TERMINAL']
      formatter SimpleCov::Formatter::MultiFormatter.new([
                                                           SimpleCov::Formatter::HTMLFormatter,
                                                           SimpleCov::Formatter::TerminalFormatter
                                                         ])
    end
  end
end

# Add lib directory to load path
$LOAD_PATH.unshift File.expand_path('../lib', __dir__)

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  # Use expect syntax (not should)
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  # :pbs specs submit real jobs to a PBS Professional cluster and are opt-in.
  # Run them explicitly with `bundle exec rspec --tag pbs` on a host with `qsub`.
  config.filter_run_excluding pbs: true unless ENV['ROCOTO_RUN_PBS_SPECS']

  # :slurm specs submit real jobs to a Slurm cluster and are opt-in.
  # Run them explicitly with `bundle exec rspec --tag slurm` on a host with `sbatch`.
  config.filter_run_excluding slurm: true unless ENV['ROCOTO_RUN_SLURM_SPECS']

  # Configure output format
  config.default_formatter = 'doc' if config.files_to_run.one?

  # Run specs in random order to surface order dependencies
  config.order = :random
  Kernel.srand config.seed

  # Allow more verbose output when running a single spec file
  if config.files_to_run.one?
    config.default_formatter = 'doc'
  end
end
