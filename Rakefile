# frozen_string_literal: true

require 'bundler/setup'
require 'rspec/core/rake_task'
require 'rake/testtask'

# Default task runs all specs
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = '--format documentation'
end

# rocoto_actor keeps the Minitest suite it was developed with. It is not
# rewritten as specs on purpose: it spawns real actor processes, and a hand
# conversion of its concurrency assertions could weaken them without any test
# failing to say so. If it is ever converted, this suite is the oracle that
# shows the conversion was faithful.
#
# The pattern matches only *_test.rb, which leaves the soak and fault-matrix
# harnesses out of the suite exactly as they were upstream: they are operator
# tools, run by hand.
Rake::TestTask.new(:test) do |t|
  t.libs << 'lib'
  t.pattern = 'test/rocoto_actor/**/*_test.rb'
end

# Task to run specs with coverage
desc 'Run specs with coverage report'
task :coverage do
  ENV['COVERAGE'] = 'true'
  Rake::Task[:spec].reenable
  Rake::Task[:spec].invoke
end

# Task to run specs with coverage shown in terminal
desc 'Run specs with coverage report in terminal'
task :coverage_terminal do
  ENV['COVERAGE'] = 'true'
  ENV['COVERAGE_TERMINAL'] = 'true'
  Rake::Task[:spec].reenable
  Rake::Task[:spec].invoke
end

# RuboCop tasks
begin
  require 'rubocop/rake_task'

  RuboCop::RakeTask.new(:rubocop) do |task|
    task.options = ['--display-cop-names']
  end

  desc 'Run RuboCop with auto-correct'
  task :rubocop_fix do
    sh 'bundle exec rubocop -a'
  end
rescue LoadError
  # RuboCop not available
end

# Both suites, so `rake` on its own remains the single command that runs
# everything, whichever runner a given test happens to use.
task default: %i[spec test]

desc 'List all rake tasks'
task :tasks do
  puts 'Available tasks:'
  puts '  rake                   - Run both suites'
  puts '  rake spec              - Run all specs'
  puts '  rake test              - Run the rocoto_actor Minitest suite'
  puts '  rake coverage          - Run specs with HTML coverage report'
  puts '  rake coverage_terminal - Run specs with coverage shown in terminal'
  puts '  rake -T                - List all tasks'
end
