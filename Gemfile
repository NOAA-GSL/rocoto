# frozen_string_literal: true

source "https://rubygems.org"

# Ruby 3.4.0 has ABI incompatibility with nokogiri precompiled binaries.
# Error: "undefined symbol: ruby_abi_version" when loading nokogiri C extensions.
# Fixed in Ruby 3.4.1 (verified via testing).
ruby ">= 3.3.0", "!= 3.4.0"

gem "nokogiri", "~> 1.19.3" # Uses precompiled libxml2 on supported platforms
gem "open4", "~> 1.3"
gem "sqlite3", "~> 2.0"
gem "thread", "~> 0.2"

# Legacy Ruby shim libraries for compatibility
# TODO: These can likely be removed after refactoring parsedate usage to use Date.parse
gem "rubysl-date", "~> 1.0"
gem "rubysl-parsedate", "~> 1.0"

group :development, :test do
  # rocoto_actor keeps its own Minitest suite under test/rocoto_actor/; see
  # the Rakefile. minitest is a bundled gem, so it has to be declared here
  # or `require "minitest/autorun"` fails under Bundler.
  gem "minitest", "~> 5.0"
  gem "rake", "~> 13.0"
  gem "rspec", "~> 3.13"
end

group :development do
  gem "rubocop", "~> 1.66", require: false
  gem "rubocop-rake", "~> 0.6", require: false
  gem "rubocop-rspec", "~> 3.0", require: false
end

group :test do
  gem "simplecov", "~> 0.22", require: false
end
