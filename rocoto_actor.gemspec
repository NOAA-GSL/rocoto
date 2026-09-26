# frozen_string_literal: true

require_relative "lib/rocoto_actor/version"

Gem::Specification.new do |spec|
  spec.name = "rocoto_actor"
  spec.version = RocotoActor::VERSION
  spec.summary = "Process-isolated actors for Ruby"
  spec.description = "Runs Ruby actors in isolated processes over Unix socket pairs."
  spec.authors = ["Christopher W. Harrop"]
  # Resolved from the gem root, not the caller's working directory, so the
  # list is the same when this gemspec is evaluated as a path gem elsewhere.
  spec.files = Dir.chdir(__dir__) { Dir["lib/**/*.rb", "README.md", "CHANGELOG.md", "LICENSE"] }
  spec.require_paths = ["lib"]
  spec.required_ruby_version = ">= 3.3"
  spec.license = "Apache-2.0"
  spec.homepage = "https://github.com/christopherwharrop-noaa/rocoto_actor"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"
end
