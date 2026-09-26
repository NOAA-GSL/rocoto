# frozen_string_literal: true

require_relative "lib/rocoto_actor/version"

Gem::Specification.new do |spec|
  spec.name = "rocoto_actor"
  spec.version = RocotoActor::VERSION
  spec.summary = "Process-isolated actors for Ruby"
  spec.description = "Runs Ruby actors in isolated processes over Unix socket pairs."
  spec.authors = ["Christopher W. Harrop"]
  spec.files = Dir["lib/**/*.rb", "README.md"]
  spec.require_paths = ["lib"]
  spec.required_ruby_version = ">= 3.2"
  spec.metadata["rubygems_mfa_required"] = "true"
end
