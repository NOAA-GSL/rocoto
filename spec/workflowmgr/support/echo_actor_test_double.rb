# frozen_string_literal: true

# Tiny fixture class used only by actor_spec.rb to exercise WorkflowMgr::Actor
# without depending on any real, more complex served object.
class EchoActorTestDouble
  def initialize(name)
    @name = name
  end

  def greet(suffix = "")
    "hello #{@name}#{suffix}"
  end

  # Defined for this class's own sake, the way any class might define it for
  # logging. A handle answers to_s itself and never forwards it, but that is
  # no reason to refuse to serve the class -- if spawning this ever starts
  # failing, the shadowed-name check has been made too broad again.
  def to_s
    "echo actor for #{@name}"
  end

  def boom
    raise ArgumentError, "kaboom"
  end

  def nap(seconds)
    sleep(seconds)
    "awake"
  end

  # Raises an Errno error of its own, which must reach the caller as that
  # error rather than being mistaken for the actor itself failing.
  def read_missing_file
    File.read("/nonexistent/rocoto-actor-spec")
  end

  # A result that cannot be encoded as JSON: a String tagged as UTF-8 that
  # holds bytes which are not. Command output and file contents really do
  # contain such bytes.
  def bad_bytes
    "abc\xC3\x28".dup.force_encoding("UTF-8")
  end

  # Fails with an error whose message holds bytes that are not valid text,
  # the way Errno does when the path it names has a latin-1 byte in it.
  def read_file_named_with_bad_bytes
    File.read("/nonexistent/rocoto-actor-spec-caf\xE9")
  end

  # An object the codec has no way to carry, because it says nothing about
  # how to rebuild itself.
  def uncarryable
    Object.new
  end

  # A result far larger than a socket buffer, which can only be handed back
  # in pieces, as fast as the caller reads it.
  def big_payload(size)
    "x" * size
  end

  # The same, but with non-ASCII characters in it, where one character is
  # more than one byte and the difference between the two matters.
  def big_utf8_payload(count)
    "café " * count
  end

  # Reports what actually arrived, to check the outbound direction.
  def byte_count(text)
    text.bytesize
  end

  # An exception whose class has no name at all.
  def anonymous_boom
    raise Class.new(StandardError), "anonymous boom"
  end

  # A programming error rather than a runtime one: LoadError is not a
  # StandardError, so it stands in for broken code that must not keep
  # serving, but whose cause the caller still needs to be told.
  def bad_require
    require "no_such_library_for_rocoto_spec"
  end

  # Ends the actor process in the middle of a request.
  def die
    exit(1)
  end
end
