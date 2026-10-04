# frozen_string_literal: true

require_relative "rocoto_actor/version"
require_relative "rocoto_actor/errors"
require_relative "rocoto_actor/error_reporting"
require_relative "rocoto_actor/threads"
require_relative "rocoto_actor/protocol"
require_relative "rocoto_actor/handle"
require_relative "rocoto_actor/timer"
require_relative "rocoto_actor/decode_bindings"
require_relative "rocoto_actor/future"

# RocotoActor runs each actor in its own operating-system process, behind a Unix
# socket pair, so that a blocked or crashed actor cannot take the application
# down with it. See docs/rocoto_actor/architecture.md for the process, thread
# and lock model, and docs/rocoto_actor/REVIEW.md for the concurrency
# invariants a change here has to preserve.
module RocotoActor
  # How often a watchdog checks whether the application that owns it is still
  # alive, in seconds.
  PARENT_CHECK_INTERVAL = 0.1

  # How long an actor is given to boot before its spawn is abandoned.
  START_TIMEOUT = 5

  # Bounds on an actor's outbound mailbox: a count of messages, and a total
  # payload size. Reaching either rejects rather than blocks the caller.
  DEFAULT_MAILBOX_SIZE = 1_000
  DEFAULT_MAILBOX_BYTES = 16 * 1024 * 1024
end
