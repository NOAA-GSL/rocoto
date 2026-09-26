# frozen_string_literal: true

require "socket"
require "rbconfig"

require_relative "rocoto_actor/version"
require_relative "rocoto_actor/errors"
require_relative "rocoto_actor/handle"
require_relative "rocoto_actor/timer"
require_relative "rocoto_actor/broker_client"
require_relative "rocoto_actor/context"
require_relative "rocoto_actor/transport"
require_relative "rocoto_actor/future"
require_relative "rocoto_actor/reference"
require_relative "rocoto_actor/launcher"
require_relative "rocoto_actor/broker"

# Actors are created only through RocotoActor::ActorBroker in the application
# process, or through RocotoActor.context inside an actor.
#
# Public API: ActorBroker, ActorHandle, ActorContext, Timer, Future, ExitStatus,
# the error classes, and the module functions below. Reference, Transport,
# BrokerClient, Launcher, and Runner are private constants.
module RocotoActor
  PARENT_CHECK_INTERVAL = 0.1
  START_TIMEOUT = 5
  DEFAULT_MAILBOX_SIZE = 1_000
  DEFAULT_MAILBOX_BYTES = 16 * 1024 * 1024

  module_function

  # True inside an actor process; decoded handles then bind to the socket.
  def worker_process?
    @worker_process == true
  end

  def worker_process!
    @worker_process = true
  end

  # The actor's ActorContext; nil in the application process.
  def context
    @context
  end

  def context=(context)
    @context = context
  end

  def broker_client(socket)
    @broker_clients ||= {}.compare_by_identity
    @broker_clients[socket] ||= BrokerClient.new(socket)
  end
end
