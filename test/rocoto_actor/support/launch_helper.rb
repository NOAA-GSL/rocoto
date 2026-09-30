# frozen_string_literal: true

# Launches one actor process directly through the private launcher and waits
# for it to boot, for tests that exercise Reference without a broker.
module LaunchHelper
  LAUNCHER = RocotoActor.const_get(:Launcher)

  def spawn_actor(actor_class, *, start_timeout: RocotoActor::START_TIMEOUT, **)
    reference, boot = LAUNCHER.launch(actor_class, *, **)
    boot.value(timeout: start_timeout)
    reference
  rescue StandardError => error
    reference&.stop(force: true, timeout: 0)
    raise LAUNCHER.startup_error(reference, error)
  end
end
