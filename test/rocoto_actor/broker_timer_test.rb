# frozen_string_literal: true

require_relative "support/broker_test_case"
class BrokerTimerTest < BrokerTestCase
  def test_actor_schedules_a_one_shot_message_to_itself
    ticker = @broker.spawn(TickerActor, name: "ticker")
    scheduled_at = ticker.ask(op: :clock).value(timeout: 2)

    ticker.ask(op: :schedule, name: :once, after: 0.2).value(timeout: 2)

    assert_empty ticker.ask(op: :ticks).value(timeout: 2)
    wait_until { ticker.ask(op: :ticks).value(timeout: 2).size == 1 }
    name, from_self, at = ticker.ask(op: :ticks).value(timeout: 2).first
    assert_equal :once, name
    assert from_self, "tick should arrive as a tell from the actor itself"
    assert_operator at - scheduled_at, :>=, 0.19
    sleep 0.3
    assert_equal 1, ticker.ask(op: :ticks).value(timeout: 2).size
  end

  def test_recurring_timer_fires_until_cancelled
    ticker = @broker.spawn(TickerActor, name: "ticker")
    ticker.ask(op: :schedule, name: :beat, every: 0.05).value(timeout: 2)

    wait_until { ticker.ask(op: :ticks).value(timeout: 2).size >= 4 }
    assert_equal true, ticker.ask(op: :cancel, name: :beat).value(timeout: 2)
    count = ticker.ask(op: :ticks).value(timeout: 2).size
    sleep 0.3

    assert_operator ticker.ask(op: :ticks).value(timeout: 2).size, :<=, count + 1 # at most one tick already in flight
    assert_equal false, ticker.ask(op: :cancel, name: :beat).value(timeout: 2)
  end

  def test_cancelled_before_firing_never_arrives
    ticker = @broker.spawn(TickerActor, name: "ticker")
    ticker.ask(op: :schedule, name: :later, after: 0.2).value(timeout: 2)

    assert_equal true, ticker.ask(op: :cancel, name: :later).value(timeout: 2)
    sleep 0.35

    assert_empty ticker.ask(op: :ticks).value(timeout: 2)
  end

  def test_timers_die_with_the_incarnation_and_initialize_can_reschedule
    ticker = @broker.spawn(TickerActor, :schedule_in_init, name: "ticker", restart: :on_failure,
                                                           restart_backoff: 0.01)
    ticker.ask(op: :schedule, name: :beat, every: 0.05).value(timeout: 2)
    wait_until { ticker.ask(op: :ticks).value(timeout: 2).map(&:first).include?(:boot) }

    assert_raises(RocotoActor::ActorStoppedError) { ticker.ask(op: :crash).value(timeout: 2) }
    wait_until { ticker.state == :running && ticker.generation == 2 }
    sleep 0.3
    ticks = ticker.ask(op: :ticks).value(timeout: 2).map(&:first)

    assert_equal [:boot], ticks, "only the new incarnation's own schedule should fire"
  end

  def test_undeliverable_ticks_are_reported_and_recurrence_continues
    reported = Queue.new
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    ticker = broker.spawn(TickerActor, name: "ticker", mailbox_size: 2)
    ticker.ask(op: :schedule, name: :beat, every: 0.05).value(timeout: 2)
    ticker.ask(op: :hang)
    ticker.ask(op: :fill, payload: "x" * (2 * 1024 * 1024)) # blocks the writer; later ticks fill the mailbox

    first = reported.pop(timeout: 5)
    assert_equal [RocotoActor::MailboxFullError, "scheduled tell to #{ticker.id}"], first
    second = reported.pop(timeout: 5)
    assert_equal first, second, "the recurring timer should keep trying"
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_schedule_arguments_are_validated_and_bounded
    ticker = @broker.spawn(TickerActor, name: "ticker")
    [{ after: nil, every: nil }, { after: -1, every: nil }, { after: nil, every: 0.001 }].each do |bad|
      error = assert_raises(RocotoActor::RemoteError, bad.inspect) do
        ticker.ask(op: :schedule, name: :bad, **bad).value(timeout: 2)
      end
      assert_equal "ArgumentError", error.remote_class, bad.inspect
    end
    RocotoActor::ActorBroker::MAX_TIMERS_PER_ACTOR.times do |index|
      ticker.ask(op: :schedule, name: :"t#{index}", after: 60).value(timeout: 2)
    end
    error = assert_raises(RocotoActor::RemoteError) { ticker.ask(op: :schedule, name: :extra, after: 60).value(timeout: 2) }
    assert_equal "RocotoActor::Error", error.remote_class
    assert_match(/100 timers/, error.remote_message)
  end

  def test_timer_returned_to_the_application_cannot_be_cancelled_there
    ticker = @broker.spawn(TickerActor, name: "ticker")
    ticker.ask(op: :schedule, name: :beat, every: 1).value(timeout: 2)

    timer = ticker.ask(op: :timer, name: :beat).value(timeout: 2)

    assert_kind_of RocotoActor::Timer, timer
    assert_raises(RocotoActor::Error) { timer.cancel }
    assert @broker.stop(timeout: 2)
  end
end
