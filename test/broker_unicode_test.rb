# frozen_string_literal: true

require_relative "support/broker_test_case"

# Multi-byte text travelling every direction it can: application to actor and
# back, actor to actor through the broker, constructor arguments, names and
# paths, error messages, scheduled messages, and lifecycle events.
#
# Frame length matters as much as content. A frame's length header is a binary
# string and a multi-byte payload is UTF-8, so only payloads whose header holds
# a byte above 0x7F ever exercised the incompatibility these tests guard
# against. A single short sample passes either way, so each case sends several
# lengths that straddle those boundaries.
class BrokerUnicodeTest < BrokerTestCase
  SAMPLE = "café naïve — ☕ 日本語 🧪"
  SAMPLES = [1, 2, 3, 5, 8].map { |repetitions| SAMPLE * repetitions }.freeze

  def test_ask_and_reply_carry_multibyte_text
    SAMPLES.each do |text|
      assert_equal "database: #{text}", @database.ask(text).value(timeout: 5)
    end
  end

  def test_tell_carries_multibyte_text
    collector = @broker.spawn(CollectorActor, name: "collector")

    SAMPLES.each { |text| collector.tell(op: :record, value: text) }

    wait_until { collector.ask(:messages).value(timeout: 5).size == SAMPLES.size }
    assert_equal SAMPLES, collector.ask(:messages).value(timeout: 5).map(&:first)
  end

  def test_a_brokered_ask_between_actors_carries_multibyte_text
    SAMPLES.each do |text|
      assert_equal "database: #{text}", @worker.ask(message: text, timeout: 5).value(timeout: 5)
    end
  end

  def test_a_brokered_tell_between_actors_carries_multibyte_text
    sender = @broker.spawn(CollectorActor, name: "sender")
    receiver = @broker.spawn(CollectorActor, name: "receiver")

    SAMPLES.each { |text| sender.tell(op: :tell_to, target: receiver, message: { op: :record, value: text }) }

    wait_until { receiver.ask(:messages).value(timeout: 5).size == SAMPLES.size }
    assert_equal SAMPLES, receiver.ask(:messages).value(timeout: 5).map(&:first)
  end

  def test_constructor_arguments_carry_multibyte_text
    SAMPLES.first(2).each_with_index do |text, index|
      actor = @broker.spawn(ExampleActor, text, name: "ctor#{index}")

      assert_equal "#{text}: ok", actor.ask("ok").value(timeout: 5)
    end
  end

  def test_actor_names_and_paths_carry_multibyte_text
    parent = @broker.spawn(ExampleActor, "p", name: "aktør-café")
    child = @broker.spawn(ExampleActor, "c", name: "enfant-née", parent: parent)

    assert_equal "aktør-café", parent.path
    assert_equal "aktør-café/enfant-née", child.path
    assert_equal [child], parent.children
  end

  def test_a_raised_multibyte_message_arrives_as_a_remote_error
    SAMPLES.each do |text|
      error = assert_raises(RocotoActor::RemoteError) { @database.ask(raise: text).value(timeout: 5) }

      assert_equal "ArgumentError", error.remote_class
      assert_equal text, error.remote_message
    end
  end

  def test_a_scheduled_message_carries_multibyte_text
    ticker = @broker.spawn(TickerActor, name: "ticker")
    text = SAMPLES.last

    ticker.ask(op: :schedule, name: text, after: 0.02).value(timeout: 5)

    wait_until { ticker.ask(op: :ticks).value(timeout: 5).any? }
    assert_equal text, ticker.ask(op: :ticks).value(timeout: 5).first.first
  end

  # The reason travels from the dead actor's error frame, through the broker,
  # to a watching actor on the event thread.
  def test_a_lifecycle_event_reason_carries_multibyte_text
    text = SAMPLES.last
    watcher = @broker.spawn(WatcherActor, name: "watcher")
    target = @broker.spawn(ExampleActor, "t", name: "target")
    watcher.ask(op: :watch, handle: target).value(timeout: 5)

    target.tell(raise: text) # an unhandled exception in a told message ends the actor

    wait_until { watcher.ask(op: :events).value(timeout: 5).any? }
    event = watcher.ask(op: :events).value(timeout: 5).first
    assert_equal :failed, event[:event]
    assert_equal "ArgumentError: #{text}", event[:reason]
  end
end
