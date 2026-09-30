# frozen_string_literal: true

require_relative "support/broker_test_case"
class BrokerTellTest < BrokerTestCase
  def test_application_tell_is_delivered_in_order_with_asks_and_has_no_sender
    collector = @broker.spawn(CollectorActor, name: "collector")

    assert_nil collector.tell(op: :record, value: 1)
    collector.tell(op: :record, value: 2)

    assert_equal [[1, nil], [2, nil]], collector.ask(:messages).value(timeout: 2)
  end

  def test_actors_fan_out_with_tell_and_reply_through_the_sender_handle
    coordinator = @broker.spawn(CoordinatorActor, 3, name: "coordinator")

    assert_equal 3, coordinator.ask(:start).value(timeout: 2)
    wait_until { coordinator.ask(:results).value(timeout: 2).size == 3 }

    assert_equal coordinator.children.sort_by(&:id), coordinator.ask(:results).value(timeout: 2).sort_by(&:id)
  end

  def test_routed_ask_carries_the_sender_handle
    coordinator = @broker.spawn(CoordinatorActor, 1, name: "coordinator")
    worker = coordinator.children.first

    assert_nil coordinator.ask(:call_record).value(timeout: 3)

    assert_equal [[:via_call, coordinator.id]], worker.ask(:messages).value(timeout: 2)
  end

  def test_tell_and_call_from_one_actor_arrive_in_order
    sender = @broker.spawn(CollectorActor, name: "sender")
    target = @broker.spawn(CollectorActor, name: "target")

    assert_nil sender.ask(op: :tell_then_call, target: target).value(timeout: 3)

    assert_equal [[:told, sender.id], [:called, sender.id]], target.ask(:messages).value(timeout: 2)
  end

  def test_tell_to_a_stopped_actor_is_rejected_for_application_and_actors
    sender = @broker.spawn(CollectorActor, name: "sender")
    target = @broker.spawn(CollectorActor, name: "target")
    target.stop(force: true)

    assert_raises(RocotoActor::ActorStoppedError) { target.tell(op: :record, value: 1) }
    error = assert_raises(RocotoActor::RemoteError) do
      sender.ask(op: :tell_to, target: target, message: { op: :record, value: 1 }).value(timeout: 2)
    end
    assert_equal "RocotoActor::ActorStoppedError", error.remote_class
    assert_equal [], sender.ask(:messages).value(timeout: 2)
  end
end
