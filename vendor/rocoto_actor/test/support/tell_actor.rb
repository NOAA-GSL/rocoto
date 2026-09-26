# frozen_string_literal: true

# Records told and asked messages with their sender, and exercises reply and
# ordering patterns built on tell.
class CollectorActor
  def initialize
    @messages = []
  end

  def receive(message)
    return @messages if message == :messages

    case message.fetch(:op)
    when :record then @messages << [message[:value], RocotoActor.context.sender&.id]
    when :ping then RocotoActor.context.sender.tell(op: :pong, from: RocotoActor.context.handle)
    when :boom then raise "told to fail"
    when :binary_boom then raise "bad input: \xFF".b
    when :tell_then_call
      target = message.fetch(:target)
      target.tell(op: :record, value: :told)
      target.call({ op: :record, value: :called }, timeout: 2)
      :done
    when :tell_to then message.fetch(:target).tell(message.fetch(:message))
    end
    nil
  end
end

# Fans work out to collectors with tell and gathers their pong replies.
class CoordinatorActor
  def initialize(worker_count)
    @workers = Array.new(worker_count) { |index| RocotoActor.context.spawn(CollectorActor, name: "w#{index}") }
    @results = []
  end

  def receive(message)
    case message
    when :start
      @workers.each { |worker| worker.tell(op: :ping) }
      @workers.size
    when :results then @results
    when :call_record then @workers.first.call({ op: :record, value: :via_call }, timeout: 2)
    when Hash then @results << message[:from] if message[:op] == :pong
    end
  end
end
