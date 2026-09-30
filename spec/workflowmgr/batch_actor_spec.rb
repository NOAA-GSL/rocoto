# frozen_string_literal: true

require 'spec_helper'
require 'workflowmgr/batch_actor'

# Stands in for a batch system, so that nothing here talks to a scheduler.
class FakeBatchSystem
  attr_reader :submitted

  def initialize(answer = ['123.sched', 'submitted'])
    @answer = answer
    @submitted = []
  end

  def submit(task)
    @submitted << task
    @answer
  end

  def statuses(jobids)
    jobids.to_h do |id|
      [id, { jobid: id, state: 'RUNNING', native_state: 'R', start_time: Time.at(1_700_000_000).getgm }]
    end
  end

  def delete(jobid)
    "deleted #{jobid}"
  end
end

RSpec.describe 'the batch system served by an actor' do
  let(:cycle) { Time.at(1_700_000_000).getgm }

  describe WorkflowMgr::BatchWire do
    def round_trip(value)
      described_class.decode(described_class.encode(value))
    end

    # The transport carries only these, so an encoded value holding anything
    # else would be refused at the boundary rather than here.
    def carryable?(value)
      case value
      when Array then value.all? { |element| carryable?(element) }
      when Hash then value.all? { |key, element| carryable?(key) && carryable?(element) }
      when nil, true, false, String, Integer, Float, Symbol then true
      else false
      end
    end

    it 'leaves alone what the transport already carries' do
      plain = { name: 'foo', cores: 1, final: false, command: '/bin/true', list: [1, :two, nil] }

      expect(round_trip(plain)).to eq(plain)
    end

    it 'carries the Times a status record holds' do
      statuses = { '123' => { jobid: '123', state: 'RUNNING', start_time: cycle, end_time: cycle } }
      carried = round_trip(statuses)

      expect(carried['123'][:start_time]).to eq(cycle)
      expect(carried['123'][:start_time].utc?).to be true
      expect(carryable?(described_class.encode(statuses))).to be true
    end

    it 'carries scheduler output that is not valid text, byte for byte' do
      # qsub and qstat print whatever they please, and it is not always UTF-8.
      output = "submitted caf\xE9\n".dup.force_encoding('US-ASCII')
      encoded = described_class.encode(['123', output])

      # Both halves matter, and in this order: a round trip alone passes even
      # if encode does nothing at all, because decode leaves a plain string
      # alone and the bytes never left this process. What has to be true is
      # that the *encoded* form holds nothing the transport would refuse.
      expect(carryable?(encoded)).to be true
      expect(encoded.last).not_to eq(output)
      expect(described_class.decode(encoded)[1].bytes).to eq(output.bytes)
    end
  end

  describe WorkflowMgr::BatchActor do
    def build_actor(answer = ['123.sched', 'submitted'], dryrun: false)
      actor = described_class.new('pbspro', 2, 45, 45, dryrun: dryrun)
      actor.instance_variable_set(:@batchsystem, FakeBatchSystem.new(answer))
      actor
    end

    it 'refuses anything that is not a batch system operation' do
      expect { build_actor.receive({ op: :not_a_batch_call, args: [] }) }
        .to raise_error(NoMethodError, /not a batch system operation/)
    end

    it 'submits and then hands back that submission’s answer' do
      actor = build_actor
      actor.submit_parts({ name: 'foo', command: '/bin/true' }, { 'A' => '1' }, ['#PBS -l x'], cycle)

      expect(actor.get_submit_status('foo', cycle)).to eq(['123.sched', 'submitted'])
    end

    it 'answers a second time without waiting again' do
      actor = build_actor
      actor.submit_parts({ name: 'foo' }, {}, [], cycle)
      actor.get_submit_status('foo', cycle)

      # The queue is emptied by the first answer, so a second wait would
      # block forever rather than repeat it.
      expect(actor.get_submit_status('foo', cycle)).to eq(['123.sched', 'submitted'])
    end

    it 'answers for a job it was never asked to submit, rather than waiting' do
      expect(build_actor.get_submit_status('never-submitted', cycle)).to eq([nil, nil])
    end

    it 'rebuilds the task from the three plain things a batch system reads' do
      actor = build_actor
      actor.submit_parts({ name: 'foo', command: '/bin/true' }, { 'START' => '2013' }, ['#PBS -l walltime=1'], cycle)
      actor.get_submit_status('foo', cycle)
      task = actor.instance_variable_get(:@batchsystem).submitted.first

      expect(task.attributes[:command]).to eq('/bin/true')
      expect(task.envars).to eq({ 'START' => '2013' })
      natives = []
      task.each_native { |native| natives << native }
      expect(natives).to eq(['#PBS -l walltime=1'])
    end

    it 'submits without a thread pool in a dryrun' do
      # Pool workers sleep waiting for work that a dryrun never produces.
      actor = build_actor([nil, 'This is a dryrun'], dryrun: true)
      actor.submit_parts({ name: 'foo' }, {}, [], cycle)

      expect(actor.instance_variable_get(:@pool)).to be_nil
      expect(actor.get_submit_status('foo', cycle)).to eq([nil, 'This is a dryrun'])
    end

    # The engine holds a BatchActor directly when BatchQueueServer is false and
    # a BatchProxy otherwise, and calls submit(task, cycle) on whichever it has.
    # Nothing pinned that, so the two drifted: the actor kept only the wire
    # signature, every submission on the in-process path raised ArgumentError,
    # and the engine reported it at a verbosity the integration specs had turned
    # off before exiting 1. The whole failure looked like an empty test run.
    it 'takes a Task from a caller, the way the proxy does' do
      actor = build_actor
      task = WorkflowMgr::Task.new(0, { name: 'foo', command: '/bin/true' }, { 'A' => '1' }, :a_dependency, nil)
      task.add_native('#PBS -l x')

      actor.submit(task, cycle)

      expect(actor.get_submit_status('foo', cycle)).to eq(['123.sched', 'submitted'])
      submitted = actor.instance_variable_get(:@batchsystem).submitted.first
      expect(submitted.attributes[:command]).to eq('/bin/true')
      expect(submitted.envars).to eq({ 'A' => '1' })
    end

    it 'agrees with the proxy about what submit takes' do
      # The drift above is invisible until something calls the one the engine
      # does not hold, so compare them directly rather than trusting both.
      expect(described_class.instance_method(:submit).arity)
        .to eq(WorkflowMgr::BatchProxy.instance_method(:submit).arity)
    end

    it 'answers the stop! the engine cleans up with' do
      actor = build_actor
      task = WorkflowMgr::Task.new(0, { name: 'foo', command: '/bin/true' }, {}, nil, nil)
      actor.submit(task, cycle)
      actor.get_submit_status('foo', cycle)

      # That ensure block calls stop! only if the object answers it. An
      # in-process actor that does not leaves its submission pool running, with
      # workers waiting for work that will never arrive.
      expect(actor).to respond_to(:stop!)
      expect { actor.stop! }.not_to raise_error
    end
  end

  describe WorkflowMgr::BatchProxy do
    # Records what the proxy sends, so the extraction can be checked without
    # starting a process.
    let(:handle) do
      Class.new do
        attr_reader :asked

        def ask(message)
          @asked = message
          Class.new { def value(timeout: nil) = WorkflowMgr::BatchWire.encode(nil) }.new
        end
      end.new
    end

    it 'sends a task as its attributes, envars and natives, and nothing else' do
      task = WorkflowMgr::Task.new(0, { name: 'foo', command: '/bin/true' }, { 'A' => '1' }, :a_dependency, nil)
      task.add_native('#PBS -l x')
      described_class.new(handle).submit(task, cycle)

      # The dependency trees are the part that cannot be serialised, and a
      # submission has no use for them.
      attributes, envars, natives, sent_cycle = WorkflowMgr::BatchWire.decode(handle.asked[:args])
      expect(handle.asked[:op]).to eq(:submit_parts)
      expect(attributes[:command]).to eq('/bin/true')
      expect(envars).to eq({ 'A' => '1' })
      expect(natives).to eq(['#PBS -l x'])
      expect(sent_cycle).to eq(cycle)
    end

    it 'serves what the actor serves, and nothing else' do
      proxy = described_class.new(handle)

      expect(proxy).to respond_to(:submit, :get_submit_status, :statuses, :delete)
      expect { proxy.not_a_batch_call }.to raise_error(NoMethodError)
    end
  end
end
