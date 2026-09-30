# frozen_string_literal: true

require 'spec_helper'
require 'fileutils'
require 'tmpdir'
require 'workflowmgr/database_actor'

# Stands in for the reply an actor sends back.
class CannedFuture
  def initialize(value)
    @value = value
  end

  def value(timeout: nil)
    @value
  end
end

# Stands in for the actor handle, recording what it was asked so the proxy's
# half of the conversion can be checked without starting a process.
class RecordingHandle
  attr_reader :asked

  def initialize(reply)
    @reply = reply
  end

  def ask(message)
    @asked = message
    CannedFuture.new(@reply)
  end
end

RSpec.describe 'the workflow database served by an actor' do
  let(:cycle_time) { Time.at(1_700_000_000).getgm }

  describe WorkflowMgr::DatabaseWire do
    # The shapes here are the database's real return values, not invented
    # ones: a Time bare, a Time among plain fields, and a Time used as a
    # hash key, which is how load_jobs returns its jobs.
    def round_trip(value)
      described_class.decode(described_class.encode(value))
    end

    # What the transport will carry, and nothing else.
    def carryable?(value)
      case value
      when Array then value.all? { |element| carryable?(element) }
      when Hash then value.all? { |key, element| carryable?(key) && carryable?(element) }
      when nil, true, false, String, Integer, Float, Symbol then true
      else false
      end
    end

    it 'encodes into values the transport can actually carry' do
      # A round trip alone proves nothing here: encode and decode run in this
      # process, so doing nothing at all passes every one of the examples
      # below. What has to be true is that the encoded form contains no Time,
      # Job or Cycle, because those are what the transport refuses.
      job = WorkflowMgr::Job.new('7', 'post', cycle_time, 1, 'QUEUED', 'Q', 0, 0, 0, 0.0)
      cycle = WorkflowMgr::Cycle.new(cycle_time)
      encoded = described_class.encode({ 'post' => { cycle_time => job },
                                         cycles: [cycle], when: cycle_time })

      expect(carryable?(encoded)).to be true
    end

    it 'leaves alone everything the transport already carries' do
      plain = { group: 'g1', cycledef: '2023 2024 01:00:00', activation_offset: -3600, position: nil,
                list: [1, 2.5, 'three', :four, true, nil] }

      expect(round_trip(plain)).to eq(plain)
    end

    it 'carries a Time as UTC seconds, which is exactly what the database stores' do
      carried = round_trip(cycle_time)

      expect(carried).to eq(cycle_time)
      expect(carried.utc?).to be true
    end

    it 'carries a Job by the fields it declares' do
      job = WorkflowMgr::Job.new('42.pbs', 'foo', cycle_time, 8, 'RUNNING', 'R', 0, 1, 0, 12.5)
      carried = round_trip(job)

      expect(carried).to be_a(WorkflowMgr::Job)
      expect([carried.id, carried.task, carried.cycle, carried.cores]).to eq(['42.pbs', 'foo', cycle_time, 8])
      expect([carried.state, carried.native_state, carried.tries, carried.duration]).to eq(['RUNNING', 'R', 1, 12.5])
    end

    it 'carries a Cycle and lets its constructor derive the state, as it always does' do
      activated = Time.at(1_700_000_100).getgm
      zero = Time.at(0).getgm
      cycle = WorkflowMgr::Cycle.new(cycle_time,
                                     { activated: activated, expired: zero, done: zero, draining: zero })
      carried = round_trip(cycle)

      expect(carried.cycle).to eq(cycle_time)
      expect(carried.activated).to eq(activated)
      expect(carried.state).to eq(:active)
    end

    it 'carries the shape load_jobs returns, where a Time is a hash key' do
      job = WorkflowMgr::Job.new('7', 'post', cycle_time, 1, 'QUEUED', 'Q', 0, 0, 0, 0.0)
      carried = round_trip({ 'post' => { cycle_time => job } })

      # A key is as easy to lose as a value, and losing this one would make
      # every job look as though it belonged to a cycle that does not exist.
      expect(carried['post'].keys).to eq([cycle_time])
      expect(carried['post'][cycle_time].id).to eq('7')
    end

    it 'carries the shapes that hold a Time among plain fields' do
      cycledefs = [{ group: 'g1', cycledef: 'x', activation_offset: 0, position: cycle_time }]
      downpaths = [{ path: '/scratch', downtime: cycle_time, host: 'node1', pid: 7 }]

      expect(round_trip(cycledefs).first[:position]).to eq(cycle_time)
      expect(round_trip(downpaths).first[:downtime]).to eq(cycle_time)
      expect(round_trip(downpaths).first[:pid]).to eq(7)
    end
  end

  describe WorkflowMgr::DatabaseActor do
    it 'refuses anything that is not a database operation' do
      # The proxy keeps its own list, but this is the guard that holds when a
      # message arrives from somewhere the proxy did not build it: another
      # actor, or a bug. Constructing the database opens nothing.
      dir = Dir.mktmpdir('rocoto-dbactor-spec-')
      actor = described_class.new(File.join(dir, 'workflow.db'), Process.pid)

      expect { actor.receive({ op: :not_a_database_call, args: [] }) }
        .to raise_error(NoMethodError, /not a database operation/)
    ensure
      FileUtils.remove_entry(dir, true)
    end
  end

  describe WorkflowMgr::DatabaseProxy do
    it 'serves exactly what the database class defines, and nothing else' do
      proxy = described_class.new(RecordingHandle.new(nil))

      # Taken from the database class rather than listed here, so the two
      # cannot drift apart as it grows methods.
      expect(proxy).to respond_to(:load_jobs, :lock_workflow, :dbopen)
      expect(proxy).not_to respond_to(:create_tables)
      expect { proxy.not_a_database_call }.to raise_error(NoMethodError)
    end

    it 'refuses a block, which would be dropped on the way across' do
      proxy = described_class.new(RecordingHandle.new(nil))

      expect { proxy.load_jobs { :ignored } }.to raise_error(ArgumentError, /block/)
    end

    it 'converts the arguments it sends and the answer it receives' do
      handle = RecordingHandle.new(WorkflowMgr::DatabaseWire.encode([cycle_time]))
      proxy = described_class.new(handle)

      expect(proxy.load_cycles([cycle_time])).to eq([cycle_time])
      expect(handle.asked[:op]).to eq(:load_cycles)
      expect(WorkflowMgr::DatabaseWire.decode(handle.asked[:args])).to eq([[cycle_time]])
    end
  end
end
