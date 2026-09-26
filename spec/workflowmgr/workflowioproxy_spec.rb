# frozen_string_literal: true

require 'spec_helper'
require 'fileutils'
require 'socket'
require 'tmpdir'
require 'workflowmgr/workflowioproxy'

# Stands in for the workflow database. The proxy uses it only to remember
# which filesystems have stopped answering.
class FakeDownpathDB
  attr_reader :added, :deleted, :discarded

  def initialize(initial = [])
    @downpaths = initial
    @added = []
    @deleted = []
    @discarded = 0
  end

  def load_downpaths
    @downpaths.dup
  end

  def add_downpaths(paths)
    @added.concat(paths)
    @downpaths.concat(paths)
  end

  def delete_downpaths(paths)
    @deleted.concat(paths)
    paths.each { |path| @downpaths.delete(path) }
  end

  # The real database is an actor, and a handle whose reply was abandoned
  # refuses every later call until the reply is discarded.
  def discard_pending!
    @discarded += 1
    true
  end
end

# A database that cannot be written to, the way an actor that has wedged or
# died behaves when asked.
class FailingWriteDB < FakeDownpathDB
  def add_downpaths(_paths)
    raise WorkflowMgr::Actor::ActorTimeout, 'the database actor did not answer'
  end

  def delete_downpaths(_paths)
    raise WorkflowMgr::Actor::ActorTimeout, 'the database actor did not answer'
  end
end

# A database that cannot even be read at startup.
class FailingLoadDB < FakeDownpathDB
  def load_downpaths
    raise WorkflowMgr::Actor::ActorTimeout, 'the database actor did not answer'
  end
end

# An io process that is gone as soon as it is started, which is what a node
# out of memory looks like from here.
class DeadIOActor
  def actor_pid
    -1
  end

  def stop!; end

  def public_send(*)
    raise WorkflowMgr::Actor::ActorUnavailable, 'the io process died again'
  end
end

RSpec.describe WorkflowMgr::WorkflowIOProxy do
  let(:dir) { Dir.mktmpdir('rocoto-io-spec-') }
  let(:file) { File.join(dir, 'data.txt') }
  let(:db_server) { FakeDownpathDB.new }
  let(:options) { Struct.new(:database).new('unused') }
  # Rocoto's configuration exposes this exact name, so a double is easier
  # here than a class defining a method Ruby style guides disallow.
  let(:config) { double(WorkflowIOServer: true) }
  # Resolved the same way the proxy resolves it, because a block recorded by
  # another host is deliberately ignored and these specs need theirs kept.
  let(:host) { Socket.getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3] }

  before do
    File.write(file, "hello\n")
  end

  after do
    FileUtils.remove_entry(dir, true)
  end

  # The hang specs stub IO_TIMEOUT down to a second so that a wedged call
  # gives up quickly. That same budget is then baked into the replacement
  # actor at spawn time, and a replacement has to fork+exec a fresh Ruby
  # interpreter and load workflowmgr inside it -- which does not reliably
  # finish within a second on a loaded machine. Recovery is given a real
  # budget instead, so these specs fail when isolation is broken rather than
  # when the machine happens to be busy.
  #
  # Nothing is given up by replacing explicitly here: that an actor is
  # replaced automatically, and answers, is asserted separately below under
  # the ordinary timeout.
  def with_recovery_budget(proxy)
    stub_const("#{described_class}::IO_TIMEOUT", 30)
    proxy.send(:replace_wedged_server)
  end

  # Freezes the io actor so that it cannot answer, whatever we do. A real
  # D-state hang cannot be conjured here, but SIGSTOP produces the property
  # that matters.
  def freeze_io(proxy)
    pid = proxy.instance_variable_get(:@server).actor_pid
    Process.kill('STOP', pid)
    pid
  end

  def thaw(pids)
    Array(pids).each do |pid|
      Process.kill('CONT', pid)
      Process.kill('KILL', pid)
    rescue Errno::ESRCH
      nil
    end
  end

  it 'answers filesystem questions from a process of its own' do
    proxy = described_class.new(db_server, config, options)

    expect(proxy.exist?(file)).to be true
    expect(proxy.read(file)).to eq("hello\n")
    expect(proxy.size(file)).to eq(6)
  ensure
    proxy&.stop!
  end

  it 'brings back file contents that are not valid text, byte for byte' do
    # A workflow document written in latin-1 is still a workflow document,
    # and rocoto reads every one of them through this proxy. DRb carried
    # such bytes, so nothing downstream expects to have to cope without them.
    File.binwrite(file, "<!-- caf\xE9 latin-1 -->\n")
    proxy = described_class.new(db_server, config, options)

    expect(proxy.read(file).bytes).to eq(File.binread(file).bytes)
  ensure
    proxy&.stop!
  end

  it 'does the work in this process when the io server is turned off' do
    proxy = described_class.new(db_server, double(WorkflowIOServer: false), options)

    expect(proxy.exist?(file)).to be true
    expect(proxy.instance_variable_get(:@server)).to be_a(WorkflowMgr::WorkflowIO)
  ensure
    proxy&.stop!
  end

  it 'blames the filesystem rather than the directory, and keeps serving others' do
    stub_const("#{described_class}::IO_TIMEOUT", 1)
    proxy = described_class.new(db_server, config, options)
    # Pinned rather than read from the machine, so this asserts the same
    # thing wherever it runs.
    allow(proxy).to receive(:mount_points).and_return(['/', dir])
    frozen = freeze_io(proxy)

    expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang)

    # The mount is what gets distrusted. Blaming the directory instead means
    # every sibling directory on the same wedged filesystem hangs the run
    # again in turn, each one costing another IO_TIMEOUT.
    expect(db_server.added.map { |entry| entry[:path] }).to eq([dir])

    # Anything on it is refused outright, without being touched.
    expect { proxy.read(File.join(dir, 'elsewhere.txt')) }.to raise_error(WorkflowMgr::WorkflowIOHang)

    with_recovery_budget(proxy)
    expect(proxy.exist?('/etc/hostname')).to be true
  ensure
    proxy&.stop!
    thaw(frozen)
  end

  it 'refuses only what is really on the blocked filesystem, not what merely starts the same' do
    proxy = described_class.new(db_server, config, options)
    proxy.instance_variable_set(:@blocks,
                                [{ path: '/scratch', downtime: Time.now, host: host, pid: 1 }])

    # A raw string-prefix test refuses /scratchX too, which is somebody
    # else's filesystem entirely, so one hang silently stops unrelated work.
    expect { proxy.exist?('/scratch/proj/f') }.to raise_error(WorkflowMgr::WorkflowIOHang)
    expect(proxy.exist?('/scratchX/f')).to be false
    expect(proxy.exist?('/scratch_archive/f')).to be false
  ensure
    proxy&.stop!
  end

  it 'blocks the path itself when the filesystem it lives on is the root one' do
    stub_const("#{described_class}::IO_TIMEOUT", 1)
    proxy = described_class.new(db_server, config, options)
    allow(proxy).to receive(:mount_points).and_return(['/'])
    frozen = freeze_io(proxy)

    expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang)

    # Blocking / would refuse the workflow database, the rocoto install and
    # every dependency at once. Remembering nothing instead is no better: the
    # path is then retried by every check, for this run and every run after,
    # each one paying IO_TIMEOUT before giving up. So the path itself is
    # what gets remembered.
    expect(db_server.added.map { |entry| entry[:path] }).to eq([file])
    expect(proxy.exist?('/etc/hostname')).to be true
  ensure
    proxy&.stop!
    thaw(frozen)
  end

  it 'records a relative path as itself, having no way to place it' do
    stub_const("#{described_class}::IO_TIMEOUT", 1)
    proxy = described_class.new(db_server, config, options)
    frozen = freeze_io(proxy)

    # Placing it means resolving it, and resolving touches the filesystem
    # that is currently hanging.
    Dir.chdir(dir) do
      expect { proxy.exist?('relfile.txt') }.to raise_error(WorkflowMgr::WorkflowIOHang)
    end

    expect(db_server.added.map { |entry| entry[:path] }).to eq(['relfile.txt'])
    with_recovery_budget(proxy)
    expect(proxy.exist?('/etc/hostname')).to be true
  ensure
    proxy&.stop!
    thaw(frozen)
  end

  it 'retests a block that is old enough to doubt, and lifts it when the filesystem answers' do
    stale = { path: dir, downtime: Time.now - (described_class::BLOCK_TTL + 60), host: host, pid: 424_242 }
    db = FakeDownpathDB.new([stale])
    proxy = described_class.new(db, config, options)
    allow(proxy).to receive(:answers?).and_return(true)

    expect(proxy.exist?(file)).to be true
    expect(db.deleted).to eq([stale])
  ensure
    proxy&.stop!
  end

  it 'retests a filesystem once per run, however many checks are refused' do
    stale = { path: dir, downtime: Time.now - (described_class::BLOCK_TTL + 60), host: host, pid: 424_242 }
    proxy = described_class.new(FakeDownpathDB.new([stale]), config, options)
    allow(proxy).to receive(:answers?).and_return(false)

    3.times { expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang) }

    # Each retest is a process that may itself wedge on the bad filesystem
    # and can never be killed, so one per run is the whole budget.
    expect(proxy).to have_received(:answers?).once
  ensure
    proxy&.stop!
  end

  it 'does not restart the clock on a block each time a check is refused' do
    downtime = Time.now - 60
    proxy = described_class.new(FakeDownpathDB.new, config, options)
    proxy.instance_variable_set(:@blocks, [{ path: dir, downtime: downtime, host: host, pid: 1 }])

    expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang)

    # Re-arming here keeps a block permanently young, so the filesystem is
    # never retested and never recovers.
    expect(proxy.instance_variable_get(:@blocks).first[:downtime]).to eq(downtime)
  ensure
    proxy&.stop!
  end

  it 'ignores a block another host recorded, which says nothing about this one' do
    elsewhere = { path: dir, downtime: Time.now, host: 'some.other.host', pid: 1 }
    proxy = described_class.new(FakeDownpathDB.new([elsewhere]), config, options)

    expect(proxy.exist?(file)).to be true
  ensure
    proxy&.stop!
  end

  it 'ignores an empty path left in the database by an older run' do
    # Older rocotos recorded "" for a hang on a top-level path. Honouring one
    # refuses every path there is, so it is dropped on sight.
    empty = { path: '', downtime: Time.now, host: host, pid: 1 }
    proxy = described_class.new(FakeDownpathDB.new([empty]), config, options)

    expect(proxy.instance_variable_get(:@blocks)).to be_empty
    expect(proxy.exist?(file)).to be true
  ensure
    proxy&.stop!
  end

  it 'carries on without the list of blocks when the database cannot be read' do
    # Losing it costs only the memory of what was wedged before, and the
    # first check to hang records it again. Ending the run over it would cost
    # far more, and a cron-driven rocoto would keep paying it.
    proxy = described_class.new(FailingLoadDB.new, config, options)

    expect(proxy.instance_variable_get(:@blocks)).to be_empty
    expect(proxy.exist?(file)).to be true
  ensure
    proxy&.stop!
  end

  it 'repairs the shared database handle when a write to it fails' do
    stub_const("#{described_class}::IO_TIMEOUT", 1)
    db = FailingWriteDB.new
    proxy = described_class.new(db, config, options)
    frozen = freeze_io(proxy)

    expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang)

    # Swallowing the error is only half of it. The database is an actor
    # shared with the rest of rocoto, and a timed-out call leaves a reply
    # outstanding on its handle, so every later call by anybody raises
    # ActorBusy -- the run still ends, just later and blaming something
    # unrelated. Discarding that reply is what actually restores it.
    expect(db.discarded).to eq(1)
  ensure
    proxy&.stop!
    thaw(frozen)
  end

  it 'replaces an io process that died and answers the call anyway' do
    proxy = described_class.new(db_server, config, options)
    first = proxy.instance_variable_get(:@server).actor_pid

    # Killed outright, the way an out-of-memory kill would.
    Process.kill('KILL', first)

    expect(proxy.exist?(file)).to be true
    expect(proxy.instance_variable_get(:@server).actor_pid).not_to eq(first)
  ensure
    proxy&.stop!
  end

  it 'refuses to touch the filesystem once it has been shut down' do
    proxy = described_class.new(db_server, config, options)
    proxy.stop!

    # Quietly spawning a fresh actor here would leave one running with
    # nobody owning it.
    expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang)
  end

  it 'reports an io process it cannot restart as a hang, which callers know how to survive' do
    proxy = described_class.new(db_server, config, options)
    Process.kill('KILL', proxy.instance_variable_get(:@server).actor_pid)

    # Every caller of this proxy rescues WorkflowIOHang and nothing else, so
    # anything else escaping here ends the run -- the opposite of the point.
    allow(WorkflowMgr::Actor).to receive(:spawn).and_raise(StandardError, 'cannot fork')

    expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang)
  ensure
    proxy&.stop!
  end

  it 'gives up as a hang when each replacement io process dies as fast as it is started' do
    proxy = described_class.new(db_server, config, options)
    Process.kill('KILL', proxy.instance_variable_get(:@server).actor_pid)
    allow(WorkflowMgr::Actor).to receive(:spawn).and_return(DeadIOActor.new)

    # One death earns a restart and a second try. A second is not worth
    # ending the run over either, so it costs this call and nothing more.
    expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang, /could not be restarted/)
  ensure
    proxy&.stop!
  end

  it 'records the hang against the process that actually hung' do
    stub_const("#{described_class}::IO_TIMEOUT", 1)
    proxy = described_class.new(db_server, config, options)
    frozen = freeze_io(proxy)

    expect { proxy.exist?(file) }.to raise_error(WorkflowMgr::WorkflowIOHang)

    # The pid recorded is the io actor's, not rocoto's: killing rocoto
    # instead would be a bad day.
    expect(db_server.added.first[:pid]).to eq(frozen)
    expect(db_server.added.first[:pid]).not_to eq(Process.pid)
  ensure
    proxy&.stop!
    thaw(frozen)
  end

  describe 'identifying the filesystem a path lives on' do
    let(:proxy) { described_class.allocate }

    it 'normalises a path lexically, because resolving it would touch the filesystem' do
      expect(proxy.send(:normalize, '/scratch/proj/rundir/')).to eq('/scratch/proj/rundir')
      expect(proxy.send(:normalize, '//a/b')).to eq('/a/b')
      expect(proxy.send(:normalize, '/a//b/../c')).to eq('/a/c')
      expect(proxy.send(:normalize, 'relative/file')).to be_nil
    end

    it 'unescapes mount points, which the kernel writes with octal escapes' do
      expect(proxy.send(:unescape, '/mnt/with\040space')).to eq('/mnt/with space')
    end

    it 'never names the root filesystem as the thing to blame' do
      allow(proxy).to receive(:mount_points).and_return(['/'])
      expect(proxy.send(:mount_for, '/anything/at/all')).to be_nil
    end

    it 'picks the most specific filesystem containing the path' do
      allow(proxy).to receive(:mount_points).and_return(['/', '/scratch', '/scratch/project'])

      expect(proxy.send(:mount_for, '/scratch/project/run/f')).to eq('/scratch/project')
      expect(proxy.send(:mount_for, '/scratch/other/f')).to eq('/scratch')
      # Not /scratch: it shares a prefix as text, but not as path components.
      expect(proxy.send(:mount_for, '/scratchX/f')).to be_nil
    end
  end
end
