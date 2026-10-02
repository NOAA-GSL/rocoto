# frozen_string_literal: true

require 'spec_helper'
require 'rocoto_actor'

# Threads is a private constant, reached the way the library's own tests reach
# its internals. Describing RocotoActor::Threads directly would raise NameError.
RSpec.describe 'RocotoActor::Threads' do
  let(:threads) { RocotoActor.const_get(:Threads) }

  it 'runs the block on a new thread and returns it' do
    queue = Queue.new
    thread = threads.start('example') { queue << Thread.current }

    expect(thread).to be_a(Thread)
    expect(queue.pop).to equal(thread)
    thread.join
  end

  it 'names the thread so it can be identified in a crash dump' do
    thread = threads.start('reader-123') { :done }
    thread.join

    expect(thread.name).to eq('rocoto-actor-reader-123')
  end

  # A loop that reports its own errors would otherwise have them printed twice,
  # so quiet: true is passed for those and must not become the default.
  it 'silences exception reporting only when asked' do
    quiet = threads.start('quiet', quiet: true) { :done }
    loud = threads.start('loud') { :done }
    [quiet, loud].each(&:join)

    expect(quiet.report_on_exception).to be(false)
    expect(loud.report_on_exception).to be(true)
  end

  # Ruby raises ThreadError both when the user's RLIMIT_NPROC is exhausted and
  # for lock misuse, so a caller cannot tell the two apart. The library
  # translates the one it means into ResourceLimitError and names the thread
  # that could not be created, since that is the only clue to which part of the
  # broker failed to start.
  it 'translates a thread that cannot be created into ResourceLimitError' do
    allow(Thread).to receive(:new).and_raise(ThreadError, 'cannot create Thread')

    expect { threads.start('reader-7') { :never } }
      .to raise_error(RocotoActor::ResourceLimitError, /reader-7 thread: cannot create Thread/)
  end
end
