# frozen_string_literal: true

require_relative "spec_helper"
require "fileutils"
require "open3"
require "timeout"
require "tmpdir"

describe "Seen concurrency" do
  before do
    @dir = Dir.mktmpdir("seen-concurrency")
    10.times do |i|
      subdir = File.join(@dir, "dir#{i}")
      Dir.mkdir(subdir)
      20.times { |j| File.write(File.join(subdir, "file#{j}.txt"), "needle\n") }
    end
  end

  after do
    FileUtils.remove_entry(@dir)
  end

  # Retry in case a fast search finishes before the ticker starts.
  def ticks_during
    5.times do
      ticks = 0
      thread = Thread.new { loop { ticks += 1 } }
      sleep 0.01 while ticks.zero?
      before = ticks
      yield
      during = ticks - before
      thread.kill
      thread.join
      return during if during.positive?
    end

    0
  end

  def assert_independent_enumerations(results, expected_count)
    started = Queue.new
    resume = Queue.new
    first = Thread.new do
      count = 0
      results.each do
        count += 1
        if count == 1
          started << true
          resume.pop
        end
      end
      count
    end

    started.pop
    second_count = results.count
    resume << true

    assert_equal expected_count, second_count
    assert_equal expected_count, first.value
  ensure
    if first&.alive?
      resume << true
      first.kill
      first.join
    end
  end

  def count_external(results)
    count = 0
    loop do
      results.next
      count += 1
    end
    count
  end

  def scheduler_probe
    Class.new do
      attr_reader :blocking_operations, :cooperative_yields, :io_waits

      def initialize
        @blocking_operations = 0
        @cooperative_yields = 0
        @io_waits = 0
      end

      def block(*) = false
      def unblock(*) = nil
      def close = nil

      def kernel_sleep(duration = nil)
        @cooperative_yields += 1 if duration == 0
        0
      end

      def io_wait(io, events, timeout = nil)
        @io_waits += 1
        IO.select([io], nil, nil, timeout)
        events
      end

      def fiber(&block)
        Fiber.new(blocking: false, &block).tap(&:resume)
      end

      def fiber_interrupt(fiber, exception)
        fiber.raise(exception)
      end

      def blocking_operation_wait(operation)
        @blocking_operations += 1
        worker = Thread.new { operation.call }
        Thread.pass while worker.alive?
        worker.value
      end
    end.new
  end

  def slow_line_results
    path = File.join(@dir, "large.txt")
    chunk = "haystack\n" * (1024 * 1024 / 9)
    File.open(path, "wb") { |file| 16.times { file.write(chunk) } }
    pattern = "(?i:(?:ha|hay|hays|haystac)+z)"

    Seen.each_line(pattern:, paths: [path])
  end

  def forked_iteration_error(results)
    reader, writer = IO.pipe
    results.next
    sleep 0.05
    pid = fork do
      reader.close
      begin
        loop { results.next }
      rescue => error
        writer.write("#{error.class}: #{error.message}")
      ensure
        writer.close
      end
      exit! 0
    end
    writer.close
    _, status = Timeout.timeout(5) { Process.waitpid2(pid) }

    assert_predicate status, :success?
    reader.read
  ensure
    if pid && !status
      begin
        Process.kill("KILL", pid)
        Process.waitpid(pid)
      rescue Errno::ESRCH, Errno::ECHILD
      end
    end
    results.rewind
    reader&.close unless reader&.closed?
    writer&.close unless writer&.closed?
  end

  def rss_after_abandoning(path, method, count)
    script = <<~'RUBY'
      require "seen"

      path, method, count = ARGV
      Integer(count).times do
        results = Seen.each_line(pattern: "needle", paths: [path], no_ignore: true)
        results.public_send(method)
        results = nil
        4.times do
          GC.start(full_mark: true, immediate_sweep: true)
          GC.compact
        end
      end

      rss = if File.readable?("/proc/self/status")
        File.read("/proc/self/status")[/^VmRSS:\s+(\d+)/, 1].to_i
      else
        IO.popen(["ps", "-o", "rss=", "-p", Process.pid.to_s], &:read).to_i
      end
      puts rss
    RUBY
    output, status = Open3.capture2(
      {"RUBYOPT" => nil, "RUBYLIB" => nil},
      Gem.ruby,
      "-I#{File.expand_path("../lib", __dir__)}",
      "-e",
      script,
      path,
      method.to_s,
      count.to_s
    )

    assert_predicate status, :success?
    Integer(output)
  end

  def assert_rewind_releases_resources(kind, wrapper = "direct")
    path = File.join(@dir, "rewind.txt")
    File.write(path, "needle\n" * 100_000)
    script = <<~'RUBY'
      require "seen"
      require "timeout"

      def resources
        threads = if File.directory?("/proc/self/task")
          Dir.children("/proc/self/task").size
        else
          IO.popen(["ps", "-M", "-p", Process.pid.to_s], &:read).lines.size - 1
        end
        [threads, Dir.children("/dev/fd").size]
      end

      path, kind, wrapper = ARGV
      source = if kind == "path"
        Seen.each_path(paths: Array.new(100_000, path), no_ignore: true)
      else
        Seen.each_line(pattern: "needle", paths: [path])
      end
      results = case wrapper
      when "direct" then source
      when "with_index" then source.with_index
      when "with_object" then source.with_object(nil)
      when "lazy" then source.lazy.map { |value| value }.select { true }
      when "to_enum" then source.to_enum
      when "chain" then source.chain([]).to_enum
      end
      baseline = resources
      20.times do
        results.next
        results.rewind
        GC.start
      end
      begin
        Timeout.timeout(5) do
          sleep 0.01 until resources.zip(baseline).all? { |actual, before| actual <= before + 2 }
        end
      rescue Timeout::Error
        abort "#{kind}/#{wrapper}: threads/fds before=#{baseline.inspect}, after=#{resources.inspect}"
      end
      abort "rewind did not restart" unless results.next
      results.rewind
      puts "released #{kind}/#{wrapper} workers and descriptors"
    RUBY
    output, status = Open3.capture2e(
      {"RUBYOPT" => nil, "RUBYLIB" => nil}, Gem.ruby,
      "-I#{File.expand_path("../lib", __dir__)}", "-e", script, path, kind, wrapper
    )

    assert_predicate status, :success?, output
  end

  it "releases the GVL so other threads run during search" do
    during = ticks_during { Seen.each_path(paths: [@dir], hidden: true).to_a }
    assert_predicate during, :positive?, "other threads should run during Seen.each_path"
  end

  it "releases the GVL so other threads run during grep" do
    during = ticks_during { Seen.each_line(pattern: "needle", paths: [@dir]).to_a }
    assert_predicate during, :positive?, "other threads should run during Seen.each_line"
  end

  it "waits through scheduler-visible IO readiness" do
    scheduler = scheduler_probe
    Fiber.set_scheduler(scheduler)
    results = nil
    Fiber.schedule { results = Seen.each_path(paths: [@dir], type: "f").to_a }
    Fiber.set_scheduler(nil)

    assert_equal 200, results.length
    assert_predicate scheduler.io_waits, :positive?
    assert_equal 0, scheduler.blocking_operations
  ensure
    Fiber.set_scheduler(nil) if scheduler && Fiber.scheduler.equal?(scheduler)
  end

  it "cooperatively yields while scheduler results stay ready" do
    path = File.join(@dir, "dense.txt")
    File.binwrite(path, "needle\n" * 2048)
    scheduler = scheduler_probe
    Fiber.set_scheduler(scheduler)
    count = nil
    Fiber.schedule { count = Seen.each_line(pattern: "needle", paths: [path]).count }
    Fiber.set_scheduler(nil)

    assert_equal 2048, count
    assert_equal 2, scheduler.cooperative_yields
  ensure
    Fiber.set_scheduler(nil) if scheduler && Fiber.scheduler.equal?(scheduler)
  end

  it "can be interrupted by Timeout during search" do
    results = Seen.each_path(paths: Array.new(10_000, @dir), hidden: true)

    assert_raises(Timeout::Error) do
      Timeout.timeout(0.01) { results.count }
    end

    assert_kind_of String, results.first
    assert_equal 200, Seen.each_path(paths: [@dir], type: "f").count
  end

  it "cancels workers when consumption stops early" do
    Timeout.timeout(5) do
      100.times { assert_kind_of String, Seen.each_path(paths: [@dir], hidden: true).first }
    end
  end

  it "releases native path workers and descriptors after rewind" do
    assert_rewind_releases_resources("path")
  end

  it "releases native line workers and descriptors after rewind" do
    assert_rewind_releases_resources("line")
  end

  %w[with_index with_object lazy to_enum chain].each do |wrapper|
    %w[path line].each do |kind|
      it "releases native #{kind} resources after #{wrapper} rewind" do
        assert_rewind_releases_resources(kind, wrapper)
      end
    end
  end

  it "keeps composed iterations independent when one is rewound" do
    [Seen.each_path(paths: [@dir], type: "f"), Seen.each_line(pattern: "needle", paths: [@dir])].each do |source|
      left = source.lazy
      right = source.with_index
      left.next
      assert_equal 0, right.next.last
      left.rewind
      GC.start

      assert_equal 199, count_external(right)
      assert_equal 200, count_external(left)
    end
  end

  it "keeps native cursor state alive across GC inside a yielded block" do
    path = File.join(@dir, "compact.txt")
    File.write(path, "needle " * 40)
    texts = []

    Seen.each_line(pattern: "needle", paths: [path], column: true) do |_, _, _, text|
      GC.start
      GC.compact
      texts << text
    end

    assert_equal 40, texts.size
    assert_equal 1, texts.map(&:object_id).uniq.size
    assert_equal "needle " * 40, texts.first
  end

  it "rewinds external iteration without cancelling an independent each" do
    [Seen.each_path(paths: [@dir], type: "f"), Seen.each_line(pattern: "needle", paths: [@dir])].each do |results|
      started = Queue.new
      resume = Queue.new
      thread = Thread.new do
        count = 0
        results.each do
          count += 1
          if count == 1
            started << true
            resume.pop
          end
        end
        count
      end

      Timeout.timeout(5) do
        started.pop
        results.next
        results.rewind
        resume << true
        assert_equal 200, thread.value
        assert_equal 200, count_external(results)
      end
    ensure
      thread&.kill
      thread&.join
    end
  end

  it "releases batches held by abandoned external iteration" do
    payload = File.join(@dir, "payload")
    Dir.mkdir(payload)
    line = "needle#{"x" * 4_089}\n"
    20.times { |index| File.binwrite(File.join(payload, index.to_s), line * 40) }

    external = rss_after_abandoning(payload, :next, 20)
    internal = rss_after_abandoning(payload, :first, 20)

    assert_operator external, :<, internal + (32 * 1024),
      "external=#{external} KiB, internal=#{internal} KiB"
  end

  it "delivers every result when the consumer lags behind the walk" do
    seen = []

    Timeout.timeout(5) do
      Seen.each_path(paths: [@dir], hidden: true) do |path|
        sleep 0.005 if seen.size < 3
        seen << path
      end
    end

    assert_equal Seen.each_path(paths: [@dir], hidden: true).to_a.sort, seen.sort
  end

  it "consumes the same Enumerator independently" do
    Timeout.timeout(5) do
      assert_independent_enumerations(Seen.each_path(paths: [@dir], type: "f"), 200)
      assert_independent_enumerations(Seen.each_line(pattern: "needle", paths: [@dir]), 200)
    end
  end

  it "keeps external iteration independent of internal iteration" do
    results = Seen.each_path(paths: [@dir], type: "f")

    assert_kind_of String, results.next
    assert_equal 200, results.count
    assert_equal 199, count_external(results)
  end

  it "recovers after repeated Timeout interruption while grepping" do
    results = slow_line_results

    3.times do
      assert_raises(Timeout::Error) do
        Timeout.timeout(0.01) { results.to_a }
      end
    end

    assert_equal 200, Seen.each_line(pattern: "needle", paths: [@dir]).count
  end

  it "preserves Interrupt while grepping" do
    results = slow_line_results
    started = Queue.new
    thread = Thread.new do
      started << true
      results.count
      :completed
    rescue Interrupt => error
      error
    end
    started.pop
    sleep 0.01

    assert_predicate thread, :alive?, "grep completed before it could be interrupted"
    thread.raise(Interrupt, "test interrupt")
    error = Timeout.timeout(5) { thread.value }

    assert_instance_of Interrupt, error
    assert_equal "test interrupt", error.message
    refute_kind_of Seen::Error, error
  ensure
    thread&.kill
    thread&.join
  end

  it "searches in a forked child after the parent has searched" do
    skip "fork is unavailable" unless Process.respond_to?(:fork)

    Seen.each_path(paths: [@dir], hidden: true).to_a
    pid = fork do
      Seen.each_path(paths: [@dir], hidden: true).to_a
      Seen.each_line(pattern: "needle", paths: [@dir]).to_a
      exit 0
    end
    _, status = Process.waitpid2(pid)

    assert_predicate status, :success?, "forked child should search, got #{status.inspect}"
  end

  it "interrupts inherited live iteration in a forked child" do
    skip "fork is unavailable" unless Process.respond_to?(:fork)

    path = File.join(@dir, "dir0", "file0.txt")
    paths = Array.new(100_000, path)

    search = Seen.each_path(paths:, type: "f", no_ignore: true)
    assert_equal "RuntimeError: Path search interrupted", forked_iteration_error(search)

    grep = Seen.each_line(pattern: "needle", paths:, no_ignore: true)
    assert_equal "RuntimeError: Line search interrupted", forked_iteration_error(grep)
  end

  it "completes despite spurious thread wakeups" do
    thread = Thread.new do
      Seen.each_path(paths: [@dir], hidden: true).to_a
    rescue => e
      e
    end

    begin
      thread.wakeup while thread.alive?
    rescue ThreadError
    end

    assert_kind_of Array, thread.value, "spurious wakeups should not abort the search"
    assert_equal Seen.each_path(paths: [@dir], hidden: true).to_a.sort, thread.value.sort,
      "spurious wakeups should not truncate the search"
  end

  it "delivers a timeout despite wakeup pressure" do
    paths = Array.new(1_000, @dir)
    thread = Thread.new do
      Timeout.timeout(0.04) { Seen.each_path(paths:, hidden: true).to_a }
      :completed
    rescue Timeout::Error
      :timed_out
    end

    begin
      thread.wakeup while thread.alive?
    rescue ThreadError
    end

    assert_equal :timed_out, thread.value, "wakeup pressure should not swallow a timeout"
  end

  it "completes grep despite spurious thread wakeups" do
    thread = Thread.new do
      Seen.each_line(pattern: "needle", paths: [@dir]).to_a
    rescue => e
      e
    end

    begin
      thread.wakeup while thread.alive?
    rescue ThreadError
    end

    assert_kind_of Array, thread.value, "spurious wakeups should not abort the grep"
    assert_equal Seen.each_line(pattern: "needle", paths: [@dir]).to_a.sort, thread.value.sort,
      "spurious wakeups should not truncate the grep"
  end

  # Run in a child with YJIT off because it skips Ractor checks for C calls.
  it "searches and greps from Ractors" do
    script = <<~RUBY
      require "seen"
      Warning[:experimental] = false
      Thread.report_on_exception = false

      ractors = Array.new(4) do
        Ractor.new(ARGV[0]) do |path|
          search = Seen.each_path(pattern: "file1", paths: [path], extension: "txt").count
          grep = Seen.each_line(pattern: "needle", paths: [path]).count
          invalid_pattern = begin
            Seen.each_path(pattern: "[", paths: [path]).to_a
            false
          rescue Seen::InvalidPattern
            true
          end
          [search, grep, invalid_pattern]
        end
      end

      print ractors.map { |r| r.respond_to?(:value) ? r.value : r.take }.inspect
    RUBY
    output, status = Open3.capture2(
      {"RUBYOPT" => nil, "RUBYLIB" => nil, "RUBY_YJIT_ENABLE" => nil, "RUBY_ZJIT_ENABLE" => nil},
      Gem.ruby,
      "--disable-yjit",
      "-I#{File.expand_path("../lib", __dir__)}",
      "-e",
      script,
      @dir
    )

    assert_predicate status, :success?
    assert_equal ([[110, 200, true]] * 4).inspect, output
  end
end
