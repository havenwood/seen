# frozen_string_literal: true

require "seen/version"

begin
  require "seen/seen_native"
rescue LoadError => e
  raise LoadError, "Failed to load the seen native extension on #{RUBY_PLATFORM} (#{e.message}). " \
    "Install Rust and reinstall seen to build from source."
end

# File and content search for Ruby
module Seen
  # Tracks the implicit root separately from an explicit ".", like `fd` and `rg`.
  CWD = ["."].freeze
  private_constant :CWD

  # Every iteration owns a cursor. Direct external iteration can also close its
  # cursor explicitly before Ruby discards its Fiber on rewind.
  class Results < ::Enumerator
    def initialize(source)
      @source = source
      @cursor = nil
      super() { |output| @source.each { |*values| output.yield(*values) } }
      @external = ::Enumerator.new do |output|
        cursor = @cursor = @source.cursor
        begin
          while (values = cursor.next_values)
            output.yield(*values)
          end
        ensure
          cursor.close
          @cursor = nil if @cursor.equal?(cursor)
        end
      end
    end

    def each(&)
      return self unless block_given?

      @source.each(&)
    end

    def next = @external.next
    def next_values = @external.next_values
    def peek = @external.peek
    def peek_values = @external.peek_values
    def feed(value) = @external.feed(value)

    def freeze
      @external.freeze
      super
    end

    def rewind
      raise FrozenError, "can't modify frozen #{self.class}" if frozen?

      @cursor&.close
      @cursor = nil
      @external.rewind
      super
    end

    def initialize_copy(other)
      super
      @external.dup # Preserve Enumerator's refusal to copy live external iteration.
      initialize(@source)
    end
  end
  private_constant :Results

  class << self
    def each_path(
      pattern: nil,
      paths: CWD,
      hidden: false,
      no_ignore: false,
      case_sensitive: false,
      glob: false,
      full_path: false,
      follow: false,
      max_depth: nil,
      min_depth: nil,
      type: nil,
      extension: nil,
      exclude: [],
      min_size: nil,
      max_size: nil,
      changed_within: nil,
      changed_before: nil,
      ignore_error: true,
      ignore_file: [],
      &
    )
      results = native_each_path(
        pattern:,
        paths:,
        strip_cwd_prefix: paths.equal?(CWD),
        ignore_error:,
        ignore_file:,
        hidden:,
        no_ignore:,
        case_sensitive:,
        glob:,
        full_path:,
        follow:,
        max_depth:,
        min_depth:,
        type:,
        extension:,
        exclude:,
        min_size:,
        max_size:,
        changed_within:,
        changed_before:
      )
      results = Results.new(results)
      results.each(&)
      results
    end

    def each_line(
      pattern:,
      name: nil,
      paths: CWD,
      hidden: false,
      no_ignore: false,
      case_sensitive: false,
      content_case_sensitive: true,
      text: false,
      max_count: nil,
      heap_limit: nil,
      encoding: nil,
      column: false,
      byte_range: false,
      glob: false,
      full_path: false,
      follow: false,
      max_depth: nil,
      min_depth: nil,
      extension: nil,
      exclude: [],
      min_size: nil,
      max_size: nil,
      changed_within: nil,
      changed_before: nil,
      ignore_error: true,
      ignore_file: [],
      &
    )
      results = native_each_line(
        pattern:,
        name:,
        paths:,
        strip_cwd_prefix: paths.equal?(CWD),
        ignore_error:,
        ignore_file:,
        hidden:,
        no_ignore:,
        case_sensitive:,
        content_case_sensitive:,
        text:,
        max_count:,
        heap_limit:,
        encoding:,
        column:,
        byte_range:,
        glob:,
        full_path:,
        follow:,
        max_depth:,
        min_depth:,
        extension:,
        exclude:,
        min_size:,
        max_size:,
        changed_within:,
        changed_before:
      )
      results = Results.new(results)
      results.each(&)
      results
    end

    private :native_each_path, :native_each_line
  end
end
