# frozen_string_literal: true

require "tmpdir"
require_relative "spec_helper"

describe Seen do
  describe "module methods" do
    it "responds to .each_path" do
      assert_respond_to Seen, :each_path
    end

    it "responds to .each_line" do
      assert_respond_to Seen, :each_line
    end

    it "does not expose old API names" do
      %i[search grep entries scan].each { |name| refute_respond_to Seen, name }
    end

    it "keeps .native_each_path private" do
      refute_respond_to Seen, :native_each_path
      assert_raises(NoMethodError) { Seen.native_each_path }
    end

    it "keeps .native_each_line private" do
      refute_respond_to Seen, :native_each_line
      assert_raises(NoMethodError) { Seen.native_each_line }
    end
  end

  describe ".each_path" do
    it "returns an Enumerator" do
      results = Seen.each_path(paths: ["lib"], max_depth: 1)

      assert_kind_of Enumerator, results
      assert_kind_of String, results.first
    end

    it "yields to a block and returns the Enumerator" do
      yielded = []

      results = Seen.each_path(paths: ["lib"], max_depth: 1) { |path| yielded << path }

      assert_kind_of Enumerator, results
      assert_equal results.to_a.sort, yielded.sort
    end

    it "does not search when given no block" do
      Dir.mktmpdir("seen-unconsumed") do |dir|
        Seen.each_path(paths: [dir], type: "f")
        path = File.join(dir, "created_after_search.txt")
        File.write(path, "")

        assert_equal [path], Seen.each_path(paths: [dir], type: "f").to_a
      end
    end

    it "starts searching when the Enumerator is consumed" do
      Dir.mktmpdir("seen-lazy") do |dir|
        results = Seen.each_path(paths: [dir], type: "f")
        path = File.join(dir, "created_after_search.txt")
        File.write(path, "")

        assert_includes results.to_a, path
      end
    end

    it "can be enumerated again" do
      results = Seen.each_path(paths: ["lib"], max_depth: 1)
      first = results.to_a.sort

      refute_empty first
      assert_equal first, results.to_a.sort
    end

    it "restarts after rewind" do
      results = Seen.each_path(paths: ["lib"], max_depth: 1)
      first = results.next
      results.rewind

      assert_includes results.to_a, first
    end

    it "preserves external iteration methods across rewind" do
      results = Seen.each_path(pattern: '^seen\.rb$', paths: ["lib"])
      assert_equal ["lib/seen.rb"], results.peek_values
      assert_equal "lib/seen.rb", results.peek
      assert_equal ["lib/seen.rb"], results.next_values
      assert_nil results.feed(:ignored)
      assert_raises(StopIteration) { results.next }
      assert_same results, results.rewind
      assert_equal "lib/seen.rb", results.next
      results.rewind
    end

    it "copies unstarted enumerators with independent external cursors" do
      results = Seen.each_path(paths: ["lib"], max_depth: 1)
      copy = results.dup
      expected = results.to_a.sort

      results.next
      copy.next
      assert_raises(TypeError) { results.dup }
      assert_raises(TypeError) { copy.dup }
      results.rewind
      copy.rewind
      assert_equal expected, Array.new(expected.size) { results.next }.sort
      assert_equal expected, Array.new(expected.size) { copy.next }.sort
      results.rewind
      copy.rewind
    end

    it "omits the ./ prefix when paths defaults, as fd does" do
      results = Seen.each_path(pattern: "^LICENSE$").to_a

      assert_equal ["LICENSE"], results
    end

    it "keeps the ./ prefix for an explicit . root, as fd does" do
      results = Seen.each_path(pattern: "^LICENSE$", paths: ["."]).to_a

      assert_equal ["./LICENSE"], results
    end

    it "echoes back whichever root it was given" do
      assert_equal ["lib/seen.rb"], Seen.each_path(pattern: '^seen\.rb$', paths: ["lib"]).to_a
      assert_equal ["./lib/seen.rb"], Seen.each_path(pattern: '^seen\.rb$', paths: ["./lib"]).to_a
    end

    it "emits results again for a repeated path" do
      results = Seen.each_path(pattern: '^seen\.rb$', paths: %w[lib lib], type: "f").to_a

      assert_equal ["lib/seen.rb", "lib/seen.rb"], results.sort
    end

    it "tags result paths with the filesystem encoding" do
      results = path_results(paths: ["lib"], max_depth: 1)

      refute_empty results
      assert(results.all? { |path| path.encoding == Encoding.find("filesystem") },
        "paths should carry the filesystem encoding")
    end

    it "returns String paths that point to existing files" do
      results = path_results(paths: ["lib"], max_depth: 1)
      refute_empty results
      assert(results.all?(String),
        "all results should be String paths")
      assert(results.all? { |result| File.exist?(result) || File.symlink?(result) },
        "all paths should point to existing files or symlinks")
    end

    it "returns relative paths by default" do
      results = path_results(paths: ["lib"], max_depth: 1)
      refute_empty results
      assert(results.all? { |result| !result.start_with?("/") },
        "paths should be relative, not absolute")
      assert(results.all? { |result| result.start_with?("lib") },
        "results should start with the search path")
    end

    it "accepts pattern as keyword argument and uses it" do
      with_pattern = path_results(pattern: "seen", paths: ["lib"], max_depth: 1)
      without_pattern = path_results(paths: ["lib"], max_depth: 1)

      assert_operator with_pattern.size, "<=", without_pattern.size,
        "pattern should filter results"
      assert(with_pattern.any? { |p| p.include?("seen") },
        "results should match the pattern")
    end

    it "accepts multiple paths and searches all of them" do
      results = path_results(pattern: "spec", paths: %w[lib spec], max_depth: 2)

      refute_empty results, "should find spec matches"
      assert(results.any? { |p| p.start_with?("spec") },
        "results should include paths from spec directory")
    end

    it "accepts options with pattern and paths" do
      results = path_results(pattern: "spec", paths: ["."], type: "d", max_depth: 2)

      assert_includes results, "./spec"
      assert(results.all? { |p| File.directory?(p) },
        "should only include directories with type: d")
    end

    it "yields nothing when pattern matches nothing" do
      results = path_results(pattern: "nonexistent_xyz_123_abc", paths: ["."])
      assert_empty results
    end
  end
end
