const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("lmdb", .{
        .root_source_file = .{ .src_path = .{ .owner = b, .sub_path = "src/lmdb.zig" } },
        // .root_source_file = .{ .cwd_relative = "src/lmdb.zig" },
        .target = target,
        .optimize = optimize,
    });

    mod.addIncludePath(.{ .src_path = .{ .owner = b, .sub_path = "lmdb/libraries/liblmdb" } });

    mod.addCSourceFiles(.{ .files = &.{
        "./lmdb/libraries/liblmdb/midl.c",
        "./lmdb/libraries/liblmdb/mdb.c",
    } });

    const unit_tests = b.addTest(.{
        .root_source_file = .{ .cwd_relative = "src/lmdb.zig" },
        .target = target,
        .optimize = optimize,
    });

    unit_tests.addIncludePath(.{ .src_path = .{ .owner = b, .sub_path = "lmdb/libraries/liblmdb" } });
    unit_tests.addCSourceFiles(.{ .files = &.{
        "./lmdb/libraries/liblmdb/midl.c",
        "./lmdb/libraries/liblmdb/mdb.c",
    } });
    unit_tests.linkLibC();

    const test_bin = b.addInstallBinFile(unit_tests.getEmittedBin(), "./lmdb_test");

    const run_unit_tests = b.addRunArtifact(unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&unit_tests.step);
    test_step.dependOn(&test_bin.step);
}
