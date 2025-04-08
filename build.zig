const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lmdb = b.addModule("lmdb", .{
        .root_source_file = b.path("src/lmdb.zig"),
        .target = target,
        .optimize = optimize,
    });
    lmdb.addIncludePath(b.path("lmdb"));
    lmdb.addCSourceFiles(.{ .files = &.{
        "./lmdb/midl.c",
        "./lmdb/mdb.c",
    } });
    lmdb.link_libc = true;

    const db = b.addModule("db", .{
        .root_source_file = b.path("src/db.zig"),
    });
    db.addImport("lmdb", lmdb);

    const lmdb_tests = b.addTest(.{
        .root_source_file = b.path("src/lmdb.zig"),
        .target = target,
        .optimize = optimize,
    });
    lmdb_tests.addIncludePath(b.path("lmdb"));
    lmdb_tests.addCSourceFiles(.{ .files = &.{
        "./lmdb/midl.c",
        "./lmdb/mdb.c",
    } });
    lmdb_tests.linkLibC();

    const db_tests = b.addTest(.{
        .root_source_file = b.path("src/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    db_tests.root_module.addImport("lmdb", lmdb);

    const lmdb_test_bin = b.addInstallBinFile(lmdb_tests.getEmittedBin(), "./lmdb_test");
    const db_test_bin = b.addInstallBinFile(db_tests.getEmittedBin(), "./db_test");

    const run_lmdb_tests = b.addRunArtifact(lmdb_tests);
    const run_db_tests = b.addRunArtifact(db_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.result_cached = false;
    test_step.dependOn(&run_lmdb_tests.step);
    test_step.dependOn(&lmdb_tests.step);
    test_step.dependOn(&run_db_tests.step);
    test_step.dependOn(&db_tests.step);
    test_step.dependOn(&lmdb_test_bin.step);
    test_step.dependOn(&db_test_bin.step);
}
