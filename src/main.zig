const std = @import("std");
const lmdb = @cImport(@cInclude("lmdb.h"));

const print = std.debug.print;

fn get(txn: ?*lmdb.MDB_txn, dbi: lmdb.MDB_dbi, k: anytype, comptime T: type) ?T {
    var key = lmdb.MDB_val{
        .mv_size = @sizeOf(@TypeOf(k)),
        .mv_data = @constCast(@ptrCast(&k)),
    };
    var val: lmdb.MDB_val = undefined;
    return switch (lmdb.mdb_get(txn, dbi, &key, &val)) {
        0 => @as(*T, @ptrFromInt(@intFromPtr(val.mv_data))).*,
        else => |err| {
            _ = err;
            // print("get err: {}\n", .{err});
            return null;
        },
    };
}

fn put(txn: ?*lmdb.MDB_txn, dbi: lmdb.MDB_dbi, k: anytype, v: anytype) void {
    var key = lmdb.MDB_val{
        .mv_size = @sizeOf(@TypeOf(k)),
        .mv_data = @constCast(@ptrCast(&k)),
    };
    var val = lmdb.MDB_val{
        .mv_size = @sizeOf(@TypeOf(v)),
        .mv_data = @constCast(@ptrCast(&v)),
    };
    switch (lmdb.mdb_put(txn, dbi, &key, &val, 0)) {
        0 => {},
        else => |err| {
            print("put err: {}\n", .{err});
        },
    }
}

pub fn main() void {
    var env: ?*lmdb.MDB_env = undefined;
    _ = lmdb.mdb_env_create(&env);
    _ = lmdb.mdb_env_set_maxdbs(env, 10);
    _ = lmdb.mdb_env_set_mapsize(env, 1024 * 1024 * 120);
    _ = lmdb.mdb_env_open(env, "./db1", lmdb.MDB_NOSYNC | lmdb.MDB_WRITEMAP, 0o664);
    defer lmdb.mdb_env_close(env);

    for (0..1000000) |i| {
        var txn: ?*lmdb.MDB_txn = undefined;
        switch (lmdb.mdb_txn_begin(env, null, 0, &txn)) {
            0 => {},
            else => |err| {
                print("txn err: {}\n", .{err});
            },
        }

        var db: lmdb.MDB_dbi = undefined;
        _ = lmdb.mdb_dbi_open(txn, "subdb2", lmdb.MDB_CREATE | lmdb.MDB_INTEGERKEY, &db);
        if (i == 0) {
            var db_stat: lmdb.MDB_stat = undefined;
            _ = lmdb.mdb_stat(txn, db, &db_stat);
            // print("{}\n", .{db_stat});
        }
        defer lmdb.mdb_dbi_close(env, db);

        const Val = struct {
            a: u64,
            b: i64,
            c: [16]u8,
        };

        var new_val = Val{
            .a = 123,
            .b = -123,
            .c = undefined,
        };
        std.mem.copyForwards(u8, &new_val.c, "a c efghabcdefgh");

        const key: u64 = i + 1000;
        if (get(txn, db, key, Val)) |val| {
            if (i % 100000 == 0) {
                print("{}: {}\n", .{ i, val });
            }
            new_val = val;
            new_val.a += 1;
            new_val.b -= 1;
            std.mem.copyForwards(u8, &new_val.c, "a c efghabcdefgh");
        } else {
            if (i % 100000 == 0) {
                print("not found\n", .{});
            }
        }

        put(txn, db, key, new_val);

        switch (lmdb.mdb_txn_commit(txn)) {
            0 => {},
            lmdb.MDB_MAP_FULL => {
                print("resize\n", .{});
                _ = lmdb.mdb_env_set_mapsize(env, 0);
            },
            else => |err| {
                print("commit err: {}\n", .{err});
            },
        }
    }

    switch (lmdb.mdb_env_sync(env, 1)) {
        0 => {},
        else => |err| {
            print("sync err: {}\n", .{err});
        },
    }

    // var env_info: lmdb.MDB_envinfo = undefined;
    // _ = lmdb.mdb_env_info(env, &env_info);

    // var env_stat: lmdb.MDB_stat = undefined;
    // _ = lmdb.mdb_env_stat(env, &env_stat);

    // print("{}\n", .{env_info});
    // print("{}\n", .{env_stat});

    print("done!\n", .{});
}

test "hash" {
    const pw = "affeaffe";

    var hash_buffer: [128]u8 = undefined;

    var buffer: [1024 * 1024]u8 = undefined;
    var alloc = std.heap.FixedBufferAllocator.init(&buffer);

    const result = try std.crypto.pwhash.argon2.strHash(pw, .{
        .allocator = alloc.allocator(),
        .params = std.crypto.pwhash.argon2.Params.fromLimits(1000, 1024 * 10),
    }, &hash_buffer);

    print("{s}\n", .{result});

    if (std.crypto.pwhash.argon2.strVerify(result, "affeaffe", .{
        .allocator = alloc.allocator(),
    })) {
        print("verified\n", .{});
    } else |err| {
        print("not verified: {}\n", .{err});
    }
}
