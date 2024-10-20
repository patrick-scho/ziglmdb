const std = @import("std");
const lmdb = @cImport(@cInclude("lmdb.h"));

const print = std.debug.print;

pub fn Lmdb(comptime KeySize: comptime_int) type {
    _ = KeySize;
    return struct {
        pub fn init(comptime path: []const u8) @This() {
            var res: @This() = undefined;

            _ = lmdb.mdb_env_create(&res.env);
            // mdb_env_set_maxreaders(env, 1);
            // mdb_env_set_maxdbs(env, 1); // named databases
            // mdb_env_set_mapsize(env, 1024*1024);

            _ = lmdb.mdb_env_open(res.env, path.ptr, 0, 0o664);
            // /*MDB_FIXEDMAP |MDB_NOSYNC |MDB_NOSUBDIR*/

            _ = lmdb.mdb_txn_begin(res.env, null, 0, &res.txn);
            _ = lmdb.mdb_dbi_open(res.txn, null, 0, &res.dbi);

            return res;
        }

        pub fn deinit(self: @This()) void {
            _ = lmdb.mdb_txn_commit(self.txn);
            _ = lmdb.mdb_dbi_close(self.env, self.dbi);
            _ = lmdb.mdb_env_close(self.env);
        }

        pub fn get(self: @This(), comptime T: type, key: []const u8) ?T {
            var k = lmdb.MDB_val{
                .mv_data = @ptrFromInt(@intFromPtr(key.ptr)),
                .mv_size = key.len,
            };

            var v: lmdb.MDB_val = undefined;

            const res = lmdb.mdb_get(self.txn, self.dbi, &k, &v);

            if (res == 0 and v.mv_size == @sizeOf(T)) {
                if (v.mv_data) |data| {
                    return @as(*T, @ptrFromInt(@intFromPtr(data))).*;
                }
            }

            return null;
        }

        pub fn put(self: @This(), comptime T: type, key: []const u8, val: T) void {
            var k = lmdb.MDB_val{
                .mv_data = @ptrFromInt(@intFromPtr(key.ptr)),
                .mv_size = key.len,
            };

            var v = lmdb.MDB_val{
                .mv_data = @ptrFromInt(@intFromPtr(&val)),
                .mv_size = @sizeOf(T),
            };

            const res = lmdb.mdb_put(self.txn, self.dbi, &k, &v, 0);
            _ = res;

            // return val;
        }

        env: ?*lmdb.MDB_env,
        dbi: lmdb.MDB_dbi,
        txn: ?*lmdb.MDB_txn,
    };
}

pub fn main() !void {
    var db = Lmdb(16).init("./db");
    defer db.deinit();

    var testKey = [_]u8{0} ** 16;
    @memcpy(testKey[0..5], "abcde");
    // @memcpy(testKey[5..10], "abcde");

    const u_1 = db.get(u8, &testKey);
    print("u1: {?}\n", .{u_1});

    var u_2 = db.get(u8, "abcde" ++ "12345");
    db.put(u8, "abcde" ++ "12345", u_2.? + 1);

    u_2 = db.get(u8, "abcde" ++ "12345");
    print("u2: {?}\n", .{u_2});
}
