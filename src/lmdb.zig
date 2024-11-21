const std = @import("std");
const lmdb = @cImport(@cInclude("lmdb.h"));

pub usingnamespace lmdb;

pub fn Cursor(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        pub const Flags = enum(c_uint) {
            First = lmdb.MDB_FIRST,
            FirstDup = lmdb.MDB_FIRST_DUP,
            GetBoth = lmdb.MDB_GET_BOTH,
            GetBothRange = lmdb.MDB_GET_BOTH_RANGE,
            GetCurrent = lmdb.MDB_GET_CURRENT,
            GetMultiple = lmdb.MDB_GET_MULTIPLE,
            Last = lmdb.MDB_LAST,
            LastDup = lmdb.MDB_LAST_DUP,
            Next = lmdb.MDB_NEXT,
            NextDup = lmdb.MDB_NEXT_DUP,
            NextMultiple = lmdb.MDB_NEXT_MULTIPLE,
            NextNodup = lmdb.MDB_NEXT_NODUP,
            Prev = lmdb.MDB_PREV,
            PrevDup = lmdb.MDB_PREV_DUP,
            PrevNodup = lmdb.MDB_PREV_NODUP,
            Set = lmdb.MDB_SET,
            SetKey = lmdb.MDB_SET_KEY,
            SetRange = lmdb.MDB_SET_RANGE,
        };

        ptr: ?*lmdb.MDB_cursor = undefined,

        pub fn close(self: *Self) void {
            _ = lmdb.mdb_cursor_close(self.ptr);
        }

        pub fn put(self: *Self, k: K, v: V) void {
            var key = lmdb.MDB_val{
                .mv_size = @sizeOf(@TypeOf(k)),
                .mv_data = @constCast(@ptrCast(&k)),
            };
            var val = lmdb.MDB_val{
                .mv_size = @sizeOf(@TypeOf(v)),
                .mv_data = @constCast(@ptrCast(&v)),
            };
            switch (lmdb.mdb_cursor_put(self.ptr, &key, &val, 0)) {
                0 => {},
                else => |err| {
                    std.debug.print("put err: {}\n", .{err});
                },
            }
        }

        pub fn get(self: *Self, k: *K, flags: Flags) ?V {
            var key = lmdb.MDB_val{
                .mv_size = @sizeOf(K),
                .mv_data = @constCast(@ptrCast(k)),
            };
            var val: lmdb.MDB_val = undefined;
            return switch (lmdb.mdb_cursor_get(self.ptr, &key, &val, @intFromEnum(flags))) {
                0 => {
                    k.* = std.mem.bytesToValue(K, key.mv_data.?);
                    return std.mem.bytesToValue(V, val.mv_data.?);
                    //k.* = @as(*align(1) K, @ptrCast(key.mv_data)).*;
                    //return @as(?*align(1) V, @ptrCast(val.mv_data)).?.*;
                },
                lmdb.MDB_NOTFOUND => null,
                else => |err| {
                    std.debug.print("get err: {}\n", .{err});
                    return null;
                },
            };
        }

        pub fn del(self: *Self, k: K) void {
            var key = lmdb.MDB_val{
                .mv_size = @sizeOf(@TypeOf(k)),
                .mv_data = @constCast(@ptrCast(&k)),
            };
            switch (lmdb.mdb_cursor_del(self.ptr, &key, 0)) {
                0 => {},
                else => |err| {
                    std.debug.print("del err: {}\n", .{err});
                },
            }
        }

        pub fn has(self: *Dbi, k: K, flags: Flags) bool {
            return self.get(k, flags) != null;
        }
    };
}

pub fn Dbi(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        ptr: lmdb.MDB_dbi = undefined,
        txn: *const Txn = undefined,
        env: *const Env = undefined,

        pub fn close(self: Self) void {

            // TODO: necessary?
            lmdb.mdb_dbi_close(self.env.ptr, self.ptr);
        }

        pub fn cursor(self: Self) !Cursor(K, V) {
            var result = Cursor(K, V){};

            return switch (lmdb.mdb_cursor_open(self.txn.ptr, self.ptr, &result.ptr)) {
                0 => result,
                else => error.CursorOpen,
            };
        }

        pub fn put(self: Self, k: K, v: V) void {
            var key = lmdb.MDB_val{
                .mv_size = @sizeOf(@TypeOf(k)),
                .mv_data = @constCast(@ptrCast(&k)),
            };
            var val = lmdb.MDB_val{
                .mv_size = @sizeOf(@TypeOf(v)),
                .mv_data = @constCast(@ptrCast(&v)),
            };
            switch (lmdb.mdb_put(self.txn.ptr, self.ptr, &key, &val, 0)) {
                0 => {},
                else => |err| {
                    std.debug.print("put err: {}\n", .{err});
                },
            }
        }

        pub fn get(self: Self, k: K) ?V {
            var key = lmdb.MDB_val{
                .mv_size = @sizeOf(@TypeOf(k)),
                .mv_data = @constCast(@ptrCast(&k)),
            };
            var val: lmdb.MDB_val = undefined;
            return switch (lmdb.mdb_get(self.txn.ptr, self.ptr, &key, &val)) {
                0 => {
                    return std.mem.bytesToValue(V, val.mv_data.?);
                    //@as(?*align(1) V, @ptrCast(val.mv_data)).?.*
                },
                else => |err| {
                    std.debug.print("get err: {}\n", .{err});
                    return null;
                },
            };
        }

        pub fn del(self: Self, k: K) void {
            var key = lmdb.MDB_val{
                .mv_size = @sizeOf(@TypeOf(k)),
                .mv_data = @constCast(@ptrCast(&k)),
            };
            switch (lmdb.mdb_del(self.txn.ptr, self.ptr, &key, null)) {
                0 => {},
                else => |err| {
                    std.debug.print("del err: {}\n", .{err});
                },
            }
        }

        pub fn has(self: Self, k: K) bool {
            return self.get(k) != null;
        }
    };
}

pub const Txn = struct {
    ptr: ?*lmdb.MDB_txn = undefined,
    env: *const Env = undefined,

    pub fn dbi(self: *const Txn, name: [*c]const u8, comptime K: type, comptime V: type) !Dbi(K, V) {
        var result = Dbi(K, V){ .env = self.env, .txn = self };
        // TODO: lmdb.MDB_INTEGERKEY?
        switch (lmdb.mdb_dbi_open(self.ptr, name, lmdb.MDB_CREATE, &result.ptr)) {
            0 => return result,
            else => |err| {
                std.debug.print("dbi err: {}\n", .{err});
                return error.DbiOpen;
            },
        }
    }

    pub fn commit(self: Txn) void {
        switch (lmdb.mdb_txn_commit(self.ptr)) {
            0 => {},
            lmdb.MDB_MAP_FULL => {
                std.debug.print("resize\n", .{});
                _ = lmdb.mdb_env_set_mapsize(self.env.ptr, 0);
            },
            else => |err| {
                std.debug.print("commit err: {}\n", .{err});
            },
        }
    }

    pub fn abort(self: Txn) void {
        lmdb.mdb_txn_abort(self.ptr);
    }
};

pub const Env = struct {
    ptr: ?*lmdb.MDB_env = undefined,

    pub fn open(name: [*c]const u8, size: lmdb.mdb_size_t) Env {
        var result = Env{};

        _ = lmdb.mdb_env_create(&result.ptr);
        _ = lmdb.mdb_env_set_maxdbs(result.ptr, 10);
        _ = lmdb.mdb_env_set_mapsize(result.ptr, size);
        _ = lmdb.mdb_env_open(result.ptr, name, lmdb.MDB_WRITEMAP, 0o664);
        // _ = lmdb.mdb_env_open(result.ptr, name, lmdb.MDB_NOSYNC | lmdb.MDB_WRITEMAP, 0o664);

        return result;
    }

    pub fn close(self: Env) void {
        lmdb.mdb_env_close(self.ptr);
    }

    pub fn txn(self: *const Env) !Txn {
        var result = Txn{ .env = self };
        switch (lmdb.mdb_txn_begin(self.ptr, null, 0, &result.ptr)) {
            0 => return result,
            else => |err| {
                std.debug.print("txn err: {}\n", .{err});
                return error.TxnOpen;
            },
        }
    }

    pub fn sync(self: *Env) void {
        switch (lmdb.mdb_env_sync(self.ptr, 1)) {
            0 => {},
            else => |err| {
                std.debug.print("sync err: {}\n", .{err});
            },
        }
    }
};

test "basic" {
    var env = Env.open("db", 1024 * 1024 * 1);

    var txn = try env.txn();

    const Value = struct {
        i: i64 = 123,
        s: [16]u8 = undefined,
    };

    var dbi = try txn.dbi("abc", u64, Value);

    var val = dbi.get(1) orelse Value{ .i = 5 };
    std.debug.print("{}\n", .{val});

    val.i += 1;

    dbi.put(1, val);

    txn.commit();
    dbi.close();
    env.sync();
    env.close();
}

test "cursor" {
    var env = Env.open("db", 1024 * 1024 * 1);

    var txn = try env.txn();

    const Value = struct {
        i: i64 = 123,
        s: [16]u8 = undefined,
    };

    var dbi = try txn.dbi("def", u64, Value);

    for (0..10) |i| {
        dbi.put(i, Value{ .i = @intCast(i + 23) });
    }

    var cursor = try dbi.cursor();

    var key: u64 = undefined;
    {
        const val = cursor.get(&key, .First);
        std.debug.print("{}: {?}\n", .{ key, val });
    }

    while (cursor.get(&key, .Next)) |val| {
        std.debug.print("{}: {?}\n", .{ key, val });
    }

    cursor.close();

    txn.commit();
    dbi.close();
    env.sync();
    env.close();
}

// pub fn get(txn: ?*lmdb.MDB_txn, dbi: lmdb.MDB_dbi, k: anytype, comptime T: type) ?T {
//     var key = lmdb.MDB_val{
//         .mv_size = @sizeOf(@TypeOf(k)),
//         .mv_data = @constCast(@ptrCast(&k)),
//     };
//     var val: lmdb.MDB_val = undefined;
//     return switch (lmdb.mdb_get(txn, dbi, &key, &val)) {
//         0 => @as(?*align(1) T, @alignCast(@ptrCast(val.mv_data))).?.*,
//         else => |err| {
//             std.debug.print("get err: {}\n", .{err});
//             return null;
//         },
//     };
// }

// pub fn put(txn: ?*lmdb.MDB_txn, dbi: lmdb.MDB_dbi, k: anytype, v: anytype) void {
//     var key = lmdb.MDB_val{
//         .mv_size = @sizeOf(@TypeOf(k)),
//         .mv_data = @constCast(@ptrCast(&k)),
//     };
//     var val = lmdb.MDB_val{
//         .mv_size = @sizeOf(@TypeOf(v)),
//         .mv_data = @constCast(@ptrCast(&v)),
//     };
//     switch (lmdb.mdb_put(txn, dbi, &key, &val, 0)) {
//         0 => {},
//         else => |err| {
//             std.debug.print("put err: {}\n", .{err});
//         },
//     }
// }

// pub fn del(txn: ?*lmdb.MDB_txn, dbi: lmdb.MDB_dbi, k: anytype) void {
//     var key = lmdb.MDB_val{
//         .mv_size = @sizeOf(@TypeOf(k)),
//         .mv_data = @constCast(@ptrCast(&k)),
//     };
//     switch (lmdb.mdb_del(txn, dbi, &key, null)) {
//         0 => {},
//         else => |err| {
//             std.debug.print("del err: {}\n", .{err});
//         },
//     }
// }

// pub fn has(txn: ?*lmdb.MDB_txn, dbi: lmdb.MDB_dbi, k: anytype) bool {
//     var key = lmdb.MDB_val{
//         .mv_size = @sizeOf(@TypeOf(k)),
//         .mv_data = @constCast(@ptrCast(&k)),
//     };
//     var val: lmdb.MDB_val = undefined;
//     return switch (lmdb.mdb_get(txn, dbi, &key, &val)) {
//         0 => true,
//         lmdb.MDB_NOTFOUND => false,
//         else => |err| {
//             std.debug.print("has err: {}\n", .{err});
//             return false;
//         },
//     };
// }
