const std = @import("std");
const lmdb = @import("lmdb");

const PRNG_SEED = 0;

pub fn Db(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        dbi: lmdb.Dbi,

        pub fn init(txn: lmdb.Txn, name: [:0]const u8) !Self {
            return .{
                .dbi = try txn.dbi(name),
            };
        }
        pub fn put(self: Self, k: K, v: V) !void {
            try self.dbi.put(k, v);
        }
        pub fn get(self: Self, k: K) !V {
            return try self.dbi.get(k, V);
        }
        pub fn del(self: Self, k: K) !void {
            try self.dbi.del(k);
        }
        pub fn has(self: Self, k: K) !bool {
            return try self.dbi.has(k);
        }
        pub const Iterator = struct {
            cursor: lmdb.Cursor,
            k: ?K,
            v: ?V,

            pub fn next(self: *Iterator) ?struct { key: K, val: V } {
                if (self.k != null and self.v != null) {
                    const result = .{ .key = self.k.?, .val = self.v.? };

                    var k = self.k.?;
                    self.v = self.cursor.get(&k, V, .Next) catch return null;
                    if (self.v != null) {
                        self.k = k;
                    }
                    return result;
                } else {
                    return null;
                }
            }
        };
        pub fn iterator(self: Self) !Iterator {
            var cursor = try self.dbi.cursor();

            var k: K = undefined;
            const v = try cursor.get(&k, V, .First);
            return .{ .cursor = cursor, .k = k, .v = v };
        }
    };
}

pub const Prng = struct {
    var prng = std.Random.DefaultPrng.init(PRNG_SEED);

    pub fn gen(dbi: lmdb.Dbi, comptime T: type) !T {
        var buf: [@sizeOf(T)]u8 = undefined;
        // TODO: limit loop
        while (true) {
            prng.fill(&buf);
            const t = std.mem.bytesToValue(T, &buf);
            if (!try dbi.has(t)) {
                return t;
            }
        }
    }
};

fn SetListBase(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        pub const Index = u64;
        pub const Key = K;
        pub const Val = V;
        pub const View = SetListViewBase(K, V);

        idx: ?Index = null,

        fn open_dbi(txn: lmdb.Txn) !lmdb.Dbi {
            return try txn.dbi("SetList");
        }
        pub fn init(txn: lmdb.Txn) !Self {
            const head = View.Head{};
            const dbi = try open_dbi(txn);
            const idx = try Prng.gen(dbi, Index);
            try dbi.put(idx, head);
            return .{ .idx = idx };
        }
        pub fn open(self: Self, txn: lmdb.Txn) !View {
            // create new head
            if (self.idx == null) {
                return error.NotInitialized;
            }
            // get head from dbi
            const dbi = try open_dbi(txn);
            const head = try dbi.get(self.idx.?, View.Head);
            return .{
                .dbi = dbi,
                .idx = self.idx.?,
                .head = head,
            };
        }
    };
}

fn SetListViewBase(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        pub const ItemIndex = struct { SetListBase(K, V).Index, Key };
        pub const Key = K;
        pub const Val = V;

        pub const Head = struct {
            len: usize = 0,
            first: ?K = null,
            last: ?K = null,
        };
        pub const Item = struct {
            next: ?K = null,
            prev: ?K = null,
            data: V,
        };

        dbi: lmdb.Dbi,
        idx: SetListBase(K, V).Index,
        head: Head,

        fn item_idx(self: Self, k: K) ItemIndex {
            return .{ self.idx, k };
        }
        fn item_get(self: Self, k: K) !Item {
            return try self.dbi.get(self.item_idx(k), Item);
        }
        fn item_put(self: Self, k: K, item: Item) !void {
            try self.dbi.put(self.item_idx(k), item);
        }
        fn head_update(self: Self) !void {
            try self.dbi.put(self.idx, self.head);
        }
        pub fn del(self: *Self, k: K) !void {
            const item = try self.item_get(k);

            if (item.prev != null) {
                var prev = try self.item_get(item.prev.?);
                prev.next = item.next;
                try self.item_put(item.prev.?, prev);
            }

            if (item.next != null) {
                var next = try self.item_get(item.next.?);
                next.prev = item.prev;
                try self.item_put(item.next.?, next);
            }

            if (self.head.first == k) self.head.first = item.next;
            if (self.head.last == k) self.head.last = item.prev;
            self.head.len -= 1;
            try self.head_update();

            try self.dbi.del(self.item_idx(k));
        }
        pub fn clear(self: *Self) !void {
            var it = self.iterator();
            while (it.next()) |kv| {
                try self.del(kv.key);
            }
        }
        pub fn len(self: Self) usize {
            return self.head.len;
        }
        pub fn append(self: *Self, key: Key, val: Val) !void {
            if (self.head.len == 0) {
                const item = Item{ .data = val };
                try self.item_put(key, item);

                self.head.len = 1;
                self.head.first = key;
                self.head.last = key;
                try self.head_update();
            } else {
                const prev_idx = self.head.last.?;
                var prev = try self.item_get(prev_idx);

                const item = Item{ .prev = prev_idx, .data = val };
                try self.item_put(key, item);

                prev.next = key;
                try self.item_put(prev_idx, prev);

                self.head.last = key;
                self.head.len += 1;
                try self.head_update();
            }
        }
        pub fn get(self: Self, key: Key) !Val {
            const item = try self.item_get(key);
            return item.data;
        }
        pub fn has(self: Self, key: Key) !bool {
            return self.dbi.has(self.item_idx(key));
        }
        pub const Iterator = struct {
            pub const Result = ?struct { key: K, val: V };

            slv: SetListViewBase(K, V),
            idx: ?K,
            dir: enum { Forward, Backward },

            pub fn next(self: *Iterator) Result {
                if (self.idx != null) {
                    const k = self.idx.?;
                    const item = self.slv.item_get(k) catch return null;
                    self.idx = switch (self.dir) {
                        .Forward => item.next,
                        .Backward => item.prev,
                    };
                    return .{ .key = k, .val = item.data };
                } else {
                    return null;
                }
            }
        };
        pub fn iterator(self: Self) Iterator {
            return .{
                .slv = self,
                .idx = self.head.first,
                .dir = .Forward,
            };
        }
        pub fn reverse_iterator(self: Self) Iterator {
            return .{
                .slv = self,
                .idx = self.head.last,
                .dir = .Backward,
            };
        }
    };
}

pub fn Set(comptime K: type) type {
    return struct {
        pub const Key = K;
        pub const Val = void;

        pub const Base = SetListBase(Key, Val);
        pub const View = struct {
            const ViewBase = SetListViewBase(Key, Val);

            base: ViewBase,

            pub fn del(self: *@This(), key: Key) !void {
                try self.base.del(key);
            }
            pub fn clear(self: *@This()) !void {
                try self.base.clear();
            }
            pub fn len(self: @This()) usize {
                return self.base.len();
            }
            pub fn append(self: *@This(), key: Key) !void {
                try self.base.append(key, {});
            }
            pub fn has(self: @This(), key: Key) !bool {
                return try self.base.has(key);
            }
            pub fn iterator(self: @This()) ViewBase.Iterator {
                return self.base.iterator();
            }
            pub fn reverse_iterator(self: @This()) ViewBase.Iterator {
                return self.base.reverse_iterator();
            }
        };

        base: Base,

        pub fn init(txn: lmdb.Txn) !@This() {
            return .{ .base = try Base.init(txn) };
        }
        pub fn open(self: @This(), txn: lmdb.Txn) !View {
            return .{ .base = try self.base.open(txn) };
        }
    };
}

pub fn List(comptime V: type) type {
    return struct {
        pub const Key = u64;
        pub const Val = V;

        pub const Base = SetListBase(Key, Val);
        pub const View = struct {
            const ViewBase = SetListViewBase(Key, Val);

            base: ViewBase,

            fn gen(self: @This()) !Key {
                // TODO: limit loop
                while (true) {
                    const key = try Prng.gen(self.base.dbi, Key);
                    if (!try self.base.dbi.has(self.base.item_idx(key))) {
                        return key;
                    }
                }
            }
            pub fn del(self: *@This(), key: Key) !void {
                try self.base.del(key);
            }
            pub fn clear(self: *@This()) !void {
                try self.base.clear();
            }
            pub fn len(self: @This()) usize {
                return self.base.len();
            }
            pub fn append(self: *@This(), val: Val) !Key {
                const key = try self.gen();
                try self.base.append(key, val);
                return key;
            }
            pub fn get(self: @This(), key: Key) !Val {
                return try self.base.get(key);
            }
            pub fn has(self: @This(), key: Key) !bool {
                return try self.base.has(key);
            }
            pub fn iterator(self: @This()) ViewBase.Iterator {
                return self.base.iterator();
            }
            pub fn reverse_iterator(self: @This()) ViewBase.Iterator {
                return self.base.reverse_iterator();
            }
        };

        base: Base,

        pub fn init(txn: lmdb.Txn) !@This() {
            return .{ .base = try Base.init(txn) };
        }
        pub fn open(self: @This(), txn: lmdb.Txn) !View {
            return .{ .base = try self.base.open(txn) };
        }
    };
}

pub fn SetList(comptime K: type, comptime V: type) type {
    return struct {
        pub const Key = K;
        pub const Val = V;

        pub const Base = SetListBase(Key, Val);
        pub const View = struct {
            const ViewBase = SetListViewBase(Key, Val);

            base: ViewBase,

            pub fn del(self: *@This(), key: Key) !void {
                try self.base.del(key);
            }
            pub fn clear(self: *@This()) !void {
                try self.base.clear();
            }
            pub fn len(self: @This()) usize {
                return self.base.len();
            }
            pub fn append(self: *@This(), key: Key, val: Val) !void {
                try self.base.append(key, val);
            }
            pub fn get(self: @This(), key: Key) !Val {
                return try self.base.get(key);
            }
            pub fn has(self: @This(), key: Key) !bool {
                return try self.base.has(key);
            }
            pub fn iterator(self: @This()) ViewBase.Iterator {
                return self.base.iterator();
            }
            pub fn reverse_iterator(self: @This()) ViewBase.Iterator {
                return self.base.reverse_iterator();
            }
        };

        base: Base,

        pub fn init(txn: lmdb.Txn) !@This() {
            return .{ .base = try Base.init(txn) };
        }
        pub fn open(self: @This(), txn: lmdb.Txn) !View {
            return .{ .base = try self.base.open(txn) };
        }
    };
}

const DB_SIZE = 1024 * 1024 * 1;

test "db" {
    const env = try lmdb.Env.open("db", DB_SIZE);
    defer env.close();

    const txn = try env.txn();
    defer txn.commit() catch {};

    var db = try Db(u32, u32).init(txn, "123");
    var n: u32 = 456;
    if (try db.has(123)) {
        n = try db.get(123);
        n += 1;
    }
    try db.put(123, n);
    std.debug.print("n: {}\n", .{n});
}

// test "list" {
//     const env = try lmdb.Env.open("db", DB_SIZE);
//     defer env.close();

//     const txn = try env.txn();
//     defer txn.commit();

//     const db = List.init(txn, "b", u32);
// }

test "set" {
    var env = try lmdb.Env.open("db", 1024 * 1024 * 1);
    // env.sync();
    defer env.close();

    var txn = try env.txn();
    defer txn.commit() catch {};

    var dbi = try txn.dbi("abc");

    const A = struct {
        ml: Set(usize),
    };

    var a: A = undefined;
    const a_idx: u64 = 27;
    if (try dbi.has(a_idx)) {
        a = try dbi.get(a_idx, A);
    } else {
        a = A{ .ml = try Set(usize).init(txn) };
        try dbi.put(a_idx, a);
    }

    var ml = try a.ml.open(txn);

    const len = ml.len();
    std.debug.print("{}\n", .{len});
    try ml.append(len);
    std.debug.print("{}\n", .{try ml.has(len)});
    var it = ml.iterator();
    while (it.next()) |i| {
        std.debug.print("{}\n", .{i});
    }
}

test "list" {
    var env = try lmdb.Env.open("db", 1024 * 1024 * 1);
    // env.sync();
    defer env.close();

    var txn = try env.txn();
    defer txn.commit() catch {};

    var dbi = try txn.dbi("def");

    const A = struct {
        ml: List(usize),
    };

    var a: A = undefined;
    const a_idx: u64 = 27;
    if (try dbi.has(a_idx)) {
        a = try dbi.get(a_idx, A);
    } else {
        a = A{ .ml = try List(usize).init(txn) };
        try dbi.put(a_idx, a);
    }

    var ml = try a.ml.open(txn);

    const len = ml.len();
    std.debug.print("{}\n", .{len});
    const newest = try ml.append(len * 10);
    std.debug.print("{}: {}\n", .{ newest, try ml.get(newest) });
    var it = ml.iterator();
    while (it.next()) |i| {
        std.debug.print("{}: {}\n", .{ i.key, i.val });
    }
}

test "setlist" {
    var env = try lmdb.Env.open("db", 1024 * 1024 * 1);
    // env.sync();
    defer env.close();

    var txn = try env.txn();
    defer txn.commit() catch {};

    var dbi = try txn.dbi("ghi");

    const A = struct {
        ml: SetList(usize, usize),
    };

    var a: A = undefined;
    const a_idx: u64 = 27;
    if (try dbi.has(a_idx)) {
        a = try dbi.get(a_idx, A);
    } else {
        a = A{ .ml = try SetList(usize, usize).init(txn) };
        try dbi.put(a_idx, a);
    }

    var ml = try a.ml.open(txn);

    const len = ml.len();
    std.debug.print("{}\n", .{len});
    try ml.append(len, len * 10);
    std.debug.print("{}\n", .{try ml.get(len)});
    var it = ml.iterator();
    while (it.next()) |i| {
        std.debug.print("{}: {}\n", .{ i.key, i.val });
    }
}
