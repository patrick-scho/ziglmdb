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
pub fn Set(comptime K: type) type {
    return struct {
        idx: ?Index = null,

        const Self = @This();
        pub const Index = u64;
        pub const View = SetView(K);

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

pub fn SetView(comptime K: type) type {
    return struct {
        const Self = @This();
        const ItemIndex = struct { Set(K).Index, K };

        pub const Head = struct {
            len: usize = 0,
            first: ?K = null,
            last: ?K = null,
        };
        pub const Item = struct {
            next: ?K = null,
            prev: ?K = null,
        };

        dbi: lmdb.Dbi,
        idx: Set(K).Index,
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
        pub fn append(self: *Self, k: K) !void {
            if (self.head.len == 0) {
                const item = Item{};
                try self.item_put(k, item);

                self.head.len = 1;
                self.head.first = k;
                self.head.last = k;
                try self.head_update();
            } else {
                const prev_idx = self.head.last.?;
                var prev = try self.item_get(prev_idx);

                const item = Item{ .prev = prev_idx };
                try self.item_put(k, item);

                prev.next = k;
                try self.item_put(prev_idx, prev);

                self.head.last = k;
                self.head.len += 1;
                try self.head_update();
            }
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
        pub fn has(self: Self, k: K) !bool {
            return self.dbi.has(self.item_idx(k));
        }
        pub fn len(self: Self) usize {
            return self.head.len;
        }
        pub const Iterator = struct {
            sv: SetView(K),
            idx: ?K,
            dir: enum { Forward, Backward },

            pub fn next(self: *Iterator) ?K {
                if (self.idx != null) {
                    const k = self.idx.?;
                    const item = self.sv.item_get(k) catch return null;
                    self.idx = switch (self.dir) {
                        .Forward => item.next,
                        .Backward => item.prev,
                    };
                    return k;
                } else {
                    return null;
                }
            }
        };
        pub fn iterator(self: Self) Iterator {
            return .{
                .sv = self,
                .idx = self.head.first,
                .dir = .Forward,
            };
        }
        pub fn reverse_iterator(self: Self) Iterator {
            return .{
                .sv = self,
                .idx = self.head.last,
                .dir = .Backward,
            };
        }
    };
}
pub fn List(comptime V: type) type {
    return struct {
        idx: ?Index = null,

        const Self = @This();
        pub const Index = u64;
        pub const View = ListView(V);

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

pub fn ListView(comptime V: type) type {
    return struct {
        const Self = @This();
        const K = u64;
        const ItemIndex = struct { List(V).Index, K };

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
        idx: List(V).Index,
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
        fn gen(self: Self) !K {
            // TODO: limit loop
            while (true) {
                const k = try Prng.gen(self.dbi, K);
                if (!try self.dbi.has(self.item_idx(k))) {
                    return k;
                }
            }
        }
        pub fn append(self: *Self, v: V) !K {
            if (self.head.len == 0) {
                const k = try self.gen();
                const item = Item{ .data = v };
                try self.item_put(k, item);

                self.head.len = 1;
                self.head.first = k;
                self.head.last = k;
                try self.head_update();

                return k;
            } else {
                const prev_idx = self.head.last.?;
                var prev = try self.item_get(prev_idx);

                const k = try self.gen();
                const item = Item{ .prev = prev_idx, .data = v };
                try self.item_put(k, item);

                prev.next = k;
                try self.item_put(prev_idx, prev);

                self.head.last = k;
                self.head.len += 1;
                try self.head_update();

                return k;
            }
        }
        pub fn get(self: Self, k: K) !V {
            const item = try self.item_get(k);
            return item.data;
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
        pub fn len(self: Self) usize {
            return self.head.len;
        }
        pub const Iterator = struct {
            lv: ListView(V),
            idx: ?K,
            dir: enum { Forward, Backward },

            pub fn next(self: *Iterator) ?struct { key: K, val: V } {
                if (self.idx != null) {
                    const k = self.idx.?;
                    const item = self.lv.item_get(k) catch return null;
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
                .lv = self,
                .idx = self.head.first,
                .dir = .Forward,
            };
        }
        pub fn reverse_iterator(self: Self) Iterator {
            return .{
                .lv = self,
                .idx = self.head.last,
                .dir = .Backward,
            };
        }
    };
}

pub fn SetList(comptime K: type, comptime V: type) type {
    return struct {
        idx: ?Index = null,

        const Self = @This();
        pub const Index = u64;
        pub const View = SetListView(K, V);

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

pub fn SetListView(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        const ItemIndex = struct { SetList(K, V).Index, K };

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
        idx: SetList(K, V).Index,
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
        pub fn append(self: *Self, k: K, v: V) !void {
            if (self.head.len == 0) {
                const item = Item{ .data = v };
                try self.item_put(k, item);

                self.head.len = 1;
                self.head.first = k;
                self.head.last = k;
                try self.head_update();
            } else {
                const prev_idx = self.head.last.?;
                var prev = try self.item_get(prev_idx);

                const item = Item{ .prev = prev_idx, .data = v };
                try self.item_put(k, item);

                prev.next = k;
                try self.item_put(prev_idx, prev);

                self.head.last = k;
                self.head.len += 1;
                try self.head_update();
            }
        }
        pub fn get(self: Self, k: K) !V {
            const item = try self.item_get(k);
            return item.data;
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
        pub fn has(self: Self, k: K) !bool {
            return self.dbi.has(self.item_idx(k));
        }
        pub fn len(self: Self) usize {
            return self.head.len;
        }
        pub const Iterator = struct {
            slv: SetListView(K, V),
            idx: ?K,
            dir: enum { Forward, Backward },

            pub fn next(self: *Iterator) ?struct { key: K, val: V } {
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

test "maplist" {
    var env = try lmdb.Env.open("db", 1024 * 1024 * 1);
    // env.sync();
    defer env.close();

    var txn = try env.txn();
    defer txn.commit() catch {};

    var dbi = try txn.dbi("abc");

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
