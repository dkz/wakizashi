const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const DomainValue = union(enum) {
    seq: []const u8,
    fn clone(self: DomainValue, allocator: Allocator) Allocator.Error!DomainValue {
        switch (self) {
            .seq => |seq| {
                const target = try allocator.alloc(u8, seq.len);
                @memcpy(target, seq);
                return DomainValue{ .seq = target };
            },
        }
    }
    fn destroy(self: DomainValue, allocator: Allocator) void {
        switch (self) {
            .seq => |seq| allocator.free(seq),
        }
    }
};
const DomainValueHashEql = struct {
    pub fn eql(_: @This(), this: DomainValue, that: DomainValue) bool {
        return std.meta.eql(this, that);
    }
    pub fn hash(_: @This(), this: DomainValue) u64 {
        switch (this) {
            .seq => |seq| return std.hash.Wyhash.hash(0, seq),
        }
    }
};

/// Stealing this cool technique from bdddb:
/// instead of using multiple copies of the data and wasting CPU cycles on equality checks
/// store every encountered value in a unified domain index and assign a word id to it.
/// Highly unlikely that a dataset for any program exceeds word size (?).
const Domain = struct {
    /// Next unique index available for a domain value.
    sequence: u64 = 0,
    index: std.HashMap(DomainValue, u64, DomainValueHashEql, std.hash_map.default_max_load_percentage),
    /// Own, hash, and assign an unique id to this domain value.
    fn register(self: *Domain, value: DomainValue) Allocator.Error!u64 {
        const id = self.sequence;
        const copy = try value.clone(self.index.allocator);
        errdefer copy.destroy(self.index.allocator);
        try self.index.put(value, id);
        self.sequence += 1;
        return id;
    }
};

const ArrayListStorage = struct {
    arity: u32,
    backend: std.ArrayList(u64),
    fn insert(self: *ArrayListStorage, tuple: []const u64) Allocator.Error!void {
        std.debug.assert(self.arity == tuple.len);
        return self.backend.appendSlice(tuple);
    }
    fn iterator(self: *ArrayListStorage, query: []const ?u64) Iterator {
        return Iterator{
            .index = 0,
            .arity = self.arity,
            .query = query,
            .backend = &self.backend,
        };
    }
    const Iterator = struct {
        index: u32,
        arity: u32,
        query: []const ?u64,
        backend: *std.ArrayList(u64),
        fn next(self: *Iterator) ?[]const u64 {
            loop: while (self.index * self.arity < self.backend.items.len) : (self.index += 1) {
                const i = self.index;
                const a = self.arity;
                const slice = self.backend.items[i * a .. i * a + a];
                for (0..self.arity) |j| {
                    if (self.query[j]) |x| {
                        if (x != slice[j]) continue :loop;
                    }
                }
                self.index += 1;
                return slice;
            }
            return null;
        }
    };
};

test "ArrayListStorage" {
    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var storage = ArrayListStorage{
        .arity = 2,
        .backend = std.ArrayList(u64).init(arena.allocator()),
    };
    try storage.insert(&[_]u64{ 1, 0 });
    try storage.insert(&[_]u64{ 1, 1 });
    try storage.insert(&[_]u64{ 1, 2 });
    try storage.insert(&[_]u64{ 2, 0 });
    try storage.insert(&[_]u64{ 2, 1 });
    var it = storage.iterator(&[_]?u64{ 2, null });
    while (it.next()) |t| {
        std.debug.print("{any}\n", .{t});
    }
}
