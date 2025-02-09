const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const HashMapUnmanaged = std.HashMapUnmanaged;

const Literal = []const u8;
const Relation = struct { arity: u32, name: []const u8 };
const RelationHashEql = struct {
    pub fn hash(_: @This(), rel: Relation) u64 {
        return std.hash.Wyhash.hash(rel.arity, rel.name);
    }
    pub fn eql(_: @This(), this: Relation, that: Relation) bool {
        return this.arity == that.arity and std.mem.eql(u8, this.name, that.name);
    }
};
const Db = struct {
    arena: ArenaAllocator,
    relations: HashMapUnmanaged(
        Relation,
        RelationImplementation,
        RelationHashEql,
        std.hash_map.default_max_load_percentage,
    ),
};

const QTuple = []const ?Literal;
const RTuple = []const Literal;

const RelationImplementation = union(enum) {
    memory_seq: struct {
        relation: std.ArrayList(RTuple),
    },
    fn query(self: *RelationImplementation, tuple: QTuple) RelationIterator {
        return switch (self.*) {
            .memory_seq => |*rel| RelationIterator{
                .memory_seq = .{
                    .relation = &rel.relation,
                    .query = tuple,
                    .cursor = 0,
                },
            },
        };
    }
};

const RelationIterator = union(enum) {
    memory_seq: MemorySeqIterator,
    fn next(self: *RelationIterator) ?RTuple {
        return switch (self.*) {
            .memory_seq => |*impl| impl.next(),
        };
    }
};

const MemorySeqIterator = struct {
    relation: *std.ArrayList(RTuple),
    query: QTuple,
    cursor: u32,
    fn next(self: *MemorySeqIterator) ?RTuple {
        loop: while (self.cursor < self.relation.items.len) {
            const tuple = self.relation.items[self.cursor];
            self.cursor += 1;
            for (self.query, tuple) |q, t| {
                if (q) |spec| {
                    if (!std.mem.eql(u8, spec, t)) continue :loop;
                }
            }
            return tuple;
        }
        return null;
    }
};

test "Db" {
    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var relation = std.ArrayList(RTuple).init(arena.allocator());
    try relation.append(&[_]Literal{ "a", "b" });
    try relation.append(&[_]Literal{ "a", "c" });
    try relation.append(&[_]Literal{ "c", "z" });
    var impl = RelationImplementation{ .memory_seq = .{ .relation = relation } };
    var it = impl.query(&[_]?Literal{ "a", null });
    while (it.next()) |tuple| {
        try std.json.stringify(
            tuple,
            .{},
            std.io.getStdErr().writer(),
        );
    }
}
