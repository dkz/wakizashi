const std = @import("std");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMapUnmanaged = std.HashMapUnmanaged;
const ArenaAllocator = std.heap.ArenaAllocator;

const Predicate = struct {
    arity: u64,
    name: []const u8,
    fn copyOwnedBy(self: Predicate, owner: Allocator) !Predicate {
        return Predicate{
            .arity = self.arity,
            .name = name: {
                const copy = try owner.alloc(u8, self.name.len);
                @memcpy(copy, self.name);
                break :name copy;
            },
        };
    }
};

const PredicateContext = struct {
    pub fn hash(self: @This(), key: Predicate) u64 {
        _ = self;
        return std.hash.Wyhash.hash(key.arity, key.name);
    }
    pub fn eql(self: @This(), this: Predicate, that: Predicate) bool {
        _ = self;
        return this.arity == that.arity and std.mem.eql(u8, this.name, that.name);
    }
};

const Constant = union(enum) {
    string: []const u8,
    fn copyOwnedBy(self: Constant, allocator: Allocator) !Constant {
        switch (self) {
            .string => |string| return .{ .string = string: {
                const copy = try allocator.alloc(u8, string.len);
                @memcpy(copy, self.string);
                break :string copy;
            } },
        }
    }
};

const Fact = struct {
    constants: []const Constant,
    fn init(owner: Allocator, constants: []const Constant) !Fact {
        var arena = ArenaAllocator.init(owner);
        errdefer arena.deinit();
        {
            const allocator = arena.allocator();
            const copy = try allocator.alloc(Constant, constants.len);
            for (constants, 0..) |constant, idx| {
                copy[idx] = try constant.copyOwnedBy(allocator);
            }
            return .{ .constants = copy };
        }
    }
};

const Db = struct {
    const load_precentage = std.hash_map.default_max_load_percentage;
    const FactDb = HashMapUnmanaged(
        Predicate,
        *ArrayList(Fact),
        PredicateContext,
        load_precentage,
    );
    arena: ArenaAllocator,
    facts: FactDb,
    fn init(owner: Allocator) Db {
        return Db{
            .arena = ArenaAllocator.init(owner),
            .facts = .{},
        };
    }
    fn addFact(self: *Db, predicate: Predicate, constants: []const Constant) !void {
        var local = ArenaAllocator.init(self.arena.allocator());
        errdefer local.deinit();
        const allocator = local.allocator();
        const fact = try Fact.init(allocator, constants);
        {
            if (self.facts.get(predicate)) |ptr| {
                try ptr.append(fact);
            } else {
                const key = try predicate.copyOwnedBy(allocator);
                var coll = try allocator.create(ArrayList(Fact));
                coll.* = ArrayList(Fact).init(allocator);
                try coll.append(fact);
                try self.facts.put(allocator, key, coll);
            }
        }
    }
    fn getFacts(self: *Db, predicate: Predicate) ?[]const Fact {
        if (self.facts.get(predicate)) |ptr| {
            return ptr.items;
        } else {
            return null;
        }
    }
    fn deinit(self: *Db) void {
        self.arena.deinit();
    }
};

/// Populate facts with data allocated on stack to veiry copies work as intended.
fn populate(db: *Db) !void {
    const edge = Predicate{ .name = "edge", .arity = 2 };
    const x = Constant{ .string = "x" };
    const y = Constant{ .string = "y" };
    const z = Constant{ .string = "z" };
    try db.addFact(edge, &[_]Constant{ x, y });
    try db.addFact(edge, &[_]Constant{ y, z });
}
test "Db ownership" {
    var db: Db = Db.init(std.testing.allocator);
    defer db.deinit();
    try populate(&db);
    if (db.getFacts(.{ .name = "edge", .arity = 2 })) |facts| {
        for (facts) |f| {
            std.debug.print(
                "({s}, {s}).\n",
                .{
                    f.constants[0].string,
                    f.constants[1].string,
                },
            );
        }
    }
}
