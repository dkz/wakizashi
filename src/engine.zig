const std = @import("std");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const HashMapUnmanaged = std.HashMapUnmanaged;
const StringHashMapUnmanaged = std.StringHashMapUnmanaged;
const ArenaAllocator = std.heap.ArenaAllocator;

const Predicate = struct {
    arity: u64,
    name: []const u8,
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
    fn eql(this: Constant, that: Constant) bool {
        return std.mem.eql(u8, this.string, that.string);
    }
};

const Fact = struct {
    predicate: Predicate,
    terms: []const Constant,
};

const Variable = []const u8;

const Term = union(enum) {
    wildcard,
    constant: Constant,
    variable: Variable,
};

const Atom = struct {
    predicate: Predicate,
    terms: []const Term,
};

const Rule = struct {
    predicate: Predicate,
    variables: []const Variable,
    atoms: []const Atom,
};

fn Queue(comptime T: type) type {
    return struct {
        const Self = @This();
        const Backend = std.DoublyLinkedList(T);
        backend: Backend,
        allocator: Allocator,
        fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
                .backend = .{},
            };
        }
        fn append(self: *Self, elem: T) !void {
            const node = try self.allocator.create(Backend.Node);
            node.* = .{ .data = elem };
            self.backend.append(node);
        }
        fn pop(self: *Self) ?T {
            const first = self.backend.popFirst();
            if (first) |node| {
                const data = node.data;
                self.allocator.destroy(node);
                return data;
            } else {
                return null;
            }
        }
        fn deinit(self: *Self) void {
            while (self.pop()) {}
        }
    };
}

const Db = struct {
    const load_percentage = std.hash_map.default_max_load_percentage;
    const IndexedFacts = ArrayList([]const Constant);
    const FactDb = HashMapUnmanaged(
        Predicate,
        *IndexedFacts,
        PredicateContext,
        load_percentage,
    );
    arena: ArenaAllocator,
    facts: FactDb,
    facts_storage: ArrayListUnmanaged(Constant),
    fn init(allocator: Allocator) Db {
        return Db{
            .arena = ArenaAllocator.init(allocator),
            .facts_storage = .{},
            .facts = .{},
        };
    }
    fn addFact(self: *Db, fact: Fact) !void {
        const allocator = self.arena.allocator();
        const index = index: {
            if (self.facts.get(fact.predicate)) |idx| {
                break :index idx;
            } else {
                const idx = try allocator.create(IndexedFacts);
                errdefer allocator.destroy(idx);
                idx.* = IndexedFacts.init(allocator);
                errdefer idx.deinit();
                try self.facts.put(allocator, fact.predicate, idx);
                break :index idx;
            }
        };
        try index.ensureUnusedCapacity(1);
        // This is the irreversible effect, as it increases list length,
        // storage will hold undefined elements if function fails after
        // this point:
        const target = try self.facts_storage.addManyAsSlice(allocator, fact.terms.len);
        for (fact.terms, 0..) |constant, j| {
            target[j] = constant;
        }
        index.appendAssumeCapacity(target);
    }
    fn getFactsUnified(
        self: *Db,
        atom: Atom,
        root: *StringHashMapUnmanaged(Constant),
        into: *ArrayList(*StringHashMapUnmanaged(Constant)),
        arena: *ArenaAllocator,
    ) !void {
        const allocator = arena.allocator();
        if (self.facts.get(atom.predicate)) |index| {
            fact: for (index.items) |terms| {
                const bindings = try allocator.create(StringHashMapUnmanaged(Constant));
                bindings.* = try root.clone(allocator);
                for (atom.terms, terms) |pattern, value| {
                    switch (pattern) {
                        .wildcard => {},
                        .constant => |constant| {
                            if (!Constant.eql(constant, value)) {
                                bindings.deinit(allocator);
                                allocator.destroy(bindings);
                                continue :fact;
                            }
                        },
                        .variable => |variable| {
                            if (bindings.get(variable)) |existing| {
                                if (!Constant.eql(existing, value)) {
                                    bindings.deinit(allocator);
                                    allocator.destroy(bindings);
                                    continue :fact;
                                }
                            } else {
                                try bindings.put(allocator, variable, value);
                            }
                        },
                    }
                }
                try into.append(bindings);
            }
        }
    }
    fn getInfered(
        self: *Db,
        rule: Rule,
        into: *ArrayList(Fact),
        arena: *ArenaAllocator,
    ) !void {
        const InferenceState = struct { *StringHashMapUnmanaged(Constant), []const Atom };
        const allocator = arena.allocator();
        var queue = Queue(InferenceState).init(allocator);
        const root = try allocator.create(StringHashMapUnmanaged(Constant));
        root.* = .{};
        try queue.append(.{ root, rule.atoms });
        while (queue.pop()) |state| {
            const bindings, const atoms = state;
            if (atoms.len == 0) {
                const constants = try allocator.alloc(Constant, rule.variables.len);
                for (rule.variables, 0..) |variable, index| {
                    constants[index] = bindings.get(variable).?;
                }
                try into.append(.{
                    .predicate = rule.predicate,
                    .terms = constants,
                });
            } else {
                var unified = ArrayList(*StringHashMapUnmanaged(Constant)).init(allocator);
                defer unified.deinit();
                try self.getFactsUnified(
                    atoms[0],
                    bindings,
                    &unified,
                    arena,
                );
                for (unified.items) |candidate| {
                    try queue.append(.{ candidate, atoms[1..] });
                }
            }
            bindings.deinit(allocator);
            allocator.destroy(bindings);
        }
    }
    fn deinit(self: *Db) void {
        self.arena.deinit();
    }
};

test "Basic inference" {
    const edge = Predicate{ .name = "edge", .arity = 2 };
    const x = Constant{ .string = "x" };
    const y = Constant{ .string = "y" };
    const z = Constant{ .string = "z" };
    var db = Db.init(std.testing.allocator);
    defer db.deinit();
    try db.addFact(.{
        .predicate = edge,
        .terms = &[_]Constant{ x, y },
    });
    try db.addFact(.{
        .predicate = edge,
        .terms = &[_]Constant{ y, z },
    });
    const path = Predicate{ .name = "path", .arity = 2 };
    const rule = Rule{
        .predicate = path,
        .variables = &[_]Variable{ "A", "C" },
        .atoms = &[_]Atom{
            Atom{
                .predicate = edge,
                .terms = &[_]Term{
                    Term{ .variable = "A" },
                    Term{ .variable = "B" },
                },
            },
            Atom{
                .predicate = edge,
                .terms = &[_]Term{
                    Term{ .variable = "B" },
                    Term{ .variable = "C" },
                },
            },
        },
    };
    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const out = std.io.getStdErr().writer();
    var inferred = ArrayList(Fact).init(arena.allocator());
    defer inferred.deinit();
    try db.getInfered(
        rule,
        &inferred,
        &arena,
    );
    for (inferred.items) |fact| {
        try std.json.stringify(fact, .{}, out);
        try out.writeAll("\n");
    }
}
