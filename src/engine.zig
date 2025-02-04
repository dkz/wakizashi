const std = @import("std");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMapUnmanaged = std.HashMapUnmanaged;
const StringHashMapUnmanaged = std.StringHashMapUnmanaged;
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
    fn eql(this: Constant, that: Constant) bool {
        return std.mem.eql(u8, this.string, that.string);
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

const Variable = []const u8;

const Term = union(enum) {
    wildcard,
    constant: Constant,
    variable: Variable,
    fn copyOwnedBy(self: Term, allocator: Allocator) !Term {
        return switch (self) {
            .wildcard => Term.wildcard,
            .constant => |c| Term{
                .constant = try c.copyOwnedBy(allocator),
            },
            .variable => |v| Term{
                .variable = variable: {
                    const copy = try allocator.alloc(u8, v.len);
                    @memcpy(copy, v);
                    break :variable copy;
                },
            },
        };
    }
};

const Atom = struct {
    predicate: Predicate,
    terms: []const Term,
    fn copyOwnedBy(self: Atom, owner: Allocator) !Atom {
        var arena = ArenaAllocator.init(owner);
        errdefer arena.deinit();
        {
            const allocator = arena.allocator();
            const pc = try self.predicate.copyOwnedBy(allocator);
            const tc = try allocator.alloc(Term, self.terms.len);
            for (self.terms, 0..) |term, idx| {
                tc[idx] = try term.copyOwnedBy(allocator);
            }
            return Atom{
                .predicate = pc,
                .terms = tc,
            };
        }
    }
};

const Rule = struct {
    variables: []Variable,
    atoms: []const Atom,
    fn init(
        owner: Allocator,
        source_variables: []const Variable,
        source_atoms: []const Atom,
    ) !Rule {
        var arena = ArenaAllocator.init(owner);
        errdefer arena.deinit();
        {
            const allocator = arena.allocator();
            const target_variables: []Variable = try allocator.alloc(Variable, source_variables.len);
            for (source_variables, 0..) |sv, idx| {
                const mem = try allocator.alloc(u8, sv.len);
                @memcpy(mem, sv);
                target_variables[idx] = mem;
            }
            const target_atoms = try allocator.alloc(Atom, source_atoms.len);
            for (source_atoms, 0..) |sa, idx| {
                target_atoms[idx] = try sa.copyOwnedBy(allocator);
            }
            return Rule{
                .variables = target_variables,
                .atoms = target_atoms,
            };
        }
    }
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
    const FactDb = HashMapUnmanaged(
        Predicate,
        *ArrayList(Fact),
        PredicateContext,
        load_percentage,
    );
    const RuleDb = HashMapUnmanaged(
        Predicate,
        *ArrayList(Rule),
        PredicateContext,
        load_percentage,
    );
    arena: ArenaAllocator,
    facts: FactDb,
    rules: RuleDb,
    fn init(owner: Allocator) Db {
        return Db{
            .arena = ArenaAllocator.init(owner),
            .facts = .{},
            .rules = .{},
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
                try self.facts.put(
                    self.arena.allocator(),
                    key,
                    coll,
                );
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
    fn getFactsUnified(
        self: *Db,
        atom: Atom,
        root: *StringHashMapUnmanaged(Constant),
        into: *ArrayList(*StringHashMapUnmanaged(Constant)),
        arena: *ArenaAllocator,
    ) !void {
        const allocator = arena.allocator();
        if (self.getFacts(atom.predicate)) |db| {
            fact: for (db) |fact| {
                const bindings = try allocator.create(StringHashMapUnmanaged(Constant));
                bindings.* = try root.clone(allocator);
                for (atom.terms, fact.constants) |pattern, value| {
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
                try into.append(.{ .constants = constants });
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
    fn addRule(
        self: *Db,
        predicate: Predicate,
        variables: []const Variable,
        atoms: []const Atom,
    ) !void {
        var local = ArenaAllocator.init(self.arena.allocator());
        errdefer local.deinit();
        const allocator = local.allocator();
        const rule = try Rule.init(allocator, variables, atoms);
        {
            if (self.rules.get(predicate)) |ptr| {
                try ptr.append(rule);
            } else {
                const key = try predicate.copyOwnedBy(allocator);
                var coll = try allocator.create(ArrayList(Rule));
                coll.* = ArrayList(Rule).init(allocator);
                try coll.append(rule);
                try self.rules.put(
                    self.arena.allocator(),
                    key,
                    coll,
                );
            }
        }
    }
    fn getRules(self: *Db, predicate: Predicate) ?[]const Rule {
        if (self.rules.get(predicate)) |ptr| {
            return ptr.items;
        } else {
            return null;
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
    try db.addFact(edge, &[_]Constant{ x, y });
    try db.addFact(edge, &[_]Constant{ y, z });
    const path = Predicate{ .name = "path", .arity = 2 };
    try db.addRule(path, &[_]Variable{ "A", "C" }, &[_]Atom{ Atom{
        .predicate = edge,
        .terms = &[_]Term{
            Term{ .variable = "A" },
            Term{ .variable = "B" },
        },
    }, Atom{
        .predicate = edge,
        .terms = &[_]Term{
            Term{ .variable = "B" },
            Term{ .variable = "C" },
        },
    } });
    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const out = std.io.getStdErr().writer();
    if (db.getRules(path)) |rules| {
        for (rules) |rule| {
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
    }
}
