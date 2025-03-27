const std = @import("std");
const lang = @import("lang.zig");
const database = @import("database.zig");

const panic = std.debug.panic;
const assert = std.debug.assert;

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

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

const Name = []const u8;
const Bindings = std.StringHashMapUnmanaged(u64);

const EncodedAtom = struct {
    source: *const lang.ast.Atom,
    relation: database.Relation,
    encoded: []const Term,
    const Term = union(enum) {
        wildcard,
        variable: Name,
        constant: u64,
    };
    // Rewrite a regular ast.Atom into a domain encoded atom.
    fn init(
        atom: *const lang.ast.Atom,
        domain: *database.Domain,
        domain_allocator: Allocator,
        allocator: Allocator,
    ) Allocator.Error!EncodedAtom {
        const relation = database.Relation{
            .name = atom.predicate,
            .arity = atom.terms.len,
        };
        const tuple = try allocator.alloc(Term, relation.arity);
        for (atom.terms, 0..) |term, j| {
            tuple[j] = switch (term) {
                .wildcard => .wildcard,
                .variable => |name| Term{ .variable = name },
                .literal => |value| Term{
                    .constant = try domain.register(
                        domain_allocator,
                        database.DomainValue{ .binary = value },
                    ),
                },
            };
        }
        return EncodedAtom{
            .source = atom,
            .encoded = tuple,
            .relation = relation,
        };
    }
    fn deinit(self: *const EncodedAtom, allocator: Allocator) void {
        allocator.free(self.encoded);
    }
    /// Given currently bound variables, transform the atom into a query.
    fn toQuery(self: *const EncodedAtom, bindings: *Bindings, into: []?u64) void {
        assert(self.relation.arity == into.len);
        for (into, 0..) |*q, j| {
            switch (self.encoded[j]) {
                .wildcard => q.* = null,
                .constant => |encoded| q.* = encoded,
                .variable => |variable| q.* = bindings.get(variable),
            }
        }
    }
    /// Given a set of bound variables, transform the atom into a tuple.
    fn toTuple(self: *const EncodedAtom, bindings: *Bindings, into: []u64) void {
        assert(self.relation.arity == into.len);
        for (into, 0..) |*t, j| {
            t.* = switch (self.encoded[j]) {
                .wildcard => unreachable,
                .constant => |encoded| encoded,
                .variable => |name| bindings.get(name).?,
            };
        }
    }
    /// Given a query result, produce a new copy of bindings with
    /// variable from the tuple bound to their correspoding names.
    fn unify(
        self: *const EncodedAtom,
        tuple: []const u64,
        bindings: *Bindings,
        allocator: Allocator,
    ) Allocator.Error!*Bindings {
        assert(tuple.len == self.relation.arity);
        const copy = try allocator.create(Bindings);
        copy.* = try bindings.clone(allocator);
        for (self.encoded, tuple) |e, t| {
            switch (e) {
                .wildcard => {},
                .constant => {},
                .variable => |name| {
                    if (!bindings.contains(name)) {
                        try copy.put(allocator, name, t);
                    }
                },
            }
        }
        return copy;
    }
};

const EncodedRule = struct {
    source: *const lang.ast.Rule,
    head: EncodedAtom,
    body: []const EncodedAtom,
};

pub const Evaluator = struct {
    rule: *const EncodedRule,
    delta_index: ?usize = null,
    pub fn evaluate(
        self: *Evaluator,
        idb: database.Db,
        delta: database.Db,
        into: *database.tuples.DirectCollection,
        allocator: Allocator,
    ) !void {
        const buffer = try allocator.alloc(u64, self.rule.head.relation.arity);
        defer allocator.free(buffer);
        const root = try allocator.create(Bindings);
        root.* = .empty;

        const InferenceState = struct { *Bindings, usize };
        var queue = Queue(InferenceState).init(allocator);
        try queue.append(.{ root, 0 });
        while (queue.pop()) |state| {
            const bindings, const j = state;
            if (self.rule.body.len == j) {
                self.rule.head.toTuple(bindings, buffer);
                try into.insert(allocator, buffer);
            } else {
                const atom = self.rule.body[j];
                const query = try allocator.alloc(?u64, atom.relation.arity);
                defer allocator.free(query);
                const tuple = try allocator.alloc(u64, atom.relation.arity);
                defer allocator.free(tuple);

                atom.toQuery(bindings, query);
                const from = from: {
                    if (self.delta_index) |di| {
                        if (di == j) break :from delta;
                    }
                    break :from idb;
                };
                const it = try from.query(atom.relation, query, allocator);
                defer it.destroy();
                while (it.next(tuple)) {
                    const candidate = try atom.unify(tuple, bindings, allocator);
                    try queue.append(.{ candidate, j + 1 });
                }
            }
            bindings.deinit(allocator);
            allocator.destroy(bindings);
        }
    }
};

pub const Stratum = struct {
    rules: std.SegmentedList(EncodedRule, 0) = .{},
    evals: ArrayListUnmanaged(Evaluator) = .empty,
    inits: ArrayListUnmanaged(Evaluator) = .empty,
    pub fn deinit(self: *Stratum, allocator: Allocator) void {
        var it = self.rules.iterator(0);
        while (it.next()) |ptr| {
            for (ptr.body) |*a| a.deinit(allocator);
            ptr.head.deinit(allocator);
            allocator.free(ptr.body);
        }
        self.rules.deinit(allocator);
        self.evals.deinit(allocator);
        self.inits.deinit(allocator);
    }
    pub fn add(
        self: *Stratum,
        rule: *const lang.ast.Rule,
        domain: *database.Domain,
        domain_allocator: Allocator,
        allocator: Allocator,
    ) Allocator.Error!void {
        const encoded = try self.rules.addOne(allocator);
        {
            const body = try allocator.alloc(EncodedAtom, rule.body.len);
            const head = try EncodedAtom.init(
                &rule.head,
                domain,
                domain_allocator,
                allocator,
            );
            for (rule.body, 0..) |*atom, j| {
                body[j] = try EncodedAtom.init(
                    atom,
                    domain,
                    domain_allocator,
                    allocator,
                );
            }
            encoded.* = EncodedRule{
                .source = rule,
                .head = head,
                .body = body,
            };
        }
        try self.inits.append(allocator, Evaluator{ .rule = encoded });
        for (0..rule.body.len) |j| {
            try self.evals.append(allocator, Evaluator{
                .rule = encoded,
                .delta_index = j,
            });
        }
    }
    pub fn evaluate(
        self: *Stratum,
        facts: *database.MemoryDb,
        allocator: Allocator,
    ) !void {
        var delta_arena = ArenaAllocator.init(allocator);
        var delta = database.MemoryDb{ .allocator = delta_arena.allocator() };
        for (self.inits.items) |*eval| {
            const relation = eval.rule.head.relation;
            var derived = database.tuples.DirectCollection{ .arity = relation.arity };
            defer derived.deinit(allocator);
            try eval.evaluate(facts.db(), delta.db(), &derived, allocator);
            const it = try derived.iterator(allocator);
            defer it.destroy();
            _ = try delta.store(relation, it, allocator);
        }
        try facts.merge(&delta, allocator);

        var current_arena = delta_arena;
        var current_delta = delta;
        while (true) {
            var future_arena = ArenaAllocator.init(allocator);
            var future_delta = database.MemoryDb{ .allocator = future_arena.allocator() };
            var new = false;
            for (self.evals.items) |*eval| {
                const relation = eval.rule.head.relation;
                var derived = database.tuples.DirectCollection{ .arity = relation.arity };
                defer derived.deinit(allocator);
                try eval.evaluate(facts.db(), current_delta.db(), &derived, allocator);
                const it = try derived.iterator(allocator);
                defer it.destroy();
                const buffer = try allocator.alloc(u64, relation.arity);
                defer allocator.free(buffer);
                while (it.next(buffer)) {
                    if (try facts.storeTuple(relation, buffer)) {
                        new = try future_delta.storeTuple(relation, buffer) or new;
                    }
                }
            }
            if (new) {
                current_arena.deinit();
                current_delta = future_delta;
                current_arena = future_arena;
            } else {
                current_arena.deinit();
                future_arena.deinit();
                break;
            }
        }
    }
};
