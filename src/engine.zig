const std = @import("std");
const lang = @import("lang.zig");
const database = @import("database.zig");

const panic = std.debug.panic;
const assert = std.debug.assert;

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

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

pub const RuleEvaluator = struct {
    const Name = []const u8;
    const Bindings = std.StringHashMapUnmanaged(u64);
    const EncodedAtom = struct {
        source: *const lang.ast.Atom,
        encoded: []const Term,
        relation: database.Relation,
        const Term = union(enum) {
            wildcard,
            variable: Name,
            constant: u64,
        };
        /// Rewrite a regular ast.Atom into a domain encoded atom.
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
    source: *const lang.ast.Rule,
    head: EncodedAtom,
    body: []const EncodedAtom,
    pub fn init(
        rule: *const lang.ast.Rule,
        domain: *database.Domain,
        domain_allocator: Allocator,
        allocator: Allocator,
    ) !RuleEvaluator {
        const rh = try EncodedAtom.init(&rule.head, domain, domain_allocator, allocator);
        const rb = try allocator.alloc(EncodedAtom, rule.body.len);
        for (rule.body, 0..) |*atom, j| {
            rb[j] = try EncodedAtom.init(atom, domain, domain_allocator, allocator);
        }
        return RuleEvaluator{
            .source = rule,
            .head = rh,
            .body = rb,
        };
    }
    pub fn deinit(self: *RuleEvaluator, allocator: Allocator) void {
        self.head.deinit(allocator);
        for (self.body) |*atom| atom.deinit(allocator);
        allocator.free(self.body);
    }
    pub fn evaluate(
        self: *RuleEvaluator,
        from: database.Db,
        into: *database.tuples.DirectCollection,
        allocator: Allocator,
    ) !void {
        const buffer = try allocator.alloc(u64, self.head.relation.arity);
        defer allocator.free(buffer);
        const root = try allocator.create(Bindings);
        root.* = .empty;
        const InferenceState = struct { *Bindings, []const EncodedAtom };
        var queue = Queue(InferenceState).init(allocator);
        try queue.append(.{ root, from.body });
        while (queue.pop()) |state| {
            const bindings, const atoms = state;
            if (atoms.len == 0) {
                self.head.toTuple(bindings, buffer);
                into.insert(allocator, buffer);
            } else {
                const atom = atoms[0];
                const query = try allocator.alloc(?u64, atom.relation.arity);
                defer allocator.free(query);
                const tuple = try allocator.alloc(u64, atom.relation.arity);
                defer allocator.free(tuple);

                atom.toQuery(bindings, query);
                const it = try from.query(atom.relation, query, allocator);
                defer it.destroy();
                while (it.next(tuple)) {
                    const candidate = try atom.unify(tuple, bindings, allocator);
                    try queue.append(.{ candidate, atoms[1..] });
                }
            }
            bindings.deinit(allocator);
            allocator.destroy(bindings);
        }
    }
};
