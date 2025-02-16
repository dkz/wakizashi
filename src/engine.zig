const std = @import("std");
const lang = @import("lang.zig");
const database = @import("database.zig");

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

const Bindings = std.StringHashMapUnmanaged(u64);

/// Atom, rewritten with relation information and tuple structure.
const DomainTuple = struct {
    const DomainTerm = union(enum) {
        wildcard,
        variable: []const u8,
        constant: u64,
    };
    relation: database.Relation,
    encoded: []const DomainTerm,
    source: *const lang.ast.Atom,
    fn init(
        atom: *const lang.ast.Atom,
        domain: *database.Domain,
        allocator: Allocator,
    ) Allocator.Error!DomainTuple {
        const target_relation = database.Relation{
            .name = atom.predicate,
            .arity = atom.terms.len,
        };
        const tuple = try allocator.alloc(DomainTerm, target_relation.arity);
        errdefer allocator.free(tuple);
        for (atom.terms, 0..) |term, j| {
            switch (term) {
                .wildcard => {
                    tuple[j] = .wildcard;
                },
                .variable => |name| {
                    tuple[j] = DomainTerm{ .variable = name };
                },
                .literal => |value| {
                    tuple[j] = DomainTerm{
                        // TODO Do something about rewriter using the same allocator for rewrites
                        // and pushing values to the underlying domain.
                        .constant = try domain.register(allocator, database.DomainValue{ .seq = value }),
                    };
                },
            }
        }
        return DomainTuple{
            .relation = target_relation,
            .encoded = tuple,
            .source = atom,
        };
    }
    fn initQuery(
        self: *const DomainTuple,
        bindings: *Bindings,
        query: []?u64,
    ) void {
        std.debug.assert(self.relation.arity == query.len);
        for (query, 0..) |*q, j| {
            switch (self.encoded[j]) {
                .wildcard => {
                    q.* = null;
                },
                .variable => |variable| {
                    q.* = bindings.get(variable);
                },
                .constant => |encoded| {
                    q.* = encoded;
                },
            }
        }
    }
    fn initTuple(
        self: *const DomainTuple,
        bindings: *Bindings,
        into: []u64,
    ) void {
        std.debug.assert(self.relation.arity == into.len);
        for (into, 0..) |*t, j| {
            switch (self.encoded[j]) {
                .wildcard => unreachable,
                .variable => |variable| t.* = bindings.get(variable).?,
                .constant => |encoded| t.* = encoded,
            }
        }
    }
    fn unify(
        self: *const DomainTuple,
        tuple: []const u64,
        bindings: *Bindings,
        allocator: Allocator,
    ) Allocator.Error!*Bindings {
        std.debug.assert(self.relation.arity == tuple.len);
        const result = try allocator.create(Bindings);
        errdefer allocator.destroy(result);
        result.* = try bindings.clone(allocator);
        errdefer result.deinit(allocator);
        for (self.encoded, tuple) |e, t| {
            switch (e) {
                .wildcard => {},
                .constant => {},
                .variable => |variable| {
                    if (!bindings.contains(variable)) {
                        try result.put(allocator, variable, t);
                    }
                },
            }
        }
        return result;
    }
};

const DomainRule = struct {
    head: DomainTuple,
    body: []const DomainTuple,
    source: *const lang.ast.Rule,
    fn init(rule: *const lang.ast.Rule, domain: *database.Domain, allocator: Allocator) Allocator.Error!DomainRule {
        const rule_head = try DomainTuple.init(&rule.head, domain, allocator);
        const rule_body = try allocator.alloc(DomainTuple, rule.body.len);
        errdefer allocator.free(rule_body);
        for (rule.body, 0..) |*atom, j| {
            rule_body[j] = try DomainTuple.init(atom, domain, allocator);
        }
        return DomainRule{
            .head = rule_head,
            .body = rule_body,
            .source = rule,
        };
    }
};

const Evaluator = struct {
    db: database.Db = .{},
    domain: database.Domain = .{},
    program: std.ArrayListUnmanaged(DomainRule) = .{},
    fn read(self: *Evaluator, file: *const lang.ast.File, allocator: Allocator) Allocator.Error!void {
        try self.program.ensureUnusedCapacity(allocator, file.statements.len);
        for (file.statements) |*statement| {
            const rule = try DomainRule.init(statement, &self.domain, allocator);
            try self.db.registerArrayBackend(allocator, rule.head.relation);
            self.program.appendAssumeCapacity(rule);
        }
    }
    fn unify(self: *Evaluator, from: *const DomainTuple, with: *Bindings, into: *std.ArrayList(*Bindings), arena: *ArenaAllocator) !void {
        const allocator = arena.allocator();
        const query = try allocator.alloc(?u64, from.relation.arity);
        defer allocator.free(query);
        const result = try allocator.alloc(u64, from.relation.arity);
        defer allocator.free(result);
        from.initQuery(with, query);
        var it = try self.db.query(allocator, from.relation, query);
        defer it.destroy();
        while (it.next(result)) {
            try into.append(try from.unify(result, with, allocator));
        }
    }
    fn infer(
        self: *Evaluator,
        from: *const DomainRule,
        into: *std.ArrayList([]const u64),
        arena: *ArenaAllocator,
    ) !void {
        const allocator = arena.allocator();
        const root = try allocator.create(Bindings);
        root.* = .{};
        const InferenceState = struct { *Bindings, []const DomainTuple };
        var queue = Queue(InferenceState).init(allocator);
        try queue.append(.{ root, from.body });
        while (queue.pop()) |state| {
            const bindings, const atoms = state;
            if (atoms.len == 0) {
                const tuple = try allocator.alloc(u64, from.head.relation.arity);
                errdefer allocator.free(tuple);
                from.head.initTuple(bindings, tuple);
                try into.append(tuple);
            } else {
                var unified = std.ArrayList(*Bindings).init(allocator);
                defer unified.deinit();
                try self.unify(&atoms[0], bindings, &unified, arena);
                for (unified.items) |candidate| {
                    try queue.append(.{ candidate, atoms[1..] });
                }
            }
            bindings.deinit(allocator);
            allocator.destroy(bindings);
        }
    }
    fn iterate(
        self: *Evaluator,
        arena: *ArenaAllocator,
    ) !void {
        const allocator = arena.allocator();
        for (self.program.items) |*rule| {
            var inferred = std.ArrayList([]const u64).init(allocator);
            defer inferred.deinit();
            try self.infer(rule, &inferred, arena);
            for (inferred.items) |i| {
                try self.db.insert(allocator, rule.head.relation, i);
            }
        }
    }
};

test "Basic inference" {
    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var evaluator = Evaluator{};
    const program =
        \\edge(x, y).
        \\edge(y, z).
        \\path(A, B) :- edge(A, C), edge(C, B).
    ;
    const ast = try lang.ast.parse(".", program, &arena);
    try evaluator.read(&ast, arena.allocator());
    try evaluator.iterate(&arena);
    {
        var t: [2]u64 = undefined;
        var d: [2]database.DomainValue = undefined;
        var it = try evaluator.db.query(
            arena.allocator(),
            database.Relation{ .arity = 2, .name = "edge" },
            &[2]?u64{ null, null },
        );
        defer it.destroy();
        while (it.next(&t)) {
            evaluator.domain.decodeTuple(&t, &d);
            std.debug.print("edge({s}, {s})\n", .{ d[0].seq, d[1].seq });
        }
    }
}
