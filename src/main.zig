const std = @import("std");

const lang = @import("lang.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const stdout_handle = std.io.getStdOut();
    var stdout_buffered = std.io.bufferedWriter(stdout_handle.writer());
    const stdout = stdout_buffered.writer();

    var domain = database.Domain{};
    var domain_arena = ArenaAllocator.init(gpa.allocator());
    defer domain_arena.deinit();

    var source_arena = ArenaAllocator.init(gpa.allocator());
    defer source_arena.deinit();

    var stratum_rules: ArrayListUnmanaged(engine.EncodedRule) = .empty;
    defer stratum_rules.deinit(gpa.allocator());
    defer {
        for (stratum_rules.items) |*rule| rule.deinit(gpa.allocator());
    }
    var round1: ArrayListUnmanaged(engine.Evaluator) = .empty;
    defer round1.deinit(gpa.allocator());
    defer {
        for (round1.items) |*eval| eval.deinit(gpa.allocator());
    }
    var stratum: ArrayListUnmanaged(engine.Evaluator) = .empty;
    defer stratum.deinit(gpa.allocator());
    defer {
        for (stratum.items) |*eval| eval.deinit(gpa.allocator());
    }

    const cwd = std.fs.cwd();
    var args = try std.process.argsWithAllocator(gpa.allocator());
    defer args.deinit();
    _ = args.skip();
    while (args.next()) |a| {
        var source_file = try cwd.openFile(a, .{});
        defer source_file.close();
        const source = source: {
            var reader = std.io.bufferedReader(source_file.reader());
            const rdr = reader.reader();
            break :source try rdr.readAllAlloc(source_arena.allocator(), 1024 * 8);
        };
        const file = try lang.ast.parse(a, source, &source_arena);
        for (file.statements) |*rule| {
            try stratum_rules.append(
                gpa.allocator(),
                try engine.EncodedRule.init(
                    rule,
                    &domain,
                    domain_arena.allocator(),
                    gpa.allocator(),
                ),
            );
        }
    }

    for (stratum_rules.items) |*rule| {
        try round1.append(gpa.allocator(), try rule.naive(gpa.allocator()));
        try rule.semi(&stratum, gpa.allocator());
    }

    var facts_arena = ArenaAllocator.init(gpa.allocator());
    defer facts_arena.deinit();
    var facts = database.MemoryDb{ .allocator = facts_arena.allocator() };

    var delta_arena = ArenaAllocator.init(gpa.allocator());
    var delta = database.MemoryDb{ .allocator = delta_arena.allocator() };

    for (round1.items) |*eval| {
        const relation = eval.rule.head.relation;
        var derived = database.tuples.DirectCollection{ .arity = relation.arity };
        defer derived.deinit(gpa.allocator());
        try eval.evaluate(facts.db(), delta.db(), &derived, gpa.allocator());
        const it = try derived.iterator(gpa.allocator());
        defer it.destroy();
        _ = try delta.store(relation, it, gpa.allocator());
    }

    try facts.merge(&delta, gpa.allocator());

    var current_arena = delta_arena;
    var current_delta = delta;
    while (true) {
        try stdout.writeAll("DELTA:\n");
        try print_db(stdout, &domain, &current_delta, gpa.allocator());
        var future_arena = ArenaAllocator.init(gpa.allocator());
        var future_delta = database.MemoryDb{ .allocator = future_arena.allocator() };
        var new = false;
        for (stratum.items) |*eval| {
            const relation = eval.rule.head.relation;
            const vb = try gpa.allocator().alloc(database.DomainValue, relation.arity);
            defer gpa.allocator().free(vb);
            var derived = database.tuples.DirectCollection{ .arity = relation.arity };
            defer derived.deinit(gpa.allocator());
            try eval.evaluate(facts.db(), current_delta.db(), &derived, gpa.allocator());
            const it = try derived.iterator(gpa.allocator());
            defer it.destroy();
            const buffer = try gpa.allocator().alloc(u64, relation.arity);
            defer gpa.allocator().free(buffer);
            while (it.next(buffer)) {
                if (try facts.storeTuple(relation, buffer)) {
                    try stdout.writeAll(relation.name);
                    try print_tuple(stdout, &domain, buffer, vb);
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

    try stdout.writeAll("FACTS:\n");
    try print_db(stdout, &domain, &facts, gpa.allocator());
    try stdout_buffered.flush();
}

fn print_db(
    into: anytype,
    domain: *database.Domain,
    db: *database.MemoryDb,
    allocator: Allocator,
) !void {
    var it = db.relations.iterator();
    while (it.next()) |e| {
        try into.print("{s}/{}:\n", .{
            e.key_ptr.name,
            e.key_ptr.arity,
        });
        const buffer = try allocator.alloc(u64, e.key_ptr.arity);
        defer allocator.free(buffer);
        const values = try allocator.alloc(database.DomainValue, e.key_ptr.arity);
        defer allocator.free(values);

        const backend = e.value_ptr.queries();
        var ts = try backend.each(allocator);
        defer ts.destroy();
        while (ts.next(buffer)) {
            try into.writeAll("\t");
            try print_tuple(into, domain, buffer, values);
        }
    }
}

fn print_tuple(
    into: anytype,
    domain: *database.Domain,
    tuple: []const u64,
    values: []database.DomainValue,
) !void {
    domain.decodeTuple(tuple, values);
    try into.writeAll("(");
    for (values, 1..) |*v, j| {
        try into.writeAll(v.binary);
        if (j < values.len) {
            try into.writeAll(", ");
        }
    }
    try into.writeAll(")\n");
}

fn pretty_print(what: anytype, out: anytype) !void {
    try std.json.stringify(what, .{ .whitespace = .indent_2 }, out);
}
