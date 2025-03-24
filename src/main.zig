const std = @import("std");

const lang = @import("lang.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");

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

    var stratum: ArrayListUnmanaged(engine.RuleEvaluator) = .empty;
    defer stratum.deinit(gpa.allocator());
    defer {
        for (stratum.items) |*rule| rule.deinit(gpa.allocator());
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
            try stratum.append(
                gpa.allocator(),
                try engine.RuleEvaluator.init(
                    rule,
                    &domain,
                    domain_arena.allocator(),
                    gpa.allocator(),
                ),
            );
        }
    }

    var facts_arena = ArenaAllocator.init(gpa.allocator());
    defer facts_arena.deinit();
    var facts = database.MemoryDb{ .allocator = facts_arena.allocator() };

    while (true) {
        var new: usize = 0;
        for (stratum.items) |*eval| {
            const relation = eval.head.relation;
            var derived = database.tuples.DirectCollection{ .arity = relation.arity };
            defer derived.deinit(gpa.allocator());
            try eval.evaluate(facts.db(), &derived, gpa.allocator());
            const it = try derived.iterator(gpa.allocator());
            defer it.destroy();
            new += try facts.store(relation, it, gpa.allocator());
        }
        if (new == 0) break;
    }

    var it = facts.relations.iterator();
    while (it.next()) |e| {
        const allocator = gpa.allocator();
        try stdout.print("{s}/{}:\n", .{
            e.key_ptr.name,
            e.key_ptr.arity,
        });
        const buffer = try allocator.alloc(u64, e.key_ptr.arity);
        defer allocator.free(buffer);
        const values = try allocator.alloc(database.DomainValue, e.key_ptr.arity);
        defer allocator.free(values);

        const backend = e.value_ptr.queries();
        var ts = try backend.each(gpa.allocator());
        defer ts.destroy();
        while (ts.next(buffer)) {
            domain.decodeTuple(buffer, values);
            try stdout.writeAll("\t(");
            for (values, 1..) |*v, j| {
                try stdout.writeAll(v.binary);
                if (j < values.len) {
                    try stdout.writeAll(", ");
                }
            }
            try stdout.writeAll(")\n");
        }
    }

    try stdout_buffered.flush();
}

fn pretty_print(what: anytype, out: anytype) !void {
    try std.json.stringify(what, .{ .whitespace = .indent_2 }, out);
}
