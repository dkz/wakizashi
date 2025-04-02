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

    var stratum = engine.Stratum{};
    defer stratum.deinit(gpa.allocator());

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
        const file = try lang.ast.parse(a, source, source_arena.allocator());
        for (file.statements) |*rule| {
            try stratum.add(
                rule,
                &domain,
                domain_arena.allocator(),
                gpa.allocator(),
            );
        }
    }

    var facts_arena = ArenaAllocator.init(gpa.allocator());
    defer facts_arena.deinit();
    var facts = database.MemoryDb{ .allocator = facts_arena.allocator() };

    try stratum.evaluate(&facts, gpa.allocator());

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
