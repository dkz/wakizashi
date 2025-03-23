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

    for (stratum.items) |eval| {
        try pretty_print(eval, stdout);
    }

    try stdout_buffered.flush();
}

fn pretty_print(what: anytype, out: anytype) !void {
    try std.json.stringify(what, .{ .whitespace = .indent_2 }, out);
}
