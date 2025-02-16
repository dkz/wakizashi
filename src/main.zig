const std = @import("std");
const lang = @import("lang.zig");
const database = @import("database.zig");

const ArenaAllocator = std.heap.ArenaAllocator;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // Arena for sorce code related objects, like syntax tree.
    var source_arena = ArenaAllocator.init(gpa.allocator());
    defer source_arena.deinit();

    const stdout_handle = std.io.getStdOut();
    var stdout_buffered = std.io.bufferedWriter(stdout_handle.writer());
    const stdout = stdout_buffered.writer();

    const cwd = std.fs.cwd();
    var args = try std.process.argsWithAllocator(gpa.allocator());
    defer args.deinit();
    _ = args.skip();
    while (args.next()) |a| {
        var local = ArenaAllocator.init(gpa.allocator());
        defer local.deinit();
        var source_file = try cwd.openFile(a, .{});
        defer source_file.close();
        const source = source: {
            var reader = std.io.bufferedReader(source_file.reader());
            const rdr = reader.reader();
            break :source try rdr.readAllAlloc(source_arena.allocator(), 1024);
        };
        const file = try lang.ast.parse(a, source, &source_arena);
        for (file.statements) |statement| {
            const relation = database.Relation{
                .name = statement.head.predicate,
                .arity = statement.head.terms.len,
            };
            try pretty_print(relation, stdout);
        }
    }

    try stdout_buffered.flush();
}

fn pretty_print(what: anytype, out: anytype) !void {
    try std.json.stringify(what, .{ .whitespace = .indent_2 }, out);
}
