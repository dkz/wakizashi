const std = @import("std");
const lang = @import("lang.zig");
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    {
        const cwd = std.fs.cwd();
        var args = try std.process.argsWithAllocator(gpa.allocator());
        defer args.deinit();
        _ = args.skip();
        while (args.next()) |a| {
            var file = try cwd.openFile(a, .{});
            defer file.close();
            var reader = std.io.bufferedReader(file.reader());
            const inp = try reader.reader().readAllAlloc(gpa.allocator(), 1024);
            defer gpa.allocator().free(inp);
            //const tokens = try lang.tokenize(inp, gpa.allocator());
            //_ = tokens;
        }
    }
}
