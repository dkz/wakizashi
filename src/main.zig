const std = @import("std");
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
            var stream = TokenStream{ .source = inp };
            while (stream.next()) |token| {
                std.debug.print("{}: {s}\n", .{ token.t, token.slice });
            }
        }
    }
}

const TokenType = enum {
    id,
    comma,
    dot,
    par_left,
    par_right,
    string,
};

const Token = struct { t: TokenType, slice: []const u8 };
const TokenStream = struct {
    source: []const u8,
    cursor: usize = 0,
    fn next(self: *TokenStream) ?Token {
        while (self.peek()) |char| {
            const start = self.cursor;
            self.skip();
            switch (char) {
                ' ', '\n' => {},
                '.' => return Token{ .t = .dot, .slice = self.source[start..self.cursor] },
                ',' => return Token{ .t = .comma, .slice = self.source[start..self.cursor] },
                '(' => return Token{ .t = .par_left, .slice = self.source[start..self.cursor] },
                ')' => return Token{ .t = .par_right, .slice = self.source[start..self.cursor] },
                '\'' => {
                    while (self.peek()) |n| {
                        if (n == '\'') break;
                        self.skip();
                    }
                    if (self.peek()) |n| {
                        if (n == '\'') {
                            self.skip();
                            return Token{
                                .t = .string,
                                .slice = self.source[start..self.cursor],
                            };
                        }
                    }
                    std.debug.print(
                        "Malformed string at {}: {s}.\n",
                        .{ start, self.source[start..self.cursor] },
                    );
                },
                else => {
                    if (std.ascii.isAlphabetic(char) or char == '_') {
                        while (self.peek()) |n| {
                            if (!std.ascii.isAlphabetic(n) and n != '_') break;
                            self.skip();
                        }
                        return Token{
                            .t = .id,
                            .slice = self.source[start..self.cursor],
                        };
                    } else {
                        std.debug.print("Unrecognized character at {}: '{c}'.\n", .{ start, char });
                    }
                },
            }
        } else return null;
    }
    fn peek(self: *TokenStream) ?u8 {
        if (self.cursor >= self.source.len) return null;
        return self.source[self.cursor];
    }
    fn skip(self: *TokenStream) void {
        if (self.cursor < self.source.len) {
            self.cursor += 1;
        }
    }
};

const expect = std.testing.expect;
fn expectToken(from: *TokenStream, t: TokenType, contents: []const u8) !void {
    const token = from.next().?;
    try expect(t == token.t);
    try expect(std.mem.eql(u8, token.slice, contents));
}
test "Basic token stream" {
    const source =
        \\first ('_second ',
        \\third_
        \\).
    ;
    var stream = TokenStream{ .source = source };
    try expectToken(&stream, .id, "first");
    try expectToken(&stream, .par_left, "(");
    try expectToken(&stream, .string, "'_second '");
    try expectToken(&stream, .comma, ",");
    try expectToken(&stream, .id, "third_");
    try expectToken(&stream, .par_right, ")");
    try expectToken(&stream, .dot, ".");
    try expect(stream.next() == null);
}
