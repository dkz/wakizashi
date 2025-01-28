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
            var stream = TokenStream{
                .source_name = a,
                .source = inp,
            };
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
    when,
};

const Token = struct { t: TokenType, slice: []const u8 };

const TokenStream = struct {
    /// Name of the source file, for error reporting:
    source_name: []const u8,

    /// Source file buffer view:
    source: []const u8,

    cursor: usize = 0,
    current_line: usize = 0,
    last_newline: usize = 0,

    const Position = struct {
        focus_line: []const u8,
        number: usize,
        column: usize,
    };

    /// Current cursor position in the buffer including line number, column,
    /// and a view of an entire string at cursor.
    fn position(self: *TokenStream) Position {
        const focus_from = self.last_newline;
        var focus_to = 1 + focus_from;
        while (focus_to < self.source.len) {
            if (self.source[focus_to] == '\n') break;
            focus_to += 1;
        }
        return .{
            .number = 1 + self.current_line,
            .column = 1 + self.cursor - self.last_newline,
            .focus_line = self.source[focus_from..focus_to],
        };
    }

    fn next(self: *TokenStream) ?Token {
        while (self.peek()) |char| {
            const start = self.cursor;
            self.skip();
            switch (char) {
                ' ' => {},
                '\n' => {
                    self.last_newline = self.cursor;
                    self.current_line += 1;
                },
                '%' => {
                    while (self.peek()) |n| {
                        if (n == '\n') break;
                        self.skip();
                    }
                },
                '.' => return Token{ .t = .dot, .slice = self.source[start..self.cursor] },
                ',' => return Token{ .t = .comma, .slice = self.source[start..self.cursor] },
                '(' => return Token{ .t = .par_left, .slice = self.source[start..self.cursor] },
                ')' => return Token{ .t = .par_right, .slice = self.source[start..self.cursor] },
                ':' => {
                    if (self.peek()) |n| {
                        if (n == '-') {
                            self.skip();
                            return Token{
                                .t = .when,
                                .slice = self.source[start..self.cursor],
                            };
                        }
                    }
                    const current = self.position();
                    std.debug.print(
                        "Unrecognized character ':' at {s}:{}:{}\n{}\t{s}\n",
                        .{
                            self.source_name,
                            current.number,
                            current.column,
                            current.number,
                            current.focus_line,
                        },
                    );
                },
                '\'' => {
                    while (self.peek()) |n| {
                        if (n == '\'' or n == '\n') break;
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
                    const current = self.position();
                    std.debug.print(
                        "Malformed string at {s}:{}:{}\n{}\t{s}\n",
                        .{
                            self.source_name,
                            current.number,
                            current.column,
                            current.number,
                            current.focus_line,
                        },
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
                        const current = self.position();
                        std.debug.print(
                            "Unrecognized character '{c}' at {s}:{}:{}\n{}\t{s}\n",
                            .{
                                char,
                                self.source_name,
                                current.number,
                                current.column,
                                current.number,
                                current.focus_line,
                            },
                        );
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

test "Tokenize a basic datalog program" {
    var stream = TokenStream{
        .source_name = "test_buffer",
        .source =
        \\% Connected edges sample program.
        \\edge(x, y).
        \\edge(y, z).
        \\path(A, B) :-
        \\  edge(A, B).
        \\path(A, C) :-
        \\  path(A, B),
        \\  edge(B, C).
        ,
    };
    const Expect = struct { TokenType, []const u8 };
    const pl: Expect = .{ .par_left, "(" };
    const pr: Expect = .{ .par_right, ")" };
    const dot: Expect = .{ .dot, "." };
    const comma: Expect = .{ .comma, "," };
    const when: Expect = .{ .when, ":-" };
    const path: Expect = .{ .id, "path" };
    const edge: Expect = .{ .id, "edge" };
    const x: Expect = .{ .id, "x" };
    const y: Expect = .{ .id, "y" };
    const z: Expect = .{ .id, "z" };
    const a: Expect = .{ .id, "A" };
    const b: Expect = .{ .id, "B" };
    const c: Expect = .{ .id, "C" };
    const expected = [_]struct { TokenType, []const u8 }{
        edge,
        pl,
        x,
        comma,
        y,
        pr,
        dot,
        edge,
        pl,
        y,
        comma,
        z,
        pr,
        dot,
        path,
        pl,
        a,
        comma,
        b,
        pr,
        when,
        edge,
        pl,
        a,
        comma,
        b,
        pr,
        dot,
        path,
        pl,
        a,
        comma,
        c,
        pr,
        when,
        path,
        pl,
        a,
        comma,
        b,
        pr,
        comma,
        edge,
        pl,
        b,
        comma,
        c,
        pr,
        dot,
    };
    var idx: usize = 0;
    while (try stream.next(struct {})) |token| {
        const t, const s = expected[idx];
        try std.testing.expect(token.t == t);
        try std.testing.expect(std.mem.eql(u8, token.slice, s));
        idx += 1;
    }
    try std.testing.expect(idx == expected.len);
}
