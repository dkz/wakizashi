const std = @import("std");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const ast = struct {
    const Term = union(enum) {
        wildcard,
        literal: []const u8,
        variable: []const u8,
    };
    const Atom = struct {
        predicate: []const u8,
        terms: ?[]const Term = null,
    };
    const Rule = struct {
        head: Atom,
        body: ?[]const Atom = null,
    };
};

const parsers = struct {
    fn Result(comptime T: type) type {
        return struct { usize, T };
    }

    fn term(tokens: []const Token, at: usize) !Result(ast.Term) {
        const this = tokens[at];
        const next = 1 + at;
        switch (this.t) {
            .string => return .{ next, ast.Term{ .literal = this.slice } },
            .identifier => {
                if (std.mem.eql(u8, this.slice, "_")) return .{ next, ast.Term.wildcard };
                if (std.ascii.isUpper(this.slice[0])) return .{ next, ast.Term{ .variable = this.slice } };
                return .{ next, ast.Term{ .literal = this.slice } };
            },
            else => return error.ParseError,
        }
    }

    fn predicate(tokens: []const Token, at: usize) !Result([]const u8) {
        const this = tokens[at];
        const next = 1 + at;
        if (this.t != .identifier) return error.ParseError;
        if (std.ascii.isUpper(this.slice[0])) return error.ParseError;
        if (std.mem.eql(u8, this.slice, "_")) return error.ParseError;
        return .{ next, this.slice };
    }

    fn atom(tokens: []const Token, cursor: usize, allocator: Allocator) !Result(ast.Atom) {
        var at = cursor;
        at, const pred = try predicate(tokens, at);
        if (tokens[at].t != .parenthesis_left) return .{ at, ast.Atom{ .predicate = pred } };
        at += 1;
        if (tokens[at].t == .parenthesis_right) {
            at += 1;
            return .{ at, ast.Atom{ .predicate = pred } };
        }
        var terms = ArrayList(ast.Term).init(allocator);
        errdefer terms.deinit();
        {
            at, const t = try term(tokens, at);
            try terms.append(t);
        }
        while (at < tokens.len) {
            if (tokens[at].t == .parenthesis_right) {
                at += 1;
                return .{
                    at,
                    ast.Atom{ .predicate = pred, .terms = try terms.toOwnedSlice() },
                };
            }
            if (tokens[at].t == .comma) {
                at += 1;
                at, const t = try term(tokens, at);
                try terms.append(t);
            } else return error.ParseError;
        }
        return error.ParseError;
    }

    fn rule(tokens: []const Token, cursor: usize, allocator: std.mem.Allocator) !Result(ast.Rule) {
        var at = cursor;
        at, const head = try atom(tokens, at, allocator);
        switch (tokens[at].t) {
            .dot => {
                at += 1;
                return .{ at, .{ .head = head } };
            },
            .implication => {
                at += 1;
                var atoms = ArrayList(ast.Atom).init(allocator);
                errdefer atoms.deinit();
                while (true) {
                    at, const a = try atom(tokens, at, allocator);
                    try atoms.append(a);
                    switch (tokens[at].t) {
                        .dot => {
                            at += 1;
                            return .{
                                at,
                                .{ .head = head, .body = try atoms.toOwnedSlice() },
                            };
                        },
                        .comma => {
                            at += 1;
                            continue;
                        },
                        else => return error.ParseError,
                    }
                }
            },
            else => return error.ParseError,
        }
    }
};

test "Parsers" {
    const allocator = std.testing.allocator;
    const source =
        \\% Comment
        \\path(A, B) :- edge(A, B).
    ;
    const tokens = try tokenize(source, allocator);
    defer allocator.free(tokens);
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        _, const rule = try parsers.rule(tokens, 0, arena.allocator());
        try std.testing.expect(std.mem.eql(u8, rule.head.predicate, "path"));
        try std.testing.expect(std.mem.eql(u8, rule.head.terms.?[0].variable, "A"));
        try std.testing.expect(std.mem.eql(u8, rule.head.terms.?[1].variable, "B"));
        try std.testing.expect(std.mem.eql(u8, rule.body.?[0].predicate, "edge"));
        try std.testing.expect(std.mem.eql(u8, rule.body.?[0].terms.?[0].variable, "A"));
        try std.testing.expect(std.mem.eql(u8, rule.body.?[0].terms.?[1].variable, "B"));
    }
}

const Token = struct {
    t: Type,
    slice: []const u8,
    const Type = enum {
        identifier, // Alphabetic identifier, a constant or a variable including wildcards.
        string, // String constant enclosed in apostophes symbol, can't span mutiple lines.
        implication, // Punctuation to separate rule's head from it's body.
        parenthesis_left,
        parenthesis_right,
        comma,
        dot,
    };
};

fn tokenize(source: []const u8, allocator: Allocator) ![]Token {
    var stream = Stream{ .buf = source };
    var tokens = ArrayList(Token).init(allocator);
    errdefer tokens.deinit();
    while (stream.next()) |t| {
        try tokens.append(t);
    }
    return tokens.toOwnedSlice();
}

test "Tokenization" {
    const allocator = std.testing.allocator;
    const source =
        \\% Comment
        \\predicate('string constant', Variable).
    ;
    const tokens = try tokenize(source, allocator);
    defer allocator.free(tokens);
    {
        const result = &[_]struct { Token.Type, []const u8 }{
            .{ .identifier, "predicate" },
            .{ .parenthesis_left, "(" },
            .{ .string, "string constant" },
            .{ .comma, "," },
            .{ .identifier, "Variable" },
            .{ .parenthesis_right, ")" },
            .{ .dot, "." },
        };
        for (tokens, result) |token, expected| {
            const t, const slice = expected;
            try std.testing.expect(std.mem.eql(u8, token.slice, slice));
            try std.testing.expect(token.t == t);
        }
    }
}

const Stream = struct {
    buf: []const u8,
    pos: usize = 0,
    fn peek(self: *Stream) ?u8 {
        if (self.pos >= self.buf.len) return null;
        return self.buf[self.pos];
    }
    fn skip(self: *Stream) void {
        if (self.pos >= self.buf.len) return;
        self.pos += 1;
    }
    fn next(self: *Stream) ?Token {
        while (self.peek()) |char| {
            switch (char) {
                ' ' => self.skip(),
                '%' => self.onComment(),
                '.' => return self.onDot(),
                ',' => return self.onComma(),
                '(' => return self.onParenthesisLeft(),
                ')' => return self.onParenthesisRight(),
                ':' => if (self.onColon()) |t| return t,
                '\'' => if (self.onString()) |t| return t,
                '\n' => self.onNewline(),
                else => {
                    if (std.ascii.isAlphabetic(char) or char == '_') {
                        return self.onIdentifier();
                    }
                    return null; // TODO should be an error;
                },
            }
        } else return null;
    }
    inline fn produce(self: *Stream, t: Token.Type, from: usize) Token {
        return .{ .t = t, .slice = self.buf[from..self.pos] };
    }
    inline fn onNewline(self: *Stream) void {
        self.skip();
    }
    inline fn onDot(self: *Stream) Token {
        const from = self.pos;
        self.skip();
        return self.produce(.dot, from);
    }
    inline fn onComma(self: *Stream) Token {
        const from = self.pos;
        self.skip();
        return self.produce(.comma, from);
    }
    inline fn onParenthesisLeft(self: *Stream) Token {
        const from = self.pos;
        self.skip();
        return self.produce(.parenthesis_left, from);
    }
    inline fn onParenthesisRight(self: *Stream) Token {
        const from = self.pos;
        self.skip();
        return self.produce(.parenthesis_right, from);
    }
    inline fn onComment(self: *Stream) void {
        while (self.peek()) |char| {
            if (char == '\n') break;
            self.skip();
        }
    }
    inline fn onColon(self: *Stream) ?Token {
        const from = self.pos;
        self.skip();
        if (self.peek()) |char| {
            if (char == '-') {
                self.skip();
                return self.produce(.implication, from);
            }
        }
        return null; // TODO should be an error;
    }
    inline fn onString(self: *Stream) ?Token {
        self.skip();
        const from = self.pos;
        while (self.peek()) |char| {
            if (char == '\'' or char == '\n') break;
            self.skip();
        }
        if (self.peek()) |char| {
            if (char == '\'') {
                const token = self.produce(.string, from);
                self.skip();
                return token;
            }
        }
        return null; // TODO should be an error;
    }
    inline fn onIdentifier(self: *Stream) Token {
        const from = self.pos;
        self.skip();
        while (self.peek()) |char| {
            if (!std.ascii.isAlphabetic(char) and char != '_') break;
            self.skip();
        }
        return self.produce(.identifier, from);
    }
};
