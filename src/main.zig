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
            const tokens = try stream.tokenize(gpa.allocator());
            _ = tokens;
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

    fn tokenize(self: *TokenStream, allocator: std.mem.Allocator) ![]Token {
        var tokens = std.ArrayList(Token).init(allocator);
        errdefer tokens.deinit();
        while (self.next()) |t| {
            try tokens.append(t);
        }
        return tokens.toOwnedSlice();
    }

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

fn Consumed(comptime T: type) type {
    return struct { usize, T };
}

const Term = union(enum) {
    underscore,
    literal: []const u8,
    variable: []const u8,
};

fn parseTerm(from: []const Token, at: usize) !Consumed(Term) {
    if (from.len <= at) return error.ParseError;
    const token = from[at];
    switch (token.t) {
        .id => {
            if (std.mem.eql(u8, token.slice, "_")) return .{ 1, Term.underscore };
            if (std.ascii.isUpper(token.slice[0])) return .{ 1, Term{ .variable = token.slice } };
            return .{ 1, Term{ .literal = token.slice } };
        },
        .string => return .{ 1, Term{ .literal = token.slice } },
        else => return error.ParseError,
    }
}

fn parsePredicate(from: []const Token, at: usize) !Consumed([]const u8) {
    if (from.len <= at) return error.ParseError;
    const token = from[at];
    if (token.t != .id) return error.ParseError;
    if (std.ascii.isUpper(token.slice[0])) return error.ParseError;
    if (std.mem.eql(u8, token.slice, "_")) return error.ParseError;
    return .{ 1, token.slice };
}

const Atom = struct {
    predicate: []const u8,
    terms: []const Term = &[0]Term{},
};

fn parseAtom(from: []const Token, at: usize, allocator: std.mem.Allocator) !Consumed(Atom) {
    if (from.len <= at) return error.ParseError;
    _, const predicate = try parsePredicate(from, at);
    var tokens: usize = 1;
    if (at + tokens < from.len and from[at + tokens].t == .par_left) {
        // Skip '('
        tokens += 1;

        if (from[at + tokens].t == .par_right) {
            tokens += 1;
            return .{ tokens, .{
                .predicate = predicate,
                .terms = &[0]Term{},
            } };
        }

        var terms = std.ArrayList(Term).init(allocator);
        errdefer terms.deinit();

        const consumed0, const term0 = try parseTerm(from, at + tokens);
        try terms.append(term0);
        tokens += consumed0;

        while (at + tokens < from.len) {
            if (from[at + tokens].t == .par_right) {
                tokens += 1;
                return .{ tokens, .{
                    .predicate = predicate,
                    .terms = try terms.toOwnedSlice(),
                } };
            }
            if (from[at + tokens].t == .comma) {
                // Skip comma
                tokens += 1;
                const consumed, const term = try parseTerm(from, at + tokens);
                try terms.append(term);
                tokens += consumed;
            } else return error.ParseError;
        } else return error.ParseError;
    } else {
        return .{ 1, .{ .predicate = predicate } };
    }
}

const Rule = struct {
    head: Atom,
    body: []const Atom = &[0]Atom{},
};

fn parseRule(from: []const Token, at: usize, allocator: std.mem.Allocator) !Consumed(Rule) {
    var consumed: usize = 0;
    const consumed_head, const head = try parseAtom(from, at, allocator);
    consumed += consumed_head;
    if (at + consumed >= from.len) return error.ParseError;
    switch (from[at + consumed].t) {
        .dot => {
            consumed += 1;
            return .{ consumed, .{ .head = head } };
        },
        .when => {
            consumed += 1;
            var atoms = std.ArrayList(Atom).init(allocator);
            errdefer atoms.deinit();
            while (true) {
                const consumed_atom, const atom = try parseAtom(from, at + consumed, allocator);
                consumed += consumed_atom;
                try atoms.append(atom);
                if (at + consumed >= from.len) return error.ParseError;
                switch (from[at + consumed].t) {
                    .dot => {
                        consumed += 1;
                        return .{ consumed, .{ .head = head, .body = try atoms.toOwnedSlice() } };
                    },
                    .comma => {
                        consumed += 1;
                        continue;
                    },
                    else => return error.ParseError,
                }
            }
        },
        else => return error.ParseError,
    }
}

fn parseProgram(from: []const Token, at: usize, allocator: std.mem.Allocator) !Consumed([]const Rule) {
    var tokens: usize = 0;
    var rules = std.ArrayList(Rule).init(allocator);
    errdefer rules.deinit();
    while (at + tokens < from.len) {
        const consumed, const rule = try parseRule(from, at + tokens, allocator);
        try rules.append(rule);
        tokens += consumed;
    }
    return .{ tokens, try rules.toOwnedSlice() };
}

test "Parse a simple program" {
    var stream = TokenStream{
        .source_name = "test_buffer",
        .source =
        \\path(A, B) :- edge(A, B).
        \\path(A, C) :- path(A, B), edge(B, C).
        ,
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const tokens = try stream.tokenize(arena.allocator());
    _, const rules = try parseProgram(tokens, 0, arena.allocator());
    {
        const eql = std.mem.eql;
        const expect = std.testing.expect;
        try expect(eql(u8, rules[0].head.predicate, "path"));
        try expect(eql(u8, rules[0].head.terms[0].variable, "A"));
        try expect(eql(u8, rules[0].head.terms[1].variable, "B"));
        try expect(eql(u8, rules[0].body[0].predicate, "edge"));
        try expect(eql(u8, rules[0].body[0].terms[0].variable, "A"));
        try expect(eql(u8, rules[0].body[0].terms[1].variable, "B"));
        try expect(eql(u8, rules[1].head.predicate, "path"));
        try expect(eql(u8, rules[1].head.terms[0].variable, "A"));
        try expect(eql(u8, rules[1].head.terms[1].variable, "C"));
        try expect(eql(u8, rules[1].body[0].predicate, "path"));
        try expect(eql(u8, rules[1].body[0].terms[0].variable, "A"));
        try expect(eql(u8, rules[1].body[0].terms[1].variable, "B"));
        try expect(eql(u8, rules[1].body[1].predicate, "edge"));
        try expect(eql(u8, rules[1].body[1].terms[0].variable, "B"));
        try expect(eql(u8, rules[1].body[1].terms[1].variable, "C"));
    }
}

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
    const tokens = try stream.tokenize(std.testing.allocator);
    defer std.testing.allocator.free(tokens);
    for (expected, 0..) |expect, j| {
        const t, const s = expect;
        try std.testing.expect(tokens[j].t == t);
        try std.testing.expect(std.mem.eql(u8, tokens[j].slice, s));
    }
}
