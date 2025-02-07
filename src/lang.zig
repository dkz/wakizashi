const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const DynamicBitSet = std.bit_set.DynamicBitSet;
const ArrayList = std.ArrayList;

const Literal = []const u8;

pub const ast = struct {
    pub const Term = union(enum) {
        wildcard,
        literal: Literal,
        variable: Literal,
    };
    pub const Atom = struct {
        predicate: Literal,
        terms: ?[]const Term = null,
        token: u32,
    };
    pub const Rule = struct {
        head: Atom,
        body: ?[]const Atom = null,
        token: u32,
    };
    pub const Program = struct {
        arena: ArenaAllocator,
        tokens: []const Token,
        rules: []const Rule,
    };

    pub fn parse(source: []const u8, allocator: Allocator) !Program {
        var arena = ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const tokens = try tokenize(source, arena.allocator());
        var parser = Parser{ .stream = tokens };
        var rules = ArrayList(ast.Rule).init(arena.allocator());
        while (!parser.eof()) {
            const rule, parser = try parser.rule(arena.allocator());
            try rules.append(rule);
        }
        return Program{
            .arena = arena,
            .tokens = tokens,
            .rules = try rules.toOwnedSlice(),
        };
    }
};

pub const data = struct {
    pub const TupleElement = union(enum) {
        variable: Literal,
        constant: Literal,
    };

    pub const Statement = union(enum) {
        fact: struct {
            predicate: Literal,
            tuple: ?[]const Literal = null,
        },
        rule: struct {
            predicate: Literal,
            tuple: ?[]const TupleElement = null,
            atoms: ?[]const ast.Atom = null,
        },
    };

    pub fn parse(rule: ast.Rule, allocator: Allocator) !Statement {
        const rule_predicate = rule.head.predicate;
        const rule_implies = if (rule.body) |body| body.len > 0 else false;
        const rule_states = if (rule.head.terms) |terms| terms.len > 0 else false;
        if (!rule_implies) {
            if (!rule_states) {
                return Statement{ .fact = .{ .predicate = rule_predicate } };
            }
            var tuple = ArrayList(Literal).init(allocator);
            errdefer tuple.deinit();
            for (rule.head.terms.?) |term| switch (term) {
                .literal => |it| try tuple.append(it),
                else => return error.ParseError,
            };
            return Statement{
                .fact = .{
                    .predicate = rule_predicate,
                    .tuple = try tuple.toOwnedSlice(),
                },
            };
        } else {
            if (!rule_states) {
                return Statement{ .rule = .{ .predicate = rule_predicate, .atoms = rule.body } };
            }
            const head = rule.head.terms.?;
            var tuple = ArrayList(TupleElement).init(allocator);
            errdefer tuple.deinit();
            var unbound = try DynamicBitSet.initEmpty(allocator, head.len);
            defer unbound.deinit();
            for (head, 0..) |term, idx| switch (term) {
                .wildcard => return error.ParseError,
                .literal => |it| {
                    try tuple.append(TupleElement{ .constant = it });
                },
                .variable => |it| {
                    try tuple.append(TupleElement{ .variable = it });
                    unbound.set(idx);
                },
            };
            atom: for (rule.body.?) |atom| {
                _ = atom.terms orelse continue :atom;
                for (atom.terms.?) |term| switch (term) {
                    .variable => |this| {
                        for (tuple.items, 0..) |t, idx| switch (t) {
                            .variable => |that| {
                                if (std.mem.eql(u8, this, that)) {
                                    unbound.unset(idx);
                                }
                            },
                            else => {},
                        };
                    },
                    else => {},
                };
            }
            if (unbound.count() > 0) return error.ParseError;
            return Statement{
                .rule = .{
                    .predicate = rule_predicate,
                    .tuple = try tuple.toOwnedSlice(),
                    .atoms = rule.body,
                },
            };
        }
    }
};

test "Data" {
    const allocator = std.testing.allocator;
    const source =
        \\path(A, B) :- edge(A, B).
        \\fact('one', 'two').
    ;
    var program = try ast.parse(source, allocator);
    defer program.arena.deinit();
    var arena = ArenaAllocator.init(allocator);
    defer arena.deinit();
    try std.json.stringify(
        try data.parse(program.rules[0], arena.allocator()),
        .{ .whitespace = .indent_2 },
        std.io.getStdErr().writer(),
    );
    try std.json.stringify(
        try data.parse(program.rules[1], arena.allocator()),
        .{ .whitespace = .indent_2 },
        std.io.getStdErr().writer(),
    );
}

/// Scans through pre-allocated token stream and allocates an AST structure using allocator provided.
/// Parsers are call-by-value, so fallback parser always stays up on stack for robust error reporting.
const Parser = struct {
    /// Combines typed parsing result with an immutable continuation of the parser.
    fn Result(comptime T: type) type {
        return struct { T, Parser };
    }
    /// Current position of the parser in token's stream slice.
    cursor: u32 = 0,
    stream: []const Token,

    inline fn eof(self: Parser) bool {
        return self.stream.len <= self.cursor;
    }

    /// Pop token from the stream and bump cursor if stream has more tokens.
    fn advance(self: Parser) ?Result(Token) {
        if (self.eof()) return null;
        return .{
            self.stream[self.cursor],
            .{
                .stream = self.stream,
                .cursor = self.cursor + 1,
            },
        };
    }

    /// Pop token from the stream but only if it matches the token type.
    fn lookup(self: Parser, t: Token.Type) ?Result(Token) {
        var parser = self;
        if (parser.advance()) |result| {
            const token, parser = result;
            return if (token.t == t) .{ token, parser } else null;
        }
        return null;
    }

    /// An identifier literal, a string literal, a variable or wildcard.
    fn term(self: Parser) !Result(ast.Term) {
        var parser = self;
        if (self.lookup(.identifier)) |result| {
            const token, parser = result;
            if (std.mem.eql(u8, token.slice, "_")) {
                return .{ ast.Term.wildcard, parser };
            }
            if (std.ascii.isUpper(token.slice[0])) {
                return .{ ast.Term{ .variable = token.slice }, parser };
            }
            return .{ ast.Term{ .literal = token.slice }, parser };
        }
        if (self.lookup(.string)) |result| {
            const token, parser = result;
            return .{
                ast.Term{ .literal = token.slice },
                parser,
            };
        }
        return error.ParserError;
    }

    /// Predicate symbol.
    fn predicate(self: Parser) !Result([]const u8) {
        var parser = self;
        if (parser.lookup(.identifier)) |result| {
            const token, parser = result;
            if (std.ascii.isUpper(token.slice[0])) return error.ParseError;
            if (std.mem.eql(u8, token.slice, "_")) return error.ParseError;
            return .{ token.slice, parser };
        }
        return error.ParseError;
    }

    /// Atom consists of a predicate symbol and a list of terms in parenthesis.
    fn atom(self: Parser, allocator: Allocator) !Result(ast.Atom) {
        var parser = self;
        const pred, parser = try parser.predicate();
        if (parser.lookup(.parenthesis_left)) |parenthesis_left| {
            _, parser = parenthesis_left;
            if (parser.lookup(.parenthesis_right)) |parenthesis_right| {
                _, parser = parenthesis_right;
                return .{ ast.Atom{ .token = self.cursor, .predicate = pred }, parser };
            }
            var terms = ArrayList(ast.Term).init(allocator);
            errdefer terms.deinit();
            {
                const t, parser = try parser.term();
                try terms.append(t);
            }
            while (parser.advance()) |result| {
                const token, parser = result;
                switch (token.t) {
                    .parenthesis_right => {
                        return .{
                            ast.Atom{
                                .token = self.cursor,
                                .predicate = pred,
                                .terms = try terms.toOwnedSlice(),
                            },
                            parser,
                        };
                    },
                    .comma => {
                        const t, parser = try parser.term();
                        try terms.append(t);
                    },
                    else => return error.ParseError,
                }
            }
            return error.ParseError;
        } else {
            return .{ ast.Atom{ .token = self.cursor, .predicate = pred }, parser };
        }
    }

    /// Datalog rule.
    fn rule(self: Parser, allocator: Allocator) !Result(ast.Rule) {
        var parser = self;
        const head, parser = try parser.atom(allocator);
        if (parser.lookup(.dot)) |result| {
            _, parser = result;
            return .{ ast.Rule{
                .token = self.cursor,
                .head = head,
            }, parser };
        }
        if (parser.lookup(.implication)) |implication| {
            _, parser = implication;
            var atoms = ArrayList(ast.Atom).init(allocator);
            errdefer atoms.deinit();
            while (true) {
                const at, parser = try parser.atom(allocator);
                try atoms.append(at);
                if (parser.lookup(.dot)) |dot| {
                    _, parser = dot;
                    return .{
                        ast.Rule{
                            .token = self.cursor,
                            .head = head,
                            .body = try atoms.toOwnedSlice(),
                        },
                        parser,
                    };
                }
                if (parser.lookup(.comma)) |comma| {
                    _, parser = comma;
                    continue;
                }
                return error.ParseError;
            }
        }
        return error.ParseError;
    }
};

test "Parsing" {
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
        const parser = Parser{ .stream = tokens, .cursor = 0 };
        const rule, _ = try parser.rule(arena.allocator());
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
