const std = @import("std");

const Allocator = std.mem.Allocator;
const DynamicBitSet = std.bit_set.DynamicBitSet;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const panic = std.debug.panic;
const assert = std.debug.assert;

pub const Annotation = struct {
    line: u32,
    column: u32,
    fn annotate(source: []const u8, at: usize) Annotation {
        std.debug.assert(0 <= at);
        std.debug.assert(at < source.len);
        var j: u32 = 0;
        var lines: u32 = 1;
        var column: u32 = 0;
        while (j <= at) {
            switch (source[j]) {
                '\n' => {
                    lines += 1;
                    column = 0;
                },
                else => {
                    column += 1;
                },
            }
            j += 1;
        }
        return .{
            .line = lines,
            .column = column,
        };
    }
};

pub const ast = struct {
    pub const Literal = []const u8;
    pub const Term = union(enum) {
        wildcard,
        literal: Literal,
        variable: Literal,
    };
    pub const Atom = struct {
        const nullary: []const Term = &[0]Term{};
        predicate: Literal,
        terms: []const Term = nullary,
        /// Location of this atom in the file's token stream.
        token: u32,
    };
    pub const Rule = struct {
        const empty: []const Atom = &[0]Atom{};
        head: Atom,
        body: []const Atom = empty,
        /// Location of this rule in the file's token stream.
        token: u32,
    };
    pub const File = struct {
        name: []const u8,
        source: []const u8,
        tokens: []const Token,
        statements: []const Rule,
        pub fn annotateRule(self: *const File, rule: *const Rule) Annotation {
            const token = &self.tokens[rule.token];
            const start = @intFromPtr(self.source.ptr);
            const end = @intFromPtr(token.slice.ptr);
            return Annotation.annotate(self.source, end - start);
        }
        pub fn annotateAtom(self: *const File, atom: *const Atom) Annotation {
            const token = &self.tokens[atom.token];
            const start = @intFromPtr(self.source.ptr);
            const end = @intFromPtr(token.slice.ptr);
            return Annotation.annotate(self.source, end - start);
        }
    };

    /// Extract an abstract syntax tree from file buffer contents named `name`.
    pub fn parse(name: []const u8, source: []const u8, allocator: Allocator) !File {
        const tokens = tokens: {
            var stream = TokenStream{ .source = source };
            var ts: ArrayListUnmanaged(Token) = .empty;
            while (stream.next() catch |e| {
                const ann = stream.annotation();
                switch (e) {
                    error.UnknownToken => {
                        panic("{s}:{} Unknown token on position {}", .{
                            name,
                            ann.line,
                            ann.column,
                        });
                    },
                }
            }) |t| try ts.append(allocator, t);
            break :tokens try ts.toOwnedSlice(allocator);
        };
        var statements: ArrayListUnmanaged(Rule) = .empty;
        var parser = Parser{ .stream = tokens };
        while (!parser.eof()) {
            const rule, parser = try parser.rule(allocator);
            try statements.append(allocator, rule);
        }
        return File{
            .name = name,
            .source = source,
            .tokens = tokens,
            .statements = try statements.toOwnedSlice(allocator),
        };
    }
};

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
            var terms: ArrayListUnmanaged(ast.Term) = .empty;
            errdefer terms.deinit(allocator);
            {
                const t, parser = try parser.term();
                try terms.append(allocator, t);
            }
            while (parser.advance()) |result| {
                const token, parser = result;
                switch (token.t) {
                    .parenthesis_right => {
                        return .{
                            ast.Atom{
                                .token = self.cursor,
                                .predicate = pred,
                                .terms = try terms.toOwnedSlice(allocator),
                            },
                            parser,
                        };
                    },
                    .comma => {
                        const t, parser = try parser.term();
                        try terms.append(allocator, t);
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
            var atoms: ArrayListUnmanaged(ast.Atom) = .empty;
            errdefer atoms.deinit(allocator);
            while (true) {
                const at, parser = try parser.atom(allocator);
                try atoms.append(allocator, at);
                if (parser.lookup(.dot)) |dot| {
                    _, parser = dot;
                    return .{
                        ast.Rule{
                            .token = self.cursor,
                            .head = head,
                            .body = try atoms.toOwnedSlice(allocator),
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

const TokenStream = struct {
    const Error = error{
        UnknownToken,
    };
    source: []const u8,
    cursor: usize = 0,
    fn peek(self: *TokenStream) ?u8 {
        if (self.cursor >= self.source.len) return null;
        return self.source[self.cursor];
    }
    fn skip(self: *TokenStream) void {
        if (self.cursor >= self.source.len) return;
        self.cursor += 1;
    }
    fn annotation(self: *TokenStream) Annotation {
        return Annotation.annotate(self.source, self.cursor);
    }
    fn next(self: *TokenStream) Error!?Token {
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
                    return error.UnknownToken;
                },
            }
        } else return null;
    }
    inline fn produce(self: *TokenStream, t: Token.Type, from: usize) Token {
        return .{ .t = t, .slice = self.source[from..self.cursor] };
    }
    inline fn onNewline(self: *TokenStream) void {
        self.skip();
    }
    inline fn onDot(self: *TokenStream) Token {
        const from = self.cursor;
        self.skip();
        return self.produce(.dot, from);
    }
    inline fn onComma(self: *TokenStream) Token {
        const from = self.cursor;
        self.skip();
        return self.produce(.comma, from);
    }
    inline fn onParenthesisLeft(self: *TokenStream) Token {
        const from = self.cursor;
        self.skip();
        return self.produce(.parenthesis_left, from);
    }
    inline fn onParenthesisRight(self: *TokenStream) Token {
        const from = self.cursor;
        self.skip();
        return self.produce(.parenthesis_right, from);
    }
    inline fn onComment(self: *TokenStream) void {
        while (self.peek()) |char| {
            if (char == '\n') break;
            self.skip();
        }
    }
    inline fn onColon(self: *TokenStream) ?Token {
        const from = self.cursor;
        self.skip();
        if (self.peek()) |char| {
            if (char == '-') {
                self.skip();
                return self.produce(.implication, from);
            }
        }
        return error.UnknownToken;
    }
    inline fn onString(self: *TokenStream) ?Token {
        self.skip();
        const from = self.cursor;
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
    inline fn onIdentifier(self: *TokenStream) Token {
        const from = self.cursor;
        self.skip();
        while (self.peek()) |char| {
            if (!std.ascii.isAlphabetic(char) and char != '_') break;
            self.skip();
        }
        return self.produce(.identifier, from);
    }
};
