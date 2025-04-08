const std = @import("std");

const Allocator = std.mem.Allocator;
const DynamicBitSet = std.bit_set.DynamicBitSet;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const panic = std.debug.panic;
const assert = std.debug.assert;

pub const Source = struct {
    name: []const u8,
    code: []const u8,
};

pub const Annotation = struct {
    source: Source,
    position: usize,
    line: u32,
    column: u32,
    fn annotate(source: Source, at: usize) Annotation {
        std.debug.assert(0 <= at);
        std.debug.assert(at < source.code.len);
        var j: u32 = 0;
        var lines: u32 = 1;
        var column: u32 = 0;
        while (j <= at) {
            switch (source.code[j]) {
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
            .position = at,
            .source = source,
        };
    }
    pub fn view(self: Annotation) []const u8 {
        var from = self.position;
        while (true) {
            if (from == 0) break;
            if (self.source.code[from - 1] == '\n') break;
            from -= 1;
        }
        var to = self.position;
        while (true) {
            if (to == self.source.code.len) break;
            if (self.source.code[to] == '\n') break;
            to += 1;
        }
        return self.source.code[from..to];
    }
};
pub const TokenAnnotation = struct { Token, Annotation };

pub const ErrorHandler = struct {
    ptr: *anyopaque,
    vtable: struct {
        onUnknownToken: *const fn (ptr: *anyopaque, an: Annotation) void,
        onMalformedString: *const fn (ptr: *anyopaque, an: Annotation) void,
        onExpectedTerm: *const fn (ptr: *anyopaque, an: TokenAnnotation) void,
        onExpectedPredicate: *const fn (ptr: *anyopaque, an: TokenAnnotation) void,
        onExpectedRuleClause: *const fn (ptr: *anyopaque, an: TokenAnnotation) void,
        onExpectedRuleDef: *const fn (ptr: *anyopaque, an: TokenAnnotation) void,
        onExpectedAtomTerm: *const fn (ptr: *anyopaque, an: TokenAnnotation) void,
        onIncompleteAtom: *const fn (ptr: *anyopaque, an: TokenAnnotation) void,
    },
    fn onUnknownToken(self: ErrorHandler, an: Annotation) void {
        self.vtable.onUnknownToken(self.ptr, an);
    }
    fn onMalformedString(self: ErrorHandler, an: Annotation) void {
        self.vtable.onMalformedString(self.ptr, an);
    }
    fn onExpectedTerm(self: ErrorHandler, an: TokenAnnotation) void {
        self.vtable.onExpectedTerm(self.ptr, an);
    }
    fn onExpectedPredicate(self: ErrorHandler, an: TokenAnnotation) void {
        self.vtable.onExpectedPredicate(self.ptr, an);
    }
    fn onExpectedRuleClause(self: ErrorHandler, an: TokenAnnotation) void {
        self.vtable.onExpectedRuleClause(self.ptr, an);
    }
    fn onExpectedRuleDef(self: ErrorHandler, an: TokenAnnotation) void {
        self.vtable.onExpectedRuleDef(self.ptr, an);
    }
    fn onExpectedAtomTerm(self: ErrorHandler, an: TokenAnnotation) void {
        self.vtable.onExpectedAtomTerm(self.ptr, an);
    }
    fn onIncompleteAtom(self: ErrorHandler, an: TokenAnnotation) void {
        self.vtable.onIncompleteAtom(self.ptr, an);
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
        source: Source,
        tokens: []const Token,
        statements: []const Rule,
    };

    /// Extract an abstract syntax tree from file buffer contents named `name`.
    pub fn parse(
        source: Source,
        handler: ErrorHandler,
        allocator: Allocator,
    ) (Allocator.Error || error{ParseError})!File {
        const tokens = tokens: {
            var error_state: bool = false;
            var stream = TokenStream{ .source = source };
            var ts: ArrayListUnmanaged(Token) = .empty;
            while (true) {
                const token = stream.next(handler) catch |e| switch (e) {
                    error.ParseError => {
                        error_state = true;
                        continue;
                    },
                };
                if (token) |t| try ts.append(allocator, t) else break;
            }
            if (error_state) return error.ParseError;
            break :tokens try ts.toOwnedSlice(allocator);
        };
        const statements = statements: {
            var error_state: bool = false;
            var rules: ArrayListUnmanaged(Rule) = .empty;
            var parser = Parser{ .source = source, .stream = tokens };
            while (!parser.eof()) {
                const rule, parser = parser.rule(allocator, handler) catch |e| switch (e) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.ParseError => {
                        error_state = true;
                        parser = parser.recover();
                        continue;
                    },
                };
                if (!error_state) try rules.append(allocator, rule);
            }
            if (error_state) return error.ParseError;
            break :statements try rules.toOwnedSlice(allocator);
        };
        return File{
            .source = source,
            .tokens = tokens,
            .statements = statements,
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
    source: Source,

    inline fn eof(self: Parser) bool {
        return self.stream.len <= self.cursor;
    }
    fn annotation(self: Parser) TokenAnnotation {
        assert(!self.eof());
        const token = self.stream[self.cursor];
        return token.annotate(self.source);
    }

    /// Pop token from the stream and bump cursor if stream has more tokens.
    fn advance(self: Parser) ?Result(Token) {
        if (self.eof()) return null;
        return .{
            self.stream[self.cursor],
            .{
                .source = self.source,
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

    fn recover(self: Parser) Parser {
        var parser = self;
        while (true) {
            if (parser.advance()) |next| {
                const token, parser = next;
                if (token.t == .dot) return parser;
            } else {
                return parser;
            }
        }
    }

    /// An identifier literal, a string literal, a variable or wildcard.
    fn term(self: Parser, handler: ErrorHandler) !Result(ast.Term) {
        assert(!self.eof());
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
        handler.onExpectedTerm(self.annotation());
        return error.ParseError;
    }

    /// Predicate symbol.
    fn predicate(self: Parser, handler: ErrorHandler) !Result([]const u8) {
        assert(!self.eof());
        var parser = self;
        if (parser.lookup(.identifier)) |result| {
            const token, parser = result;
            if (std.ascii.isUpper(token.slice[0])) {
                handler.onExpectedPredicate(self.annotation());
                return error.ParseError;
            }
            if (std.mem.eql(u8, token.slice, "_")) {
                handler.onExpectedPredicate(self.annotation());
                return error.ParseError;
            }
            return .{ token.slice, parser };
        }
        handler.onExpectedPredicate(self.annotation());
        return error.ParseError;
    }

    /// Atom consists of a predicate symbol and a list of terms in parenthesis.
    fn atom(
        self: Parser,
        allocator: Allocator,
        handler: ErrorHandler,
    ) !Result(ast.Atom) {
        var parser = self;
        const pred, parser = try parser.predicate(handler);
        if (parser.lookup(.parenthesis_left)) |parenthesis_left| {
            _, parser = parenthesis_left;
            if (parser.lookup(.parenthesis_right)) |parenthesis_right| {
                _, parser = parenthesis_right;
                return .{ ast.Atom{ .token = self.cursor, .predicate = pred }, parser };
            }
            var terms: ArrayListUnmanaged(ast.Term) = .empty;
            errdefer terms.deinit(allocator);
            {
                const t, parser = try parser.term(handler);
                try terms.append(allocator, t);
            }
            while (parser.advance()) |result| {
                const prior = parser;
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
                        const t, parser = try parser.term(handler);
                        try terms.append(allocator, t);
                    },
                    else => {
                        handler.onExpectedAtomTerm(prior.annotation());
                        return error.ParseError;
                    },
                }
            }
            handler.onIncompleteAtom(parser.annotation());
            return error.ParseError;
        } else {
            return .{ ast.Atom{ .token = self.cursor, .predicate = pred }, parser };
        }
    }

    /// Datalog rule.
    fn rule(
        self: Parser,
        allocator: Allocator,
        handler: ErrorHandler,
    ) !Result(ast.Rule) {
        var parser = self;
        const head, parser = try parser.atom(allocator, handler);
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
                const at, parser = try parser.atom(allocator, handler);
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
                handler.onExpectedRuleClause(parser.annotation());
                return error.ParseError;
            }
        }
        handler.onExpectedRuleDef(parser.annotation());
        return error.ParseError;
    }
};

pub const Token = struct {
    t: Type,
    slice: []const u8,
    pub const Type = enum {
        identifier, // Alphabetic identifier, a constant or a variable including wildcards.
        string, // String constant enclosed in apostophes symbol, can't span mutiple lines.
        implication, // Punctuation to separate rule's head from it's body.
        parenthesis_left,
        parenthesis_right,
        comma,
        dot,
    };
    fn annotate(self: Token, source: Source) TokenAnnotation {
        const start = @intFromPtr(source.code.ptr);
        const end = @intFromPtr(self.slice.ptr);
        return .{ self, Annotation.annotate(source, end - start) };
    }
};

const TokenStream = struct {
    source: Source,
    cursor: usize = 0,
    fn peek(self: *TokenStream) ?u8 {
        if (self.cursor >= self.source.code.len) return null;
        return self.source.code[self.cursor];
    }
    fn skip(self: *TokenStream) void {
        if (self.cursor >= self.source.code.len) return;
        self.cursor += 1;
    }
    fn annotation(self: *TokenStream, at: usize) Annotation {
        return Annotation.annotate(self.source, at);
    }
    fn next(self: *TokenStream, handler: ErrorHandler) error{ParseError}!?Token {
        while (self.peek()) |char| {
            switch (char) {
                ' ' => self.skip(),
                '%' => self.onComment(),
                '.' => return self.onDot(),
                ',' => return self.onComma(),
                '(' => return self.onParenthesisLeft(),
                ')' => return self.onParenthesisRight(),
                ':' => if (try self.onColon(handler)) |t| return t,
                '\'' => if (try self.onString(handler)) |t| return t,
                '\n' => self.onNewline(),
                else => {
                    if (std.ascii.isAlphabetic(char) or char == '_') {
                        return self.onIdentifier();
                    }
                    handler.onUnknownToken(self.annotation(self.cursor));
                    self.skip();
                    return error.ParseError;
                },
            }
        } else return null;
    }
    inline fn produce(self: *TokenStream, t: Token.Type, from: usize) Token {
        return .{ .t = t, .slice = self.source.code[from..self.cursor] };
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
    inline fn onColon(self: *TokenStream, handler: ErrorHandler) !?Token {
        const from = self.cursor;
        self.skip();
        if (self.peek()) |char| {
            if (char == '-') {
                self.skip();
                return self.produce(.implication, from);
            }
        }
        handler.onUnknownToken(self.annotation(from));
        return error.ParseError;
    }
    inline fn onString(self: *TokenStream, handler: ErrorHandler) !?Token {
        const start = self.cursor;
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
        handler.onMalformedString(self.annotation(start));
        return error.ParseError;
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
