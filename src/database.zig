//! Everything related to encoding, decoding, storing, and querying tuples.
//! Database tuples are encoded as []u64 slice, with Domain struct providing
//! encoding and decoding facilities.
//!
//! Database functions allowed to return OutOfMemory error.
//! Catch it on top and report a user-friendly message of what program was doing
//! when it encountered an OutOfMemory, then panic and crash.

const std = @import("std");
const mem = std.mem;
const builtin = @import("builtin");
const endian = builtin.cpu.arch.endian();
const assert = std.debug.assert;

const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayList = std.ArrayList;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const HashMapUnmanaged = std.HashMapUnmanaged;
const Wyhash = std.hash.Wyhash;

const AnyWriter = std.io.AnyWriter;
const AnyReader = std.io.AnyReader;

/// Supported datatypes in database tuples.
/// For simplicity, every value corresponds only to a binary blob.
/// TODO This makes range queries impossible.
pub const DomainValue = union(enum) {
    binary: []const u8,
    pub fn clone(self: DomainValue, allocator: Allocator) Allocator.Error!DomainValue {
        switch (self) {
            .binary => |binary| {
                const target = try allocator.alloc(u8, binary.len);
                @memcpy(target, binary);
                return DomainValue{ .binary = target };
            },
        }
    }
    const binary_tag: u8 = 0x01;
    pub fn backup(self: DomainValue, into: AnyWriter) !void {
        switch (self) {
            .binary => |binary| {
                try into.writeInt(u8, binary_tag, endian);
                try into.writeInt(usize, binary.len, endian);
                try into.writeAll(binary);
            },
        }
    }
    pub fn restore(from: AnyReader, allocator: Allocator) !DomainValue {
        const tag = try from.readInt(u8, endian);
        switch (tag) {
            binary_tag => {
                const size = try from.readInt(usize, endian);
                const target = try allocator.alloc(u8, size);
                errdefer allocator.free(target);
                const read = try from.readAll(target);
                if (read < size) return error.CorruptInput;
                return DomainValue{ .binary = target };
            },
            else => return error.CorruptInput,
        }
    }
    pub const Equality = struct {
        pub fn eql(_: Equality, this: DomainValue, that: DomainValue) bool {
            return mem.eql(u8, this.binary, that.binary);
        }
        pub fn hash(_: Equality, this: DomainValue) u64 {
            switch (this) {
                .binary => |binary| return Wyhash.hash(0, binary),
            }
        }
    };
};

test "DomainValue: backup and restore" {
    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var output = ArrayList(u8).init(allocator);
    {
        const value = DomainValue{ .binary = "test" };
        const writer = output.writer();
        try value.backup(writer.any());
    }
    {
        var source = std.io.fixedBufferStream(output.items);
        const reader = source.reader();
        const value = try DomainValue.restore(reader.any(), allocator);
        try std.testing.expect(mem.eql(u8, value.binary, "test"));
    }
}

/// Responsible for decoding an encoding collections of Domain Values to tuples.
/// Instead of using multiple copies of the data and wasting CPU cycles on equality checks
/// Domain stores every encountered value in a domain index and assigns a u64 id to it.
pub const Domain = struct {
    const load_factor = std.hash_map.default_max_load_percentage;
    const Index = HashMapUnmanaged(DomainValue, u64, DomainValue.Equality, load_factor);
    array: ArrayListUnmanaged(DomainValue) = .{},
    index: Index = .{},
    /// Returns an unique id assigned to this domain value.
    pub fn register(self: *Domain, allocator: Allocator, value: DomainValue) Allocator.Error!u64 {
        if (self.index.get(value)) |id| {
            return id;
        } else {
            const id = self.array.items.len;
            const copy = try value.clone(allocator);
            try self.array.append(allocator, copy);
            try self.index.put(allocator, copy, id);
            return id;
        }
    }
    /// Transform a DomainValue slice into a `tuple`, a []u64 slice of value identifiers.
    pub fn encodeTuple(
        self: *Domain,
        allocator: Allocator,
        from: []const DomainValue,
        into: []u64,
    ) Allocator.Error!void {
        std.debug.assert(from.len == into.len);
        for (0..from.len) |j| {
            into[j] = try self.register(allocator, from[j]);
        }
    }
    pub fn encodeQuery(
        self: *Domain,
        allocator: Allocator,
        from: []const ?DomainValue,
        into: []?u64,
    ) Allocator.Error!void {
        std.debug.assert(from.len == into.len);
        for (0..from.len) |j| {
            into[j] = if (from[j]) |value| try self.register(allocator, value) else null;
        }
    }
    /// Returns a view into this Domain, caller does not own the data.
    pub fn decodeTuple(
        self: *Domain,
        from: []const u64,
        into: []DomainValue,
    ) void {
        std.debug.assert(from.len == into.len);
        for (0..from.len) |j| {
            into[j] = self.array.items[from[j]];
        }
    }
    pub fn backup(self: *Domain, into: AnyWriter) !void {
        try into.writeInt(usize, self.array.items.len, endian);
        for (self.array.items) |value| {
            try value.backup(into);
        }
    }
    pub fn restore(from: AnyReader, allocator: Allocator) !Domain {
        const size = try from.readInt(usize, endian);
        var vals = ArrayListUnmanaged(DomainValue){};
        var idxs = Index{};
        for (0..size) |_| try vals.append(allocator, try DomainValue.restore(from, allocator));
        for (0.., vals.items) |j, v| try idxs.put(allocator, v, j);
        return Domain{ .array = vals, .index = idxs };
    }
};

test "Domain: backup and restore" {
    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var output = ArrayList(u8).init(allocator);

    const values = &[_]DomainValue{
        .{ .binary = "901" },
        .{ .binary = "text" },
        .{ .binary = "977" },
    };
    var expected: [3]u64 = undefined;
    var received: [3]u64 = undefined;
    {
        var domain = Domain{};
        for (values) |v| _ = try domain.register(allocator, v);
        try domain.encodeTuple(allocator, values, &expected);
        const writer = output.writer();
        try domain.backup(writer.any());
    }
    {
        var source = std.io.fixedBufferStream(output.items);
        const reader = source.reader();
        var domain = try Domain.restore(reader.any(), allocator);
        try domain.encodeTuple(allocator, values, &received);
    }
    try std.testing.expect(mem.eql(u64, &expected, &received));
}

/// Abstract tuple iterator.
/// Callers must call destroy() after receiving an instance.
pub const TupleIterator = struct {
    ptr: *anyopaque,
    nextFn: *const fn (ctx: *anyopaque, into: []u64) bool,
    destroyFn: *const fn (ctx: *anyopaque) void,
    /// Returns true if search produces a new tuple,
    /// which in turn gets written to an `into` slice.
    /// If method returns false, buffer might contain garbage.
    pub fn next(self: *const TupleIterator, into: []u64) bool {
        return self.nextFn(self.ptr, into);
    }
    pub fn destroy(self: *const TupleIterator) void {
        self.destroyFn(self.ptr);
    }
};

/// Namespace for common tuple iterators.
pub const iterators = struct {
    pub const SliceIterator = struct {
        /// Null for on-stack iterators.
        allocator: ?Allocator = null,
        slice: []const u64,
        index: usize = 0,
        arity: usize,
        pub fn iterator(self: *SliceIterator) TupleIterator {
            return .{ .ptr = self, .nextFn = next, .destroyFn = destroy };
        }
        fn destroy(ptr: *anyopaque) void {
            const self: *SliceIterator = @ptrCast(@alignCast(ptr));
            if (self.allocator) |allocator| allocator.destroy(self);
        }
        fn next(ptr: *anyopaque, into: []u64) bool {
            const self: *SliceIterator = @ptrCast(@alignCast(ptr));
            assert(self.arity == into.len);
            const a = self.arity;
            const i = self.index;
            if (i * a < self.slice.len) {
                self.index += 1;
                @memcpy(into, self.slice[i * a .. a + i * a]);
                return true;
            } else {
                return false;
            }
        }
    };
    /// Yields tuples from parent iterator,
    /// but only those that match a specified pattern.
    /// Make sure it has an unique access to the parent iterator.
    pub const PatternIterator = struct {
        /// Null for on-stack iterators.
        allocator: ?Allocator = null,
        parent: TupleIterator,
        pattern: []const ?u64,
        pub fn iterator(self: *PatternIterator) TupleIterator {
            return .{ .ptr = self, .nextFn = next, .destroyFn = destroy };
        }
        fn destroy(ptr: *anyopaque) void {
            const self: *PatternIterator = @ptrCast(@alignCast(ptr));
            self.parent.destroy();
            if (self.allocator) |allocator| allocator.destroy(self);
        }
        fn next(ptr: *anyopaque, into: []u64) bool {
            const self: *PatternIterator = @ptrCast(@alignCast(ptr));
            assert(self.pattern.len == into.len);
            while (self.parent.next(into)) {
                if (tuples.matches(self.pattern, into)) return true;
            }
            return false;
        }
    };
};

/// Common tuple utilities;
const tuples = struct {
    /// Test whether tuple matches the specified pattern.
    fn matches(pattern: []const ?u64, tuple: []const u64) bool {
        for (pattern, 0..) |pat, j| {
            if (pat) |q| if (q != tuple[j]) return false;
        }
        return true;
    }

    const Ordering = struct {
        dimension: usize,
        fn compare(self: Ordering, this: []const u64, that: []const u64) std.math.Order {
            return std.math.order(this[self.dimension], that[self.dimension]);
        }
    };

    const Collection = struct {
        arity: usize,
        array: ArrayListUnmanaged(u64) = .{},
        fn deinit(self: *Collection, allocator: Allocator) void {
            self.array.deinit(allocator);
        }
        inline fn elementAt(self: *Collection, index: usize, elem: usize) u64 {
            return self.array.items[index * self.arity + elem];
        }
        inline fn tupleAt(self: *Collection, index: usize) []u64 {
            return self.array.items[index * self.arity .. self.arity + index * self.arity];
        }
        fn each(self: *Collection, allocator: Allocator) Allocator.Error!TupleIterator {
            const it = try allocator.create(iterators.SliceIterator);
            it.* = .{ .allocator = allocator, .arity = self.arity, .slice = self.array.items };
            return it.iterator();
        }
    };

    /// Perform operations on a slice that contains tuples of specified arity.
    const MutableSlice = struct {
        arity: usize,
        slice: []u64,
        inline fn tupleAt(self: *MutableSlice, index: usize) []u64 {
            return self.slice[index * self.arity .. self.arity + index * self.arity];
        }
        /// Swaps two tuples in a slice using a temporary buffer.
        fn swap(
            self: *MutableSlice,
            this: usize,
            that: usize,
            temp: []u64,
        ) void {
            assert(this != that);
            assert(temp.len == self.arity);
            @memcpy(temp, self.tupleAt(this));
            @memcpy(self.tupleAt(this), self.tupleAt(that));
            @memcpy(self.tupleAt(that), temp);
        }
    };
};

/// Abstract writer for a tuple storage.
pub const TupleStorage = struct {
    ptr: *anyopaque,
    insertFn: *const fn (
        ctx: *anyopaque,
        source: TupleIterator,
        allocator: Allocator,
    ) Allocator.Error!usize,
    pub fn insert(
        self: *const TupleStorage,
        source: TupleIterator,
        allocator: Allocator,
    ) Allocator.Error!usize {
        return self.insertFn(self.ptr, source, allocator);
    }
};

/// Interface for querying and reading a tuple storage.
pub const QueryBackend = struct {
    ptr: *anyopaque,
    queryFn: *const fn (
        ctx: *anyopaque,
        pattern: []const ?u64,
        allocator: Allocator,
    ) Allocator.Error!TupleIterator,
    /// Returns an iterator over an entire storage.
    eachFn: *const fn (
        ctx: *anyopaque,
        allocator: Allocator,
    ) Allocator.Error!TupleIterator,
    pub fn query(
        self: *const QueryBackend,
        pattern: []const ?u64,
        allocator: Allocator,
    ) Allocator.Error!TupleIterator {
        return self.queryFn(self.ptr, pattern, allocator);
    }
    pub fn each(
        self: *const QueryBackend,
        allocator: Allocator,
    ) Allocator.Error!TupleIterator {
        self.eachFn(self.ptr, allocator);
    }
};

/// K-dimensional tree as in-memory tuple storage and query backend.
/// Unlike convensional k-d trees, this one does not allow duplicates.
/// Optimized for fast insertion, not fast querying, may produce unbalanced trees,
/// but it exist primarity for a small performance gain for IDB.
pub const DynamicKDTree = struct {
    allocator: Allocator,
    /// Tuples are copied into a single continuous array of element values.
    /// Since datalog database never shrinks and deletion is not required,
    /// every insert populated the end of the list.
    /// Because of this, tuples and nodes have identical indexes.
    nodes: ArrayListUnmanaged(Node) = .{},
    tuples: tuples.Collection,

    /// Each node saves only the hash of the tuple to detect duplicates,
    /// and index pointers to the left and right nodes.
    /// Extern and packed structs do not allow optional unions,
    /// hence value 0 acts as an indicator of abcense (tree nodes can't point to root).
    const Node = extern struct {
        hash: u64,
        left: usize = 0,
        right: usize = 0,
    };

    const LookupSlotOutcome = union(enum) {
        duplicate,
        vacant: *usize,
    };

    /// Try to find a vacant leaf pointer in the tree.
    /// Returns a pointer to `left` or `right` fields of a target Node struct.
    fn lookupSlot(self: *DynamicKDTree, tuple: []const u64, hash: u64) LookupSlotOutcome {
        // Insert root node as an edge-case without calling this function.
        // If node list is empty, tree has nowhere to write new node index.
        assert(0 < self.nodes.items.len);
        var depth: usize = 0;
        var index: usize = 0;
        // Either a discovered slot (only if dereferences to 0),
        // or the next node to check.
        while (true) {
            const node = &self.nodes.items[index];
            const comp = self.tuples.elementAt(index, depth % self.tuples.arity);
            const proj = tuple[depth % self.tuples.arity];
            const next: *usize = next: {
                if (proj == comp) {
                    // Do a fast hash-collision check first:
                    if (hash == node.hash) {
                        if (mem.eql(u64, tuple, self.tuples.tupleAt(index))) {
                            return .duplicate;
                        }
                    }
                }
                if (proj < comp) {
                    break :next &node.left;
                } else {
                    break :next &node.right;
                }
            };
            if (next.* == 0) return LookupSlotOutcome{ .vacant = next };
            index = next.*;
            depth += 1;
        }
    }

    fn insert(self: *DynamicKDTree, tuple: []const u64) Allocator.Error!bool {
        const hash = Wyhash.hash(0, mem.sliceAsBytes(tuple));
        const next = self.nodes.items.len;
        if (0 < next) {
            switch (self.lookupSlot(tuple, hash)) {
                .duplicate => return false,
                .vacant => |ptr| ptr.* = next,
            }
        }
        try self.tuples.array.appendSlice(self.allocator, tuple);
        const node = try self.nodes.addOne(self.allocator);
        node.* = .{ .hash = hash };
        return true;
    }

    fn insertFromIterator(
        self: *DynamicKDTree,
        source: TupleIterator,
        allocator: Allocator,
    ) Allocator.Error!usize {
        var count: usize = 0;
        const buffer = try allocator.alloc(u64, self.tuples.arity);
        while (source.next(buffer)) {
            if (try self.insert(buffer)) count += 1;
        }
        return count;
    }

    fn query(
        self: *DynamicKDTree,
        pattern: []const ?u64,
        allocator: Allocator,
    ) Allocator.Error!TupleIterator {
        const it = try allocator.create(QueryIterator);
        it.* = .{
            .allocator = allocator,
            .pattern = pattern,
            .tree = self,
            .queue = QueryIterator.Queue.init(allocator),
        };
        if (0 < self.nodes.items.len) {
            try it.queue.writeItem(.{});
        }
        return it.iterator();
    }

    pub fn deinit(self: *DynamicKDTree) void {
        self.tuples.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
    }

    pub fn storage(self: *DynamicKDTree) TupleStorage {
        const interface = struct {
            fn insertFn(
                ptr: *anyopaque,
                source: TupleIterator,
                allocator: Allocator,
            ) Allocator.Error!usize {
                const target: *DynamicKDTree = @ptrCast(@alignCast(ptr));
                return target.insertFromIterator(source, allocator);
            }
        };
        return .{
            .ptr = self,
            .insertFn = interface.insertFn,
        };
    }

    pub fn queries(self: *DynamicKDTree) QueryBackend {
        const interface = struct {
            fn queryFn(
                ptr: *anyopaque,
                pattern: []const ?u64,
                allocator: Allocator,
            ) Allocator.Error!TupleIterator {
                const target: *DynamicKDTree = @ptrCast(@alignCast(ptr));
                return target.query(pattern, allocator);
            }
            fn eachFn(
                ptr: *anyopaque,
                allocator: Allocator,
            ) Allocator.Error!TupleIterator {
                const target: *DynamicKDTree = @ptrCast(@alignCast(ptr));
                return target.tuples.each(allocator);
            }
        };
        return .{
            .ptr = self,
            .queryFn = interface.queryFn,
            .eachFn = interface.eachFn,
        };
    }

    /// BFS in the tree cutting subtrees according to pattern.
    const QueryIterator = struct {
        /// Tree doesn't store depth, so BFS forced to carry it along with node index.
        const State = struct {
            depth: usize = 0,
            index: usize = 0,
            fn descend(self: *const State, to: usize) State {
                return .{ .depth = self.depth + 1, .index = to };
            }
        };
        const Queue = std.fifo.LinearFifo(State, .Dynamic);

        allocator: Allocator,
        pattern: []const ?u64,
        tree: *DynamicKDTree,
        queue: Queue,

        pub fn iterator(self: *QueryIterator) TupleIterator {
            return .{ .ptr = self, .destroyFn = destroy, .nextFn = next };
        }
        fn destroy(ptr: *anyopaque) void {
            const self: *QueryIterator = @ptrCast(@alignCast(ptr));
            self.queue.deinit();
            self.allocator.destroy(self);
        }
        fn next(ptr: *anyopaque, into: []u64) bool {
            errdefer unreachable; // TODO make .next() also throw an error I guess.
            const self: *QueryIterator = @ptrCast(@alignCast(ptr));
            assert(self.pattern.len == into.len);
            while (self.queue.readItem()) |state| {
                const elem = state.depth % self.tree.tuples.arity;
                const node = &self.tree.nodes.items[state.index];
                if (self.pattern[elem]) |pat| {
                    // If current depth's element is defined in the pattern,
                    // pick the corrent subtree for traversal.
                    const comp = self.tree.tuples.elementAt(state.index, elem);
                    if (pat < comp) {
                        if (0 < node.left) try self.queue.writeItem(state.descend(node.left));
                    } else {
                        if (0 < node.right) try self.queue.writeItem(state.descend(node.right));
                    }
                } else {
                    // If current depth's element is a wildcard, traverse both subtrees.
                    if (0 < node.left) try self.queue.writeItem(state.descend(node.left));
                    if (0 < node.right) try self.queue.writeItem(state.descend(node.right));
                }
                const tuple = self.tree.tuples.tupleAt(state.index);
                if (tuples.matches(self.pattern, tuple)) {
                    @memcpy(into, tuple);
                    return true;
                }
            }
            return false;
        }
    };
};

/// Identifies relation by name and relation arity.
/// Each relation in database corresponds to a specific relation backend.
pub const Relation = struct {
    arity: usize,
    name: []const u8,
    pub fn clone(self: Relation, allocator: Allocator) Allocator.Error!Relation {
        const name_copy = try allocator.alloc(u8, self.name.len);
        @memcpy(name_copy, self.name);
        return .{ .arity = self.arity, .name = name_copy };
    }
    pub const Equality = struct {
        pub fn eql(_: @This(), this: Relation, that: Relation) bool {
            return this.arity == that.arity and std.mem.eql(u8, this.name, that.name);
        }
        pub fn hash(_: @This(), this: Relation) u64 {
            return std.hash.Wyhash.hash(this.arity, this.name);
        }
    };
};

pub const Db = struct {
    const load_factor = std.hash_map.default_max_load_percentage;
    allocator: Allocator,
    relations: std.HashMapUnmanaged(Relation, *DynamicKDTree, Relation.Equality, load_factor) = .{},
    pub fn create(allocator: Allocator) Db {
        return .{ .allocator = allocator };
    }
    pub fn setListRelationBackend(self: *Db, relation: Relation) Allocator.Error!void {
        if (!self.relations.contains(relation)) {
            const backend = try self.allocator.create(DynamicKDTree);
            backend.* = .{
                .allocator = self.allocator,
                .tuples = tuples.Collection{ .arity = relation.arity },
            };
            try self.relations.put(self.allocator, relation, backend);
        }
    }
    pub fn query(
        self: *Db,
        relation: Relation,
        pattern: []const ?u64,
        allocator: Allocator,
    ) !TupleIterator {
        const rel = self.relations.get(relation).?;
        const backend = rel.queries();
        return backend.query(pattern, allocator);
    }
    pub fn insert(
        self: *Db,
        relation: Relation,
        iterator: TupleIterator,
        allocator: Allocator,
    ) !usize {
        return self.relations.get(relation).?.storage().insert(iterator, allocator);
    }
};
