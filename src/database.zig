const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const DomainValue = union(enum) {
    seq: []const u8,
    pub fn clone(self: DomainValue, allocator: Allocator) Allocator.Error!DomainValue {
        switch (self) {
            .seq => |seq| {
                const target = try allocator.alloc(u8, seq.len);
                @memcpy(target, seq);
                return DomainValue{ .seq = target };
            },
        }
    }
    pub const Equality = struct {
        pub fn eql(_: @This(), this: DomainValue, that: DomainValue) bool {
            return std.mem.eql(u8, this.seq, that.seq);
        }
        pub fn hash(_: @This(), this: DomainValue) u64 {
            switch (this) {
                .seq => |seq| return std.hash.Wyhash.hash(0, seq),
            }
        }
    };
};

/// Stealing this cool technique from bdddb:
/// instead of using multiple copies of the data and wasting CPU cycles on equality checks
/// store every encountered value in a unified domain index and assign a word id to it.
/// Highly unlikely that a dataset for any program exceeds word size (?).
pub const Domain = struct {
    const load_factor = std.hash_map.default_max_load_percentage;
    array: std.ArrayListUnmanaged(DomainValue) = .{},
    index: std.HashMapUnmanaged(DomainValue, u64, DomainValue.Equality, load_factor) = .{},
    /// Own, hash, and assign an unique id to this domain value.
    pub fn register(self: *Domain, allocator: Allocator, value: DomainValue) Allocator.Error!u64 {
        if (self.index.get(value)) |id| {
            return id;
        } else {
            const id = self.array.items.len;
            try self.index.ensureUnusedCapacity(allocator, 1);
            try self.array.ensureUnusedCapacity(allocator, 1);
            const copy = try value.clone(allocator);
            self.array.appendAssumeCapacity(copy);
            self.index.putAssumeCapacity(copy, id);
            return id;
        }
    }
    /// Relational tuples in this encoding become a simple []u64 slice.
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

/// Abstract database tuple iterator.
/// Callers own an instance of iterator and must call destroy().
/// An opaque pointer with vtable actually takes less boilerplate in Zig
/// in comparison to enum dispatch.
pub const TupleIterator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        next: *const fn (ctx: *anyopaque, into: []u64) bool,
        destroy: *const fn (ctx: *anyopaque) void,
    };
    /// Returns true if search produces a new tuple,
    /// which in turn gets written to an `into` slice.
    /// If method returns false, buffer might contain garbage.
    pub fn next(self: *const TupleIterator, into: []u64) bool {
        return self.vtable.next(self.ptr, into);
    }
    pub fn destroy(self: *const TupleIterator) void {
        self.vtable.destroy(self.ptr);
    }
};

/// Trivial tuple iterator backed by a slice.
/// Can be used to produce iterators from ArrayLists as well as singleton iterators.
pub const SliceTupleIterator = struct {
    /// Non-null for iterators created on heap to pass up the stack,
    /// like for iterators created inside relation backends.
    allocator: ?Allocator = null,
    slice: []const u64,
    index: usize = 0,
    arity: usize,

    pub fn ofSingleton(tuple: []const u64) SliceTupleIterator {
        return .{ .slice = tuple, .arity = tuple.len };
    }

    pub fn iterator(self: *SliceTupleIterator) TupleIterator {
        return .{ .vtable = VTable, .ptr = self };
    }

    const VTable = &TupleIterator.VTable{
        .destroy = destroy,
        .next = next,
    };

    fn destroy(ptr: *anyopaque) void {
        const self: *SliceTupleIterator = @ptrCast(@alignCast(ptr));
        if (self.allocator) |allocator| allocator.destroy(self);
    }

    fn next(ptr: *anyopaque, into: []u64) bool {
        const self: *SliceTupleIterator = @ptrCast(@alignCast(ptr));
        std.debug.assert(self.arity == into.len);
        const a = self.arity;
        const i = self.index;
        if (i * a < self.slice.len) {
            self.index += 1;
            @memcpy(into, self.slice[i * a .. i * a + a]);
            return true;
        } else {
            return false;
        }
    }
};

/// Returns tuples from backend filtered by the pattern.
/// Note it owns the backend iterator, call to destroy destroys the backend.
pub const QueryTupleIterator = struct {
    /// Non-null when iterator was created on heap.
    allocator: ?Allocator,
    backend: TupleIterator,
    pattern: []const ?u64,

    pub fn iterator(self: *QueryTupleIterator) TupleIterator {
        return .{ .vtable = VTable, .ptr = self };
    }

    const VTable = &TupleIterator.VTable{
        .destroy = destroy,
        .next = next,
    };

    fn destroy(ptr: *anyopaque) void {
        const self: *QueryTupleIterator = @ptrCast(@alignCast(ptr));
        self.backend.destroy();
        if (self.allocator) |allocator| allocator.destroy(self);
    }

    fn next(ptr: *anyopaque, into: []u64) bool {
        const self: *QueryTupleIterator = @ptrCast(@alignCast(ptr));
        std.debug.assert(self.pattern.len == into.len);
        loop: while (self.backend.next(into)) {
            for (self.pattern, 0..) |pat, j| {
                if (pat) |q| if (q != into[j]) continue :loop;
            }
            return true;
        }
        return false;
    }
};

pub const InsertError = error{RelationInsertError} || Allocator.Error;

/// Abstract writer for a relational storage.
/// Immutable extensional database relations can omit implementation for insert.
pub const RelationStorage = struct {
    ptr: *anyopaque,
    insertFn: *const fn (
        ctx: *anyopaque,
        iterator: TupleIterator,
        allocator: Allocator,
    ) InsertError!usize,
    pub fn insert(
        self: *const RelationStorage,
        iterator: TupleIterator,
        allocator: Allocator,
    ) InsertError!usize {
        return self.insertFn(self.ptr, iterator, allocator);
    }
};

pub const AccessError = error{RelationAccessError} || Allocator.Error;

/// Enables querying a relational storage.
pub const RelationBackend = struct {
    ptr: *anyopaque,
    queryFn: *const fn (
        ctx: *anyopaque,
        pattern: []const ?u64,
        allocator: Allocator,
    ) AccessError!TupleIterator,
    /// Returns an iterator over an entire relation.
    tuplesFn: *const fn (ctx: *anyopaque, allocator: Allocator) AccessError!TupleIterator,
    pub fn query(
        self: *const RelationBackend,
        pattern: []const ?u64,
        allocator: Allocator,
    ) AccessError!TupleIterator {
        return self.queryFn(self.ptr, pattern, allocator);
    }
    pub fn tuples(
        self: *const RelationBackend,
        allocator: Allocator,
    ) AccessError!TupleIterator {
        self.tuplesFn(self.ptr, allocator);
    }
};

pub const KDTree = struct {
    /// Arity of stored tuples.
    arity: usize,
    allocator: Allocator,
    /// Stores tuple data into a single continuous array.
    tuples: std.ArrayListUnmanaged(u64) = .{},
    /// Stores all tree nodes into a single continuous array for better locality.
    nodes: std.ArrayListUnmanaged(Node) = .{},

    /// Refers to index in the tuples data array,
    /// and to left and right nodes in the nodes array.
    /// Packed structs can't contain optional unions:
    /// instead we know that a tree is a tree and
    /// child nodes can't link to a root,
    /// so left and right cannot have a meaningfull 0 value.
    const Node = packed struct {
        hash: u64,
        left: usize,
        right: usize,
    };

    pub fn deinit(self: *KDTree) void {
        self.tuples.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
    }

    // Interfaces:

    pub fn storage(self: *KDTree) RelationStorage {
        return .{ .ptr = self, .insertFn = storageInsert };
    }

    fn storageInsert(ptr: *anyopaque, iterator: TupleIterator, allocator: Allocator) InsertError!usize {
        const self: *KDTree = @ptrCast(@alignCast(ptr));
        var count: usize = 0;
        const buff = try allocator.alloc(u64, self.arity);
        // TODO: presort to make k-d tree balanced at least after one insert?
        while (iterator.next(buff)) {
            if (try self.insertNode(buff)) count += 1;
        }
        return count;
    }

    pub fn backend(self: *KDTree) RelationBackend {
        return .{ .ptr = self, .queryFn = backendQuery, .tuplesFn = backendTuples };
    }

    fn backendQuery(ptr: *anyopaque, pattern: []const ?u64, allocator: Allocator) AccessError!TupleIterator {
        const self: *KDTree = @ptrCast(@alignCast(ptr));
        const it = try allocator.create(QueryIterator);
        errdefer allocator.destroy(it);
        it.* = .{
            .allocator = allocator,
            .pattern = pattern,
            .tree = self,
            .queue = QueryIterator.Queue.init(allocator),
        };
        errdefer it.queue.deinit();
        if (0 < self.nodes.items.len) {
            try it.queue.writeItem(.{ .depth = 0, .node_idx = 0 });
        }
        return it.iterator();
    }

    fn backendTuples(ptr: *anyopaque, allocator: Allocator) AccessError!TupleIterator {
        const self: *KDTree = @ptrCast(@alignCast(ptr));
        const it = try allocator.create(SliceTupleIterator);
        it.* = .{ .allocator = allocator, .arity = self.arity, .slice = self.tuples.items };
        return it.iterator();
    }

    const QueryIterator = struct {
        const State = struct { depth: usize, node_idx: usize };
        const Queue = std.fifo.LinearFifo(State, .Dynamic);
        allocator: Allocator,
        pattern: []const ?u64,
        tree: *KDTree,

        /// BFS queue with node idx:
        queue: Queue,

        pub fn iterator(self: *@This()) TupleIterator {
            return .{ .vtable = VTable, .ptr = self };
        }

        const VTable = &TupleIterator.VTable{
            .destroy = destroy,
            .next = next,
        };

        fn destroy(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.queue.deinit();
            self.allocator.destroy(self);
        }

        fn next(ptr: *anyopaque, into: []u64) bool {
            errdefer unreachable; // TODO make .next() also throw an error I guess.
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.pattern.len == into.len);
            while (self.queue.readItem()) |state| {
                const node = &self.tree.nodes.items[state.node_idx];
                if (self.pattern[state.depth % self.tree.arity]) |pat| {
                    const comp = self.tree.elemAt(state.node_idx, state.depth % self.tree.arity);
                    if (pat < comp) {
                        // Inspect only left subtree.
                        if (0 < node.left) {
                            try self.queue.writeItem(.{ .depth = 1 + state.depth, .node_idx = node.left });
                        }
                    } else {
                        // Inspect only right subtree.
                        if (0 < node.right) {
                            try self.queue.writeItem(.{ .depth = 1 + state.depth, .node_idx = node.right });
                        }
                    }
                } else {
                    // It's a Wildcard, add both subtrees to BFS:
                    if (0 < node.left) {
                        try self.queue.writeItem(.{ .depth = 1 + state.depth, .node_idx = node.left });
                    }
                    if (0 < node.right) {
                        try self.queue.writeItem(.{ .depth = 1 + state.depth, .node_idx = node.right });
                    }
                }
                const tuple = self.tree.tupleAt(state.node_idx);
                if (self.matches(tuple)) {
                    @memcpy(into, tuple);
                    return true;
                }
            }
            return false;
        }

        fn matches(self: *QueryIterator, tuple: []const u64) bool {
            for (self.pattern, 0..) |pat, j| {
                if (pat) |q| if (q != tuple[j]) return false;
            }
            return true;
        }
    };

    pub fn backup(self: *KDTree, into: std.io.AnyWriter) !void {
        // TODO: this function just yolo-s the memory layout into file,
        // using platform's endianness and usize. But it should be fine?
        const builtin = @import("builtin");
        const endian = builtin.cpu.arch.endian();
        try into.writeInt(usize, self.arity, endian);
        {
            const binary_nodes = std.mem.sliceAsBytes(self.nodes.items);
            try into.writeInt(usize, self.nodes.items.len, endian);
            try into.writeAll(binary_nodes);
        }
        {
            const binary_tuples = std.mem.sliceAsBytes(self.tuples.items);
            try into.writeInt(usize, self.tuples.items.len, endian);
            try into.writeAll(binary_tuples);
        }
    }

    pub fn restore(from: std.io.AnyReader, allocator: Allocator) !KDTree {
        const builtin = @import("builtin");
        const endian = builtin.cpu.arch.endian();
        const from_arity = try from.readInt(usize, endian);
        const nodes_count = try from.readInt(usize, endian);
        const from_nodes: []Node = try allocator.alloc(Node, nodes_count);
        errdefer allocator.free(from_nodes);
        _ = try from.readAll(std.mem.sliceAsBytes(from_nodes));

        const tuples_count = try from.readInt(usize, endian);
        const from_tuples = try allocator.alloc(u64, tuples_count);
        errdefer allocator.free(from_tuples);
        _ = try from.readAll(std.mem.sliceAsBytes(from_tuples));
        return KDTree{
            .allocator = allocator,
            .arity = from_arity,
            .tuples = std.ArrayListUnmanaged(u64){
                .items = from_tuples,
                .capacity = from_tuples.len,
            },
            .nodes = std.ArrayListUnmanaged(Node){
                .items = from_nodes,
                .capacity = from_nodes.len,
            },
        };
    }

    // Internals:

    /// Returns tuple's element.
    inline fn elemAt(self: *KDTree, index: usize, elem: usize) u64 {
        return self.tuples.items[index * self.arity + elem];
    }

    inline fn tupleAt(self: *KDTree, index: usize) []const u64 {
        return self.tuples.items[index * self.arity .. self.arity + index * self.arity];
    }

    fn insertNode(self: *KDTree, tuple: []const u64) InsertError!bool {
        const hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(tuple));
        if (self.nodes.items.len == 0) {
            // No root node special case.
            try self.tuples.appendSlice(self.allocator, tuple);
            const node = try self.nodes.addOne(self.allocator);
            node.* = .{
                .hash = hash,
                .left = 0,
                .right = 0,
            };
            return true;
        }
        if (self.lookupNodeSlot(tuple, hash)) |ptr| {
            const node_idx = self.nodes.items.len;
            // TODO: if any of those fails, leaves tree in unconsistent state
            // however probably an application should just crash on Out of memory
            // and call it a day.
            try self.tuples.appendSlice(self.allocator, tuple);
            const node = try self.nodes.addOne(self.allocator);
            node.* = .{
                .hash = hash,
                .left = 0,
                .right = 0,
            };
            ptr.* = node_idx;
            return true;
        } else {
            return false;
        }
    }

    /// Find a fitting free slot in a tree node.
    /// It points to left or right field of an existing tree node.
    /// Returns null if target tuple is a dublicate.
    fn lookupNodeSlot(self: *KDTree, tuple: []const u64, hash: u64) ?*usize {
        std.debug.assert(0 < self.nodes.items.len);
        var depth: usize = 0;
        var slot: ?*usize = null;
        var at: usize = 0;
        while (true) {
            const node = &self.nodes.items[at];
            const comp = self.elemAt(at, depth % self.arity);
            const proj = tuple[depth % self.arity];
            if (proj < comp) {
                depth += 1;
                slot = &node.left;
            } else if (proj == comp) {
                // First, check for a hash collision:
                if (hash == node.hash) {
                    // Second, tuple equality:
                    if (std.mem.eql(u64, tuple, self.tupleAt(at))) {
                        return null;
                    }
                }
                // Pass it to the right sub-branch:
                depth += 1;
                slot = &node.right;
            } else {
                depth += 1;
                slot = &node.right;
            }
            at = slot.?.*;
            if (at == 0) return slot;
        }
    }
};

pub const Db = struct {
    const load_factor = std.hash_map.default_max_load_percentage;
    allocator: Allocator,
    relations: std.HashMapUnmanaged(Relation, *KDTree, Relation.Equality, load_factor) = .{},
    pub fn create(allocator: Allocator) Db {
        return .{ .allocator = allocator };
    }
    pub fn setListRelationBackend(self: *Db, relation: Relation) Allocator.Error!void {
        if (!self.relations.contains(relation)) {
            const backend = try self.allocator.create(KDTree);
            errdefer self.allocator.destroy(backend);
            backend.* = .{
                .allocator = self.allocator,
                .arity = relation.arity,
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
        const backend = rel.backend();
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
