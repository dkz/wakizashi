const std = @import("std");
const db = @import("database.zig");
const Allocator = std.mem.Allocator;

pub const KDTree = struct {
    /// Arity of stored tuples.
    arity: usize,
    allocator: Allocator,
    tuples_counter: usize = 0,
    /// Stores tuple data into a single continuous array.
    tuples: std.ArrayListUnmanaged(u64) = .{},
    /// Stores tuple hashes for duplicate detection.
    hashes: std.ArrayListUnmanaged(u64) = .{},
    /// Stores all tree nodes into a single continuous array for better locality.
    nodes: std.ArrayListUnmanaged(Node) = .{},

    root: ?usize = null,

    pub fn deinit(self: *KDTree) void {
        self.tuples.deinit(self.allocator);
        self.hashes.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
    }

    // Interfaces:

    pub fn storage(self: *KDTree) db.RelationStorage {
        return .{ .ptr = self, .insertFn = storageInsert };
    }

    fn storageInsert(ptr: *anyopaque, iterator: db.TupleIterator, allocator: Allocator) db.InsertError!usize {
        const self: *KDTree = @ptrCast(@alignCast(ptr));
        var count: usize = 0;
        const buff = try allocator.alloc(u64, self.arity);
        // TODO: presort to make k-d tree balanced at least after one insert?
        while (iterator.next(buff)) {
            if (try self.insertNode(buff)) count += 1;
        }
        return count;
    }

    pub fn backend(self: *KDTree) db.RelationBackend {
        return .{ .ptr = self, .queryFn = backendQuery, .tuplesFn = backendTuples };
    }

    fn backendQuery(ptr: *anyopaque, pattern: []const ?u64, allocator: Allocator) db.AccessError!db.TupleIterator {
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
        if (self.root) |root| {
            try it.queue.writeItem(.{ .depth = 0, .node_idx = root });
        }
        return it.iterator();
    }

    fn backendTuples(ptr: *anyopaque, allocator: Allocator) db.AccessError!db.TupleIterator {
        const self: *KDTree = @ptrCast(@alignCast(ptr));
        const it = try allocator.create(db.SliceTupleIterator);
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

        pub fn iterator(self: *@This()) db.TupleIterator {
            return .{ .vtable = VTable, .ptr = self };
        }

        const VTable = &db.TupleIterator.VTable{
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
                    const comp = self.tree.elemAt(node.index, state.depth % self.tree.arity);
                    if (pat < comp) {
                        // Inspect only left subtree.
                        if (node.left) |l| {
                            try self.queue.writeItem(.{ .depth = 1 + state.depth, .node_idx = l });
                        }
                    } else {
                        // Inspect only right subtree.
                        if (node.right) |r| {
                            try self.queue.writeItem(.{ .depth = 1 + state.depth, .node_idx = r });
                        }
                    }
                } else {
                    // It's a Wildcard, add both subtrees to BFS:
                    if (node.left) |l| {
                        try self.queue.writeItem(.{ .depth = 1 + state.depth, .node_idx = l });
                    }
                    if (node.right) |r| {
                        try self.queue.writeItem(.{ .depth = 1 + state.depth, .node_idx = r });
                    }
                }
                const tuple = self.tree.tupleAt(node.index);
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

    // Internals:

    /// Returns tuple's element.
    inline fn elemAt(self: *KDTree, index: usize, elem: usize) u64 {
        return self.tuples.items[index * self.arity + elem];
    }

    inline fn tupleAt(self: *KDTree, index: usize) []const u64 {
        return self.tuples.items[index * self.arity .. self.arity + index * self.arity];
    }

    fn insertNode(self: *KDTree, tuple: []const u64) db.InsertError!bool {
        const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(tuple));
        if (self.lookupNodeSlot(tuple, hash)) |ptr| {
            const node_idx = self.nodes.items.len;
            const index = self.tuples_counter;
            self.tuples_counter += 1;
            // TODO: if any of those fails, leaves tree in unconsistent state
            // however probably an application should just crash on Out of memory
            // and call it a day.
            try self.tuples.appendSlice(self.allocator, tuple);
            try self.hashes.append(self.allocator, hash);
            const node = try self.nodes.addOne(self.allocator);
            node.* = .{
                .index = index,
                .left = null,
                .right = null,
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
    fn lookupNodeSlot(self: *KDTree, tuple: []const u64, hash: u64) ?*?usize {
        var depth: usize = 0;
        var at: *?usize = &self.root;
        while (at.*) |node_idx| {
            const node = &self.nodes.items[node_idx];
            const comp = self.elemAt(node.index, depth % self.arity);
            const proj = tuple[depth % self.arity];
            if (proj < comp) {
                depth += 1;
                at = &node.left;
            } else if (proj == comp) {
                // First, check for a hash collision:
                if (hash == self.hashes.items[node.index]) {
                    // Second, tuple equality:
                    if (std.mem.eql(u64, tuple, self.tupleAt(node.index))) {
                        return null;
                    }
                }
                // Pass it to the right sub-branch:
                depth += 1;
                at = &node.right;
            } else {
                depth += 1;
                at = &node.right;
            }
        }
        return at;
    }

    /// Refers to index in the tuples data array,
    ///  and to left and right nodes in the nodes array.
    const Node = struct {
        index: usize,
        left: ?usize,
        right: ?usize,
    };
};
