//! Everything related to encoding, decoding, storing, and querying tuples.
//! Database tuples are encoded as []u64 slice, with Domain struct providing
//! encoding and decoding facilities.
//!
//! Database functions allowed to return OutOfMemory error.
//! Catch it on top and report a user-friendly message of what program was doing
//! when it encountered an OutOfMemory, then panic and crash.

const std = @import("std");
const mem = std.mem;
const fmt = std.fmt;
const builtin = @import("builtin");
const endian = builtin.cpu.arch.endian();
const assert = std.debug.assert;
const panic = std.debug.panic;

const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ThreadSafeAllocator = std.heap.ThreadSafeAllocator;
const ArrayList = std.ArrayList;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const HashMapUnmanaged = std.HashMapUnmanaged;
const Wyhash = std.hash.Wyhash;
const Random = std.Random;

const concurrent = @import("concurrent.zig");
const Channel = concurrent.Channel;
const ThreadPool = std.Thread.Pool;

const AnyWriter = std.io.AnyWriter;
const AnyReader = std.io.AnyReader;
const StreamSource = std.io.StreamSource;

const Dir = std.fs.Dir;
const File = std.fs.File;

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
        pub fn iterator(context: *SliceIterator) TupleIterator {
            const interface = struct {
                fn destroyFn(ptr: *anyopaque) void {
                    const self: *SliceIterator = @ptrCast(@alignCast(ptr));
                    if (self.allocator) |allocator| allocator.destroy(self);
                }
                fn nextFn(ptr: *anyopaque, into: []u64) bool {
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
            return .{
                .ptr = context,
                .nextFn = interface.nextFn,
                .destroyFn = interface.destroyFn,
            };
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
pub const tuples = struct {
    /// Test whether tuple matches the specified pattern.
    fn matches(pattern: []const ?u64, tuple: []const u64) bool {
        for (pattern, 0..) |pat, j| {
            if (pat) |q| if (q != tuple[j]) return false;
        }
        return true;
    }

    /// Stores tuples of same arity side by side in an array list,
    /// and provides a slice iterator for contents.
    pub const DirectCollection = struct {
        arity: usize,
        array: ArrayListUnmanaged(u64) = .{},
        pub fn deinit(self: *DirectCollection, allocator: Allocator) void {
            self.array.deinit(allocator);
        }
        pub inline fn insert(
            self: *DirectCollection,
            allocator: Allocator,
            tuple: []const u64,
        ) Allocator.Error!void {
            assert(self.arity == tuple.len);
            return self.array.appendSlice(allocator, tuple);
        }
        pub inline fn insertFromIterator(
            self: *DirectCollection,
            it: TupleIterator,
            allocator: Allocator,
        ) Allocator.Error!void {
            const buffer = try allocator.alloc(u64, self.arity);
            defer allocator.free(buffer);
            while (it.next(buffer)) try self.insert(allocator, buffer);
        }
        pub inline fn length(self: *DirectCollection) usize {
            return self.array.items.len / self.arity;
        }
        pub inline fn elementAt(self: *DirectCollection, index: usize, elem: usize) u64 {
            return self.array.items[index * self.arity + elem];
        }
        pub inline fn tupleAt(self: *DirectCollection, index: usize) []u64 {
            return self.array.items[index * self.arity .. self.arity + index * self.arity];
        }
        pub fn iterator(self: *DirectCollection, allocator: Allocator) Allocator.Error!TupleIterator {
            const it = try allocator.create(iterators.SliceIterator);
            it.* = .{ .allocator = allocator, .arity = self.arity, .slice = self.array.items };
            return it.iterator();
        }
        fn pointers(self: *DirectCollection, allocator: Allocator) Allocator.Error![][*]const u64 {
            const coll = try allocator.alloc([*]const u64, self.length());
            for (0..self.length()) |j| coll[j] = self.tupleAt(j).ptr;
            return coll;
        }
        pub fn fromIterator(
            arity: usize,
            it: TupleIterator,
            allocator: Allocator,
        ) Allocator.Error!DirectCollection {
            var target = DirectCollection{ .arity = arity };
            try target.insertFromIterator(it, allocator);
            return target;
        }
    };

    /// Hoare's k-smallest selection algorithm based on Lomuto partition scheme,
    /// adapted for multi-dimensional tuples. Uses many-item pointers to avoid memcpy.
    const quickselect = struct {
        /// While Lomuto scheme performs more swaps, it guarantees that perition index
        /// contains the pivot element. Hoare's scheme can't guarantee that pivot element
        /// lands at parition index which becomes extremelly annoying for locating a median.
        fn partition(
            list: [][*]const u64,
            lower_index_inclusive: usize,
            upper_index_inclusive: usize,
            pivot_index: usize,
            component: usize,
        ) usize {
            assert(lower_index_inclusive < upper_index_inclusive);
            const pivot = list[pivot_index];
            {
                const t = list[upper_index_inclusive];
                list[upper_index_inclusive] = list[pivot_index];
                list[pivot_index] = t;
            }
            var store = lower_index_inclusive;
            for (lower_index_inclusive..upper_index_inclusive) |j| {
                if (list[j][component] < pivot[component]) {
                    const t = list[store];
                    list[store] = list[j];
                    list[j] = t;
                    store += 1;
                }
            }
            {
                const t = list[store];
                list[store] = list[upper_index_inclusive];
                list[upper_index_inclusive] = t;
            }
            return store;
        }
        /// K-smallest quick select using Lomuto scheme defined above.
        fn select(
            list: [][*]const u64,
            with_lower_index_inclusive: usize,
            with_upper_index_inclusive: usize,
            index: usize,
            component: usize,
            random: Random,
        ) usize {
            assert(with_lower_index_inclusive < with_upper_index_inclusive);
            var state: struct { usize, usize } = .{
                with_lower_index_inclusive,
                with_upper_index_inclusive,
            };
            while (true) {
                const lower, const upper = state;
                if (lower == upper) {
                    return lower;
                }
                const pi = random.intRangeAtMostBiased(usize, lower, upper);
                const split = partition(list, lower, upper, pi, component);
                if (split == index) {
                    return split;
                } else if (index < split) {
                    state = .{ lower, split - 1 };
                } else {
                    state = .{ split + 1, upper };
                }
            }
        }
        fn median(
            list: [][*]const u64,
            component: usize,
            random: Random,
        ) struct { [][*]const u64, usize, [][*]const u64 } {
            assert(0 < list.len);
            const i = select(list, 0, list.len - 1, list.len / 2, component, random);
            return .{ list[0..i], i, list[i + 1 .. list.len] };
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
        return self.eachFn(self.ptr, allocator);
    }
};

/// Binary K-dimentional tree format for storing static, balanced trees in files.
const kdf = struct {
    /// Each tree node starts with a header, followed by tuple data record.
    /// Node header stores offsets to the left subtree (lt) and the right (ge).
    /// Zero offset means there's no child.
    const NodeHeader = extern struct {
        lt: usize = 0,
        ge: usize = 0,
    };

    /// ArrayList's slice and its address gets invalidated after growth,
    /// so instead of pointers, export code uses MemoryStored to keep
    /// an offset inside an ArrayList, enabling quick reference via ptr().
    const MemoryStored = struct {
        offset: usize,
        inline fn ptr(self: *const MemoryStored, in: *ArrayList(u8)) *NodeHeader {
            return @alignCast(mem.bytesAsValue(NodeHeader, in.items[self.offset..]));
        }
    };

    /// Stores a single tuple into memory block, prepending a header to it.
    /// Returns header's MemoryStored, used later to store a correct offset
    /// to it's children.
    fn exportNode(into: *ArrayList(u8), tuple: []const u64) Allocator.Error!MemoryStored {
        const offset = into.items.len;
        try into.appendSlice(mem.asBytes(&NodeHeader{}));
        try into.appendSlice(mem.sliceAsBytes(tuple));
        return MemoryStored{ .offset = offset };
    }

    /// Reads node header and the corresponding tuple at current position,
    /// following binary format defined by exportNode().
    fn importNode(from: *StreamSource, into: []u64) !?NodeHeader {
        const reader = from.reader();
        const header = header: {
            var nb: [@sizeOf(NodeHeader)]u8 = undefined;
            reader.readNoEof(&nb) catch |e| switch (e) {
                error.EndOfStream => return null,
                else => return e,
            };
            break :header mem.bytesToValue(NodeHeader, &nb);
        };
        reader.readNoEof(mem.sliceAsBytes(into)) catch |e| switch (e) {
            error.EndOfStream => return null,
            else => return e,
        };
        return header;
    }

    const SubtreeType = enum { lt, ge };
    const ExportState = struct {
        t: SubtreeType,
        parent: MemoryStored,
        subtree: [][*]const u64,
        depth: usize,
        fn descendLt(
            self: ExportState,
            header: MemoryStored,
            lt: [][*]const u64,
        ) ExportState {
            return .{
                .t = .lt,
                .parent = header,
                .subtree = lt,
                .depth = 1 + self.depth,
            };
        }
        fn descendGe(
            self: ExportState,
            header: MemoryStored,
            ge: [][*]const u64,
        ) ExportState {
            return .{
                .t = .ge,
                .parent = header,
                .subtree = ge,
                .depth = 1 + self.depth,
            };
        }
    };

    fn exportSubtrees(
        arity: usize,
        into: *ArrayList(u8),
        stack: *ArrayList(ExportState),
        random: Random,
    ) Allocator.Error!void {
        while (stack.pop()) |from| {
            if (1 < from.subtree.len) {
                const lt, const median, const ge =
                    tuples.quickselect.median(from.subtree, from.depth % arity, random);
                const stored = try exportNode(into, from.subtree[median][0..arity]);
                const parent = from.parent.ptr(into);
                switch (from.t) {
                    .lt => parent.lt = stored.offset,
                    .ge => parent.ge = stored.offset,
                }
                if (0 < ge.len) try stack.append(from.descendGe(stored, ge));
                if (0 < lt.len) try stack.append(from.descendLt(stored, lt));
            } else {
                const stored = try exportNode(into, from.subtree[0][0..arity]);
                const parent = from.parent.ptr(into);
                switch (from.t) {
                    .lt => parent.lt = stored.offset,
                    .ge => parent.ge = stored.offset,
                }
            }
        }
    }

    /// Export a set of tuples into a static in-memory k-d tree buffer.
    fn exportTree(
        arity: usize,
        into: *ArrayList(u8),
        from: [][*]const u64,
        allocator: Allocator,
    ) Allocator.Error!void {
        if (1 < from.len) {
            var rng = Random.Xoroshiro128.init(@bitCast(std.time.microTimestamp()));
            var stack = ArrayList(ExportState).init(allocator);
            defer stack.deinit();
            const lt, const median, const ge =
                tuples.quickselect.median(from, 0, rng.random());
            const header = try exportNode(into, from[median][0..arity]);
            if (0 < ge.len) try stack.append(.{
                .t = .ge,
                .depth = 1,
                .parent = header,
                .subtree = ge,
            });
            if (0 < lt.len) try stack.append(.{
                .t = .lt,
                .depth = 1,
                .parent = header,
                .subtree = lt,
            });
            return exportSubtrees(arity, into, &stack, rng.random());
        } else {
            _ = try exportNode(into, from[0][0..arity]);
        }
    }

    const EachIterator = struct {
        /// Null for on-stack iterators.
        allocator: ?Allocator = null,
        /// Make sure source is not shared, as position might jump between calls
        /// and iterator becomes inconsistent.
        source: StreamSource,
        arity: usize,
        pub fn iterator(self: *EachIterator) TupleIterator {
            return .{ .ptr = self, .destroyFn = destroy, .nextFn = next };
        }
        fn destroy(ptr: *anyopaque) void {
            const self: *EachIterator = @ptrCast(@alignCast(ptr));
            if (self.allocator) |allocator| allocator.destroy(self);
        }
        fn next(ptr: *anyopaque, into: []u64) bool {
            errdefer unreachable;
            const self: *EachIterator = @ptrCast(@alignCast(ptr));
            const head = try importNode(&self.source, into);
            return head != null;
        }
    };

    /// Query iterator tailored for the described binary format.
    const QueryIterator = struct {
        const Stack = ArrayListUnmanaged(State);
        const State = struct {
            at: usize = 0,
            depth: usize = 0,
            fn descend(self: State, to: usize) State {
                return .{ .at = to, .depth = 1 + self.depth };
            }
        };
        arity: usize,
        source: StreamSource,
        pattern: []const ?u64,
        stack_allocator: Allocator,
        stack: Stack = .{},
        pub fn iterator(self: *QueryIterator) TupleIterator {
            return .{ .ptr = self, .destroyFn = destroy, .nextFn = next };
        }
        fn destroy(ptr: *anyopaque) void {
            const self: *QueryIterator = @ptrCast(@alignCast(ptr));
            self.stack.deinit(self.stack_allocator);
        }
        fn next(ptr: *anyopaque, into: []u64) bool {
            errdefer unreachable; // TODO make .next() also throw an error.
            const self: *QueryIterator = @ptrCast(@alignCast(ptr));
            assert(self.pattern.len == into.len);
            while (self.stack.pop()) |state| {
                const elem = state.depth % self.arity;
                try self.source.seekTo(state.at);
                if (try importNode(&self.source, into)) |header| {
                    if (self.pattern[elem]) |pat| {
                        if (pat < into[elem]) {
                            if (0 < header.lt)
                                try self.stack.append(self.stack_allocator, state.descend(header.lt));
                        } else {
                            if (0 < header.ge)
                                try self.stack.append(self.stack_allocator, state.descend(header.ge));
                        }
                    } else {
                        if (0 < header.ge) try self.stack.append(self.stack_allocator, state.descend(header.ge));
                        if (0 < header.lt) try self.stack.append(self.stack_allocator, state.descend(header.lt));
                    }
                    if (tuples.matches(self.pattern, into)) return true;
                }
            }
            return false;
        }
    };

    fn importAsIterator(
        from: StreamSource,
        arity: usize,
        allocator: Allocator,
    ) !TupleIterator {
        const it = try allocator.create(EachIterator);
        it.* = EachIterator{
            .arity = arity,
            .source = from,
            .allocator = allocator,
        };
        return it.iterator();
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
    tuples: tuples.DirectCollection,

    /// Each node saves only the hash of the tuple to detect duplicates,
    /// and index pointers to the left and right nodes.
    /// Extern and packed structs do not allow optional unions,
    /// hence value 0 acts as an indicator of abcense (tree nodes can't point to root).
    const Node = struct {
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
        try self.tuples.insert(self.allocator, tuple);
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
        defer allocator.free(buffer);
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
                return target.tuples.iterator(allocator);
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

/// Block k-d tree backed by a forest directory of persisted k-d trees.
/// Each bucket in the forest is a static balanced k-d tree of `base * 2^l` elements.
/// Trees are either full or empty.
const BlockKDTree = struct {
    const base_size = 1024;
    arity: usize,
    allocator: Allocator,
    staging: DynamicKDTree,
    buckets: Dir,
    fn open(arity: usize, directory: Dir, allocator: Allocator) !BlockKDTree {
        var arena = ArenaAllocator.init(allocator);
        defer arena.deinit();
        const local = arena.allocator();
        var staging = DynamicKDTree{
            .allocator = allocator,
            .tuples = tuples.DirectCollection{ .arity = arity },
        };
        import_staging: {
            const file = directory.openFile("staging", .{}) catch |e| {
                switch (e) {
                    error.FileNotFound => break :import_staging,
                    else => return e,
                }
            };
            defer file.close();
            var it = try kdf.importAsIterator(
                StreamSource{ .file = file },
                arity,
                local,
            );
            defer it.destroy();
            _ = try staging.insertFromIterator(it, local);
        }
        return BlockKDTree{
            .arity = arity,
            .allocator = allocator,
            .buckets = directory,
            .staging = staging,
        };
    }
    fn flush(self: *BlockKDTree, allocator: Allocator) !void {
        var arena = ArenaAllocator.init(allocator);
        defer arena.deinit();
        const local = arena.allocator();
        const file = try self.buckets.createFile("staging", .{ .truncate = true });
        defer file.close();
        if (0 < self.staging.tuples.length()) {
            var into = ArrayList(u8).init(local);
            const source = try self.staging.tuples.pointers(local);
            try kdf.exportTree(self.arity, &into, source, local);
            try file.writeAll(into.items);
        }
    }
    fn close(self: *BlockKDTree) void {
        self.staging.deinit();
        self.buckets.close();
    }
    fn insert(self: *BlockKDTree, tuple: []const u64, allocator: Allocator) !void {
        _ = try self.staging.insert(tuple);
        if (base_size <= self.staging.tuples.length()) {
            var arena = ArenaAllocator.init(allocator);
            defer arena.deinit();
            const local = arena.allocator();
            var merge = merge: {
                const it = try self.staging.tuples.iterator(local);
                var result = tuples.DirectCollection{ .arity = self.arity };
                try result.insertFromIterator(it, local);
                self.staging.deinit();
                self.staging = DynamicKDTree{
                    .allocator = self.allocator,
                    .tuples = tuples.DirectCollection{ .arity = self.arity },
                };
                break :merge result;
            };
            var j: usize = 0;
            while (true) : (j += 1) {
                if (try self.importBucket(j, local)) |bucket| {
                    try merge.insertFromIterator(try bucket.iterator(), local);
                    bucket.destroy();
                    try self.removeBucket(j);
                } else {
                    try self.exportBucket(j, &merge, local);
                    return;
                }
            }
        }
    }

    const WorkerMessage = union(enum) {
        NextTuple: []const u64,
        Terminated,
    };

    const WorkerChannel = Channel(WorkerMessage, 1024);

    fn queryWorker(
        from: File,
        arity: usize,
        pattern: []const ?u64,
        owner: *ConcurrentQueryIterator,
    ) void {
        defer from.close();
        defer owner.channel.send(.Terminated);

        var local = std.heap.GeneralPurposeAllocator(.{}).init;
        const allocator = local.allocator();
        defer {
            const state = local.deinit();
            if (state != .ok) {
                panic("Leak detected in a BlockKDTree worker thread.\n", .{});
            }
        }

        const buffer = allocator.alloc(u64, arity) catch |e| {
            panic("BKD-tree buffer allocation failed, ran out of memory: {}\n", .{e});
        };
        defer allocator.free(buffer);

        var streamer = kdf.QueryIterator{
            .arity = arity,
            .pattern = pattern,
            .stack_allocator = allocator,
            .source = StreamSource{ .file = from },
        };
        streamer.stack.append(allocator, .{ .at = 0, .depth = 0 }) catch |e| {
            panic("BKD-tree stack allocation failed, ran out of memory: {}\n", .{e});
        };
        var it = streamer.iterator();
        defer it.destroy();

        while (it.next(buffer)) {
            const shared = owner.shared.allocator();
            const output = shared.alloc(u64, arity) catch |e| {
                panic("BKD-tree buffer allocation failed, ran out of memory: {}\n", .{e});
            };
            @memcpy(output, buffer);
            owner.channel.send(.{ .NextTuple = output });
        }
    }

    const ConcurrentQueryIterator = struct {
        allocator: Allocator,
        /// Drains iterator of the staging area before consuming tuples from worker channel.
        /// Afterwards, destroys the iterator and sets the field to `null`.
        staging_iterator: ?TupleIterator,
        /// Memory arena shared between workers, used to transfer data via the channel.
        shared_arena: *ArenaAllocator,
        shared: ThreadSafeAllocator,
        channel: WorkerChannel = .{},
        /// Number of workers connected to the output channel.
        workers: usize,
        fn init(
            allocator: Allocator,
            staging_iterator: TupleIterator,
            workers: usize,
        ) Allocator.Error!ConcurrentQueryIterator {
            const arena = try allocator.create(ArenaAllocator);
            arena.* = ArenaAllocator.init(allocator);
            return .{
                .allocator = allocator,
                .shared_arena = arena,
                .shared = ThreadSafeAllocator{ .child_allocator = arena.allocator() },
                .staging_iterator = staging_iterator,
                .workers = workers,
            };
        }
        fn iterator(self: *ConcurrentQueryIterator) TupleIterator {
            return TupleIterator{ .ptr = self, .destroyFn = destroy, .nextFn = next };
        }
        fn destroy(ptr: *anyopaque) void {
            const self: *ConcurrentQueryIterator = @ptrCast(@alignCast(ptr));
            if (self.staging_iterator) |s| s.destroy();
            self.shared_arena.deinit();
            self.allocator.destroy(self);
        }
        fn next(ptr: *anyopaque, into: []u64) bool {
            errdefer unreachable; // TODO next() must return errors.
            const self: *ConcurrentQueryIterator = @ptrCast(@alignCast(ptr));
            if (self.staging_iterator) |it| {
                if (it.next(into)) return true;
                it.destroy();
                self.staging_iterator = null;
            }
            while (0 < self.workers) {
                switch (self.channel.recv()) {
                    .Terminated => self.workers -|= 1,
                    .NextTuple => |tuple| {
                        const allocator = self.shared_arena.allocator();
                        defer allocator.free(tuple);
                        @memcpy(into, tuple);
                        return true;
                    },
                }
            }
            return false;
        }
    };

    fn query(
        self: *BlockKDTree,
        pattern: []const ?u64,
        allocator: Allocator,
        pool: *ThreadPool,
    ) !TupleIterator {
        var bit = self.buckets.iterate();
        var sources: ArrayListUnmanaged(File) = .empty;
        defer sources.deinit(allocator);
        errdefer for (sources.items) |f| f.close();
        files: while (try bit.next()) |*entry| {
            if (entry.kind != .file) continue;
            for (entry.name) |ch| {
                if (!std.ascii.isDigit(ch)) continue :files;
            }
            try sources.append(
                allocator,
                try self.buckets.openFile(entry.name, .{}),
            );
        }

        var it = try allocator.create(ConcurrentQueryIterator);
        it.* = try ConcurrentQueryIterator.init(
            allocator,
            try self.staging.query(pattern, allocator),
            sources.items.len,
        );
        errdefer allocator.destroy(it);
        for (sources.items) |source| try pool.spawn(
            queryWorker,
            .{ source, self.arity, pattern, it },
        );
        return it.iterator();
    }

    const BucketContext = struct {
        file: File,
        tree: *BlockKDTree,
        allocator: Allocator,
        fn iterator(self: *const BucketContext) !TupleIterator {
            return try kdf.importAsIterator(
                StreamSource{ .file = self.file },
                self.tree.arity,
                self.allocator,
            );
        }
        fn destroy(self: *const BucketContext) void {
            self.file.close();
        }
    };
    fn importBucket(self: *BlockKDTree, index: usize, allocator: Allocator) !?BucketContext {
        var buf: [256]u8 = undefined;
        const file = self.buckets.openFile(
            fmt.bufPrint(&buf, "{d}", .{index}) catch unreachable,
            .{},
        ) catch |e| switch (e) {
            error.FileNotFound => {
                return null;
            },
            else => return e,
        };
        return BucketContext{
            .allocator = allocator,
            .tree = self,
            .file = file,
        };
    }
    fn removeBucket(self: *BlockKDTree, index: usize) !void {
        var buf: [256]u8 = undefined;
        return self.buckets.deleteFile(
            fmt.bufPrint(&buf, "{d}", .{index}) catch unreachable,
        );
    }
    fn exportBucket(
        self: *BlockKDTree,
        index: usize,
        from: *tuples.DirectCollection,
        allocator: Allocator,
    ) !void {
        var arena = ArenaAllocator.init(allocator);
        defer arena.deinit();
        const local = arena.allocator();
        var buf: [256]u8 = undefined;
        const file = try self.buckets.createFile(
            fmt.bufPrint(&buf, "{d}", .{index}) catch unreachable,
            .{ .truncate = true },
        );
        defer file.close();
        var into = ArrayList(u8).init(local);
        const source = try from.pointers(local);
        try kdf.exportTree(self.arity, &into, source, local);
        try file.writeAll(into.items);
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

pub const Db = struct {
    const QueryError = error{QueryError} || Allocator.Error;
    ptr: *anyopaque,
    queryFn: *const fn (
        ctx: *anyopaque,
        relation: Relation,
        pattern: []const ?u64,
        allocator: Allocator,
    ) QueryError!TupleIterator,
    pub fn query(
        self: Db,
        relation: Relation,
        pattern: []const ?u64,
        allocator: Allocator,
    ) QueryError!TupleIterator {
        return self.queryFn(self.ptr, relation, pattern, allocator);
    }
};

/// Trivial Db implementation backed by in-memory dynamic k-d trees.
pub const MemoryDb = struct {
    allocator: Allocator,
    relations: HashMapUnmanaged(
        Relation,
        DynamicKDTree,
        Relation.Equality,
        std.hash_map.default_max_load_percentage,
    ) = .empty,
    pub fn store(
        self: *MemoryDb,
        relation: Relation,
        source: TupleIterator,
        allocator: Allocator,
    ) !usize {
        if (!self.relations.contains(relation)) {
            try self.relations.put(
                self.allocator,
                relation,
                DynamicKDTree{
                    .allocator = self.allocator,
                    .tuples = tuples.DirectCollection{
                        .arity = relation.arity,
                    },
                },
            );
        }
        if (self.relations.getPtr(relation)) |rel| {
            return rel.insertFromIterator(source, allocator);
        } else {
            return 0;
        }
    }
    pub fn db(self: *MemoryDb) Db {
        const interface = struct {
            fn queryFn(
                ctx: *anyopaque,
                relation: Relation,
                pattern: []const ?u64,
                allocator: Allocator,
            ) Db.QueryError!TupleIterator {
                const this: *MemoryDb = @ptrCast(@alignCast(ctx));
                if (this.relations.getPtr(relation)) |rel| {
                    return rel.query(pattern, allocator);
                } else return error.QueryError;
            }
        };
        return Db{
            .ptr = self,
            .queryFn = interface.queryFn,
        };
    }
};
