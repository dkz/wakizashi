const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const InsertError = error{DatabaseInsertError} || Allocator.Error;
pub const AccessError = error{DatabaseAccessError} || Allocator.Error;

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
    const VTable = struct {
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

pub const RelationBackend = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    const VTable = struct {
        /// Bulk insertion method, populates this backend by draining the iterator.
        /// Caller provides an allocator for intermediate operations like allocating a buffer.
        /// AllocatorError will be reported as an InsertError.
        /// Return the number of unique tuples inserted.
        insert: *const fn (
            ctx: *anyopaque,
            iterator: TupleIterator,
            allocator: Allocator,
        ) InsertError!usize,

        /// Returns an iterators for all tuples matching the pattern.
        /// Caller provides an allocator for intermediate operations and allocating the iterator.
        /// Caller owns the iterator.
        query: *const fn (
            ctx: *anyopaque,
            pattern: []const ?u64,
            allocator: Allocator,
        ) AccessError!TupleIterator,

        destroy: *const fn (ctx: *anyopaque) void,
    };

    pub fn insert(self: *RelationBackend, iterator: TupleIterator, allocator: Allocator) InsertError!usize {
        return self.vtable.insert(self.ptr, iterator, allocator);
    }

    pub fn query(self: *RelationBackend, pattern: []const ?u64, allocator: Allocator) AccessError!TupleIterator {
        return self.vtable.query(self.ptr, pattern, allocator);
    }

    pub fn destroy(self: *RelationBackend) void {
        self.vtable.destroy(self.ptr);
    }
};

pub const ListRelationBackend = struct {
    allocator: Allocator,
    array: std.ArrayListUnmanaged(u64) = .{},
    arity: usize,

    pub fn create(opts: ListRelationBackend) Allocator.Error!RelationBackend {
        const context = try opts.allocator.create(ListRelationBackend);
        context.* = opts;
        return RelationBackend{ .vtable = VTable, .ptr = context };
    }

    const VTable = &RelationBackend.VTable{
        .insert = insert,
        .query = query,
        .destroy = destroy,
    };

    fn destroy(ptr: *anyopaque) void {
        const self: *ListRelationBackend = @ptrCast(@alignCast(ptr));
        self.array.deinit(self.allocator);
    }

    fn insert(ptr: *anyopaque, iterator: TupleIterator, allocator: Allocator) InsertError!usize {
        const self: *ListRelationBackend = @ptrCast(@alignCast(ptr));
        var count: usize = 0;
        const buff = try allocator.alloc(u64, self.arity);
        const a = self.arity;
        loop: while (iterator.next(buff)) {
            var j: usize = 0;
            while (j * self.arity < self.array.items.len) : (j += 1) {
                if (std.mem.eql(u64, buff, self.array.items[j * a .. a + j * a])) continue :loop;
            }
            count += 1;
            self.array.appendSlice(self.allocator, buff) catch
                return error.DatabaseInsertError;
        }
        return count;
    }

    fn query(ptr: *anyopaque, pattern: []const ?u64, allocator: Allocator) AccessError!TupleIterator {
        const self: *ListRelationBackend = @ptrCast(@alignCast(ptr));
        const items = try allocator.create(SliceTupleIterator);
        errdefer allocator.destroy(items);
        items.* = .{
            .allocator = allocator,
            .arity = self.arity,
            .slice = self.array.items,
        };
        const filter = try allocator.create(QueryTupleIterator);
        filter.* = .{
            .allocator = allocator,
            .backend = items.iterator(),
            .pattern = pattern,
        };
        return filter.iterator();
    }
};

pub const Db = struct {
    const load_factor = std.hash_map.default_max_load_percentage;
    allocator: Allocator,
    relations: std.HashMapUnmanaged(Relation, RelationBackend, Relation.Equality, load_factor) = .{},
    pub fn create(allocator: Allocator) Db {
        return .{ .allocator = allocator };
    }
    pub fn setListRelationBackend(self: *Db, relation: Relation) Allocator.Error!void {
        if (!self.relations.contains(relation)) {
            try self.relations.ensureUnusedCapacity(self.allocator, 1);
            const backend = try ListRelationBackend.create(.{
                .allocator = self.allocator,
                .arity = relation.arity,
            });
            return self.relations.putAssumeCapacityNoClobber(relation, backend);
        }
    }
    pub fn query(
        self: *Db,
        relation: Relation,
        pattern: []const ?u64,
        allocator: Allocator,
    ) !TupleIterator {
        const ptr = self.relations.getPtr(relation).?;
        return ptr.query(pattern, allocator);
    }
    pub fn insert(
        self: *Db,
        relation: Relation,
        iterator: TupleIterator,
        allocator: Allocator,
    ) !usize {
        const ptr = self.relations.getPtr(relation).?;
        return ptr.insert(iterator, allocator);
    }
};
