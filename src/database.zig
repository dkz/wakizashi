const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const InsertError = error{DatabaseInsertError};
pub const AccessError = error{DatabaseAccessError};

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
    allocator: Allocator,
    slice: []const u64,
    index: usize = 0,
    arity: usize,

    pub fn create(opts: SliceTupleIterator) Allocator.Error!TupleIterator {
        const context = try opts.allocator.create(SliceTupleIterator);
        context.* = opts;
        return TupleIterator{ .vtable = VTable, .ptr = context };
    }

    const VTable = &TupleIterator.VTable{
        .destroy = destroy,
        .next = next,
    };

    fn destroy(ptr: *anyopaque) void {
        const self: *SliceTupleIterator = @ptrCast(@alignCast(ptr));
        self.allocator.destroy(self);
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
    allocator: Allocator,
    backend: TupleIterator,
    pattern: []const ?u64,

    pub fn create(opts: QueryTupleIterator) Allocator.Error!TupleIterator {
        const context = try opts.allocator.create(QueryTupleIterator);
        context.* = opts;
        return TupleIterator{ .vtable = VTable, .ptr = context };
    }

    const VTable = &TupleIterator.VTable{
        .destroy = destroy,
        .next = next,
    };

    fn destroy(ptr: *anyopaque) void {
        const self: *QueryTupleIterator = @ptrCast(@alignCast(ptr));
        self.backend.destroy();
        self.allocator.destroy(self);
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
        insert: *const fn (
            ctx: *anyopaque,
            iterator: TupleIterator,
            allocator: Allocator,
        ) InsertError!void,

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

    pub fn insert(self: *RelationBackend, iterator: TupleIterator, allocator: Allocator) InsertError!void {
        return self.vtable.insert(self.ptr, iterator, allocator);
    }

    pub fn query(self: *RelationBackend, pattern: []const ?u64, allocator: Allocator) AccessError!TupleIterator {
        return self.vtable.query(self.ptr, pattern, allocator);
    }

    pub fn destroy(self: *RelationBackend) void {
        self.vtable.destroy(self.ptr);
    }

    /// Inserts a unique tuple into this backend.
    /// Returns true if backend accepts a unique tuple, or false if it is already listed.
    pub fn insertSingleUnique(self: *RelationBackend, tuple: []const u64, allocator: Allocator) (AccessError || InsertError)!bool {

        // Throwaway buffer for checking whether target tuple exists.
        const throwaway = allocator.alloc(u64, tuple.len) catch return error.DatabaseInsertError;
        defer allocator.free(throwaway);
        // Copy of the target tuple as a query pattern.
        const pattern = allocator.alloc(?u64, tuple.len) catch return error.DatabaseInsertError;
        defer allocator.free(pattern);
        for (tuple, 0..) |x, j| pattern[j] = x;

        var it = try self.query(pattern, allocator);
        defer it.destroy();
        if (!it.next(throwaway)) {
            var insertion = SliceTupleIterator.create(.{
                .allocator = allocator,
                .arity = tuple.len,
                .slice = tuple,
            }) catch return error.DatabaseInsertError;
            defer insertion.destroy();
            try self.insert(insertion, allocator);
            return true;
        } else {
            return false;
        }
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

    fn insert(ptr: *anyopaque, iterator: TupleIterator, allocator: Allocator) InsertError!void {
        const self: *ListRelationBackend = @ptrCast(@alignCast(ptr));
        const buff = allocator.alloc(u64, self.arity) catch return error.DatabaseInsertError;
        while (iterator.next(buff)) {
            self.array.appendSlice(self.allocator, buff) catch
                return error.DatabaseInsertError;
        }
    }

    fn query(ptr: *anyopaque, pattern: []const ?u64, allocator: Allocator) AccessError!TupleIterator {
        const self: *ListRelationBackend = @ptrCast(@alignCast(ptr));
        const iterator = SliceTupleIterator.create(.{
            .allocator = allocator,
            .arity = self.arity,
            .slice = self.array.items,
        }) catch return error.DatabaseAccessError;
        const filter = QueryTupleIterator.create(.{
            .allocator = allocator,
            .backend = iterator,
            .pattern = pattern,
        }) catch return error.DatabaseAccessError;
        return filter;
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
    ) !void {
        const ptr = self.relations.getPtr(relation).?;
        return ptr.insert(iterator, allocator);
    }
    pub fn insertSingleUnique(
        self: *Db,
        relation: Relation,
        tuple: []const u64,
        allocator: Allocator,
    ) !bool {
        const ptr = self.relations.getPtr(relation).?;
        return ptr.insertSingleUnique(tuple, allocator);
    }
};
