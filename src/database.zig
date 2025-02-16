const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const DomainValue = union(enum) {
    seq: []const u8,
    fn clone(self: DomainValue, allocator: Allocator) Allocator.Error!DomainValue {
        switch (self) {
            .seq => |seq| {
                const target = try allocator.alloc(u8, seq.len);
                @memcpy(target, seq);
                return DomainValue{ .seq = target };
            },
        }
    }
};
const DomainValueHashEql = struct {
    pub fn eql(_: @This(), this: DomainValue, that: DomainValue) bool {
        return std.mem.eql(u8, this.seq, that.seq);
    }
    pub fn hash(_: @This(), this: DomainValue) u64 {
        switch (this) {
            .seq => |seq| return std.hash.Wyhash.hash(0, seq),
        }
    }
};

/// Stealing this cool technique from bdddb:
/// instead of using multiple copies of the data and wasting CPU cycles on equality checks
/// store every encountered value in a unified domain index and assign a word id to it.
/// Highly unlikely that a dataset for any program exceeds word size (?).
pub const Domain = struct {
    const load_factor = std.hash_map.default_max_load_percentage;
    array: std.ArrayListUnmanaged(DomainValue) = .{},
    index: std.HashMapUnmanaged(DomainValue, u64, DomainValueHashEql, load_factor) = .{},
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
    fn clone(self: Relation, allocator: Allocator) Allocator.Error!Relation {
        const name_copy = try allocator.alloc(u8, self.name.len);
        @memcpy(name_copy, self.name);
        return .{ .arity = self.arity, .name = name_copy };
    }
};
const RelationHashEql = struct {
    pub fn eql(_: @This(), this: Relation, that: Relation) bool {
        return this.arity == that.arity and std.mem.eql(u8, this.name, that.name);
    }
    pub fn hash(_: @This(), this: Relation) u64 {
        return std.hash.Wyhash.hash(this.arity, this.name);
    }
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
    pub fn next(self: *TupleIterator, into: []u64) bool {
        return self.vtable.next(self.ptr, into);
    }
    pub fn destroy(self: *TupleIterator) void {
        self.vtable.destroy(self.ptr);
    }
};

/// Trivial relation storage backed by an ArrayList.
const ArrayListBackend = struct {
    arity: usize,
    array: std.ArrayListUnmanaged(u64) = .{},

    fn insert(
        self: *ArrayListBackend,
        database_allocator: Allocator,
        tuple: []const u64,
    ) Allocator.Error!void {
        std.debug.assert(self.arity == tuple.len);
        return self.array.appendSlice(database_allocator, tuple);
    }

    fn query(
        self: *ArrayListBackend,
        allocator: Allocator,
        tuple: []const ?u64,
    ) Allocator.Error!TupleIterator {
        return IterImpl.create(allocator, .{ .owner = self, .query = tuple, .allocator = allocator });
    }

    const IterImpl = struct {
        index: u32 = 0,
        owner: *const ArrayListBackend,
        query: []const ?u64,
        allocator: Allocator,
        const VTable = &TupleIterator.VTable{
            .next = next,
            .destroy = destroy,
        };
        fn create(allocator: Allocator, state: IterImpl) Allocator.Error!TupleIterator {
            const self = try allocator.create(IterImpl);
            self.* = state;
            return TupleIterator{ .vtable = VTable, .ptr = self };
        }
        fn next(ptr: *anyopaque, into: []u64) bool {
            const self: *IterImpl = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.owner.arity == into.len);
            loop: while (self.index * self.owner.arity < self.owner.array.items.len) : (self.index += 1) {
                const i = self.index;
                const a = self.owner.arity;
                const slice = self.owner.array.items[i * a .. i * a + a];
                for (0..a) |j| {
                    if (self.query[j]) |x| {
                        if (x != slice[j]) continue :loop;
                    }
                }
                self.index += 1;
                @memcpy(into, slice);
                return true;
            }
            return false;
        }
        fn destroy(ptr: *anyopaque) void {
            const self: *IterImpl = @ptrCast(@alignCast(ptr));
            self.allocator.destroy(self);
        }
    };
};

const RelationBackend = union(enum) {
    array_list: ArrayListBackend,
    fn insert(
        self: *RelationBackend,
        database_allocator: Allocator,
        tuple: []const u64,
    ) Allocator.Error!void {
        return switch (self.*) {
            .array_list => |*backend| backend.insert(database_allocator, tuple),
        };
    }
    fn query(
        self: *RelationBackend,
        allocator: Allocator,
        tuple: []const ?u64,
    ) Allocator.Error!TupleIterator {
        return switch (self.*) {
            .array_list => |*backend| backend.query(allocator, tuple),
        };
    }
};

pub const Db = struct {
    const load_factor = std.hash_map.default_max_load_percentage;
    relations: std.HashMapUnmanaged(Relation, RelationBackend, RelationHashEql, load_factor) = .{},
    pub fn registerArrayBackend(self: *Db, allocator: Allocator, relation: Relation) Allocator.Error!void {
        if (!self.relations.contains(relation)) {
            return self.relations.putNoClobber(
                allocator,
                relation,
                RelationBackend{ .array_list = .{ .arity = relation.arity } },
            );
        }
    }
    pub fn query(
        self: *Db,
        allocator: Allocator,
        relation: Relation,
        tuple: []const ?u64,
    ) Allocator.Error!TupleIterator {
        const ptr = self.relations.getPtr(relation).?;
        return ptr.query(allocator, tuple);
    }
    pub fn insert(
        self: *Db,
        allocator: Allocator,
        relation: Relation,
        tuple: []const u64,
    ) Allocator.Error!void {
        const ptr = self.relations.getPtr(relation).?;
        return ptr.insert(allocator, tuple);
    }
};
