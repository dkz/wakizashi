const std = @import("std");

const Allocator = std.mem.Allocator;
const HashMapUnmanaged = std.HashMapUnmanaged;
const SinglyLinkedList = std.SinglyLinkedList;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

pub fn DirectedGraph(
    comptime V: type,
    comptime E: type,
    comptime Context: type,
) type {
    return struct {
        pub const Vertices = SinglyLinkedList(V);

        const Self = @This();
        const Edges = SinglyLinkedList(struct { V, E });
        const load_percentage = std.hash_map.default_max_load_percentage;
        graph: HashMapUnmanaged(V, Edges, Context, load_percentage) = .empty,
        pub fn deinit(self: *Self, allocator: Allocator) void {
            var it = self.graph.iterator();
            while (it.next()) |e| {
                while (e.value_ptr.popFirst()) |node| allocator.destroy(node);
            }
            self.graph.deinit(allocator);
        }
        pub fn add(self: *Self, from: V, to: V, edge: E, allocator: Allocator) !void {
            const get = try self.graph.getOrPut(allocator, from);
            if (!get.found_existing) {
                get.value_ptr.* = Edges{};
            }
            const node = try allocator.create(Edges.Node);
            node.* = Edges.Node{ .data = .{ to, edge } };
            get.value_ptr.prepend(node);
        }
        const VisitedSet = HashMapUnmanaged(V, void, Context, load_percentage);

        /// Kosaraju's algorithm for locating strognly connected components.
        pub fn components(
            self: *Self,
            into: *ArrayListUnmanaged(Vertices),
            allocator: Allocator,
        ) !void {
            var visited: VisitedSet = .empty;
            defer visited.deinit(allocator);
            var ordered = Vertices{};
            defer while (ordered.popFirst()) |node| allocator.destroy(node);
            var it = self.graph.iterator();
            while (it.next()) |v| try self.dfs(
                v.key_ptr.*,
                &visited,
                &ordered,
                allocator,
            );
            var t = try self.transpose(allocator);
            defer t.deinit(allocator);
            visited.clearRetainingCapacity();
            while (ordered.popFirst()) |node| {
                defer allocator.destroy(node);
                if (!visited.contains(node.data)) {
                    const component = try into.addOne(allocator);
                    component.* = .{};
                    try t.assign(node.data, component, &visited, allocator);
                }
            }
        }
        fn dfs(
            self: *Self,
            vertex: V,
            visited: *VisitedSet,
            ordered: *Vertices,
            allocator: Allocator,
        ) !void {
            if (!visited.contains(vertex)) {
                try visited.put(allocator, vertex, {});
                if (self.graph.getPtr(vertex)) |edges| {
                    var ptr = edges.first;
                    while (ptr) |node| {
                        const to, _ = node.data;
                        try self.dfs(to, visited, ordered, allocator);
                        ptr = node.next;
                    }
                }
                const node = try allocator.create(Vertices.Node);
                node.* = .{ .data = vertex };
                ordered.prepend(node);
            }
        }
        fn transpose(self: *Self, allocator: Allocator) !Self {
            var t = Self{};
            var it = self.graph.iterator();
            while (it.next()) |e| {
                const from = e.key_ptr.*;
                var ptr = e.value_ptr.first;
                while (ptr) |node| {
                    const to, const edge = node.data;
                    try t.add(to, from, edge, allocator);
                    ptr = node.next;
                }
            }
            return t;
        }
        fn assign(
            self: *Self,
            vertex: V,
            component: *Vertices,
            visited: *VisitedSet,
            allocator: Allocator,
        ) !void {
            if (!visited.contains(vertex)) {
                try visited.put(allocator, vertex, {});
                const assigned = try allocator.create(Vertices.Node);
                assigned.* = .{ .data = vertex };
                component.prepend(assigned);
                if (self.graph.getPtr(vertex)) |edges| {
                    var ptr = edges.first;
                    while (ptr) |node| {
                        const to, _ = node.data;
                        try self.assign(to, component, visited, allocator);
                        ptr = node.next;
                    }
                }
            }
        }
    };
}
