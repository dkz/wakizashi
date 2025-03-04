const std = @import("std");

const Allocator = std.mem.Allocator;
const ThreadSafeAllocator = std.heap.ThreadSafeAllocator;

const Mutex = std.Thread.Mutex;
const Condition = std.Thread.Condition;
const ThreadPool = std.Thread.Pool;

/// Go-style channel using a fixed circular buffer under a mutex.
fn Channel(comptime T: type, comptime size: u8) type {
    return struct {
        const Self = @This();
        mutex: Mutex = .{},
        buffer: [size]?T = [_]?T{null} ** size,
        reader: usize = 0,
        writer: usize = 0,
        recv_cond: Condition = .{},
        send_cond: Condition = .{},
        fn send(self: *Self, message: T) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            while (true) {
                if (self.buffer[self.writer]) |_| {
                    self.recv_cond.wait(&self.mutex);
                } else {
                    self.buffer[self.writer] = message;
                    self.writer = (self.writer + 1) % self.buffer.len;
                    self.send_cond.signal();
                    return;
                }
            }
        }
        fn recv(self: *Self) T {
            self.mutex.lock();
            defer self.mutex.unlock();
            while (true) {
                if (self.buffer[self.reader]) |data| {
                    self.buffer[self.reader] = null;
                    self.reader = (self.reader + 1) % self.buffer.len;
                    self.recv_cond.signal();
                    return data;
                } else {
                    self.send_cond.wait(&self.mutex);
                }
            }
        }
    };
}
