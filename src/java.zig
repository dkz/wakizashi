/// Parsing of a java heap file. Assumes 64-bit identifiers.
/// Check HPROF agent documentation for a complete specification:
/// https://hg.openjdk.org/jdk6/jdk6/jdk/raw-file/tip/src/share/demo/jvmti/hprof/manual.html
const std = @import("std");

const Allocator = std.mem.Allocator;
const LimitedReader = std.io.LimitedReader;

const assert = std.debug.assert;
const panic = std.debug.panic;

/// Virtual table for data dispatching methods that matter.
/// Method's argument lifetimes are limited to method call.
pub fn DispatchingMethods(comptime Context: type) type {
    return struct {
        onConstantString: *const fn (context: *Context, obj: ConstantString) void,
    };
}

/// Couples dispatching context with virtual table for convenience.
pub fn Dispatcher(comptime Context: type) type {
    return struct {
        context: *Context,
        handlers: DispatchingMethods(Context),
        const Self = @This();
        fn onConstantString(self: *const Self, obj: ConstantString) void {
            return self.handlers.onConstantString(self.context, obj);
        }
    };
}

/// Reader should be a GenericReader and dispatcher should be a Dispatcher struct.
pub fn traverse(reader: anytype, dispatcher: anytype, allocator: Allocator) void {
    var buffer: [64]u8 = undefined;
    const version = reader.readUntilDelimiter(&buffer, 0) catch |e|
        panic("Heap parser's unexpected failure: {}\n", .{e});
    if (!std.mem.eql(u8, version, "JAVA PROFILE 1.0.2")) {
        panic("Unrecognized heap format and version '{s}'.\n", .{version});
    }
    const id_size = reader.readInt(u32, .big) catch |e|
        panic("Heap parser's unexpected failure: {}\n", .{e});
    if (id_size != 8) panic("Invalid id size: {}\n", .{id_size});
    // Ignores the heap timestamp:
    _ = reader.readInt(u64, .big) catch 0;

    while (SegmentHeader.parseUnsafe(reader)) |segment| {
        switch (segment.tag) {
            .constant_string => {
                const string = ConstantString.parseUnsafe(reader, segment, allocator);
                defer string.destroy(allocator);
                dispatcher.onConstantString(string);
            },
            else => {
                reader.skipBytes(segment.size, .{}) catch |e|
                    panic("Heap parser's unexpected failure: {}\n", .{e});
            },
        }
    }
}

/// Sample dispatcher context implementation,
/// debug-prints every object encountered by the parser.
pub const DebugDispatcher = struct {
    pub fn dispatcher(self: *DebugDispatcher) Dispatcher(DebugDispatcher) {
        return .{ .context = self, .handlers = handlers };
    }
    fn onConstantString(_: *@This(), obj: ConstantString) void {
        std.debug.print("ConstantString({}, '{s}')\n", .{ obj.sid, obj.string });
    }
    const handlers = DispatchingMethods(@This()){
        .onConstantString = onConstantString,
    };
};

/// Heap dump contains tagged segments.
/// Important ones are strings, loaded classes, stack traces, and the heap itself.
/// Parser will skip the rest.
const SegmentTag = enum(u8) {
    constant_string = 0x01,
    load_class = 0x02,
    unload_class = 0x03,
    stack_frame = 0x04,
    stack_trace = 0x05,
    alloc_sites = 0x06,
    heap_summary = 0x07,
    start_thread = 0x0A,
    end_thread = 0x0B,
    head_dump = 0x0C,
    head_dump_segment = 0x1C,
    head_dump_end = 0x2C,
    cpu_samples = 0x0D,
    control_settings = 0x0E,
};

const SegmentHeader = struct {
    tag: SegmentTag,
    time: u32,
    size: u32,
    /// Retrieves the next segment header if available.
    /// Pass the header to skip() in order to skip the segment.
    /// Otherwise call a corresponding parser function on the reader.
    fn parse(reader: anytype) !?SegmentHeader {
        const tag = reader.readEnum(SegmentTag, .big) catch |e| switch (e) {
            error.EndOfStream => return null,
            else => return e,
        };
        const time = try reader.readInt(u32, .big);
        const size = try reader.readInt(u32, .big);
        return SegmentHeader{ .tag = tag, .time = time, .size = size };
    }
    /// Panics on unexpected errors (any errors).
    fn parseUnsafe(reader: anytype) ?SegmentHeader {
        return parse(reader) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};

/// A string from the JVM's string pool.
/// `sid` is short for string id, referred by field and class names.
pub const ConstantString = struct {
    sid: u64,
    string: []const u8,
    fn destroy(self: ConstantString, allocator: Allocator) void {
        allocator.free(self.string);
    }
    fn parse(
        reader: anytype,
        header: SegmentHeader,
        allocator: Allocator,
    ) !ConstantString {
        const sid = try reader.readInt(u64, .big);
        const string = try allocator.alloc(u8, header.size -| 8);
        _ = try reader.read(string);
        return ConstantString{ .sid = sid, .string = string };
    }
    fn parseUnsafe(
        reader: anytype,
        header: SegmentHeader,
        allocator: Allocator,
    ) ConstantString {
        return parse(reader, header, allocator) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};
