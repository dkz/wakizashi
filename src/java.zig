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
        onConstantString: *const fn (context: Context, obj: ConstantString) void,
        onLoadClass: *const fn (context: Context, obj: LoadClass) void,
        onStackFrame: *const fn (context: Context, obj: StackFrame) void,
        onStackTrace: *const fn (context: Context, obj: StackTrace) void,
        onClassDump: *const fn (context: Context, obj: ClassDump) void,
        onInstanceDump: *const fn (context: Context, obj: InstanceDump) void,
        onObjectArrayDump: *const fn (context: Context, obj: ObjectArrayDump) void,
        onPrimitiveArrayDump: *const fn (context: Context, obj: PrimitiveArrayDump) void,
    };
}

/// Couples dispatching context with virtual table for convenience.
pub fn Dispatcher(comptime Context: type) type {
    return struct {
        context: Context,
        handlers: DispatchingMethods(Context),
        const Self = @This();
        fn onConstantString(self: Self, obj: ConstantString) void {
            return self.handlers.onConstantString(self.context, obj);
        }
        fn onLoadClass(self: Self, obj: LoadClass) void {
            return self.handlers.onLoadClass(self.context, obj);
        }
        fn onStackFrame(self: Self, obj: StackFrame) void {
            return self.handlers.onStackFrame(self.context, obj);
        }
        fn onStackTrace(self: Self, obj: StackTrace) void {
            return self.handlers.onStackTrace(self.context, obj);
        }
        fn onClassDump(self: Self, obj: ClassDump) void {
            return self.handlers.onClassDump(self.context, obj);
        }
        fn onInstanceDump(self: Self, obj: InstanceDump) void {
            return self.handlers.onInstanceDump(self.context, obj);
        }
        fn onObjectArrayDump(self: Self, obj: ObjectArrayDump) void {
            return self.handlers.onObjectArrayDump(self.context, obj);
        }
        fn onPrimitiveArrayDump(self: Self, obj: PrimitiveArrayDump) void {
            return self.handlers.onPrimitiveArrayDump(self.context, obj);
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
            .load_class => dispatcher.onLoadClass(LoadClass.parseUnsafe(reader)),
            .stack_frame => dispatcher.onStackFrame(StackFrame.parseUnsafe(reader)),
            .stack_trace => {
                const trace = StackTrace.parseUnsafe(reader, allocator);
                defer trace.destroy(allocator);
                dispatcher.onStackTrace(trace);
            },
            .heap_dump, .heap_dump_segment => {
                var stream = std.io.limitedReader(reader, segment.size);
                const heap_reader = stream.reader();
                while (RecordTag.parseUnsafe(heap_reader)) |t| switch (t) {
                    .class_dump => {
                        const dump = ClassDump.parseUnsafe(heap_reader, allocator);
                        defer dump.destroy(allocator);
                        dispatcher.onClassDump(dump);
                    },
                    .instance_dump => {
                        const instance = InstanceDump.parseUnsafe(heap_reader, allocator);
                        defer instance.destroy(allocator);
                        dispatcher.onInstanceDump(instance);
                    },
                    .object_array_dump => {
                        const array = ObjectArrayDump.parseUnsafe(heap_reader, allocator);
                        defer array.destroy(allocator);
                        dispatcher.onObjectArrayDump(array);
                    },
                    .primitive_array_dump => {
                        const array = PrimitiveArrayDump.parseUnsafe(heap_reader, allocator);
                        defer array.destroy(allocator);
                        dispatcher.onPrimitiveArrayDump(array);
                    },
                    else => {
                        std.debug.print("{any}\n", .{t});
                        return;
                    },
                };
            },
            else => {
                reader.skipBytes(segment.size, .{}) catch |e|
                    panic("Heap parser's unexpected failure: {}\n", .{e});
            },
        }
    }
}

/// Sample dispatcher context implementation.
pub const NoopDispatcher = struct {
    pub fn dispatcher() Dispatcher(void) {
        return .{ .context = {}, .handlers = handlers };
    }
    fn onConstantString(_: void, _: ConstantString) void {}
    fn onLoadClass(_: void, _: LoadClass) void {}
    fn onStackFrame(_: void, _: StackFrame) void {}
    fn onStackTrace(_: void, _: StackTrace) void {}
    fn onClassDump(_: void, _: ClassDump) void {}
    fn onInstanceDump(_: void, _: InstanceDump) void {}
    fn onObjectArrayDump(_: void, _: ObjectArrayDump) void {}
    fn onPrimitiveArrayDump(_: void, _: PrimitiveArrayDump) void {}
    const handlers = DispatchingMethods(void){
        .onConstantString = onConstantString,
        .onLoadClass = onLoadClass,
        .onStackFrame = onStackFrame,
        .onStackTrace = onStackTrace,
        .onClassDump = onClassDump,
        .onInstanceDump = onInstanceDump,
        .onObjectArrayDump = onObjectArrayDump,
        .onPrimitiveArrayDump = onPrimitiveArrayDump,
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
    heap_dump = 0x0C,
    heap_dump_segment = 0x1C,
    heap_dump_end = 0x2C,
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

const RecordTag = enum(u8) {
    root_jni_global = 0x01,
    root_jni_local = 0x02,
    root_java_frame = 0x03,
    root_native_stack = 0x04,
    root_sticky_class = 0x05,
    root_thread_block = 0x06,
    root_monitor_used = 0x07,
    root_thread_object = 0x08,
    class_dump = 0x20,
    instance_dump = 0x21,
    object_array_dump = 0x22,
    primitive_array_dump = 0x23,
    fn parse(reader: anytype) !?RecordTag {
        return reader.readEnum(RecordTag, .big) catch |e| switch (e) {
            error.EndOfStream => return null,
            else => return e,
        };
    }
    fn parseUnsafe(reader: anytype) ?RecordTag {
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

const LoadClass = struct {
    id: u64,
    name_sid: u64,
    fn parse(reader: anytype) !LoadClass {
        const class_serial = try reader.readInt(u32, .big);
        const id = try reader.readInt(u64, .big);
        const trace_serial = try reader.readInt(u32, .big);
        const name_sid = try reader.readInt(u64, .big);
        _ = class_serial;
        _ = trace_serial;
        return LoadClass{ .id = id, .name_sid = name_sid };
    }
    fn parseUnsafe(reader: anytype) LoadClass {
        return parse(reader) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};

const StackFrame = struct {
    frame_id: u64,
    method_sid: u64,
    signature_sid: u64, // Points to a method signature string.
    source_sid: u64, // Points to a source file name string.
    line_number: u32,
    fn parse(reader: anytype) !StackFrame {
        const frame_id = try reader.readInt(u64, .big);
        const method_sid = try reader.readInt(u64, .big);
        const signature_sid = try reader.readInt(u64, .big);
        const source_sid = try reader.readInt(u64, .big);
        const class_serial = try reader.readInt(u32, .big);
        const line_number = try reader.readInt(u32, .big);
        _ = class_serial;
        return StackFrame{
            .frame_id = frame_id,
            .method_sid = method_sid,
            .signature_sid = signature_sid,
            .source_sid = source_sid,
            .line_number = line_number,
        };
    }
    fn parseUnsafe(reader: anytype) StackFrame {
        return parse(reader) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};

const StackTrace = struct {
    thread: u32,
    frames: []const u64,
    fn destroy(self: StackTrace, allocator: Allocator) void {
        allocator.free(self.frames);
    }
    fn parse(reader: anytype, allocator: Allocator) !StackTrace {
        const stack_serial = try reader.readInt(u32, .big);
        const thread_serial = try reader.readInt(u32, .big);
        const frames_count = try reader.readInt(u32, .big);
        const frames = try allocator.alloc(u64, frames_count);
        for (frames) |*frame| frame.* = try reader.readInt(u64, .big);
        _ = stack_serial;
        return .{
            .thread = thread_serial,
            .frames = frames,
        };
    }
    fn parseUnsafe(reader: anytype, allocator: Allocator) StackTrace {
        return parse(reader, allocator) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};

const PrimitiveType = enum(u8) {
    reference = 2,
    boolean = 4,
    char,
    float,
    double,
    byte,
    short,
    int,
    long,
};

const PrimitiveValue = union(PrimitiveType) {
    reference: u64,
    boolean: [1]u8,
    char: [2]u8,
    float: [4]u8,
    double: [8]u8,
    byte: [1]u8,
    short: [2]u8,
    int: [4]u8,
    long: [8]u8,
    fn parseBytes(comptime size: usize, reader: anytype) ![size]u8 {
        var bytes: [size]u8 = undefined;
        const count = try reader.read(&bytes);
        if (count < size)
            panic("Heap parser's unexpected failure: stream abruptly ended\n", .{});
        return bytes;
    }
    fn parse(t: PrimitiveType, reader: anytype) !PrimitiveValue {
        return switch (t) {
            .reference => .{ .reference = try reader.readInt(u64, .big) },
            .boolean => .{ .boolean = try parseBytes(1, reader) },
            .char => .{ .char = try parseBytes(2, reader) },
            .float => .{ .float = try parseBytes(4, reader) },
            .double => .{ .double = try parseBytes(8, reader) },
            .byte => .{ .byte = try parseBytes(1, reader) },
            .short => .{ .short = try parseBytes(2, reader) },
            .int => .{ .int = try parseBytes(4, reader) },
            .long => .{ .long = try parseBytes(8, reader) },
        };
    }
    fn parseUnsafe(t: PrimitiveType, reader: anytype) PrimitiveValue {
        return parse(t, reader) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};

const ClassDump = struct {
    const Static = struct { name_sid: u64, value: PrimitiveValue };
    const Field = struct { name_sid: u64, type: PrimitiveType };
    id: u64,
    super: u64,
    instance_size: u32,
    statics: []const Static,
    fields: []const Field,
    fn destroy(self: ClassDump, allocator: Allocator) void {
        allocator.free(self.statics);
        allocator.free(self.fields);
    }
    fn parse(reader: anytype, allocator: Allocator) !ClassDump {
        const id = try reader.readInt(u64, .big);
        const stack_serial = try reader.readInt(u32, .big);
        const super = try reader.readInt(u64, .big);
        const loader = try reader.readInt(u64, .big);
        const signer = try reader.readInt(u64, .big);
        const domain = try reader.readInt(u64, .big);
        // Reserved fields, never used.
        _ = try reader.readInt(u64, .big);
        _ = try reader.readInt(u64, .big);
        const instance_size = try reader.readInt(u32, .big);
        const constants_count = try reader.readInt(u16, .big);
        for (0..constants_count) |_| {
            _ = try reader.readInt(u32, .big);
            const t = try reader.readEnum(PrimitiveType, .big);
            _ = try PrimitiveValue.parse(t, reader);
        }
        const statics_count = try reader.readInt(u16, .big);
        const statics = try allocator.alloc(ClassDump.Static, statics_count);
        errdefer allocator.free(statics);
        for (statics) |*static| {
            static.name_sid = try reader.readInt(u64, .big);
            const t = try reader.readEnum(PrimitiveType, .big);
            static.value = try PrimitiveValue.parse(t, reader);
        }
        const fields_count = try reader.readInt(u16, .big);
        const fields = try allocator.alloc(ClassDump.Field, fields_count);
        errdefer allocator.free(fields);
        for (fields) |*field| {
            field.name_sid = try reader.readInt(u64, .big);
            field.type = try reader.readEnum(PrimitiveType, .big);
        }
        _ = loader;
        _ = signer;
        _ = domain;
        _ = stack_serial;
        return ClassDump{
            .id = id,
            .super = super,
            .instance_size = instance_size,
            .statics = statics,
            .fields = fields,
        };
    }
    fn parseUnsafe(reader: anytype, allocator: Allocator) ClassDump {
        return parse(reader, allocator) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};

const InstanceDump = struct {
    id: u64,
    class: u64,
    bytes: []const u8,
    fn destroy(self: InstanceDump, allocator: Allocator) void {
        allocator.free(self.bytes);
    }
    fn parse(reader: anytype, allocator: Allocator) !InstanceDump {
        const id = try reader.readInt(u64, .big);
        const stack_serial = try reader.readInt(u32, .big);
        const class = try reader.readInt(u64, .big);
        const bytes_count = try reader.readInt(u32, .big);
        const bytes = try allocator.alloc(u8, bytes_count);
        const count = try reader.read(bytes);
        if (count < bytes_count)
            panic("Heap parser's unexpected failure: stream abruptly ended\n", .{});
        _ = stack_serial;
        return InstanceDump{
            .id = id,
            .class = class,
            .bytes = bytes,
        };
    }
    fn parseUnsafe(reader: anytype, allocator: Allocator) InstanceDump {
        return parse(reader, allocator) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};

const ObjectArrayDump = struct {
    id: u64,
    class: u64,
    elements: []const u64,
    fn destroy(self: ObjectArrayDump, allocator: Allocator) void {
        allocator.free(self.elements);
    }
    fn parse(reader: anytype, allocator: Allocator) !ObjectArrayDump {
        const id = try reader.readInt(u64, .big);
        const stack_serial = try reader.readInt(u32, .big);
        const elements_count = try reader.readInt(u32, .big);
        const class = try reader.readInt(u64, .big);
        const elements = try allocator.alloc(u64, elements_count);
        for (elements) |*element| element.* = try reader.readInt(u64, .big);
        _ = stack_serial;
        return ObjectArrayDump{
            .id = id,
            .class = class,
            .elements = elements,
        };
    }
    fn parseUnsafe(reader: anytype, allocator: Allocator) ObjectArrayDump {
        return parse(reader, allocator) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};

const PrimitiveArrayDump = struct {
    id: u64,
    element: PrimitiveType,
    bytes: []const u8,
    fn destroy(self: PrimitiveArrayDump, allocator: Allocator) void {
        allocator.free(self.bytes);
    }
    fn parse(reader: anytype, allocator: Allocator) !PrimitiveArrayDump {
        const id = try reader.readInt(u64, .big);
        const stack_serial = try reader.readInt(u32, .big);
        const elements_count = try reader.readInt(u32, .big);
        const t = try reader.readEnum(PrimitiveType, .big);
        const s = elements_count * @as(usize, switch (t) {
            .reference => 8,
            .boolean => 1,
            .char => 2,
            .float => 4,
            .double => 8,
            .byte => 1,
            .short => 2,
            .int => 4,
            .long => 8,
        });
        const bytes = try allocator.alloc(u8, s);
        const count = try reader.read(bytes);
        if (count < s)
            panic("Heap parser's unexpected failure: stream abruptly ended\n", .{});
        _ = stack_serial;
        return PrimitiveArrayDump{
            .id = id,
            .element = t,
            .bytes = bytes,
        };
    }
    fn parseUnsafe(reader: anytype, allocator: Allocator) PrimitiveArrayDump {
        return parse(reader, allocator) catch |e|
            panic("Heap parser's unexpected failure: {}\n", .{e});
    }
};
