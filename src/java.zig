/// Parsing of a java heap file. Assumes 64-bit identifiers.
/// Check HPROF agent documentation for a complete specification:
/// https://hg.openjdk.org/jdk6/jdk6/jdk/raw-file/tip/src/share/demo/jvmti/hprof/manual.html
const std = @import("std");

const Allocator = std.mem.Allocator;
const LimitedReader = std.io.LimitedReader;

const assert = std.debug.assert;
const panic = std.debug.panic;

/// Virtual table for data dispatching methods that matter.
/// Method's argument lifetimes are limited to method call:
/// it's relevant for structs that require a buffer, like stack traces or strings.
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
        onRootThread: *const fn (context: Context, obj: RootThread) void,
        onRootLocal: *const fn (context: Context, obj: RootLocal) void,
        onRootReference: *const fn (context: Context, reference: u64) void,
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
        fn onRootThread(self: Self, obj: RootThread) void {
            return self.handlers.onRootThread(self.context, obj);
        }
        fn onRootLocal(self: Self, obj: RootLocal) void {
            return self.handlers.onRootLocal(self.context, obj);
        }
        fn onRootReference(self: Self, reference: u64) void {
            return self.handlers.onRootReference(self.context, reference);
        }
    };
}

/// Remembers byte position in an underlying reader.
/// This hack is significatly faster for consecutive reads compared to using
/// seekable stream and getPos() calls.
fn PositionAwareReader(comptime ReaderType: type) type {
    return struct {
        backend: ReaderType,
        position: usize = 0,
        pub const Error = ReaderType.Error;
        pub const Reader = std.io.Reader(*Self, Error, read);
        pub fn read(self: *Self, dest: []u8) Error!usize {
            const bytes_count = try self.backend.read(dest);
            self.position += bytes_count;
            return bytes_count;
        }
        pub fn reader(self: *Self) Reader {
            return .{ .context = self };
        }
        const Self = @This();
    };
}

/// Source should be a GenericReader and dispatcher should be a Dispatcher struct.
pub fn traverse(source: anytype, dispatcher: anytype, allocator: Allocator) void {
    // Panics on unexpected errors: any errors.
    errdefer |e| panic("Heap parser's unexpected failure: {}\n", .{e});

    var aware = PositionAwareReader(@TypeOf(source)){ .backend = source };
    const reader = aware.reader();

    var buffer: [64]u8 = undefined;
    const version = try reader.readUntilDelimiter(&buffer, 0);
    if (!std.mem.eql(u8, version, "JAVA PROFILE 1.0.2")) {
        panic("Unrecognized heap format and version '{s}'.\n", .{version});
    }
    const id_size = try reader.readInt(u32, .big);
    if (id_size != 8) panic("Invalid id size: {}\n", .{id_size});
    // Ignores the heap timestamp:
    _ = reader.readInt(u64, .big) catch 0;

    while (try SegmentHeader.parse(reader)) |segment| {
        switch (segment.tag) {
            .constant_string => {
                const string = try ConstantString.parse(reader, segment, allocator);
                defer string.destroy(allocator);
                dispatcher.onConstantString(string);
            },
            .load_class => dispatcher.onLoadClass(try LoadClass.parse(reader)),
            .stack_frame => dispatcher.onStackFrame(try StackFrame.parse(reader)),
            .stack_trace => {
                const trace = try StackTrace.parse(reader, allocator);
                defer trace.destroy(allocator);
                dispatcher.onStackTrace(trace);
            },
            .heap_dump, .heap_dump_segment => {
                var stream = std.io.limitedReader(reader, segment.size);
                const heap_reader = stream.reader();
                while (try RecordTag.parse(heap_reader)) |t| switch (t) {
                    .class_dump => {
                        const dump = try ClassDump.parse(heap_reader, allocator);
                        defer dump.destroy(allocator);
                        dispatcher.onClassDump(dump);
                    },
                    .instance_dump => {
                        const instance = try InstanceDump.parse(heap_reader, aware);
                        dispatcher.onInstanceDump(instance);
                    },
                    .object_array_dump => {
                        const array = try ObjectArrayDump.parse(heap_reader, aware);
                        dispatcher.onObjectArrayDump(array);
                    },
                    .primitive_array_dump => {
                        const array = try PrimitiveArrayDump.parse(heap_reader, aware);
                        dispatcher.onPrimitiveArrayDump(array);
                    },
                    .root_thread_object => dispatcher.onRootThread(try RootThread.parse(heap_reader)),
                    .root_java_frame => dispatcher.onRootLocal(try RootLocal.parse(heap_reader)),
                    .root_jni_local => dispatcher.onRootLocal(try RootLocal.parse(heap_reader)),
                    .root_jni_global => {
                        const id = try heap_reader.readInt(u64, .big);
                        const jni = try heap_reader.readInt(u64, .big);
                        _ = jni;
                        dispatcher.onRootReference(id);
                    },
                    .root_native_stack, .root_thread_block => {
                        const id = try heap_reader.readInt(u64, .big);
                        const thread_serial = try heap_reader.readInt(u32, .big);
                        dispatcher.onRootLocal(.{ .id = id, .thread = thread_serial, .frame = null });
                    },
                    .root_sticky_class, .root_monitor_used => {
                        dispatcher.onRootReference(try heap_reader.readInt(u64, .big));
                    },
                };
            },
            else => try reader.skipBytes(segment.size, .{}),
        }
    }
}

/// Heap dump contains tagged segments.
/// Important ones are strings, loaded classes, stack traces, and the heap itself.
/// Parser skips the rest.
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
        try reader.readNoEof(string);
        return ConstantString{ .sid = sid, .string = string };
    }
};

/// Indicates a class that has been loaded by a class loader,
/// assigns a name from the string pool to the class object.
pub const LoadClass = struct {
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
};

pub const StackFrame = struct {
    frame_id: u64,
    method_sid: u64,
    signature_sid: u64,
    source_sid: u64,
    line_number: ?u32,
    fn parse(reader: anytype) !StackFrame {
        const frame_id = try reader.readInt(u64, .big);
        const method_sid = try reader.readInt(u64, .big);
        const signature_sid = try reader.readInt(u64, .big);
        const source_sid = try reader.readInt(u64, .big);
        const class_serial = try reader.readInt(u32, .big);
        const line_number = try reader.readInt(i32, .big);
        _ = class_serial;
        return StackFrame{
            .frame_id = frame_id,
            .method_sid = method_sid,
            .signature_sid = signature_sid,
            .source_sid = source_sid,
            .line_number = if (line_number < 0) null else @abs(line_number),
        };
    }
};

pub const StackTrace = struct {
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
};

pub const PrimitiveType = enum(u8) {
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

pub const PrimitiveValue = union(PrimitiveType) {
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
        try reader.readNoEof(&bytes);
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
};

pub const ClassDump = struct {
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
};

pub const BinaryBlob = struct {
    pos: u64,
    len: usize,
};

pub const InstanceDump = struct {
    id: u64,
    class: u64,
    bytes: BinaryBlob,
    fn parse(reader: anytype, aware: anytype) !InstanceDump {
        const id = try reader.readInt(u64, .big);
        const stack_serial = try reader.readInt(u32, .big);
        const class = try reader.readInt(u64, .big);
        const bytes_count = try reader.readInt(u32, .big);
        const position = aware.position;
        try reader.skipBytes(bytes_count, .{});
        _ = stack_serial;
        return InstanceDump{
            .id = id,
            .class = class,
            .bytes = BinaryBlob{
                .pos = position,
                .len = bytes_count,
            },
        };
    }
};

pub const ObjectArrayDump = struct {
    id: u64,
    class: u64,
    bytes: BinaryBlob,
    fn parse(reader: anytype, aware: anytype) !ObjectArrayDump {
        const id = try reader.readInt(u64, .big);
        const stack_serial = try reader.readInt(u32, .big);
        const elements_count = try reader.readInt(u32, .big);
        const class = try reader.readInt(u64, .big);
        const bytes_count = 8 * elements_count;
        const position = aware.position;
        try reader.skipBytes(bytes_count, .{});
        _ = stack_serial;
        return ObjectArrayDump{
            .id = id,
            .class = class,
            .bytes = BinaryBlob{
                .pos = position,
                .len = bytes_count,
            },
        };
    }
};

pub const PrimitiveArrayDump = struct {
    id: u64,
    element_type: PrimitiveType,
    bytes: BinaryBlob,
    fn parse(reader: anytype, aware: anytype) !PrimitiveArrayDump {
        const id = try reader.readInt(u64, .big);
        const stack_serial = try reader.readInt(u32, .big);
        const elements_count = try reader.readInt(u32, .big);
        const element_type = try reader.readEnum(PrimitiveType, .big);
        const bytes_count = elements_count * @as(usize, switch (element_type) {
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
        const position = aware.position;
        try reader.skipBytes(bytes_count, .{});
        _ = stack_serial;
        return PrimitiveArrayDump{
            .id = id,
            .element_type = element_type,
            .bytes = BinaryBlob{
                .pos = position,
                .len = bytes_count,
            },
        };
    }
};

/// A thread object for a running thread.
pub const RootThread = struct {
    id: u64,
    thread: u32,
    fn parse(reader: anytype) !RootThread {
        const id = try reader.readInt(u64, .big);
        const thread_serial = try reader.readInt(u32, .big);
        const stack_serial = try reader.readInt(u32, .big);
        _ = stack_serial;
        return RootThread{
            .id = id,
            .thread = thread_serial,
        };
    }
};

// Shared between JNI local and regular local variable.
// I don't think it matters much for heap analysis.
pub const RootLocal = struct {
    id: u64,
    thread: u32,
    /// Frame's index in thread's stack trace if available.
    /// Shouldn't be confused with frame's id.
    frame: ?u32,
    fn parse(reader: anytype) !RootLocal {
        const id = try reader.readInt(u64, .big);
        const thread_serial = try reader.readInt(u32, .big);
        const frame_number = try reader.readInt(i32, .big);
        return RootLocal{
            .id = id,
            .thread = thread_serial,
            .frame = if (frame_number < 0) null else @abs(frame_number),
        };
    }
};

const NoopDispatcher = struct {
    fn dispatcher() Dispatcher(void) {
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
    fn onRootThread(_: void, _: RootThread) void {}
    fn onRootLocal(_: void, _: RootLocal) void {}
    fn onRootReference(_: void, _: u64) void {}
    const handlers = DispatchingMethods(void){
        .onConstantString = onConstantString,
        .onLoadClass = onLoadClass,
        .onStackFrame = onStackFrame,
        .onStackTrace = onStackTrace,
        .onClassDump = onClassDump,
        .onInstanceDump = onInstanceDump,
        .onObjectArrayDump = onObjectArrayDump,
        .onPrimitiveArrayDump = onPrimitiveArrayDump,
        .onRootThread = onRootThread,
        .onRootLocal = onRootLocal,
        .onRootReference = onRootReference,
    };
};
