/// Parsing of a java heap file. Assumes 64-bit identifiers.
/// Check HPROF agent documentation for a complete specification:
/// https://hg.openjdk.org/jdk6/jdk6/jdk/raw-file/tip/src/share/demo/jvmti/hprof/manual.html
const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const LimitedReader = std.io.LimitedReader;
const HashMap = std.AutoHashMapUnmanaged;
const SegmentedList = std.SegmentedList;
const File = std.fs.File;

const assert = std.debug.assert;
const panic = std.debug.panic;

/// Virtual table for data dispatching methods that matter.
/// Method's argument lifetimes are limited to method call:
/// it's relevant for structs that require a buffer, like stack traces or strings.
pub fn HandlerMethods(comptime Context: type, comptime Error: type) type {
    return struct {
        onHeapTimestamp: ?*const fn (context: Context, ts: u64) Error!void = null,
        onConstantString: ?*const fn (context: Context, obj: ConstantString) Error!void = null,
        onLoadClass: ?*const fn (context: Context, obj: LoadClass) Error!void = null,
        onStackFrame: ?*const fn (context: Context, obj: StackFrame) Error!void = null,
        onStackTrace: ?*const fn (context: Context, obj: StackTrace) Error!void = null,
        onClassDump: ?*const fn (context: Context, obj: ClassDump) Error!void = null,
        onInstanceDump: ?*const fn (context: Context, obj: InstanceDump) Error!void = null,
        onObjectArrayDump: ?*const fn (context: Context, obj: ObjectArrayDump) Error!void = null,
        onPrimitiveArrayDump: ?*const fn (context: Context, obj: PrimitiveArrayDump) Error!void = null,
        onRootThread: ?*const fn (context: Context, obj: RootThread) Error!void = null,
        onRootLocal: ?*const fn (context: Context, obj: RootLocal) Error!void = null,
        onRootReference: ?*const fn (context: Context, reference: u64) Error!void = null,
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

pub fn genericScan(
    comptime Context: type,
    comptime Error: type,
    comptime Reader: type,
    source: Reader,
    context: Context,
    methods: HandlerMethods(Context, Error),
    allocator: Allocator,
) !void {
    var aware = PositionAwareReader(Reader){ .backend = source };
    const reader = aware.reader();

    var buffer: [64]u8 = undefined;
    const version = try reader.readUntilDelimiter(&buffer, 0);
    if (!std.mem.eql(u8, version, "JAVA PROFILE 1.0.2")) {
        panic("Unrecognized heap format and version '{s}'.\n", .{version});
    }

    const id_size = try reader.readInt(u32, .big);
    if (id_size != 8) panic("Invalid id size: {}\n", .{id_size});

    const ts = try reader.readInt(u64, .big);
    if (methods.onHeapTimestamp) |func| try func(context, ts);

    while (try SegmentHeader.parse(reader)) |segment| switch (segment.tag) {
        .constant_string => {
            if (methods.onConstantString) |func| {
                const obj = try ConstantString.parse(reader, segment, allocator);
                defer obj.destroy(allocator);
                try func(context, obj);
            } else {
                try reader.skipBytes(segment.size, .{});
            }
        },
        .load_class => {
            if (methods.onLoadClass) |func| {
                try func(context, try LoadClass.parse(reader));
            } else {
                try reader.skipBytes(segment.size, .{});
            }
        },
        .stack_frame => {
            if (methods.onStackTrace) |func| {
                const obj = try StackTrace.parse(reader, allocator);
                defer obj.destroy(allocator);
                try func(context, obj);
            } else {
                try reader.skipBytes(segment.size, .{});
            }
        },
        .stack_trace => {
            if (methods.onStackTrace) |func| {
                const obj = try StackTrace.parse(reader, allocator);
                defer obj.destroy(allocator);
                try func(context, obj);
            } else {
                try reader.skipBytes(segment.size, .{});
            }
        },
        .heap_dump, .heap_dump_segment => {
            var stream = std.io.limitedReader(reader, segment.size);
            const heap = stream.reader();
            while (try RecordTag.parse(heap)) |tag| switch (tag) {
                .class_dump => {
                    const obj = try ClassDump.parse(heap, allocator);
                    defer obj.destroy(allocator);
                    if (methods.onClassDump) |func| {
                        try func(context, obj);
                    }
                },
                .instance_dump => {
                    const obj = try InstanceDump.parse(heap, aware);
                    if (methods.onInstanceDump) |func| {
                        try func(context, obj);
                    }
                },
                .object_array_dump => {
                    const obj = try ObjectArrayDump.parse(heap, aware);
                    if (methods.onObjectArrayDump) |func| {
                        try func(context, obj);
                    }
                },
                .primitive_array_dump => {
                    const obj = try PrimitiveArrayDump.parse(heap, aware);
                    if (methods.onPrimitiveArrayDump) |func| {
                        try func(context, obj);
                    }
                },
                .root_thread_object => {
                    const obj = try RootThread.parse(heap);
                    if (methods.onRootThread) |func| {
                        try func(context, obj);
                    }
                },
                .root_java_frame, .root_jni_local => {
                    const obj = try RootLocal.parse(heap);
                    if (methods.onRootLocal) |func| {
                        try func(context, obj);
                    }
                },
                .root_jni_global => {
                    const id = try heap.readInt(u64, .big);
                    const jni = try heap.readInt(u64, .big);
                    _ = jni;
                    if (methods.onRootReference) |func| {
                        try func(context, id);
                    }
                },
                .root_native_stack, .root_thread_block => {
                    const id = try heap.readInt(u64, .big);
                    const thread_serial = try heap.readInt(u32, .big);
                    if (methods.onRootLocal) |func| {
                        try func(context, .{
                            .id = id,
                            .thread = thread_serial,
                            .frame = null,
                        });
                    }
                },
                .root_sticky_class, .root_monitor_used => {
                    const id = try heap.readInt(u64, .big);
                    if (methods.onRootReference) |func| {
                        try func(context, id);
                    }
                },
            };
        },
        else => try reader.skipBytes(segment.size, .{}),
    };
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
    const Field = struct { name: u64, type: PrimitiveType };
    const Static = struct { name: u64, value: PrimitiveValue };
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
        const statics = try allocator.alloc(Static, statics_count);
        errdefer allocator.free(statics);
        for (statics) |*static| {
            static.name = try reader.readInt(u64, .big);
            const t = try reader.readEnum(PrimitiveType, .big);
            static.value = try PrimitiveValue.parse(t, reader);
        }
        const fields_count = try reader.readInt(u16, .big);
        const fields = try allocator.alloc(Field, fields_count);
        errdefer allocator.free(fields);
        for (fields) |*field| {
            field.name = try reader.readInt(u64, .big);
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
    fn extract(self: BinaryBlob, from: File, allocator: Allocator) ![]u8 {
        const buf = try allocator.alloc(u8, self.len);
        const pos = try from.getPos();
        defer from.seekTo(pos) catch |e| {
            panic("Unable to revert seek position for file {any}: {}\n", .{ from, e });
        };
        try from.seekTo(self.pos);
        if (self.len != try from.read(buf)) {
            panic("Unable to extract binary blob from file {any}\n", .{from});
        }
        return buf;
    }
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

pub const FieldValue = struct {
    name: ?[]const u8,
    value: PrimitiveValue,
};

pub const ClassRegistry = struct {
    pub const ScanError = Allocator.Error;

    const Class = struct {
        name: u64,
        superclass: u64 = 0,
        fields_offset: usize = 0,
        fields_size: usize = 0,
    };

    arena: ArenaAllocator,
    strings: HashMap(u64, []const u8) = .empty,
    classes: HashMap(u64, Class) = .empty,
    fields: SegmentedList(ClassDump.Field, 0) = .{},

    pub const FieldIterator = struct {
        allocator: Allocator,
        registry: *ClassRegistry,
        owned_buffer: []const u8,
        stream: std.io.FixedBufferStream([]u8),
        current_class: u64,
        current_field: u64 = 0,
        pub fn destroy(self: *FieldIterator) void {
            self.allocator.free(self.owned_buffer);
        }
        pub fn next(self: *FieldIterator) !?FieldValue {
            while (self.registry.classes.get(self.current_class)) |class| {
                if (self.current_field < class.fields_size) {
                    const index = class.fields_offset + self.current_field;
                    const field = self.registry.fields.at(index);
                    const value: FieldValue = .{
                        .name = self.registry.strings.get(field.name),
                        .value = try PrimitiveValue.parse(field.type, self.stream.reader()),
                    };
                    self.current_field += 1;
                    return value;
                } else {
                    if (self.current_class == 0) return null;
                    self.current_class = class.superclass;
                    self.current_field = 0;
                }
            } else {
                return null;
            }
        }
    };

    pub fn parseInstance(
        self: *ClassRegistry,
        from: File,
        object: InstanceDump,
        allocator: Allocator,
    ) !FieldIterator {
        const owned_buffer = try object.bytes.extract(from, allocator);
        errdefer allocator.free(owned_buffer);
        return FieldIterator{
            .allocator = allocator,
            .owned_buffer = owned_buffer,
            .registry = self,
            .stream = std.io.fixedBufferStream(owned_buffer),
            .current_class = object.class,
        };
    }

    fn destroy(self: *ClassRegistry) void {
        self.arena.deinit();
    }

    pub fn onConstantString(self: *ClassRegistry, constant: ConstantString) ScanError!void {
        const allocator = self.arena.allocator();
        const owned = try allocator.dupe(u8, constant.string);
        try self.strings.put(allocator, constant.sid, owned);
    }
    pub fn onLoadClass(self: *ClassRegistry, class: LoadClass) ScanError!void {
        const allocator = self.arena.allocator();
        try self.classes.put(allocator, class.id, Class{
            .name = class.name_sid,
        });
    }
    pub fn onClassDump(self: *ClassRegistry, class: ClassDump) ScanError!void {
        const allocator = self.arena.allocator();
        const fields_index = self.fields.len;
        try self.fields.appendSlice(allocator, class.fields);
        if (self.classes.getPtr(class.id)) |ptr| {
            ptr.superclass = class.super;
            ptr.fields_size = class.fields.len;
            ptr.fields_offset = fields_index;
        }
    }
};
