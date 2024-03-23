const std = @import("std");

pub const Header = packed struct {
    type: u8,
    size: u64 = 0,
};

pub const ValueType = enum(u8) { int = 0, float, string, bool, null, key };

pub fn StringLiteral(v: []const u8) []const u8 {
    return @as([]const u8, v);
}
pub fn CheddarKey(v: []const u8) Value {
    return Value{ .value = .{ .key = v } };
}
pub fn CheddarValue(v: anytype) Value {
    std.log.info("{any}", .{@TypeOf(v)});
    switch (@TypeOf(v)) {
        []const u8 => return Value{ .value = .{ .string = v } },
        u64, comptime_int => return Value{ .value = .{ .int = v } },
        f64, comptime_float => return Value{ .value = .{ .float = v } },
        bool => return Value{ .value = .{ .bool = v } },
        else => {
            unreachable;
        },
    }
}
pub const Value = struct {
    value: union(ValueType) { int: u64, float: f64, string: []const u8, bool: bool, null: bool, key: []const u8 } = .{ .null = true },

    pub fn serialize_value(self: Value) ![]u8 {
        switch (self.value) {
            .null => {
                unreachable;
            },
            .int => {
                const b: []u8 = try std.heap.c_allocator.dupeZ(u8, std.mem.asBytes(&self.value.int));
                return b;
            },
            .float => {
                const b: []u8 = try std.heap.c_allocator.dupeZ(u8, std.mem.asBytes(&self.value.float));
                return b;
            },
            .string => {
                const b = try std.heap.c_allocator.dupeZ(u8, self.value.string[0..]);
                return b;
            },
            .key => {
                const b = try std.heap.c_allocator.dupeZ(u8, self.value.key[0..]);
                return b;
            },
            .bool => {
                const b: [1]u8 = .{@as(u8, @intFromBool(self.value.bool))};
                return try std.heap.c_allocator.dupeZ(u8, b[0..]);
            },
        }
    }
    pub fn serialize(self: Value, t: u8) ![]u8 {
        const data = try self.serialize_value();
        const head = Header{ .type = t, .size = data.len };
        var h: [9]u8 = @bitCast(head);
        var body = std.ArrayList(u8).init(std.heap.c_allocator);
        try body.appendSlice(&h);
        try body.append('|');
        try body.appendSlice(data);
        return try body.toOwnedSlice();
    }
    pub fn deserialize(head: Header, value: []u8) !*Value {
        const kind: ValueType = @enumFromInt(head.type);
        var data: *Value = try std.heap.c_allocator.create(Value);
        switch (kind) {
            .int => {
                data.value = .{ .int = std.mem.readInt(u64, value[0..8], std.builtin.Endian.little) };
                return data;
            },
            .float => {
                data.value = .{ .float = std.mem.bytesToValue(f64, value[0..8]) };
                return data;
            },
            .string => {
                data.value = .{ .string = value };
                return data;
            },
            .key => {
                data.value = .{ .key = value };
                return data;
            },
            .bool => {
                data.value = .{ .bool = std.mem.bytesToValue(bool, value[0..1]) };
                return data;
            },
            .null => {
                unreachable;
            },
        }
        return data;
    }

    pub fn deserialize_reader(value: anytype) !*Value {
        var index = std.ArrayList(u8).init(std.heap.c_allocator);
        _ = try value.reader().streamUntilDelimiter(index.writer(), '|', null);

        const head: *Header = @alignCast(@ptrCast(index.items[0..9]));
        const data = try std.heap.c_allocator.alloc(u8, head.size);
        _ = try value.read(data);
        return try Value.deserialize(head.*, data);
    }
};
