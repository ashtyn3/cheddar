const std = @import("std");

pub const Header = packed struct {
    type: u8,
    size: u64 = 0,
};

pub const ValueType = enum(u8) { int = 0, float, string, bool, null };

pub const Value = struct {
    value: union(ValueType) { int: u64, float: f64, string: []const u8, bool: bool, null: bool } = .{ .null = true },

    fn serialize_value(self: Value) ![]u8 {
        switch (self.value) {
            .null => {
                unreachable;
            },
            .int => {
                var b: []u8 = try std.heap.page_allocator.dupeZ(u8, std.mem.asBytes(&self.value.int));
                return b;
            },
            .float => {
                var b: []u8 = try std.heap.page_allocator.dupeZ(u8, std.mem.asBytes(&self.value.float));
                return b;
            },
            .string => {
                var b = try std.heap.page_allocator.dupeZ(u8, self.value.string[0..]);
                return b;
            },
            .bool => {
                var b: [1]u8 = .{@as(u8, @intFromBool(self.value.bool))};
                return b[0..];
            },
        }
    }
    fn serialize(self: Value, t: u8) ![]u8 {
        var data = try self.serialize_value();
        var head = Header{ .type = t, .size = data.len };
        var h: [9]u8 = @bitCast(head);
        var body = std.ArrayList(u8).init(std.heap.page_allocator);
        try body.appendSlice(&h);
        try body.append('|');
        try body.appendSlice(data);
        return try body.toOwnedSlice();
    }
    pub fn deserialize(head: Header, value: []u8) !*Value {
        var kind: ValueType = @enumFromInt(head.type);
        var data: *Value = try std.heap.page_allocator.create(Value);
        switch (kind) {
            .int => {
                data.value = .{ .int = std.mem.readInt(u64, value[0..8], std.builtin.Endian.Little) };
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
        var index = std.ArrayList(u8).init(std.heap.page_allocator);
        _ = try value.reader().streamUntilDelimiter(index.writer(), '|', null);

        var head: *Header = @alignCast(@ptrCast(index.items[0..9]));
        var data = try std.heap.page_allocator.alloc(u8, head.size);
        _ = try value.read(data);
        return try Value.deserialize(head.*, data);
    }
};
