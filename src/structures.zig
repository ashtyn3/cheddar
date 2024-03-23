const std = @import("std");
const values = @import("./values.zig");

pub const Column = struct {
    index: u16,
    // 0th byte: not null
    // 1st: default
    // 2nd: size (1 = set min, 2 = set max, 3 = set both)
    not_null: bool = false,
    max_size: ?values.Value = null,
    min_size: ?values.Value = null,
    default: ?values.Value = null,
    is_primary: bool = false,
    kind: values.ValueType,
    name: values.Value,
    pub fn serialize(self: Column) ![]u8 {
        var options: u24 = 0x000;
        var col_data = std.ArrayList(u8).init(std.heap.c_allocator);
        var idx_val = values.Value{ .value = .{ .int = self.index } };
        try col_data.append(@intFromBool(self.is_primary));
        try col_data.appendSlice(try idx_val.serialize(0));
        try col_data.append(0);
        try col_data.append(@intFromEnum(self.kind));
        try col_data.append(0);
        try col_data.appendSlice(try self.name.serialize(@intFromEnum(values.ValueType.string)));
        try col_data.append(0);
        if (self.default) |_| {
            options |= 0x010;
        }
        if (self.not_null) {
            options |= 0x100;
        }
        try col_data.append(@as(u8, @truncate(options & 0xf)));
        try col_data.append(@as(u8, @truncate(options & 0x0f)));
        try col_data.append(@as(u8, @truncate(options & 0x00f)));

        if (self.max_size) |ms| {
            options |= 0x001;
            try col_data.appendSlice(try ms.serialize(@intFromEnum(values.ValueType.int)));
            try col_data.append(0);
        }
        if (self.min_size) |ms| {
            options |= 0x002;
            try col_data.appendSlice(try ms.serialize(@intFromEnum(values.ValueType.int)));
            try col_data.append(0);
        }
        // const len_val = Value{ .value = .{ .int = col_data.items.len } };
        // try col_data.insertSlice(0, try len_val.serialize());

        return col_data.items;
    }

    pub fn init(t: *Table, name: []const u8, kind: values.ValueType) Column {
        return .{ .index = @as(u16, @truncate(t.cols.items.len)), .kind = kind, .name = values.Value{ .value = .{ .string = name } } };
    }
};
pub const Table = struct {
    cols: std.ArrayList(Column),
    map: std.StringHashMap(usize),
    name: []const u8,
    keyed: bool,
    pub fn init(name: []const u8) Table {
        const cols = std.ArrayList(Column).init(std.heap.c_allocator);

        return Table{
            .cols = cols,
            .name = name,
            .map = std.StringHashMap(usize).init(std.heap.c_allocator),
            .keyed = false,
        };
    }
    pub fn column(self: *Table, col: Column) !void {
        try self.map.put(try col.name.serialize_value(), self.cols.items.len);
        if (col.is_primary) {
            self.keyed = true;
        }
        try self.cols.append(col);
    }
    pub fn serialize(self: Table) ![]u8 {
        var data = std.ArrayList(u8).init(std.heap.c_allocator);
        for (self.cols.items) |col| {
            try data.appendSlice(try col.serialize());
            try data.append('\n');
        }
        return try data.toOwnedSlice();
    }
};
