const std = @import("std");
const values = @import("./values.zig");

pub const DBErrors = error{
    InvalidColumn,
};
pub const Column = struct {
    index: u16 = 0,
    // 0th byte: not null
    // 1st: default
    // 2nd: size (1 = set min, 2 = set max, 3 = set both)
    not_null: bool = false,
    max_size: ?values.Value = null,
    min_size: ?values.Value = null,
    default: ?values.Value = null,
    is_primary: bool = false,
    kind: values.ValueType = .null,
    name: values.Value = .{ .value = .{ .null = true } },
    pub fn serialize(self: Column) ![]u8 {
        var col_data = std.ArrayList(u8).init(std.heap.c_allocator);
        var idx_val = values.Value{ .value = .{ .uint = self.index } };
        try col_data.append(@intFromBool(self.is_primary));
        try col_data.appendSlice(try idx_val.serialize(0));
        try col_data.append(@intFromEnum(self.kind));
        try col_data.appendSlice(try self.name.serialize(@intFromEnum(values.ValueType.string)));
        // try col_data.append(0);

        // if (self.default) |_| {
        //     try col_data.append(1);
        // } else {
        //     try col_data.append(0);
        // }
        //
        // if (self.not_null) {
        //     try col_data.append(1);
        // } else {
        //     try col_data.append(0);
        // }

        // try col_data.append(@as(u8, @truncate(options & 0x0f)));

        // if (self.max_size) |ms| {
        //     try col_data.append(1);
        //     try col_data.appendSlice(try ms.serialize(@intFromEnum(values.ValueType.uint)));
        // try col_data.append(0);
        // }
        // if (self.min_size) |ms| {
        // try col_data.append(1);
        // try col_data.appendSlice(try ms.serialize(@intFromEnum(values.ValueType.uint)));
        // try col_data.append(0);
        // }
        // const len_val = Value{ .value = .{ .int = col_data.items.len } };
        // try col_data.insertSlice(0, try len_val.serialize());

        return col_data.toOwnedSlice();
    }

    pub fn deserialize(data: []u8) !Column {
        var buf = std.io.fixedBufferStream(data);
        var col = Column{};
        const prim_byte = try buf.reader().readByte();
        if (prim_byte == 1) {
            col.is_primary = true;
        } else {
            col.is_primary = false;
        }
        const temp_idx = try values.Value.deserialize_reader(&buf);
        col.index = @truncate(temp_idx.value.uint);

        col.kind = @enumFromInt(try buf.reader().readByte());
        const temp_name = try values.Value.deserialize_reader(&buf);
        col.name = temp_name.*;
        // col.default = values.CheddarValue((try buf.reader().readByte()) == 1);
        // col.not_null = (try buf.reader().readByte()) == 1;
        // if ((try buf.reader().readByte()) == 1) {
        //     const max_s = try values.Value.deserialize_reader(&buf);
        //     col.max_size = max_s.*;
        // }
        // if ((try buf.reader().readByte()) == 1) {
        //     const min_s = try values.Value.deserialize_reader(&buf);
        //     col.min_size = min_s.*;
        // }
        return col;
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

    pub fn deserialize(name: []const u8, raw: []u8) !Table {
        var data = std.io.fixedBufferStream(raw);
        var table = Table.init(name);
        while (true) {
            var raw_cols = std.ArrayList(u8).init(std.heap.c_allocator);
            data.reader().streamUntilDelimiter(raw_cols.writer(), '\n', null) catch {
                break;
            };
            const col = try Column.deserialize(raw_cols.items);
            try table.column(col);
        }
        return table;
    }
};
