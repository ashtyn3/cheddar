const std = @import("std");
const values = @import("./values.zig");
const structs = @import("./structures.zig");

pub const Row_t = struct { column_data: []values.Value, table: structs.Table };
pub fn Row(table: *structs.Table, comptime cols: anytype) !Row_t {
    var raw_columns = try std.heap.c_allocator.alloc(values.Value, table.cols.items.len);
    var expected = raw_columns.len;
    var cols_allocated: u64 = 0;
    if (table.keyed) {
        expected -= 1;
    }
    inline for (std.meta.fields(@TypeOf(cols))) |f| {
        const field: std.builtin.Type.StructField = f;
        const value = @field(cols, field.name);
        if (table.map.get(field.name)) |idx| {
            raw_columns[idx] = values.CheddarValue(value);
            cols_allocated += 1;
        }
    }
    if (expected != cols_allocated) {
        std.log.err("Cannot add row. Invalid column count.", .{});
    }

    return Row_t{ .column_data = raw_columns, .table = table.* };
}
