const instance = @import("../instance.zig");
const structs = @import("../structures.zig");
const values = @import("../values.zig");
const std = @import("std");

pub fn eq(self: *instance.Instance, table: structs.Table, col: []const u8, value: values.Value) !std.StringArrayHashMap(instance.Instance.RowSegment) {
    const iter = try self.get_column(table.name, col);
    var id: []const u8 = "";
    for (iter) |v| {
        if (@intFromEnum(v.value.value) == @intFromEnum(value.value)) {
            if (std.mem.eql(u8, try v.value.serialize_value(), try value.serialize_value())) {
                id = v.key.id;
            }
        }
    }
    var res = std.StringArrayHashMap(instance.Instance.RowSegment).init(self.alloc);
    for (0..table.cols.items.len) |i| {
        const key = try std.fmt.allocPrint(std.heap.c_allocator, "{s}.{d}.{s}", .{ table.name, i, id });
        const seg = try self.get_row_seg(key);
        try res.put(table.cols.items[i].name.value.string, seg);
    }

    return res;
}
