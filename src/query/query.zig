const instance = @import("../instance.zig");
const structs = @import("../structures.zig");
const values = @import("../values.zig");
const std = @import("std");
const log = std.log.scoped(.query);

pub fn eq(self: *instance.Instance, table: structs.Table, col: []const u8, value: values.Value) !std.StringArrayHashMap(instance.Instance.RowSegment) {
    var id: []const u8 = "";
    const cache_key = try std.fmt.allocPrint(std.heap.c_allocator, "{s},{any}", .{ col, value });
    const hash = try std.fmt.allocPrint(std.heap.c_allocator, "{d}", .{std.hash.XxHash3.hash(0, cache_key)});
    if (self.cache.map.get(hash)) |h| {
        id = h.value.row;
    }
    if (id.len == 0) {
        const iter = try self.get_column(table.name, col);
        for (iter) |v| {
            if (@intFromEnum(v.value.value) == @intFromEnum(value.value)) {
                if (std.mem.eql(u8, try v.value.serialize_value(), try value.serialize_value())) {
                    id = v.key.id;
                }
            }
        }
    }

    var res = std.StringArrayHashMap(instance.Instance.RowSegment).init(self.alloc);
    if (id.len == 0) {
        log.err("Cannot find row with specified column value", .{});
        return res;
    }

    try self.cache.map.put(hash, .{ .row = id }, .{});
    for (0..table.cols.items.len) |i| {
        const key = try std.fmt.allocPrint(std.heap.c_allocator, "{s}.{d}.{s}", .{ table.name, i, id });
        const seg = try self.get_row_seg(key);
        try res.put(table.cols.items[i].name.value.string, seg);
    }

    return res;
}
