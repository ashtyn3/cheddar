const cache = @import("./cache.zig");
const rdb = @import("./rocks.zig");
const std = @import("std");
const log = std.log.scoped(.compactor);

pub const MarkedRange = struct {
    kind: enum { ROW, TAB, COL },
    table: []const u8 = "",
    col: u64 = 0,
    row: []const u8 = "",
};

pub fn compact(c: *cache.Cache, db: rdb.RocksDB) void {
    log.info("starting compaction", .{});
    while (true) {
        while (c.compaction.last) |comp| {
            switch (comp.data.kind) {
                .TAB => {
                    const data = db.delete(comp.data.table);
                    if (data == null) {
                        log.info("compacted table: {s}", .{comp.data.table});
                    }
                    var table = db.iter(comp.data.table).val;
                    while (table.next()) |seg| {
                        _ = db.delete(seg.key);
                    }
                },
                else => {
                    return;
                },
            }
            _ = c.compaction.pop();
        }
    }
}
