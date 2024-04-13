const std = @import("std");
const cache = @import("./cache/cache.zig");
const comp = @import("./compactor.zig");

pub const Cache = struct {
    alloc: std.mem.Allocator,
    map: cache.Cache(Cache_map),
    compaction: std.DoublyLinkedList(comp.MarkedRange),
    const Cache_map = struct {
        id: u64 = 0,
        tombstoned: bool = false,
        row: []const u8 = "",
    };
    pub fn init(a: std.mem.Allocator) !*Cache {
        const s = try a.create(Cache);
        s.* = .{
            .alloc = a,
            .map = try cache.Cache(Cache_map).init(a, .{ .max_size = 10000 }),
            .compaction = std.DoublyLinkedList(comp.MarkedRange){ .last = null, .first = null, .len = 0 },
        };
        return s;
    }
};
