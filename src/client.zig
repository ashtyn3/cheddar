const runtime = @import("./query/runtime.zig");
const instance = @import("./instance.zig");
const std = @import("std");
const log = std.log.scoped(.net);

pub const Client = struct {
    conn: *std.net.Server.Connection,
    alloc: std.mem.Allocator,
};
pub fn handle(conn: *std.net.Server.Connection, alloc: std.mem.Allocator, inst: *instance.Instance) void {
    var self = alloc.create(Client) catch {
        return;
    };
    self.alloc = alloc;
    self.conn = conn;

    while (true) {
        var buffer = std.ArrayList(u8).init(std.heap.c_allocator);
        self.conn.stream.reader().streamUntilDelimiter(buffer.writer(), '\n', null) catch {
            return;
        };
        if (buffer.items.len != 0) {
            log.info("got request.\n{s}", .{buffer.items});
            runtime.eval(alloc, inst, @ptrCast(buffer.items)) catch {};
        }
    }
}
