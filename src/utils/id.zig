const std = @import("std");

pub fn generateId() ![]const u8 {
    var buf: [16]u8 = std.mem.zeroes([16]u8);
    std.crypto.random.bytes(&buf);
    const size = std.base64.standard.Encoder.calcSize(16);
    const b64_buf = try std.heap.page_allocator.alloc(u8, size);
    const b64_id = std.base64.url_safe.Encoder.encode(b64_buf, &buf);

    return b64_id;
}
