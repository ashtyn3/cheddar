const std = @import("std");
const instance = @import("./instance.zig");
// const rdb = @cImport(@cInclude("rocksdb/c.h"));
const print = std.debug.print;

pub fn main() !void {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();
    // var q = try query.init(allocator);
    // _ = q;

    // print("{}", .{allocator});
    var inst = try instance.new_instance("127.0.0.1", 8080);

    // var t = Table.init("how");
    // var col = Column.init(&t, "age", ValueType.string);
    // try t.cols.append(col);
    //
    // var col2 = Column.init(&t, "name", ValueType.string);
    // try t.cols.append(col2);
    //
    // try inst.insert_table(t);

    // var iter = inst.db.iter("how").val;
    // while (iter.next()) |line| {
    //     print("{s}: {any}\n", .{ line.key, line.value });
    // }

    var res = try inst.get_column("how", "age");
    print("{any}\n", .{res.items});
    //other.1.zIEvL51GcizebfbeUYesNA==
    // _ = inst.db.get("other.0.G38yXhkHDcOcx1FAjaJOuQ==");
    // print("{s}", .{(try inst.get_row_seg("other", 1, "zIEvL51GcizebfbeUYesNA==")).key.id});
    // var b = Header{ .type = 0 };
    // var v = Value{ .value = .{ .int = 555 } };
    //
    // try inst.insert_row_seg("how", 0, &b, v);

    // const addr = inst.server.listen_address;
    // print("Listening on {}, access this port to end the program\n", .{addr.getPort()});
    // try inst.incoming();
}
