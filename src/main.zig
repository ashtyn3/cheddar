const std = @import("std");
const instance = @import("./instance.zig");
const structs = @import("./structures.zig");
const values = @import("./values.zig");
const query = @import("./query/query.zig");
const runtime = @import("./query/runtime.zig");
const row = @import("./row.zig");
// const rdb = @cImport(@cInclude("rocksdb/c.h"));
const print = std.debug.print;

pub fn main() !void {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();
    // var q = try query.init(allocator);
    // _ = q;

    // print("{}", .{allocator});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var inc = try instance.new_instance(allocator, "127.0.0.1", 8080);
    // try inc.drop_table("how");
    // try inc.incoming();

    var iter = inc.db.iter("").val;
    while (iter.next()) |line| {
        print("{s}: {any}\n", .{ line.key, line.value });
    }

    var t = structs.Table.init("how");
    var col = structs.Column.init(&t, "id", .key);
    col.is_primary = true;
    try t.column(col);

    const col2 = structs.Column.init(&t, "age", values.ValueType.uint);
    try t.column(col2);

    const col3 = structs.Column.init(&t, "name", values.ValueType.string);
    try t.column(col3);

    const r = try row.Row(&t, .{ .name = values.StringLiteral("ashtyn"), .age = 2 });
    // const r2 = try row.Row(&t, .{ .name = values.StringLiteral("bob"), .age = 20 });
    // print("{any}", .{r});
    // try inc.insert_table(t);
    try inc.insert_row(r);
    // try inc.insert_row(r2);
    //
    // const res = try inc.get_column("how", "id");
    // const res = try query.eq(inc, t, "id", values.CheddarKey("aZYgK-0SXReI85S0X6zCzA=="));
    // try runtime.eval(allocator, inc);
    // const res = try query.eq(inc, t, "name", values.CheddarValue(values.StringLiteral("bob")));
    // print("{s}\n", .{res.get("id").?.value.value.key});
    //
    // const res1 = try query.eq(inc, t, "name", values.CheddarValue(values.StringLiteral("bob")));
    // print("{s}\n", .{res1.get("id").?.value.value.key});
}
