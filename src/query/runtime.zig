const std = @import("std");
const ziglua = @import("ziglua");
const instance = @import("../instance.zig");
const structs = @import("../structures.zig");
const values = @import("../values.zig");
const row = @import("../row.zig");
const util = @import("../utils/id.zig");
const generateId = util.generateId;

const Lua = ziglua.Lua;
const log = std.log.scoped(.runtime);
fn column(lua: *Lua) i32 {
    const kind = lua.toInteger(1) catch -1;
    const name = lua.toString(2) catch "";
    if (kind == -1 or name.len == 0) {
        log.err("invalid column data", .{});
        return 0;
    }
    var col = structs.Column{
        .kind = values.ValueType.null,
        .index = 0,
        .name = values.CheddarValue(values.StringLiteral("hi")),
    };
    col.name = values.Value{ .value = .{ .string = name } };
    col.kind = @enumFromInt(kind);
    if (col.kind == .key) {
        col.is_primary = true;
    }

    lua.pushAny(col) catch |err| {
        log.err("failed to create column: {any}", .{err});
        return 0;
    };
    return 1;
}
fn table(lua: *Lua) i32 {
    const name = lua.toString(1) catch "";

    var intern_table = structs.Table.init(name);
    const len = lua.rawLen(2);

    for (0..len) |i| {
        const index: c_longlong = @intCast(i);
        lua.pushInteger(index + 1);
        _ = lua.getTable(2);

        if (lua.isNoneOrNil(-1)) {
            lua.pop(1);
            break;
        }
        // TODO: implement table version for column options
        var col: ?ziglua.Parsed(structs.Column) = lua.toAnyAlloc(structs.Column, -1) catch null;
        if (col) |c| {
            col.?.value.index = @intCast(intern_table.cols.items.len);
            intern_table.column(col.?.value) catch {
                log.err("invalid column {s} could not add", .{c.value.name.value.string});
            };
        }
        lua.pop(1);
    }
    _ = lua.getGlobal("ctx") catch null;
    const c: ?ziglua.Parsed(*instance.Instance) = lua.toAnyAlloc(*instance.Instance, 3) catch null;
    if (c) |ctx| {
        ctx.value.insert_table(intern_table) catch {
            log.err("failed to insert table {s}", .{name});
        };
    }
    return 0;
}

fn encode_value(lua: *Lua, index: i32) values.Value {
    const v = lua.typeOf(index);
    switch (v) {
        .number => {
            const num = lua.toInteger(index) catch 0;
            if (num < 0) {
                unreachable;
            } else {
                return values.Value{ .value = .{ .uint = @intCast(num) } };
            }
        },
        .string => {
            const str = lua.toString(index) catch "";
            return values.Value{ .value = .{ .string = str } };
        },
        .boolean => {
            const str = lua.toBoolean(index);
            return values.Value{ .value = .{ .bool = str } };
        },
        else => {
            unreachable;
        },
    }
}
pub const KVPair = struct {
    key: []const u8,
    value: values.Value,
};
fn kv(lua: *Lua) i32 {
    const key = lua.toString(1) catch "";
    const value = encode_value(lua, 2);

    lua.pushAny(KVPair{ .key = key, .value = value }) catch {
        return 0;
    };
    return 1;
}
fn drop_table(lua: *Lua) i32 {
    const table_name = lua.toString(1) catch "";
    _ = lua.getGlobal("ctx") catch |err| {
        log.err("{}", .{err});
        return 0;
    };
    const c: ?ziglua.Parsed(*instance.Instance) = lua.toAnyAlloc(*instance.Instance, -1) catch |err| {
        log.err("parsing: {}", .{err});
        return 0;
    };
    if (c) |ctx| {
        ctx.value.drop_table(table_name) catch |err| {
            log.err("failed to drop table: {}", .{err});
            return 0;
        };
        log.info("dropped table", .{});
    }
    return 0;
}
fn insert(lua: *Lua) i32 {
    const table_name = lua.toString(1) catch "";
    const len = lua.rawLen(2);
    var pairs = std.ArrayList(KVPair).init(std.heap.c_allocator);

    for (0..len) |i| {
        const index: c_longlong = @intCast(i);
        lua.pushInteger(index + 1);
        _ = lua.getTable(2);

        if (lua.isNoneOrNil(-1)) {
            lua.pop(1);
            break;
        }
        const pair: ?ziglua.Parsed(KVPair) = lua.toAnyAlloc(KVPair, -1) catch null;

        if (pair) |p| {
            pairs.append(p.value) catch {
                std.log.err("failed to make pair", .{});
                continue;
            };
        }
    }

    _ = lua.getGlobal("ctx") catch |err| {
        log.err("{}", .{err});
        return 0;
    };
    const c: ?ziglua.Parsed(*instance.Instance) = lua.toAnyAlloc(*instance.Instance, -1) catch |err| {
        log.err("{}", .{err});
        return 0;
    };
    if (c) |ctx| {
        const t = ctx.value.get_table_seg(table_name) catch |err| {
            log.err("bad table: {}", .{err});
            return 0;
        };
        var vals = std.heap.c_allocator.alloc(values.Value, t.cols.items.len) catch {
            return 0;
        };

        for (pairs.items) |p| {
            const id = ctx.value.column_idx_map(table_name, p.key) catch |err| {
                log.err("{}", .{err});
                return 0;
            };
            vals[id] = p.value;
        }

        const r = row.Row_t{ .table = t, .column_data = vals };
        ctx.value.insert_row(r) catch |err| {
            log.err("Couldn't insert into table: {}", .{err});
            return 0;
        };
    }
    return 0;
}

pub fn eval(allocator: std.mem.Allocator, inst: *instance.Instance, code: [:0]const u8) !void {

    // Initialize the Lua vm
    var lua = try Lua.init(&allocator);
    defer lua.deinit();

    try lua.pushAny(inst);
    lua.setGlobal("ctx");

    lua.pushFunction(ziglua.wrap(table));
    lua.setGlobal("table");

    lua.pushFunction(ziglua.wrap(drop_table));
    lua.setGlobal("drop_table");

    lua.pushFunction(ziglua.wrap(column));
    lua.setGlobal("column");

    lua.pushFunction(ziglua.wrap(kv));
    lua.setGlobal("kv");

    lua.pushFunction(ziglua.wrap(insert));
    lua.setGlobal("insert");

    lua.pushNumber(@intFromEnum(values.ValueType.key));
    lua.setGlobal("key");

    lua.pushNumber(@intFromEnum(values.ValueType.uint));
    lua.setGlobal("uint");

    lua.pushNumber(@intFromEnum(values.ValueType.float));
    lua.setGlobal("float");

    lua.pushNumber(@intFromEnum(values.ValueType.string));
    lua.setGlobal("string");

    var str = std.ArrayList(u8).init(allocator);
    try str.appendSlice(code);
    try str.append(0);

    try lua.doString(@ptrCast(str.items));
    // if (!lua.isNoneOrNil(1)) {
    //     std.log.debug("hi", .{});
    //     var buf = std.ArrayList(u8).init(allocator);
    //     std.json.stringify();
    // }
}
