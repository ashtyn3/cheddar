const std = @import("std");
const ziglua = @import("ziglua");
const instance = @import("../instance.zig");
const structs = @import("../structures.zig");
const values = @import("../values.zig");

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

    lua.pushAny(col) catch {
        log.err("failed to create column", .{});
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

pub fn eval(allocator: std.mem.Allocator, inst: *instance.Instance) !void {

    // Initialize the Lua vm
    var lua = try Lua.init(&allocator);
    defer lua.deinit();

    try lua.pushAny(inst);
    lua.setGlobal("ctx");

    lua.pushFunction(ziglua.wrap(table));
    lua.setGlobal("table");

    lua.pushFunction(ziglua.wrap(column));
    lua.setGlobal("column");

    lua.pushNumber(@intFromEnum(values.ValueType.key));
    lua.setGlobal("key");

    lua.pushNumber(@intFromEnum(values.ValueType.uint));
    lua.setGlobal("uint");

    lua.pushNumber(@intFromEnum(values.ValueType.float));
    lua.setGlobal("float");

    lua.pushNumber(@intFromEnum(values.ValueType.string));
    lua.setGlobal("string");

    try lua.doFile("query.lua");
}
