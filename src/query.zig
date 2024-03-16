const std = @import("std");
const ziglua = @import("ziglua");

const Lua = ziglua.Lua;

pub const Query_engine = struct { alloc: std.mem.Allocator, lua: Lua };

fn adder(lua: *Lua) i32 {
    const a = lua.toInteger(1) catch 0;
    const b = lua.toInteger(2) catch 0;
    lua.pushInteger(a + b);
    return 1;
}

pub fn init(alloc: std.mem.Allocator) !Query_engine {
    var lua = try Lua.init(alloc);
    lua.pushFunction(ziglua.wrap(adder));
    lua.setGlobal("add");
    lua.open(.{ .base = true });
    return Query_engine{ .alloc = alloc, .lua = lua };
}
