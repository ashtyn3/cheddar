const std = @import("std");
const rdb = @import("./rocks.zig");
const util = @import("./utils/id.zig");
const generateId = util.generateId;
const net = std.net;
const values = @import("./values.zig");
const row = @import("./row.zig");
const structs = @import("./structures.zig");
const cache = @import("./cache/cache.zig");
const network = @import("network");
const print = std.debug.print;
const log = std.log.scoped(.instance);
const client = @import("./client.zig");

pub const Cache = struct {
    alloc: std.mem.Allocator,
    map: cache.Cache(Cache_map),
    const Cache_map = struct {
        id: u64,
    };
    fn init(a: std.mem.Allocator) !*Cache {
        const s = try a.create(Cache);
        s.* = .{
            .alloc = a,
            .map = try cache.Cache(Cache_map).init(a, .{ .max_size = 10000 }),
        };
        return s;
    }
};

pub const Instance = struct {
    alloc: std.mem.Allocator,
    db: rdb.RocksDB,
    server: *std.net.Server,
    cache: *Cache,
    addr: net.Address,
    pub fn incoming(self: *Instance) !void {
        var pool = try self.alloc.create(std.Thread.Pool);
        try std.Thread.Pool.init(pool, .{ .allocator = self.alloc });
        while (true) {
            var connection = self.server.accept() catch {
                continue;
            };

            try pool.spawn(client.handle, .{ &connection, self.alloc });
        }
        std.Thread.Pool.waitAndWork();
    }
    pub const RowKey = struct { id: []const u8, col: u64, table: []const u8 };

    pub const RowSegment = struct {
        key: RowKey,
        head: values.Header,
        value: values.Value,
    };

    pub fn partial_key(table: []const u8, col: u64) ![]const u8 {
        const key = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.{any}", .{ table, col });
        return key;
    }

    pub fn insert_row_seg(self: *Instance, table: []const u8, col: u64, head: values.Header, value: values.Value, id: ?[]const u8) ![]const u8 {
        var gen_id = try generateId();
        if (id) |passed| {
            gen_id = passed;
        }
        const key = try std.fmt.allocPrint(std.heap.c_allocator, "{s}.{any}.{s}", .{ table, col, gen_id });
        _ = self.db.set(key, try value.serialize(head.type));

        return gen_id;
    }

    pub fn get_row_seg(self: *Instance, key: []const u8) !RowSegment {
        var data = self.db.get(key);
        var buf = std.io.fixedBufferStream(data.val);

        const head_b = try std.heap.c_allocator.alloc(u8, 9);
        defer std.heap.c_allocator.free(head_b);

        _ = try buf.reader().read(head_b);
        const header: *values.Header = @alignCast(@ptrCast(head_b));
        const v = try values.Value.deserialize(header.*, data.val[10..]);

        var key_reader = std.mem.split(u8, key, ".");
        const table = key_reader.next().?;
        var col: u64 = 0;
        if (key_reader.next()) |col_b| {
            col = try std.fmt.parseUnsigned(u64, col_b, 10);
        }
        const id = key_reader.next().?;
        return RowSegment{ .head = header.*, .value = v.*, .key = .{ .id = id, .col = col, .table = table } };
    }
    fn column_idx_map(self: *Instance, table: []const u8, col: []const u8) !u64 {
        if (self.cache.map.get(col)) |c| {
            return c.value.id;
        }
        const data = self.db.get(table);
        var buf = std.io.fixedBufferStream(data.val);
        var kind: values.ValueType = values.ValueType.null;
        var idx: u64 = 0;
        while (true) {
            var raw_column = std.ArrayList(u8).init(std.heap.c_allocator);
            buf.reader().streamUntilDelimiter(raw_column.writer(), '\n', null) catch {
                break;
            };
            var struct_data = std.io.fixedBufferStream(try raw_column.toOwnedSlice());
            try struct_data.reader().skipBytes(1, .{});
            const index = try values.Value.deserialize_reader(&struct_data);
            idx = index.value.int;
            try struct_data.reader().skipBytes(1, .{});
            kind = @enumFromInt(try struct_data.reader().readByte());
            try struct_data.reader().skipBytes(1, .{});
            const name = try values.Value.deserialize_reader(&struct_data);
            if (std.mem.eql(u8, col, name.value.string)) {
                return index.value.int;
            }
        }
        try self.cache.map.put(col, .{ .id = idx }, .{});
        return std.math.maxInt(u64);
    }

    pub fn get_column(self: *Instance, table: []const u8, col: []const u8) ![]RowSegment {
        const idx = try self.column_idx_map(table, col);
        const partial_filter = partial_key(table, idx);

        var iter = self.db.iter(try partial_filter).val;
        var list = std.ArrayList(RowSegment).init(std.heap.c_allocator);
        while (iter.next()) |line| {
            try list.append(try self.get_row_seg(line.key));
        }
        return list.toOwnedSlice();
    }

    pub fn insert_row(self: *Instance, r: row.Row_t) !void {
        var id: []const u8 = try generateId();
        if (r.table.cols.items[0].is_primary) {
            const head = values.Header{ .type = @intFromEnum(r.table.cols.items[0].kind) };
            id = try self.insert_row_seg(r.table.name, 0, head, .{ .value = .{ .key = id } }, id);
        }
        for (r.column_data, 0..) |col, i| {
            const head = values.Header{ .type = @intFromEnum(r.table.cols.items[i].kind) };
            if (i != 0) {
                _ = try self.insert_row_seg(r.table.name, i, head, col, id);
            }
        }
    }

    pub fn insert_table(self: *Instance, t: structs.Table) !void {
        _ = self.db.set(t.name, try t.serialize());
    }
};

pub fn new_instance(allocator: std.mem.Allocator, addr: []const u8, port: u16) !*Instance {
    var arena = std.heap.ArenaAllocator.init(allocator);
    const db_alloc = arena.allocator();

    const c = try Cache.init(db_alloc);

    const loopback = try net.Ip4Address.parse(addr, port);
    const host = net.Address{ .in = loopback };

    var server = try host.listen(.{ .reuse_port = true });

    std.fs.cwd().access("db", .{}) catch {
        log.info("Creating db at ./db.", .{});
    };

    log.info("Starting db server.", .{});
    log.info("Address: {s}:{d}", .{ addr, port });

    const inst = try allocator.create(Instance);
    inst.* = .{ .db = rdb.RocksDB.open(db_alloc, "db").val, .alloc = allocator, .addr = host, .server = &server, .cache = c };

    return inst;
}
