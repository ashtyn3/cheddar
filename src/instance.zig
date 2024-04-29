const std = @import("std");
const rdb = @import("./rocks.zig");
const util = @import("./utils/id.zig");
const generateId = util.generateId;
const net = std.net;
const values = @import("./values.zig");
const row = @import("./row.zig");
const structs = @import("./structures.zig");
const network = @import("network");
const print = std.debug.print;
const log = std.log.scoped(.instance);
const client = @import("./client.zig");
const data_cache = @import("./cache.zig");
const comp = @import("./compactor.zig");

pub const InstanceErrors = error{
    MissingTable,
    MissingColumn,
};
pub const Instance = struct {
    alloc: std.mem.Allocator,
    db: rdb.RocksDB,
    server: std.net.Server,
    cache: *data_cache.Cache,
    addr: net.Address,
    pool: *std.Thread.Pool,
    pub fn incoming(self: *Instance) !void {
        try self.pool.spawn(comp.compact, .{ self.cache, self.db });
        while (true) {
            var connection = self.server.accept() catch {
                self.server.deinit();
                break;
            };

            try self.pool.spawn(client.handle, .{ &connection, self.alloc, self });
        }
        // var group = std.Thread.WaitGroup{};
        // self.pool.waitAndWork(&group);
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

    pub fn get_table_seg(self: *Instance, name: []const u8) !structs.Table {
        const raw = self.db.get(name);
        switch (raw) {
            .val => {
                return structs.Table.deserialize(name, raw.val) catch |err| {
                    std.log.info("bad table: {}", .{err});
                    return InstanceErrors.MissingTable;
                };
            },
            .not_found => {
                return InstanceErrors.MissingTable;
            },
            else => {
                unreachable;
            },
        }
    }

    pub fn column_idx_map(self: *Instance, table: []const u8, col: []const u8) !u64 {
        if (self.cache.map.get(table)) |ct| {
            if (ct.value.tombstoned) {
                return InstanceErrors.MissingTable;
            }
        } else {
            switch (self.db.get(table)) {
                .not_found => {
                    return InstanceErrors.MissingTable;
                },
                else => {},
            }
        }

        if (self.cache.map.get(col)) |c| {
            return c.value.id;
        }
        const data = self.db.get(table);
        var buf = std.io.fixedBufferStream(data.val);
        while (true) {
            var raw_column = std.ArrayList(u8).init(std.heap.c_allocator);
            buf.reader().streamUntilDelimiter(raw_column.writer(), '\n', null) catch {
                break;
            };
            const col_data = try structs.Column.deserialize(try raw_column.toOwnedSlice());
            if (std.mem.eql(u8, col, col_data.name.value.string)) {
                try self.cache.map.put(col, .{ .id = col_data.index }, .{});
                return col_data.index;
            }
        }
        // try self.cache.map.put(col, .{ .id = idx }, .{});
        return InstanceErrors.MissingColumn;
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
        if (self.cache.map.get(r.table.name)) |ct| {
            if (ct.value.tombstoned) {
                return InstanceErrors.MissingTable;
            }
        } else {
            switch (self.db.get(r.table.name)) {
                .not_found => {
                    return InstanceErrors.MissingTable;
                },
                else => {},
            }
        }
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

    pub fn drop_table(self: *Instance, name: []const u8) !void {
        var node = std.DoublyLinkedList(comp.MarkedRange).Node{ .data = .{ .kind = .TAB, .table = name } };
        self.cache.compaction.prepend(&node);
        try self.cache.map.put(name, .{ .tombstoned = true }, .{});
    }

    pub fn insert_table(self: *Instance, t: structs.Table) !void {
        log.info("creating table: {s}", .{t.name});
        _ = self.db.set(t.name, try t.serialize());
    }
};

pub fn new_instance(allocator: std.mem.Allocator, addr: []const u8, port: u16) !*Instance {
    _ = std.fs.cwd().openDir("db", .{}) catch {
        const path = try std.fs.cwd().realpathAlloc(allocator, ".");
        log.info("Creating db at: {s}/db", .{path});
    };
    var arena = std.heap.ArenaAllocator.init(allocator);
    const db_alloc = arena.allocator();

    const c = try data_cache.Cache.init(allocator);

    const loopback = try net.Ip4Address.parse(addr, port);
    const host = net.Address{ .in = loopback };

    const server = try host.listen(.{ .reuse_port = true });

    log.info("Starting db server.", .{});
    log.info("Address: {s}:{d}", .{ addr, port });

    const pool = try allocator.create(std.Thread.Pool);
    try std.Thread.Pool.init(pool, .{ .allocator = allocator });

    const inst = try allocator.create(Instance);
    inst.* = .{ .db = rdb.RocksDB.open(db_alloc, "db").val, .alloc = allocator, .addr = host, .server = server, .cache = c, .pool = pool };

    return inst;
}
