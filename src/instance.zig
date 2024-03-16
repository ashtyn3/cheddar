const std = @import("std");
const rdb = @import("./rocks.zig");
const util = @import("./utils/id.zig");
const generateId = util.generateId;
const net = std.net;
const values = @import("./values.zig");
const structs = @import("./structures.zig");
const print = std.debug.print;
const log = std.log;

pub const Instance = struct {
    alloc: std.mem.Allocator,
    db: rdb.RocksDB,
    server: std.net.StreamServer,
    addr: net.Address,
    fn incoming(self: *Instance) !void {
        while (true) {
            var client = try self.server.accept();

            log.info("Connection: {s}", .{client.address});

            const t = try std.Thread.spawn(.{}, handle, .{ self, client.stream });
            t.detach();
        }
    }
    pub const RowKey = struct { id: []const u8, col: u64, table: []const u8 };

    pub const RowSegment = struct {
        key: RowKey,
        head: values.Header,
        value: values.Value,
    };

    pub fn handle(_: *Instance, con: net.Stream) !void {
        var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const allocator = gpa.allocator();

        while (true) {
            var message = std.mem.zeroes([2]u8);
            _ = con.reader().read(&message) catch {
                break;
            };
            if (message[0] != 0) {
                var head = std.mem.bytesToValue(values.Header, &message);
                switch (head.type) {
                    1 => {
                        var body = try allocator.alloc(u8, head.size);
                        _ = try con.reader().read(body);
                        print("{}", .{body});
                    },
                    else => {
                        break;
                    },
                }
            } else {
                con.close();
            }
        }
    }

    fn partial_key(table: []const u8, col: u64) ![]const u8 {
        var key = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.{}", .{ table, col });
        return key;
    }

    pub fn insert_row_seg(self: *Instance, table: []const u8, col: u64, head: *values.Header, value: values.Value) !void {
        var key = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.{}.{s}", .{ table, col, try generateId() });
        _ = self.db.set(key, try value.serialize(head.type));
    }

    pub fn get_row_seg(self: *Instance, key: []const u8) !RowSegment {
        var data = self.db.get(key);
        var buf = std.io.fixedBufferStream(data.val);

        var head_b = try std.heap.page_allocator.alloc(u8, 9);
        _ = try buf.reader().read(head_b);
        var header: *values.Header = @alignCast(@ptrCast(head_b));
        var v = try values.Value.deserialize(header.*, data.val[10..]);

        var key_reader = std.mem.split(u8, key, ".");
        var table = key_reader.next().?;
        var col: u64 = 0;
        if (key_reader.next()) |col_b| {
            col = try std.fmt.parseUnsigned(u64, col_b, 10);
        }
        var id = key_reader.next().?;
        return RowSegment{ .head = header.*, .value = v.*, .key = .{ .id = id, .col = col, .table = table } };
    }
    fn column_idx_map(self: *Instance, table: []const u8, col: []const u8) !u64 {
        var data = self.db.get(table);
        var buf = std.io.fixedBufferStream(data.val);
        var kind: values.ValueType = values.ValueType.null;
        var idx: u64 = 0;
        while (true) {
            var raw_column = std.ArrayList(u8).init(std.heap.page_allocator);
            buf.reader().streamUntilDelimiter(raw_column.writer(), '\n', null) catch {
                break;
            };
            var struct_data = std.io.fixedBufferStream(try raw_column.toOwnedSlice());
            var index = try values.Value.deserialize_reader(&struct_data);
            idx = index.value.int;
            try struct_data.reader().skipBytes(1, .{});
            kind = @enumFromInt(try struct_data.reader().readByte());
            try struct_data.reader().skipBytes(1, .{});
            var name = try values.Value.deserialize_reader(&struct_data);
            if (std.mem.eql(u8, col, name.value.string)) {
                return index.value.int;
            }
        }
        return std.math.maxInt(u64);
    }
    pub fn get_column(self: *Instance, table: []const u8, col: []const u8) !std.ArrayList(RowSegment) {
        var idx = try self.column_idx_map(table, col);
        var partial_filter = partial_key(table, idx);

        var iter = self.db.iter(try partial_filter).val;
        var list = std.ArrayList(RowSegment).init(std.heap.page_allocator);
        while (iter.next()) |line| {
            try list.append(try self.get_row_seg(line.key));
        }
        return list;
    }
    pub fn insert_table(self: *Instance, t: structs.Table) !void {
        _ = self.db.set(t.name, try t.serialize());
    }
};

pub fn new_instance(addr: []const u8, port: u16) !Instance {
    var arena = std.heap.GeneralPurposeAllocator(.{}){};

    const allocator = arena.allocator();

    const loopback = try net.Ip4Address.parse(addr, port);
    const host = net.Address{ .in = loopback };

    var server = net.StreamServer.init(net.StreamServer.Options{
        .reuse_port = true,
    });

    try server.listen(host);

    std.fs.cwd().access("db", .{}) catch {
        log.info("Creating db at ./db.", .{});
    };
    var temp: Instance = .{ .db = rdb.RocksDB.open(allocator, "db").val, .alloc = allocator, .addr = host, .server = server };

    log.info("Starting db server.", .{});
    log.info("Address: {s}:{d}", .{ addr, port });

    return temp;
}
