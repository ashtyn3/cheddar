const std = @import("std");

const rdb = @cImport(@cInclude("rocksdb/c.h"));

pub const RequestReturn = enum { val, err, not_found };
pub const RocksDB = struct {
    db: *rdb.rocksdb_t,
    allocator: std.mem.Allocator,

    // Strings in RocksDB are malloc-ed. So make a copy that our
    // allocator owns and then free the string.
    fn ownString(self: RocksDB, string: []u8) []u8 {
        const result = self.allocator.alloc(u8, string.len) catch unreachable;
        std.mem.copy(u8, result, string);
        std.heap.c_allocator.free(string);
        return result;
    }

    // Similar to ownString but for strings that are zero delimited,
    // drops the zero.
    fn ownZeroString(self: RocksDB, zstr: [*:0]u8) []u8 {
        const spanned = std.mem.span(zstr);
        const result = self.allocator.alloc(u8, spanned.len) catch unreachable;
        std.mem.copy(u8, result, spanned);
        std.heap.c_allocator.free(zstr);
        return result;
    }

    // TODO: replace std.mem.span(errStr) with ownZeroString()

    pub fn open(allocator: std.mem.Allocator, dir: []const u8) union(enum) { val: RocksDB, err: []u8 } {
        const options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);
        var err: ?[*:0]u8 = null;
        const db = rdb.rocksdb_open(options, dir.ptr, &err);
        const r = RocksDB{ .db = db.?, .allocator = allocator };
        if (err) |errStr| {
            return .{ .err = std.mem.span(errStr) };
        }
        return .{ .val = r };
    }

    pub fn close(self: RocksDB) void {
        rdb.rocksdb_close(self.db);
    }

    pub fn set(self: RocksDB, key: []const u8, value: []const u8) ?[]u8 {
        const writeOptions = rdb.rocksdb_writeoptions_create();
        var err: ?[*:0]u8 = null;
        rdb.rocksdb_put(
            self.db,
            writeOptions,
            key.ptr,
            key.len,
            value.ptr,
            value.len,
            &err,
        );
        if (err) |errStr| {
            return std.mem.span(errStr);
        }

        return null;
    }

    pub fn delete(self: RocksDB, key: []const u8) ?[]u8 {
        const writeOptions = rdb.rocksdb_writeoptions_create();
        var err: ?[*:0]u8 = null;
        rdb.rocksdb_delete(self.db, writeOptions, key.ptr, key.len, &err);
        if (err) |errStr| {
            return std.mem.span(errStr);
        }
        return null;
    }

    pub fn get(self: RocksDB, key: []const u8) union(RequestReturn) { val: []u8, err: []u8, not_found: bool } {
        const readOptions = rdb.rocksdb_readoptions_create();
        var valueLength: usize = 0;
        var err: ?[*:0]u8 = null;
        var v = rdb.rocksdb_get(
            self.db,
            readOptions,
            key.ptr,
            key.len,
            &valueLength,
            &err,
        );

        if (err) |errStr| {
            return .{ .err = std.mem.span(errStr) };
        }
        if (v == 0) {
            return .{ .not_found = true };
        }

        return .{ .val = v[0..valueLength] };
    }

    pub const IterEntry = struct {
        key: []const u8,
        value: []const u8,
    };

    pub const Iter = struct {
        iter: *rdb.rocksdb_iterator_t,
        first: bool,
        prefix: []const u8,

        pub fn next(self: *Iter) ?IterEntry {
            if (!self.first) {
                rdb.rocksdb_iter_next(self.iter);
            }

            self.first = false;
            if (rdb.rocksdb_iter_valid(self.iter) != 1) {
                return null;
            }

            var keySize: usize = 0;
            var key = rdb.rocksdb_iter_key(self.iter, &keySize);

            // Make sure key is still within the prefix
            if (self.prefix.len > 0) {
                if (self.prefix.len > keySize or
                    !std.mem.eql(u8, key[0..self.prefix.len], self.prefix))
                {
                    return null;
                }
            }

            var valueSize: usize = 0;
            var value = rdb.rocksdb_iter_value(self.iter, &valueSize);

            return IterEntry{
                .key = key[0..keySize],
                .value = value[0..valueSize],
            };
        }

        pub fn close(self: Iter) void {
            rdb.rocksdb_iter_destroy(self.iter);
        }
    };

    pub fn iter(self: RocksDB, prefix: []const u8) union(enum) { val: Iter, err: []u8 } {
        const readOptions = rdb.rocksdb_readoptions_create();
        var it = Iter{
            .iter = undefined,
            .first = true,
            .prefix = prefix,
        };
        it.iter = rdb.rocksdb_create_iterator(self.db, readOptions).?;

        var err: ?[*:0]u8 = null;
        rdb.rocksdb_iter_get_error(it.iter, &err);
        if (err) |errStr| {
            return .{ .err = std.mem.span(errStr) };
        }

        if (prefix.len > 0) {
            rdb.rocksdb_iter_seek(
                it.iter,
                prefix.ptr,
                prefix.len,
            );
        } else {
            rdb.rocksdb_iter_seek_to_first(it.iter);
        }

        return .{ .val = it };
    }
};
