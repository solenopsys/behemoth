const std = @import("std");
const fs_compat = @import("../fs_compat.zig");
const sync_compat = @import("../sync_compat.zig");
const Allocator = std.mem.Allocator;
const lmdbx = @import("lmdbx");
const Telemetry = @import("../telemetry.zig").Telemetry;

const mib: u64 = 1024 * 1024;

var mdbx_mutex = sync_compat.Mutex{};

fn traceMdbxBegin(op: []const u8, path: []const u8) void {
    std.debug.print("kv mdbx begin op={s} path={s}\n", .{ op, path });
}

fn traceMdbxEnd(op: []const u8, path: []const u8) void {
    std.debug.print("kv mdbx end op={s} path={s}\n", .{ op, path });
}

fn traceMdbxWriteOps() bool {
    const raw = std.c.getenv("BEHEMOTH_KV_TRACE_WRITES") orelse return false;
    const value = std.mem.span(raw);
    return value.len > 0 and !std.mem.eql(u8, value, "0") and !std.ascii.eqlIgnoreCase(value, "false");
}

pub const KvEngine = struct {
    pub const Config = struct {
        map_lower_mib: ?u64 = null,
        map_now_mib: ?u64 = null,
        map_upper_mib: ?u64 = null,
        map_growth_mib: ?u64 = null,
        map_shrink_mib: ?u64 = null,
        auto_compact_on_open: bool = true,
    };

    db: ?lmdbx.Database,
    path: [:0]const u8,
    allocator: Allocator,
    config: Config,

    pub fn init(allocator: Allocator, path: [:0]const u8) KvEngine {
        return initWithConfig(allocator, path, .{});
    }

    pub fn initWithConfig(allocator: Allocator, path: [:0]const u8, config: Config) KvEngine {
        return .{
            .db = null,
            .path = path,
            .allocator = allocator,
            .config = config,
        };
    }

    pub fn open(self: *KvEngine) !void {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        traceMdbxBegin("open", self.path);
        var geometry = lmdbx.geometryFromEnv();
        if (self.config.map_lower_mib) |value| geometry.lower = mibToBytes(value);
        if (self.config.map_now_mib) |value| geometry.now = mibToBytes(value);
        if (self.config.map_upper_mib) |value| geometry.upper = mibToBytes(value);
        if (self.config.map_growth_mib) |value| geometry.growth = mibToBytes(value);
        if (self.config.map_shrink_mib) |value| geometry.shrink = mibToBytes(value);
        if (geometry.now < geometry.lower or geometry.upper < geometry.now) return error.InvalidMapGeometry;
        self.db = try lmdbx.Database.openWithGeometry(self.path, geometry);
        traceMdbxEnd("open", self.path);
        if (self.config.auto_compact_on_open) self.autoCompact();
    }

    fn autoCompact(self: *KvEngine) void {
        var db = self.db orelse return;
        const ratio = db.utilization() orelse return;
        if (ratio >= 0.5) return;
        std.log.info("kv auto-compact {s}: utilization {d:.0}%, compacting...", .{
            self.path, ratio * 100,
        });
        traceMdbxBegin("auto-compact", self.path);
        db.compact(self.allocator, self.path) catch |e| {
            std.log.err("kv auto-compact {s} failed: {}", .{ self.path, e });
            return;
        };
        traceMdbxEnd("auto-compact", self.path);
        self.db = db;
        std.log.info("kv auto-compact {s}: done", .{self.path});
    }

    pub fn close(self: *KvEngine) void {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        if (self.db) |*db| {
            traceMdbxBegin("close", self.path);
            db.close();
            traceMdbxEnd("close", self.path);
            self.db = null;
        }
    }

    pub fn put(self: *KvEngine, key: []const u8, value: []const u8, tel: *Telemetry) !void {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        var db = self.db orelse return error.NotOpen;
        const traced = traceMdbxWriteOps();
        if (traced) traceMdbxBegin("put", self.path);
        try db.put(key, value);
        if (traced) traceMdbxEnd("put", self.path);
        tel.addWrite(value.len);
    }

    pub fn get(self: *KvEngine, key: []const u8, tel: *Telemetry) !?[]u8 {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        var db = self.db orelse return error.NotOpen;
        const result = try db.get(self.allocator, key);
        if (result) |data| {
            tel.addRead(data.len);
        }
        return result;
    }

    pub fn delete(self: *KvEngine, key: []const u8, tel: *Telemetry) !void {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        var db = self.db orelse return error.NotOpen;
        const traced = traceMdbxWriteOps();
        if (traced) traceMdbxBegin("delete", self.path);
        try db.delete(key);
        if (traced) traceMdbxEnd("delete", self.path);
        tel.op_count += 1;
    }

    pub fn hasKey(self: *KvEngine, key: []const u8) !bool {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        var db = self.db orelse return error.NotOpen;
        return try db.hasKey(key);
    }

    pub const Pair = struct {
        key: []const u8,
        value: []const u8,
    };

    /// Prefix scan — one LMDB pass returning key+value pairs.
    /// Caller owns the returned slice and each pair's key/value and must free them.
    pub fn getRange(self: *KvEngine, prefix: []const u8, tel: *Telemetry) ![]Pair {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        var db = self.db orelse return error.NotOpen;
        var cursor = try db.openCursor();
        defer lmdbx.Database.closeCursor(cursor);

        var pairs: std.ArrayList(Pair) = .empty;
        errdefer {
            for (pairs.items) |p| {
                self.allocator.free(p.key);
                self.allocator.free(p.value);
            }
            pairs.deinit(self.allocator);
        }

        var entry = try cursor.seekPrefix(self.allocator, prefix);
        while (entry) |e| {
            if (e.key.len < prefix.len or !std.mem.eql(u8, e.key[0..prefix.len], prefix)) {
                self.allocator.free(e.key);
                self.allocator.free(e.value);
                break;
            }
            tel.addRead(e.value.len);
            try pairs.append(self.allocator, .{ .key = e.key, .value = e.value });
            entry = try cursor.next(self.allocator);
        }

        return pairs.toOwnedSlice(self.allocator);
    }

    pub fn flush(self: *KvEngine) !void {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        var db = self.db orelse return error.NotOpen;
        const traced = traceMdbxWriteOps();
        if (traced) traceMdbxBegin("flush", self.path);
        try db.flush();
        if (traced) traceMdbxEnd("flush", self.path);
    }

    pub fn compact(self: *KvEngine) !void {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        var db = self.db orelse return error.NotOpen;
        traceMdbxBegin("compact", self.path);
        try db.compact(self.allocator, self.path);
        traceMdbxEnd("compact", self.path);
    }

    pub fn getSize(self: *KvEngine) !u64 {
        const file = try fs_compat.cwd().openFile(self.path, .{});
        defer file.close();
        const stat = try file.stat();
        return stat.size;
    }

    /// Returns bytes occupied by actual data in the mmap (used pages * page_size).
    pub fn getCacheBytes(self: *KvEngine) u64 {
        mdbx_mutex.lock();
        defer mdbx_mutex.unlock();

        var db = self.db orelse return 0;
        return db.usedBytes();
    }
};

fn mibToBytes(value: u64) isize {
    const bytes = std.math.mul(u64, value, mib) catch return std.math.maxInt(isize);
    return @intCast(@min(bytes, @as(u64, @intCast(std.math.maxInt(isize)))));
}
