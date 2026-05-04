const std = @import("std");
const fs_compat = @import("fs_compat.zig");
const sync_compat = @import("sync_compat.zig");
const Allocator = std.mem.Allocator;
const manifest_mod = @import("manifest.zig");
const Manifest = manifest_mod.Manifest;
const StoreType = manifest_mod.StoreType;
const Telemetry = @import("telemetry.zig").Telemetry;

const SqlEngine = @import("engines/sql.zig").SqlEngine;
const KvEngine = @import("engines/kv.zig").KvEngine;
const ColumnEngine = @import("engines/column.zig").ColumnEngine;
const VectorEngine = @import("engines/vector.zig").VectorEngine;
const FilesEngine = @import("engines/files.zig").FilesEngine;
const GraphEngine = @import("engines/graph.zig").GraphEngine;

fn milliTimestamp() i64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return @intCast(ts.sec * 1000 + @divTrunc(ts.nsec, std.time.ns_per_ms));
}

fn unixTimestamp() i64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return @intCast(ts.sec);
}

pub const StoreHandle = union(StoreType) {
    sql: SqlEngine,
    kv: KvEngine,
    column: ColumnEngine,
    vector: VectorEngine,
    files: FilesEngine,
    graph: GraphEngine,
};

pub const StoreInstance = struct {
    handle: StoreHandle,
    manifest: Manifest,
    manifest_path: []const u8,
    data_path_z: ?[:0]u8,
    store_dir: []const u8,
    rwlock: sync_compat.RwLock = .{},
};

pub const DumpEntry = struct {
    name: []u8,
    size: u64,
};

fn isSafeFileName(name: []const u8) bool {
    if (name.len == 0 or name.len > 255) return false;
    for (name) |c| {
        if (c == '/' or c == '\\' or c == 0) return false;
    }
    if (std.mem.eql(u8, name, "..") or std.mem.eql(u8, name, ".")) return false;
    return true;
}

fn sanitizeNameAlloc(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const result = try allocator.dupe(u8, name);
    for (result) |*c| {
        if (c.* == '/' or c.* == '\\' or c.* == ' ') c.* = '_';
    }
    return result;
}

fn writeTarEntry(writer: anytype, name: []const u8, data: []const u8) !void {
    var header = std.mem.zeroes([512]u8);

    const name_len = @min(name.len, 99);
    @memcpy(header[0..name_len], name[0..name_len]);

    _ = std.fmt.bufPrint(header[100..108], "0000644\x00", .{}) catch {};
    _ = std.fmt.bufPrint(header[108..116], "0000000\x00", .{}) catch {};
    _ = std.fmt.bufPrint(header[116..124], "0000000\x00", .{}) catch {};
    _ = std.fmt.bufPrint(header[124..136], "{o:0>11}\x00", .{data.len}) catch {};

    const mtime: u64 = @intCast(unixTimestamp());
    _ = std.fmt.bufPrint(header[136..148], "{o:0>11}\x00", .{mtime}) catch {};

    @memset(header[148..156], ' ');
    header[156] = '0';
    @memcpy(header[257..263], "ustar\x00");
    @memcpy(header[263..265], "00");

    var checksum: u32 = 0;
    for (header) |b| checksum += b;
    _ = std.fmt.bufPrint(header[148..155], "{o:0>6}\x00", .{checksum}) catch {};
    header[155] = ' ';

    try writer.writeAll(&header);
    try writer.writeAll(data);

    const remainder = data.len % 512;
    if (remainder != 0) {
        const pad = std.mem.zeroes([512]u8);
        try writer.writeAll(pad[0 .. 512 - remainder]);
    }
}

fn writeTarEnd(writer: anytype) !void {
    const zeros = std.mem.zeroes([1024]u8);
    try writer.writeAll(&zeros);
}

const GzipWriter = struct {
    const Flate = std.compress.flate;

    file: fs_compat.File,
    file_writer: std.Io.File.Writer,
    compressor: Flate.Compress,
    file_buf: [4096]u8,
    comp_buf: [Flate.max_window_len]u8,

    fn initGzip(self: *GzipWriter, file: fs_compat.File) !void {
        self.* = .{
            .file = file,
            .file_writer = undefined,
            .compressor = undefined,
            .file_buf = undefined,
            .comp_buf = undefined,
        };
        self.file_writer = file.writer(&self.file_buf);
        self.compressor = try Flate.Compress.init(
            &self.file_writer.interface,
            &self.comp_buf,
            .gzip,
            .default,
        );
    }

    fn writeAll(self: *GzipWriter, data: []const u8) !void {
        try self.compressor.writer.writeAll(data);
    }

    fn finishGzip(self: *GzipWriter) !void {
        try self.compressor.finish();
        try self.file_writer.interface.flush();
    }
};

const FileWriter = struct {
    file: fs_compat.File,

    pub fn writeAll(self: *FileWriter, data: []const u8) !void {
        try self.file.writeAll(data);
    }
};

pub const StorageCommands = struct {
    stores: std.StringHashMap(StoreInstance),
    data_dir: []const u8,
    allocator: Allocator,

    pub fn init(allocator: Allocator, data_dir: []const u8) StorageCommands {
        return .{
            .stores = std.StringHashMap(StoreInstance).init(allocator),
            .data_dir = data_dir,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *StorageCommands) void {
        var it = self.stores.iterator();
        while (it.next()) |entry| {
            std.debug.print("storage closing {s}\n", .{entry.key_ptr.*});
            switch (entry.value_ptr.handle) {
                .sql => |*e| e.close(),
                .kv => |*e| e.close(),
                .column => |*e| e.close(),
                .vector => |*e| e.close(),
                .files => |*e| e.close(),
                .graph => |*e| e.close(),
            }
            if (entry.value_ptr.data_path_z) |path_z| self.allocator.free(path_z);
            entry.value_ptr.manifest.deinit();
            self.allocator.free(entry.value_ptr.manifest_path);
            self.allocator.free(entry.value_ptr.store_dir);
            self.allocator.free(entry.key_ptr.*);
        }
        self.stores.deinit();
    }

    // ── Store lifecycle ──

    pub fn openStore(self: *StorageCommands, ms_name: []const u8, store_name: []const u8, store_type: StoreType) !void {
        const store_dir = try std.fmt.allocPrint(self.allocator, "{s}/{s}/{s}", .{ self.data_dir, ms_name, store_name });
        defer self.allocator.free(store_dir);
        const store_key = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ ms_name, store_name });
        errdefer self.allocator.free(store_key);

        if (self.stores.contains(store_key)) {
            self.allocator.free(store_key);
            return;
        }

        const store_dir_owned = try self.allocator.dupe(u8, store_dir);
        errdefer self.allocator.free(store_dir_owned);

        // data/ subfolder always exists next to manifest.json
        const data_dir = try std.fmt.allocPrint(self.allocator, "{s}/data", .{store_dir});
        defer self.allocator.free(data_dir);

        try fs_compat.cwd().makePath(data_dir);

        const mfst = try manifest_mod.ensureManifest(self.allocator, store_dir, store_name, store_type);
        errdefer {
            var manifest = mfst;
            manifest.deinit();
        }

        var data_path_z: ?[:0]u8 = null;
        var handle: StoreHandle = undefined;

        switch (store_type) {
            .sql, .column, .vector => {
                const path_tmp = try std.fmt.allocPrint(self.allocator, "{s}/data.db", .{data_dir});
                defer self.allocator.free(path_tmp);
                const path_z = try self.allocator.dupeZ(u8, path_tmp);
                data_path_z = path_z;
                switch (store_type) {
                    .sql => handle = .{ .sql = SqlEngine.init(self.allocator, path_z) },
                    .column => handle = .{ .column = ColumnEngine.init(self.allocator, path_z) },
                    .vector => handle = .{ .vector = VectorEngine.init(self.allocator, path_z) },
                    else => unreachable,
                }
            },
            .kv => {
                const path_z = try self.allocator.dupeZ(u8, data_dir);
                data_path_z = path_z;
                handle = .{ .kv = KvEngine.init(self.allocator, path_z) };
            },
            .files => {
                const path_z = try self.allocator.dupeZ(u8, data_dir);
                data_path_z = path_z;
                handle = .{ .files = FilesEngine.init(self.allocator, path_z) };
            },
            .graph => {
                const path_tmp = try std.fmt.allocPrint(self.allocator, "{s}/graph.db", .{data_dir});
                defer self.allocator.free(path_tmp);
                const path_z = try self.allocator.dupeZ(u8, path_tmp);
                data_path_z = path_z;
                handle = .{ .graph = GraphEngine.init(self.allocator, path_z) };
            },
        }

        errdefer if (data_path_z) |path_z| self.allocator.free(path_z);

        switch (handle) {
            .sql => |*e| try e.open(),
            .kv => |*e| try e.open(),
            .column => |*e| try e.open(),
            .vector => |*e| try e.open(),
            .files => |*e| try e.open(),
            .graph => |*e| try e.open(),
        }

        const manifest_path = try std.fmt.allocPrint(self.allocator, "{s}/manifest.json", .{store_dir});
        errdefer self.allocator.free(manifest_path);

        std.debug.print("storage opened {s} type={s}\n", .{ store_key, store_type.toString() });

        try self.stores.put(store_key, .{
            .handle = handle,
            .manifest = mfst,
            .manifest_path = manifest_path,
            .data_path_z = data_path_z,
            .store_dir = store_dir_owned,
        });
    }

    pub fn closeStore(self: *StorageCommands, store_key: []const u8) void {
        if (self.stores.fetchRemove(store_key)) |kv| {
            std.debug.print("storage closed {s}\n", .{kv.key});
            self.allocator.free(kv.key);
            var inst = kv.value;
            switch (inst.handle) {
                .sql => |*e| e.close(),
                .kv => |*e| e.close(),
                .column => |*e| e.close(),
                .vector => |*e| e.close(),
                .files => |*e| e.close(),
                .graph => |*e| e.close(),
            }
            if (inst.data_path_z) |path_z| self.allocator.free(path_z);
            inst.manifest.deinit();
            self.allocator.free(inst.manifest_path);
            self.allocator.free(inst.store_dir);
        }
    }

    // ── Migration state ──

    pub fn recordMigration(self: *StorageCommands, store_key: []const u8, migration_id: []const u8) !void {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        if (inst.manifest.hasMigration(migration_id)) return;
        try inst.manifest.addMigration(migration_id);
        try inst.manifest.save(inst.manifest_path);
    }

    // ── SQL/Cypher exec/query (sql, column, vector, graph) ──

    pub fn execSql(self: *StorageCommands, store_key: []const u8, sql: [*:0]const u8) !void {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        switch (inst.handle) {
            .sql => |*e| try e.execSql(sql),
            .column => |*e| try e.execSql(sql),
            .vector => |*e| try e.execSql(sql),
            .graph => |*e| try e.execSql(sql),
            else => return error.UnsupportedOperation,
        }
    }

    pub fn querySql(self: *StorageCommands, store_key: []const u8, sql: [*:0]const u8, tel: *Telemetry) ![]u8 {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        return switch (inst.handle) {
            .sql => |*e| try e.queryJson(self.allocator, sql, tel),
            .column => |*e| try e.queryJson(self.allocator, sql, tel),
            .vector => |*e| try e.queryJson(self.allocator, sql, tel),
            .graph => |*e| try e.queryJson(self.allocator, sql, tel),
            else => return error.UnsupportedOperation,
        };
    }

    // ── KV operations ──

    pub fn kvPut(self: *StorageCommands, store_key: []const u8, key: []const u8, value: []const u8, tel: *Telemetry) !void {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        switch (inst.handle) {
            .kv => |*e| try e.put(key, value, tel),
            else => return error.UnsupportedOperation,
        }
    }

    pub fn kvGet(self: *StorageCommands, store_key: []const u8, key: []const u8, tel: *Telemetry) !?[]u8 {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        return switch (inst.handle) {
            .kv => |*e| try e.get(key, tel),
            else => return error.UnsupportedOperation,
        };
    }

    pub fn kvGetRange(self: *StorageCommands, store_key: []const u8, prefix: []const u8, tel: *Telemetry) ![]KvEngine.Pair {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        return switch (inst.handle) {
            .kv => |*e| try e.getRange(prefix, tel),
            else => return error.UnsupportedOperation,
        };
    }

    pub fn kvDelete(self: *StorageCommands, store_key: []const u8, key: []const u8, tel: *Telemetry) !void {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        switch (inst.handle) {
            .kv => |*e| try e.delete(key, tel),
            else => return error.UnsupportedOperation,
        }
    }

    // ── File operations ──

    pub fn filePut(self: *StorageCommands, store_key: []const u8, key: []const u8, data: []const u8, tel: *Telemetry) !void {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        switch (inst.handle) {
            .files => |*e| try e.put(key, data, tel),
            else => return error.UnsupportedOperation,
        }
    }

    pub fn fileGet(self: *StorageCommands, store_key: []const u8, key: []const u8, tel: *Telemetry) !?[]u8 {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        return switch (inst.handle) {
            .files => |*e| try e.get(key, tel),
            else => return error.UnsupportedOperation,
        };
    }

    pub fn fileDelete(self: *StorageCommands, store_key: []const u8, key: []const u8, tel: *Telemetry) !bool {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        return switch (inst.handle) {
            .files => |*e| try e.delete(key, tel),
            else => return error.UnsupportedOperation,
        };
    }

    // ── Compact ──

    pub fn kvCompact(self: *StorageCommands, store_key: []const u8) !void {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        switch (inst.handle) {
            .kv => |*e| try e.compact(),
            else => return error.UnsupportedOperation,
        }
    }

    // ── Info ──

    pub fn getStoreType(self: *StorageCommands, store_key: []const u8) ?StoreType {
        const inst = self.stores.getPtr(store_key) orelse return null;
        return std.meta.activeTag(inst.handle);
    }

    pub fn getStoreSize(self: *StorageCommands, store_key: []const u8) !u64 {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;
        const data_path = try std.fmt.allocPrint(self.allocator, "{s}/data", .{inst.store_dir});
        defer self.allocator.free(data_path);

        var dir = fs_compat.cwd().openDir(data_path, .{ .iterate = true }) catch return 0;
        defer dir.close();

        var total: u64 = 0;
        var walker = try dir.walk(self.allocator);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (entry.kind == .file) {
                const stat = dir.statFile(entry.path) catch continue;
                total += stat.size;
            }
        }
        return total;
    }

    pub fn getManifest(self: *StorageCommands, store_key: []const u8) ?*const Manifest {
        const inst = self.stores.getPtr(store_key) orelse return null;
        return &inst.manifest;
    }

    // ── Archive ──

    pub fn createArchive(self: *StorageCommands, store_key: []const u8, output_path: []const u8) !void {
        const inst = self.stores.getPtr(store_key) orelse return error.StoreNotFound;

        const result = try std.process.run(self.allocator, fs_compat.getIo(), .{
            .argv = &[_][]const u8{ "tar", "czf", output_path, "-C", inst.store_dir, "." },
        });
        self.allocator.free(result.stdout);
        self.allocator.free(result.stderr);
    }

    // ── Dump operations ──

    pub fn createDump(self: *StorageCommands, ms_name: []const u8, store_name: []const u8) ![]u8 {
        const data_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}/{s}/data", .{ self.data_dir, ms_name, store_name });
        defer self.allocator.free(data_path);

        const dumps_dir_path = try std.fmt.allocPrint(self.allocator, "{s}/dumps", .{self.data_dir});
        defer self.allocator.free(dumps_dir_path);
        try fs_compat.cwd().makePath(dumps_dir_path);

        const ts = milliTimestamp();
        const safe_ms = try sanitizeNameAlloc(self.allocator, ms_name);
        defer self.allocator.free(safe_ms);
        const safe_store = try sanitizeNameAlloc(self.allocator, store_name);
        defer self.allocator.free(safe_store);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}_{s}_{d}.tar.gz", .{ safe_ms, safe_store, ts });
        errdefer self.allocator.free(file_name);

        const output_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ dumps_dir_path, file_name });
        defer self.allocator.free(output_path);

        const out_file = try fs_compat.cwd().createFile(output_path, .{});
        defer out_file.close();

        var gz: GzipWriter = undefined;
        try gz.initGzip(out_file);

        var data_dir = fs_compat.cwd().openDir(data_path, .{ .iterate = true }) catch {
            gz.finishGzip() catch {};
            return file_name;
        };
        defer data_dir.close();

        var walker = try data_dir.walk(self.allocator);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (entry.kind != .file) continue;
            const file_data = data_dir.readFileAlloc(self.allocator, entry.path, 2 * 1024 * 1024 * 1024) catch continue;
            defer self.allocator.free(file_data);
            try writeTarEntry(&gz, entry.path, file_data);
        }

        try writeTarEnd(&gz);
        try gz.finishGzip();

        return file_name;
    }

    pub fn listDumps(self: *StorageCommands) ![]DumpEntry {
        const dumps_dir_path = try std.fmt.allocPrint(self.allocator, "{s}/dumps", .{self.data_dir});
        defer self.allocator.free(dumps_dir_path);

        var dir = fs_compat.cwd().openDir(dumps_dir_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return self.allocator.alloc(DumpEntry, 0),
            else => return err,
        };
        defer dir.close();

        var entries: std.ArrayList(DumpEntry) = .empty;
        errdefer {
            for (entries.items) |e| self.allocator.free(e.name);
            entries.deinit(self.allocator);
        }

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind != .file) continue;
            const stat = dir.statFile(entry.name) catch continue;
            const name = try self.allocator.dupe(u8, entry.name);
            try entries.append(self.allocator, .{ .name = name, .size = stat.size });
        }

        return entries.toOwnedSlice(self.allocator);
    }

    pub fn deleteDump(self: *StorageCommands, file_name: []const u8) !void {
        if (!isSafeFileName(file_name)) return error.InvalidName;
        const path = try std.fmt.allocPrint(self.allocator, "{s}/dumps/{s}", .{ self.data_dir, file_name });
        defer self.allocator.free(path);
        try fs_compat.cwd().deleteFile(path);
    }

    pub fn readDump(self: *StorageCommands, file_name: []const u8, offset: u64, length: u32) ![]u8 {
        if (!isSafeFileName(file_name)) return error.InvalidName;
        const path = try std.fmt.allocPrint(self.allocator, "{s}/dumps/{s}", .{ self.data_dir, file_name });
        defer self.allocator.free(path);

        const file = try fs_compat.cwd().openFile(path, .{});
        defer file.close();

        try file.seekTo(offset);
        const buf = try self.allocator.alloc(u8, length);
        errdefer self.allocator.free(buf);
        const n = try file.read(buf);
        return buf[0..n];
    }
};
