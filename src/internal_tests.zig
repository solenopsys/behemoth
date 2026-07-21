const std = @import("std");
const commands_mod = @import("commands.zig");
const manifest_mod = @import("manifest.zig");
const telemetry_mod = @import("telemetry.zig");
const sync_compat = @import("sync_compat.zig");
const fs_compat = @import("fs_compat.zig");
const threads_mod = @import("threads.zig");

const StorageCommands = commands_mod.StorageCommands;
const StoreType = manifest_mod.StoreType;
const Telemetry = telemetry_mod.Telemetry;
const ThreadPool = threads_mod.ThreadPool;
const WorkItem = threads_mod.WorkItem;
const KvEngine = @import("engines/kv.zig").KvEngine;

const Iterations = 120;

const SqlCtx = struct {
    commands: *StorageCommands,
    failed: *std.atomic.Value(bool),
};

const KvCtx = struct {
    commands: *StorageCommands,
    failed: *std.atomic.Value(bool),
};

const FilesCtx = struct {
    commands: *StorageCommands,
    failed: *std.atomic.Value(bool),
};

const BlockCtx = struct {
    entered: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    release: sync_compat.ResetEvent = .{},
};

const MarkerCtx = struct {
    done: *std.atomic.Value(bool),
};

fn blockExec(ctx_ptr: ?*anyopaque) void {
    const ctx: *BlockCtx = @ptrCast(@alignCast(ctx_ptr.?));
    ctx.entered.store(true, .seq_cst);
    ctx.release.wait();
}

fn markExec(ctx_ptr: ?*anyopaque) void {
    const ctx: *MarkerCtx = @ptrCast(@alignCast(ctx_ptr.?));
    ctx.done.store(true, .seq_cst);
}

fn prepareDataDir(tmp: *std.testing.TmpDir, path_buf: []u8) ![]const u8 {
    try tmp.dir.createDir(std.testing.io, "data", .default_dir);
    const len = try tmp.dir.realPathFile(std.testing.io, "data", path_buf);
    return path_buf[0..len];
}

fn fillValue(buf: []u8, seed: usize) void {
    for (buf, 0..) |*b, i| {
        b.* = @intCast((seed + i) % 251);
    }
}

fn expectPathMissing(path: []const u8) !void {
    try std.testing.expectError(error.FileNotFound, fs_compat.cwd().access(path, .{}));
}

test "open is idempotent and store can close/reopen" {
    fs_compat.setIo(std.testing.io);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir = try prepareDataDir(&tmp, &path_buf);

    var commands = StorageCommands.init(std.testing.allocator, data_dir);
    defer commands.deinit();

    try commands.openStore("assistant-ms", "metadata", StoreType.sql);
    try std.testing.expectEqual(@as(usize, 1), commands.stores.count());

    // Same store open must be a no-op.
    try commands.openStore("assistant-ms", "metadata", StoreType.sql);
    try std.testing.expectEqual(@as(usize, 1), commands.stores.count());

    try commands.execSql("assistant-ms/metadata", "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)");
    try commands.execSql("assistant-ms/metadata", "INSERT INTO t(id, v) VALUES (1, 'ok')");

    commands.closeStore("assistant-ms/metadata");
    try std.testing.expectEqual(@as(usize, 0), commands.stores.count());
    try std.testing.expectError(error.StoreNotFound, commands.execSql("assistant-ms/metadata", "SELECT 1"));

    try commands.openStore("assistant-ms", "metadata", StoreType.sql);
    var tel = Telemetry.begin();
    const json = try commands.querySql("assistant-ms/metadata", "SELECT COUNT(*) AS count FROM t", &tel);
    defer std.testing.allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"count\":1") != null);
}

test "open rejects type mismatch for existing manifest or open store" {
    fs_compat.setIo(std.testing.io);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir = try prepareDataDir(&tmp, &path_buf);

    var commands = StorageCommands.init(std.testing.allocator, data_dir);
    defer commands.deinit();

    try commands.openStore("sheduller-ms", "crons", StoreType.sql);
    try std.testing.expectError(error.StoreTypeMismatch, commands.openStore("sheduller-ms", "crons", StoreType.files));

    commands.closeStore("sheduller-ms/crons");
    try std.testing.expectError(error.StoreTypeMismatch, commands.openStore("sheduller-ms", "crons", StoreType.files));

    try commands.openStore("sheduller-ms", "crons", StoreType.sql);
}

test "error in one store type does not break others" {
    fs_compat.setIo(std.testing.io);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir = try prepareDataDir(&tmp, &path_buf);

    var commands = StorageCommands.init(std.testing.allocator, data_dir);
    defer commands.deinit();

    try commands.openStore("sql-ms", "sql", StoreType.sql);
    try commands.openStore("kv-ms", "kv", StoreType.kv);
    try commands.execSql("sql-ms/sql", "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)");

    var tel = Telemetry.begin();
    try std.testing.expectError(error.UnsupportedOperation, commands.execSql("kv-ms/kv", "SELECT 1"));
    try std.testing.expectError(error.UnsupportedOperation, commands.kvPut("sql-ms/sql", "k", "v", &tel));

    // KV still operational after sql/kv mismatch errors.
    try commands.kvPut("kv-ms/kv", "k1", "v1", &tel);
    const kv_data = try commands.kvGet("kv-ms/kv", "k1", &tel);
    try std.testing.expect(kv_data != null);
    defer std.testing.allocator.free(kv_data.?);
    try std.testing.expectEqualStrings("v1", kv_data.?);

    // SQL still operational after mismatch errors.
    try commands.execSql("sql-ms/sql", "INSERT INTO t(id, v) VALUES (1, 'ok')");
    const sql_data = try commands.querySql("sql-ms/sql", "SELECT COUNT(*) AS count FROM t", &tel);
    defer std.testing.allocator.free(sql_data);
    try std.testing.expect(std.mem.indexOf(u8, sql_data, "\"count\":1") != null);
}

test "kv churn compact and reopen preserves live values" {
    fs_compat.setIo(std.testing.io);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir = try prepareDataDir(&tmp, &path_buf);
    const data_dir_z = try std.testing.allocator.dupeZ(u8, data_dir);
    defer std.testing.allocator.free(data_dir_z);

    var kv = KvEngine.init(std.testing.allocator, data_dir_z);
    try kv.open();
    defer kv.close();

    var tel = Telemetry.begin();
    var value: [4096]u8 = undefined;
    var expected: [4096]u8 = undefined;

    for (0..160) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "churn-key-{d}", .{i});
        fillValue(&value, i);
        try kv.put(key, &value, &tel);
    }

    for (0..160) |i| {
        if (i % 3 != 0) continue;
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "churn-key-{d}", .{i});
        try kv.delete(key, &tel);
    }

    for (0..80) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "replacement-key-{d}", .{i});
        fillValue(&value, i + 10_000);
        try kv.put(key, &value, &tel);
    }

    try kv.flush();
    try kv.compact();

    const bak_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/mdbx.dat.bak", .{data_dir});
    defer std.testing.allocator.free(bak_path);
    try expectPathMissing(bak_path);

    kv.close();

    try kv.open();
    for (0..160) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "churn-key-{d}", .{i});
        const got = try kv.get(key, &tel);
        if (i % 3 == 0) {
            try std.testing.expect(got == null);
            continue;
        }
        try std.testing.expect(got != null);
        defer std.testing.allocator.free(got.?);
        fillValue(&expected, i);
        try std.testing.expectEqualSlices(u8, &expected, got.?);
    }

    for (0..80) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "replacement-key-{d}", .{i});
        const got = try kv.get(key, &tel);
        try std.testing.expect(got != null);
        defer std.testing.allocator.free(got.?);
        fillValue(&expected, i + 10_000);
        try std.testing.expectEqualSlices(u8, &expected, got.?);
    }
}

test "kv open recovers interrupted compact backup when data file is missing" {
    fs_compat.setIo(std.testing.io);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir = try prepareDataDir(&tmp, &path_buf);
    const data_dir_z = try std.testing.allocator.dupeZ(u8, data_dir);
    defer std.testing.allocator.free(data_dir_z);

    var kv = KvEngine.init(std.testing.allocator, data_dir_z);
    try kv.open();

    var tel = Telemetry.begin();
    try kv.put("sentinel", "survives-interrupted-compact", &tel);
    try kv.flush();
    kv.close();

    const dat_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/mdbx.dat", .{data_dir});
    defer std.testing.allocator.free(dat_path);
    const bak_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/mdbx.dat.bak", .{data_dir});
    defer std.testing.allocator.free(bak_path);

    try std.Io.Dir.renameAbsolute(dat_path, bak_path, std.testing.io);
    try expectPathMissing(dat_path);

    try kv.open();
    defer kv.close();

    const got = try kv.get("sentinel", &tel);
    try std.testing.expect(got != null);
    defer std.testing.allocator.free(got.?);
    try std.testing.expectEqualStrings("survives-interrupted-compact", got.?);
    try expectPathMissing(bak_path);
}

const ConcurrentKvCtx = struct {
    engine: *KvEngine,
    id: usize,
    failed: *std.atomic.Value(bool),
};

fn concurrentKvEngineWorker(ctx: *ConcurrentKvCtx) void {
    var tel = Telemetry.begin();
    var value: [2048]u8 = undefined;
    var expected: [2048]u8 = undefined;

    for (0..700) |i| {
        var key_buf: [64]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "worker-{d}-key-{d}", .{ ctx.id, i % 160 }) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };

        fillValue(&value, ctx.id * 10_000 + i);
        ctx.engine.put(key, &value, &tel) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };

        const got = ctx.engine.get(key, &tel) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };
        if (got) |data| {
            defer std.testing.allocator.free(data);
            fillValue(&expected, ctx.id * 10_000 + i);
            if (!std.mem.eql(u8, data, &expected)) {
                ctx.failed.store(true, .seq_cst);
                return;
            }
        } else {
            ctx.failed.store(true, .seq_cst);
            return;
        }

        if (i % 5 == 0) {
            var old_key_buf: [64]u8 = undefined;
            const old_key = std.fmt.bufPrint(&old_key_buf, "worker-{d}-key-{d}", .{ ctx.id, (i + 37) % 160 }) catch {
                ctx.failed.store(true, .seq_cst);
                return;
            };
            ctx.engine.delete(old_key, &tel) catch |err| switch (err) {
                error.NotFound => {},
                else => {
                    ctx.failed.store(true, .seq_cst);
                    return;
                },
            };
        }
    }
}

test "kv engines survive concurrent churn across stores" {
    fs_compat.setIo(std.testing.io);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir = try prepareDataDir(&tmp, &path_buf);

    const WorkerCount = 8;
    var engines: [WorkerCount]KvEngine = undefined;
    var paths: [WorkerCount][:0]u8 = undefined;
    var opened: usize = 0;
    var allocated_paths: usize = 0;
    defer {
        for (0..opened) |i| engines[i].close();
        for (0..allocated_paths) |i| std.testing.allocator.free(paths[i]);
    }

    for (0..WorkerCount) |i| {
        const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/kv-{d}", .{ data_dir, i });
        defer std.testing.allocator.free(path);
        try fs_compat.cwd().makePath(path);
        paths[i] = try std.testing.allocator.dupeZ(u8, path);
        allocated_paths += 1;
        engines[i] = KvEngine.init(std.testing.allocator, paths[i]);
        try engines[i].open();
        opened += 1;
    }

    var failed = std.atomic.Value(bool).init(false);
    var ctxs: [WorkerCount]ConcurrentKvCtx = undefined;
    var threads: [WorkerCount]std.Thread = undefined;

    for (0..WorkerCount) |i| {
        ctxs[i] = .{ .engine = &engines[i], .id = i, .failed = &failed };
        threads[i] = try std.Thread.spawn(.{}, concurrentKvEngineWorker, .{&ctxs[i]});
    }
    for (&threads) |thread| thread.join();

    try std.testing.expect(!failed.load(.seq_cst));
}

fn sqlWorker(store_key: []const u8, table_name: []const u8, ctx: *SqlCtx) void {
    var i: usize = 0;
    while (i < Iterations) : (i += 1) {
        var sql_buf: [192]u8 = undefined;
        const stmt = std.fmt.bufPrintZ(
            &sql_buf,
            "INSERT INTO {s}(id, value) VALUES ({d}, 'v-{d}')",
            .{ table_name, i, i },
        ) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };
        ctx.commands.execSql(store_key, stmt) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };
    }
}

fn kvWorker(ctx: *KvCtx) void {
    var i: usize = 0;
    while (i < Iterations) : (i += 1) {
        var key_buf: [64]u8 = undefined;
        var val_buf: [64]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "k-{d}", .{i}) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };
        const value = std.fmt.bufPrint(&val_buf, "v-{d}", .{i}) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };

        var tel = Telemetry.begin();
        ctx.commands.kvPut("kv-ms/kv", key, value, &tel) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };

        const got = ctx.commands.kvGet("kv-ms/kv", key, &tel) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };
        if (got) |data| {
            defer std.testing.allocator.free(data);
            if (!std.mem.eql(u8, data, value)) {
                ctx.failed.store(true, .seq_cst);
                return;
            }
        } else {
            ctx.failed.store(true, .seq_cst);
            return;
        }
    }
}

fn filesWorker(ctx: *FilesCtx) void {
    var i: usize = 0;
    while (i < Iterations) : (i += 1) {
        var key_buf: [64]u8 = undefined;
        var val_buf: [64]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "f-{d}.txt", .{i}) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };
        const value = std.fmt.bufPrint(&val_buf, "blob-{d}", .{i}) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };

        var tel = Telemetry.begin();
        ctx.commands.filePut("files-ms/files", key, value, &tel) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };

        const got = ctx.commands.fileGet("files-ms/files", key, &tel) catch {
            ctx.failed.store(true, .seq_cst);
            return;
        };
        if (got) |data| {
            defer std.testing.allocator.free(data);
            if (!std.mem.eql(u8, data, value)) {
                ctx.failed.store(true, .seq_cst);
                return;
            }
        } else {
            ctx.failed.store(true, .seq_cst);
            return;
        }
    }
}

test "store controllers process sql/kv/vector/files independently across threads" {
    fs_compat.setIo(std.testing.io);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir = try prepareDataDir(&tmp, &path_buf);

    var commands = StorageCommands.init(std.testing.allocator, data_dir);
    defer commands.deinit();

    try commands.openStore("sql-ms", "sql", StoreType.sql);
    try commands.openStore("kv-ms", "kv", StoreType.kv);
    try commands.openStore("vec-ms", "vec", StoreType.vector);
    try commands.openStore("files-ms", "files", StoreType.files);

    try commands.execSql("sql-ms/sql", "CREATE TABLE IF NOT EXISTS t_sql (id INTEGER PRIMARY KEY, value TEXT)");
    try commands.execSql("vec-ms/vec", "CREATE TABLE IF NOT EXISTS t_vec (id INTEGER PRIMARY KEY, value TEXT)");

    var failed = std.atomic.Value(bool).init(false);

    var sql_ctx = SqlCtx{ .commands = &commands, .failed = &failed };
    var vec_ctx = SqlCtx{ .commands = &commands, .failed = &failed };
    var kv_ctx = KvCtx{ .commands = &commands, .failed = &failed };
    var files_ctx = FilesCtx{ .commands = &commands, .failed = &failed };

    const sql_thread = try std.Thread.spawn(.{}, sqlWorker, .{ "sql-ms/sql", "t_sql", &sql_ctx });
    const vec_thread = try std.Thread.spawn(.{}, sqlWorker, .{ "vec-ms/vec", "t_vec", &vec_ctx });
    const kv_thread = try std.Thread.spawn(.{}, kvWorker, .{&kv_ctx});
    const files_thread = try std.Thread.spawn(.{}, filesWorker, .{&files_ctx});

    sql_thread.join();
    vec_thread.join();
    kv_thread.join();
    files_thread.join();

    try std.testing.expect(!failed.load(.seq_cst));

    var tel = Telemetry.begin();
    const sql_json = try commands.querySql("sql-ms/sql", "SELECT COUNT(*) as count FROM t_sql", &tel);
    defer std.testing.allocator.free(sql_json);
    try std.testing.expect(std.mem.indexOf(u8, sql_json, "\"count\":120") != null);

    const vec_json = try commands.querySql("vec-ms/vec", "SELECT COUNT(*) as count FROM t_vec", &tel);
    defer std.testing.allocator.free(vec_json);
    try std.testing.expect(std.mem.indexOf(u8, vec_json, "\"count\":120") != null);
}

const ConcWriteCtx = struct {
    commands: *StorageCommands,
    store_key: []const u8,
    id: usize,
    failed: *std.atomic.Value(bool),
};

fn concWriteExec(ctx_ptr: ?*anyopaque) void {
    const ctx: *ConcWriteCtx = @ptrCast(@alignCast(ctx_ptr.?));
    var buf: [128]u8 = undefined;
    const stmt = std.fmt.bufPrintZ(&buf, "INSERT INTO conc(v) VALUES ('w-{d}')", .{ctx.id}) catch {
        ctx.failed.store(true, .seq_cst);
        return;
    };
    ctx.commands.execSql(ctx.store_key, stmt) catch {
        ctx.failed.store(true, .seq_cst);
    };
}

test "multiple clients submit ops to same store via worker without per-store lock" {
    fs_compat.setIo(std.testing.io);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_dir = try prepareDataDir(&tmp, &path_buf);

    var commands = StorageCommands.init(std.testing.allocator, data_dir);
    defer commands.deinit();

    try commands.openStore("ms", "shared", StoreType.sql);
    try commands.execSql("ms/shared", "CREATE TABLE IF NOT EXISTS conc (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)");

    var pool = ThreadPool.init(std.testing.allocator);
    defer pool.deinit();
    try pool.start();
    defer pool.stop();

    const N = 20;
    var failed = std.atomic.Value(bool).init(false);
    var ctxs: [N]ConcWriteCtx = undefined;
    var items: [N]WorkItem = undefined;

    for (0..N) |i| {
        ctxs[i] = .{ .commands = &commands, .store_key = "ms/shared", .id = i, .failed = &failed };
        items[i] = .{
            .store_key = "ms/shared",
            .op = .exec_sql,
            .payload = &[_]u8{},
            .result = null,
            .done = .{},
            .err = null,
            .exec_ctx = &ctxs[i],
            .exec_fn = concWriteExec,
        };
    }

    // Simulate N clients concurrently submitting to the same sql worker
    const SubmitCtx = struct {
        pool: *ThreadPool,
        items_ptr: [*]WorkItem,
        start: usize,
        end: usize,

        fn run(self: *const @This()) void {
            for (self.start..self.end) |i| {
                self.pool.getWorker(.sql).submit(&self.items_ptr[i]);
            }
        }
    };
    const half = N / 2;
    var s1 = SubmitCtx{ .pool = &pool, .items_ptr = &items, .start = 0, .end = half };
    var s2 = SubmitCtx{ .pool = &pool, .items_ptr = &items, .start = half, .end = N };
    const t1 = try std.Thread.spawn(.{}, SubmitCtx.run, .{&s1});
    const t2 = try std.Thread.spawn(.{}, SubmitCtx.run, .{&s2});
    t1.join();
    t2.join();

    for (&items) |*item| {
        try item.done.timedWait(2 * std.time.ns_per_s);
    }

    try std.testing.expect(!failed.load(.seq_cst));

    var tel = Telemetry.begin();
    const json = try commands.querySql("ms/shared", "SELECT COUNT(*) as count FROM conc", &tel);
    defer std.testing.allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"count\":20") != null);
}

test "thread pool keeps store-type workers isolated" {
    fs_compat.setIo(std.testing.io);
    var pool = ThreadPool.init(std.testing.allocator);
    defer pool.deinit();
    try pool.start();
    defer pool.stop();

    var blocker = BlockCtx{};
    var sql_item = WorkItem{
        .store_key = "sql-ms/sql",
        .op = .exec_sql,
        .payload = &[_]u8{},
        .result = null,
        .done = .{},
        .err = null,
        .exec_ctx = &blocker,
        .exec_fn = blockExec,
    };
    pool.getWorker(.sql).submit(&sql_item);

    const enter_deadline_ms = 500;
    var waited_ms: usize = 0;
    while (!blocker.entered.load(.seq_cst) and waited_ms < enter_deadline_ms) : (waited_ms += 1) {
        sync_compat.sleep(std.time.ns_per_ms);
    }
    try std.testing.expect(blocker.entered.load(.seq_cst));

    var kv_done = std.atomic.Value(bool).init(false);
    var marker = MarkerCtx{ .done = &kv_done };
    var kv_item = WorkItem{
        .store_key = "kv-ms/kv",
        .op = .kv_get,
        .payload = &[_]u8{},
        .result = null,
        .done = .{},
        .err = null,
        .exec_ctx = &marker,
        .exec_fn = markExec,
    };
    pool.getWorker(.kv).submit(&kv_item);

    try kv_item.done.timedWait(200 * std.time.ns_per_ms);
    try std.testing.expect(kv_done.load(.seq_cst));
    try std.testing.expect(!sql_item.done.isSet());

    blocker.release.set();
    try sql_item.done.timedWait(200 * std.time.ns_per_ms);
    try std.testing.expect(sql_item.done.isSet());
}
