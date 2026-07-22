const std = @import("std");
const fs_compat = @import("fs_compat.zig");
const sync_compat = @import("sync_compat.zig");
const transport = @import("transport");
const cmds = @import("commands.zig");
const mfst = @import("manifest.zig");
const tel_mod = @import("telemetry.zig");
const threads_mod = @import("threads.zig");
const valkey_mod = @import("valkey.zig");
const management_mod = @import("management.zig");

const StorageCommands = cmds.StorageCommands;
const StoreType = mfst.StoreType;
const Telemetry = tel_mod.Telemetry;
const ThreadPool = threads_mod.ThreadPool;
const WorkItem = threads_mod.WorkItem;
const KvEngine = @import("engines/kv.zig").KvEngine;
const Allocator = std.mem.Allocator;
const ValkeyConfig = valkey_mod.Config;

// C API from capnp_wrap.cpp (compiled directly into the exe via build.zig)
const c = @cImport(@cInclude("transport.h"));

var shutdown_requested = std.atomic.Value(bool).init(false);
var cache_key_counter = std.atomic.Value(u64).init(0);
var stores_rwlock: sync_compat.RwLock = .{};
var pool: ThreadPool = undefined;

fn storeCmdName(cmd: c_uint) []const u8 {
    return switch (cmd) {
        c.REQ_KV_PUT => "kv-put",
        c.REQ_KV_PUT_FROM_CACHE => "kv-put-from-cache",
        c.REQ_KV_GET => "kv-get",
        c.REQ_KV_GET_TO_CACHE => "kv-get-to-cache",
        c.REQ_KV_DELETE => "kv-delete",
        c.REQ_KV_LIST => "kv-list",
        c.REQ_KV_COMPACT => "kv-compact",
        else => "other",
    };
}

fn traceStoreOp(cmd: c_uint) bool {
    return switch (cmd) {
        c.REQ_KV_PUT,
        c.REQ_KV_PUT_FROM_CACHE,
        c.REQ_KV_GET_TO_CACHE,
        c.REQ_KV_DELETE,
        c.REQ_KV_COMPACT,
        => true,
        else => false,
    };
}

/// Transport binding configuration — choose ZeroMQ IPC or TCP.
pub const BindConfig = union(enum) {
    unix: []const u8,
    tcp: struct { host: []const u8, port: u16 },
    endpoint: []const u8,
};

pub fn endpointForConfig(allocator: Allocator, cfg: BindConfig) ![:0]u8 {
    return switch (cfg) {
        .unix => |path| std.fmt.allocPrintSentinel(allocator, "ipc://{s}", .{path}, 0),
        .tcp => |tcp| std.fmt.allocPrintSentinel(allocator, "tcp://{s}:{d}", .{ tcp.host, tcp.port }, 0),
        .endpoint => |value| allocator.dupeZ(u8, value),
    };
}

pub fn start(allocator: Allocator, data_dir: []const u8, cfg: BindConfig, valkey_cfg: ValkeyConfig) !void {
    try fs_compat.cwd().makePath(data_dir);
    installSignalHandlers();
    shutdown_requested.store(false, .seq_cst);

    try valkey_mod.start(allocator, data_dir, valkey_cfg);
    defer valkey_mod.stop(valkey_cfg);

    const endpoint = try endpointForConfig(allocator, cfg);
    defer allocator.free(endpoint);
    const target = posix_compat.getenv("FUJIN_TARGET") orelse posix_compat.getenv("BEHEMOTH_FUJIN_TARGET") orelse "behemoth";
    var service = try transport.Service.init(allocator, .{
        .endpoint = endpoint,
        .identity = target,
        .target = target,
        .shared = false,
        .services_json = "[{\"name\":\"storage\",\"methods\":[]}]",
        .limits = .{ .max_envelope_bytes = 64 * 1024, .max_payload_bytes = 16 * 1024 * 1024 },
        .recv_timeout_ms = 25,
        .send_timeout_ms = 1000,
    });
    defer service.deinit();

    var management = try management_mod.initFromEnv(allocator);
    defer if (management) |*controller| controller.deinit();
    if (management) |*controller| {
        std.log.info("storage JS management interface loaded from {s}", .{controller.script_path});
    }

    var commands = StorageCommands.initWithManagement(allocator, data_dir, if (management) |*controller| controller else null);
    defer commands.deinit();
    try autoOpenStores(allocator, &commands, &service, data_dir);

    pool = ThreadPool.init(allocator);
    try pool.start();
    defer {
        pool.stop();
        pool.deinit();
    }

    std.debug.print("storage service connected to Fujin at {s}\n", .{endpoint});
    if (valkey_cfg.enabled) {
        std.debug.print("storage valkey listening on tcp:{s}:{d}\n", .{ valkey_cfg.host, valkey_cfg.port });
    }

    while (true) {
        if (shutdown_requested.load(.seq_cst)) {
            std.debug.print("storage signal received, shutting down\n", .{});
            break;
        }

        var handler_context = StorageHandler{ .commands = &commands, .service = &service };
        _ = service.handleOne(handler_context.handler()) catch |err| {
            std.debug.print("storage transport error: {s}\n", .{@errorName(err)});
            sync_compat.sleep(25 * std.time.ns_per_ms);
        };
    }
}

const StorageHandler = struct {
    commands: *StorageCommands,
    service: *transport.Service,

    fn handler(self: *StorageHandler) transport.ServiceHandler {
        return .{ .context = self, .handle_fn = handleOpaque };
    }

    fn handleOpaque(context: *anyopaque, allocator: Allocator, request: transport.ServiceRequest) !transport.ServiceResponse {
        const self: *StorageHandler = @ptrCast(@alignCast(context));
        return handleRequest(allocator, self.commands, self.service, request);
    }
};

/// Best-effort: a fujin registration hiccup shouldn't fail the store
/// operation that triggered it. Logged, not propagated.
fn registerStoreService(allocator: Allocator, service: *transport.Service, ms: []const u8, store: []const u8) void {
    const name = std.fmt.allocPrint(allocator, "storage:{s}/{s}", .{ ms, store }) catch return;
    defer allocator.free(name);
    service.registerService(name) catch |err| {
        std.debug.print("storage registerService failed name={s}: {s}\n", .{ name, @errorName(err) });
    };
}

fn handleRequest(allocator: Allocator, commands: *StorageCommands, service: *transport.Service, request: transport.ServiceRequest) !transport.ServiceResponse {
    if (request.envelope.codec != .capnp) return error.CapnpPayloadRequired;
    const reader = c.transport_req_reader_decode(request.payload.ptr, request.payload.len);
    if (reader == null) {
        const invalid = encodeError("invalid capnp message");
        defer if (invalid.ptr) |p| c.transport_free_buf(p, invalid.len);
        return .{ .payload = try copyCBytes(allocator, invalid), .codec = .capnp };
    }
    defer c.transport_req_reader_free(reader);

    var shutdown = false;
    const resp = dispatch(allocator, commands, service, reader, &shutdown) catch |err| blk: {
        std.debug.print("storage dispatch error: {s}\n", .{@errorName(err)});
        break :blk encodeError(@errorName(err));
    };
    defer if (resp.ptr) |p| c.transport_free_buf(p, resp.len);
    if (shutdown) shutdown_requested.store(true, .seq_cst);
    return .{ .payload = try copyCBytes(allocator, resp), .codec = .capnp };
}

fn copyCBytes(allocator: Allocator, bytes: CBytes) ![]u8 {
    const ptr = bytes.ptr orelse return error.ResponseEncodeFailed;
    return allocator.dupe(u8, ptr[0..bytes.len]);
}

// ── Store operation context (runs in per-type worker thread) ──────────────────

const StoreOp = struct {
    commands: *StorageCommands,
    allocator: Allocator,
    cmd: c_uint,
    store_key: []const u8,
    ms: []const u8,
    store: []const u8,
    // Params (borrowed — safe because client thread waits for done)
    sql_ptr: [*:0]const u8 = "",
    key: []const u8 = "",
    cache_key: []const u8 = "",
    value: []const u8 = "",
    prefix: []const u8 = "",
    migration_id: []const u8 = "",
    output_path: []const u8 = "",
    file_name: []const u8 = "",
    dump_offset: u64 = 0,
    dump_length: u32 = 0,
    // Result
    result: CBytes = .{ .ptr = null, .len = 0 },
    op_err: ?anyerror = null,

    fn execute(ctx: ?*anyopaque) void {
        const self: *StoreOp = @ptrCast(@alignCast(ctx));
        stores_rwlock.lockShared();
        defer stores_rwlock.unlockShared();
        const traced = traceStoreOp(self.cmd);
        if (traced) {
            if (self.commands.stores.getPtr(self.store_key)) |inst| {
                std.debug.print("storage op begin cmd={s} store={s} path={s}/data\n", .{ storeCmdName(self.cmd), self.store_key, inst.store_dir });
            } else {
                std.debug.print("storage op begin cmd={s} store={s} path=<missing>\n", .{ storeCmdName(self.cmd), self.store_key });
            }
        }
        self.run() catch |e| {
            if (traced) {
                std.debug.print("storage op error cmd={s} store={s} err={s}\n", .{ storeCmdName(self.cmd), self.store_key, @errorName(e) });
            }
            self.op_err = e;
            return;
        };
        if (traced) {
            std.debug.print("storage op end cmd={s} store={s}\n", .{ storeCmdName(self.cmd), self.store_key });
        }
    }

    fn run(self: *StoreOp) !void {
        var tel = Telemetry.begin();

        switch (self.cmd) {
            c.REQ_EXEC_SQL => {
                try self.commands.execSql(self.store_key, self.sql_ptr);
                tel.op_count += 1;
                self.result = encodeAffected(tel, 0);
            },
            c.REQ_QUERY_SQL => {
                const json = try self.commands.querySql(self.store_key, self.sql_ptr, &tel);
                defer self.allocator.free(json);
                self.result = encodeData(tel, json);
            },
            c.REQ_SIZE => {
                const size = try self.commands.getStoreSize(self.store_key);
                tel.op_count += 1;
                self.result = encodeSize(tel, size);
            },
            c.REQ_STATS => {
                const stats = try self.commands.getStoreStats(self.store_key);
                tel.op_count += 1;
                self.result = encodeStoreStats(tel, stats.cache_bytes, stats.disk_bytes);
            },
            c.REQ_MANIFEST => {
                const m = self.commands.getManifest(self.store_key) orelse return error.StoreNotFound;
                tel.op_count += 1;
                const mig_z = try self.allocator.alloc([:0]u8, m.migrations.items.len);
                defer {
                    for (mig_z) |item| self.allocator.free(item);
                    self.allocator.free(mig_z);
                }
                const mig_ptrs = try self.allocator.alloc([*:0]const u8, m.migrations.items.len);
                defer self.allocator.free(mig_ptrs);
                for (m.migrations.items, 0..) |mig, i| {
                    mig_z[i] = try self.allocator.dupeZ(u8, mig);
                    mig_ptrs[i] = mig_z[i].ptr;
                }
                const version = std.fmt.parseUnsigned(u32, m.version, 10) catch 1;
                self.result = encodeManifest(tel, m.name, @intFromEnum(m.store_type), version, mig_ptrs);
            },
            c.REQ_MIGRATE => {
                try self.commands.recordMigration(self.store_key, self.migration_id);
                tel.op_count += 1;
                self.result = encodeOk(tel);
            },
            c.REQ_ARCHIVE => {
                try self.commands.createArchive(self.store_key, self.output_path);
                tel.op_count += 1;
                self.result = encodeOk(tel);
            },
            c.REQ_KV_PUT => {
                try self.commands.kvPut(self.store_key, self.key, self.value, &tel);
                self.result = encodeOk(tel);
            },
            c.REQ_KV_PUT_FROM_CACHE => {
                const data = try valkey_mod.get(self.allocator, self.cache_key) orelse return error.CacheKeyNotFound;
                defer self.allocator.free(data);
                tel.addRead(@intCast(data.len));
                try self.commands.kvPut(self.store_key, self.key, data, &tel);
                self.result = encodeOk(tel);
            },
            c.REQ_KV_GET => {
                const data = try self.commands.kvGet(self.store_key, self.key, &tel);
                if (data) |d| {
                    defer self.allocator.free(d);
                    self.result = encodeFound(tel, 1, d);
                } else {
                    self.result = encodeFound(tel, 0, &[_]u8{});
                }
            },
            c.REQ_KV_GET_TO_CACHE => {
                const data = try self.commands.kvGet(self.store_key, self.key, &tel);
                if (data) |d| {
                    defer self.allocator.free(d);
                    const generated_cache_key = try makeCacheKey(self.allocator);
                    defer self.allocator.free(generated_cache_key);
                    try valkey_mod.put(generated_cache_key, d);
                    tel.addWrite(@intCast(d.len));
                    self.result = encodeFound(tel, 1, generated_cache_key);
                } else {
                    self.result = encodeFound(tel, 0, &[_]u8{});
                }
            },
            c.REQ_KV_DELETE => {
                try self.commands.kvDelete(self.store_key, self.key, &tel);
                self.result = encodeFound(tel, 1, &[_]u8{});
            },
            c.REQ_KV_LIST => {
                const pairs = try self.commands.kvGetRange(self.store_key, self.prefix, &tel);
                defer {
                    for (pairs) |p| {
                        self.allocator.free(p.key);
                        self.allocator.free(p.value);
                    }
                    self.allocator.free(pairs);
                }
                const n = pairs.len;
                const key_ptrs = try self.allocator.alloc([*c]const u8, n);
                defer self.allocator.free(key_ptrs);
                const key_lens = try self.allocator.alloc(usize, n);
                defer self.allocator.free(key_lens);
                const val_ptrs = try self.allocator.alloc([*c]const u8, n);
                defer self.allocator.free(val_ptrs);
                const val_lens = try self.allocator.alloc(usize, n);
                defer self.allocator.free(val_lens);
                for (pairs, 0..) |p, i| {
                    key_ptrs[i] = p.key.ptr;
                    key_lens[i] = p.key.len;
                    val_ptrs[i] = p.value.ptr;
                    val_lens[i] = p.value.len;
                }
                var out_p: ?[*]u8 = null;
                var out_l: usize = 0;
                _ = c.transport_encode_kv_pairs(
                    &out_p,
                    &out_l,
                    telC(tel),
                    @ptrCast(key_ptrs.ptr),
                    @ptrCast(key_lens.ptr),
                    @ptrCast(val_ptrs.ptr),
                    @ptrCast(val_lens.ptr),
                    @intCast(n),
                );
                self.result = .{ .ptr = out_p, .len = out_l };
            },
            c.REQ_FILE_PUT => {
                try self.commands.filePut(self.store_key, self.key, self.value, &tel);
                self.result = encodeOk(tel);
            },
            c.REQ_FILE_GET => {
                const data = try self.commands.fileGet(self.store_key, self.key, &tel);
                if (data) |d| {
                    defer self.allocator.free(d);
                    self.result = encodeFound(tel, 1, d);
                } else {
                    self.result = encodeFound(tel, 0, &[_]u8{});
                }
            },
            c.REQ_FILE_DELETE => {
                _ = try self.commands.fileDelete(self.store_key, self.key, &tel);
                self.result = encodeFound(tel, 1, &[_]u8{});
            },
            c.REQ_FILE_LIST => {
                const inst = self.commands.stores.getPtr(self.store_key) orelse return error.StoreNotFound;
                switch (inst.handle) {
                    .files => |*e| {
                        var dir = fs_compat.cwd().openDir(e.base_path, .{ .iterate = true }) catch {
                            tel.op_count += 1;
                            self.result = encodeKeys(tel, &[_][*:0]const u8{});
                            return;
                        };
                        defer dir.close();

                        var walker = try dir.walk(self.allocator);
                        defer walker.deinit();

                        var key_z_arr: std.ArrayList([:0]u8) = .empty;
                        defer {
                            for (key_z_arr.items) |k| self.allocator.free(k);
                            key_z_arr.deinit(self.allocator);
                        }

                        while (try walker.next()) |entry| {
                            if (entry.kind != .file) continue;
                            try key_z_arr.append(self.allocator, try self.allocator.dupeZ(u8, entry.path));
                        }

                        const key_ptrs = try self.allocator.alloc([*:0]const u8, key_z_arr.items.len);
                        defer self.allocator.free(key_ptrs);

                        for (key_z_arr.items, 0..) |k, i| {
                            key_ptrs[i] = k.ptr;
                        }

                        tel.op_count += 1;
                        self.result = encodeKeys(tel, key_ptrs);
                    },
                    else => return error.UnsupportedOperation,
                }
            },
            c.REQ_KV_COMPACT => {
                try self.commands.kvCompact(self.store_key);
                tel.op_count += 1;
                self.result = encodeOk(tel);
            },
            c.REQ_DUMP_CREATE => {
                const file_name = try self.commands.createDump(self.ms, self.store);
                defer self.commands.allocator.free(file_name);
                tel.op_count += 1;
                self.result = encodeData(tel, file_name);
            },
            else => return error.UnknownCommand,
        }
    }
};

// ── Dispatch ──────────────────────────────────────────────────────────────────

const CBytes = struct { ptr: ?[*]u8, len: usize };

fn makeCacheKey(allocator: Allocator) ![]u8 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const nanos: u128 = @as(u128, @intCast(ts.sec)) * std.time.ns_per_s + @as(u128, @intCast(ts.nsec));
    const n = cache_key_counter.fetchAdd(1, .monotonic);
    return std.fmt.allocPrint(allocator, "transport:kvs:{x}:{x}", .{ nanos, n });
}

fn dispatch(
    allocator: Allocator,
    commands: *StorageCommands,
    service: *transport.Service,
    reader: ?*c.TransportRequestReader,
    shutdown: *bool,
) !CBytes {
    const cmd = c.transport_req_reader_cmd(reader);
    const ms = std.mem.span(c.transport_req_reader_ms(reader));
    const store = std.mem.span(c.transport_req_reader_store(reader));

    var tel = Telemetry.begin();

    // ── Inline commands (no store worker needed) ──
    switch (cmd) {
        c.REQ_PING => {
            tel.op_count += 1;
            return encodeOk(tel);
        },
        c.REQ_SHUTDOWN => {
            tel.op_count += 1;
            shutdown.* = true;
            return encodeOk(tel);
        },
        c.REQ_OPEN => {
            stores_rwlock.lock();
            defer stores_rwlock.unlock();
            try commands.openExistingStore(ms, store);
            tel.op_count += 1;
            return encodeOk(tel);
        },
        c.REQ_CREATE => {
            stores_rwlock.lock();
            defer stores_rwlock.unlock();
            const raw = @as(u8, @intCast(c.transport_req_reader_store_type(reader)));
            const st: StoreType = @enumFromInt(raw);
            try commands.createStore(ms, store, st);
            // A store created at runtime is routable the same instant it
            // exists — fujin learns about it dynamically, not just at
            // startup autoload (see autoOpenStores).
            registerStoreService(allocator, service, ms, store);
            tel.op_count += 1;
            return encodeOk(tel);
        },
        c.REQ_CLOSE => {
            stores_rwlock.lock();
            defer stores_rwlock.unlock();
            const store_key = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ ms, store });
            defer allocator.free(store_key);
            commands.closeStore(store_key);
            tel.op_count += 1;
            return encodeOk(tel);
        },
        c.REQ_FILE_LIST => {
            // Global list (ms="" && store="") — handled inline with read lock
            if (ms.len == 0 and store.len == 0) {
                stores_rwlock.lockShared();
                defer stores_rwlock.unlockShared();
                tel.op_count += 1;
                const count = commands.stores.count();
                const key_z_arr = try allocator.alloc([:0]u8, count);
                defer {
                    for (key_z_arr) |k| allocator.free(k);
                    allocator.free(key_z_arr);
                }
                const key_ptrs = try allocator.alloc([*:0]const u8, count);
                defer allocator.free(key_ptrs);
                var i: usize = 0;
                var it = commands.stores.keyIterator();
                while (it.next()) |key| : (i += 1) {
                    key_z_arr[i] = try allocator.dupeZ(u8, key.*);
                    key_ptrs[i] = key_z_arr[i].ptr;
                }
                return encodeKeys(tel, key_ptrs[0..i]);
            }
            // Store-specific FILE_LIST falls through to worker dispatch
        },
        c.REQ_DUMP_LIST => {
            const entries = try commands.listDumps();
            defer {
                for (entries) |e| allocator.free(e.name);
                allocator.free(entries);
            }
            tel.op_count += 1;
            const n = entries.len;
            const key_ptrs = try allocator.alloc([*c]const u8, n);
            defer allocator.free(key_ptrs);
            const key_lens = try allocator.alloc(usize, n);
            defer allocator.free(key_lens);
            const val_ptrs = try allocator.alloc([*c]const u8, n);
            defer allocator.free(val_ptrs);
            const val_lens = try allocator.alloc(usize, n);
            defer allocator.free(val_lens);
            var size_bufs = try allocator.alloc([8]u8, n);
            defer allocator.free(size_bufs);
            for (entries, 0..) |e, i| {
                key_ptrs[i] = e.name.ptr;
                key_lens[i] = e.name.len;
                std.mem.writeInt(u64, &size_bufs[i], e.size, .little);
                val_ptrs[i] = &size_bufs[i];
                val_lens[i] = 8;
            }
            var out_p: ?[*]u8 = null;
            var out_l: usize = 0;
            _ = c.transport_encode_kv_pairs(
                &out_p,
                &out_l,
                telC(tel),
                @ptrCast(key_ptrs.ptr),
                @ptrCast(key_lens.ptr),
                @ptrCast(val_ptrs.ptr),
                @ptrCast(val_lens.ptr),
                @intCast(n),
            );
            return .{ .ptr = out_p, .len = out_l };
        },
        c.REQ_DUMP_DELETE => {
            const file_name = std.mem.span(c.transport_req_reader_file_name(reader));
            try commands.deleteDump(file_name);
            tel.op_count += 1;
            return encodeOk(tel);
        },
        c.REQ_DUMP_READ => {
            const file_name = std.mem.span(c.transport_req_reader_file_name(reader));
            const offset = c.transport_req_reader_dump_offset(reader);
            const length = c.transport_req_reader_dump_length(reader);
            const chunk = try commands.readDump(file_name, offset, length);
            defer allocator.free(chunk);
            tel.op_count += 1;
            return encodeData(tel, chunk);
        },
        else => {},
    }

    // ── Store operations: route through per-type worker ──
    const store_key = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ ms, store });
    defer allocator.free(store_key);

    // Brief read lock to determine store type for routing
    stores_rwlock.lockShared();
    const store_type = commands.getStoreType(store_key);
    stores_rwlock.unlockShared();

    if (store_type == null) return error.StoreNotFound;

    // Extract command-specific params from reader (before submitting to worker)
    var sql_z: ?[:0]u8 = null;
    if (cmd == c.REQ_EXEC_SQL or cmd == c.REQ_QUERY_SQL) {
        sql_z = try allocator.dupeZ(u8, std.mem.span(c.transport_req_reader_sql(reader)));
    }
    defer if (sql_z) |z| allocator.free(z);

    var op = StoreOp{
        .commands = commands,
        .allocator = allocator,
        .cmd = cmd,
        .store_key = store_key,
        .ms = ms,
        .store = store,
    };

    if (sql_z) |z| {
        op.sql_ptr = z.ptr;
    }

    if (cmd == c.REQ_KV_PUT or cmd == c.REQ_KV_GET or cmd == c.REQ_KV_DELETE or
        cmd == c.REQ_KV_PUT_FROM_CACHE or cmd == c.REQ_KV_GET_TO_CACHE or
        cmd == c.REQ_FILE_PUT or cmd == c.REQ_FILE_GET or cmd == c.REQ_FILE_DELETE)
    {
        op.key = std.mem.span(c.transport_req_reader_key(reader));
    }

    if (cmd == c.REQ_KV_PUT_FROM_CACHE) {
        op.cache_key = std.mem.span(c.transport_req_reader_cache_key(reader));
    }

    if (cmd == c.REQ_KV_PUT or cmd == c.REQ_FILE_PUT) {
        const v_ptr = c.transport_req_reader_value_ptr(reader);
        const v_len = c.transport_req_reader_value_len(reader);
        op.value = if (v_ptr) |p| p[0..v_len] else &[_]u8{};
    }

    if (cmd == c.REQ_KV_LIST) {
        op.prefix = std.mem.span(c.transport_req_reader_prefix(reader));
    }

    if (cmd == c.REQ_MIGRATE) {
        op.migration_id = std.mem.span(c.transport_req_reader_migration_id(reader));
    }

    if (cmd == c.REQ_ARCHIVE) {
        op.output_path = std.mem.span(c.transport_req_reader_output_path(reader));
    }

    // Submit to per-type worker and wait
    var item = WorkItem{
        .store_key = store_key,
        .op = .exec_sql,
        .payload = &[_]u8{},
        .result = null,
        .done = .{},
        .err = null,
        .exec_ctx = @ptrCast(&op),
        .exec_fn = StoreOp.execute,
    };

    pool.getWorkerForStore(store_type.?, store_key).submit(&item);
    item.done.wait();

    if (op.op_err) |e| return e;
    return op.result;
}

// ── Encode helpers ────────────────────────────────────────────────────────────

fn telC(tel: Telemetry) c.TelemetryC {
    return .{ .dur_us = tel.durationUs(), .op_count = @as(u32, @intCast(tel.op_count)) };
}

fn encodeOk(tel: Telemetry) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    _ = c.transport_encode_ok(&p, &l, telC(tel));
    return .{ .ptr = p, .len = l };
}
fn encodeError(msg: []const u8) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    const owned_msg: ?[:0]u8 = std.heap.c_allocator.dupeZ(u8, msg) catch null;
    defer if (owned_msg) |buf| std.heap.c_allocator.free(buf);
    const c_msg: [*:0]const u8 = if (owned_msg) |buf| buf.ptr else "unknown error";
    _ = c.transport_encode_error(&p, &l, c_msg);
    return .{ .ptr = p, .len = l };
}
fn encodeAffected(tel: Telemetry, n: i64) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    _ = c.transport_encode_affected(&p, &l, telC(tel), n);
    return .{ .ptr = p, .len = l };
}
fn encodeSize(tel: Telemetry, size: u64) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    _ = c.transport_encode_size(&p, &l, telC(tel), size);
    return .{ .ptr = p, .len = l };
}
fn encodeStoreStats(tel: Telemetry, cache_bytes: u64, disk_bytes: u64) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    _ = c.transport_encode_store_stats(&p, &l, telC(tel), cache_bytes, disk_bytes);
    return .{ .ptr = p, .len = l };
}
fn encodeFound(tel: Telemetry, found: i32, data: []const u8) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    _ = c.transport_encode_found(&p, &l, telC(tel), found, data.ptr, data.len);
    return .{ .ptr = p, .len = l };
}
fn encodeKeys(tel: Telemetry, keys: []const [*:0]const u8) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    const key_ptrs: [*c][*c]const u8 = @ptrCast(@constCast(keys.ptr));
    _ = c.transport_encode_keys(&p, &l, telC(tel), key_ptrs, @intCast(keys.len));
    return .{ .ptr = p, .len = l };
}
fn encodeData(tel: Telemetry, data: []const u8) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    _ = c.transport_encode_data(&p, &l, telC(tel), data.ptr, data.len);
    return .{ .ptr = p, .len = l };
}
fn encodeManifest(tel: Telemetry, name: []const u8, store_type: u8, version: u32, migs: [][*:0]const u8) CBytes {
    var p: ?[*]u8 = null;
    var l: usize = 0;
    _ = c.transport_encode_manifest(&p, &l, telC(tel), name.ptr, store_type, version, @ptrCast(migs.ptr), @intCast(migs.len));
    return .{ .ptr = p, .len = l };
}

// ── Signal / socket helpers ───────────────────────────────────────────────────

fn installSignalHandlers() void {
    const act: std.posix.Sigaction = .{
        .handler = .{ .handler = onSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);
    std.posix.sigaction(std.posix.SIG.INT, &act, null);
}

fn onSignal(_: std.posix.SIG) callconv(.c) void {
    shutdown_requested.store(true, .seq_cst);
}

fn autoOpenStores(allocator: Allocator, commands: *StorageCommands, service: *transport.Service, data_dir: []const u8) !void {
    var root = fs_compat.cwd().openDir(data_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer root.close();

    var opened: usize = 0;
    var ms_iter = root.iterate();
    while (try ms_iter.next()) |ms_entry| {
        if (ms_entry.kind != .directory) continue;
        var ms_dir = root.openDir(ms_entry.name, .{ .iterate = true }) catch continue;
        defer ms_dir.close();

        var store_iter = ms_dir.iterate();
        while (try store_iter.next()) |store_entry| {
            if (store_entry.kind != .directory) continue;

            const manifest_path = try std.fmt.allocPrint(
                allocator,
                "{s}/{s}/{s}/manifest.json",
                .{ data_dir, ms_entry.name, store_entry.name },
            );
            defer allocator.free(manifest_path);

            var manifest = mfst.Manifest.load(allocator, manifest_path) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => {
                    std.debug.print("storage autoload skip {s}/{s}: {s}\n", .{ ms_entry.name, store_entry.name, @errorName(err) });
                    continue;
                },
            };
            const store_type = manifest.store_type;
            manifest.deinit();

            commands.openStore(ms_entry.name, store_entry.name, store_type) catch |err| {
                std.debug.print("storage autoload failed {s}/{s}: {s}\n", .{ ms_entry.name, store_entry.name, @errorName(err) });
                continue;
            };
            registerStoreService(allocator, service, ms_entry.name, store_entry.name);
            opened += 1;
        }
    }
    std.debug.print("storage autoload complete, opened={d}\n", .{opened});
}
