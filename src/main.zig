const std = @import("std");
const posix_compat = @import("posix_compat.zig");
const fs_compat = @import("fs_compat.zig");
const commands = @import("commands.zig");
const manifest_mod = @import("manifest.zig");
const valkey_mod = @import("valkey.zig");
const build_options = @import("build_options");
const StoreType = manifest_mod.StoreType;
const Telemetry = @import("telemetry.zig").Telemetry;
const StorageCommands = commands.StorageCommands;
const ValkeyConfig = valkey_mod.Config;
const with_transport = build_options.with_transport;
const c = if (with_transport) @cImport(@cInclude("transport.h")) else struct {};
const transport = if (with_transport) @import("transport") else struct {};
const server = if (with_transport) @import("server.zig") else struct {
    pub const BindConfig = union(enum) {
        unix: []const u8,
        tcp: struct { host: []const u8, port: u16 },
        endpoint: []const u8,
    };
    pub fn start(_: std.mem.Allocator, _: []const u8, _: BindConfig, _: ValkeyConfig) !void {
        return error.TransportDisabled;
    }
};
const health_checker = if (with_transport) struct {
    fn run(allocator: std.mem.Allocator, bind_cfg: server.BindConfig, timeout_ms: u32) !bool {
        const endpoint = try server.endpointForConfig(allocator, bind_cfg);
        defer allocator.free(endpoint);
        var identity_buffer: [64]u8 = undefined;
        const identity = try std.fmt.bufPrint(&identity_buffer, "behemoth-health-{d}", .{std.c.getpid()});
        const target = posix_compat.getenv("FUJIN_TARGET") orelse posix_compat.getenv("BEHEMOTH_FUJIN_TARGET") orelse "behemoth";
        // A self-contained NRPC client: its own DEALER socket and identity,
        // registered with Fujin so the pong routes back. The ping is capnp, so
        // send/recv are driven at the peer level rather than through call().
        var client = try transport.Client.init(allocator, .{
            .endpoint = endpoint,
            .identity = identity,
            .target = identity,
            .shared = false,
            .services_json = "[]",
            .limits = .{ .max_envelope_bytes = 64 * 1024, .max_payload_bytes = 16 * 1024 * 1024 },
            .recv_timeout_ms = @intCast(timeout_ms),
            .send_timeout_ms = @intCast(timeout_ms),
        });
        defer client.deinit();

        const req = c.transport_req_ping() orelse return false;
        defer c.transport_req_free(req);

        var out_buf: ?[*]u8 = null;
        var out_len: usize = 0;
        if (c.transport_req_encode(req, @ptrCast(&out_buf), &out_len) != 0) return false;
        const raw = out_buf orelse return false;
        defer c.transport_free_buf(raw, out_len);
        const ping_env = transport.Envelope{
            .kind = .request,
            .request_id = identity,
            .to = .{ .target = target, .service = "storage" },
            .from = .{ .target = identity },
            .method = "ping",
            .codec = .capnp,
            .deadline_ms = timeout_ms,
        };
        const ping_bytes = try transport.envelope.encodeAlloc(allocator, &ping_env);
        defer allocator.free(ping_bytes);
        try client.peer.send(ping_bytes, raw[0..out_len]);

        var incoming = (try client.peer.recv()) orelse return false;
        defer incoming.deinit();
        const response_env = try incoming.parseEnvelope();
        if (response_env.kind != .response or !std.mem.eql(u8, response_env.request_id, identity)) return false;
        const resp = c.transport_resp_decode(incoming.payload().ptr, incoming.payload().len) orelse return false;
        defer c.transport_resp_free(resp);

        return c.transport_resp_ok(resp) == 1;
    }
} else struct {
    fn run(_: std.mem.Allocator, _: server.BindConfig, _: u32) !bool {
        return error.TransportDisabled;
    }
};

comptime {
    if (@import("builtin").is_test) {
        _ = @import("internal_tests.zig");
    }
}

fn writeStdout(data: []const u8) void {
    _ = posix_compat.write(std.posix.STDOUT_FILENO, data) catch {};
}

fn printJson(allocator: std.mem.Allocator, ok: bool, data: ?[]const u8, tel: *const Telemetry) void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    var aw: std.Io.Writer.Allocating = .fromArrayList(allocator, &buf);
    const w = &aw.writer;

    if (ok) {
        w.writeAll("{\"ok\":true") catch return;
    } else {
        w.writeAll("{\"ok\":false") catch return;
    }
    if (data) |d| {
        w.writeAll(",\"data\":") catch return;
        w.writeAll(d) catch return;
    }
    w.writeAll(",\"telemetry\":") catch return;
    tel.writeJson(w) catch return;
    w.writeAll("}\n") catch return;

    buf = aw.toArrayList();
    writeStdout(buf.items);
}

fn printError(msg: []const u8) void {
    std.debug.print("{{\"ok\":false,\"error\":\"{s}\"}}\n", .{msg});
}

fn printErrorName(prefix: []const u8, err: anyerror) void {
    std.debug.print("{{\"ok\":false,\"error\":\"{s}: {s}\"}}\n", .{ prefix, @errorName(err) });
}

pub fn main(init: std.process.Init) !void {
    fs_compat.setIo(init.io);
    const allocator = init.gpa;
    const args = try std.process.Args.toSlice(init.minimal.args, init.arena.allocator());

    if (args.len < 2) {
        printUsage();
        return;
    }

    const data_dir = getDataDir(args);
    var cmds = StorageCommands.init(allocator, data_dir);
    defer cmds.deinit();

    const cmd = args[1];

    if (std.mem.eql(u8, cmd, "start")) {
        if (!with_transport) {
            printError("start is disabled in this build (transport=false)");
            return;
        }
        const bind_cfg = try getBindConfig(allocator, args, data_dir);
        defer switch (bind_cfg) {
            .unix => |p| allocator.free(p),
            .tcp => |t| allocator.free(t.host),
            .endpoint => |value| allocator.free(value),
        };
        const valkey_cfg = try getValkeyConfig(allocator, args);
        defer if (valkey_cfg.enabled) allocator.free(valkey_cfg.host);
        try server.start(allocator, data_dir, bind_cfg, valkey_cfg);
        return;
    }
    if (std.mem.eql(u8, cmd, "health")) {
        if (!with_transport) {
            printError("health is disabled in this build (transport=false)");
            std.process.exit(1);
        }
        const bind_cfg = getBindConfig(allocator, args, data_dir) catch |err| {
            printErrorName("health failed", err);
            std.process.exit(1);
        };
        defer switch (bind_cfg) {
            .unix => |p| allocator.free(p),
            .tcp => |t| allocator.free(t.host),
            .endpoint => |value| allocator.free(value),
        };
        const timeout_ms = getHealthTimeoutMs(args) catch |err| {
            printErrorName("health failed", err);
            std.process.exit(1);
        };
        const healthy = health_checker.run(allocator, bind_cfg, timeout_ms) catch |err| {
            printErrorName("health failed", err);
            std.process.exit(1);
        };
        if (!healthy) {
            printError("health failed");
            std.process.exit(1);
        }

        var tel = Telemetry.begin();
        tel.op_count += 1;
        printJson(allocator, true, null, &tel);
        return;
    }

    if (std.mem.eql(u8, cmd, "open")) {
        if (args.len < 5) return printError("usage: open <ms> <store> <type>");
        const store_type = StoreType.fromString(args[4]) orelse return printError("unknown type");
        var tel = Telemetry.begin();
        cmds.openStore(args[2], args[3], store_type) catch |err|
            return printErrorName("open failed", err);
        tel.op_count += 1;
        printJson(allocator, true, null, &tel);
    } else if (std.mem.eql(u8, cmd, "close")) {
        if (args.len < 4) return printError("usage: close <ms> <store>");
        var tel = Telemetry.begin();
        const key = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ args[2], args[3] });
        defer allocator.free(key);
        cmds.closeStore(key);
        tel.op_count += 1;
        printJson(allocator, true, null, &tel);
    } else if (std.mem.eql(u8, cmd, "exec")) {
        if (args.len < 4) return printError("usage: exec <ms/store> <sql>");
        var tel = Telemetry.begin();
        const sql_z = try allocator.dupeZ(u8, args[3]);
        defer allocator.free(sql_z);
        cmds.execSql(args[2], sql_z) catch |err|
            return printErrorName("exec failed", err);
        tel.op_count += 1;
        printJson(allocator, true, null, &tel);
    } else if (std.mem.eql(u8, cmd, "query")) {
        if (args.len < 4) return printError("usage: query <ms/store> <sql>");
        var tel = Telemetry.begin();
        const sql_z = try allocator.dupeZ(u8, args[3]);
        defer allocator.free(sql_z);
        const data = cmds.querySql(args[2], sql_z, &tel) catch |err|
            return printErrorName("query failed", err);
        defer allocator.free(data);
        printJson(allocator, true, data, &tel);
    } else if (std.mem.eql(u8, cmd, "size")) {
        if (args.len < 3) return printError("usage: size <ms/store>");
        var tel = Telemetry.begin();
        const size = cmds.getStoreSize(args[2]) catch |err|
            return printErrorName("size failed", err);
        tel.op_count += 1;
        const data = try std.fmt.allocPrint(allocator, "{}", .{size});
        defer allocator.free(data);
        printJson(allocator, true, data, &tel);
    } else if (std.mem.eql(u8, cmd, "manifest")) {
        if (args.len < 3) return printError("usage: manifest <ms/store>");
        if (cmds.getManifest(args[2])) |mfst| {
            var tel = Telemetry.begin();
            tel.op_count += 1;

            var buf: std.ArrayList(u8) = .empty;
            errdefer buf.deinit(allocator);
            var aw: std.Io.Writer.Allocating = .fromArrayList(allocator, &buf);
            const w = &aw.writer;
            w.print("{{\"name\":\"{s}\",\"type\":\"{s}\",\"migrations\":[", .{ mfst.name, mfst.store_type.toString() }) catch return;
            for (mfst.migrations.items, 0..) |m, i| {
                if (i > 0) w.writeByte(',') catch return;
                w.print("\"{s}\"", .{m}) catch return;
            }
            w.writeAll("]}") catch return;
            buf = aw.toArrayList();
            const data = buf.toOwnedSlice(allocator) catch null;
            defer if (data) |d| allocator.free(d);
            printJson(allocator, true, data, &tel);
        } else {
            return printError("store not found");
        }
    } else if (std.mem.eql(u8, cmd, "migrate")) {
        if (args.len < 4) return printError("usage: migrate <ms/store> <migration_id>");
        var tel = Telemetry.begin();
        cmds.recordMigration(args[2], args[3]) catch |err|
            return printErrorName("migrate failed", err);
        tel.op_count += 1;
        printJson(allocator, true, null, &tel);
    } else if (std.mem.eql(u8, cmd, "archive")) {
        if (args.len < 4) return printError("usage: archive <ms/store> <output_path>");
        var tel = Telemetry.begin();
        cmds.createArchive(args[2], args[3]) catch |err|
            return printErrorName("archive failed", err);
        tel.op_count += 1;
        printJson(allocator, true, null, &tel);
    } else {
        printUsage();
    }
}

fn getDataDir(args: []const []const u8) []const u8 {
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, "--data-dir") and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return posix_compat.getenv("DATA_DIR") orelse "./data";
}

fn getBindConfig(allocator: std.mem.Allocator, args: []const []const u8, data_dir: []const u8) !server.BindConfig {
    _ = data_dir;
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, "--fujin") and i + 1 < args.len) {
            return .{ .endpoint = try allocator.dupe(u8, args[i + 1]) };
        }
        if (std.mem.eql(u8, arg, "--socket") and i + 1 < args.len) {
            return .{ .unix = try allocator.dupe(u8, args[i + 1]) };
        }
        if (std.mem.eql(u8, arg, "--tcp") and i + 1 < args.len) {
            const addr = args[i + 1];
            const colon = std.mem.lastIndexOf(u8, addr, ":") orelse return error.InvalidTcpAddress;
            const host = try allocator.dupe(u8, addr[0..colon]);
            const port = std.fmt.parseUnsigned(u16, addr[colon + 1 ..], 10) catch return error.InvalidTcpPort;
            return .{ .tcp = .{ .host = host, .port = port } };
        }
    }
    const endpoint = posix_compat.getenv("BEHEMOTH_FUJIN_ZMQ_ENDPOINT") orelse
        posix_compat.getenv("FUJIN_ZMQ_ENDPOINT") orelse "tcp://127.0.0.1:5557";
    return .{ .endpoint = try allocator.dupe(u8, endpoint) };
}

fn getValkeyConfig(allocator: std.mem.Allocator, args: []const []const u8) !ValkeyConfig {
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, "--no-valkey")) {
            return .{ .enabled = false, .host = "", .port = 0 };
        }
        if (std.mem.eql(u8, arg, "--valkey") and i + 1 < args.len) {
            const addr = args[i + 1];
            const colon = std.mem.lastIndexOf(u8, addr, ":") orelse return error.InvalidValkeyAddress;
            const host = try allocator.dupe(u8, addr[0..colon]);
            const port = std.fmt.parseUnsigned(u16, addr[colon + 1 ..], 10) catch return error.InvalidValkeyPort;
            return .{ .enabled = true, .host = host, .port = port };
        }
    }
    return .{
        .enabled = true,
        .host = try allocator.dupe(u8, "127.0.0.1"),
        .port = 6379,
    };
}

fn getHealthTimeoutMs(args: []const []const u8) !u32 {
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, "--timeout-ms") and i + 1 < args.len) {
            const timeout = std.fmt.parseUnsigned(u32, args[i + 1], 10) catch return error.InvalidTimeoutMs;
            if (timeout == 0) return error.InvalidTimeoutMs;
            return timeout;
        }
    }
    return 1000;
}

fn printUsage() void {
    if (with_transport) {
        std.debug.print(
            \\storage - native storage engine
            \\
            \\usage: storage <command> [args...] [--data-dir <path>]
            \\       storage start [--data-dir <path>] [--fujin <zmq-endpoint>] [--valkey <host>:<port>|--no-valkey]
            \\       storage health [--fujin <zmq-endpoint>] [--timeout-ms <ms>]
            \\
            \\commands:
            \\  start                                  (transport server + embedded valkey)
            \\  health                                 (transport ping probe)
            \\  open <ms> <store> <SQL|KEY_VALUE|COLUMN|VECTOR|FILES|GRAPH>
            \\  close <ms> <store>
            \\  exec <ms/store> <sql>
            \\  query <ms/store> <sql>
            \\  size <ms/store>
            \\  manifest <ms/store>
            \\  migrate <ms/store> <migration_id>
            \\  archive <ms/store> <output_path>
            \\
        , .{});
    } else {
        std.debug.print(
            \\storage - native storage engine
            \\
            \\usage: storage <command> [args...] [--data-dir <path>]
            \\
            \\commands:
            \\  open <ms> <store> <SQL|KEY_VALUE|COLUMN|VECTOR|FILES|GRAPH>
            \\  close <ms> <store>
            \\  exec <ms/store> <sql>
            \\  query <ms/store> <sql>
            \\  size <ms/store>
            \\  manifest <ms/store>
            \\  migrate <ms/store> <migration_id>
            \\  archive <ms/store> <output_path>
            \\
        , .{});
    }
}
