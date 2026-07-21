const std = @import("std");
const qjs = @import("qjs_client.zig");
const StoreType = @import("manifest.zig").StoreType;
const SqlEngine = @import("engines/sql.zig").SqlEngine;
const KvEngine = @import("engines/kv.zig").KvEngine;
const FilesEngine = @import("engines/files.zig").FilesEngine;
const GraphEngine = @import("engines/graph.zig").GraphEngine;

pub const StoreConfig = union(StoreType) {
    sql: SqlEngine.Config,
    kv: KvEngine.Config,
    column: SqlEngine.Config,
    vector: SqlEngine.Config,
    files: FilesEngine.Config,
    graph: GraphEngine.Config,
};

pub fn defaultConfig(store_type: StoreType) StoreConfig {
    return switch (store_type) {
        .sql => .{ .sql = .{} },
        .kv => .{ .kv = .{} },
        .column => .{ .column = .{} },
        .vector => .{ .vector = .{} },
        .files => .{ .files = .{} },
        .graph => .{ .graph = .{} },
    };
}

/// Loads the script for each store open. Replacing the file changes the next
/// initialization without restarting storage or rebuilding an engine.
pub const Manager = struct {
    allocator: std.mem.Allocator,
    qjs_client: qjs.Client,
    script_path: []u8,

    pub fn init(allocator: std.mem.Allocator, qjs_path: []const u8, script_path: []const u8) !Manager {
        var client = try qjs.Client.init(allocator, qjs_path);
        errdefer client.deinit();
        return .{
            .allocator = allocator,
            .qjs_client = client,
            .script_path = try allocator.dupe(u8, script_path),
        };
    }

    pub fn deinit(self: *Manager) void {
        self.qjs_client.deinit();
        self.allocator.free(self.script_path);
        self.* = undefined;
    }

    pub fn configureStore(self: *Manager, store_key: []const u8, store_type: StoreType) !StoreConfig {
        const script = try std.Io.Dir.cwd().readFileAlloc(
            std.Options.debug_io,
            self.script_path,
            self.allocator,
            .limited(1024 * 1024),
        );
        defer self.allocator.free(script);

        const invocation = try buildInvocation(self.allocator, script, store_key, store_type);
        defer self.allocator.free(invocation);

        var result = try self.qjs_client.eval(invocation);
        defer result.deinit(self.allocator);
        if (result.code != 0) {
            std.log.err("storage management script {s} failed: {s}", .{ self.script_path, result.text });
            return error.ManagementScriptFailed;
        }
        return parseConfig(self.allocator, store_type, result.text);
    }
};

pub fn initFromEnv(allocator: std.mem.Allocator) !?Manager {
    const script_z = std.c.getenv("BEHEMOTH_STORAGE_JS_SCRIPT") orelse return null;
    const qjs_z = std.c.getenv("BEHEMOTH_QJS_LIB") orelse return error.QjsLibraryPathRequired;
    return try Manager.init(allocator, std.mem.span(qjs_z), std.mem.span(script_z));
}

fn buildInvocation(allocator: std.mem.Allocator, script: []const u8, store_key: []const u8, store_type: StoreType) ![]u8 {
    var store_json: std.ArrayList(u8) = .empty;
    defer store_json.deinit(allocator);
    try store_json.appendSlice(allocator, "{\"key\":");
    try appendJsonString(&store_json, allocator, store_key);
    try store_json.appendSlice(allocator, ",\"type\":");
    try appendJsonString(&store_json, allocator, store_type.toString());
    try store_json.append(allocator, '}');

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try out.appendSlice(allocator, script);
    try out.appendSlice(allocator,
        \\;(() => {
        \\  "use strict";
        \\  const calls = [];
        \\  const engines = Object.freeze({
        \\    sqlite: Object.freeze({ configure(options = {}) { calls.push({ engine: "sqlite", options }); } }),
        \\    kv: Object.freeze({ configure(options = {}) { calls.push({ engine: "kv", options }); } }),
        \\    files: Object.freeze({ configure(options = {}) { calls.push({ engine: "files", options }); } }),
        \\    graph: Object.freeze({ configure(options = {}) { calls.push({ engine: "graph", options }); } }),
        \\  });
        \\  if (typeof configureStore !== "function") throw new Error("script must define configureStore(store, engines)");
        \\  configureStore(
    );
    try out.appendSlice(allocator, store_json.items);
    try out.appendSlice(allocator, ", engines);\n  return JSON.stringify(calls);\n})()");
    return try out.toOwnedSlice(allocator);
}

fn appendJsonString(out: *std.ArrayList(u8), allocator: std.mem.Allocator, value: []const u8) !void {
    try out.append(allocator, '"');
    for (value) |byte| {
        switch (byte) {
            '"' => try out.appendSlice(allocator, "\\\""),
            '\\' => try out.appendSlice(allocator, "\\\\"),
            '\n' => try out.appendSlice(allocator, "\\n"),
            '\r' => try out.appendSlice(allocator, "\\r"),
            '\t' => try out.appendSlice(allocator, "\\t"),
            else => {
                if (byte < 0x20) {
                    const escaped = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{byte});
                    defer allocator.free(escaped);
                    try out.appendSlice(allocator, escaped);
                } else {
                    try out.append(allocator, byte);
                }
            },
        }
    }
    try out.append(allocator, '"');
}

fn parseConfig(allocator: std.mem.Allocator, store_type: StoreType, text: []const u8) !StoreConfig {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, text, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return error.InvalidManagementResult;
    if (parsed.value.array.items.len == 0) return defaultConfig(store_type);
    if (parsed.value.array.items.len != 1) return error.MultipleManagementConfigurations;

    const call = parsed.value.array.items[0];
    if (call != .object) return error.InvalidManagementResult;
    const engine = try requiredString(&call.object, "engine");
    const options = call.object.get("options") orelse return error.InvalidManagementResult;
    if (options != .object) return error.InvalidManagementResult;

    return switch (store_type) {
        .sql => blk: {
            if (!std.mem.eql(u8, engine, "sqlite")) return error.ManagementEngineMismatch;
            break :blk .{ .sql = try parseSqlConfig(&options.object) };
        },
        .column => blk: {
            if (!std.mem.eql(u8, engine, "sqlite")) return error.ManagementEngineMismatch;
            break :blk .{ .column = try parseSqlConfig(&options.object) };
        },
        .vector => blk: {
            if (!std.mem.eql(u8, engine, "sqlite")) return error.ManagementEngineMismatch;
            break :blk .{ .vector = try parseSqlConfig(&options.object) };
        },
        .kv => blk: {
            if (!std.mem.eql(u8, engine, "kv")) return error.ManagementEngineMismatch;
            break :blk .{ .kv = try parseKvConfig(&options.object) };
        },
        .files => blk: {
            if (!std.mem.eql(u8, engine, "files")) return error.ManagementEngineMismatch;
            break :blk .{ .files = try parseFilesConfig(&options.object) };
        },
        .graph => blk: {
            if (!std.mem.eql(u8, engine, "graph")) return error.ManagementEngineMismatch;
            break :blk .{ .graph = try parseGraphConfig(&options.object) };
        },
    };
}

fn parseSqlConfig(obj: *const std.json.ObjectMap) !SqlEngine.Config {
    var config = SqlEngine.Config{};
    if (try optionalU64(obj, "cacheKiB", 1, 1024 * 1024)) |value| config.cache_kib = @intCast(value);
    if (try optionalU64(obj, "busyTimeoutMs", 0, 600_000)) |value| config.busy_timeout_ms = @intCast(value);
    if (obj.get("tempStore")) |value| {
        if (value != .string) return error.InvalidManagementResult;
        config.temp_store = if (std.mem.eql(u8, value.string, "memory"))
            .memory
        else if (std.mem.eql(u8, value.string, "file"))
            .file
        else
            return error.InvalidManagementResult;
    }
    return config;
}

fn parseKvConfig(obj: *const std.json.ObjectMap) !KvEngine.Config {
    var config = KvEngine.Config{};
    config.map_lower_mib = try optionalU64(obj, "mapLowerMiB", 1, 1024 * 1024);
    config.map_now_mib = try optionalU64(obj, "mapNowMiB", 1, 1024 * 1024);
    config.map_upper_mib = try optionalU64(obj, "mapUpperMiB", 1, 1024 * 1024);
    config.map_growth_mib = try optionalU64(obj, "mapGrowthMiB", 1, 1024 * 1024);
    config.map_shrink_mib = try optionalU64(obj, "mapShrinkMiB", 1, 1024 * 1024);
    if (obj.get("autoCompactOnOpen")) |value| {
        if (value != .bool) return error.InvalidManagementResult;
        config.auto_compact_on_open = value.bool;
    }
    return config;
}

fn parseFilesConfig(obj: *const std.json.ObjectMap) !FilesEngine.Config {
    var config = FilesEngine.Config{};
    if (try optionalU64(obj, "maxReadBytes", 1, 1024 * 1024 * 1024)) |value| config.max_read_bytes = @intCast(value);
    return config;
}

fn parseGraphConfig(obj: *const std.json.ObjectMap) !GraphEngine.Config {
    var config = GraphEngine.Config{};
    config.buffer_pool_bytes = try optionalU64(obj, "bufferPoolBytes", 1, 1024 * 1024 * 1024 * 1024);
    config.max_db_bytes = try optionalU64(obj, "maxDbBytes", 1, 8 * 1024 * 1024 * 1024 * 1024);
    config.max_threads = try optionalU64(obj, "maxThreads", 1, 256);
    config.checkpoint_threshold_bytes = try optionalU64(obj, "checkpointThresholdBytes", 1, 1024 * 1024 * 1024 * 1024);
    if (obj.get("autoCheckpoint")) |value| {
        if (value != .bool) return error.InvalidManagementResult;
        config.auto_checkpoint = value.bool;
    }
    return config;
}

fn requiredString(obj: *const std.json.ObjectMap, key: []const u8) ![]const u8 {
    const value = obj.get(key) orelse return error.InvalidManagementResult;
    if (value != .string) return error.InvalidManagementResult;
    return value.string;
}

fn optionalU64(obj: *const std.json.ObjectMap, key: []const u8, min: u64, max: u64) !?u64 {
    const value = obj.get(key) orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidManagementResult;
    const result: u64 = @intCast(value.integer);
    if (result < min or result > max) return error.InvalidManagementResult;
    return result;
}

test "management config applies SQLite initialization settings" {
    const config = try parseConfig(
        std.testing.allocator,
        .sql,
        "[{\"engine\":\"sqlite\",\"options\":{\"cacheKiB\":4096,\"busyTimeoutMs\":2500,\"tempStore\":\"file\"}}]",
    );
    switch (config) {
        .sql => |sql| {
            try std.testing.expectEqual(@as(u32, 4096), sql.cache_kib);
            try std.testing.expectEqual(@as(u32, 2500), sql.busy_timeout_ms);
            try std.testing.expectEqual(SqlEngine.TempStore.file, sql.temp_store);
        },
        else => unreachable,
    }
}

test "management config rejects an engine incompatible with the store type" {
    try std.testing.expectError(
        error.ManagementEngineMismatch,
        parseConfig(std.testing.allocator, .kv, "[{\"engine\":\"sqlite\",\"options\":{}}]"),
    );
}
