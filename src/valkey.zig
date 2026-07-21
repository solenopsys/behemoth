const std = @import("std");
const fs_compat = @import("fs_compat.zig");

const c = @cImport({
    @cInclude("valkey_wrapper.h");
});

pub const Config = struct {
    enabled: bool = true,
    host: []const u8 = "127.0.0.1",
    port: u16 = 6379,
};

pub fn start(allocator: std.mem.Allocator, data_dir: []const u8, cfg: Config) !void {
    if (!cfg.enabled) return;

    const valkey_dir = try std.fmt.allocPrint(allocator, "{s}/valkey", .{data_dir});
    defer allocator.free(valkey_dir);
    try fs_compat.cwd().makePath(valkey_dir);

    const host_z = try allocator.dupeZ(u8, cfg.host);
    defer allocator.free(host_z);
    const dir_z = try allocator.dupeZ(u8, valkey_dir);
    defer allocator.free(dir_z);

    if (c.valkey_wrapper_start(host_z.ptr, cfg.port, dir_z.ptr) != c.VALKEY_WRAPPER_OK) {
        std.debug.print("valkey start failed: {s}\n", .{std.mem.span(c.valkey_wrapper_last_error())});
        return error.ValkeyStartFailed;
    }
}

pub fn stop(cfg: Config) void {
    if (!cfg.enabled) return;
    if (c.valkey_wrapper_stop() != c.VALKEY_WRAPPER_OK) {
        std.debug.print("valkey stop failed: {s}\n", .{std.mem.span(c.valkey_wrapper_last_error())});
    }
}

pub fn isRunning() bool {
    return c.valkey_wrapper_is_running() == 1;
}

pub fn put(key: []const u8, value: []const u8) !void {
    if (key.len == 0) return error.InvalidKey;
    if (c.valkey_wrapper_put(key.ptr, key.len, value.ptr, value.len) != c.VALKEY_WRAPPER_OK) {
        std.debug.print("valkey put failed: {s}\n", .{std.mem.span(c.valkey_wrapper_last_error())});
        return error.ValkeyPutFailed;
    }
}

pub fn get(allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
    if (key.len == 0) return error.InvalidKey;

    var out_ptr: ?[*]u8 = null;
    var out_len: usize = 0;
    const rc = c.valkey_wrapper_get(key.ptr, key.len, @ptrCast(&out_ptr), &out_len);
    if (rc == c.VALKEY_WRAPPER_NOT_FOUND) return null;
    if (rc != c.VALKEY_WRAPPER_OK) {
        std.debug.print("valkey get failed: {s}\n", .{std.mem.span(c.valkey_wrapper_last_error())});
        return error.ValkeyGetFailed;
    }
    defer c.valkey_wrapper_free(out_ptr);

    const raw = out_ptr orelse return try allocator.alloc(u8, 0);
    const copy = try allocator.alloc(u8, out_len);
    @memcpy(copy, raw[0..out_len]);
    return copy;
}

pub fn delete(key: []const u8) !i64 {
    if (key.len == 0) return error.InvalidKey;

    var deleted: i64 = 0;
    if (c.valkey_wrapper_delete(key.ptr, key.len, &deleted) != c.VALKEY_WRAPPER_OK) {
        std.debug.print("valkey delete failed: {s}\n", .{std.mem.span(c.valkey_wrapper_last_error())});
        return error.ValkeyDeleteFailed;
    }
    return deleted;
}
