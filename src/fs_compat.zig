const std = @import("std");
const posix_compat = @import("posix_compat.zig");

var runtime_io: ?std.Io = null;

pub fn setIo(new_io: std.Io) void {
    runtime_io = new_io;
}

fn io() std.Io {
    return runtime_io orelse @panic("fs_compat io is not initialized");
}

pub fn getIo() std.Io {
    return io();
}

pub fn cwd() Dir {
    return .{ .inner = std.Io.Dir.cwd() };
}

pub const File = struct {
    inner: std.Io.File,

    pub fn close(self: File) void {
        self.inner.close(io());
    }

    pub fn stat(self: File) !std.Io.File.Stat {
        return self.inner.stat(io());
    }

    pub fn writeAll(self: File, data: []const u8) !void {
        try posix_compat.writeAll(self.inner.handle, data);
    }

    pub fn readToEndAlloc(self: File, allocator: std.mem.Allocator, max_bytes: usize) ![]u8 {
        const st = try self.stat();
        if (st.size > max_bytes) return error.FileTooBig;
        const buf = try allocator.alloc(u8, @intCast(st.size));
        errdefer allocator.free(buf);
        const n = try std.posix.read(self.inner.handle, buf);
        return buf[0..n];
    }

    pub fn seekTo(self: File, offset: u64) !void {
        if (std.c.lseek(self.inner.handle, @intCast(offset), std.c.SEEK.SET) == -1) return error.Unexpected;
    }

    pub fn read(self: File, buf: []u8) !usize {
        return std.posix.read(self.inner.handle, buf);
    }

    pub fn writer(self: File, buffer: []u8) std.Io.File.Writer {
        return self.inner.writer(io(), buffer);
    }
};

pub const Dir = struct {
    inner: std.Io.Dir,

    pub const Entry = std.Io.Dir.Entry;

    pub fn close(self: Dir) void {
        self.inner.close(io());
    }

    pub fn makePath(self: Dir, sub_path: []const u8) !void {
        try self.inner.createDirPath(io(), sub_path);
    }

    pub fn openDir(self: Dir, sub_path: []const u8, options: std.Io.Dir.OpenOptions) !Dir {
        return .{ .inner = try self.inner.openDir(io(), sub_path, options) };
    }

    pub fn openFile(self: Dir, sub_path: []const u8, options: std.Io.Dir.OpenFileOptions) !File {
        return .{ .inner = try self.inner.openFile(io(), sub_path, options) };
    }

    pub fn createFile(self: Dir, sub_path: []const u8, options: std.Io.Dir.CreateFileOptions) !File {
        return .{ .inner = try self.inner.createFile(io(), sub_path, options) };
    }

    pub fn deleteFile(self: Dir, sub_path: []const u8) !void {
        try self.inner.deleteFile(io(), sub_path);
    }

    pub fn access(self: Dir, sub_path: []const u8, options: std.Io.Dir.AccessOptions) !void {
        try self.inner.access(io(), sub_path, options);
    }

    pub fn statFile(self: Dir, sub_path: []const u8) !std.Io.File.Stat {
        return self.inner.statFile(io(), sub_path, .{});
    }

    pub fn readFileAlloc(self: Dir, allocator: std.mem.Allocator, sub_path: []const u8, max_bytes: usize) ![]u8 {
        const file = try self.openFile(sub_path, .{});
        defer file.close();
        return file.readToEndAlloc(allocator, max_bytes);
    }

    pub fn iterate(self: Dir) Iterator {
        return .{ .inner = self.inner.iterate() };
    }

    pub fn walk(self: Dir, allocator: std.mem.Allocator) !Walker {
        return .{ .inner = try self.inner.walk(allocator) };
    }
};

pub const Iterator = struct {
    inner: std.Io.Dir.Iterator,

    pub fn next(self: *Iterator) !?std.Io.Dir.Entry {
        return self.inner.next(io());
    }
};

pub const Walker = struct {
    inner: std.Io.Dir.Walker,

    pub fn next(self: *Walker) !?std.Io.Dir.Walker.Entry {
        return self.inner.next(io());
    }

    pub fn deinit(self: *Walker) void {
        self.inner.deinit();
    }
};
