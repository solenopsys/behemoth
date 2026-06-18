const std = @import("std");
const sync_compat = @import("sync_compat.zig");
const posix_compat = @import("posix_compat.zig");
const Allocator = std.mem.Allocator;
const manifest_mod = @import("manifest.zig");
const StoreType = manifest_mod.StoreType;

const SqlEngine = @import("engines/sql.zig").SqlEngine;
const KvEngine = @import("engines/kv.zig").KvEngine;
const ColumnEngine = @import("engines/column.zig").ColumnEngine;
const VectorEngine = @import("engines/vector.zig").VectorEngine;
const FilesEngine = @import("engines/files.zig").FilesEngine;
const GraphEngine = @import("engines/graph.zig").GraphEngine;

const default_worker_shards: usize = 4;
const max_worker_shards: usize = 16;

pub const WorkItem = struct {
    store_key: []const u8,
    op: Op,
    payload: []const u8,
    result: ?[]u8,
    done: sync_compat.ResetEvent,
    err: ?anyerror,
    exec_ctx: ?*anyopaque = null,
    exec_fn: ?*const fn (?*anyopaque) void = null,

    pub const Op = enum {
        exec_sql,
        query_sql,
        kv_put,
        kv_get,
        kv_delete,
        file_put,
        file_get,
        file_delete,
    };
};

/// Per-type worker thread — processes work items sequentially
pub const StoreWorker = struct {
    thread: ?std.Thread,
    queue: std.ArrayList(*WorkItem),
    queue_head: usize,
    allocator: Allocator,
    mutex: sync_compat.Mutex,
    cond: sync_compat.Condition,
    running: bool,
    store_type: StoreType,

    pub fn init(allocator: Allocator, store_type: StoreType) StoreWorker {
        return .{
            .thread = null,
            .queue = .empty,
            .queue_head = 0,
            .allocator = allocator,
            .mutex = .{},
            .cond = .{},
            .running = false,
            .store_type = store_type,
        };
    }

    pub fn start(self: *StoreWorker) !void {
        self.running = true;
        self.thread = try std.Thread.spawn(.{}, workerLoop, .{self});
    }

    pub fn stop(self: *StoreWorker) void {
        self.mutex.lock();
        self.running = false;
        self.cond.signal();
        self.mutex.unlock();

        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    pub fn submit(self: *StoreWorker, item: *WorkItem) void {
        self.mutex.lock();
        self.queue.append(self.allocator, item) catch {};
        self.cond.signal();
        self.mutex.unlock();
    }

    fn workerLoop(self: *StoreWorker) void {
        while (true) {
            self.mutex.lock();

            while (self.pendingCountLocked() == 0 and self.running) {
                self.cond.wait(&self.mutex);
            }

            if (!self.running and self.pendingCountLocked() == 0) {
                self.mutex.unlock();
                break;
            }

            const item = self.popLocked();
            self.mutex.unlock();

            if (item.exec_fn) |exec_fn| {
                exec_fn(item.exec_ctx);
            }

            // Signal done
            item.done.set();
        }
    }

    fn pendingCountLocked(self: *const StoreWorker) usize {
        return self.queue.items.len - self.queue_head;
    }

    fn popLocked(self: *StoreWorker) *WorkItem {
        const item = self.queue.items[self.queue_head];
        self.queue_head += 1;

        if (self.queue_head == self.queue.items.len) {
            self.queue.clearRetainingCapacity();
            self.queue_head = 0;
        } else if (self.queue_head >= 1024 and self.queue_head * 2 >= self.queue.items.len) {
            const remaining = self.queue.items.len - self.queue_head;
            std.mem.copyForwards(*WorkItem, self.queue.items[0..remaining], self.queue.items[self.queue_head..]);
            self.queue.shrinkRetainingCapacity(remaining);
            self.queue_head = 0;
        }

        return item;
    }

    pub fn deinit(self: *StoreWorker) void {
        self.queue.deinit(self.allocator);
    }
};

pub const StoreWorkerGroup = struct {
    workers: [max_worker_shards]StoreWorker,
    shard_count: usize,
    store_type: StoreType,

    pub fn init(allocator: Allocator, store_type: StoreType, shard_count: usize) StoreWorkerGroup {
        var group: StoreWorkerGroup = undefined;
        group.shard_count = clampShardCount(shard_count);
        group.store_type = store_type;
        for (0..max_worker_shards) |i| {
            group.workers[i] = StoreWorker.init(allocator, store_type);
        }
        return group;
    }

    pub fn start(self: *StoreWorkerGroup) !void {
        var started: usize = 0;
        errdefer {
            while (started > 0) {
                started -= 1;
                self.workers[started].stop();
            }
        }
        while (started < self.shard_count) : (started += 1) {
            try self.workers[started].start();
        }
    }

    pub fn stop(self: *StoreWorkerGroup) void {
        for (0..self.shard_count) |i| {
            self.workers[i].stop();
        }
    }

    pub fn first(self: *StoreWorkerGroup) *StoreWorker {
        return &self.workers[0];
    }

    pub fn workerForStore(self: *StoreWorkerGroup, store_key: []const u8) *StoreWorker {
        const shard = @as(usize, @intCast(std.hash.Wyhash.hash(0, store_key) % self.shard_count));
        return &self.workers[shard];
    }

    pub fn deinit(self: *StoreWorkerGroup) void {
        for (0..max_worker_shards) |i| {
            self.workers[i].deinit();
        }
    }
};

/// Thread pool — sharded workers per store type. Same store key is serialized;
/// different stores of the same type can run concurrently.
pub const ThreadPool = struct {
    sql_workers: StoreWorkerGroup,
    kv_workers: StoreWorkerGroup,
    column_workers: StoreWorkerGroup,
    vector_workers: StoreWorkerGroup,
    files_workers: StoreWorkerGroup,
    graph_workers: StoreWorkerGroup,

    pub fn init(allocator: Allocator) ThreadPool {
        const shard_count = detectWorkerShards();
        return .{
            .sql_workers = StoreWorkerGroup.init(allocator, .sql, shard_count),
            .kv_workers = StoreWorkerGroup.init(allocator, .kv, shard_count),
            .column_workers = StoreWorkerGroup.init(allocator, .column, shard_count),
            .vector_workers = StoreWorkerGroup.init(allocator, .vector, shard_count),
            .files_workers = StoreWorkerGroup.init(allocator, .files, shard_count),
            .graph_workers = StoreWorkerGroup.init(allocator, .graph, shard_count),
        };
    }

    pub fn start(self: *ThreadPool) !void {
        try self.sql_workers.start();
        errdefer self.sql_workers.stop();
        try self.kv_workers.start();
        errdefer self.kv_workers.stop();
        try self.column_workers.start();
        errdefer self.column_workers.stop();
        try self.vector_workers.start();
        errdefer self.vector_workers.stop();
        try self.files_workers.start();
        errdefer self.files_workers.stop();
        try self.graph_workers.start();
    }

    pub fn stop(self: *ThreadPool) void {
        self.sql_workers.stop();
        self.kv_workers.stop();
        self.column_workers.stop();
        self.vector_workers.stop();
        self.files_workers.stop();
        self.graph_workers.stop();
    }

    pub fn getWorker(self: *ThreadPool, store_type: StoreType) *StoreWorker {
        return switch (store_type) {
            .sql => self.sql_workers.first(),
            .kv => self.kv_workers.first(),
            .column => self.column_workers.first(),
            .vector => self.vector_workers.first(),
            .files => self.files_workers.first(),
            .graph => self.graph_workers.first(),
        };
    }

    pub fn getWorkerForStore(self: *ThreadPool, store_type: StoreType, store_key: []const u8) *StoreWorker {
        return switch (store_type) {
            .sql => self.sql_workers.workerForStore(store_key),
            .kv => self.kv_workers.workerForStore(store_key),
            .column => self.column_workers.workerForStore(store_key),
            .vector => self.vector_workers.workerForStore(store_key),
            .files => self.files_workers.workerForStore(store_key),
            .graph => self.graph_workers.workerForStore(store_key),
        };
    }

    pub fn deinit(self: *ThreadPool) void {
        self.sql_workers.deinit();
        self.kv_workers.deinit();
        self.column_workers.deinit();
        self.vector_workers.deinit();
        self.files_workers.deinit();
        self.graph_workers.deinit();
    }
};

fn detectWorkerShards() usize {
    if (posix_compat.getenv("BEHEMOTH_WORKER_SHARDS")) |raw| {
        if (std.fmt.parseUnsigned(usize, raw, 10)) |parsed| {
            return clampShardCount(parsed);
        } else |_| {}
    }
    if (posix_compat.getenv("STORAGE_WORKER_SHARDS")) |raw| {
        if (std.fmt.parseUnsigned(usize, raw, 10)) |parsed| {
            return clampShardCount(parsed);
        } else |_| {}
    }
    return default_worker_shards;
}

fn clampShardCount(value: usize) usize {
    if (value < 1) return 1;
    if (value > max_worker_shards) return max_worker_shards;
    return value;
}
