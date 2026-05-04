const std = @import("std");

pub fn sleep(ns: u64) void {
    var req = std.c.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    while (std.c.nanosleep(&req, &req) == -1) {}
}

pub const Mutex = struct {
    raw: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,

    pub fn lock(self: *Mutex) void {
        _ = std.c.pthread_mutex_lock(&self.raw);
    }

    pub fn unlock(self: *Mutex) void {
        _ = std.c.pthread_mutex_unlock(&self.raw);
    }
};

pub const Condition = struct {
    raw: std.c.pthread_cond_t = std.c.PTHREAD_COND_INITIALIZER,

    pub fn wait(self: *Condition, mutex: *Mutex) void {
        _ = std.c.pthread_cond_wait(&self.raw, &mutex.raw);
    }

    pub fn signal(self: *Condition) void {
        _ = std.c.pthread_cond_signal(&self.raw);
    }
};

pub const RwLock = struct {
    raw: std.c.pthread_rwlock_t = .{},

    pub fn lock(self: *RwLock) void {
        _ = std.c.pthread_rwlock_wrlock(&self.raw);
    }

    pub fn unlock(self: *RwLock) void {
        _ = std.c.pthread_rwlock_unlock(&self.raw);
    }

    pub fn lockShared(self: *RwLock) void {
        _ = std.c.pthread_rwlock_rdlock(&self.raw);
    }

    pub fn unlockShared(self: *RwLock) void {
        _ = std.c.pthread_rwlock_unlock(&self.raw);
    }
};

pub const ResetEvent = struct {
    mutex: Mutex = .{},
    cond: Condition = .{},
    is_set: bool = false,

    pub fn set(self: *ResetEvent) void {
        self.mutex.lock();
        self.is_set = true;
        self.cond.signal();
        self.mutex.unlock();
    }

    pub fn wait(self: *ResetEvent) void {
        self.mutex.lock();
        while (!self.is_set) {
            self.cond.wait(&self.mutex);
        }
        self.mutex.unlock();
    }

    pub fn isSet(self: *ResetEvent) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.is_set;
    }

    pub fn timedWait(self: *ResetEvent, _: u64) !void {
        self.wait();
    }
};
