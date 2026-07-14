import { expect, test } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { StorageConnection } from "../bun-transport/src/index";

const repoRoot = resolve(import.meta.dir, "..");

function defaultStorageBin(): string {
  if (process.env.STORAGE_BIN) return process.env.STORAGE_BIN;
  const arch = process.arch === "x64" ? "x86_64" : process.arch === "arm64" ? "aarch64" : process.arch;
  const candidates = [
    join(repoRoot, "storage/zig-out/bin/storage-test-host"),
    join(repoRoot, `storage/zig-out/bin/storage-${arch}-gnu`),
    join(repoRoot, "storage/zig-out/bin/storage"),
  ];
  return candidates.find((candidate) => existsSync(candidate)) ?? candidates[candidates.length - 1];
}

const storageBin = defaultStorageBin();

type StorageHandle = {
  dataDir: string;
  socketPath: string;
  proc: ReturnType<typeof Bun.spawn>;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function freeTcpPort(): Promise<number> {
  return await new Promise((resolvePort, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") {
        server.close();
        reject(new Error("failed to allocate tcp port"));
        return;
      }
      const port = addr.port;
      server.close(() => resolvePort(port));
    });
  });
}

function makeValue(seed: number, size = 8192): Buffer {
  const value = Buffer.allocUnsafe(size);
  for (let i = 0; i < value.length; i++) value[i] = (seed + i) % 251;
  return value;
}

async function waitForStorage(handle: StorageHandle): Promise<void> {
  const deadline = Date.now() + 10_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    const exitCode = (handle.proc as any).exitCode;
    if (exitCode !== null && exitCode !== undefined) break;
    if (existsSync(handle.socketPath)) {
      try {
        const conn = new StorageConnection(handle.socketPath, { operationTimeoutMs: 1000 });
        conn.ping();
        conn.close();
        return;
      } catch (err) {
        lastError = err;
      }
    }
    await sleep(50);
  }

  const exitCode = await Promise.race([handle.proc.exited, sleep(1).then(() => undefined)]);
  let stderr = "";
  try {
    stderr = await new Response(handle.proc.stderr).text();
  } catch {
  }
  throw new Error(`storage did not become ready; exit=${exitCode ?? "running"}; last=${String(lastError)}; stderr=${stderr}`);
}

async function startStorage(options: { dataDir?: string; valkey?: boolean; valkeyPort?: number } = {}): Promise<StorageHandle> {
  if (!existsSync(storageBin)) {
    throw new Error(`storage binary not found: ${storageBin}; run "cd storage && zig build" first`);
  }

  const dataDir = options.dataDir ?? mkdtempSync(join(tmpdir(), "behemoth-storage-"));
  const socketPath = join(dataDir, "storage.sock");
  const args = [storageBin, "start", "--data-dir", dataDir, "--socket", socketPath];
  if (options.valkey === false) {
    args.push("--no-valkey");
  } else {
    const port = options.valkeyPort ?? (await freeTcpPort());
    args.push("--valkey", `127.0.0.1:${port}`);
  }

  const proc = Bun.spawn(args, {
    cwd: repoRoot,
    env: {
      ...process.env,
      LD_LIBRARY_PATH: [
        join(repoRoot, "storage/.zig-cache/valkey-wrapper/x86_64-gnu/lib"),
        join(repoRoot, "storage/zig-out/lib"),
        process.env.LD_LIBRARY_PATH ?? "",
      ].filter(Boolean).join(":"),
    },
    stdout: "ignore",
    stderr: "ignore",
  });
  const handle = { dataDir, socketPath, proc };
  await waitForStorage(handle);
  return handle;
}

async function stopStorage(handle: StorageHandle): Promise<void> {
  try {
    const conn = new StorageConnection(handle.socketPath, { operationTimeoutMs: 1000, reconnectAttempts: 0 });
    conn.shutdown();
    conn.close();
  } catch {
  }

  const exited = await Promise.race([handle.proc.exited, sleep(5000).then(() => undefined)]);
  if (exited === undefined) {
    handle.proc.kill();
    await handle.proc.exited;
  }
}

async function runKvStressProcess(socketPath: string, workerId: number, stores: string[], ops: number): Promise<void> {
  const transportPath = join(repoRoot, "bun-transport/src/index.ts");
  const code = `
    const { StorageConnection } = await import(${JSON.stringify(transportPath)});
    const socketPath = process.env.KV_STRESS_SOCKET_PATH;
    const workerId = Number(process.env.KV_STRESS_WORKER_ID);
    const ops = Number(process.env.KV_STRESS_OPS);
    const stores = JSON.parse(process.env.KV_STRESS_STORES);
    const payload = Buffer.from("x".repeat(2048));
    const conn = new StorageConnection(socketPath, { operationTimeoutMs: 5000, reconnectAttempts: 0 });
    try {
      for (let i = 0; i < ops; i++) {
        const [ms, store] = stores[(i + workerId) % stores.length].split("/");
        const key = "parallel:" + workerId + ":" + (i % 160);
        conn.kvPut(ms, store, key, payload);
        if ((i & 1) === 0) conn.kvGet(ms, store, key);
        if (i % 5 === 0) {
          try {
            conn.kvDelete(ms, store, "parallel:" + workerId + ":" + ((i + 37) % 160));
          } catch (_) {}
        }
      }
    } finally {
      conn.close();
    }
  `;

  const proc = Bun.spawn(["bun", "--eval", code], {
    cwd: repoRoot,
    env: {
      ...process.env,
      KV_STRESS_SOCKET_PATH: socketPath,
      KV_STRESS_WORKER_ID: String(workerId),
      KV_STRESS_OPS: String(ops),
      KV_STRESS_STORES: JSON.stringify(stores),
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    const stderr = await new Response(proc.stderr).text();
    throw new Error(`kv stress worker ${workerId} failed with ${exitCode}: ${stderr}`);
  }
}

test("transport kv churn survives compact and server restart", async () => {
  const handle = await startStorage({ valkey: false });
  try {
    const samples = new Map<string, Buffer>();
    let conn = new StorageConnection(handle.socketPath, { operationTimeoutMs: 5000 });
    conn.create("stress-ms", "kv", "kv");

    for (let worker = 0; worker < 6; worker++) {
      for (let i = 0; i < 90; i++) {
        const key = `w${worker}:k${i}`;
        const value = makeValue(worker * 10_000 + i);
        conn.kvPut("stress-ms", "kv", key, value);
        if (i % 5 === 0) conn.kvDelete("stress-ms", "kv", key);
        else if (i % 17 === 0) samples.set(key, value);
      }
    }

    conn.kvCompact("stress-ms", "kv");
    const stats = conn.getStats("stress-ms", "kv");
    expect(stats.diskBytes > 0n).toBe(true);
    conn.close();

    await stopStorage(handle);
    const restarted = await startStorage({ dataDir: handle.dataDir, valkey: false });
    handle.proc = restarted.proc;
    conn = new StorageConnection(handle.socketPath, { operationTimeoutMs: 5000 });
    conn.open("stress-ms", "kv");

    for (const [key, expected] of samples) {
      const got = conn.kvGet("stress-ms", "kv", key);
      expect(got?.equals(expected)).toBe(true);
    }
    conn.close();
  } finally {
    await stopStorage(handle);
    rmSync(handle.dataDir, { recursive: true, force: true });
  }
}, 60_000);

test("parallel kv clients across stores do not crash embedded-valkey storage", async () => {
  const handle = await startStorage({ valkey: true });
  const stores = [
    "agent-ms/processing",
    "auth-ms/tokens",
    "dag-ms/processing",
    "calls-ms/recordings",
    "calls-ms/fragments",
    "threads-ms/threads",
    "static-ms/content",
    "access-ms/access",
    "oauth-ms/states",
    "companies-ms/kvs",
  ];

  try {
    const conn = new StorageConnection(handle.socketPath, { operationTimeoutMs: 5000, reconnectAttempts: 0 });
    for (const storeKey of stores) {
      const [ms, store] = storeKey.split("/");
      conn.create(ms, store, "kv");
    }
    conn.close();

    await Promise.all(stores.map((_, workerId) => runKvStressProcess(handle.socketPath, workerId, stores, 450)));

    const check = new StorageConnection(handle.socketPath, { operationTimeoutMs: 5000, reconnectAttempts: 0 });
    check.ping();
    check.close();
  } finally {
    await stopStorage(handle);
    rmSync(handle.dataDir, { recursive: true, force: true });
  }
}, 90_000);

test("embedded valkey cache path survives repeated same-dir restarts", async () => {
  const dataDir = mkdtempSync(join(tmpdir(), "behemoth-valkey-"));
  const valkeyPort = await freeTcpPort();
  let handle: StorageHandle | undefined;

  try {
    for (let cycle = 0; cycle < 3; cycle++) {
      handle = await startStorage({ dataDir, valkey: true, valkeyPort });
      const conn = new StorageConnection(handle.socketPath, { operationTimeoutMs: 5000, reconnectAttempts: 1 });
      if (cycle === 0) conn.create("cache-ms", "kv", "kv");
      else conn.open("cache-ms", "kv");

      for (let i = 0; i < 60; i++) {
        const sourceKey = `cycle:${cycle}:source:${i}`;
        const copyKey = `cycle:${cycle}:copy:${i}`;
        const value = makeValue(cycle * 1000 + i, 4096);
        conn.kvPut("cache-ms", "kv", sourceKey, value);
        const cacheKey = conn.kvGetToCache("cache-ms", "kv", sourceKey);
        expect(cacheKey).toBeTruthy();
        conn.kvPutFromCache("cache-ms", "kv", copyKey, cacheKey!);
        expect(conn.kvGet("cache-ms", "kv", copyKey)?.equals(value)).toBe(true);
      }

      conn.close();
      await stopStorage(handle);
      handle = undefined;
      await sleep(200);
    }
  } finally {
    if (handle) await stopStorage(handle);
    rmSync(dataDir, { recursive: true, force: true });
  }
}, 90_000);
