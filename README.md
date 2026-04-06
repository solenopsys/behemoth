# Behemoth - Storage

<p align="center">
  <img src="behemoth.png" width="672"/>
</p>
 
Behemoth executes storage work in native services outside application event loops.  
Each service or tenant can own a compact isolated store boundary.

Behemoth is a native multi-engine data platform focused on one practical goal: provide the right storage model for each workload behind a single runtime and transport surface.

It combines relational, key-value, columnar, vector, file, and graph capabilities in one service, while keeping the implementation compact and deployment-friendly.



## Problem Statement

Behemoth is designed to solve a concrete bottleneck: I/O pressure in application runtime paths can block event loops and reduce effective throughput under mixed workloads.

The project moves critical data-path operations into native components and a dedicated transport/runtime layer to avoid event-loop stalls, reduce latency spikes, and maximize useful work per CPU time slice.

It also addresses architectural coupling problems typical for shared monolithic databases in SaaS systems: large shared indexes, tenant data mixing risk, and difficult service-level evolution.

## Contrast: Traditional vs Behemoth

- Traditional way: app runtime + shared Postgres/Redis/Vector DB + cross-service coupling.
- Behemoth way: app runtime + native transport + per-service/per-tenant micro-stores.

## Public Benchmark Snapshot

Open benchmark and vendor-published test highlights behind the selected engines:

- `sqlite3`: SQLite reports small-blob read/write workloads about `35% faster` than direct file I/O in its `kvtest` scenario, with about `20%` lower disk usage for the same blob dataset.
- `lmdbx`: libmdbx reports `10–20%` higher CRUD benchmark performance than LMDB in tmpfs scenarios, and up to `30%` with specific build options.
- `sqlite-vec`: public sqlite-vec benchmark results (author-run, Mac M1) show query times around `33ms` on SIFT1M (`vec0` mode) vs `46ms` DuckDB and `136ms` NumPy in the same brute-force comparison setup.

## Target Environments

Behemoth targets two extremes with the same architecture:

- Super-compact platforms where memory and storage footprint must stay minimal.
- Cloud environments where high throughput per core and predictable latency are required.

## Operations Model

Behemoth is designed for low-ops operation:

- Stores run on a single node in the current model.
- No dedicated database administration layer is required for routine service operation.
- Operational focus stays on application and service lifecycle, not heavyweight DB fleet management.

## Why Behemoth

- One runtime for multiple data models instead of operating many separate databases per feature.
- Engine-per-workload approach: each store type uses a backend that is strong in that class.
- Native-first implementation (Zig/C/C++) with low operational overhead.
- Unified transport layer with Bun integration via `bun-transport`.
- Microservice-first storage isolation: each service owns its own store boundary.
- Tenant-first storage isolation: each business tenant can receive dedicated micro-stores.

## SaaS Isolation Model

Behemoth is designed as a microservice storage platform for SaaS environments:

- Each business tenant gets isolated micro-storage units with minimal memory usage.
- Each microservice keeps its own isolated storage boundary.
- No cross-tenant data mixing by design at storage layout level.
- No cross-service index coupling by design.

This model improves security, tenancy isolation, and operational predictability for business cloud deployments.

## Why Small Is Better Here

Instead of one ever-growing shared database, Behemoth favors many compact stores:

- Smaller indexes per store, which stay fast and cache-friendly.
- Lower risk of global index/database bloat.
- Better blast-radius control: one store issue does not degrade the full tenant universe.
- Easier microservice evolution, versioning, and upgrades without global schema lockstep.
- Better horizontal data distribution options because data is naturally partitioned.

## Fault Isolation And Recovery

Store-level isolation also improves resilience and recovery behavior:

- If one isolated store is corrupted (rare), other stores remain intact.
- Other microservices continue operating because their storage boundaries are independent.
- Recovery can be targeted to the affected store/service instead of restoring a global monolithic database.
- Per-microservice dumps can be produced independently, often in megabytes or less for compact service datasets.

## Replication Roadmap

For clustered deployments, the planned replication model is:

- Single writer per store/shard.
- Multiple read copies/replicas.
- Replica convergence via disk-level synchronization workflows.

This keeps the write path deterministic while enabling scale-out reads and resilient recovery topologies.

## Purpose

Provide a unified native data runtime for platform services that need different storage models without fragmenting the architecture into many unrelated data stacks.

## Responsibility Boundary

Behemoth owns native storage and transport foundations.
It does not own product business workflows, UI logic, or domain orchestration policies.

## Storage Types

| Store Type | Primary Use | Engine Under The Hood |
| --- | --- | --- |
| `sql` | Transactions, relational data, standard SQL access | `sqlite3` |
| `kv` | Low-latency key-value access and range/prefix operations | `lmdbx` |
| `column` | Column-oriented analytics-like access patterns | `column` module over `sqlite3` |
| `vector` | Embeddings and similarity search | `sqlite3` + `sqlite-vec` |
| `files` | Binary/blob file persistence | Filesystem-backed engine (`std.fs`) |
| `graph` | Graph-shaped data and graph traversal/query workloads | `ryugraph` |

## Engine Advantages

### `sqlite3` (SQL Foundation)

`sqlite3` is one of the most battle-tested embedded SQL engines and remains a strong compact default for service-local relational workloads. In SQLite's open `kvtest` measurements for small blobs, database access is reported around `35%` faster than direct files in the tested setup, while using about `20%` less disk for the same data shape.

### `lmdbx` (KV Foundation)

`lmdbx` gives a high-performance embedded KV path with mmap-based design and low operational overhead. In libmdbx public CRUD benchmark notes, it is reported as typically `10–20%` faster than LMDB, and up to `30%` faster under specific build settings in in-memory/tmpfs-like scenarios.

### `sqlite-vec` (Vector Extension)

`sqlite-vec` provides vector search in a compact SQLite-centric stack, which fits local and micro-store deployments better than heavyweight external vector services. In public sqlite-vec benchmark posts (author-run brute-force tests), reported query times include roughly `33ms` for `vec0` on SIFT1M test conditions, with faster modes shown for static/in-memory variants.

### `ryugraph` (Graph Foundation)

`ryugraph` is used as the graph-specialized backend so graph traversal and graph-shaped query patterns stay isolated from SQL/KV concerns. Public, standardized, independently reproduced benchmark figures for this exact engine are currently less established than for SQLite/LMDB-family engines, so Behemoth treats it as a specialized graph path chosen for workload fit, not generic graph leaderboard claims.

### Filesystem Engine (`std.fs`)

The filesystem-backed engine keeps binary/blob persistence simple and portable with near-zero abstraction overhead. It is intentionally minimal: for file-native object semantics, skipping an extra database layer reduces both footprint and operational complexity.

### Column Layer (on top of SQLite)

The column layer reuses SQLite foundations to provide analytics-style access behavior without introducing another heavy external service. This keeps the deployment surface compact while extending query patterns beyond plain row-oriented access.

## Benchmark Notes

Benchmark numbers above come from publicly available engine documentation and benchmark posts. They are workload-specific, hardware-dependent, and not universal; always validate on your own dataset and deployment profile.

## Benchmark Sources

- SQLite: `Internal Versus External BLOBs` (`kvtest`) — https://www.sqlite.org/intern-v-extern-blob.html
- libmdbx: README benchmark note vs LMDB (`+10–20%`, up to `+30%`) — https://github.com/isar/libmdbx
- sqlite-vec: public benchmark post by project author — https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/

## Transport Architecture

Behemoth transport is optimized for very fast local/native communication:

- Unix domain sockets for low-overhead local IPC.
- Cap'n Proto as the wire format and RPC layer.
- Native transport implementation (`transport/`) with Bun integration (`bun-transport/`).

This combination minimizes serialization and syscalls overhead compared to heavier network-first stacks.

## Minimal Execution Example (bun-transport)

```ts
import { StorageConnection } from "bun-transport";

const conn = new StorageConnection({
  kind: "unix",
  socketPath: "/run/behemoth.sock",
});

// 1) create/open isolated stores
conn.open("ms-sales", "tenant-42-kv", "kv");
conn.open("ms-sales", "tenant-42-sql", "sql");

// 2) put/get in KV store
conn.kvPut("ms-sales", "tenant-42-kv", "status", Buffer.from("active"));
const status = conn.kvGet("ms-sales", "tenant-42-kv", "status");
console.log(status?.toString("utf8")); // active

// 3) SQL query in SQL store
conn.execSql("ms-sales", "tenant-42-sql", "create table if not exists users(id integer primary key, name text)");
conn.execSql("ms-sales", "tenant-42-sql", "insert into users(name) values ('Alice')");
const rows = conn.querySql("ms-sales", "tenant-42-sql", "select id, name from users order by id desc limit 1");
console.log(rows);
```

## Concurrency Model

Behemoth uses a multi-threaded execution model to isolate heavy operations:

- Engine-specific execution paths run in dedicated threads.
- Work is separated by engine/store type to reduce cross-workload contention.
- Transport and storage responsibilities are decoupled to avoid blocking critical request paths.

The practical result is better CPU quantum utilization and more stable latency under mixed traffic.

## Built-In Store Metadata

Each store carries metadata used by the runtime for lifecycle control:

- Store manifest metadata (`manifest.json`) to track store identity/type.
- Migration tracking hooks for controlled schema/data evolution.
- Dump/archive lifecycle support for backup and portability operations.

This makes migration and dump management part of the engine workflow, not an external afterthought.

## Architecture

- `storage/`: core storage runtime and multi-engine dispatch.
- `transport/`: Cap'n Proto transport layer and wire protocol implementation.
- `bun-transport/`: Bun bindings for integration with the native transport layer.

## Data Layout

```text
<data-dir>/
  <ms-name>/
    <store-name>/
      manifest.json
      data/
        data.db      # sqlite-backed stores
        mdbx.dat     # lmdbx-backed stores
        ...          # files/graph/vector specific artifacts
```

## Build (Storage)

```bash
cd storage
zig build -Dall -Doptimize=ReleaseFast
```

## Build (Transport)

```bash
cd transport
zig build -Dall -Doptimize=ReleaseFast
```

## Build (Container)

```bash
podman build --layers -f storage/Containerfile -t behemoth .
```
