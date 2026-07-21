<div align="center">
  <img src="behemoth.png" width="672"/>
</div>
 
# Behemoth - Storage

![Status](https://img.shields.io/badge/status-active%20development-orange)
![Build](https://img.shields.io/badge/build-manual%20checks-blue)
![Version](https://img.shields.io/badge/version-0.1.0-blueviolet)
![License](https://img.shields.io/badge/license-Apache--2.0-green)

Behemoth is a storage layer for true microservices: small, specialized stores instead of a single shared database.

Behemoth executes storage work in native services outside application event loops.  
Each service or tenant can own a compact isolated store boundary. It combines relational, key-value, columnar, vector, file, and graph capabilities in one service, while keeping the implementation compact and deployment-friendly.

## Problem Statement

Behemoth is designed to solve a concrete bottleneck: I/O pressure in application runtime paths can block event loops and reduce effective throughput under mixed workloads.

The project moves critical data-path operations into native components and a dedicated transport/runtime layer to avoid event-loop stalls, reduce latency spikes, and maximize useful work per CPU time slice.

It also addresses architectural coupling problems typical for shared monolithic databases in SaaS systems: large shared indexes, tenant data mixing risk, and difficult service-level evolution.

## Contrast: Traditional vs Behemoth

| Dimension | Traditional (Shared DB Stack) | Behemoth (Micro-Store Stack) |
| --- | --- | --- |
| Runtime coupling | App runtime contends with shared DB pressure | App orchestration is separated from native storage execution |
| Data topology | Large shared databases per environment | Per-service and per-tenant compact stores |
| Index growth | Global index bloat over time | Small local indexes per store boundary |
| Fault blast radius | Single DB failures can impact many services/tenants | Store-level isolation contains failures |
| Migrations and dumps | Global coordination and heavy snapshots | Targeted per-service migration and compact dumps |
| Horizontal scaling | Requires heavier cross-service partitioning plans | Naturally partitioned by service/tenant boundaries |

## Public Benchmark Snapshot

Open benchmark and vendor-published test highlights behind the selected engines:

- `sqlite3`: SQLite reports small-blob read/write workloads about `35% faster` than direct file I/O in its `kvtest` scenario, with about `20%` lower disk usage for the same blob dataset.
- `lmdbx`: one of the fastest embedded KV engines in practice; public benchmark notes show very high throughput, including million-keys-per-second class read performance depending on workload and hardware.
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
- Shared transport layer from `navite/libs/transport` with Bun integration via
  `navite/libs/cruller-transport` (`bun-transport`).
- Microservice-first storage isolation: each service owns its own store boundary.
- Tenant-first storage isolation: each business tenant can receive dedicated micro-stores.

## SaaS Isolation Model

Behemoth is designed as a microservice storage platform for SaaS environments:

- A `scope` is the primary storage-placement boundary. By default each
  business scope has its own Behemoth instance and its own isolated
  micro-storage units.
- Each microservice keeps its own isolated storage boundary.
- No cross-tenant data mixing by design at storage layout level.
- No cross-service index coupling by design.

This model improves security, tenancy isolation, and operational predictability for business cloud deployments.

### Scaling one scope

One scope is not permanently bound to one machine. When its workload grows,
the stores of that scope may be split across several Behemoth instances and
cluster nodes. This keeps the scope and its data ownership intact while
isolating resource-heavy workloads from business-critical ones.

For example, the stores for logs and telemetry of one customer can run on a
separate Behemoth instance from the customer's transactional business
stores. Fujin routes each logical storage address to the instance that owns
it; clients keep using the logical address and do not need to know which
node currently serves the store.

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

## Status

- Stage: active development with production-oriented architecture.
- Runtime model today: single-node, isolated micro-stores.
- Replication model: currently roadmap (`single writer + replicated read copies`).
- License: Apache License 2.0 (`LICENSE`) with copyright holder `Aleksei Shtorm`.

## Purpose

Provide a unified native data runtime for platform services that need different storage models without fragmenting the architecture into many unrelated data stacks.

## Responsibility Boundary

Behemoth owns the native storage foundation and consumes the shared transport.
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

`lmdbx` is one of the fastest embedded KV engines, with mmap-based design, strong low-latency behavior, and very high throughput. In practical benchmark profiles it is commonly discussed in the million-keys-per-second class for reads (hardware/workload dependent), which is why it is used as the KV foundation in Behemoth.

### `sqlite-vec` (Vector Extension)

`sqlite-vec` provides vector search in a compact SQLite-centric stack, which fits local and micro-store deployments better than heavyweight external vector services. In public sqlite-vec benchmark posts (author-run brute-force tests), reported query times include roughly `33ms` for `vec0` on SIFT1M test conditions, with faster modes shown for static/in-memory variants.

### `ryugraph` (Graph Foundation)

`ryugraph` is used as the graph-specialized backend so graph traversal and graph-shaped query patterns stay isolated from SQL/KV concerns. Public, standardized, independently reproduced benchmark figures for this exact engine are currently less established than for SQLite/LMDB-family engines, so Behemoth treats it as a specialized graph path chosen for workload fit, not generic graph leaderboard claims. References: https://ryugraph.io/docs and local wrapper sources in `native/wrapers/ryugraph`.

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
- RyuGraph docs — https://ryugraph.io/docs

## Transport Architecture

Behemoth no longer owns a dedicated storage transport. It is a regular peer
of the cluster messaging system:

```
storage client -> Fujin -> Behemoth -> storage engine
```

- Behemoth connects to Fujin through the universal `transport.Service`
  connector, implemented with ZMQ `DEALER` on the peer side and Fujin's
  single `ROUTER` on the broker side.
- The cluster envelope is universal: it carries routing, correlation,
  deadline, scope, user and codec metadata. Fujin routes it in exactly the
  same way as a request for any other service.
- Behemoth registers its blanket peer name `behemoth` and registers each
  known store as `storage:<ms>/<store>`. A store created at runtime is
  registered immediately.
- A deployment normally creates one Behemoth instance per scope. A busy
  scope may have several instances, each registering the stores it owns;
  Fujin maps the logical storage address to the owning peer.
- `navite/libs/transport` provides the native connector and envelope;
  `navite/libs/cruller-transport` exposes the equivalent universal
  messaging connector to Bun through `libmessage.so`.

The only storage-specific protocol that remains is the **payload**. After
Fujin has routed a request to Behemoth, Behemoth decodes the payload using
the existing Cap'n Proto `wire` request/response schema. That schema
describes storage operations such as open, SQL, KV, files and dumps; it is
not a network transport and it does not choose a socket, endpoint or route.

This separation is intentional:

| Layer | Responsibility |
| --- | --- |
| Universal messaging envelope | Routing through Fujin, request correlation, scope/user context, errors and streaming. |
| Behemoth Cap'n Proto `wire` payload | Storage command and storage result representation. |
| Storage engine | Execution against SQL, KV, files, vector, graph or column data. |

The legacy `StorageConnection` API in `cruller-transport` remains only as a
compatibility adapter for callers that still speak the storage `wire`
payload. New cross-process communication must use the universal messaging
connector rather than a direct per-storage socket.

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

- `src/`: core storage runtime and multi-engine dispatch.
- `../../libs/transport/`: shared message envelope and ZeroMQ endpoints.
- `../../libs/cruller-transport/`: Bun FFI bindings for universal messaging;
  it also keeps a temporary compatibility client for the storage payload.

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
cd converged/navite/apps/behemoth
zig build -Dall -Doptimize=ReleaseFast
```

## Build (Transport)

```bash
cd converged/navite/libs/transport
zig build -Dall -Doptimize=ReleaseFast
```

## Build (Container)

```bash
podman build --layers -f converged/navite/apps/behemoth/Containerfile -t behemoth .
```
