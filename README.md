<p align="center">
  <img src="behemoth.png" width="560"/>
</p>

# Behemoth

Behemoth is a native multi-engine data platform focused on one practical goal: provide the right storage model for each workload behind a single runtime and transport surface.

It combines relational, key-value, columnar, vector, file, and graph capabilities in one service, while keeping the implementation compact and deployment-friendly.

## Why Behemoth

- One runtime for multiple data models instead of operating many separate databases per feature.
- Engine-per-workload approach: each store type uses a backend that is strong in that class.
- Native-first implementation (Zig/C/C++) with low operational overhead.
- Unified transport layer and JS/Bun bindings for integration from application services.

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

- One of the most battle-tested embedded databases in production ecosystems.
- Strong reliability profile, predictable behavior, and mature SQL tooling.
- Excellent compactness and deployment simplicity for embedded/service-local usage.

### `lmdbx` (KV Foundation)

- High-performance B+tree KV engine with very low overhead and strong read performance.
- Widely trusted in systems that need predictable latency and robustness.
- Efficient footprint and operational simplicity compared with heavyweight network KV servers.

### `sqlite-vec` (Vector Extension)

- Adds vector search to a compact SQLite-based stack.
- Useful when you want vector capability without deploying a dedicated heavy vector database.
- Good balance of practical performance and minimal operational complexity.

### `ryugraph` (Graph Foundation)

- Purpose-built graph engine for graph-native workloads.
- Better fit for traversal-heavy/query-connected data than forcing graph logic into pure relational layouts.
- Keeps graph concerns isolated while still integrated into the same Behemoth runtime.

### Filesystem Engine (`std.fs`)

- Direct and efficient for binary asset/blob persistence.
- Minimal abstraction overhead and strong portability.
- Compact by design: no extra database layer when object semantics are file-native.

### Column Layer (on top of SQLite)

- Reuses a mature SQL core while providing column-oriented access behavior for analytics-style workloads.
- Avoids introducing another heavy external dependency for this access model.
- Keeps deployment compact while covering a broader query profile.

## Architecture

- `storage/`: core storage runtime and multi-engine dispatch.
- `transport/`: Cap'n Proto transport layer and wire protocol implementation.
- `bun-transport/`: Bun/Node-compatible bindings for integration from JS services.

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
