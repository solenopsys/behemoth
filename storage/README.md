# Behemoth Storage

## Purpose
Implements the core native storage engine used by platform services.

## Responsibility Boundary
Owns low-level persistence and data layout mechanics; does not own network protocol handling or application-domain validation rules.

## JavaScript Engine Management

`storage` can load the QuickJS-ng wrapper already used by `native/centimanus` and execute a JavaScript initialization function before every store handle is opened. The script is re-read on each open, so replacing it changes subsequent store initialization without rebuilding or restarting the storage engine.

Build the wrapper once:

```bash
cd ../../wrapers/qjs
zig build
```

Enable management for the storage server:

```bash
BEHEMOTH_QJS_LIB=../../wrapers/qjs/zig-out/lib/libqjs.so \
BEHEMOTH_STORAGE_JS_SCRIPT=scripts/default-management.js \
zig build run -- start --no-valkey --data-dir ./data
```

The script must define `configureStore(store, engines)`. `store` contains `{ key, type }`. `engines` exposes type-specific interfaces:

| Store type | JS interface | Initialization parameters |
| --- | --- | --- |
| `sql`, `column`, `vector` | `engines.sqlite.configure(options)` | `cacheKiB`, `busyTimeoutMs`, `tempStore` (`memory` or `file`) |
| `kv` | `engines.kv.configure(options)` | `mapLowerMiB`, `mapNowMiB`, `mapUpperMiB`, `mapGrowthMiB`, `mapShrinkMiB`, `autoCompactOnOpen` |
| `files` | `engines.files.configure(options)` | `maxReadBytes` |
| `graph` | `engines.graph.configure(options)` | `bufferPoolBytes`, `maxDbBytes`, `maxThreads`, `autoCheckpoint`, `checkpointThresholdBytes` |

The JavaScript interface records a typed initialization command; Zig validates that it belongs to the current store type and applies it to the native engine. The script never receives a database handle.

## build
podman build -f Containerfile -t behemoth-storage .
