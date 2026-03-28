<p align="center">
  <img src="behemoth.png" width="300"/>
</p>

# Behemoth

Multi-engine storage server written in Zig. Supports SQL, key-value, column, vector, files, and graph store types in a single process, exposed over a Cap'n Proto transport layer.

## Structure

```
behemoth/
  storage/     # core storage engine (Zig)
  transport/   # Cap'n Proto transport layer (Zig + C++)
  bun-transport/  # Bun/Node.js client bindings
```

## Build

```bash
cd storage
zig build -Dall -Doptimize=ReleaseFast
```

## Container

```bash
podman build --layers -f storage/Containerfile -t behemoth .
```

## Data layout

```
<data-dir>/
  <ms-name>/
    <store-name>/
      manifest.json
      data/
        data.db     # SQL
        mdbx.dat    # KV
        ...         # FILES
```
