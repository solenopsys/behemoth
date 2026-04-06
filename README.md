<p align="center">
  <img src="behemoth.png" width="560"/>
</p>

# Behemoth

## Purpose
Provides the native data platform for the portal: a unified runtime for storage and transport services.

## Responsibility Boundary
Owns native data execution and transport foundations; does not own product business logic, UI workflows, or microservice domain rules.

## Storage Types

| Store Type | What It Is For | Implementation Engine |
| --- | --- | --- |
| `sql` | Relational queries and transactional records | `sqlite3` |
| `kv` | Fast key-value access | `lmdbx` |
| `column` | Column-oriented analytics-style access | `column` wrapper on `sqlite3` |
| `vector` | Embeddings and similarity search | `sqlite3` + `sqlite-vec` |
| `files` | Binary/blob file storage | Filesystem-backed engine (`std.fs`) |
| `graph` | Graph data and graph queries | `ryugraph` |

## Structure

- `storage/`: native storage engine
- `transport/`: Cap'n Proto transport layer
- `bun-transport/`: Bun/Node.js transport bindings
