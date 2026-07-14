# Behemoth Transport

## Purpose
Implements the native transport layer for communication with the storage/runtime components.

## Wire transport
The library uses the Zig `zimq` module for both supported endpoint kinds:

- Unix paths become ZeroMQ `ipc://` endpoints.
- Host/port targets become ZeroMQ `tcp://` endpoints.

Each request or response remains the same flat Cap'n Proto byte buffer and is
sent as one ZeroMQ message. The existing C ABI keeps its numeric connection
handle so Bun consumers do not need changes; it is now an opaque ZMQ handle,
not a POSIX file descriptor. `libzimq.so` is installed beside `libtransport.so`
and is resolved through `$ORIGIN`.

## Responsibility Boundary
Owns protocol-level message transport and service exposure; does not own storage internals or business-level API semantics.

## Testing mock
`zig build mock` → `zig-out/lib/libtransport-mock.so` — in-memory implementation of the same C ABI (`src/mock.zig`): no sockets, no capnp, no storage backend. Point a consumer's transport lib path at it to run service tests against "storage" without infrastructure, then read data back through the same API (kv_get/kv_list/file_get) to assert what was written. Pool keys are isolated (multi-tenant routing is observable); requests against unregistered pool keys return null, like a dead connection. `transport_mock_marker()` distinguishes the mock from the real lib.
