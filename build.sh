#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

TARGET="${TARGET:-x86_64-linux-musl}"
OPTIMIZE="${OPTIMIZE:-ReleaseSafe}"
CONVERGED_ROOT="${CONVERGED_ROOT:-../../../../converged-portal}"

case "$TARGET" in
    x86_64-linux-musl) ARTIFACT_TARGET=x86_64-musl ;;
    aarch64-linux-musl) ARTIFACT_TARGET=aarch64-musl ;;
    *) echo "storage requires an Alpine/musl target: x86_64-linux-musl or aarch64-linux-musl" >&2; exit 2 ;;
esac

# Storage/Valkey outputs are prepared by the storage build pipeline. Running
# this build here would also rebuild its host integration target.
test -f "zig-out/bin/storage-$ARTIFACT_TARGET" || {
    echo "missing zig-out/bin/storage-$ARTIFACT_TARGET; build storage first" >&2
    exit 1
}
test -f "zig-out/lib/libvalkey-$ARTIFACT_TARGET.so" || {
    echo "missing zig-out/lib/libvalkey-$ARTIFACT_TARGET.so; build storage first" >&2
    exit 1
}
(cd "$CONVERGED_ROOT/native/wrapers/zimq" && zig build "-Dtarget=$TARGET" "-Doptimize=$OPTIMIZE")

mkdir -p .container-libs
cp "$CONVERGED_ROOT/native/wrapers/zimq/zig-out/lib/libzimq-$ARTIFACT_TARGET.so" .container-libs/libzimq.so
