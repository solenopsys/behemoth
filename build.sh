#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

TARGET="${TARGET:-x86_64-linux-musl}"
OPTIMIZE="${OPTIMIZE:-ReleaseSafe}"

case "$TARGET" in
    x86_64-linux-musl) ARTIFACT_TARGET=x86_64-musl ;;
    aarch64-linux-musl) ARTIFACT_TARGET=aarch64-musl ;;
    x86_64-linux-gnu) ARTIFACT_TARGET=x86_64-gnu ;;
    aarch64-linux-gnu) ARTIFACT_TARGET=aarch64-gnu ;;
    *) echo "storage requires an x86_64/aarch64 Linux GNU or musl target" >&2; exit 2 ;;
esac

zig build "-Dtarget=$TARGET" "-Doptimize=$OPTIMIZE" --prefix "zig-out/$ARTIFACT_TARGET"
