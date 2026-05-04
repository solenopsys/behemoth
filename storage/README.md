# Behemoth Storage

## Purpose
Implements the core native storage engine used by platform services.

## Responsibility Boundary
Owns low-level persistence and data layout mechanics; does not own network protocol handling or application-domain validation rules.

## build
podman build -f Containerfile -t behemoth-storage .
