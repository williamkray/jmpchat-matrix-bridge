#!/usr/bin/env bash
set -e

# Build inside Docker to ensure a repeatable environment with all dependencies
# (CGO, libopus, sqlite-dev). The binary is extracted to the current directory.
# Extra arguments are passed to docker build (e.g. -t williamkray/jmpchat-matrix-bridge:v0.6.0).
docker build -t jmpchat-matrix-bridge $(./build-args.sh) "$@" .
container_id=$(docker create jmpchat-matrix-bridge)
docker cp "$container_id:/app/jmpchat-matrix-bridge" ./jmpchat-matrix-bridge
docker rm "$container_id" > /dev/null
echo "Built: ./jmpchat-matrix-bridge"
