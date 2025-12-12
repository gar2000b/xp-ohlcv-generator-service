#!/usr/bin/env bash
set -euo pipefail

echo "Building Docker image for xp-ohlcv-generator-service ..."

# Find the single jar in target/
shopt -s nullglob
JARS=(target/*.jar)

if [ "${#JARS[@]}" -ne 1 ]; then
  echo "❌ Expected exactly one JAR in target/, found ${#JARS[@]}." >&2
  exit 1
fi

JAR="${JARS[0]}"
BASENAME="$(basename "$JAR" .jar)"
VERSION="${BASENAME##*-}"

# Validate version format like #.#.#
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "❌ Invalid version format parsed from '$BASENAME'" >&2
  exit 1
fi

echo "Using version: $VERSION"
echo "Found JAR: $JAR"

IMAGE="gar2000b/xp-ohlcv-generator-service"

echo "Building Docker image: $IMAGE:$VERSION"
docker build \
  --build-arg APP_VERSION="$VERSION" \
  -t "$IMAGE:$VERSION" .

echo "Tagging image as latest"
docker tag "$IMAGE:$VERSION" "$IMAGE:latest"

echo "✅ All done."
echo "You can now test with:"
echo "    docker run --rm -p 8083:8083 $IMAGE:$VERSION"

