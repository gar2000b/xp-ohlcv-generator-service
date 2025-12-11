#!/usr/bin/env bash
set -euo pipefail

IMAGE="gar2000b/xp-ohlcv-generator-service"

# Find the version from the single jar in target/
shopt -s nullglob
JARS=(target/*.jar)

if [ "${#JARS[@]}" -ne 1 ]; then
  echo "❌ Expected exactly one JAR in target/, found ${#JARS[@]}." >&2
  exit 1
fi

JAR="${JARS[0]}"
BASENAME="$(basename "$JAR" .jar)"
VERSION="${BASENAME##*-}"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "❌ Failed to parse version (got '$VERSION')" >&2
  exit 1
fi

echo "Pushing Docker images for version: $VERSION"

# Push both versioned and latest tags
docker push "$IMAGE:$VERSION"
docker push "$IMAGE:latest"

echo "Push complete!"
echo "Image available on Docker Hub:"
echo "    https://hub.docker.com/r/$IMAGE/tags"

