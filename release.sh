#!/usr/bin/env bash
set -e

# Always run from the directory where this script is located
cd "$(dirname "$0")"

rm -rf target

echo ">>> Releasing Maven build to Git..."
./release-mvn-to-git.sh

echo ">>> Building Docker image..."
./build-docker-image.sh

echo ">>> Pushing Docker image..."
./push-docker-image.sh

echo ">>> Release workflow complete!"

