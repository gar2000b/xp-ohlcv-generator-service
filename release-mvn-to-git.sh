#!/usr/bin/env bash
set -euo pipefail

# Remove target directory - try multiple times with delays for Windows file locks
echo "Removing target directory..."
rm -rf target 2>/dev/null || true
sleep 1
rm -rf target 2>/dev/null || true
sleep 1
rm -rf target 2>/dev/null || true

# Wait longer for Windows to fully release file handles before Maven runs
# Maven's clean goal will also try to delete target, so handles must be released
echo "Waiting 2 seconds for file handles to fully release..."
sleep 2

mvn -B release:prepare release:perform && git push && git pull
