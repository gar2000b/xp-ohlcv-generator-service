#!/usr/bin/env bash
set -euo pipefail

git reset --hard origin/main \
  && git clean -fd \
  && rm -rf target \
  && mvn clean

