#!/usr/bin/env bash
set -euo pipefail

rm -rf target \
  && mvn -B release:prepare release:perform \
  && git push

