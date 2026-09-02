#!/usr/bin/env bash

set -euo pipefail

version=${1:-}
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-patch[1-9][0-9]*)?$ ]]; then
  echo "invalid release version: ${version:-<empty>}" >&2
  echo "expected X.Y.Z or X.Y.Z-patchN with N >= 1" >&2
  exit 1
fi

printf '%s\n' "$version"
