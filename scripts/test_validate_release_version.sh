#!/usr/bin/env bash

set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
validator="${repo_root}/scripts/validate-release-version.sh"

for version in 0.5.1 10.20.30 0.5.0-patch1 0.5.0-patch12; do
  test "$("$validator" "$version")" = "$version"
done

for version in '' v0.5.1 0.5 0.5.0-alpha 0.5.0-patch 0.5.0-patch0 0.5.0-patch01; do
  if "$validator" "$version" >/dev/null 2>&1; then
    echo "validator unexpectedly accepted: ${version:-<empty>}" >&2
    exit 1
  fi
done

echo "release version contract passed"
