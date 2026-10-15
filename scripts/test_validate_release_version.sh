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

branch_of="${repo_root}/scripts/release-source-branch.sh"

test "$("$branch_of" 0.7.5 refs/heads/main)" = main
test "$("$branch_of" 0.7.6 refs/heads/release/0.7)" = release/0.7
test "$("$branch_of" 0.7.6-patch1 refs/heads/release/0.7)" = release/0.7
test "$("$branch_of" 10.20.30 refs/heads/release/10.20)" = release/10.20

for args in \
  '0.7.6 refs/heads/release/0.8' \
  '0.7.6 refs/heads/release/0.70' \
  '0.7.6 refs/heads/release/0' \
  '0.7.6 refs/heads/release/0.7/x' \
  '0.7.6 refs/heads/feature' \
  '0.7.6 refs/tags/v0.7.6' \
  '0.7.6 release/0.7' \
  '0.7.6 ' \
  'v0.7.6 refs/heads/release/0.7'; do
  read -r version ref <<<"$args"
  if "$branch_of" "$version" "${ref:-}" >/dev/null 2>&1; then
    echo "source branch check unexpectedly accepted: $args" >&2
    exit 1
  fi
done

echo "release version contract passed"
