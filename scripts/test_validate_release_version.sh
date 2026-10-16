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

is_highest="${repo_root}/scripts/release-is-highest.sh"
dist_tag="${repo_root}/scripts/npm-dist-tag.sh"

# `git ls-remote --tags --refs` lines for release tags of <versions>.
tags() {
  local v
  for v in "$@"; do printf '0123abcd\trefs/tags/v%s\n' "$v"; done
}

released=(0.6.2 0.7.5 0.7.6 0.7.6-patch1)
test "$(tags "${released[@]}" | "$is_highest" 0.7.7)" = true
test "$(tags "${released[@]}" | "$is_highest" 0.8.0)" = true
test "$(tags "${released[@]}" | "$is_highest" 0.7.6)" = true
test "$(tags "${released[@]}" 0.8.0 | "$is_highest" 0.7.7)" = false
test "$(tags "${released[@]}" 0.8.0 | "$is_highest" 0.8.0)" = true
test "$(tags "${released[@]}" | "$is_highest" 0.7.6-patch2)" = false
test "$(tags "${released[@]}" | "$is_highest" 0.7.4)" = false
test "$(tags 0.9.0 0.10.0 | "$is_highest" 0.10.1)" = true
test "$(tags 0.9.0 0.10.0 | "$is_highest" 0.9.1)" = false
test "$(tags | "$is_highest" 0.1.0)" = true
test "$(tags 0.7.6 | "$is_highest" '')" = false

test "$(tags "${released[@]}" | "$dist_tag" 0.8.0)" = latest
test "$(tags "${released[@]}" 0.8.0 | "$dist_tag" 0.8.1)" = latest
test "$(tags "${released[@]}" 0.8.0 | "$dist_tag" 0.7.7)" = release-0.7
test "$(tags "${released[@]}" 0.8.0 | "$dist_tag" 0.7.6-patch2)" = release-0.7
test "$(tags "${released[@]}" | "$dist_tag" 0.0.0-stage)" = release-0.0
test "$(tags 0.10.0 | "$dist_tag" 0.9.3)" = release-0.9

for version in '' v0.8.0 0.8 main; do
  if tags 0.7.6 | "$dist_tag" "$version" >/dev/null 2>&1; then
    echo "dist-tag unexpectedly chose a tag for: ${version:-<empty>}" >&2
    exit 1
  fi
done

echo "release version contract passed"
