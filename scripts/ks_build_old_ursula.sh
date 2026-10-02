#!/usr/bin/env bash
# Builds the `ursula` binary at a git revision, for the keyed-streams rolling-upgrade drill
# (docs/architecture/keyed-streams-drills.md), and prints the binary's path.
#
#   scripts/ks_build_old_ursula.sh [rev] [out_dir]
#
# rev defaults to e6d8d70 (main's 0.5.1 release, the base keyed-streams branched from); out_dir
# defaults to target/ks-old/<rev>. An existing binary is reused. The source is exported with
# `git archive`, so no worktree is created; a shallow clone fetches the revision first.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rev="${1:-e6d8d70770e1991d2eedf0ce2c3f74ac01f57f43}"
out="${2:-$repo_root/target/ks-old/${rev:0:7}}"

if [ -x "$out/ursula" ]; then
  echo "$out/ursula"
  exit 0
fi
if ! git -C "$repo_root" cat-file -e "$rev^{commit}" 2>/dev/null; then
  git -C "$repo_root" fetch --quiet --depth 1 origin "$rev" >&2
fi
src="$(mktemp -d "${TMPDIR:-/tmp}/ks-old-src.XXXXXX")"
trap 'rm -rf "$src"' EXIT
git -C "$repo_root" archive "$rev" | tar -x -C "$src"
target="${KS_OLD_TARGET_DIR:-$repo_root/target/ks-old-build}"
(cd "$src" && CARGO_TARGET_DIR="$target" cargo build --release -p ursula --bin ursula >&2)
mkdir -p "$out"
cp "$target/release/ursula" "$out/ursula"
echo "$out/ursula"
