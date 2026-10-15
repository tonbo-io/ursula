#!/usr/bin/env bash
#
# Prints the branch a stable release of <version> may be cut from, given the
# ref the release workflow was dispatched on, or fails.
#
# A release ships from main, or a patch ships from the maintenance branch of
# its line: release/<major>.<minor>, named after the version's major.minor.
# main releases only the version in its Cargo.toml, so once main moves to the
# next minor, patches of the previous line come from its release branch.

set -euo pipefail

version=${1:-}
run_ref=${2:-}

"$(dirname -- "$0")/validate-release-version.sh" "$version" >/dev/null
[[ "$version" =~ ^([0-9]+)\.([0-9]+)\. ]]
line="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"

case "$run_ref" in
  refs/heads/main)
    branch=main
    ;;
  "refs/heads/release/${line}")
    branch="release/${line}"
    ;;
  *)
    echo "release ${version} must be dispatched on refs/heads/main or refs/heads/release/${line}, not ${run_ref:-<empty>}" >&2
    exit 1
    ;;
esac

printf '%s\n' "$branch"
