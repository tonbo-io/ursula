#!/usr/bin/env bash
#
# Prints true when <version> is a stable release (X.Y.Z) at or above every
# other stable release tag, and false otherwise. The release tags are read from
# stdin as `git ls-remote --tags --refs` lines (`<sha> refs/tags/v<version>`),
# so the caller picks the remote. An X.Y.Z-patchN release, a patch to an older
# line, or any version that is not a stable release is never the highest.
#
# The `latest` image tag and the `latest` npm dist-tag move only to the highest
# stable release, so publishing an older line never moves them back.

set -euo pipefail

version=${1:-}

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  # Drain stdin so the writer of the pipe never sees a broken pipe.
  cat >/dev/null
  echo false
  exit 0
fi

highest="$(awk -v current="$version" '
    { tag = $2; sub(/^refs\/tags\/v/, "", tag) }
    tag ~ /^[0-9]+\.[0-9]+\.[0-9]+$/ && tag != current { print tag }
  ' |
  sort -V | tail -n 1)"

if [ -z "$highest" ] ||
  [ "$(printf '%s\n' "$highest" "$version" | sort -V | tail -n 1)" = "$version" ]; then
  echo true
else
  echo false
fi
