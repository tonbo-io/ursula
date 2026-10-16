#!/usr/bin/env bash
#
# Prints the npm dist-tag that version <version> of the @tonbo/sqlite-ursula
# packages is published under: `latest` for the highest stable release
# (release-is-highest.sh, which reads the release tags from stdin), and
# `release-<major>.<minor>` for any other version, such as a 0.7.x patch
# published once 0.8.0 is out.

set -euo pipefail

version=${1:-}

if [[ ! "$version" =~ ^([0-9]+)\.([0-9]+)\.[0-9]+ ]]; then
  echo "not a release version: ${version:-<empty>}" >&2
  exit 1
fi
line="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"

if [ "$("$(dirname -- "$0")/release-is-highest.sh" "$version")" = true ]; then
  echo latest
else
  echo "release-${line}"
fi
