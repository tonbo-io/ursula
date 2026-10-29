#!/usr/bin/env bash
#
# Prints the version the Python client (clients/sqlite-ursula-python) carries
# for release <version>: X.Y.Z as it is, and X.Y.Z-patchN as X.Y.Z.postN,
# since PEP 440 has no `-patchN` and hatchling refuses it.
# check-release-versions.sh holds `__version__` to it, and pypi-publish.yml
# maps a release tag through it.

set -euo pipefail

version=$("$(dirname -- "$0")/validate-release-version.sh" "${1:-}")

if [[ "$version" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-patch([1-9][0-9]*)$ ]]; then
  echo "${BASH_REMATCH[1]}.post${BASH_REMATCH[2]}"
else
  echo "$version"
fi
