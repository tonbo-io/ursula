#!/usr/bin/env bash
#
# Asserts that everything carrying the release version agrees.
#
# The stable release workflow publishes every crate, image, and chart before
# creating the immutable Git tag and GitHub Release. A version mismatch must be
# found before any registry accepts an artifact. `charts/ursula-chaos` once
# drifted independently and failed on two consecutive releases, because a
# release PR did not touch that chart and so never ran its workflow.
#
# This runs on every pull request instead, where a forgotten file is one more
# commit rather than one more release.
#
# The workspace version is the source of truth: the tag is `v` plus that, and
# every chart is published as part of the same release.

set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "${repo_root}"

# The first `version =` under [workspace.package], not the first in the file.
workspace=$(awk '
  /^\[workspace\.package\]/ { in_section = 1; next }
  /^\[/ { in_section = 0 }
  in_section && $1 == "version" { gsub(/"/, "", $3); print $3; exit }
' Cargo.toml)

if [ -z "${workspace}" ]; then
  echo "could not read version from [workspace.package] in Cargo.toml" >&2
  exit 1
fi

./scripts/validate-release-version.sh "${workspace}" >/dev/null

status=0
for chart in charts/ursula charts/ursula-chaos; do
  version=$(awk '$1 == "version:" { print $2; exit }' "${chart}/Chart.yaml")
  app_version=$(awk '$1 == "appVersion:" { gsub(/"/, "", $2); print $2; exit }' "${chart}/Chart.yaml")

  if [ "${version}" != "${workspace}" ]; then
    echo "${chart}/Chart.yaml version is ${version}, workspace is ${workspace}" >&2
    status=1
  fi
  if [ "${app_version}" != "${workspace}" ]; then
    echo "${chart}/Chart.yaml appVersion is ${app_version}, workspace is ${workspace}" >&2
    status=1
  fi
done

# The docs' release notes have a section for the release, and a GitHub
# release-body notice (`release.yml` puts it above the generated notes) opens
# with the same paragraph as that section.
notes=docs/web/src/content/docs/pages/release-notes.mdx
if ! grep -qxF "## ${workspace}" "${notes}"; then
  echo "${notes} has no '## ${workspace}' section" >&2
  status=1
fi
notice=".github/release-notes/${workspace}.md"
if [ -f "${notice}" ]; then
  first=$(head -n 1 "${notice}")
  if [ -z "${first}" ]; then
    echo "${notice} starts with an empty line" >&2
    status=1
  elif ! grep -qxF "${first}" "${notes}"; then
    echo "${notes} does not contain the first line of ${notice}" >&2
    status=1
  fi
fi

# The docs' install commands pin the released image and chart.
pins=$(grep -rhoE 'ghcr\.io/tonbo-io/ursula:[0-9][^ ]*|charts/ursula --version [0-9][^ ]*' \
  docs/web/src/content/docs/pages || true)
while IFS= read -r pin; do
  [ -n "${pin}" ] || continue
  if [ "${pin##*[: ]}" != "${workspace}" ]; then
    echo "docs pin '${pin}' is not ${workspace}" >&2
    status=1
  fi
done <<<"${pins}"

if [ "${status}" -ne 0 ]; then
  cat >&2 <<EOF

The stable release workflow publishes every chart at the workspace version.
Bring them together in this pull request; after a registry accepts an artifact
the only remedy is another release.
EOF
  exit 1
fi

echo "release versions agree: ${workspace}"
