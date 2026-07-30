#!/usr/bin/env bash
#
# Asserts that everything carrying the release version agrees.
#
# Both Helm publish workflows refuse to package a chart whose `version` and
# `appVersion` differ from the tag being pushed, which is the right check in
# the wrong place: it runs after the tag exists, and a tag cannot be corrected,
# only superseded. `charts/ursula-chaos` drifted to 0.4.0 that way and its
# publish failed on two consecutive releases before anybody noticed, because
# the release PR does not touch that chart and so never ran its workflow.
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

if [ "${status}" -ne 0 ]; then
  cat >&2 <<EOF

A release tag publishes every chart at the tag's version, and each publish
workflow refuses a chart that disagrees. Bring them together in this pull
request; after the tag is pushed the only remedy is another release.
EOF
  exit 1
fi

echo "release versions agree: ${workspace}"
