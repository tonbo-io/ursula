#!/bin/sh
# Installs sqlite-ursula from the wheels in <wheels> into a new virtual environment of each Python,
# and runs smoke.py there (.github/workflows/sqlite-ursula-python-wheels.yml):
#
#   smoke.sh <wheels> <version> <installer> <pythons> <wheel> <smoke.py arguments...>
#
# <installer> is `uv` (<pythons> are versions uv installs, such as `3.14t`) or `pip` (<pythons> are
# executables, whose venv module makes the environment). <wheel> is `best`, the wheel the installer
# picks among all of them for this host, or `any`, the py3-none-any wheel. Nothing comes from an
# index.
set -eu

wheels=$1 version=$2 installer=$3 pythons=$4 wheel=$5
shift 5
here=$(cd "$(dirname "$0")" && pwd)

case "$wheel" in
  best) requirement="sqlite-ursula==$version" ;;
  any) requirement="$wheels/sqlite_ursula-$version-py3-none-any.whl" ;;
  *) echo "wheel is best or any, not $wheel" >&2 && exit 2 ;;
esac

for python in $pythons; do
  scratch=$(mktemp -d)
  case "$installer" in
    uv)
      uv venv --python "$python" "$scratch/venv"
      uv pip install --python "$scratch/venv/bin/python" --no-index --find-links "$wheels" "$requirement"
      ;;
    pip)
      "$python" -m venv "$scratch/venv"
      "$scratch/venv/bin/python" -m pip install --disable-pip-version-check --no-cache-dir \
        --no-index --find-links "$wheels" "$requirement"
      ;;
    *) echo "installer is uv or pip, not $installer" >&2 && exit 2 ;;
  esac
  # Outside the checkout, so the installed package is the one imported.
  (cd "$scratch" && "$scratch/venv/bin/python" "$here/smoke.py" "$version" "$@")
done
