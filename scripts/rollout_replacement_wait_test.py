#!/usr/bin/env python3
"""Exercise the real replacement wait in conditional shell call contexts."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "charts/ursula/files/graceful-rollout.sh"


class ReplacementWaitTest(unittest.TestCase):
    def run_wait(self, shell, context, mode):
        with tempfile.TemporaryDirectory() as directory:
            counter = Path(directory) / "count"
            counter.write_text("0\n")
            result = subprocess.run(
                [shell, "-c", r'''
set -eu
. "$ROLLOUT_SCRIPT"
stop_forward() { :; }
log() { printf '%s\n' "$*" >&2; }
sleep() { :; }
kubectl() {
  case "$1" in
    delete)
      cat >/dev/null
      [ "$MODE" != delete_failure ]
      ;;
    -n)
      count=$(cat "$COUNTER")
      count=$((count + 1))
      printf '%s\n' "$count" >"$COUNTER"
      # Bound the test itself even with the old broken implementation.
      if [ "$count" -gt 300 ]; then
        printf '%s' aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
      elif [ "$MODE" = success ] && [ "$count" -ge 2 ]; then
        printf '%s' aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
      elif [ "$MODE" != absent ]; then
        printf '%s' 11111111-2222-3333-4444-555555555555
      fi
      ;;
    *) exit 90 ;;
  esac
}
invoke() {
  case "$CONTEXT" in
    or) replace_pod 0 11111111-2222-3333-4444-555555555555 || return 1 ;;
    if) if replace_pod 0 11111111-2222-3333-4444-555555555555; then return 0; else return 1; fi ;;
    direct) replace_pod 0 11111111-2222-3333-4444-555555555555 ;;
  esac
}
invoke
'''],
                env={
                    **os.environ,
                    "ROLLOUT_SCRIPT": str(SCRIPT),
                    "ROLLOUT_SOURCE_ONLY": "1",
                    "NAMESPACE": "test",
                    "STATEFULSET": "ursula",
                    "REPLICAS": "3",
                    "EXPECTED_GROUPS": "1",
                    "TARGET_IMAGE": "test",
                    "CONTEXT": context,
                    "MODE": mode,
                    "COUNTER": str(counter),
                },
                input="",
                capture_output=True,
                text=True,
                timeout=20,
            )
            return result, int(counter.read_text())

    def test_wait_outcomes_in_all_call_contexts(self):
        for shell in ("sh", "bash"):
            for context in ("or", "if", "direct"):
                for mode, status, observations in (
                    ("unchanged", 1, 300),
                    ("absent", 1, 300),
                    ("success", 0, 2),
                    ("delete_failure", 1, 0),
                ):
                    with self.subTest(shell=shell, context=context, mode=mode):
                        result, count = self.run_wait(shell, context, mode)
                        self.assertEqual(result.returncode, status, result.stderr)
                        self.assertEqual(count, observations, result.stderr)


if __name__ == "__main__":
    unittest.main()
