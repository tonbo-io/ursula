"""Transcribes a tracevfs log of the PHASETWO prototype into a publication vector.

usage: python3 transcribe.py <runs/NODE/NAME.log> <NAME> <NAME.trace>

See README.md for the rule.
"""
import re
import sys

src, name, out = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(src, encoding="utf-8").read().splitlines()
version = re.search(r"sqlite (\S+)", lines[0]).group(1)
node = src.split("/")[-2]
events = []
for raw in lines[1:]:
    m = re.match(r"(\d+) \S+ (?:---- )?(c\d+) (\S+) ?(.*)", raw)
    if not m:
        continue
    num, conn, kind, rest = int(m.group(1)), m.group(2), m.group(3), m.group(4)
    ev = None
    if kind in ("MARK", "ARM"):
        continue  # the harness's own PRAGMAs, not calls SQLite makes
    elif kind == "main":
        if rest.startswith("OPEN"):
            continue
        if rest.startswith("SHMLOCK lock excl WRITE rc=0"):
            ev = "lock-write"
        elif rest.startswith("SHMLOCK unlock excl WRITE"):
            ev = "unlock-write"
        elif rest.startswith("FCNTL COMMIT_PHASETWO"):
            ev = "phasetwo"
        elif rest.startswith("CLOSE"):
            ev = "close"
        else:
            ev = "other"
    elif kind == "wal":
        # A wal FAULT line reports the result of the write logged just before it.
        if rest.startswith("WRITE"):
            ev = "commit" if "COMMIT FRAME PAGE" in rest else "wal-write"
        else:
            continue
    else:
        continue
    if events and events[-1][1] == conn and events[-1][2] == ev and ev in ("other", "wal-write"):
        continue
    events.append((num, conn, ev))
with open(out, "w", encoding="utf-8") as f:
    f.write(f"# Transcribed from proto-phasetwo/runs/{node}/{name}.log (SQLite {version}).\n")
    f.write("# One line per call on a main db handle, or per WAL write of its connection:\n")
    f.write("# <trace line> <connection> <event>. See README.md.\n")
    for num, conn, ev in events:
        f.write(f"{num} {conn} {ev}\n")
print(name, len(events))
