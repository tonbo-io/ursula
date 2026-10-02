#!/usr/bin/env python3
"""Print the key numbers from ursula-state-probe JSONL results (no new measurement).

Usage: summarize.py [RESULTS_DIR]   (default: target/state-probe)
"""
import json
import os
import sys


def mb(x):
    return f"{x / 1e6:8.2f} MB"


def summarize(path):
    print(f"\n== {os.path.basename(path)}")
    with open(path) as f:
        for line in f:
            row = json.loads(line)
            m = row.pop("m", None)
            text = json.dumps(row, sort_keys=True)[:300]
            if m:
                s = m["snapshot"]
                g = m.get("gauges", {})
                text += (
                    f" | heap={mb(m['heap_actual_bytes'])} tight={mb(m['heap_tight_bytes'])}"
                    f" snap={mb(s['total_bytes'])} offsets={s['record_offsets_bytes']}"
                    f" msgrec={s['message_records_count']} refs={s['cold_chunks_count']}"
                    f" receipts={s['receipt_count']} ttl_heap={g.get('ttl_heap_entries')}"
                )
            print("  " + text)


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "target/state-probe"
    for name in sorted(os.listdir(root)):
        if name.endswith(".jsonl"):
            summarize(os.path.join(root, name))
    gate = os.path.join(root, "gate.json")
    if os.path.exists(gate):
        with open(gate) as f:
            data = json.load(f)
        print("\n== gate findings")
        for finding in data.get("findings", []):
            print(f"  {finding['severity']}: {finding['message']}")


if __name__ == "__main__":
    main()
