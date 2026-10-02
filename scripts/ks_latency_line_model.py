#!/usr/bin/env python3
"""Keyed-streams M0c: Pi submit->provider estimates from measured commit latency.

Two estimators of design §9.2:

* ``mva``: the mean-value line model exactly as the design states it. One
  Session line per harness (a FIFO single server), service ``S = L + 1.1 ms``,
  N streaming conversations as partial sources re-armed 100 ms after their
  commit settles (closed network, exact MVA), a blocking commit waits
  ``S * (1 + Q)`` with Q the mean queue length of the N sources, and
  submit->provider = 4 blocking commits + 3 ms.
* ``sim``: a discrete-event simulation of the same line whose service times are
  drawn from a measured sample file (``<latency_us> <bytes>`` per line, as
  written by ``ursula-bench record-match --samples-out``) instead of the mean.
  It reports the mean/p50/p99 of submit->provider and the line throughput.

Usage:
  ks_latency_line_model.py table                      # reproduce the §9.2 table
  ks_latency_line_model.py mva  <mean_L_ms> [N ...]
  ks_latency_line_model.py sim  <samples_file> [N ...]
"""

import heapq
import random
import statistics
import sys

OVERHEAD_MS = 1.1  # Pi per-commit line overhead (fitted to probe2)
THINK_MS = 100.0  # partial throttle re-arm after settle
BLOCKING_COMMITS = 4
SUBMIT_FIXED_MS = 3.0


def mva(mean_l_ms, n):
    s = mean_l_ms + OVERHEAD_MS
    q = 0.0
    x = 0.0
    for k in range(1, n + 1):
        r = s * (1 + q)
        x = k / (THINK_MS + r)
        q = x * r
    wait = s * (1 + q)
    return BLOCKING_COMMITS * wait + SUBMIT_FIXED_MS, x * 1000.0, 1000.0 / s


def load_samples(path):
    out = []
    with open(path) as fh:
        for line in fh:
            parts = line.split()
            if parts:
                out.append(int(parts[0]) / 1000.0)
    if not out:
        raise SystemExit(f"no samples in {path}")
    return out


def sim(samples, n, horizon_ms=600_000.0, submit_every_ms=1_000.0, seed=1):
    """Single FIFO line. Partial sources: n, each re-armed THINK_MS after its
    commit settles. Every ``submit_every_ms`` a submit enqueues 4 blocking
    commits back to back (each enqueued when the previous one settles)."""
    rng = random.Random(seed)
    svc = lambda: rng.choice(samples) + OVERHEAD_MS  # noqa: E731
    events = []  # (time, seq, kind, payload)
    seq = 0

    def push(t, kind, payload=None):
        nonlocal seq
        heapq.heappush(events, (t, seq, kind, payload))
        seq += 1

    for i in range(n):
        push(rng.uniform(0, THINK_MS), "partial", i)
    push(submit_every_ms * 0.5, "submit", None)
    queue = []  # FIFO of (kind, payload)
    busy_until = None
    now = 0.0
    completed = 0
    submits = []  # submit -> provider latencies
    submit_state = {}

    def start_next(t):
        nonlocal busy_until
        if busy_until is None and queue:
            kind, payload = queue.pop(0)
            busy_until = t + svc()
            push(busy_until, "done", (kind, payload))

    while events:
        now, _, kind, payload = heapq.heappop(events)
        if now > horizon_ms:
            break
        if kind == "partial":
            queue.append(("partial", payload))
            start_next(now)
        elif kind == "submit":
            sid = len(submit_state)
            submit_state[sid] = [now, 0]
            queue.append(("block", sid))
            start_next(now)
            push(now + submit_every_ms, "submit", None)
        elif kind == "done":
            busy_until = None
            completed += 1
            dkind, dpayload = payload
            if dkind == "partial":
                push(now + THINK_MS, "partial", dpayload)
            else:
                st = submit_state[dpayload]
                st[1] += 1
                if st[1] < BLOCKING_COMMITS:
                    queue.append(("block", dpayload))
                else:
                    submits.append(now - st[0] + SUBMIT_FIXED_MS)
            start_next(now)
    submits.sort()
    pct = lambda p: submits[min(len(submits) - 1, int(len(submits) * p))]  # noqa: E731
    return {
        "n": n,
        "submit_to_provider_mean_ms": round(statistics.mean(submits), 1),
        "p50_ms": round(pct(0.5), 1),
        "p99_ms": round(pct(0.99), 1),
        "line_commits_per_s": round(completed / (min(now, horizon_ms) / 1000.0), 1),
    }


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    cmd = argv[1]
    if cmd == "table":
        for l in (4, 5, 6, 8):
            row = [f"{mva(l, n)[0]:.0f}" for n in (1, 4, 8, 16)]
            print(f"L={l} ms: N=1/4/8/16 -> {row} ms; N=16 throughput {mva(l, 16)[1]:.0f}/s")
        return 0
    if cmd == "mva":
        mean_l = float(argv[2])
        ns = [int(v) for v in argv[3:]] or [1, 4, 8, 16]
        for n in ns:
            s2p, thr, cap = mva(mean_l, n)
            print(f"N={n}: submit->provider {s2p:.1f} ms, partial throughput {thr:.0f}/s, line capacity {cap:.0f}/s")
        return 0
    if cmd == "sim":
        samples = load_samples(argv[2])
        ns = [int(v) for v in argv[3:]] or [1, 4, 8, 16]
        for n in ns:
            print(sim(samples, n))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
