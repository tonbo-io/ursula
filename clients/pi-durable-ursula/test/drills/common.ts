// Shared knobs and reporting for the M4 drills (design §10 M4, §11.7). Every duration is a knob so
// the nightly workflow runs the design's 10-minute outages and CI runs short ones.
import { appendFileSync, mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
import type { Timing } from "../../src/storage.ts";
import type { FleetOptions } from "../stack/fleet.ts";
import { OwnerFleet } from "../stack/fleet.ts";
import { sleep } from "../stack/proc.ts";

const num = (name: string, fallback: number): number => {
	const raw = process.env[name];
	if (raw === undefined || raw === "") return fallback;
	const value = Number(raw);
	if (!Number.isFinite(value) || value < 0) throw new Error(`${name} must be a non-negative number, got ${raw}`);
	return value;
};

export const KNOBS = {
	/** Indexer and S3 outage length (design: 10 min). */
	outageMs: num("DRILL_OUTAGE_S", 600) * 1000,
	/** Live traffic before and after each disruption. */
	settleMs: num("DRILL_SETTLE_S", 20) * 1000,
	/** Deadline for E to catch up / owners to recover after a disruption. */
	recoveryMs: num("DRILL_RECOVERY_S", 180) * 1000,
	owners: num("DRILL_OWNERS", 8),
	commitIntervalMs: num("DRILL_COMMIT_INTERVAL_MS", 100),
	payloadBytes: num("DRILL_PAYLOAD_BYTES", 512),
	overlayCapBytes: num("DRILL_OVERLAY_CAP_BYTES", 256 * 1024 * 1024),
	/** Per-commit deadline before poisoning (owner default 30 s). */
	commitDeadlineMs: num("DRILL_COMMIT_DEADLINE_MS", 30_000),
};

/** Owner timing for drills: production deadlines (knobs), flush-waits often enough that E moves. */
export const DRILL_TIMING: Partial<Timing> = {
	commitDeadlineMs: KNOBS.commitDeadlineMs,
	keyedWaitMs: 10_000,
	flushMaxRecords: 20,
	flushMaxAgeMs: 2_000,
	flushBackoffMaxMs: 2_000,
	backoffMaxMs: 1_000,
};

export function fleetOn(baseUrl: string, bucket: string, extra: Partial<FleetOptions> = {}): OwnerFleet {
	return new OwnerFleet({
		baseUrl,
		bucket,
		owners: KNOBS.owners,
		commitIntervalMs: KNOBS.commitIntervalMs,
		payloadBytes: KNOBS.payloadBytes,
		overlayCapBytes: KNOBS.overlayCapBytes,
		timing: DRILL_TIMING,
		...extra,
	});
}

/** Waits until every owner has committed at least `n` more times than in `from`. */
export async function untilEveryOwnerCommits(fleet: OwnerFleet, n: number, timeoutMs: number): Promise<void> {
	const from = fleet.owners.map((owner) => owner.counters.commits);
	const deadline = Date.now() + timeoutMs;
	while (!fleet.owners.every((owner, i) => owner.counters.commits >= (from[i] ?? 0) + n)) {
		if (Date.now() > deadline) throw new Error(`owners did not each commit ${n} times within ${timeoutMs} ms:\n${fleet.errors()}`);
		await sleep(200);
	}
}

/** Samples `f` every `everyMs` until `stop()` and keeps the maximum. */
export function sampleMax(f: () => number, everyMs = 250): { max: () => number; stop: () => void } {
	let max = f();
	const timer = setInterval(() => {
		max = Math.max(max, f());
	}, everyMs);
	return { max: () => max, stop: () => clearInterval(timer) };
}

/**
 * Appends one drill's measured outcome to `$DRILL_RESULTS_DIR/results.jsonl` (default
 * `drill-results/` in this package) and prints it, for docs/architecture/keyed-streams-drills.md.
 */
export function record(drill: string, result: Record<string, unknown>): void {
	const dir = resolve(process.env.DRILL_RESULTS_DIR ?? join(import.meta.dirname, "../../drill-results"));
	mkdirSync(dir, { recursive: true });
	const line = JSON.stringify({ drill, at: new Date().toISOString(), knobs: KNOBS, ...result });
	appendFileSync(join(dir, "results.jsonl"), `${line}\n`);
	console.log(`[drill] ${line}`);
}
