// Drill: S3 outage (design §10 M4: "a 10 min S3 outage producing only retries and poison, and no
// faulted task").
//
// Live bounded owners commit through a single node whose cold store and keyed indexer both use S3,
// reached through a fault proxy that resets every connection for DRILL_OUTAGE_S. Expected: no
// `StorageRejected` (no faulted Pi task) at any time; poison and failed opens are allowed during the
// outage; after S3 returns every owner is open again and commits, E catches up, and every
// acknowledged commit is stored byte-for-byte.
import { afterAll, expect, it } from "vitest";
import { Stack } from "../stack/cluster.ts";
import { delta, type OwnerFleet } from "../stack/fleet.ts";
import { sleep, until } from "../stack/proc.ts";
import { s3Available } from "../stack/s3.ts";
import { fleetOn, KNOBS, record, sampleMax, untilEveryOwnerCommits } from "./common.ts";

/** The node's cumulative cold-flush write errors (`/__ursula/metrics`). */
async function coldWriteErrors(stack: Stack): Promise<number> {
	const r = await fetch(`${stack.nodes[0]?.url}/__ursula/metrics`);
	const metrics = (await r.json()) as { cold_flush_write_errors?: number };
	return metrics.cold_flush_write_errors ?? 0;
}

let stack: Stack | undefined;
let fleet: OwnerFleet | undefined;
afterAll(async () => {
	await fleet?.stop();
	await stack?.stop();
});

it.runIf(s3Available())("S3 outage: only retries and poison, no faulted task, full recovery", async () => {
	stack = await Stack.start({ s3: true });
	await stack.createBucket("drill");
	fleet = fleetOn(stack.url, "drill");
	fleet.start();
	await untilEveryOwnerCommits(fleet, 5, 120_000);
	await sleep(KNOBS.settleMs);

	const before = fleet.totals();
	const errorsBefore = await coldWriteErrors(stack);
	const lag = sampleMax(() => fleet?.maxOverlayRecords() ?? 0);
	const proxy = stack.s3Proxy;
	if (proxy === undefined) throw new Error("no S3 proxy");
	proxy.down();
	const outageStart = Date.now();
	await sleep(KNOBS.outageMs);
	const during = delta(fleet.totals(), before);
	const coldErrors = (await coldWriteErrors(stack)) - errorsBefore;
	proxy.up();
	const recovery = Date.now();
	await untilEveryOwnerCommits(fleet, 5, KNOBS.recoveryMs);
	const resumeMs = Date.now() - recovery;
	const targets = fleet.owners.map((owner) => owner.storage?.tail ?? 0);
	await until(
		"E to catch up after S3 returns",
		() => (fleet?.owners.every((owner, i) => (owner.storage?.localStore?.overlayFloor ?? 0) >= (targets[i] ?? 0)) ? true : undefined),
		KNOBS.recoveryMs,
		200,
	);
	const catchUpMs = Date.now() - recovery;
	await sleep(KNOBS.settleMs);
	lag.stop();
	await fleet.stop();
	const totals = delta(fleet.totals(), before);
	const problems = await fleet.verify(stack.url);
	record("s3-outage", {
		outage_ms: recovery - outageStart,
		commits_during_outage: during.commits,
		poison: totals.poison,
		faulted: totals.faulted,
		open_retries: totals.openFailures,
		node_cold_write_errors_during_outage: coldErrors,
		max_overlay_records: lag.max(),
		reopens: totals.reopens,
		resume_ms: resumeMs,
		e_catch_up_ms: catchUpMs,
		verified_records: fleet.owners.reduce((n, owner) => n + owner.acked.size, 0),
		byte_mismatches: problems.length,
	});
	expect(problems, problems.join("\n")).toEqual([]);
	expect(totals.faulted, fleet.errors()).toBe(0);
	// The outage was real: the node's cold flushes failed.
	expect(coldErrors).toBeGreaterThan(0);
	// Poison is allowed; any poisoned owner must have reopened (every owner committed again above).
	expect(totals.reopens).toBeGreaterThanOrEqual(totals.poison);
});
