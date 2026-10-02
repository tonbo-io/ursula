// Drill: keyed indexer outage (design §10 M4: "a 10 min indexer outage: zero poison among owners
// whose overlays stay under the cap and whose Session-line reads stay local, and E catching up after
// recovery").
//
// Live bounded owners commit through a single S3-backed node while the indexer process is killed for
// DRILL_OUTAGE_S. Expected: every owner keeps committing during the outage (flush-waits retry, the
// overlay grows but stays under the cap), zero poison, zero faulted tasks, and after the indexer
// restarts every owner's E reaches the tail it had at recovery; every acknowledged commit is stored
// byte-for-byte.
import { afterAll, expect, it } from "vitest";
import { Stack } from "../stack/cluster.ts";
import { delta, type OwnerFleet } from "../stack/fleet.ts";
import { sleep, until } from "../stack/proc.ts";
import { s3Available } from "../stack/s3.ts";
import { fleetOn, KNOBS, record, sampleMax, untilEveryOwnerCommits } from "./common.ts";

let stack: Stack | undefined;
let fleet: OwnerFleet | undefined;
afterAll(async () => {
	await fleet?.stop();
	await stack?.stop();
});

it.runIf(s3Available())("indexer outage: owners keep committing under the cap, zero poison, E catches up", async () => {
	stack = await Stack.start({ s3: true });
	await stack.createBucket("drill");
	fleet = fleetOn(stack.url, "drill");
	fleet.start();
	await untilEveryOwnerCommits(fleet, 5, 120_000);
	await until("E to follow the tail before the outage", () => (fleet?.caughtUp(40) ? true : undefined), KNOBS.recoveryMs, 250);
	await sleep(KNOBS.settleMs);

	const before = fleet.totals();
	const overlay = sampleMax(() => fleet?.maxOverlayBytes() ?? 0);
	const lag = sampleMax(() => fleet?.maxOverlayRecords() ?? 0);
	await stack.stopIndexer(true);
	const outageStart = Date.now();
	await sleep(KNOBS.outageMs);
	const during = delta(fleet.totals(), before);
	const recovery = Date.now();
	await stack.restartIndexer();
	// E must reach each owner's tail at recovery.
	const targets = fleet.owners.map((owner) => owner.storage?.tail ?? 0);
	await until(
		"E to catch up after the indexer restart",
		() => (fleet?.owners.every((owner, i) => (owner.storage?.localStore?.overlayFloor ?? 0) >= (targets[i] ?? 0)) ? true : undefined),
		KNOBS.recoveryMs,
		200,
	);
	const catchUpMs = Date.now() - recovery;
	await sleep(KNOBS.settleMs);
	overlay.stop();
	lag.stop();
	await fleet.stop();
	const totals = delta(fleet.totals(), before);
	const problems = await fleet.verify(stack.url);
	const result = {
		outage_ms: recovery - outageStart,
		commits_during_outage: during.commits,
		poison: totals.poison,
		faulted: totals.faulted,
		max_overlay_bytes: overlay.max(),
		max_overlay_records: lag.max(),
		overlay_cap_bytes: KNOBS.overlayCapBytes,
		e_catch_up_ms: catchUpMs,
		remote_reads: totals.remoteReads,
		verified_records: fleet.owners.reduce((n, owner) => n + owner.acked.size, 0),
		byte_mismatches: problems.length,
	};
	record("indexer-outage", result);
	expect(problems, problems.join("\n")).toEqual([]);
	expect(totals.poison, fleet.errors()).toBe(0);
	expect(totals.faulted, fleet.errors()).toBe(0);
	expect(overlay.max()).toBeLessThan(KNOBS.overlayCapBytes);
	// Every owner kept committing through the outage (at least a third of its nominal rate).
	const nominal = (KNOBS.outageMs / (KNOBS.commitIntervalMs + 20)) * fleet.owners.length;
	expect(during.commits).toBeGreaterThan(nominal / 3);
});
