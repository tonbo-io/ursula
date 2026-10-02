// Drill: bucket purge under live keyed traffic (design §3.8, §10 M4, U23).
//
// Two buckets of live bounded owners on an S3-backed node. `DELETE /__ursula/purge/{bucket}` on the
// first one, while both fleets keep committing. Expected: the purge completes with the indexer
// drained; `{bucket}/` and `.keyed/{bucket}/` are empty in S3, and stay empty while the purged
// bucket's owners keep retrying (no resurrection); the purged owners poison (their stream is gone),
// never fault; the other bucket's owners see zero poison and every acknowledged commit there is
// stored byte-for-byte; its prefixes are intact.
import { afterAll, expect, it } from "vitest";
import { Stack } from "../stack/cluster.ts";
import type { OwnerFleet } from "../stack/fleet.ts";
import { sleep, until } from "../stack/proc.ts";
import { s3Available } from "../stack/s3.ts";
import { fleetOn, KNOBS, record, untilEveryOwnerCommits } from "./common.ts";

let stack: Stack | undefined;
const fleets: OwnerFleet[] = [];
afterAll(async () => {
	await Promise.all(fleets.map((fleet) => fleet.stop()));
	await stack?.stop();
});

it.runIf(s3Available())("purge: both prefixes erased under live traffic, other buckets untouched", async () => {
	const s = await Stack.start({ s3: true });
	stack = s;
	await s.createBucket("purged");
	await s.createBucket("kept");
	const purged = fleetOn(s.url, "purged");
	const kept = fleetOn(s.url, "kept");
	fleets.push(purged, kept);
	purged.start();
	kept.start();
	await untilEveryOwnerCommits(purged, 20, 120_000);
	await untilEveryOwnerCommits(kept, 20, 120_000);
	// Both prefixes of the purged bucket hold objects: projection namespaces and cold log chunks.
	for (const owner of purged.owners) await (await fetch(`${s.nodes[0]?.adminUrl}/__ursula/flush-cold/${owner.stream}`, { method: "POST" })).text();
	await until("cold objects and namespaces of the purged bucket", async () => ((await s.listS3("purged/")).length > 0 && (await s.listS3(".keyed/purged/")).length > 0 ? true : undefined), 60_000, 250);
	const objectsBefore = (await s.listS3("purged/")).length + (await s.listS3(".keyed/purged/")).length;
	await sleep(KNOBS.settleMs);

	const keptBefore = kept.totals();
	const started = Date.now();
	const report = await until(
		"the purge to complete",
		async () => {
			const r = await fetch(`${s.nodes[0]?.url}/__ursula/purge/purged`, { method: "DELETE" });
			if (!r.ok) return undefined;
			const body = (await r.json()) as Record<string, unknown>;
			return body.cold_gc_complete === true && body.keyed_drain_complete === true ? body : undefined;
		},
		120_000,
		500,
	);
	const purgeMs = Date.now() - started;
	const emptyAfterPurge = (await s.listS3("purged/")).length + (await s.listS3(".keyed/purged/")).length;
	// Live traffic continues; the purged owners keep retrying opens against the fenced bucket.
	await sleep(KNOBS.settleMs);
	await untilEveryOwnerCommits(kept, 5, KNOBS.recoveryMs);
	const emptyLater = (await s.listS3("purged/")).length + (await s.listS3(".keyed/purged/")).length;
	await purged.stop();
	await kept.stop();
	const keptTotals = kept.totals();
	const purgedTotals = purged.totals();
	const problems = await kept.verify(s.url);
	const keptObjects = (await s.listS3("kept/")).length + (await s.listS3(".keyed/kept/")).length;
	record("purge", {
		objects_before: objectsBefore,
		purge_ms: purgeMs,
		bucket_prefix_absent: report.bucket_prefix_absent,
		keyed_prefix_absent: report.keyed_prefix_absent,
		objects_after_purge: emptyAfterPurge,
		objects_after_settle: emptyLater,
		purged_poison: purgedTotals.poison,
		purged_faulted: purgedTotals.faulted,
		kept_poison: keptTotals.poison - keptBefore.poison,
		kept_faulted: keptTotals.faulted,
		kept_objects: keptObjects,
		verified_records: kept.owners.reduce((n, owner) => n + owner.acked.size, 0),
		byte_mismatches: problems.length,
	});
	expect(report.bucket_prefix_absent).toBe(true);
	expect(report.keyed_prefix_absent).toBe(true);
	expect(emptyAfterPurge).toBe(0);
	expect(emptyLater).toBe(0);
	expect(purgedTotals.faulted, purged.errors()).toBe(0);
	expect(purgedTotals.poison).toBeGreaterThan(0);
	expect(keptTotals.poison, kept.errors()).toBe(0);
	expect(keptTotals.faulted, kept.errors()).toBe(0);
	expect(keptObjects).toBeGreaterThan(0);
	expect(problems, problems.join("\n")).toEqual([]);
});
