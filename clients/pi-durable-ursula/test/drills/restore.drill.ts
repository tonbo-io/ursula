// Drill: restore plus continuity rebuild (design §3.8 "Disaster recovery", §5.5, §10 M4: "restore
// plus continuity rebuild, with keyed-state answering 503 until the rebuild catches up and owners
// reopening after poison").
//
// Live bounded owners on an S3-backed node. Every Raft group is exported through the backup API (the
// endpoints `ursulactl backup` uses), the owners keep committing so the projection moves past the
// backup, then the node is destroyed and a fresh one (same S3 cold root, same indexer) imports the
// backup. Expected: the owners poison at their commit deadline (never fault); keyed-state answers
// 503 for every restored stream (its namespace is ahead of the restored log) until the indexer's
// continuity check has rebuilt it, then serves exactly the restored tail; the owners reopen and
// commit again; every acknowledged commit below the restored tail, and every commit after the
// restore, is stored byte-for-byte.
import { afterAll, expect, it } from "vitest";
import { K, META } from "../../src/families.ts";
import { b64 } from "../../src/tuple.ts";
import { Stack } from "../stack/cluster.ts";
import type { OwnerFleet } from "../stack/fleet.ts";
import { sleep, until } from "../stack/proc.ts";
import { s3Available } from "../stack/s3.ts";
import { fleetOn, KNOBS, record, untilEveryOwnerCommits } from "./common.ts";

let stack: Stack | undefined;
let fleet: OwnerFleet | undefined;
afterAll(async () => {
	await fleet?.stop();
	await stack?.stop();
});

async function recordTail(url: string, stream: string): Promise<number> {
	const r = await fetch(`${url}/${stream}`, { method: "HEAD" });
	if (!r.ok) throw new Error(`HEAD ${stream}: ${r.status}`);
	return Number(r.headers.get("stream-record-next"));
}

it.runIf(s3Available())("restore: keyed-state 503 until the continuity rebuild, owners reopen after poison", async () => {
	const s = await Stack.start({ s3: true });
	stack = s;
	const node = s.nodes[0];
	if (node === undefined) throw new Error("no node");
	await s.createBucket("drill");
	const f = fleetOn(s.url, "drill");
	fleet = f;
	f.start();
	await untilEveryOwnerCommits(f, 10, 120_000);

	// Backup: export every group. The tails read first are a lower bound of what the export holds.
	const backupTails = await Promise.all(f.owners.map((owner) => recordTail(s.url, owner.stream)));
	const backup: Uint8Array[] = [];
	for (let group = 0; group < s.groupCount; group++) {
		const r = await fetch(`${node.adminUrl}/__ursula/backup/group/${group}`);
		if (!r.ok) throw new Error(`export group ${group}: ${r.status} ${await r.text()}`);
		backup.push(new Uint8Array(await r.arrayBuffer()));
	}
	// The projection moves well past the backup.
	await until(
		"E beyond the backup",
		() => (f.owners.every((owner, i) => (owner.storage?.localStore?.overlayFloor ?? 0) >= (backupTails[i] ?? 0) + 20) ? true : undefined),
		KNOBS.recoveryMs,
		200,
	);
	await sleep(KNOBS.settleMs);
	const before = f.totals();

	// Disaster: the node and its state are gone. Owners poison at their commit deadline.
	f.pause();
	await s.stopNode(node.id, true);
	const down = Date.now();
	await until("every owner poisoned", () => (f.owners.every((owner) => owner.storage === undefined) ? true : undefined), KNOBS.commitDeadlineMs + 60_000, 200);
	const poisonedMs = Date.now() - down;

	// A fresh node on the same cold root imports the backup.
	await s.startNode(node.id);
	for (const [group, bytes] of backup.entries()) {
		const r = await fetch(`${node.adminUrl}/__ursula/backup/group/${group}/import`, { method: "POST", body: bytes });
		if (!r.ok) throw new Error(`import group ${group}: ${r.status} ${await r.text()}`);
	}
	await s.raiseFeatureLevel(1);
	const restoredTails = await Promise.all(f.owners.map((owner) => recordTail(s.url, owner.stream)));
	f.owners.forEach((owner, i) => owner.rewind(restoredTails[i] ?? 0));

	// Keyed-state: 503 while the namespace is ahead of the restored log, then exactly the restored tail.
	const key = b64(K.m(META.owner));
	const firstAnswers: number[] = [];
	const rebuildStart = Date.now();
	for (const [i, owner] of f.owners.entries()) {
		const first = await fetch(`${s.url}/${owner.stream}/keyed-state?key=${key}`);
		await first.text();
		firstAnswers.push(first.status);
		const through = await until(
			`rebuild of ${owner.stream}`,
			async () => {
				const r = await fetch(`${s.url}/${owner.stream}/keyed-state?key=${key}&min_through_record=${restoredTails[i]}&timeout_ms=5000`);
				await r.text();
				return r.status === 200 ? Number(r.headers.get("stream-keyed-through")) : undefined;
			},
			KNOBS.recoveryMs,
			200,
		);
		expect(through).toBe(restoredTails[i]);
	}
	const rebuildMs = Date.now() - rebuildStart;

	// Owners reopen after poison and continue on the restored log.
	f.resume();
	await untilEveryOwnerCommits(f, 5, KNOBS.recoveryMs);
	await sleep(KNOBS.settleMs);
	await f.stop();
	const totals = f.totals();
	const problems = await f.verify(s.url);
	record("restore", {
		backup_tails: backupTails,
		restored_tails: restoredTails,
		owners_poisoned_after_ms: poisonedMs,
		first_keyed_state_statuses: firstAnswers,
		rebuild_ms: rebuildMs,
		poison: totals.poison - before.poison,
		faulted: totals.faulted,
		reopens: totals.reopens - before.reopens,
		verified_records: f.owners.reduce((n, owner) => n + owner.acked.size, 0),
		byte_mismatches: problems.length,
	});
	expect(firstAnswers.every((status) => status === 503), JSON.stringify(firstAnswers)).toBe(true);
	expect(totals.faulted, f.errors()).toBe(0);
	expect(totals.poison - before.poison).toBeGreaterThanOrEqual(f.owners.length);
	expect(totals.reopens - before.reopens).toBeGreaterThanOrEqual(f.owners.length);
	restoredTails.forEach((tail, i) => expect(tail).toBeGreaterThanOrEqual(backupTails[i] ?? 0));
	expect(problems, problems.join("\n")).toEqual([]);
});
