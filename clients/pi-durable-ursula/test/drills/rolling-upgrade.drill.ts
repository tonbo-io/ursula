// Drill: rolling upgrade from main's binary to the keyed-streams binary under live traffic, with the
// feature-level raise (design §6.3 "Exit drill (M4)", §10 M4 "a rolling upgrade with zero poison and
// byte-identical records"; bounded-state §8 "a rolling upgrade at level 0, followed by a raise to Lb1
// under live traffic").
//
// URSULA_OLD_BIN is main's binary (scripts/ks_build_old_ursula.sh builds e6d8d70); URSULA_BIN the
// new one. A 3-node cluster (disk WAL, S3 cold store) behind the gateway starts on the old binary at
// level 0 with a live writer appending binary blobs. Phase A replaces the nodes one at a time with
// the new binary, then the gateway, then raises the feature level to 1 while the writer runs. Phase B
// starts live keyed owners (keyed streams need level 1) and rolls every node again, new to new, under
// that keyed traffic. Expected: keyed creates are refused before the raise; zero poison and zero
// faulted tasks; every acknowledged blob and every acknowledged keyed commit is stored byte-for-byte
// and identical on all three nodes.
import { afterAll, expect, it } from "vitest";
import { KEYED_CONTENT_TYPE } from "../../src/protocol.ts";
import { Stack } from "../stack/cluster.ts";
import type { OwnerFleet } from "../stack/fleet.ts";
import { sleep, ursulaBinary } from "../stack/proc.ts";
import { s3Available } from "../stack/s3.ts";
import { fleetOn, KNOBS, record, untilEveryOwnerCommits } from "./common.ts";

const OLD_BIN = process.env.URSULA_OLD_BIN ?? "";
const BLOB = 64;

/** A deterministic 64-byte blob for `seq`, with non-UTF-8 bytes. */
function blob(seq: number): Uint8Array {
	const out = new Uint8Array(BLOB);
	out.set(new TextEncoder().encode(String(seq).padStart(12, "0")));
	for (let i = 12; i < BLOB; i++) out[i] = (seq * 31 + i * 7) & 0xff;
	return out;
}

/** Appends blobs in order, retrying each until acknowledged (a retry after an ambiguous failure may duplicate it). */
class BlobWriter {
	readonly acked: number[] = [];
	failures = 0;
	private stopped = false;
	private loop: Promise<void> | undefined;
	private readonly url: () => string;
	readonly stream: string;

	constructor(url: () => string, stream: string) {
		this.url = url;
		this.stream = stream;
	}

	start(): void {
		this.loop = (async () => {
			let seq = 0;
			while (!this.stopped) {
				try {
					const r = await fetch(`${this.url()}/${this.stream}`, {
						method: "POST",
						headers: { "content-type": "application/octet-stream" },
						body: blob(seq),
						signal: AbortSignal.timeout(10_000),
					});
					await r.text();
					if (r.ok) {
						this.acked.push(seq++);
						await sleep(20);
						continue;
					}
				} catch {
					// retried below
				}
				this.failures++;
				await sleep(200);
			}
		})();
	}

	async stop(): Promise<void> {
		this.stopped = true;
		await this.loop;
	}

	/** Reads the whole stream from `url` and checks every acknowledged blob is there, in order, byte-for-byte. */
	async verify(url: string): Promise<{ problems: string[]; bytes: Uint8Array; duplicates: number }> {
		const chunks: Uint8Array[] = [];
		let offset = "-1";
		for (let i = 0; i < 100_000; i++) {
			const r = await fetch(`${url}/${this.stream}?offset=${encodeURIComponent(offset)}`);
			if (r.status !== 200 && r.status !== 204) return { problems: [`read ${this.stream} at ${offset}: ${r.status} ${await r.text()}`], bytes: new Uint8Array(), duplicates: 0 };
			chunks.push(new Uint8Array(await r.arrayBuffer()));
			offset = r.headers.get("stream-next-offset") ?? offset;
			if (r.headers.get("stream-up-to-date") === "true") break;
		}
		const bytes = new Uint8Array(Buffer.concat(chunks));
		const problems: string[] = [];
		if (bytes.length % BLOB !== 0) problems.push(`${this.stream}: ${bytes.length} bytes is not a whole number of blobs`);
		let previous = -1;
		let duplicates = 0;
		const seen = new Set<number>();
		for (let at = 0; at + BLOB <= bytes.length; at += BLOB) {
			const piece = bytes.subarray(at, at + BLOB);
			const seq = Number(new TextDecoder().decode(piece.subarray(0, 12)));
			if (Buffer.compare(Buffer.from(piece), Buffer.from(blob(seq))) !== 0) problems.push(`${this.stream}: blob at byte ${at} is corrupt`);
			if (seq === previous) duplicates++;
			else if (seq !== previous + 1) problems.push(`${this.stream}: blob ${seq} follows ${previous}`);
			previous = seq;
			seen.add(seq);
		}
		for (const seq of this.acked) if (!seen.has(seq)) problems.push(`${this.stream}: acknowledged blob ${seq} is missing`);
		return { problems, bytes, duplicates };
	}
}

let stack: Stack | undefined;
let fleet: OwnerFleet | undefined;
let writer: BlobWriter | undefined;
afterAll(async () => {
	await writer?.stop();
	await fleet?.stop();
	await stack?.stop();
});

/** Rolls every node to `bin` one at a time (SIGTERM, restart on the same WAL, wait writable). */
async function roll(s: Stack, bin: string): Promise<number[]> {
	const pauses: number[] = [];
	for (const node of [...s.nodes].reverse()) {
		const started = Date.now();
		await s.stopNode(node.id);
		await s.startNode(node.id, { bin, legacy: false });
		await s.waitWritable();
		pauses.push(Date.now() - started);
		await sleep(KNOBS.settleMs);
	}
	return pauses;
}

it.runIf(OLD_BIN !== "" && s3Available())("rolling upgrade from main under live traffic, raise, then a keyed rolling restart", async () => {
	const newBin = ursulaBinary();
	const s = await Stack.start({
		nodes: 3,
		s3: true,
		wal: "disk",
		nodeBins: [OLD_BIN, OLD_BIN, OLD_BIN],
		legacyNodes: [true, true, true],
		gatewayBin: OLD_BIN,
		featureLevel: 0,
	});
	stack = s;
	await s.createBucket("drill");
	const blobs = new BlobWriter(() => s.url, "drill/blobs");
	writer = blobs;
	const created = await fetch(`${s.url}/drill/blobs`, { method: "PUT", headers: { "content-type": "application/octet-stream" } });
	expect(created.ok).toBe(true);
	blobs.start();
	await sleep(KNOBS.settleMs);

	// Phase A: old -> new, node by node, then the gateway, then the raise, all under live writes.
	const pausesA = await roll(s, newBin);
	// Keyed creates are refused until the level is raised.
	const early = await fetch(`${s.url}/drill/keyed-early`, { method: "PUT", headers: { "content-type": KEYED_CONTENT_TYPE } });
	const earlyStatus = early.status;
	await early.text();
	await s.restartGateway(newBin);
	await s.raiseFeatureLevel(1);
	const level = await s.minFeatureLevel();
	await sleep(KNOBS.settleMs);

	// Phase B: live keyed owners, then roll new -> new under them.
	const f = fleetOn(s.url, "drill");
	fleet = f;
	f.start();
	await untilEveryOwnerCommits(f, 10, 120_000);
	const before = f.totals();
	const pausesB = await roll(s, newBin);
	await untilEveryOwnerCommits(f, 10, KNOBS.recoveryMs);
	await f.stop();
	await blobs.stop();
	const totals = f.totals();

	// Byte-identical on every node (each node reads its own replica after catching up).
	const keyedProblems = await f.verify(s.url);
	const blobProblems: string[] = [];
	let duplicates = 0;
	let reference: Uint8Array | undefined;
	for (const node of s.nodes) {
		const v = await blobs.verify(node.url);
		blobProblems.push(...v.problems.map((p) => `node ${node.id}: ${p}`));
		duplicates = v.duplicates;
		if (reference === undefined) reference = v.bytes;
		else if (Buffer.compare(Buffer.from(reference), Buffer.from(v.bytes)) !== 0) blobProblems.push(`node ${node.id}: replica bytes differ from node 1`);
		for (const owner of f.owners) {
			const problems = await owner.verify(node.url);
			keyedProblems.push(...problems.map((p) => `node ${node.id}: ${p}`));
		}
	}
	record("rolling-upgrade", {
		old_bin: OLD_BIN,
		keyed_create_before_raise: earlyStatus,
		level_after_raise: level,
		node_restart_ms_phase_a: pausesA,
		node_restart_ms_phase_b: pausesB,
		blobs_acked: blobs.acked.length,
		blob_write_retries: blobs.failures,
		blob_duplicates_from_retries: duplicates,
		keyed_commits_during_roll: totals.commits - before.commits,
		poison: totals.poison,
		faulted: totals.faulted,
		verified_keyed_records: f.owners.reduce((n, owner) => n + owner.acked.size, 0),
		byte_mismatches: blobProblems.length + keyedProblems.length,
	});
	expect(earlyStatus).toBeGreaterThanOrEqual(400);
	expect(level).toBeGreaterThanOrEqual(1);
	expect(blobs.acked.length).toBeGreaterThan(0);
	expect(blobProblems, blobProblems.join("\n")).toEqual([]);
	expect(keyedProblems, keyedProblems.join("\n")).toEqual([]);
	expect(totals.poison, f.errors()).toBe(0);
	expect(totals.faulted, f.errors()).toBe(0);
});
