// Drill: blue/green projection format rebuild (design §5.5 "a new format means a new namespace
// rebuilt from record 0", §6.1 U20, §10 M4).
//
// Live bounded owners on an S3-backed node served by the "blue" indexer at projection format 1.
// While blue keeps serving, the U20 maintenance CLI `ursula indexer keyed rebuild
// --projection-format 2` builds every stream's format-2 namespace (same layout, separate `v2/`
// prefix) from record 0 to the stream's tail. A "green" indexer pod at format 2 (the hidden
// `--keyed-projection-format` drill knob) then starts on the same object store from those
// namespaces, and keyed-state is cut over to it under live traffic. Expected: zero poison and zero faulted tasks; E keeps
// catching up after the cutover; at the same tail both formats answer byte-identical rows; both
// `v1/` and `v2/` exist side by side, and deleting a stream removes both (stream GC reaches every
// format of the incarnation); every acknowledged commit is stored byte-for-byte.
import { afterAll, expect, it } from "vitest";
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

/** A full scan (paged by `Stream-Keyed-After`) from one indexer pod's internal API at `source_next = tail`. */
async function scanPod(podUrl: string, bucket: string, key: string, incarnation: bigint, tail: number): Promise<{ status: number; through: string | null; body: string }> {
	let body = "";
	let through: string | null = null;
	let after: string | null = null;
	for (;;) {
		const q = `incarnation=${incarnation}&source_next=${tail}&min_through_record=${tail}&timeout_ms=60000&limit=1000${after === null ? "" : `&after=${after}`}`;
		const r = await fetch(`${podUrl}/v1/keyed/${encodeURIComponent(bucket)}/${encodeURIComponent(key)}?${q}`);
		const text = await r.text();
		if (r.status !== 200) return { status: r.status, through: r.headers.get("stream-keyed-through"), body: text };
		const pageThrough = r.headers.get("stream-keyed-through");
		if (through !== null && pageThrough !== through) return { status: 409, through: pageThrough, body: `through moved from ${through} while paging` };
		through = pageThrough;
		body += text;
		after = r.headers.get("stream-keyed-after");
		if (after === null) return { status: 200, through, body };
	}
}

it.runIf(s3Available())("blue/green: a format-2 pod rebuilds next to format 1 and takes over under live traffic", async () => {
	const s = await Stack.start({ s3: true });
	stack = s;
	await s.createBucket("drill");
	const f = fleetOn(s.url, "drill");
	fleet = f;
	f.start();
	await untilEveryOwnerCommits(f, 10, 120_000);
	await until("E to follow the tail on blue", () => (f.caughtUp(40) ? true : undefined), KNOBS.recoveryMs, 250);
	const blue = s.indexer;
	if (blue === undefined) throw new Error("no blue indexer");

	// Each stream's incarnation, from its served (v1) namespace.
	const incarnations = new Map<string, bigint>();
	for (const owner of f.owners) {
		const key = owner.stream.slice("drill/".length);
		const hex = await until(
			`the v1 namespace of ${owner.stream}`,
			async () => (await s.listS3(`.keyed/drill/${key}/`)).map((o) => /\/([0-9a-f]{16})\/v1\/CURRENT$/.exec(o)?.[1]).find((x) => x !== undefined),
			KNOBS.recoveryMs,
			250,
		);
		incarnations.set(owner.stream, BigInt(`0x${hex}`));
	}

	// Green: the rebuild CLI builds format 2 on the same store from record 0 while blue serves.
	const warmStart = Date.now();
	const rebuilt: number[] = [];
	for (const owner of f.owners) {
		const key = owner.stream.slice("drill/".length);
		const report = (await s.keyedTool("rebuild", "drill", key, incarnations.get(owner.stream) ?? 0n, ["--projection-format", "2"])) as {
			previous_through: number;
			through_record: number;
		};
		expect(report.previous_through).toBe(0);
		expect(report.through_record).toBeGreaterThan(0);
		rebuilt.push(report.through_record);
	}
	const warmMs = Date.now() - warmStart;

	// A green pod at format 2 starts from the rebuilt namespaces and catches up to the tail.
	const green = await s.spawnIndexer({ format: 2 });
	for (const owner of f.owners) {
		const tail = await recordTail(s.url, owner.stream);
		const answer = await scanPod(green.url, "drill", owner.stream.slice("drill/".length), incarnations.get(owner.stream) ?? 0n, tail);
		expect(answer.status, answer.body).toBe(200);
		expect(Number(answer.through)).toBe(tail);
	}

	// Cutover under live traffic.
	const before = f.totals();
	s.indexerProxy.retarget(green.port, true);
	const cutover = Date.now();
	const targets = f.owners.map((owner) => owner.storage?.tail ?? 0);
	await until(
		"E to catch up on green",
		() => (f.owners.every((owner, i) => (owner.storage?.localStore?.overlayFloor ?? 0) >= (targets[i] ?? 0)) ? true : undefined),
		KNOBS.recoveryMs,
		200,
	);
	const catchUpMs = Date.now() - cutover;
	await sleep(KNOBS.settleMs);
	await untilEveryOwnerCommits(f, 5, KNOBS.recoveryMs);
	await f.stop();
	const totals = f.totals();

	// Same tail, same rows from both formats.
	let compared = 0;
	for (const owner of f.owners) {
		const key = owner.stream.slice("drill/".length);
		const tail = await recordTail(s.url, owner.stream);
		const inc = incarnations.get(owner.stream) ?? 0n;
		const [b, g] = await Promise.all([scanPod(blue.url, "drill", key, inc, tail), scanPod(green.url, "drill", key, inc, tail)]);
		expect(b.status, b.body).toBe(200);
		expect(g.status, g.body).toBe(200);
		expect(g.through).toBe(b.through);
		expect(g.body).toBe(b.body);
		compared++;
	}
	const sample = f.owners[0]?.stream.slice("drill/".length) ?? "";
	const formats = new Set((await s.listS3(`.keyed/drill/${sample}/`)).map((o) => /\/(v\d+)\//.exec(o)?.[1]));
	const problems = await f.verify(s.url);

	// Retire blue; stream GC removes every format of the deleted incarnation.
	await blue.proc.stop();
	const del = await fetch(`${s.url}/drill/${sample}`, { method: "DELETE" });
	expect(del.status).toBe(204);
	await until("GC of both formats", async () => ((await s.listS3(`.keyed/drill/${sample}/`)).length === 0 ? true : undefined), 60_000, 250);

	record("blue-green", {
		warm_ms: warmMs,
		rebuilt_through_records: rebuilt,
		e_catch_up_after_cutover_ms: catchUpMs,
		poison: totals.poison - before.poison,
		faulted: totals.faulted,
		streams_compared: compared,
		formats_side_by_side: [...formats].sort(),
		verified_records: f.owners.reduce((n, owner) => n + owner.acked.size, 0),
		byte_mismatches: problems.length,
	});
	expect(totals.poison, f.errors()).toBe(0);
	expect(totals.faulted, f.errors()).toBe(0);
	expect([...formats].sort()).toEqual(["v1", "v2"]);
	expect(problems, problems.join("\n")).toEqual([]);
});
