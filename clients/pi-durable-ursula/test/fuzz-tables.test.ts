// Differential fuzz (§11.3): UrsulaStorage vs MemoryStorage on task/submission tables. Exercises
// in-batch duplicate IDs, terminal rewrites, status/conversation/kind moves, requestId remapping,
// lone surrogates, NUL-extension pairs and >1 KiB strings on the digest path, with random reopen
// (graceful and crash takeover).
import { MemoryStorage, type Storage, type StorageWrite } from "@earendil-works/pi-durable";
import { expect, it } from "vitest";
import { ctx, FakeUrsula, freshPath, openOn } from "./helpers.ts";
import { reopen, rng, trials } from "./fuzz-util.ts";

const CONVS = [1, 2];
const KINDS = ["k1", "k", "\ud800", "k\u0000x", "k\u0000", "L".repeat(1100), "L".repeat(1099) + "\ud800"];
const TASK_IDS = [10, 11, 12];
const SUB_IDS = [20, 21, 22];
const REQS = ["a", "b", "\ud801", "R".repeat(2000), "R".repeat(1999) + "S", undefined];
const TSTAT = ["pending", "running", "waiting", "completing", "terminal"] as const;
const SSTAT = ["queued", "placed", "done", "unanswered"] as const;

async function runSeed(seed: number, rounds: number): Promise<string[]> {
	const { rnd, pick } = rng(seed);
	const task = (id: number): StorageWrite => {
		const status = pick(TSTAT);
		const base = {
			id,
			conversationId: pick(CONVS),
			kind: pick(KINDS),
			version: 1,
			input: { r: Math.floor(rnd() * 100) },
			background: rnd() < 0.5,
			abortRequested: rnd() < 0.5,
		};
		const state =
			status === "waiting"
				? { status, checkpoint: { p: 1 }, on: [10], policy: "allSettled" }
				: status === "completing" || status === "terminal"
					? { status, outcome: { status: "completed", result: null } }
					: { status, checkpoint: { p: Math.floor(rnd() * 9) } };
		return { type: "task", value: { ...base, state } } as unknown as StorageWrite;
	};
	const submission = (id: number): StorageWrite => {
		const status = pick(SSTAT);
		const requestId = pick(REQS);
		const v: Record<string, unknown> = { id, conversationId: pick(CONVS), type: "input", status };
		if (requestId !== undefined) v.requestId = requestId;
		if (status === "placed" || status === "done") v.entry = 99;
		if (status === "done") v.answer = 98;
		if (status === "unanswered") v.reason = "r";
		return { type: "submission", value: v } as unknown as StorageWrite;
	};
	const snapshot = async (s: Storage): Promise<string> => {
		const out: unknown[] = [];
		const all = async (f: (c: unknown) => Promise<{ items: readonly { id: number }[]; next?: unknown }>) => {
			const ids: number[] = [];
			let c: unknown;
			do {
				const p = await f(c);
				ids.push(...p.items.map((x) => x.id));
				c = p.next;
			} while (c !== undefined);
			return ids;
		};
		for (const id of TASK_IDS) out.push(["task", id, await s.task(id as never, ctx)]);
		for (const id of SUB_IDS) out.push(["sub", id, await s.submission(id as never, ctx)]);
		for (const status of [undefined, ...TSTAT])
			for (const conversationId of [undefined, ...CONVS])
				for (const kind of [undefined, ...KINDS])
					for (const background of [undefined, true]) {
						const q = { status, conversationId, kind, background } as never;
						out.push(["scanTasks", q, await all((c) => s.scanTasks(q, 1, c as never, ctx))]);
					}
		for (const status of [undefined, ...SSTAT])
			for (const conversationId of [undefined, ...CONVS]) {
				const q = { status, conversationId } as never;
				out.push(["scanSubs", q, await all((c) => s.scanSubmissions(q, 1, c as never, ctx))]);
			}
		for (const conv of CONVS)
			for (const req of REQS)
				if (req !== undefined) out.push(["byReq", conv, req, (await s.submissionByRequest(conv as never, req, ctx))?.id]);
		return JSON.stringify(out);
	};

	const divergences: string[] = [];
	for (let trial = 0; trial < rounds && divergences.length === 0; trial++) {
		const fake = new FakeUrsula();
		const path = freshPath();
		const memory = new MemoryStorage();
		let ursula = await openOn(fake, path);
		const setup: StorageWrite[] = [
			{ type: "conversation", value: { id: 1 } as never },
			{ type: "conversation", value: { id: 2 } as never },
		];
		await memory.commit(setup, ctx);
		await ursula.commit(setup, ctx);
		const history: string[] = [];
		for (let step = 0; step < 12; step++) {
			const writes: StorageWrite[] = [];
			const n = 1 + Math.floor(rnd() * 4);
			for (let i = 0; i < n; i++) writes.push(rnd() < 0.5 ? task(pick(TASK_IDS)) : submission(pick(SUB_IDS)));
			history.push(JSON.stringify(writes.map((w) => (w as { value: unknown }).value)));
			let me: unknown;
			let mo: unknown;
			try {
				await memory.commit(writes, ctx);
			} catch (e) {
				me = (e as Error).message;
			}
			try {
				await ursula.commit(writes, ctx);
			} catch (e) {
				mo = (e as Error).message;
			}
			if (me !== mo) {
				divergences.push(`seed ${seed} trial ${trial} step ${step}: commit outcome memory=${me} ursula=${mo}`);
				break;
			}
			if (rnd() < 0.3) ursula = await reopen(fake, path, ursula, rnd() < 0.5);
			const a = await snapshot(memory);
			const b = await snapshot(ursula);
			if (a !== b) {
				const A = JSON.parse(a) as unknown[];
				const B = JSON.parse(b) as unknown[];
				const i = A.findIndex((x, j) => JSON.stringify(x) !== JSON.stringify(B[j]));
				divergences.push(
					`seed ${seed} trial ${trial} step ${step}: ${JSON.stringify(A[i])}\n  ursula: ${JSON.stringify(B[i])}\n  ${history.join("\n  ")}`,
				);
				break;
			}
		}
		await ursula.close(ctx);
	}
	return divergences;
}

for (const seed of [1, 2, 3]) {
	it(`table fuzz seed ${seed}: 0 divergences vs MemoryStorage`, async () => {
		expect(await runSeed(seed, trials("FUZZ_TABLE_TRIALS", 150))).toEqual([]);
	});
}
