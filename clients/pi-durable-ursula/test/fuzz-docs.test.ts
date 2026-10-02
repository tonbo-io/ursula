// Differential fuzz (§11.3) of the document lifecycle: UrsulaStorage vs MemoryStorage with Seq
// mapping (Ursula Seqs have claim and close-marker gaps). Covers current-only vs rewindable
// documents, historical copies, singleton vs key "" vs "k", NUL-extension kinds and keys, and
// >1 KiB kinds and keys on the digest path, with random reopen.
import { type DocumentRecord, MemoryStorage, type Storage, type StorageWrite } from "@earendil-works/pi-durable";
import { expect, it } from "vitest";
import { ctx, freshPath, openOn } from "./helpers.ts";
import { type OwnerVariant, reopen, rng, trials, VARIANTS } from "./fuzz-util.ts";

type Scope = DocumentRecord["scope"];
const SCOPES: { scope: Scope; history?: "latest" | "rewindable"; fork?: string }[] = [
	{ scope: { kind: "session" } },
	{ scope: { kind: "task", taskId: 10 as never } },
	{ scope: { kind: "conversation", conversationId: 1 as never }, history: "latest", fork: "current" },
	{ scope: { kind: "conversation", conversationId: 1 as never }, history: "rewindable", fork: "asOf" },
	{ scope: { kind: "conversation", conversationId: 2 as never }, history: "rewindable", fork: "asOf" },
];
const KINDS = ["a", "a\u0000z", "\ud800", "D".repeat(1200)];
const KEYS = [undefined, "", "\u0000", "k", "k\u0000z", "/very/long/path/".repeat(80), "/very/long/path/".repeat(80) + "x"];

async function runSeed(seed: number, rounds: number, variant: OwnerVariant): Promise<string[]> {
	const { rnd, pick } = rng(seed);
	const divergences: string[] = [];
	for (let trial = 0; trial < rounds && divergences.length === 0; trial++) {
		const fake = variant.fake();
		const path = freshPath();
		const memory = new MemoryStorage();
		let ursula = await openOn(fake, path, variant.options);
		const seqPairs: [number, number][] = [];
		const setup: StorageWrite[] = [
			{ type: "conversation", value: { id: 1 } as never },
			{ type: "conversation", value: { id: 2 } as never },
			{
				type: "task",
				value: { id: 10, conversationId: 1, kind: "t", version: 1, input: null, background: false, abortRequested: false, state: { status: "pending", checkpoint: {} } } as never,
			},
		];
		seqPairs.push([await memory.commit(setup, ctx), await ursula.commit(setup, ctx)]);
		let nextDoc = 100;
		const created: number[] = [];
		const history: string[] = [];
		for (let step = 0; step < 14; step++) {
			const writes: StorageWrite[] = [];
			const n = 1 + Math.floor(rnd() * 3);
			for (let i = 0; i < n; i++) {
				const r = rnd();
				if (r < 0.3 || created.length === 0) {
					const s = pick(SCOPES);
					const record: Record<string, unknown> = { id: nextDoc, kind: pick(KINDS), scope: s.scope };
					const key = pick(KEYS);
					if (key !== undefined) record.key = key;
					if (s.history !== undefined) Object.assign(record, { history: s.history, fork: s.fork });
					created.push(nextDoc++);
					if (rnd() < 0.2 && created.length > 1) {
						const src = pick(created.slice(0, -1));
						const pair = rnd() < 0.5 ? undefined : pick(seqPairs);
						writes.push({ type: "document.copy", record: record as never, source: { id: src as never, at: (pair ? pair[0] : "current") as never } });
					} else {
						writes.push({ type: "document.create", record: record as never, content: { kind: "base", version: 1, value: { n: 0 } } });
					}
				} else if (r < 0.75) {
					const id = pick(created);
					writes.push(
						rnd() < 0.6
							? { type: "document.change", id: id as never, content: { kind: "delta", version: pick([1, 1, 2]), ops: [["s", ["n"], Math.floor(rnd() * 50)]] as never } }
							: { type: "document.change", id: id as never, content: { kind: "base", version: pick([1, 2]), value: { n: Math.floor(rnd() * 50), b: true } } },
					);
				} else {
					writes.push({ type: "document.retire", id: pick(created) as never });
				}
			}
			const toUrsula = (w: StorageWrite): StorageWrite =>
				w.type === "document.copy" && w.source.at !== "current"
					? { ...w, source: { ...w.source, at: (seqPairs.find(([m]) => m === w.source.at) as [number, number])[1] as never } }
					: w;
			history.push(JSON.stringify(writes));
			let ms: number | undefined;
			let us: number | undefined;
			let me: string | undefined;
			let mo: string | undefined;
			try {
				ms = await memory.commit(writes, ctx);
			} catch (e) {
				me = `${(e as Error).name}: ${(e as Error).message}`;
			}
			try {
				us = await ursula.commit(writes.map(toUrsula), ctx);
			} catch (e) {
				mo = `${(e as Error).name}: ${(e as Error).message}`;
			}
			if (me !== mo) {
				divergences.push(`seed ${seed} trial ${trial} step ${step}: outcome memory=${me} ursula=${mo}\n  ${history.join("\n  ")}`);
				break;
			}
			if (ms !== undefined && us !== undefined) seqPairs.push([ms, us]);
			if (rnd() < 0.3) ursula = await reopen(fake, path, ursula, rnd() < 0.5, variant.options);
			const toMem = (u: number) => seqPairs.find(([, x]) => x === u)?.[0] ?? `?${u}`;
			const fixRec = (r: DocumentRecord | undefined) =>
				r === undefined ? r : { ...r, createdAt: toMem(r.createdAt), ...(r.retiredAt !== undefined ? { retiredAt: toMem(r.retiredAt) } : {}) };
			const out = async (s: Storage, isUrsula: boolean): Promise<string> => {
				const res: unknown[] = [];
				const points: ("current" | number)[] = ["current", ...seqPairs.map((p) => (isUrsula ? p[1] : p[0]))];
				for (const at of points) {
					for (const s0 of SCOPES)
						for (const kind of KINDS)
							for (const key of KEYS) {
								const r = await s.findDocument({ kind, scope: s0.scope, ...(key === undefined ? {} : { key }) } as never, at as never, ctx);
								res.push(["find", r?.id, isUrsula ? fixRec(r) : r]);
							}
					for (const s0 of SCOPES)
						for (const kind of [undefined, ...KINDS]) {
							const ids: number[] = [];
							let c: unknown;
							do {
								const p = await s.scanDocuments({ scope: s0.scope, at: at as never, kind } as never, 1, c as never, ctx);
								ids.push(...p.items.map((x) => x.id));
								c = p.next;
							} while (c !== undefined);
							res.push(["scan", ids]);
						}
					for (const id of created) {
						try {
							const d = await s.document(id as never, at as never, ctx);
							res.push(["doc", id, d === undefined ? undefined : { v: d.version, value: d.value, n: d.deltasSinceBase, rec: isUrsula ? fixRec(d.record) : d.record }]);
						} catch (e) {
							res.push(["doc", id, `throws ${(e as Error).message}`]);
						}
					}
				}
				return JSON.stringify(res);
			};
			const a = await out(memory, false);
			const b = await out(ursula, true);
			if (a !== b) {
				const A = JSON.parse(a) as unknown[];
				const B = JSON.parse(b) as unknown[];
				const i = A.findIndex((x, j) => JSON.stringify(x) !== JSON.stringify(B[j]));
				divergences.push(`seed ${seed} trial ${trial} step ${step}: read differs\n  memory ${JSON.stringify(A[i])}\n  ursula ${JSON.stringify(B[i])}\n  ${history.join("\n  ")}`);
				break;
			}
		}
		await ursula.close(ctx);
	}
	return divergences;
}

// Seeds 1–3 run the default (bounded) owner; the other variants run with fewer trials.
for (const [i, variant] of VARIANTS.entries()) {
	for (const seed of i === 0 ? [1, 2, 3] : [10 * i + 1]) {
		it(`document fuzz seed ${seed} (${variant.name}): 0 divergences vs MemoryStorage`, async () => {
			const n = trials("FUZZ_DOC_TRIALS", 80);
			expect(await runSeed(seed, Math.ceil(n * variant.share), variant)).toEqual([]);
		});
	}
}
