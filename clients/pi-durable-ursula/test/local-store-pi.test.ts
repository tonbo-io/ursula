// M0e at the Pi level: the commit planner and the §4.5 read plans running over a LocalStore under the
// deterministic scheduler, differentially against MemoryStorage. History written before "open" uses
// IDs below the opening `m/next_id`; afterwards writes mix old IDs (remote, merged ranges) and newly
// minted ones (complete-at-mint, §7.3). Scale with LOCAL_STORE_PI_CASES.
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { MemoryStorage, type StorageWrite } from "@earendil-works/pi-durable";
import { expect, it } from "vitest";
import type { KeyedOp } from "../src/keyed-batch.ts";
import { LocalStore } from "../src/local-store/local-store.ts";
import * as pi from "../src/pi-layer.ts";
import { planCommit } from "../src/planner.ts";
import { FullResidentStateStore, type StateView } from "../src/state-store.ts";
import { drain, FakeProjection, prng } from "./local-store-fake.ts";

const CASES = Number(process.env.LOCAL_STORE_PI_CASES ?? 2000);
const ctx: Context = BACKGROUND_CONTEXT;
const KINDS = ["k", "k\u0000", "\ud800"];
const TSTAT = ["pending", "running", "waiting", "terminal"] as const;
const SSTAT = ["queued", "placed", "done"] as const;
type Rng = ReturnType<typeof prng>;

/** A read: the same question asked of the Pi layer over a view and of MemoryStorage. */
interface PiRead {
	readonly name: string;
	readonly local: (v: StateView) => unknown;
	readonly memory: (m: MemoryStorage) => Promise<unknown>;
}

function readGen(r: Rng, ids: number[], convs: number[]): PiRead {
	const id = r.pick(ids) as never;
	const conv = r.pick(convs) as never;
	const limit = 1 + r.int(4);
	switch (r.int(8)) {
		case 0:
			return { name: `task ${id}`, local: (v) => pi.task(v, id), memory: (m) => m.task(id, ctx) };
		case 1:
			return { name: `submission ${id}`, local: (v) => pi.submission(v, id), memory: (m) => m.submission(id, ctx) };
		case 2: {
			const q = { ...(r.chance(0.5) ? { status: r.pick(TSTAT) } : {}), ...(r.chance(0.4) ? { conversationId: conv } : {}), ...(r.chance(0.3) ? { kind: r.pick(KINDS) } : {}) } as never;
			return { name: `scanTasks ${JSON.stringify(q)} ${limit}`, local: (v) => pi.scanTasks(v, q, limit, undefined), memory: (m) => m.scanTasks(q, limit, undefined, ctx) };
		}
		case 3: {
			const q = { ...(r.chance(0.5) ? { status: r.pick(SSTAT) } : {}), ...(r.chance(0.5) ? { conversationId: conv } : {}) } as never;
			return { name: `scanSubmissions ${JSON.stringify(q)}`, local: (v) => pi.scanSubmissions(v, q, limit, undefined), memory: (m) => m.scanSubmissions(q, limit, undefined, ctx) };
		}
		case 4: {
			const req = `r${r.int(4)}`;
			return { name: `byRequest ${conv} ${req}`, local: (v) => pi.submissionByRequest(v, conv, req), memory: (m) => m.submissionByRequest(conv, req, ctx) };
		}
		case 5: {
			const q = { conversationId: conv } as never;
			return { name: `scanEntries ${conv}`, local: (v) => pi.scanEntries(v, q, limit, undefined), memory: (m) => m.scanEntries(q, limit, undefined, ctx) };
		}
		case 6:
			return { name: `entry ${id}`, local: (v) => pi.entryById(v, id), memory: (m) => m.entry(id, ctx) };
		default:
			return { name: `conversation ${conv}`, local: (v) => pi.conversation(v, conv), memory: (m) => m.conversation(conv, ctx) };
	}
}

type Table = "task" | "submission" | "entry" | "conversation";

function writesGen(r: Rng, pickId: (t: Table) => number, convs: number[]): StorageWrite[] {
	const out: StorageWrite[] = [];
	for (let n = 1 + r.int(3); n > 0; n--) {
		const roll = r.rnd();
		const id = pickId(roll < 0.4 ? "task" : roll < 0.75 ? "submission" : roll < 0.9 ? "entry" : "conversation");
		const conversationId = r.pick(convs);
		if (roll < 0.4) {
			const status = r.pick(TSTAT);
			const state = status === "waiting" ? { status, checkpoint: {}, on: [], policy: "allSettled" } : status === "terminal" ? { status, outcome: { status: "completed", result: r.int(9) } } : { status, checkpoint: { p: r.int(9) } };
			out.push({ type: "task", value: { id, conversationId, kind: r.pick(KINDS), version: 1, input: null, background: r.chance(0.5), abortRequested: false, state } } as never);
		} else if (roll < 0.75) {
			const status = r.pick(SSTAT);
			const v: Record<string, unknown> = { id, conversationId, type: "input", status };
			if (r.chance(0.6)) v.requestId = `r${r.int(4)}`;
			if (status !== "queued") v.entry = 1;
			if (status === "done") v.answer = 1;
			out.push({ type: "submission", value: v } as never);
		} else if (roll < 0.9) {
			out.push({ type: "entry", value: { id, conversationId, kind: "k", n: r.int(9) } } as never);
		} else {
			out.push({ type: "conversation", value: { id } } as never);
		}
	}
	return out;
}

const errorText = (e: unknown): string => (e instanceof Error ? `${e.name}: ${e.message}` : String(e));

interface Totals {
	reads: number;
	commits: number;
	remoteReads: number;
	rejected: number;
}

async function runCase(seed: number, totals: Totals): Promise<string | undefined> {
	const r = prng(seed);
	const fake = new FakeProjection(r);
	const memory = new MemoryStorage();
	let nextId = 2;
	let persistedNextId = 0;
	const convs = [1, 2];
	/** IDs per table that may already exist; reads pick from all of them. */
	const seen: Record<Table, number[]> = { task: [3, 4], submission: [5, 6], entry: [], conversation: [] };
	const allSeen = (): number[] => Object.values(seen).flat();
	let preId = 10;
	/** Record ordinal → MemoryStorage Seq of the same commit (Seqs differ: ordinals have no gaps here, Memory starts at 1). */
	const seqOf = new Map<number, number>();

	// History before open, planned over a full-resident store.
	const full = new FullResidentStateStore();
	const genesis: StorageWrite[] = convs.map((id) => ({ type: "conversation", value: { id } }) as never);
	for (let i = 0, n0 = 1 + r.int(5); i < n0; i++) {
		const writes = i === 0 ? genesis : writesGen(r, (t) => (t === "task" || t === "submission" ? r.pick(seen[t]) : preId++), convs);
		let plan: ReturnType<typeof planCommit>;
		try {
			plan = full.readSync((v) => planCommit(v, writes, { seq: full.tail, epoch: 0, nextId, persistedNextId }));
		} catch {
			continue;
		}
		seqOf.set(plan.seq, await memory.commit(writes, ctx));
		fake.append([...plan.ops]);
		full.apply(plan.seq, plan.ops);
		nextId = Math.max(nextId, plan.nextId);
		persistedNextId = plan.persistedNextId;
	}
	const n0 = full.tail;
	fake.publishTo(r.int(n0 + 1));
	const base = r.int(n0 + 1);
	let inflight: { ordinal: number; ops: readonly KeyedOp[]; writes: StorageWrite[]; nextId: number; persisted: number; waiters: (() => void)[] } | undefined;
	const store = new LocalStore({
		keyedState: fake,
		base,
		freshFloor: nextId,
		pageLimit: 1 + r.int(8),
		cacheBudgetBytes: r.chance(0.3) ? 2000 : 1 << 20,
		backoff: () => Promise.resolve(),
		maxRetries: 1000,
		commitInFlight: () => (inflight === undefined ? undefined : new Promise<void>((resolve) => inflight?.waiters.push(resolve))),
	});
	for (let i = base; i < n0; i++) store.apply(i, fake.log[i] as KeyedOp[]);

	const failures: string[] = [];
	const checks: { read: PiRead; got: unknown; tail: number }[] = [];
	let busy = 0;
	let planning = false;
	const minted: [Table, number][] = [];

	const ack = async (): Promise<void> => {
		if (inflight === undefined) return;
		const c = inflight;
		inflight = undefined;
		store.apply(c.ordinal, c.ops);
		totals.commits++;
		try {
			seqOf.set(c.ordinal, await memory.commit(c.writes, ctx));
		} catch (e) {
			failures.push(`memory rejected a commit the planner accepted: ${errorText(e)}`);
		}
		nextId = Math.max(nextId, c.nextId);
		persistedNextId = c.persisted;
		for (const w of c.waiters) w();
	};
	const startCommit = (): void => {
		if (planning || inflight !== undefined) return;
		planning = true;
		busy++;
		const pickId = (t: Table): number => {
			const old = seen[t];
			if (old.length === 0 || r.chance(t === "entry" || t === "conversation" ? 0.85 : 0.35)) {
				const id = nextId + minted.length;
				minted.push([t, id]);
				return id;
			}
			return r.pick(old);
		};
		const writes = writesGen(r, pickId, convs);
		const input = { epoch: 0, nextId: nextId + minted.length, persistedNextId };
		store
			.read((v) => planCommit(v, writes, { ...input, seq: store.tail }))
			.then(
				(plan) => {
					inflight = { ordinal: fake.append([...plan.ops]), ops: plan.ops, writes, nextId: plan.nextId, persisted: plan.persistedNextId, waiters: [] };
					for (const [t, id] of minted.splice(0)) seen[t].push(id);
					return undefined;
				},
				async (e: unknown) => {
					minted.length = 0;
					totals.rejected++;
					try {
						await memory.commit(writes, ctx);
						failures.push(`planner rejected (${errorText(e)}) what memory accepted: ${JSON.stringify(writes)}`);
					} catch (m) {
						if (errorText(m) !== errorText(e)) failures.push(`error mismatch: memory ${errorText(m)} / local ${errorText(e)}`);
					}
				},
			)
			.finally(() => {
				planning = false;
				busy--;
			});
	};
	const startRead = (): void => {
		const read = readGen(r, [...allSeen(), nextId + 3], convs);
		busy++;
		store
			.read((v) => ({ got: read.local(v), tail: store.tail }))
			.then(({ got, tail }) => checks.push({ read, got, tail }))
			.catch((e: unknown) => failures.push(`${read.name} failed: ${errorText(e)}`))
			.finally(() => busy--);
	};

	const steps = 10 + r.int(25);
	for (let step = 0; step < steps + 400 && failures.length === 0; step++) {
		const finishing = step >= steps;
		if (finishing && busy === 0 && fake.parked.length === 0 && inflight === undefined) break;
		const roll = r.rnd();
		if (finishing) {
			await ack();
			if (fake.parked.length > 0) fake.deliver(r.int(fake.parked.length));
		} else if (roll < 0.25) {
			if (busy < 4) startRead();
		} else if (roll < 0.5) {
			if (fake.parked.length > 0) fake.deliver(r.int(fake.parked.length));
		} else if (roll < 0.62) {
			startCommit();
		} else if (roll < 0.72) {
			await ack();
		} else if (roll < 0.82) {
			fake.lag(store.tail);
			store.advanceFloor(fake.published);
		} else if (roll < 0.95) {
			store.evict(r.pick([0, 500, 1 << 20]));
		} else {
			fake.lag();
		}
		await drain();
		for (const c of checks.splice(0)) {
			totals.reads++;
			if (c.tail !== store.tail) {
				failures.push(`harness: ${c.read.name} settled at tail ${c.tail}, checked at ${store.tail}`);
				continue;
			}
			const want = JSON.stringify((await c.read.memory(memory)) ?? null);
			const g = c.got as { commitSeq?: number } | undefined;
			const got = JSON.stringify((g?.commitSeq !== undefined ? { ...g, commitSeq: seqOf.get(g.commitSeq) } : g) ?? null);
			if (want !== got) failures.push(`seed ${seed} step ${step}: ${c.read.name}\n  local:  ${got}\n  memory: ${want}`);
		}
		if (store.poisoned !== undefined) failures.push(`poisoned: ${store.poisoned.message}`);
	}
	if (failures.length === 0 && (busy > 0 || fake.parked.length > 0)) failures.push(`seed ${seed}: work did not settle`);
	totals.remoteReads += store.metrics.remoteReads;
	store.close();
	return failures[0];
}

it(`M0e Pi level: LocalStore under the scheduler matches MemoryStorage over ${CASES} cases`, async () => {
	const failures: string[] = [];
	const totals: Totals = { reads: 0, commits: 0, remoteReads: 0, rejected: 0 };
	for (let seed = 1; seed <= CASES && failures.length === 0; seed++) {
		const failure = await runCase(seed, totals);
		if (failure !== undefined) failures.push(failure);
	}
	expect(failures).toEqual([]);
	expect(totals.reads).toBeGreaterThan(CASES * 2);
	expect(totals.commits).toBeGreaterThan(CASES);
	expect(totals.remoteReads).toBeGreaterThan(CASES);
	expect(totals.rejected).toBeGreaterThan(0);

});
