// M0e (design §10): the LocalStore overlay/cache (§7.2–§7.3, §3.5) under a deterministic scheduler
// against a fake lagging, truncating projection. Properties, checked after every scheduler step:
// - `cache|R = state(tail)|R` on every explicit covered range R;
// - `cache|S = state(tail)|S` on the fresh set S of complete-at-mint, and no cache row outside R ∪ S;
// - the overlay holds exactly the records `[E, tail)`;
// - every read returns what a full-resident fold (the MemoryStorage-equivalent oracle at the keyed
//   level) returns in the same synchronous pass.
// Scale with LOCAL_STORE_MODEL_CASES (default 10^4 cases; 10^5 for the M0e exit).
import { expect, it } from "vitest";
import { FencedError } from "../src/errors.ts";
import { K } from "../src/families.ts";
import type { KeyedOp } from "../src/keyed-batch.ts";
import { isFreshKey } from "../src/local-store/fresh.ts";
import { LocalStore } from "../src/local-store/local-store.ts";
import { FullResidentStateStore, type StateView } from "../src/state-store.ts";
import { strinc } from "../src/tuple.ts";
import { drain, FakeProjection, prng } from "./local-store-fake.ts";

const CASES = Number(process.env.LOCAL_STORE_MODEL_CASES ?? 10_000);
const F0 = 50;

type Rng = ReturnType<typeof prng>;

/** Pi-shaped keys (§4.3) so the real fresh classifier is exercised. */
function keyGen(r: Rng, fresh: boolean): string {
	const id = (): number => (fresh && r.chance(0.6) ? F0 + r.int(12) : 1 + r.int(8));
	switch (r.int(8)) {
		case 0:
			return K.t(id());
		case 1:
			return K.ts(1 + r.int(2), id());
		case 2:
			return K.e(id(), id());
		case 3:
			return K.cot(id(), id());
		case 4:
			return K.tk(r.pick(["k", "k\u0000", "ÿ"]), id());
		case 5:
			return K.ds({ kind: "conversation", conversationId: id() as never }, id());
		case 6:
			return K.m(r.pick(["next_id", "owner"]));
		default:
			return K.x(id());
	}
}

/** A prefix range of some family, or a range between two random keys. */
function rangeGen(r: Rng, fresh: boolean): [string, string] {
	const id = (): number => (fresh && r.chance(0.5) ? F0 + r.int(12) : 1 + r.int(8));
	const prefixes = [K.t(), K.ts(1), K.e(id()), K.cot(id()), K.tk("k"), K.ds({ kind: "conversation", conversationId: id() as never }), K.x(id()).slice(0, 1)];
	if (r.chance(0.15)) return ["\u0001", "ÿ"];
	if (r.chance(0.6)) {
		const p = r.pick(prefixes);
		if (r.chance(0.3)) return [p + "\u0000".repeat(7) + String.fromCharCode(F0 + r.int(3)), strinc(p)];
		return [p, strinc(p)];
	}
	const a = keyGen(r, fresh);
	const b = keyGen(r, fresh);
	return a < b ? [a, b] : b < a ? [b, a] : [a, `${a}\u0000`];
}

function recordGen(r: Rng, fresh: boolean): KeyedOp[] {
	const ops: KeyedOp[] = [];
	const n = 1 + r.int(5);
	for (let i = 0; i < n; i++) {
		const roll = r.rnd();
		if (roll < 0.65) ops.push({ op: "p", key: keyGen(r, fresh), value: r.chance(0.1) ? "null" : `{"v":${r.int(1000)}}` });
		else if (roll < 0.85) ops.push({ op: "d", key: keyGen(r, fresh) });
		else {
			const [start, end] = rangeGen(r, fresh);
			ops.push({ op: "x", start, end });
		}
	}
	return ops;
}

type ReadSpec = { kind: "get"; key: string } | { kind: "scan"; start: string; end: string; take: number };
const runRead = (view: StateView, spec: ReadSpec): unknown => {
	if (spec.kind === "get") return view.get(spec.key) ?? null;
	const out: [string, string][] = [];
	for (const [k, v] of view.scan(spec.start, spec.end)) {
		out.push([k, v]);
		if (out.length >= spec.take) break;
	}
	return out;
};

interface CaseStats {
	reads: number;
	fetches: number;
	stale: number;
	commitWaits: number;
	evicted: number;
	merged: number;
	shrunk: number;
	coalesced: number;
	large: number;
}

/** One case: a world, a store opened at a random base, and a random schedule. Returns a divergence or undefined. */
async function runCase(seed: number, stats: CaseStats): Promise<string | undefined> {
	const r = prng(seed);
	const fake = new FakeProjection(r);
	const oracle = new FullResidentStateStore();
	// History before open: no key has an ID ≥ F0 (the complete-at-mint precondition).
	const n0 = r.int(8);
	for (let i = 0; i < n0; i++) {
		const ops = recordGen(r, false);
		fake.append(ops);
		oracle.apply(i, ops);
	}
	fake.publishTo(r.int(n0 + 1));
	const base = r.int(n0 + 1);
	let inflight: { ordinal: number; ops: KeyedOp[]; waiters: (() => void)[] } | undefined;
	const store = new LocalStore({
		keyedState: fake,
		base,
		freshFloor: F0,
		pageLimit: 1 + r.int(6),
		cacheBudgetBytes: r.chance(0.3) ? 300 + r.int(600) : 1 << 20,
		backoff: () => Promise.resolve(),
		maxRetries: 1000,
		// Values like `{"v":123}` count as large, so §7.5's large-values-first path runs often.
		largeValueBytes: 8,
		commitInFlight: () =>
			inflight === undefined ? undefined : new Promise<void>((resolve) => (inflight as { waiters: (() => void)[] }).waiters.push(resolve)),
	});
	for (let i = base; i < n0; i++) store.apply(i, fake.log[i] as KeyedOp[]);

	const failures: string[] = [];
	let reading = 0;
	const ack = (): void => {
		if (inflight === undefined) return;
		const { ordinal, ops, waiters } = inflight;
		inflight = undefined;
		store.apply(ordinal, ops);
		oracle.apply(ordinal, ops);
		for (const w of waiters) w();
	};
	const startRead = (): void => {
		const spec: ReadSpec = r.chance(0.4)
			? { kind: "get", key: keyGen(r, true) }
			: (() => {
					const [start, end] = rangeGen(r, true);
					return { kind: "scan", start, end, take: r.chance(0.3) ? 1_000_000 : 1 + r.int(5) };
				})();
		reading++;
		store
			.read((view) => {
				// The oracle runs inside the same synchronous pass, so both see the same tail.
				const got = runRead(view, spec);
				const want = oracle.readSync((v) => runRead(v, spec));
				return { got, want, tail: store.tail };
			})
			.then(({ got, want, tail }) => {
				stats.reads++;
				if (JSON.stringify(got) !== JSON.stringify(want)) {
					failures.push(`read ${JSON.stringify(spec)} at tail ${tail}: got ${JSON.stringify(got)} want ${JSON.stringify(want)}`);
				}
			})
			.catch((e: unknown) => failures.push(`read ${JSON.stringify(spec)} failed: ${String(e)}`))
			.finally(() => reading--);
	};

	const check = (step: string): void => {
		const rows = new Map(store.cachedRows());
		const truth = new Map(oracle.rows());
		const ranges = store.coveredRanges();
		const inRange = (k: string): boolean => ranges.some((x) => k >= x.lo && (x.hi === undefined || k < x.hi));
		for (const x of ranges) {
			for (const [k, row] of truth) {
				if (k >= x.lo && (x.hi === undefined || k < x.hi)) {
					const c = rows.get(k);
					if (c?.value !== row.value || c.record !== row.record) failures.push(`${step}: range missing/different ${JSON.stringify(k)}`);
				}
			}
		}
		for (const [k, row] of rows) {
			const t = truth.get(k);
			if (t === undefined || t.value !== row.value || t.record !== row.record) failures.push(`${step}: cache row ${JSON.stringify(k)} not in state(tail)`);
			if (!inRange(k) && !isFreshKey(k, store.freshFloor)) failures.push(`${step}: cache row ${JSON.stringify(k)} outside covered regions`);
		}
		for (const [k] of truth) if (isFreshKey(k, store.freshFloor) && !rows.has(k)) failures.push(`${step}: fresh row ${JSON.stringify(k)} missing`);
		const want = Array.from({ length: store.tail - store.overlayFloor }, (_, i) => store.overlayFloor + i);
		if (JSON.stringify(store.overlayOrdinals()) !== JSON.stringify(want)) failures.push(`${step}: overlay ${store.overlayOrdinals()} != [${store.overlayFloor}, ${store.tail})`);
		if (store.poisoned !== undefined) failures.push(`${step}: poisoned ${store.poisoned.message}`);
	};

	const steps = 12 + r.int(30);
	for (let step = 0; step < steps + 400 && failures.length === 0; step++) {
		const finishing = step >= steps;
		if (finishing && reading === 0 && fake.parked.length === 0) break;
		const roll = r.rnd();
		if (finishing) {
			ack();
			if (fake.parked.length > 0) fake.deliver(r.int(fake.parked.length));
		} else if (roll < 0.22) {
			if (reading < 4) startRead();
		} else if (roll < 0.5) {
			if (fake.parked.length > 0) fake.deliver(r.int(fake.parked.length));
		} else if (roll < 0.62) {
			if (inflight === undefined) {
				const ops = recordGen(r, true);
				inflight = { ordinal: fake.append(ops), ops, waiters: [] };
				if (r.chance(0.5)) ack();
			}
		} else if (roll < 0.7) {
			ack();
		} else if (roll < 0.8) {
			// A flush-wait completes: D_pub may cover an in-flight record, so E = min(D_pub, tail).
			fake.lag(store.tail);
			store.advanceFloor(fake.published);
		} else if (roll < 0.85) {
			fake.lag();
		} else if (roll < 0.95) {
			store.evict(r.pick([0, 200, 600, 1 << 20]));
		} else {
			store.raiseFreshFloor(store.freshFloor + r.int(4));
		}
		await drain();
		check(`seed ${seed} step ${step}`);
	}
	if (failures.length === 0 && (reading > 0 || fake.parked.length > 0)) failures.push(`seed ${seed}: reads did not settle`);
	stats.fetches += fake.requests;
	stats.stale += store.metrics.stalePagesDiscarded;
	stats.commitWaits += store.metrics.commitWaits;
	stats.evicted += store.metrics.rangesEvicted;
	stats.merged += store.metrics.pagesMerged;
	stats.shrunk += store.metrics.rangesShrunk;
	stats.coalesced += store.metrics.rangesCoalesced;
	stats.large += store.metrics.largeValuesEvicted;
	store.close();
	return failures[0];
}

it(`M0e: overlay/cache equals state(tail) on every covered range over ${CASES} cases`, async () => {
	const stats: CaseStats = { reads: 0, fetches: 0, stale: 0, commitWaits: 0, evicted: 0, merged: 0, shrunk: 0, coalesced: 0, large: 0 };
	const failures: string[] = [];
	for (let seed = 1; seed <= CASES && failures.length === 0; seed++) {
		const f = await runCase(seed, stats);
		if (f !== undefined) failures.push(f);
	}
	expect(failures).toEqual([]);
	expect(stats.reads).toBeGreaterThan(CASES);
	expect(stats.fetches).toBeGreaterThan(CASES);
	// Every §3.5 step-3 branch was exercised: stale pages (D_resp < E), pages ahead of the tail
	// while a commit was in flight, merges, and evictions; and every §7.5 eviction refinement:
	// large values first, cold-end shrinking, and coalescing of adjacent unpinned ranges.
	expect(Math.min(stats.stale, stats.commitWaits, stats.evicted, stats.merged)).toBeGreaterThan(CASES / 100);
	expect(Math.min(stats.shrunk, stats.coalesced, stats.large)).toBeGreaterThan(CASES / 100);
});

it("a page reflecting records above tail with no commit in flight poisons with FencedError", async () => {
	const r = prng(7);
	const fake = new FakeProjection(r, { unavailable: 0, noResponse: 0, timeout204: 0 });
	fake.append([{ op: "p", key: K.t(1), value: "1" }]);
	fake.publishTo(1);
	const store = new LocalStore({ keyedState: fake, base: 0 });
	const read = store.read((v) => v.get(K.t(1)));
	fake.deliver(0);
	await expect(read).rejects.toBeInstanceOf(FencedError);
	await expect(store.read((v) => v.get(K.t(2)))).rejects.toBeInstanceOf(FencedError);
});
