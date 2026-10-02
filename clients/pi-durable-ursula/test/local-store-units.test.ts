// Unit tests of the bounded owner's building blocks: skip list, complete-at-mint classifier, and
// targeted LocalStore paths (§3.5 step 3, §7.2, §7.3, §7.5).
import { describe, expect, it } from "vitest";
import { K } from "../src/families.ts";
import { isFreshKey, isFreshRange } from "../src/local-store/fresh.ts";
import { LocalStore } from "../src/local-store/local-store.ts";
import { SkipList } from "../src/local-store/skip-list.ts";
import { OrderedMap } from "../src/ordered-map.ts";
import { H } from "../src/protocol.ts";
import type { KeyedScanOutcome, KeyedScanRequest, KeyedStateTransport } from "../src/transport.ts";
import { b64, strinc, unb64 } from "../src/tuple.ts";
import { drain, prng } from "./local-store-fake.ts";

describe("SkipList", () => {
	it("matches the sorted-array OrderedMap under random operations", () => {
		const r = prng(42);
		for (let trial = 0; trial < 200; trial++) {
			const a = new SkipList<number>(trial + 1);
			const b = new OrderedMap<number>();
			const key = (): string => String.fromCharCode(...Array.from({ length: 1 + r.int(3) }, () => r.pick([0, 1, 2, 0x7f, 0xff])));
			for (let i = 0; i < 300; i++) {
				const roll = r.rnd();
				if (roll < 0.5) {
					const k = key();
					const v = r.int(100);
					a.set(k, v);
					b.set(k, v);
				} else if (roll < 0.7) {
					const k = key();
					a.delete(k);
					b.delete(k);
				} else if (roll < 0.8) {
					const [s, e] = [key(), key()].sort();
					a.deleteRange(s as string, e);
					b.deleteRange(s as string, e as string);
				} else {
					const k = key();
					const all = [...b.entries()];
					const floor = all.filter(([x]) => x <= k).at(-1);
					const ceil = all.find(([x]) => x >= k);
					expect(a.floor(k)).toEqual(floor);
					expect(a.ceiling(k)).toEqual(ceil);
					expect(a.get(k)).toEqual(b.get(k));
				}
			}
			expect([...a.entries()]).toEqual([...b.entries()]);
			expect(a.size).toBe(b.size);
		}
	});

	it("reports removed rows from deleteRange and supports unbounded ranges", () => {
		const s = new SkipList<string>();
		for (const k of ["a", "b", "c", "d"]) s.set(k, k.toUpperCase());
		const removed: string[] = [];
		expect(s.deleteRange("b", undefined, (k) => removed.push(k))).toBe(3);
		expect(removed).toEqual(["b", "c", "d"]);
		expect([...s.entries()]).toEqual([["a", "A"]]);
	});
});

describe("complete-at-mint classifier (§7.3)", () => {
	const F = 100;
	it("classifies point keys by any ID component ≥ F", () => {
		expect(isFreshKey(K.t(100), F)).toBe(true);
		expect(isFreshKey(K.t(99), F)).toBe(false);
		expect(isFreshKey(K.tc(5, 100), F)).toBe(true); // new task in an old conversation
		expect(isFreshKey(K.tc(100, 5), F)).toBe(true);
		expect(isFreshKey(K.e(5, 100), F)).toBe(true); // descending entry ID
		expect(isFreshKey(K.e(5, 99), F)).toBe(false);
		expect(isFreshKey(K.tk("k", 100), F)).toBe(true);
		expect(isFreshKey(K.tk("k", 99), F)).toBe(false);
		expect(isFreshKey(K.m("next_id"), F)).toBe(false);
		expect(isFreshKey(K.da({ id: 3, kind: "k", scope: { kind: "task", taskId: 100 }, createdAt: 1 } as never), F)).toBe(true);
		expect(isFreshKey(K.da({ id: 300, kind: "k", scope: { kind: "task", taskId: 7 }, createdAt: 1 } as never), F)).toBe(false);
	});

	it("covers new scopes and the fresh ends of old scopes, nothing else", () => {
		expect(isFreshRange(K.e(100), strinc(K.e(100)), F)).toBe(true); // new conversation's entries
		expect(isFreshRange(K.e(5), strinc(K.e(5)), F)).toBe(false); // old conversation, all entries
		expect(isFreshRange(K.e(5), K.e(5, 99), F)).toBe(true); // front of e/{old}/: IDs ≥ 100
		expect(isFreshRange(K.e(5), `${K.e(5, 99)}\u0000`, F)).toBe(false);
		expect(isFreshRange(K.ts(1, 100), strinc(K.ts(1)), F)).toBe(true); // tail of t.s/{st}/
		expect(isFreshRange(K.ts(1, 99), strinc(K.ts(1)), F)).toBe(false);
		expect(isFreshRange(K.cot(100), strinc(K.cot(100)), F)).toBe(true);
		expect(isFreshRange(K.t(), strinc(K.t()), F)).toBe(false);
		expect(isFreshRange(K.t(100), undefined, F)).toBe(false);
	});

	it("never claims a range containing a key that is not fresh", () => {
		const r = prng(9);
		const id = (): number => r.pick([0, 1, 99, 100, 101, 2 ** 40]);
		const keys = (): string[] => [K.t(id()), K.e(id(), id()), K.tk(r.pick(["", "k", "k\u0000"]), id()), K.ts(1, id()), K.ds({ kind: "task", taskId: id() as never }, id()), K.sr(id(), "q")];
		const pool = Array.from({ length: 40 }, keys).flat().sort();
		for (let i = 0; i < 4000; i++) {
			const a = r.pick(pool);
			const b = r.pick([...pool, strinc(a), `${a}\u0000`]);
			if (!(a < b) || !isFreshRange(a, b, F)) continue;
			for (const k of pool) if (k >= a && k < b) expect(isFreshKey(k, F)).toBe(true);
		}
	});
});

/** A scripted keyed-state transport: each scan waits for the test to answer it. */
class Scripted implements KeyedStateTransport {
	readonly calls: { req: KeyedScanRequest; answer: (o: KeyedScanOutcome) => void }[] = [];
	scan(req: KeyedScanRequest): Promise<KeyedScanOutcome> {
		return new Promise((answer) => this.calls.push({ req, answer }));
	}
}
const page = (through: number, rows: [string, number, string][], after?: string): KeyedScanOutcome => ({
	status: 200,
	headers: { [H.keyedThrough]: String(through) },
	rows: rows.map(([key, record, value]) => ({ key: b64(key), record, value })),
	through,
	...(after === undefined ? {} : { after: b64(after) }),
});

describe("LocalStore", () => {
	it("discards a page whose D_resp fell below E while it was in flight and refetches", async () => {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 0 });
		store.apply(0, [{ op: "p", key: K.t(1), value: "1" }]);
		store.apply(1, [{ op: "d", key: K.t(1) }]);
		const read = store.read((v) => v.get(K.t(1)) ?? null);
		await drain();
		expect(ks.calls[0]?.req.minThroughRecord).toBe(0);
		store.advanceFloor(2); // the record that deleted t/1 left the overlay
		ks.calls[0]?.answer(page(1, [[K.t(1), 0, "1"]])); // state(1) still has t/1
		await drain();
		expect(store.metrics.stalePagesDiscarded).toBe(1);
		expect(ks.calls[1]?.req.minThroughRecord).toBe(2);
		ks.calls[1]?.answer(page(2, []));
		expect(await read).toBeNull();
	});

	it("merges overlay records [D_resp, tail) over a page, including range deletes", async () => {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 0 });
		store.apply(0, [{ op: "p", key: K.t(3), value: "3" }]);
		store.apply(1, [{ op: "x", start: K.t(1), end: K.t(3) }, { op: "p", key: K.t(4), value: "4" }]);
		const read = store.read((v) => [...v.scan(K.t(), strinc(K.t()))]);
		await drain();
		ks.calls[0]?.answer(page(0, [[K.t(1), 0, "a"], [K.t(2), 0, "b"]]));
		expect(await read).toEqual([
			[K.t(3), "3"],
			[K.t(4), "4"],
		]);
	});

	it("marks only [lo, Stream-Keyed-After] covered for a truncated page", async () => {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 0 });
		const read = store.read((v) => [...v.scan(K.t(), strinc(K.t()))].length);
		await drain();
		ks.calls[0]?.answer(page(0, [[K.t(1), 0, "1"]], K.t(1)));
		await drain();
		expect(store.coveredRanges().map((r) => [r.lo, r.hi])).toEqual([[K.t(), `${K.t(1)}\u0000`]]);
		expect(unb64(ks.calls[1]?.req.start ?? "")).toBe(`${K.t(1)}\u0000`);
		ks.calls[1]?.answer(page(0, [[K.t(2), 0, "2"]]));
		expect(await read).toBe(2);
	});

	it("waits for an in-flight commit when a page is ahead of the tail", async () => {
		const ks = new Scripted();
		let release: (() => void) | undefined;
		const store = new LocalStore({ keyedState: ks, base: 0, commitInFlight: () => new Promise<void>((r) => (release = r)) });
		const read = store.read((v) => v.get(K.t(1)) ?? null);
		await drain();
		ks.calls[0]?.answer(page(1, [[K.t(1), 0, "1"]]));
		await drain();
		expect(store.metrics.commitWaits).toBe(1);
		store.apply(0, [{ op: "p", key: K.t(1), value: "1" }]);
		release?.();
		expect(await read).toBe("1");
	});

	it("writes through fresh keys and serves fresh scopes without remote reads", async () => {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 5, freshFloor: 100 });
		store.apply(5, [
			{ op: "p", key: K.e(100, 101), value: "e" },
			{ op: "p", key: K.t(7), value: "old" },
		]);
		expect(await store.read((v) => [...v.scan(K.e(100), strinc(K.e(100)))])).toEqual([[K.e(100, 101), "e"]]);
		expect(store.cachedRows().map(([k]) => k)).toEqual([K.e(100, 101)]); // t/7 is not covered: not cached
		expect(ks.calls).toHaveLength(0);
	});

	it("keeps ranges pinned by a running read, and evicts them afterwards", async () => {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 0 });
		const read = store.read((v) => [v.get(K.t(1)), v.get(K.t(2))]);
		await drain();
		ks.calls[0]?.answer(page(0, [[K.t(1), 0, "1"]]));
		await drain();
		store.evict(0); // t/1's range is pinned by the read
		expect(store.coveredRanges()).toHaveLength(1);
		ks.calls[1]?.answer(page(0, [[K.t(2), 0, "2"]]));
		expect(await read).toEqual(["1", "2"]);
		expect(ks.calls).toHaveLength(2);
		store.evict(0);
		expect(store.coveredRanges()).toHaveLength(0);
		expect(store.cacheBytes).toBe(0);
	});

	it("raises the fresh floor under pressure, oldest IDs first", async () => {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 0, freshFloor: 100 });
		store.apply(0, [100, 101, 102, 103].map((id) => ({ op: "p" as const, key: K.t(id), value: "x".repeat(100) })));
		const one = store.cacheBytes / 4;
		store.evict(one * 2);
		expect(store.freshFloor).toBe(102);
		expect(store.cachedRows().map(([k]) => k)).toEqual([K.t(102), K.t(103)]);
	});

	/** A store whose cache covers `[t/, strinc(t/))` with rows t/1..t/4 of `size` value octets each. */
	async function covered(size: number, options: { largeValueBytes?: number; big?: number } = {}): Promise<{ store: LocalStore; ks: Scripted; row: number }> {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 0, ...options });
		const read = store.read((v) => [...v.scan(K.t(), strinc(K.t()))].length);
		await drain();
		ks.calls[0]?.answer(page(0, [1, 2, 3, 4].map((id) => [K.t(id), 0, (id === options.big ? "y" : "x").repeat(id === options.big ? size * 4 : size)])));
		expect(await read).toBe(4);
		return { store, ks, row: K.t(1).length + size + 64 };
	}
	const rangesOf = (store: LocalStore): [string, string | undefined][] => store.coveredRanges().map((r) => [r.lo, r.hi]);

	it("§7.5: evicts values over the large-value threshold first, splitting their range", async () => {
		const { store, ks, row } = await covered(100, { largeValueBytes: 200, big: 3 });
		const before = store.cacheBytes;
		store.evict(before - 1);
		// Only the large row left; the rest of the range stays covered around it.
		expect(store.cachedRows().map(([k]) => k)).toEqual([K.t(1), K.t(2), K.t(4)]);
		expect(store.metrics.largeValuesEvicted).toBe(1);
		expect(rangesOf(store)).toEqual([
			[K.t(), K.t(3)],
			[`${K.t(3)}\u0000`, strinc(K.t())],
		]);
		expect(store.cacheBytes).toBe(3 * row);
		expect(await store.read((v) => v.get(K.t(4)))).toBe("x".repeat(100));
		expect(ks.calls).toHaveLength(1);
	});

	it("§7.5: shrinks a range from its cold end, away from the key last read", async () => {
		const { store, row } = await covered(100);
		// The last read scanned from the range's start: the high end is cold.
		store.evict(2 * row);
		expect(store.cachedRows().map(([k]) => k)).toEqual([K.t(1), K.t(2)]);
		expect(rangesOf(store)).toEqual([[K.t(), K.t(3)]]);
		expect(store.metrics.rangesShrunk).toBe(1);
		expect(store.metrics.rangesEvicted).toBe(0);

		const hot = await covered(100);
		expect(await hot.store.read((v) => v.get(K.t(4)))).toBe("x".repeat(100));
		// Rows above the hot key t/4 go first (none), then rows from the bottom up.
		hot.store.evict(2 * row);
		expect(hot.store.cachedRows().map(([k]) => k)).toEqual([K.t(3), K.t(4)]);
		expect(rangesOf(hot.store)).toEqual([[`${K.t(2)}\u0000`, strinc(K.t())]]);
	});

	it("§7.5: coalesces adjacent unpinned ranges once their reads settle", async () => {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 0 });
		const a = store.read((v) => [...v.scan(K.t(1), K.t(3))].length);
		await drain();
		ks.calls[0]?.answer(page(0, [[K.t(1), 0, "1"]]));
		expect(await a).toBe(1);
		const b = store.read((v) => [...v.scan(K.t(3), K.t(5))].length);
		await drain();
		ks.calls[1]?.answer(page(0, [[K.t(4), 0, "4"]]));
		expect(await b).toBe(1);
		expect(rangesOf(store)).toEqual([[K.t(1), K.t(5)]]);
		expect(store.metrics.rangesCoalesced).toBe(1);
		// The coalesced range answers reads across the old boundary locally.
		expect(await store.read((v) => [...v.scan(K.t(1), K.t(5))].map(([k]) => k))).toEqual([K.t(1), K.t(4)]);
		expect(ks.calls).toHaveLength(2);
	});

	it("§7.5: alerts once when the overlay passes the alert size, and re-arms below it", () => {
		const alerts: number[] = [];
		const store = new LocalStore({ keyedState: new Scripted(), base: 0, overlayAlertBytes: 500, onOverlayAlert: (bytes) => alerts.push(bytes) });
		for (let i = 0; i < 6; i++) store.apply(i, [{ op: "p", key: K.t(i), value: "x".repeat(100) }]);
		expect(alerts).toHaveLength(1);
		expect(alerts[0]).toBeGreaterThanOrEqual(500);
		store.advanceFloor(6);
		for (let i = 6; i < 12; i++) store.apply(i, [{ op: "p", key: K.t(i), value: "x".repeat(100) }]);
		expect(alerts).toHaveLength(2);
		expect(store.metrics.overlayAlerts).toBe(2);
	});

	it("retries transient keyed-state failures and poisons after the retry budget", async () => {
		const ks = new Scripted();
		const store = new LocalStore({ keyedState: ks, base: 0, backoff: () => Promise.resolve(), maxRetries: 2 });
		const read = store.read((v) => v.get(K.t(1)));
		for (let i = 0; i < 3; i++) {
			await drain();
			ks.calls[i]?.answer({ status: 503, headers: {}, rows: [] });
		}
		await expect(read).rejects.toThrow(/failed after 3 attempts/);
		await expect(store.read(() => 1)).rejects.toThrow(/failed after 3 attempts/);
	});

	it("rejects reads after close", async () => {
		const store = new LocalStore({ keyedState: new Scripted() });
		store.close();
		await expect(store.read(() => 1)).rejects.toThrow("closed");
	});
});
