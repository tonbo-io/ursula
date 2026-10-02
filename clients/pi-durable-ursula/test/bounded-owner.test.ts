// The bounded owner (design §3.5–§3.7, §7.2–§7.9): open through keyed-state, preload, flush loop,
// overlay cap, error policy, close, and the full-resident option.
import { StorageRejected, type StorageWrite } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { FencedError, OpenRefused, OwnershipActive } from "../src/errors.ts";
import { K, META } from "../src/families.ts";
import { type Fault, FakeUrsula, faults } from "../src/fake/index.ts";
import { encodeRecord } from "../src/keyed-batch.ts";
import { H } from "../src/protocol.ts";
import type { LogTransport } from "../src/transport.ts";
import { b64 } from "../src/tuple.ts";
import { BOUNDED_TIMING } from "./bounded-helpers.ts";
import { ctx, freshPath, openOn } from "./helpers.ts";

const conv = (id: number): StorageWrite[] => [{ type: "conversation", value: { id } as never }];
const OWNER = b64(K.m(META.owner));
const META_START = b64("\u0001");
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
const isFlushWait = (r: { op: string; scan?: { key?: string; timeoutMs?: number } }): boolean =>
	r.op === "scan" && r.scan?.key === OWNER && r.scan.timeoutMs !== undefined;

/** Write `n` conversations with a first owner and close it. Returns the log length. */
async function history(fake: FakeUrsula, path: string, n: number, options: Parameters<typeof openOn>[2] = {}): Promise<number> {
	const a = await openOn(fake, path, { stateStore: "bounded", ...options });
	for (let i = 0; i < n; i++) await a.commit(conv(100 + i), ctx);
	await a.close(ctx);
	return fake.records(path).length;
}

describe("bounded open (§3.6)", () => {
	it("reads m/ through keyed-state, replays only [D, N0), and claims at the tail", async () => {
		const fake = new FakeUrsula({ indexer: "aggressive" });
		const path = freshPath();
		const n0 = await history(fake, path, 5);
		const first = fake.requests.length;
		const b = await openOn(fake, path, { stateStore: "bounded", timing: { openLagRecords: 0 } });
		const mine = fake.requests.slice(first);
		const meta = mine.find((r) => r.op === "scan" && r.scan?.start === META_START);
		expect(meta?.scan?.minThroughRecord).toBe(n0);
		// The aggressive indexer published every record, so replay starts at N0 and finds nothing.
		expect(mine.filter((r) => r.op === "read").map((r) => r.from)).toEqual([n0]);
		expect(b.epoch).toBe(n0);
		expect(b.localStore?.overlayFloor).toBe(n0);
		expect(await b.conversation(102 as never, ctx)).toEqual({ id: 102 });
		await b.close(ctx);
	});

	it("asks for D ≥ N0 − openLagRecords and replays the rest of the log into the overlay", async () => {
		const fake = new FakeUrsula({ indexer: "paused" });
		const path = freshPath();
		const n0 = await history(fake, path, 4);
		const b = await openOn(fake, path, { stateStore: "bounded" });
		// Paused: D = 0, so the whole log is in the overlay, and reads merge state(0) with it.
		expect(b.localStore?.overlayFloor).toBe(0);
		expect(b.localStore?.overlayRecords).toBe(n0 + 1);
		expect(await b.conversation(103 as never, ctx)).toEqual({ id: 103 });
		await b.close(ctx);
	});

	it("fails with a retryable keyed-state lag error when keyed-state cannot reach N0 − openLagRecords, writing nothing", async () => {
		const fake = new FakeUrsula({ indexer: "paused" });
		const path = freshPath();
		const n0 = await history(fake, path, 3);
		await expect(openOn(fake, path, { stateStore: "bounded", timing: { openLagRecords: 1, openDeadlineMs: 300, keyedWaitMs: 50 } })).rejects.toThrow(
			/keyed-state lag/,
		);
		expect(fake.records(path).length).toBe(n0);
	});

	it("re-reads m/ at a higher D when the replay outgrows its cap", async () => {
		// A normal indexer publishes only on demand; dropping the finalize request leaves D = 0.
		const fake = new FakeUrsula({ indexer: "normal" });
		const path = freshPath();
		fake.fault = (r) => (r.op === "scan" && r.scan?.timeoutMs === 1 ? faults.dropRequest : undefined);
		const n0 = await history(fake, path, 6, { timing: { flushMaxRecords: 1000 } });
		fake.fault = undefined;
		expect(fake.projectionRows(path)).toEqual([]);
		const first = fake.requests.length;
		const b = await openOn(fake, path, { stateStore: "bounded", timing: { openReplayCapBytes: 200 } });
		const metas = fake.requests.slice(first).filter((r) => r.op === "scan" && r.scan?.start === META_START);
		expect(metas.map((r) => r.scan?.minThroughRecord)).toEqual([0, n0]);
		expect(b.localStore?.overlayFloor).toBe(n0);
		expect(await b.conversation(105 as never, ctx)).toEqual({ id: 105 });
		await b.close(ctx);
	});

	it("preloads the live set with derived t/, s/ and x/ points, so live-task reads are local", async () => {
		const fake = new FakeUrsula({ indexer: "aggressive" });
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded" });
		await a.commit(conv(1), ctx);
		const task = {
			id: 10,
			conversationId: 1,
			kind: "k",
			version: 1,
			input: null,
			background: false,
			abortRequested: false,
			state: { status: "pending", checkpoint: {} },
		};
		await a.commit([{ type: "task", value: task } as never, { type: "submission", value: { id: 11, conversationId: 1, type: "input", status: "queued" } } as never], ctx);
		await a.close(ctx);
		const b = await openOn(fake, path, { stateStore: "bounded" });
		const local = b.localStore;
		const before = local?.metrics.remoteReads ?? 0;
		expect(await b.task(10 as never, ctx)).toEqual(task);
		expect((await b.submission(11 as never, ctx))?.status).toBe("queued");
		expect(await b.conversation(1 as never, ctx)).toEqual({ id: 1 });
		// The running→pending rewrite plans locally: x/10 and t/10 are derived from t.s.
		await b.commit([{ type: "task", value: { ...task, state: { status: "running", checkpoint: {} } } } as never], ctx);
		expect(local?.metrics.remoteReads).toBe(before);
		await b.close(ctx);
	});

	describe("records beyond N0 seen during preload (the current owner is writing)", () => {
		const setup = async (): Promise<{ fake: FakeUrsula; path: string }> => {
			const fake = new FakeUrsula({ indexer: "aggressive" });
			const path = freshPath();
			const a = await openOn(fake, path, { stateStore: "bounded" });
			await a.commit(conv(10), ctx); // a never closes: a crashed or still-running owner
			let injected = false;
			fake.fault = (r) => {
				if (!injected && r.op === "scan" && r.scan?.start === b64("1")) {
					injected = true;
					// The current owner appends right after the new opener's replay; the indexer publishes it.
					void fake.appendRaw(path, encodeRecord(1, [{ op: "p", key: K.c(77), value: '{"id":77}' }]), fake.records(path).length);
				}
				return undefined;
			};
			return { fake, path };
		};

		it("fail-if-active refuses with OwnershipActive and writes nothing", async () => {
			const { fake, path } = await setup();
			const before = fake.records(path).length + 1;
			await expect(openOn(fake, path, { stateStore: "bounded", mode: "fail-if-active" })).rejects.toBeInstanceOf(OwnershipActive);
			expect(fake.records(path).length).toBe(before);
		});

		it("fence replays the new records and claims after them", async () => {
			const { fake, path } = await setup();
			const b = await openOn(fake, path, { stateStore: "bounded", mode: "fence" });
			expect(b.epoch).toBe(fake.records(path).length - 1);
			expect(await b.conversation(77 as never, ctx)).toEqual({ id: 77 });
			await b.close(ctx);
		});
	});

	it("auto falls back to the full-resident store when the node does not serve keyed-state; bounded refuses", async () => {
		const fake = new FakeUrsula({ advertiseKeyedState: false });
		const path = freshPath();
		const a = await openOn(fake, path);
		expect(a.localStore).toBeUndefined();
		await a.commit(conv(10), ctx);
		await a.close(ctx);
		await expect(openOn(fake, path, { stateStore: "bounded" })).rejects.toBeInstanceOf(OpenRefused);
	});

	it("the full-resident option replays from record 0 and reads no keyed-state", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const n0 = await history(fake, path, 3);
		const first = fake.requests.length;
		const b = await openOn(fake, path, { stateStore: "full-resident" });
		expect(b.localStore).toBeUndefined();
		expect(await b.conversation(101 as never, ctx)).toEqual({ id: 101 });
		const mine = fake.requests.slice(first);
		expect(mine.find((r) => r.op === "read")?.from).toBe(0);
		expect(mine.some((r) => r.op === "scan")).toBe(false);
		expect(b.tail).toBe(n0 + 1);
		await b.close(ctx);
	});
});

describe("read path (§3.5) and error policy (§7.6)", () => {
	/** An owner whose cache holds nothing explicit: every old key is a remote read. */
	async function coldOwner(fake: FakeUrsula, path: string, timing = {}) {
		await history(fake, path, 3);
		const b = await openOn(fake, path, { stateStore: "bounded", timing: { ...BOUNDED_TIMING, ...timing } });
		b.localStore?.evict(0);
		return b;
	}

	it("a page above the tail during an in-flight commit waits for it, then merges", async () => {
		const fake = new FakeUrsula({ indexer: "aggressive" });
		const path = freshPath();
		await history(fake, path, 2);
		let release: () => void = () => undefined;
		const gate = new Promise<void>((r) => {
			release = r;
		});
		let hold = false;
		const inner = fake.logTransport(path);
		const log: LogTransport = {
			...inner,
			append: async (body, match) => {
				const out = await inner.append(body, match);
				if (hold) await gate;
				return out;
			},
		};
		const b = await openOn(fake, path, { stateStore: "bounded", log });
		b.localStore?.evict(0);
		hold = true;
		const commit = b.commit(conv(500), ctx);
		await sleep(5); // the record landed and was published; the owner has not applied it yet
		const read = b.conversation(100 as never, ctx);
		await sleep(5);
		release();
		await commit;
		expect(await read).toEqual({ id: 100 });
		expect(b.localStore?.metrics.commitWaits).toBeGreaterThan(0);
		expect(b.poison).toBeUndefined();
		await b.close(ctx);
	});

	it("retries 429 without Retry-After, 503 and a lagging node's 400 on Session-line reads", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const b = await coldOwner(fake, path);
		fake.fault = faults.sequence(
			[faults.status(429), faults.status(503, { [H.retryAfter]: "0" }), faults.status(400, { [H.recordNext]: "0" }), faults.dropResponse],
			(r) => r.op === "scan" && !isFlushWait(r),
		);
		expect(await b.conversation(101 as never, ctx)).toEqual({ id: 101 });
		expect(b.localStore?.metrics.retries).toBe(4);
		expect(b.poison).toBeUndefined();
		await b.close(ctx);
	});

	it("poisons with a plain Error (never StorageRejected) when a Session-line read fails past its deadline", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const b = await coldOwner(fake, path, { readDeadlineMs: 150 });
		fake.fault = (r) => (r.op === "scan" && !isFlushWait(r) ? faults.status(503) : undefined);
		const started = Date.now();
		const error = await b.conversation(101 as never, ctx).then(
			() => undefined,
			(e: unknown) => e,
		);
		expect(Date.now() - started).toBeLessThan(2000);
		expect(error).toBeInstanceOf(Error);
		expect(error).not.toBeInstanceOf(StorageRejected);
		expect(String(error)).toMatch(/keyed-state read failed/);
		expect(b.poison).toBeDefined();
		await expect(b.commit(conv(900), ctx)).rejects.toThrow(/poisoned/);
		await expect(b.mintId()).rejects.toThrow(/poisoned/);
		await b.close(ctx);
	});

	it("poisons at once on 401 from a Session-line read", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const b = await coldOwner(fake, path);
		fake.fault = (r) => (r.op === "scan" && !isFlushWait(r) ? faults.status(401) : undefined);
		await expect(b.conversation(101 as never, ctx)).rejects.toThrow(/not authorized/);
		expect(b.poison).toBeDefined();
		await b.close(ctx);
	});
});

describe("flush loop (§7.9)", () => {
	it("raises E after enough records, one flush-wait at a time", async () => {
		const fake = new FakeUrsula({ indexer: "normal" });
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded", timing: { ...BOUNDED_TIMING, flushMaxRecords: 4 } });
		let inFlight = 0;
		let maxInFlight = 0;
		fake.fault = (r) => {
			if (isFlushWait(r)) {
				inFlight++;
				maxInFlight = Math.max(maxInFlight, inFlight);
				return { type: "delay", ms: 5 } satisfies Fault;
			}
			return undefined;
		};
		for (let i = 0; i < 12; i++) {
			await a.commit(conv(100 + i), ctx);
			inFlight = 0;
		}
		await sleep(50);
		expect(a.localStore?.overlayFloor).toBeGreaterThan(0);
		expect(a.localStore?.overlayRecords).toBeLessThan(4);
		expect(maxInFlight).toBe(1);
		await a.close(ctx);
	});

	it("flushes by age", async () => {
		const fake = new FakeUrsula({ indexer: "normal" });
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded", timing: { ...BOUNDED_TIMING, flushMaxRecords: 1000, flushMaxAgeMs: 30 } });
		await a.commit(conv(100), ctx);
		expect(a.localStore?.overlayFloor).toBe(0);
		await sleep(120);
		expect(a.localStore?.overlayFloor).toBe(a.tail);
		await a.close(ctx);
	});

	it("never poisons on 204s, 5xx, 429 or resets (an indexer outage only delays E); 404 poisons", async () => {
		const fake = new FakeUrsula({ indexer: "paused" });
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded", timing: { ...BOUNDED_TIMING, flushMaxRecords: 1 } });
		fake.fault = faults.random(7, 0.6, [faults.status(503), faults.status(500), faults.status(429), faults.dropRequest, faults.dropResponse], ["scan"]);
		for (let i = 0; i < 5; i++) await a.commit(conv(100 + i), ctx);
		await sleep(200);
		expect(a.poison).toBeUndefined();
		expect(a.flushMetrics?.flushWaits).toBeGreaterThan(2);
		expect(a.localStore?.overlayFloor).toBe(0);
		fake.fault = (r) => (isFlushWait(r) ? faults.status(404) : undefined);
		await sleep(150);
		expect(a.poison?.message).toMatch(/404/);
		await expect(a.commit(conv(200), ctx)).rejects.toThrow(/poisoned/);
		await a.close(ctx);
	});

	it("a takeover is detected by the zombie's next flush-wait (FencedError), even without a commit", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded", timing: { ...BOUNDED_TIMING, flushMaxRecords: 1000, flushMaxAgeMs: 60 } });
		await a.commit(conv(100), ctx);
		const b = await openOn(fake, path, { stateStore: "bounded", mode: "fence" });
		await sleep(200);
		expect(a.poison).toBeInstanceOf(FencedError);
		await expect(a.conversation(100 as never, ctx)).rejects.toBeInstanceOf(FencedError);
		expect(b.poison).toBeUndefined();
		await b.close(ctx);
		await a.close(ctx);
	});

	it("a commit at the overlay cap waits for the flush loop, then proceeds", async () => {
		const fake = new FakeUrsula({ indexer: "normal" });
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded", overlayCapBytes: 1, timing: { ...BOUNDED_TIMING, flushMaxRecords: 1000 } });
		for (let i = 0; i < 3; i++) await a.commit(conv(100 + i), ctx);
		expect(a.flushMetrics?.floorsRaised).toBeGreaterThanOrEqual(3);
		expect(a.poison).toBeUndefined();
		await a.close(ctx);
	});

	it("a commit at the overlay cap poisons at the commit deadline when keyed-state cannot catch up", async () => {
		const fake = new FakeUrsula({ indexer: "paused" });
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded", overlayCapBytes: 1, timing: { ...BOUNDED_TIMING, commitDeadlineMs: 150 } });
		const error = await a.commit(conv(100), ctx).then(
			() => undefined,
			(e: unknown) => e,
		);
		expect(error).not.toBeInstanceOf(StorageRejected);
		expect(String(error)).toMatch(/overlay reached/);
		expect(a.poison).toBeDefined();
		await a.close(ctx);
	});
});

describe("close (§3.7)", () => {
	it("appends the close marker, finalizes with min_through_record = tail and timeout_ms = 1, and stops the flush loop", async () => {
		const fake = new FakeUrsula({ indexer: "paused" });
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded", timing: { ...BOUNDED_TIMING, flushMaxRecords: 1 } });
		await a.commit(conv(100), ctx);
		await a.close(ctx);
		const finalize = fake.requests.filter((r) => r.op === "scan" && r.scan?.timeoutMs === 1);
		expect(finalize.map((r) => r.scan?.minThroughRecord)).toEqual([fake.records(path).length]);
		const after = fake.requests.length;
		await sleep(150);
		expect(fake.requests.length).toBe(after);
		await expect(a.conversation(100 as never, ctx)).rejects.toThrow(/closed/);
	});
});
