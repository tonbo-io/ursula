// Open, claim and close (design §3.6, §3.7, §7.7, §7.8; invariants I4, I5, I23).
import type { StorageWrite } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { ClaimTimeout, FencedError, OpenRefused, OwnershipActive, OwnershipContention } from "../src/errors.ts";
import { K } from "../src/families.ts";
import { faults } from "../src/fake/index.ts";
import { encodeRecord } from "../src/keyed-batch.ts";
import { planClaim } from "../src/planner.ts";
import { ctx, FakeUrsula, freshPath, openOn } from "./helpers.ts";

const conv = (id: number): StorageWrite[] => [{ type: "conversation", value: { id } as never }];
const owners = (fake: FakeUrsula, path: string): { epoch: number; nonce: string; closed_at_ms?: number }[] =>
	fake
		.records(path)
		.filter((r) => r.includes('"AW93bmVyAA"'))
		.map((r) => JSON.parse(r).ops.find((op: unknown[]) => op[1] === "AW93bmVyAA")[2]);
const foreignClaim = (n: number): string =>
	planClaim(n, { epoch: n, nonce: "f".repeat(32), host: "other", pid: 2, opened_at_ms: 1, mode: "fence" }).text;

describe("open", () => {
	it("creates the stream and writes genesis at record 0; first Seq is 1", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const s = await openOn(fake, path);
		expect(s.epoch).toBe(0);
		expect(fake.records(path)[0]).toMatch(/^\{"o":0,"ops":\[\["p","AWZvcm1hdAA",\{"pi_durable_keyed":1,"tuple":1\}\],\["p","AW93bmVyAA",/);
		expect(await s.commit(conv(10), ctx)).toBe(1);
		await s.close(ctx);
		const o = owners(fake, path);
		expect(o.length).toBe(2);
		expect(o[1]?.closed_at_ms).toBeTypeOf("number");
	});

	it("reopens after a clean close without waiting W, claims at the tail", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		await a.commit(conv(10), ctx);
		await a.close(ctx);
		const started = Date.now();
		const b = await openOn(fake, path, { timing: { activityWindowMs: 2000 } });
		expect(Date.now() - started).toBeLessThan(1000);
		expect(b.epoch).toBe(3); // genesis 0, commit 1, close marker 2
		expect(await b.conversation(10 as never, ctx)).toEqual({ id: 10 });
		await b.close(ctx);
	});

	it("fail-if-active waits W for a crashed (unclosed) owner, then claims", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		await a.commit(conv(10), ctx); // a crashes: never closes
		const started = Date.now();
		const b = await openOn(fake, path, { timing: { activityWindowMs: 80 } });
		expect(Date.now() - started).toBeGreaterThanOrEqual(75);
		expect(b.epoch).toBe(2);
		await expect(a.commit(conv(11), ctx)).rejects.toBeInstanceOf(FencedError);
		await b.close(ctx);
	});

	it("fail-if-active refuses while the owner writes within W, and writes nothing", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		let stop = false;
		const writer = (async () => {
			for (let id = 100; !stop; id++) {
				await a.commit(conv(id), ctx);
				await new Promise((r) => setTimeout(r, 5));
			}
		})();
		await new Promise((r) => setTimeout(r, 10));
		await expect(openOn(fake, path, { timing: { activityWindowMs: 200 } })).rejects.toBeInstanceOf(OwnershipActive);
		stop = true;
		await writer;
		expect(owners(fake, path).length).toBe(1); // only genesis: the refused open claimed nothing
		await a.close(ctx);
	});

	it("fail-if-active refuses when records appear during open", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		let injected = false;
		fake.fault = (r) => {
			if (r.op === "read" && !injected) {
				injected = true;
				void a.commit(conv(50), ctx);
			}
			return r.op === "read" ? faults.delay(5) : undefined;
		};
		await expect(openOn(fake, path)).rejects.toBeInstanceOf(OwnershipActive);
		fake.fault = undefined;
		await a.close(ctx);
	});

	it("fence takes over an active owner immediately; the old owner is fenced", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		await a.commit(conv(10), ctx);
		const b = await openOn(fake, path, { mode: "fence", timing: { activityWindowMs: 5000 } });
		await expect(a.commit(conv(11), ctx)).rejects.toBeInstanceOf(FencedError);
		await b.commit(conv(12), ctx);
		expect(await b.conversation(10 as never, ctx)).toEqual({ id: 10 });
		expect(await b.conversation(11 as never, ctx)).toBeUndefined();
		await a.close(ctx);
		await b.close(ctx);
	});

	it("fence wins against a busy zombie through the 412 continuation", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		let fenced: unknown;
		const zombie = (async () => {
			for (let id = 100; ; id++) {
				try {
					await a.commit(conv(id), ctx);
				} catch (e) {
					fenced = e;
					return;
				}
				// The fake answers in microtasks only; yield so timers (and the opener) can run.
				await new Promise((r) => setImmediate(r));
			}
		})();
		await new Promise((r) => setTimeout(r, 5));
		const b = await openOn(fake, path, { mode: "fence" });
		await zombie;
		expect(fenced).toBeInstanceOf(FencedError);
		await b.commit(conv(1), ctx);
		await b.close(ctx);
	});

	it("claims racing for one tail raise OwnershipContention", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		await a.close(ctx);
		const n = fake.records(path).length;
		let raced = false;
		fake.fault = (r) => {
			if (r.op === "append" && !raced) {
				raced = true;
				void fake.appendRaw(path, foreignClaim(n), n);
			}
			return undefined;
		};
		await expect(openOn(fake, path, { mode: "fence" })).rejects.toBeInstanceOf(OwnershipContention);
		expect(fake.records(path).length).toBe(n + 1);
	});

	it("a foreign non-claim record during the claim: fence retries at N+1, fail-if-active refuses", async () => {
		for (const mode of ["fence", "fail-if-active"] as const) {
			const fake = new FakeUrsula();
			const path = freshPath();
			const a = await openOn(fake, path);
			await a.close(ctx);
			const n = fake.records(path).length;
			let raced = false;
			fake.fault = (r) => {
				if (r.op === "append" && !raced) {
					raced = true;
					void fake.appendRaw(path, encodeRecord(0, [{ op: "p", key: K.c(77), value: '{"id":77}' }]), n);
				}
				return undefined;
			};
			if (mode === "fence") {
				const b = await openOn(fake, path, { mode });
				expect(b.epoch).toBe(n + 1);
				expect(await b.conversation(77 as never, ctx)).toEqual({ id: 77 });
				await b.close(ctx);
			} else {
				await expect(openOn(fake, path, { mode })).rejects.toBeInstanceOf(OwnershipActive);
			}
		}
	});

	it("an ambiguous claim resolves by read-back (landed or not)", async () => {
		for (const fault of [faults.dropResponse, faults.dropRequest, faults.duplicate, faults.status(503, undefined, true)]) {
			const fake = new FakeUrsula();
			const path = freshPath();
			fake.fault = faults.nth(fault);
			const s = await openOn(fake, path);
			expect(s.epoch).toBe(0);
			expect(fake.records(path).length).toBe(1);
			fake.fault = undefined;
			expect(await s.commit(conv(10), ctx)).toBe(1);
			await s.close(ctx);
		}
	});

	it("a claim that never resolves fails with ClaimTimeout", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		fake.fault = (r) => (r.op === "append" ? faults.status(503) : undefined);
		await expect(openOn(fake, path, { timing: { claimDeadlineMs: 100 } })).rejects.toBeInstanceOf(ClaimTimeout);
	});

	it("refuses nodes without keyed-batch-v1, and without keyed-state-v1 when required", async () => {
		await expect(openOn(new FakeUrsula({ advertiseKeyedBatch: false }), freshPath())).rejects.toBeInstanceOf(OpenRefused);
		await expect(openOn(new FakeUrsula({ advertiseKeyedState: false }), freshPath(), { requireKeyedState: true })).rejects.toBeInstanceOf(
			OpenRefused,
		);
	});

	it("opens on a node without keyed-batch-v1 only when requireKeyedBatch is false", async () => {
		const fake = new FakeUrsula({ advertiseKeyedBatch: false, advertiseKeyedState: false });
		const path = freshPath();
		const a = await openOn(fake, path, { requireKeyedBatch: false });
		await a.commit(conv(10), ctx);
		await a.close(ctx);
		await expect(openOn(fake, path)).rejects.toBeInstanceOf(OpenRefused);
		const b = await openOn(fake, path, { requireKeyedBatch: false });
		expect(await b.conversation(10 as never, ctx)).toEqual({ id: 10 });
		await b.close(ctx);
	});

	it("works without P7 (max_records replay) when keyed-state is not advertised", async () => {
		const fake = new FakeUrsula({ advertiseKeyedState: false });
		const path = freshPath();
		const a = await openOn(fake, path, { timing: { replayPageRecords: 2 } });
		for (let i = 0; i < 7; i++) await a.commit(conv(10 + i), ctx);
		await a.close(ctx);
		const b = await openOn(fake, path, { timing: { replayPageRecords: 2 } });
		expect((await b.scanConversations({}, 100, undefined, ctx)).items.length).toBe(7);
		await b.close(ctx);
	});

	it("refuses a newer m/format and a non-Pi log", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		await fake.logTransport(path).create();
		await fake.appendRaw(path, encodeRecord(0, [{ op: "p", key: K.m("format"), value: '{"pi_durable_keyed":2,"tuple":1}' }]), 0);
		await expect(openOn(fake, path)).rejects.toThrow(/newer/);
		const other = freshPath();
		await fake.logTransport(other).create();
		await fake.appendRaw(other, encodeRecord(0, []), 0);
		await expect(openOn(fake, other)).rejects.toThrow(/not a Pi Durable keyed log/);
	});

	it("open retries transient HEAD and replay failures", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		await a.commit(conv(10), ctx);
		await a.close(ctx);
		fake.fault = faults.sequence([faults.dropRequest, faults.status(503), faults.status(429)], (r) => r.op === "head" || r.op === "read");
		const b = await openOn(fake, path);
		expect(await b.conversation(10 as never, ctx)).toEqual({ id: 10 });
		await b.close(ctx);
	});
});

describe("mintId and m/next_id (§7.7)", () => {
	it("an explicit entry 100 makes the next mint 101, across reopen", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		await a.commit([...conv(1), { type: "entry", value: { id: 100, conversationId: 1, kind: "k" } as never }], ctx);
		expect(await a.mintId()).toBe(101);
		await a.close(ctx);
		const b = await openOn(fake, path);
		expect(await b.mintId()).toBe(101);
		await b.close(ctx);
	});
	it("m/next_id round-trips past MAX_SAFE_INTEGER and mintId then reports exhaustion", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path);
		await a.commit(conv(Number.MAX_SAFE_INTEGER), ctx);
		await a.close(ctx);
		const b = await openOn(fake, path);
		await expect(b.mintId()).rejects.toThrow("ID space is exhausted");
		await b.close(ctx);
	});
});

describe("close (§3.7)", () => {
	it("waits for the in-flight commit, then rejects everything with 'closed'", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const s = await openOn(fake, path);
		fake.fault = faults.nth(faults.delay(30));
		const pending = s.commit(conv(10), ctx);
		await s.close(ctx);
		expect(await pending).toBe(1);
		await expect(s.commit(conv(11), ctx)).rejects.toThrow("closed");
		await expect(s.mintId()).rejects.toThrow("closed");
		await expect(s.scanTasks({}, 1, undefined, ctx)).rejects.toThrow("closed");
		expect(owners(fake, path).at(-1)?.closed_at_ms).toBeTypeOf("number");
	});
	it("still succeeds when the close marker cannot be appended; the next open pays W", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const s = await openOn(fake, path);
		fake.fault = (r) => (r.op === "append" ? faults.status(503) : undefined);
		await s.close(ctx);
		fake.fault = undefined;
		expect(owners(fake, path).some((o) => o.closed_at_ms !== undefined)).toBe(false);
		const started = Date.now();
		const b = await openOn(fake, path, { timing: { activityWindowMs: 60 } });
		expect(Date.now() - started).toBeGreaterThanOrEqual(55);
		await b.close(ctx);
	});
	it("triggers finalize-on-close on keyed-state", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const s = await openOn(fake, path);
		await s.commit(conv(10), ctx);
		await s.close(ctx);
		const scan = fake.requests.filter((r) => r.op === "scan").at(-1);
		expect(scan?.scan?.minThroughRecord).toBe(fake.records(path).length);
		expect(scan?.scan?.timeoutMs).toBe(1);
	});
});
