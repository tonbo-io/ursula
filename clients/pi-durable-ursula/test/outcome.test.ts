// Commit outcome policy and read-back table (design §3.3, §7.6; invariants I1, I3, I6, I23).
import { MemoryStorage, StorageRejected, type StorageWrite } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { FencedError } from "../src/errors.ts";
import { type FakeRequest, type Fault, faults } from "../src/fake/index.ts";
import type { OwnerAlert } from "../src/storage.ts";
import { ctx, FakeUrsula, freshPath, openOn } from "./helpers.ts";

const conv = (id: number): StorageWrite[] => [{ type: "conversation", value: { id } as never }];
const isAppend = (r: FakeRequest): boolean => r.op === "append";

async function setup(options: Parameters<typeof openOn>[2] = {}) {
	const fake = new FakeUrsula();
	const path = freshPath();
	const storage = await openOn(fake, path, options);
	return { fake, path, storage };
}

/** Number of records that hold a given commit's conversation (exactly-once check). */
const landed = (fake: FakeUrsula, path: string, id: number): number =>
	fake.records(path).filter((r) => r.includes(`{"id":${id}}`) && r.includes('"p"')).length;

describe("ambiguous outcomes resolve by read-back and land exactly once", () => {
	const cases: [string, (Fault | undefined)[]][] = [
		["response dropped (landed)", [faults.dropResponse]],
		["request dropped (not landed)", [faults.dropRequest]],
		["duplicated request (second answer 412)", [faults.duplicate]],
		["503 after apply", [faults.status(503, undefined, true)]],
		["503 before apply", [faults.status(503)]],
		["429 with Retry-After", [faults.status(429, { "retry-after": "0" })]],
		["injected 412 without apply", [faults.status(412)]],
		["timeout, then the delayed request lands", [faults.delayApply(15)]],
		["timeout → 429 storm without Retry-After → delayed commit", [faults.delayApply(30), faults.status(429), faults.status(429), faults.status(429)]],
		["timeout, then 422 (read back, not rejected)", [faults.dropRequest, faults.status(422)]],
		["timeout, then a 404 whose read-back finds the resend landed", [faults.dropRequest, faults.status(404, undefined, true)]],
	];
	for (const [name, sequence] of cases) {
		it(name, async () => {
			const { fake, path, storage } = await setup();
			fake.fault = faults.sequence(sequence, isAppend);
			const appendsBefore = fake.requests.filter(isAppend).length;
			const seq = await storage.commit(conv(10), ctx);
			await fake.settle();
			// Every injected fault was consumed by an attempt of this commit.
			expect(fake.requests.filter(isAppend).length - appendsBefore).toBeGreaterThanOrEqual(sequence.length);
			expect(seq).toBe(1);
			expect(landed(fake, path, 10)).toBe(1);
			expect(await storage.conversation(10 as never, ctx)).toEqual({ id: 10 });
			fake.fault = undefined;
			expect(await storage.commit(conv(11), ctx)).toBeGreaterThan(seq);
			await storage.close(ctx);
		});
	}

	it("read-back retries through transport errors and a lagging node's 400", async () => {
		const { fake, path, storage } = await setup();
		fake.fault = faults.all(
			faults.nth(faults.dropResponse, 1, isAppend),
			faults.sequence([faults.dropRequest, faults.status(503), faults.status(400, { "stream-record-next": "0" })], (r) => r.op === "read"),
		);
		expect(await storage.commit(conv(10), ctx)).toBe(1);
		expect(landed(fake, path, 10)).toBe(1);
		await storage.close(ctx);
	});
});

describe("deterministic rejections", () => {
	for (const status of [400, 413, 422]) {
		it(`${status} on the first attempt is StorageRejected with an alert`, async () => {
			const alerts: OwnerAlert[] = [];
			const { fake, path, storage } = await setup({ onAlert: (a) => alerts.push(a) });
			fake.fault = faults.nth(faults.status(status));
			await expect(storage.commit(conv(10), ctx)).rejects.toBeInstanceOf(StorageRejected);
			expect(alerts.map((a) => a.status)).toEqual([status]);
			expect(landed(fake, path, 10)).toBe(0);
			expect(await storage.commit(conv(10), ctx)).toBe(1);
			await storage.close(ctx);
		});
	}
	it("429 without Retry-After (node quota) is StorageRejected with no retry", async () => {
		const { fake, storage } = await setup();
		fake.fault = faults.nth(faults.status(429));
		const before = fake.requests.length;
		await expect(storage.commit(conv(10), ctx)).rejects.toBeInstanceOf(StorageRejected);
		expect(fake.requests.length - before).toBe(1);
		await storage.close(ctx);
	});
	it("a rejected commit does not advance m/next_id or nextId (I23)", async () => {
		const { fake, path, storage } = await setup();
		fake.fault = faults.nth(faults.status(422));
		await expect(storage.commit([{ type: "conversation", value: { id: 500 } as never }], ctx)).rejects.toBeInstanceOf(StorageRejected);
		expect(await storage.mintId()).toBe(2);
		await storage.close(ctx);
		const again = await openOn(fake, path);
		expect(await again.mintId()).toBe(2);
		await again.close(ctx);
	});
	it("pre-check rejections write nothing", async () => {
		const { fake, path, storage } = await setup();
		const before = fake.records(path).length;
		let deep: unknown = 1;
		for (let i = 0; i < 130; i++) deep = [deep];
		const write = { type: "entry", value: { id: 5, conversationId: 1, kind: "k", deep } } as unknown as StorageWrite;
		await expect(storage.commit([write], ctx)).rejects.toBeInstanceOf(StorageRejected);
		const big = { type: "entry", value: { id: 6, conversationId: 1, kind: "k", text: "x".repeat(33 * 1024 * 1024) } } as unknown as StorageWrite;
		await expect(storage.commit([big], ctx)).rejects.toThrow(/exceeds the 33554432-byte limit/);
		expect(fake.records(path).length).toBe(before);
		await storage.close(ctx);
	});
	it("MemoryStorage validation errors keep their class and message", async () => {
		const { storage } = await setup();
		const memory = new MemoryStorage();
		const writes: StorageWrite[] = [...conv(7), { type: "entry", value: { id: 7, conversationId: 7, kind: "k" } as never }];
		const m = await memory.commit(writes, ctx).catch((e: Error) => e);
		const u = await storage.commit(writes, ctx).catch((e: Error) => e);
		expect(u).toBeInstanceOf(Error);
		expect(u).not.toBeInstanceOf(StorageRejected);
		expect((u as Error).message).toBe((m as Error).message);
		await storage.close(ctx);
	});
});

describe("poison", () => {
	for (const status of [404, 409, 410]) {
		it(`${status} on the first attempt poisons`, async () => {
			const { fake, storage } = await setup();
			fake.fault = faults.nth(faults.status(status));
			const error = await storage.commit(conv(10), ctx).catch((e: Error) => e);
			expect(error).toBeInstanceOf(Error);
			expect(error).not.toBeInstanceOf(StorageRejected);
			await expect(storage.conversation(1 as never, ctx)).rejects.toThrow(/poisoned/);
			await expect(storage.mintId()).rejects.toThrow(/poisoned/);
			await storage.close(ctx);
		});
	}
	it("401 poisons with a plain Error", async () => {
		const { fake, storage } = await setup();
		fake.fault = faults.nth(faults.status(401));
		await expect(storage.commit(conv(10), ctx)).rejects.toThrow(/not authorized/);
		await expect(storage.commit(conv(11), ctx)).rejects.toThrow(/poisoned/);
	});
	it("ambiguous then 410 with nothing landed: read back, then poison", async () => {
		const { fake, path, storage } = await setup();
		fake.fault = faults.sequence([faults.dropRequest, faults.status(410)], isAppend);
		await expect(storage.commit(conv(10), ctx)).rejects.toThrow(/unavailable/);
		expect(landed(fake, path, 10)).toBe(0);
		await expect(storage.task(1 as never, ctx)).rejects.toThrow(/poisoned/);
	});
	it("a 2xx with another Stream-Record-Start poisons", async () => {
		const { fake, storage } = await setup();
		fake.fault = faults.nth(faults.status(204, { "stream-record-start": "99" }));
		await expect(storage.commit(conv(10), ctx)).rejects.toThrow(/invariant violation/);
		await expect(storage.commit(conv(11), ctx)).rejects.toThrow(/poisoned/);
	});
	it("transient failures past the commit deadline poison (never StorageRejected)", async () => {
		const { fake, storage } = await setup({ timing: { commitDeadlineMs: 150 } });
		fake.fault = (r) => (r.op === "append" ? faults.status(503) : undefined);
		const error = await storage.commit(conv(10), ctx).catch((e: Error) => e);
		expect(error).not.toBeInstanceOf(StorageRejected);
		expect((error as Error).message).toMatch(/did not resolve/);
		await expect(storage.commit(conv(11), ctx)).rejects.toThrow(/poisoned/);
	});
	it("a foreign record at N fences the owner (FencedError, terminal)", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path, { mode: "fence" });
		await a.commit(conv(10), ctx);
		const b = await openOn(fake, path, { mode: "fence" });
		await b.commit(conv(20), ctx);
		await expect(a.commit(conv(11), ctx)).rejects.toBeInstanceOf(FencedError);
		await expect(a.conversation(10 as never, ctx)).rejects.toBeInstanceOf(FencedError);
		await a.close(ctx);
		// The zombie's close marker never lands.
		expect(await b.commit(conv(21), ctx)).toBe(b.tail - 1);
		await b.close(ctx);
	});
});

describe("random faults vs MemoryStorage", () => {
	it("every commit lands exactly once and state matches the oracle", async () => {
		const choices: Fault[] = [
			faults.dropRequest,
			faults.dropResponse,
			faults.duplicate,
			faults.delayApply(3),
			faults.delay(2),
			faults.status(503),
			faults.status(503, undefined, true),
			faults.status(429, { "retry-after": "0" }),
			faults.status(412),
		];
		for (let seed = 1; seed <= 8; seed++) {
			const fake = new FakeUrsula();
			const path = freshPath();
			const storage = await openOn(fake, path);
			const memory = new MemoryStorage();
			fake.fault = faults.random(seed, 0.35, choices);
			for (let i = 0; i < 25; i++) {
				const writes: StorageWrite[] = [
					...conv(100 + i),
					{ type: "entry", value: { id: 1000 + i, conversationId: 100 + i, kind: "k", payload: "y".repeat(i % 3 === 0 ? 1 << 20 : 10) } as never },
				];
				await memory.commit(writes, ctx);
				await storage.commit(writes, ctx);
				await fake.settle();
			}
			fake.fault = undefined;
			for (let i = 0; i < 25; i++) {
				expect(landed(fake, path, 100 + i)).toBe(1);
				expect((await storage.entry((1000 + i) as never, ctx))?.entry).toEqual((await memory.entry((1000 + i) as never, ctx))?.entry);
			}
			expect(await storage.scanConversations({}, 1000, undefined, ctx)).toEqual(await memory.scanConversations({}, 1000, undefined, ctx));
			await storage.close(ctx);
		}
	});
});
