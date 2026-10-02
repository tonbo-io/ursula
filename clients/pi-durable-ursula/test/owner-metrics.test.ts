// Owner metrics (design §7.6) and the overlay alert (§7.5): Session-line remote reads and their
// latency, retries, pinned bytes, and poison, fence, contention and refusal counts in a shared sink.
import type { StorageWrite } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { FencedError, OwnershipActive, OwnershipContention } from "../src/errors.ts";
import { K, META } from "../src/families.ts";
import { FakeUrsula, faults } from "../src/fake/index.ts";
import { OwnerMetrics } from "../src/metrics.ts";
import { planClaim } from "../src/planner.ts";
import type { OwnerAlert } from "../src/storage.ts";
import { b64 } from "../src/tuple.ts";
import { BOUNDED_TIMING } from "./bounded-helpers.ts";
import { ctx, freshPath, openOn } from "./helpers.ts";

const conv = (id: number): StorageWrite[] => [{ type: "conversation", value: { id } as never }];
const OWNER = b64(K.m(META.owner));
const isFlushWait = (r: { op: string; scan?: { key?: string; timeoutMs?: number } }): boolean =>
	r.op === "scan" && r.scan?.key === OWNER && r.scan.timeoutMs !== undefined;

describe("owner metrics (§7.6)", () => {
	it("counts open and Session-line remote reads, their retries and latency, and pinned bytes", async () => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded" });
		for (let i = 0; i < 3; i++) await a.commit(conv(100 + i), ctx);
		await a.close(ctx);

		const metrics = new OwnerMetrics();
		const b = await openOn(fake, path, { stateStore: "bounded", metrics, timing: BOUNDED_TIMING });
		const opened = b.metrics();
		expect(opened.openRemoteReads).toBeGreaterThan(0);
		expect(opened.sessionLineRemoteReads).toBe(0);
		b.localStore?.evict(0);
		fake.fault = faults.sequence([faults.status(503, { "retry-after": "0" })], (r) => r.op === "scan" && !isFlushWait(r));
		expect(await b.conversation(101 as never, ctx)).toEqual({ id: 101 });
		const after = b.metrics();
		expect(after.sessionLineRemoteReads).toBe(2);
		expect(after.remoteReadRetries).toBe(1);
		expect(after.remoteReadLatency.count).toBe(opened.openRemoteReads + 2);
		expect(after.remoteReadLatency.p99Ms).toBeGreaterThanOrEqual(after.remoteReadLatency.p50Ms);
		// The overlay [E, tail) is the pinned set.
		expect(after.pinnedBytes).toBe(b.localStore?.overlayBytes);
		expect(after.pinnedRecords).toBe(after.tail - after.overlayFloor);
		// A local read issues no remote read.
		expect(await b.conversation(101 as never, ctx)).toEqual({ id: 101 });
		expect(b.metrics().sessionLineRemoteReads).toBe(2);
		expect(after.poisons).toBe(0);
		await b.close(ctx);
	});

	it("counts poisons and fences once per storage, and refusals and contention at open, in a shared sink", async () => {
		const metrics = new OwnerMetrics();
		const fake = new FakeUrsula();
		const path = freshPath();
		const a = await openOn(fake, path, { stateStore: "bounded", metrics });
		await a.commit(conv(10), ctx);
		// The owner is active: a fail-if-active open is refused.
		const active = openOn(fake, path, { stateStore: "bounded", metrics, timing: { activityWindowMs: 200 } });
		await a.commit(conv(11), ctx);
		await expect(active).rejects.toBeInstanceOf(OwnershipActive);
		expect(metrics.activeRefusals).toBe(1);

		// A takeover fences a; a's next commit and read both fail, but a counts once.
		const b = await openOn(fake, path, { stateStore: "bounded", mode: "fence", metrics });
		await expect(a.commit(conv(12), ctx)).rejects.toBeInstanceOf(FencedError);
		await expect(a.commit(conv(13), ctx)).rejects.toBeInstanceOf(FencedError);
		expect(metrics.poisons).toBe(1);
		expect(metrics.fences).toBe(1);
		await a.close(ctx);
		await b.close(ctx);

		// Two claims racing for one tail.
		const n = fake.records(path).length;
		let raced = false;
		fake.fault = (r) => {
			if (r.op === "append" && !raced) {
				raced = true;
				const foreign = planClaim(n, { epoch: n, nonce: "f".repeat(32), host: "other", pid: 2, opened_at_ms: 1, mode: "fence" }).text;
				void fake.appendRaw(path, foreign, n);
			}
			return undefined;
		};
		await expect(openOn(fake, path, { stateStore: "bounded", mode: "fence", metrics })).rejects.toBeInstanceOf(OwnershipContention);
		expect(metrics.contentions).toBe(1);
	});

	it("§7.5: alerts once when the overlay passes its alert size while keyed-state lags", async () => {
		const fake = new FakeUrsula({ indexer: "paused" });
		const path = freshPath();
		const alerts: OwnerAlert[] = [];
		const a = await openOn(fake, path, { stateStore: "bounded", overlayAlertBytes: 2000, onAlert: (x) => alerts.push(x), timing: BOUNDED_TIMING });
		for (let i = 0; i < 20; i++) await a.commit(conv(100 + i), ctx);
		expect(alerts).toHaveLength(1);
		expect(alerts[0]).toMatchObject({ kind: "overlay-size", thresholdBytes: 2000 });
		expect(a.metrics().overlayAlerts).toBe(1);
		expect(a.metrics().pinnedBytes).toBeGreaterThanOrEqual(2000);
		expect(a.poison).toBeUndefined();
		await a.close(ctx);
	});
});
