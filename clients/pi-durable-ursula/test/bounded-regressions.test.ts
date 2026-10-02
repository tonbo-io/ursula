// Regression tests for defects found while wiring the bounded LocalStore into UrsulaStorage.
import type { StorageWrite } from "@earendil-works/pi-durable";
import { expect, it } from "vitest";
import { K } from "../src/families.ts";
import { encodeRecord } from "../src/keyed-batch.ts";
import { H } from "../src/protocol.ts";
import type { LogTransport } from "../src/transport.ts";
import { b64 } from "../src/tuple.ts";
import { ctx, FakeUrsula, freshPath, openOn } from "./helpers.ts";

const sessionDoc = (id: number, kind: string, value: unknown): StorageWrite =>
	({
		type: "document.create",
		record: { id, kind, scope: { kind: "conversation", conversationId: 1 }, history: "all", createdAt: 0 },
		content: { kind: "base", version: 1, value },
	}) as unknown as StorageWrite;

it("a document.copy whose source is not cached reads it remotely instead of rejecting the commit", async () => {
	// The planner used to catch every error of its copy-source read, including the LocalStore's
	// internal cache miss, and turned it into StorageRejected (a durable faulted task in Pi).
	const fake = new FakeUrsula();
	const path = freshPath();
	const a = await openOn(fake, path, { stateStore: "bounded" });
	await a.commit([{ type: "conversation", value: { id: 1 } } as StorageWrite], ctx);
	await a.commit([{ type: "conversation", value: { id: 2 } } as StorageWrite], ctx);
	const seq = await a.commit([sessionDoc(10, "k", { v: 1 })], ctx);
	await a.close(ctx);
	// Reopen with a fresh floor above every ID: document 10 is no longer complete-at-mint.
	const b = await openOn(fake, path, { stateStore: "bounded", cacheBudgetBytes: 4096 });
	b.localStore?.evict(0);
	const copy = {
		type: "document.copy",
		record: { id: 11, kind: "k", scope: { kind: "conversation", conversationId: 2 }, history: "all", createdAt: 0 },
		source: { id: 10, at: seq },
	} as unknown as StorageWrite;
	await b.commit([copy], ctx);
	expect((await b.document(11 as never, "current", ctx))?.value).toEqual({ v: 1 });
	await b.close(ctx);
});

it("a fence open whose catch-up replay fails rejects instead of spinning", async () => {
	// The LocalStore swallowed a rejected commitInFlight wait and asked again; a pre-claim replay
	// that fails at once (past the open deadline) then spun forever.
	const fake = new FakeUrsula({ indexer: "aggressive" });
	const path = freshPath();
	const a = await openOn(fake, path, { stateStore: "bounded" });
	await a.commit([{ type: "conversation", value: { id: 10 } } as StorageWrite], ctx);
	let injected = false;
	fake.fault = (r) => {
		if (!injected && r.op === "scan" && r.scan?.start === b64("1")) {
			injected = true;
			void fake.appendRaw(path, encodeRecord(1, [{ op: "p", key: K.c(77), value: '{"id":77}' }]), fake.records(path).length);
		}
		// Once the foreign record is published, every log read fails.
		return injected && r.op === "read" ? { type: "respond", status: 503 } : undefined;
	};
	await expect(openOn(fake, path, { stateStore: "bounded", mode: "fence", timing: { openDeadlineMs: 300 } })).rejects.toThrow(/deadline/);
}, 10_000);

it("a bounded open whose replay hits a lagging replica catches up instead of failing as active", async () => {
	// The replay used `consistency=local` reads and did not check that it reached HEAD's N0. A
	// lagging replica then left store.tail < N0, and the activity long-poll at that tail saw the
	// owner's old records: a spurious OwnershipActive.
	const fake = new FakeUrsula({ publishTarget: () => 1 });
	const path = freshPath();
	const a = await openOn(fake, path, { stateStore: "bounded" });
	for (let id = 1; id <= 4; id++) await a.commit([{ type: "conversation", value: { id } } as StorageWrite], ctx);
	// The previous owner stops without a close marker; it is stale, not active.
	const n0 = fake.records(path).length;
	const inner = fake.logTransport(path);
	let laggingReads = 0;
	const log: LogTransport = {
		...inner,
		readRecords: async (from, options) => {
			const page = await inner.readRecords(from, options);
			if (options?.leader === true || options?.longPollMs !== undefined) return page;
			// A replica two records behind: everything at or above n0 − 2 is not there yet.
			const visible = n0 - 2;
			laggingReads++;
			const records = page.records.slice(0, Math.max(0, visible - from));
			const next = from + records.length;
			return {
				...page,
				status: records.length > 0 ? 200 : 204,
				records,
				headers: { ...page.headers, [H.recordNext]: String(next), [H.upToDate]: "true" },
			};
		},
	};
	const b = await openOn(fake, path, { stateStore: "bounded", mode: "fail-if-active", log });
	expect(laggingReads).toBeGreaterThan(0);
	expect(b.localStore?.tail).toBeGreaterThan(n0);
	await b.close(ctx);
});
