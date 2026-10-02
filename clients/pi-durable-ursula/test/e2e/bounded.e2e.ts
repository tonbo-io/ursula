// The bounded owner (LocalStore over keyed-state, design §3.5–§3.7, §7) against the real keyed stack:
// the node's `{stream}/keyed-state` proxy (P3) in front of a keyed-mode `ursula indexer` that folds
// the node's log into `.keyed/` on a filesystem object store.
import type { StorageWrite } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { FencedError } from "../../src/errors.ts";
import { K, META } from "../../src/families.ts";
import { HttpLogTransport } from "../../src/http.ts";
import { b64 } from "../../src/tuple.ts";
import { ctx } from "../helpers.ts";
import { baseUrl, freshStream, openHttp, restartIndexer } from "./env.ts";

const conv = (id: number): StorageWrite[] => [{ type: "conversation", value: { id } as never }];
/** Flush (raise E) after every couple of records, so the owner leans on the indexer early. */
const EAGER_FLUSH = { flushMaxRecords: 2, flushBackoffMaxMs: 200 } as const;

/** Waits until `check` holds, polling. */
async function until(check: () => boolean, ms = 30_000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!check()) {
		if (Date.now() > deadline) throw new Error("condition not reached in time");
		await new Promise((r) => setTimeout(r, 25));
	}
}

/** `GET {stream}/keyed-state` for the owner row, waiting for `through`. */
async function ownerRow(stream: string, through: number): Promise<Response> {
	const key = b64(K.m(META.owner));
	return fetch(`${baseUrl()}/${stream}/keyed-state?key=${key}&min_through_record=${through}&timeout_ms=10000`);
}

describe("bounded owner on the real keyed stack", () => {
	it("HEAD advertises keyed-state-v1 and the owner opens the bounded store", async () => {
		const stream = freshStream();
		const s = await openHttp(stream);
		try {
			const head = await new HttpLogTransport({ baseUrl: baseUrl(), stream }).head();
			expect(head.headers["stream-extensions"]).toContain("keyed-state-v1");
			expect(s.localStore).toBeDefined();
			expect(await s.commit(conv(10), ctx)).toBe(1);
		} finally {
			await s.close(ctx);
		}
	});

	it("serves keyed-state rows from the indexer through the node", async () => {
		const stream = freshStream();
		const s = await openHttp(stream);
		await s.commit(conv(10), ctx);
		const tail = s.tail;
		await s.close(ctx);
		const r = await ownerRow(stream, tail);
		expect(r.status).toBe(200);
		expect(r.headers.get("content-type")).toBe("application/vnd.durable-stream-keyed-rows+ndjson");
		expect(Number(r.headers.get("stream-keyed-through"))).toBeGreaterThanOrEqual(tail);
		expect(r.headers.get("stream-extensions")).toContain("keyed-state-v1");
		const rows = (await r.text()).trim().split("\n");
		expect(rows.length).toBe(1);
		expect(JSON.parse(rows[0] as string).key).toBe(b64(K.m(META.owner)));
		// Beyond the record tail is the node's 400 with the tail.
		const beyond = await ownerRow(stream, tail + 100);
		expect(beyond.status).toBe(400);
		expect(beyond.headers.get("stream-record-next")).toBe(String(tail + 1));
	});

	it("raises E through flush-waits and reopens from the projection", async () => {
		const stream = freshStream();
		const a = await openHttp(stream, { timing: EAGER_FLUSH });
		for (let i = 0; i < 12; i++) await a.commit(conv(100 + i), ctx);
		const local = a.localStore;
		expect(local).toBeDefined();
		await until(() => (local?.overlayFloor ?? 0) >= 10);
		expect(a.flushMetrics?.floorsRaised).toBeGreaterThan(0);
		await a.close(ctx);

		const b = await openHttp(stream);
		expect((await b.scanConversations({}, 100, undefined, ctx)).items.length).toBe(12);
		expect(b.localStore?.metrics.remoteReads).toBeGreaterThan(0);
		expect(b.poison).toBeUndefined();
		await b.close(ctx);
	});

	it("takeover: fence claims an active bounded owner, reads its state, and fences it", async () => {
		const stream = freshStream();
		const a = await openHttp(stream, { timing: EAGER_FLUSH });
		for (let i = 0; i < 6; i++) await a.commit(conv(10 + i), ctx);
		const b = await openHttp(stream, { mode: "fence" });
		expect(b.localStore).toBeDefined();
		expect(b.epoch).toBeGreaterThan(a.epoch);
		await expect(a.commit(conv(99), ctx)).rejects.toBeInstanceOf(FencedError);
		for (let i = 0; i < 6; i++) expect(await b.conversation((10 + i) as never, ctx)).toEqual({ id: 10 + i });
		expect(await b.conversation(99 as never, ctx)).toBeUndefined();
		await b.commit(conv(20), ctx);
		await b.close(ctx);

		const c = await openHttp(stream);
		expect((await c.scanConversations({}, 100, undefined, ctx)).items.map((r) => r.id)).toEqual([10, 11, 12, 13, 14, 15, 20]);
		await c.close(ctx);
	});

	it("indexer restart: a live owner keeps committing and a reopen reads everything", async () => {
		const stream = freshStream();
		const a = await openHttp(stream, { timing: EAGER_FLUSH });
		for (let i = 0; i < 5; i++) await a.commit(conv(200 + i), ctx);
		await until(() => (a.localStore?.overlayFloor ?? 0) >= 4);
		await restartIndexer();
		for (let i = 5; i < 10; i++) await a.commit(conv(200 + i), ctx);
		// The flush loop rides out the restart and raises E again from the new process.
		await until(() => (a.localStore?.overlayFloor ?? 0) >= 9);
		expect(a.poison).toBeUndefined();
		await a.close(ctx);

		await restartIndexer();
		const b = await openHttp(stream);
		const ids = (await b.scanConversations({}, 100, undefined, ctx)).items.map((r) => r.id);
		expect(ids).toEqual([200, 201, 202, 203, 204, 205, 206, 207, 208, 209]);
		await b.commit(conv(300), ctx);
		await b.close(ctx);
	});
});
