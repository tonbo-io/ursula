// Open, claim, fencing and commit ambiguity (design §3.3, §3.6, §3.7, §7.8) against a real
// single-node ursula through the HTTP transports.
import type { StorageWrite } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { FencedError, OwnershipActive, OwnershipContention } from "../../src/errors.ts";
import { K, META } from "../../src/families.ts";
import { HttpLogTransport, splitRecords } from "../../src/http.ts";
import { encodeRecord, utf8 } from "../../src/keyed-batch.ts";
import { planClaim } from "../../src/planner.ts";
import { KEYED_CONTENT_TYPE } from "../../src/protocol.ts";
import { type HttpOutcome, type LogTransport, TransportError } from "../../src/transport.ts";
import { b64 } from "../../src/tuple.ts";
import { ctx } from "../helpers.ts";
import { baseUrl, freshStream, openHttp } from "./env.ts";

const conv = (id: number): StorageWrite[] => [{ type: "conversation", value: { id } as never }];
const OWNER = b64(K.m(META.owner));

/** Every stored record of `stream`, as text, read straight from the node. */
async function records(stream: string): Promise<string[]> {
	const r = await fetch(`${baseUrl()}/${stream}?record=0`);
	expect(r.status).toBe(200);
	return splitRecords(new Uint8Array(await r.arrayBuffer())).map((b) => new TextDecoder().decode(b));
}

const owners = async (stream: string): Promise<{ epoch: number; closed_at_ms?: number }[]> =>
	(await records(stream))
		.filter((r) => r.includes(`"${OWNER}"`))
		.map((r) => JSON.parse(r).ops.find((op: unknown[]) => op[1] === OWNER)[2]);

/** A third-party append with `Stream-Record-Match`. */
async function rawAppend(stream: string, text: string, match: number): Promise<number> {
	const r = await fetch(`${baseUrl()}/${stream}`, {
		method: "POST",
		headers: { "content-type": KEYED_CONTENT_TYPE, "stream-record-match": String(match) },
		body: text,
	});
	return r.status;
}

const foreignClaim = (n: number): string =>
	planClaim(n, { epoch: n, nonce: "f".repeat(32), host: "other", pid: 2, opened_at_ms: 1, mode: "fence" }).text;

/** A LogTransport whose appends pass through `hook` first. */
function hooked(
	stream: string,
	hook: (inner: LogTransport, body: Uint8Array, match: number) => Promise<HttpOutcome | undefined>,
): LogTransport {
	const inner = new HttpLogTransport({ baseUrl: baseUrl(), stream });
	return {
		head: () => inner.head(),
		create: () => inner.create(),
		readRecords: (from, options) => inner.readRecords(from, options),
		append: async (body, match) => (await hook(inner, body, match)) ?? inner.append(body, match),
	};
}

describe("open and close on a real node", () => {
	it("creates the stream, writes genesis at record 0, and closes with a marker", async () => {
		const stream = freshStream();
		const s = await openHttp(stream);
		expect(s.epoch).toBe(0);
		expect(await s.commit(conv(10), ctx)).toBe(1);
		await s.close(ctx);
		const stored = await records(stream);
		expect(stored.length).toBe(3);
		expect(stored[0]).toMatch(/^\{"o":0,"ops":\[\["p","AWZvcm1hdAA",\{"pi_durable_keyed":1,"tuple":1\}\],\["p","AW93bmVyAA",/);
		const o = await owners(stream);
		expect(o.at(-1)?.closed_at_ms).toBeTypeOf("number");
		const head = await new HttpLogTransport({ baseUrl: baseUrl(), stream }).head();
		expect(head.headers["content-type"]).toBe(KEYED_CONTENT_TYPE);
	});

	it("reopens after a clean close without waiting W and reads back the state", async () => {
		const stream = freshStream();
		const a = await openHttp(stream);
		await a.commit(conv(10), ctx);
		await a.close(ctx);
		const started = Date.now();
		const b = await openHttp(stream, { timing: { activityWindowMs: 3000 } });
		expect(Date.now() - started).toBeLessThan(2000);
		expect(b.epoch).toBe(3);
		expect(await b.conversation(10 as never, ctx)).toEqual({ id: 10 });
		await b.close(ctx);
	});

	it("replays across many max_records pages", async () => {
		const stream = freshStream();
		const a = await openHttp(stream);
		for (let i = 0; i < 25; i++) await a.commit(conv(100 + i), ctx);
		await a.close(ctx);
		const b = await openHttp(stream, { timing: { replayPageRecords: 4 } });
		expect((await b.scanConversations({}, 100, undefined, ctx)).items.length).toBe(25);
		await b.close(ctx);
	});

	it("commits and replays a record over 1 MiB byte-exactly", async () => {
		const stream = freshStream();
		const a = await openHttp(stream);
		const big = "é😀\u0000x".repeat(200_000);
		await a.commit([...conv(1), { type: "entry", value: { id: 5, conversationId: 1, kind: "k", data: { big, n: 1.5 } } as never }], ctx);
		await a.close(ctx);
		const b = await openHttp(stream);
		const found = await b.entry(5 as never, ctx);
		expect(found?.entry.data).toEqual({ big, n: 1.5 });
		expect((await records(stream))[1]?.length).toBeGreaterThan(1024 * 1024);
		await b.close(ctx);
	});
});

describe("fail-if-active and fence on a real node", () => {
	it("fail-if-active waits W for a crashed owner, then claims; the old owner is fenced", async () => {
		const stream = freshStream();
		const a = await openHttp(stream);
		await a.commit(conv(10), ctx); // a crashes: never closes
		const started = Date.now();
		const b = await openHttp(stream, { timing: { activityWindowMs: 200 } });
		expect(Date.now() - started).toBeGreaterThanOrEqual(190);
		expect(b.epoch).toBe(2);
		await expect(a.commit(conv(11), ctx)).rejects.toBeInstanceOf(FencedError);
		await b.commit(conv(12), ctx);
		await b.close(ctx);
	});

	it("fail-if-active refuses while the owner writes within W, and writes nothing", async () => {
		const stream = freshStream();
		const a = await openHttp(stream);
		let stop = false;
		const writer = (async () => {
			for (let id = 100; !stop; id++) {
				await a.commit(conv(id), ctx);
				await new Promise((r) => setTimeout(r, 10));
			}
		})();
		await new Promise((r) => setTimeout(r, 20));
		await expect(openHttp(stream, { timing: { activityWindowMs: 500 } })).rejects.toBeInstanceOf(OwnershipActive);
		stop = true;
		await writer;
		expect((await owners(stream)).length).toBe(1);
		await a.close(ctx);
	});

	it("fence takes over an active owner; the old owner is fenced and its later write is absent", async () => {
		const stream = freshStream();
		const a = await openHttp(stream);
		await a.commit(conv(10), ctx);
		const b = await openHttp(stream, { mode: "fence", timing: { activityWindowMs: 5000 } });
		await expect(a.commit(conv(11), ctx)).rejects.toBeInstanceOf(FencedError);
		await expect(a.commit(conv(12), ctx)).rejects.toBeInstanceOf(FencedError);
		await b.commit(conv(13), ctx);
		expect(await b.conversation(10 as never, ctx)).toEqual({ id: 10 });
		expect(await b.conversation(11 as never, ctx)).toBeUndefined();
		await a.close(ctx);
		await b.close(ctx);
	});

	it("fence wins against a busy zombie", async () => {
		const stream = freshStream();
		const a = await openHttp(stream);
		let fenced: unknown;
		const zombie = (async () => {
			for (let id = 100; ; id++) {
				try {
					await a.commit(conv(id), ctx);
				} catch (e) {
					fenced = e;
					return;
				}
			}
		})();
		await new Promise((r) => setTimeout(r, 20));
		const b = await openHttp(stream, { mode: "fence" });
		await zombie;
		expect(fenced).toBeInstanceOf(FencedError);
		await b.commit(conv(1), ctx);
		await b.close(ctx);
	});

	it("a foreign claim racing for the same tail raises OwnershipContention", async () => {
		const stream = freshStream();
		const a = await openHttp(stream);
		await a.close(ctx);
		const n = (await records(stream)).length;
		let raced = false;
		const log = hooked(stream, async (_inner, _body, match) => {
			if (!raced) {
				raced = true;
				expect(await rawAppend(stream, foreignClaim(match), match)).toBeLessThan(300);
			}
			return undefined;
		});
		await expect(openHttp(stream, { mode: "fence", log })).rejects.toBeInstanceOf(OwnershipContention);
		expect((await records(stream)).length).toBe(n + 1);
	});

	it("a foreign non-claim record during the claim: fence retries at N+1, fail-if-active refuses", async () => {
		for (const mode of ["fence", "fail-if-active"] as const) {
			const stream = freshStream();
			const a = await openHttp(stream);
			await a.close(ctx);
			const n = (await records(stream)).length;
			let raced = false;
			const log = hooked(stream, async (_inner, _body, match) => {
				if (!raced) {
					raced = true;
					await rawAppend(stream, encodeRecord(0, [{ op: "p", key: K.c(77), value: '{"id":77}' }]), match);
				}
				return undefined;
			});
			if (mode === "fence") {
				const b = await openHttp(stream, { mode, log });
				expect(b.epoch).toBe(n + 1);
				expect(await b.conversation(77 as never, ctx)).toEqual({ id: 77 });
				await b.close(ctx);
			} else {
				await expect(openHttp(stream, { mode, log })).rejects.toBeInstanceOf(OwnershipActive);
			}
		}
	});

	it("concurrent fence openers: every loser fails with OwnershipContention or is fenced", async () => {
		const stream = freshStream();
		const first = await openHttp(stream);
		await first.close(ctx);
		const results = await Promise.allSettled([0, 1, 2].map(() => openHttp(stream, { mode: "fence" })));
		const opened = results.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []));
		for (const r of results) if (r.status === "rejected") expect(r.reason).toBeInstanceOf(OwnershipContention);
		expect(opened.length).toBeGreaterThan(0);
		const winner = opened.reduce((x, y) => (x.epoch > y.epoch ? x : y));
		for (const s of opened) {
			if (s === winner) await s.commit(conv(1), ctx);
			else await expect(s.commit(conv(2), ctx)).rejects.toBeInstanceOf(FencedError);
		}
		for (const s of opened) await s.close(ctx);
	});
});

describe("commit ambiguity on a real node (§3.3)", () => {
	it("a lost append response resolves by read-back: landed once, no duplicate", async () => {
		const stream = freshStream();
		let drop = false;
		const log = hooked(stream, async (inner, body, match) => {
			if (!drop) return undefined;
			drop = false;
			await inner.append(body, match);
			throw new TransportError("timeout", "e2e: response dropped");
		});
		const s = await openHttp(stream, { log });
		drop = true;
		expect(await s.commit(conv(10), ctx)).toBe(1);
		expect(await s.commit(conv(11), ctx)).toBe(2);
		await s.close(ctx);
		expect((await records(stream)).length).toBe(4);
	});

	it("a request that never arrived is resent after read-back finds the record absent", async () => {
		const stream = freshStream();
		let drop = false;
		const log = hooked(stream, async () => {
			if (!drop) return undefined;
			drop = false;
			throw new TransportError("connection", "e2e: request dropped");
		});
		const s = await openHttp(stream, { log });
		drop = true;
		expect(await s.commit(conv(10), ctx)).toBe(1);
		await s.close(ctx);
		expect((await records(stream)).length).toBe(3);
	});

	it("a duplicated append answers 412 to the second copy and still succeeds", async () => {
		const stream = freshStream();
		let dup = false;
		const log = hooked(stream, async (inner, body, match) => {
			if (!dup) return undefined;
			dup = false;
			await inner.append(body, match);
			return inner.append(body, match);
		});
		const s = await openHttp(stream, { log });
		dup = true;
		expect(await s.commit(conv(10), ctx)).toBe(1);
		await s.close(ctx);
		expect((await records(stream)).length).toBe(3);
	});

	it("rejects invalid JSON with 400 at the HTTP boundary (P1)", async () => {
		const stream = freshStream();
		const s = await openHttp(stream);
		const log = new HttpLogTransport({ baseUrl: baseUrl(), stream });
		const out = await log.append(utf8('{"o":0,"ops":['), s.tail);
		expect(out.status).toBe(400);
		await s.close(ctx);
	});
});
