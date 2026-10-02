// The fake Ursula's own semantics: record_match, P1/P2 rejections, P7 reads, and keyed-state (P3)
// folding exactly like the owner's StateStore (applier conformance, I27).
import { describe, expect, it } from "vitest";
import { FakeUrsula } from "../src/fake/index.ts";
import { encodeRecord, fromUtf8, type KeyedOp, parseKeyedBatch, utf8 } from "../src/keyed-batch.ts";
import { H, KEYED_CONTENT_TYPE, LIMITS } from "../src/protocol.ts";
import { FullResidentStateStore } from "../src/state-store.ts";
import { b64 } from "../src/tuple.ts";
import { rng } from "./fuzz-util.ts";

const PATH = "/b/s";
async function created(options = {}): Promise<FakeUrsula> {
	const fake = new FakeUrsula(options);
	expect((await fake.logTransport(PATH).create()).status).toBe(201);
	return fake;
}

describe("log", () => {
	it("enforces Stream-Record-Match and reports the tail on 412", async () => {
		const fake = await created();
		const log = fake.logTransport(PATH);
		const a = await log.append(utf8('{"ops":[]}'), 0);
		expect(a.status).toBe(204);
		expect(a.headers[H.recordStart]).toBe("0");
		expect(a.headers[H.recordNext]).toBe("1");
		const b = await log.append(utf8('{"ops":[]}'), 0);
		expect(b.status).toBe(412);
		expect(b.headers[H.recordNext]).toBe("1");
	});
	it("answers 400 / 413 / 422 / 404 without committing", async () => {
		const fake = await created();
		const log = fake.logTransport(PATH);
		expect((await log.append(utf8("{"), 0)).status).toBe(400);
		expect((await log.append(new Uint8Array([0xff]), 0)).status).toBe(400);
		expect((await log.append(utf8('{"ops":[["p","AB",1]]}'), 0)).status).toBe(422);
		expect((await log.append(new Uint8Array(LIMITS.maxRecordBytes + 1), 0)).status).toBe(413);
		expect((await fake.logTransport("/b/none").append(utf8('{"ops":[]}'), 0)).status).toBe(404);
		expect(fake.records(PATH)).toEqual([]);
	});
	it("stores P1-normalized text", async () => {
		const fake = await created();
		await fake.logTransport(PATH).append(utf8(' {"ops" : [ ] , "b":1.50e3, "a":2 }'), 0);
		expect(fake.records(PATH)).toEqual(['{"ops":[],"b":1.50e3,"a":2}']);
	});
	it("P7 max_bytes returns complete records, at least one", async () => {
		const fake = await created();
		const log = fake.logTransport(PATH);
		for (let i = 0; i < 5; i++) await log.append(utf8(`{"ops":[],"i":${i}}`), i);
		const one = await log.readRecords(0, { maxBytes: 1 });
		expect(one.records.length).toBe(1);
		const two = await log.readRecords(1, { maxBytes: 2 * ('{"ops":[],"i":0}'.length + 1) });
		expect(two.records.map(fromUtf8)).toEqual(['{"ops":[],"i":1}', '{"ops":[],"i":2}']);
		expect(two.headers[H.recordNext]).toBe("3");
		expect(two.headers[H.upToDate]).toBeUndefined();
		const rest = await log.readRecords(3, { maxRecords: 10 });
		expect(rest.records.length).toBe(2);
		expect(rest.headers[H.upToDate]).toBe("true");
		expect((await log.readRecords(6)).status).toBe(400);
	});
	it("long-poll wakes on append and times out with 204", async () => {
		const fake = await created();
		const log = fake.logTransport(PATH);
		const idle = await log.readRecords(0, { longPollMs: 20 });
		expect(idle.status).toBe(204);
		const waiting = log.readRecords(0, { longPollMs: 5000 });
		await log.append(utf8('{"ops":[]}'), 0);
		expect((await waiting).records.length).toBe(1);
	});
	it("create is idempotent and refuses another content type", async () => {
		const fake = await created();
		expect((await fake.logTransport(PATH).create()).status).toBe(200);
		fake.createStream("/b/plain", "application/json");
		expect((await fake.logTransport("/b/plain").create()).status).toBe(409);
		expect(KEYED_CONTENT_TYPE).toBe("application/json; profile=keyed-batch-v1");
	});
});

describe("keyed-state", () => {
	it("folds exactly like the owner's StateStore at every D (I27)", async () => {
		for (let seed = 1; seed <= 20; seed++) {
			const { rnd, pick } = rng(seed);
			const keys = ["a", "a\u0000", "ab", "b", "bÿ", "c", "\u0000", "ÿÿ"];
			const fake = await created();
			const log = fake.logTransport(PATH);
			const store = new FullResidentStateStore();
			for (let n = 0; n < 30; n++) {
				const ops: KeyedOp[] = [];
				for (let j = Math.floor(rnd() * 5); j > 0; j--) {
					const r = rnd();
					if (r < 0.6) ops.push({ op: "p", key: pick(keys), value: JSON.stringify({ n, j, v: rnd() < 0.2 ? null : [1, "x"] }) });
					else if (r < 0.8) ops.push({ op: "d", key: pick(keys) });
					else {
						const [a, b] = [pick(keys), pick(keys)].sort();
						if ((a as string) < (b as string)) ops.push({ op: "x", start: a as string, end: b as string });
					}
				}
				const text = encodeRecord(0, ops);
				expect((await log.append(utf8(text), n)).status).toBe(204);
				store.apply(n, parseKeyedBatch(text));
				fake.publish(PATH, n + 1);
				const expected = store.rows().map(([k, row]) => ({ key: b64(k), record: row.record, value: row.value }));
				expect(fake.projectionRows(PATH)).toEqual(expected);
				// Paginated scans reproduce the same rows.
				const ks = fake.keyedStateTransport(PATH);
				const got: unknown[] = [];
				let after: string | undefined;
				for (;;) {
					const page = await ks.scan({ ...(after === undefined ? {} : { after }), limit: 2, minThroughRecord: n + 1 });
					expect(page.status).toBe(200);
					expect(page.through).toBe(n + 1);
					got.push(...page.rows);
					if (page.after === undefined) break;
					after = page.after;
				}
				expect(got).toEqual(expected);
			}
		}
	});

	it("serves point reads, ranges, 204 on timeout, and 400 beyond the tail", async () => {
		let paused = false;
		const fake = await created({ publishTarget: (tail: number) => (paused ? 0 : tail) });
		const log = fake.logTransport(PATH);
		const ks = fake.keyedStateTransport(PATH);
		await log.append(utf8(encodeRecord(0, [{ op: "p", key: "k", value: "1" }, { op: "p", key: "m", value: "2" }])), 0);
		expect((await ks.scan({ key: b64("k") })).rows).toEqual([{ key: b64("k"), record: 0, value: "1" }]);
		expect((await ks.scan({ start: b64("l"), end: b64("n") })).rows.map((r) => r.value)).toEqual(["2"]);
		expect((await ks.scan({ start: b64("n"), end: b64("l") })).rows).toEqual([]);
		expect((await ks.scan({ key: b64("k"), limit: 1 })).status).toBe(400);
		expect((await ks.scan({ start: "AB" })).status).toBe(400);
		const beyond = await ks.scan({ minThroughRecord: 5 });
		expect(beyond.status).toBe(400);
		expect(beyond.headers[H.recordNext]).toBe("1");
		paused = true;
		await log.append(utf8(encodeRecord(0, [{ op: "d", key: "k" }])), 1);
		const lag = await ks.scan({ minThroughRecord: 2, timeoutMs: 10 });
		expect(lag.status).toBe(204);
		expect(lag.through).toBe(1);
		fake.createStream("/b/plain", "application/json");
		expect((await fake.keyedStateTransport("/b/plain").scan({})).status).toBe(404);
	});
});
