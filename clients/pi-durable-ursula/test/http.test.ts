// HTTP transports (design §3.3, §3.6, §5.3, §5.4) against a local HTTP stub, then UrsulaStorage
// over HTTP against the fake Ursula behind the same stub.
import type { StorageWrite } from "@earendil-works/pi-durable";
import { afterEach, describe, expect, it } from "vitest";
import { FakeUrsula } from "../src/fake/index.ts";
import { HttpKeyedStateTransport, HttpLogTransport, httpTransports, parseKeyedRows, splitRecords, streamUrl } from "../src/http.ts";
import { utf8 } from "../src/keyed-batch.ts";
import { KEYED_CONTENT_TYPE } from "../src/protocol.ts";
import { UrsulaStorage } from "../src/storage.ts";
import { TransportError } from "../src/transport.ts";
import { ctx, FAST } from "./helpers.ts";
import { fakeHandler, type Stub, startStub } from "./http-stub.ts";

const stubs: Stub[] = [];
const stub = async (...args: Parameters<typeof startStub>): Promise<Stub> => {
	const s = await startStub(...args);
	stubs.push(s);
	return s;
};
afterEach(async () => {
	await Promise.all(stubs.splice(0).map((s) => s.close()));
});

const text = (b: Uint8Array): string => new TextDecoder().decode(b);

describe("streamUrl and NDJSON splitting", () => {
	it("percent-encodes each path segment and trims the base's trailing slash", () => {
		expect(streamUrl("http://h:1/", "b/a b%")).toBe("http://h:1/b/a%20b%25");
		expect(() => streamUrl("http://h:1", "only")).toThrow(TypeError);
	});

	it("splits on LF only, keeping each record's exact bytes", () => {
		const raw = utf8('{"a":"é\\u00e9"}\n{"n":1.50e3}\r\n{"x":"\\n"}');
		const parts = splitRecords(raw);
		expect(parts.map(text)).toEqual(['{"a":"é\\u00e9"}', '{"n":1.50e3}\r', '{"x":"\\n"}']);
		expect(parts[0]?.buffer).toBe(raw.buffer);
	});
});

describe("HttpLogTransport", () => {
	it("HEAD, create and append send the protocol headers and return lowercase headers", async () => {
		const s = await stub((r) => {
			if (r.method === "POST") return { status: 204, headers: { "Stream-Record-Start": "7", "Stream-Record-Next": "8" } };
			if (r.method === "PUT") return { status: 201, headers: { "Stream-Extensions": "keyed-batch-v1" } };
			return { status: 200, headers: { "Stream-Record-Next": "7", "Stream-Extensions": "json-record-coordinates-v1, keyed-batch-v1" } };
		});
		const log = new HttpLogTransport({ baseUrl: s.baseUrl, stream: "bkt/h-1", token: "secret" });
		const head = await log.head();
		expect(head.status).toBe(200);
		expect(head.headers["stream-record-next"]).toBe("7");
		expect(head.headers["stream-extensions"]).toBe("json-record-coordinates-v1, keyed-batch-v1");
		expect((await log.create()).status).toBe(201);
		const body = utf8('{"o":0,"ops":[]}');
		const appended = await log.append(body, 7);
		expect(appended).toEqual({ status: 204, headers: expect.objectContaining({ "stream-record-start": "7" }) });
		const [h, put, post] = s.requests;
		expect(h?.method).toBe("HEAD");
		expect(h?.path).toBe("/bkt/h-1");
		expect(h?.headers.authorization).toBe("Bearer secret");
		expect(put?.headers["content-type"]).toBe(KEYED_CONTENT_TYPE);
		expect(put?.body.length).toBe(0);
		expect(post?.headers["content-type"]).toBe(KEYED_CONTENT_TYPE);
		expect(post?.headers["stream-record-match"]).toBe("7");
		expect(text(post?.body ?? new Uint8Array())).toBe('{"o":0,"ops":[]}');
	});

	it("returns error statuses as values with the plain-text message", async () => {
		const s = await stub(() => ({ status: 412, headers: { "Stream-Record-Next": "9" }, body: "record match failed\n" }));
		const log = new HttpLogTransport({ baseUrl: s.baseUrl, stream: "b/s" });
		const out = await log.append(utf8("{}"), 3);
		expect(out.status).toBe(412);
		expect(out.message).toBe("record match failed");
		expect(out.headers["stream-record-next"]).toBe("9");
		expect((await log.readRecords(3)).records).toEqual([]);
	});

	it("readRecords builds the query and splits records", async () => {
		const s = await stub(() => ({
			status: 200,
			headers: { "Stream-Record-Start": "4", "Stream-Record-Next": "6", "Stream-Up-To-Date": "true" },
			body: '{"o":1,"ops":[["p","AQ",{"b":1,"a":2}]]}\n{"o":1,"ops":[]}\n',
		}));
		const log = new HttpLogTransport({ baseUrl: s.baseUrl, stream: "b/s" });
		const out = await log.readRecords(4, { maxRecords: 2, leader: true });
		expect(out.records.map(text)).toEqual(['{"o":1,"ops":[["p","AQ",{"b":1,"a":2}]]}', '{"o":1,"ops":[]}']);
		expect(out.headers["stream-up-to-date"]).toBe("true");
		const q = s.requests[0]?.query;
		expect(Object.fromEntries(q ?? [])).toEqual({ record: "4", max_records: "2", consistency: "leader" });
	});

	it("sends max_bytes only after the node advertised keyed-state-v1, paging by records before", async () => {
		let advertise = false;
		const s = await stub(() => ({
			status: 200,
			headers: {
				"Stream-Record-Start": "0",
				"Stream-Record-Next": "0",
				...(advertise ? { "Stream-Extensions": "keyed-batch-v1, keyed-state-v1" } : {}),
			},
		}));
		const log = new HttpLogTransport({ baseUrl: s.baseUrl, stream: "b/s", fallbackPageRecords: 50 });
		await log.readRecords(0, { maxBytes: 1024 });
		expect(Object.fromEntries(s.requests[0]?.query ?? [])).toEqual({ record: "0", max_records: "50" });
		await log.readRecords(0, { maxBytes: 1024, maxRecords: 1 });
		expect(Object.fromEntries(s.requests[1]?.query ?? [])).toEqual({ record: "0", max_records: "1" });
		advertise = true;
		await log.head();
		await log.readRecords(0, { maxBytes: 1024 });
		expect(Object.fromEntries(s.requests[3]?.query ?? [])).toEqual({ record: "0", max_bytes: "1024" });
	});

	it("long-poll adds live/timeout_ms and extends the client timeout by the wait", async () => {
		const s = await stub(() => ({
			status: 204,
			delayMs: 150,
			headers: { "Stream-Record-Next": "3", "Stream-Up-To-Date": "true" },
		}));
		const log = new HttpLogTransport({ baseUrl: s.baseUrl, stream: "b/s", timeoutMs: 100 });
		const out = await log.readRecords(3, { longPollMs: 200 });
		expect(out.status).toBe(204);
		expect(out.records).toEqual([]);
		expect(Object.fromEntries(s.requests[0]?.query ?? [])).toEqual({ record: "3", live: "long-poll", timeout_ms: "200" });
	});

	it("a record count that disagrees with the coordinates is a protocol error", async () => {
		const s = await stub(() => ({ status: 200, headers: { "Stream-Record-Start": "0", "Stream-Record-Next": "2" }, body: "{}\n" }));
		const log = new HttpLogTransport({ baseUrl: s.baseUrl, stream: "b/s" });
		const error = await log.readRecords(0).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(Error);
		expect(error).not.toBeInstanceOf(TransportError);
	});

	it("maps a missing response to TransportError: timeout and connection", async () => {
		const s = await stub(() => ({ status: 204, delayMs: 300 }));
		const slow = new HttpLogTransport({ baseUrl: s.baseUrl, stream: "b/s", timeoutMs: 50 });
		const timeout = await slow.append(utf8("{}"), 0).catch((e: unknown) => e);
		expect(timeout).toBeInstanceOf(TransportError);
		expect((timeout as TransportError).kind).toBe("timeout");
		const closed = await startStub(() => ({ status: 200 }));
		const url = closed.baseUrl;
		await closed.close();
		const refused = await new HttpLogTransport({ baseUrl: url, stream: "b/s" }).head().catch((e: unknown) => e);
		expect(refused).toBeInstanceOf(TransportError);
		expect((refused as TransportError).kind).toBe("connection");
	});
});

describe("HttpKeyedStateTransport", () => {
	it("encodes every parameter and slices raw value text", async () => {
		const body =
			'{"key":"AW93bmVyAA","record":3,"value":{"z":1,"a":[1.50e3,-0.0]}}\n' +
			'{"key":"AQ","record":12,"value":{"2":"b","1":"a"}}\n' +
			'{"key":"Ag","record":0,"value":"\\ud800 \\u00e9"}\n';
		const s = await stub(() => ({
			status: 200,
			headers: { "Stream-Keyed-Through": "13", "Stream-Keyed-After": "Ag", "Cache-Control": "no-store" },
			body,
		}));
		const ks = new HttpKeyedStateTransport({ baseUrl: s.baseUrl, stream: "b/s", token: "t" });
		const out = await ks.scan({ start: "AQ", end: "Aw", limit: 3, minThroughRecord: 13, timeoutMs: 500 });
		expect(out.status).toBe(200);
		expect(out.through).toBe(13);
		expect(out.after).toBe("Ag");
		expect(out.rows).toEqual([
			{ key: "AW93bmVyAA", record: 3, value: '{"z":1,"a":[1.50e3,-0.0]}' },
			{ key: "AQ", record: 12, value: '{"2":"b","1":"a"}' },
			{ key: "Ag", record: 0, value: '"\\ud800 \\u00e9"' },
		]);
		const r = s.requests[0];
		expect(r?.path).toBe("/b/s/keyed-state");
		expect(r?.headers.authorization).toBe("Bearer t");
		expect(Object.fromEntries(r?.query ?? [])).toEqual({ start: "AQ", end: "Aw", limit: "3", min_through_record: "13", timeout_ms: "500" });
		await ks.scan({ key: "AQ" });
		await ks.scan({ after: "AQ" });
		expect(Object.fromEntries(s.requests[1]?.query ?? [])).toEqual({ key: "AQ" });
		expect(Object.fromEntries(s.requests[2]?.query ?? [])).toEqual({ after: "AQ" });
	});

	it("handles 204 with Stream-Keyed-Through and error statuses", async () => {
		const s = await stub((r) =>
			r.query.has("min_through_record")
				? { status: 204, headers: { "Stream-Keyed-Through": "5" } }
				: { status: 404, body: "not a keyed stream" },
		);
		const ks = new HttpKeyedStateTransport({ baseUrl: s.baseUrl, stream: "b/s" });
		expect(await ks.scan({ minThroughRecord: 9, timeoutMs: 1 })).toEqual({
			status: 204,
			headers: expect.objectContaining({ "stream-keyed-through": "5" }),
			through: 5,
			rows: [],
		});
		const missing = await ks.scan({});
		expect(missing.status).toBe(404);
		expect(missing.message).toBe("not a keyed stream");
		expect(missing.rows).toEqual([]);
	});

	it("rejects malformed rows", () => {
		for (const bad of [
			'{"record":1,"key":"AQ","value":1}\n',
			'{"key":"AQ","record":1,"value":}\n',
			'{"key":"A=","record":1,"value":1}\n',
			'{"key":"AQ","record":01,"value":1}\n',
			'{"key":"AQ","record":1,"value":1\n',
		]) {
			expect(() => parseKeyedRows(utf8(bad))).toThrow(/malformed row/);
		}
		expect(() => parseKeyedRows(new Uint8Array([0xff, 0x0a]))).toThrow(/UTF-8/);
		expect(parseKeyedRows(new Uint8Array())).toEqual([]);
	});
});

describe("UrsulaStorage over HTTP (fake Ursula behind the stub)", () => {
	const conv = (id: number): StorageWrite[] => [{ type: "conversation", value: { id } as never }];

	it("opens, commits, closes, reopens and reads keyed state through the HTTP clients", async () => {
		const fake = new FakeUrsula();
		const s = await stub(fakeHandler(fake));
		const open = () =>
			UrsulaStorage.open({ ...httpTransports({ baseUrl: s.baseUrl, stream: "b/http-1" }), timing: FAST, host: "test", pid: 1 });
		const a = await open();
		expect(await a.commit(conv(10), ctx)).toBe(1);
		expect(await a.commit(conv(11), ctx)).toBe(2);
		await a.close(ctx);
		const b = await open();
		expect(b.epoch).toBe(4);
		expect((await b.scanConversations({}, 10, undefined, ctx)).items).toEqual([{ id: 10 }, { id: 11 }]);
		await b.close(ctx);
		// The keyed-state client against the fake's fold, including truncation.
		const ks = new HttpKeyedStateTransport({ baseUrl: s.baseUrl, stream: "b/http-1" });
		const all = await ks.scan({ minThroughRecord: fake.records("/b/http-1").length });
		expect(all.rows).toEqual(fake.projectionRows("/b/http-1"));
		const page = await ks.scan({ limit: 1 });
		expect(page.rows.length).toBe(1);
		expect(page.after).toBe(page.rows[0]?.key);
		const rest = await ks.scan({ after: page.after as string, limit: 1000 });
		expect([...page.rows, ...rest.rows]).toEqual(all.rows);
	});

	it("an ambiguous append (response timeout) resolves through read-back", async () => {
		const fake = new FakeUrsula();
		const handler = fakeHandler(fake);
		let delayed = 0;
		const s = await stub(async (r) => {
			const out = await handler(r);
			// The first commit's append lands but its response arrives after the client gave up.
			if (r.method === "POST" && r.query.size === 0 && delayed++ === 1) return { ...out, delayMs: 200 };
			return out;
		});
		const storage = await UrsulaStorage.open({
			...httpTransports({ baseUrl: s.baseUrl, stream: "b/http-2", timeoutMs: 50 }),
			timing: FAST,
			host: "test",
			pid: 1,
		});
		expect(await storage.commit(conv(10), ctx)).toBe(1);
		expect(fake.records("/b/http-2").length).toBe(2);
		await storage.close(ctx);
	});
});
