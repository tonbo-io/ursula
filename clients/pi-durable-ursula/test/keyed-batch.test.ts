// keyed-batch-v1 encoding, P1 normalization and P2 grammar (design §4.1, §5.1, §5.2, §11.6).
import { describe, expect, it } from "vitest";
import { encodeRecord, jsonDepth, KeyedBatchError, normalizeJsonMessage, parseKeyedBatch } from "../src/keyed-batch.ts";
import { b64 } from "../src/tuple.ts";

const status = (f: () => unknown): number | undefined => {
	try {
		f();
	} catch (e) {
		if (e instanceof KeyedBatchError) return e.status;
		throw e;
	}
	return undefined;
};
const nest = (d: number): string => "[".repeat(d) + "]".repeat(d);

describe("P1 normalization", () => {
	it("strips insignificant whitespace only", () => {
		expect(normalizeJsonMessage(' { "b" : 1 ,\n\t"a" : [ 1.50e3 , "x y" ] }\r\n')).toBe('{"b":1,"a":[1.50e3,"x y"]}');
	});
	it("keeps member order, duplicates, number text and lone-surrogate escapes", () => {
		const t = '{"2":1,"1":2,"a":1,"a":2,"n":1e400,"s":"\\ud800"}';
		expect(normalizeJsonMessage(t)).toBe(t);
	});
	it("accepts depth 127 and rejects 128", () => {
		expect(normalizeJsonMessage(nest(127))).toBe(nest(127));
		expect(status(() => normalizeJsonMessage(nest(128)))).toBe(400);
		expect(jsonDepth(nest(127))).toBe(127);
		expect(jsonDepth('"[[["')).toBe(0);
	});
	it("rejects invalid JSON with 400", () => {
		for (const bad of ["", "{", "[1,]", "01", "1.", '"\\x"', "tru", '{"a" 1}', "1 2", '"\u0001"']) {
			expect(status(() => normalizeJsonMessage(bad)), bad).toBe(400);
		}
	});
});

describe("P2 grammar", () => {
	const k = b64("k");
	it("parses the three op shapes with raw value text", () => {
		const ops = parseKeyedBatch(`{"o":1,"ops":[["p","${k}",{"z":1,"a":1.50e3}],["d","${k}"],["x","${b64("a")}","${b64("b")}"]],"extra":{"ignored":true}}`);
		expect(ops).toEqual([
			{ op: "p", key: "k", value: '{"z":1,"a":1.50e3}' },
			{ op: "d", key: "k" },
			{ op: "x", start: "a", end: "b" },
		]);
	});
	it("compares member names and op codes after unescaping", () => {
		expect(parseKeyedBatch(`{"\\u006fps":[["\\u0070","${k}",null]]}`)).toEqual([{ op: "p", key: "k", value: "null" }]);
	});
	it("accepts empty ops and a put of null", () => {
		expect(parseKeyedBatch('{"ops":[]}')).toEqual([]);
	});
	it("rejects grammar violations with 422", () => {
		const cases = [
			"[]",
			"{}",
			'{"ops":[],"ops":[]}',
			'{"ops":{}}',
			'{"ops":[1]}',
			'{"ops":[[]]}',
			`{"ops":[["q","${k}"]]}`,
			`{"ops":[["p","${k}"]]}`,
			`{"ops":[["d","${k}",1]]}`,
			'{"ops":[["p","AB",1]]}',
			'{"ops":[["p","AA==",1]]}',
			'{"ops":[["p","+/",1]]}',
			'{"ops":[["p","",1]]}',
			'{"ops":[["p","\\u0041A",1]]}',
			`{"ops":[["x","${b64("b")}","${b64("a")}"]]}`,
			`{"ops":[["x","${b64("a")}","${b64("a")}"]]}`,
			`{"ops":[["p","${b64("x".repeat(4097))}",1]]}`,
			'{"\\ud800":1,"ops":[]}',
		];
		for (const c of cases) expect(status(() => parseKeyedBatch(c)), c).toBe(422);
	});
	it("allows lone surrogates in member names inside values", () => {
		expect(parseKeyedBatch(`{"ops":[["p","${k}",{"\\ud800":1}]]}`)).toHaveLength(1);
	});
	it("prefers 400 for invalid JSON", () => {
		expect(status(() => parseKeyedBatch('{"ops":[}'))).toBe(400);
		expect(status(() => parseKeyedBatch("[1,]"))).toBe(400);
	});
	it("accepts 4096-octet keys", () => {
		expect(parseKeyedBatch(`{"ops":[["p","${b64("x".repeat(4096))}",1]]}`)).toHaveLength(1);
	});
	it("encodeRecord round-trips through parseKeyedBatch", () => {
		const ops = [
			{ op: "p", key: "\u0000ÿ", value: '{"a":[1,2]}' },
			{ op: "d", key: "z" },
			{ op: "x", start: "a", end: "b" },
		] as const;
		const text = encodeRecord(7, ops);
		expect(normalizeJsonMessage(text)).toBe(text);
		expect(parseKeyedBatch(text)).toEqual(ops);
	});
});
