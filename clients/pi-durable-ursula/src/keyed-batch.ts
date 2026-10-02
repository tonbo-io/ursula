// keyed-batch-v1 records (design §4.1, P1 §5.1, P2 §5.2): encoding, JSON text validation and
// minification, grammar validation, and op extraction with values kept as their exact stored text.
//
// The same code serves three roles: the owner's encoder, the owner's replay parser, and the fake
// server's validator. Values are never round-tripped through JSON.parse/JSON.stringify, so the
// owner sees exactly the bytes the server stores (I7, I22).
import { LIMITS } from "./protocol.ts";
import { b64, unb64 } from "./tuple.ts";

/** One fold op. Keys are binary strings; `value` is JSON text. */
export type KeyedOp =
	| { readonly op: "p"; readonly key: string; readonly value: string }
	| { readonly op: "d"; readonly key: string }
	| { readonly op: "x"; readonly start: string; readonly end: string };

/** A malformed record. `status` is the HTTP status a server answers with: 400 for JSON, 422 for grammar. */
export class KeyedBatchError extends Error {
	readonly status: 400 | 413 | 422;
	constructor(status: 400 | 413 | 422, message: string) {
		super(message);
		this.name = "KeyedBatchError";
		this.status = status;
	}
}

/** Encode one record: `{"o":epoch,"ops":[…]}`. The output has no insignificant whitespace. */
export function encodeRecord(epoch: number, ops: readonly KeyedOp[]): string {
	const parts = ops.map((op) => {
		switch (op.op) {
			case "p":
				return `["p","${b64(op.key)}",${op.value}]`;
			case "d":
				return `["d","${b64(op.key)}"]`;
			case "x":
				return `["x","${b64(op.start)}","${b64(op.end)}"]`;
		}
	});
	return `{"o":${epoch},"ops":[${parts.join(",")}]}`;
}

/** Maximum nesting depth of valid JSON text (scalar 0, empty container 1). */
export function jsonDepth(text: string): number {
	let depth = 0;
	let max = 0;
	let inString = false;
	for (let i = 0; i < text.length; i++) {
		const c = text.charCodeAt(i);
		if (inString) {
			if (c === 0x5c) i++;
			else if (c === 0x22) inString = false;
			continue;
		}
		if (c === 0x22) inString = true;
		else if (c === 0x7b || c === 0x5b) {
			depth++;
			if (depth > max) max = depth;
		} else if (c === 0x7d || c === 0x5d) depth--;
	}
	return max;
}

const isWs = (c: number): boolean => c === 0x20 || c === 0x09 || c === 0x0a || c === 0x0d;
const isDigit = (c: number): boolean => c >= 0x30 && c <= 0x39;
const isHex = (c: number): boolean => isDigit(c) || (c >= 0x41 && c <= 0x46) || (c >= 0x61 && c <= 0x66);

/** Validating RFC 8259 scanner. Lone-surrogate escapes are valid (P1.1). Depth is capped at 127. */
class Scanner {
	readonly s: string;
	constructor(s: string) {
		this.s = s;
	}
	fail(i: number, what: string): never {
		throw new KeyedBatchError(400, `invalid JSON at character ${i}: ${what}`);
	}
	ws(i: number): number {
		while (i < this.s.length && isWs(this.s.charCodeAt(i))) i++;
		return i;
	}
	/** Scan one value starting at `i` (no leading whitespace); return the index after it. */
	value(i: number, level: number): number {
		const c = this.s.charCodeAt(i);
		if (c === 0x22) return this.string(i);
		if (c === 0x7b || c === 0x5b) {
			if (level + 1 > LIMITS.maxJsonDepth) {
				throw new KeyedBatchError(400, `JSON nesting depth exceeds ${LIMITS.maxJsonDepth}`);
			}
			return c === 0x7b ? this.object(i, level + 1) : this.array(i, level + 1);
		}
		if (c === 0x2d || isDigit(c)) return this.number(i);
		for (const lit of ["true", "false", "null"]) if (this.s.startsWith(lit, i)) return i + lit.length;
		return this.fail(i, "unexpected character");
	}
	string(i: number): number {
		i++;
		for (;;) {
			if (i >= this.s.length) this.fail(i, "unterminated string");
			const c = this.s.charCodeAt(i);
			if (c === 0x22) return i + 1;
			if (c < 0x20) this.fail(i, "control character in string");
			if (c === 0x5c) {
				const e = this.s.charCodeAt(i + 1);
				if (e === 0x75) {
					for (let k = 2; k < 6; k++) if (!isHex(this.s.charCodeAt(i + k))) this.fail(i, "bad \\u escape");
					i += 6;
				} else if ([0x22, 0x5c, 0x2f, 0x62, 0x66, 0x6e, 0x72, 0x74].includes(e)) i += 2;
				else this.fail(i, "bad escape");
			} else i++;
		}
	}
	number(i: number): number {
		const start = i;
		if (this.s.charCodeAt(i) === 0x2d) i++;
		if (this.s.charCodeAt(i) === 0x30) i++;
		else if (isDigit(this.s.charCodeAt(i))) while (isDigit(this.s.charCodeAt(i))) i++;
		else this.fail(start, "bad number");
		if (this.s.charCodeAt(i) === 0x2e) {
			i++;
			if (!isDigit(this.s.charCodeAt(i))) this.fail(start, "bad fraction");
			while (isDigit(this.s.charCodeAt(i))) i++;
		}
		const e = this.s.charCodeAt(i);
		if (e === 0x65 || e === 0x45) {
			i++;
			const sign = this.s.charCodeAt(i);
			if (sign === 0x2b || sign === 0x2d) i++;
			if (!isDigit(this.s.charCodeAt(i))) this.fail(start, "bad exponent");
			while (isDigit(this.s.charCodeAt(i))) i++;
		}
		return i;
	}
	array(i: number, level: number): number {
		i = this.ws(i + 1);
		if (this.s.charCodeAt(i) === 0x5d) return i + 1;
		for (;;) {
			i = this.ws(this.value(i, level));
			const c = this.s.charCodeAt(i);
			if (c === 0x5d) return i + 1;
			if (c !== 0x2c) this.fail(i, "expected , or ]");
			i = this.ws(i + 1);
		}
	}
	object(i: number, level: number): number {
		i = this.ws(i + 1);
		if (this.s.charCodeAt(i) === 0x7d) return i + 1;
		for (;;) {
			if (this.s.charCodeAt(i) !== 0x22) this.fail(i, "expected member name");
			i = this.ws(this.string(i));
			if (this.s.charCodeAt(i) !== 0x3a) this.fail(i, "expected :");
			i = this.ws(this.value(this.ws(i + 1), level));
			const c = this.s.charCodeAt(i);
			if (c === 0x7d) return i + 1;
			if (c !== 0x2c) this.fail(i, "expected , or }");
			i = this.ws(i + 1);
		}
	}
}

/** Validate one JSON message text (P1.1, P1.3) and remove insignificant whitespace (P1.2). */
export function normalizeJsonMessage(text: string): string {
	const sc = new Scanner(text);
	const start = sc.ws(0);
	if (start >= text.length) sc.fail(start, "empty body");
	const end = sc.ws(sc.value(start, 0));
	if (end !== text.length) sc.fail(end, "trailing characters");
	let out = "";
	let inString = false;
	let runStart = 0;
	for (let i = 0; i < text.length; i++) {
		const c = text.charCodeAt(i);
		if (inString) {
			if (c === 0x5c) i++;
			else if (c === 0x22) inString = false;
			continue;
		}
		if (c === 0x22) inString = true;
		else if (isWs(c)) {
			out += text.slice(runStart, i);
			runStart = i + 1;
		}
	}
	return out + text.slice(runStart);
}

const grammar = (reason: string): never => {
	throw new KeyedBatchError(422, `invalid keyed batch at message 0: ${reason}`);
};

function readKey(s: string, i: number, end: number): string {
	// Keys are checked on raw characters: escapes are invalid (P2.4).
	const raw = s.slice(i + 1, end - 1);
	if (s.charCodeAt(i) !== 0x22 || raw.includes("\\")) return grammar("key is not a canonical base64url string");
	const key = unb64(raw);
	if (key === undefined) return grammar("key is not canonical unpadded base64url");
	if (key.length < 1 || key.length > LIMITS.maxKeyOctets) return grammar("key length out of range");
	return key;
}

/**
 * Parse a stored (normalized) keyed-batch-v1 message into ops. Throws `KeyedBatchError` with status
 * 400 for invalid JSON and 422 for grammar violations. Values are slices of `text`.
 */
export function parseKeyedBatch(text: string): KeyedOp[] {
	const sc = new Scanner(text);
	let i = sc.ws(0);
	if (text.charCodeAt(i) !== 0x7b) {
		// Validate JSON first so that JSON errors take precedence over grammar errors.
		const end = sc.ws(sc.value(i, 0));
		if (end !== text.length) sc.fail(end, "trailing characters");
		return grammar("message is not an object");
	}
	let opsSpan: [number, number] | undefined;
	let opsCount = 0;
	i = sc.ws(i + 1);
	if (text.charCodeAt(i) !== 0x7d) {
		for (;;) {
			if (text.charCodeAt(i) !== 0x22) sc.fail(i, "expected member name");
			const nameEnd = sc.string(i);
			const name = JSON.parse(text.slice(i, nameEnd)) as string;
			i = sc.ws(nameEnd);
			if (text.charCodeAt(i) !== 0x3a) sc.fail(i, "expected :");
			const vStart = sc.ws(i + 1);
			const vEnd = sc.value(vStart, 1);
			if (!name.isWellFormed()) grammar("member name contains an unpaired surrogate");
			if (name === "ops") {
				opsCount++;
				opsSpan = [vStart, vEnd];
			}
			i = sc.ws(vEnd);
			const c = text.charCodeAt(i);
			if (c === 0x7d) break;
			if (c !== 0x2c) sc.fail(i, "expected , or }");
			i = sc.ws(i + 1);
		}
	}
	const end = sc.ws(i + 1);
	if (end !== text.length) sc.fail(end, "trailing characters");
	if (opsCount !== 1 || opsSpan === undefined) return grammar("expected exactly one ops member");
	return parseOps(sc, opsSpan[0]);
}

function parseOps(sc: Scanner, start: number): KeyedOp[] {
	const s = sc.s;
	if (s.charCodeAt(start) !== 0x5b) return grammar("ops is not an array");
	const ops: KeyedOp[] = [];
	let i = sc.ws(start + 1);
	if (s.charCodeAt(i) === 0x5d) return ops;
	for (;;) {
		if (s.charCodeAt(i) !== 0x5b) grammar(`op ${ops.length} is not an array`);
		// Collect element spans of this op.
		const spans: [number, number][] = [];
		let j = sc.ws(i + 1);
		if (s.charCodeAt(j) !== 0x5d) {
			for (;;) {
				const e = sc.value(j, 3);
				spans.push([j, e]);
				j = sc.ws(e);
				const c = s.charCodeAt(j);
				if (c === 0x5d) break;
				if (c !== 0x2c) sc.fail(j, "expected , or ]");
				j = sc.ws(j + 1);
			}
		}
		const first = spans[0];
		if (first === undefined || s.charCodeAt(first[0]) !== 0x22) grammar(`op ${ops.length} has no op code`);
		const code = JSON.parse(s.slice(first![0], first![1])) as string;
		const span = (k: number): [number, number] => spans[k] as [number, number];
		if (code === "p") {
			if (spans.length !== 3) grammar(`put op ${ops.length} must have 3 elements`);
			const key = readKey(s, ...span(1));
			ops.push({ op: "p", key, value: s.slice(...span(2)) });
		} else if (code === "d") {
			if (spans.length !== 2) grammar(`delete op ${ops.length} must have 2 elements`);
			ops.push({ op: "d", key: readKey(s, ...span(1)) });
		} else if (code === "x") {
			if (spans.length !== 3) grammar(`range delete op ${ops.length} must have 3 elements`);
			const a = readKey(s, ...span(1));
			const b = readKey(s, ...span(2));
			if (!(a < b)) grammar(`range delete op ${ops.length} has start >= end`);
			ops.push({ op: "x", start: a, end: b });
		} else grammar(`op ${ops.length} has unknown op code`);
		i = sc.ws(j + 1);
		const c = s.charCodeAt(i);
		if (c === 0x5d) return ops;
		if (c !== 0x2c) sc.fail(i, "expected , or ]");
		i = sc.ws(i + 1);
	}
}

/** Read the `"o"` member of a record this package encoded (`{"o":N,…`), or undefined. */
export function recordEpoch(text: string): number | undefined {
	const m = /^\{"o":(\d+),/.exec(text);
	return m === null ? undefined : Number(m[1]);
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
export const utf8 = (text: string): Uint8Array => encoder.encode(text);
/** Decode UTF-8, throwing `KeyedBatchError(400)` on invalid input. */
export function fromUtf8(bytes: Uint8Array): string {
	try {
		return decoder.decode(bytes);
	} catch {
		throw new KeyedBatchError(400, "invalid UTF-8");
	}
}
export function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
	return true;
}
