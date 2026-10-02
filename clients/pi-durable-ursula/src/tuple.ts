// Tuple layer (design §4.2). Keys are "binary strings": JavaScript strings whose code units are all
// in 0..255, one code unit per octet. Comparing such strings with `<` equals unsigned octet order,
// which is the order the server folds and scans in.
import { createHash } from "node:crypto";

const ch = (n: number): string => String.fromCharCode(n);

export const MAX_U64 = (1n << 64n) - 1n;

/** Length in octets above which `str()` switches to the 33-octet digest form. */
export const STR_SHORT_MAX = 1024;

/** 8 octets, big-endian, ascending. */
export function u64(x: number | bigint): string {
	const v = BigInt(x);
	if (v < 0n || v > MAX_U64) throw new RangeError(`u64 out of range: ${x}`);
	let s = "";
	for (let i = 7; i >= 0; i--) s += ch(Number((v >> BigInt(i * 8)) & 0xffn));
	return s;
}

/** 8 octets, big-endian, of 2^64−1−x: descending order. */
export const u64desc = (x: number | bigint): string => u64(MAX_U64 - BigInt(x));

/** Read the big-endian u64 at `offset` of a binary string. */
export function readU64(s: string, offset: number): bigint {
	if (offset < 0 || offset + 8 > s.length) throw new RangeError("readU64 out of bounds");
	let v = 0n;
	for (let i = 0; i < 8; i++) v = (v << 8n) | BigInt(s.charCodeAt(offset + i));
	return v;
}

/** Read a u64desc component back as its ascending value. */
export const readU64Desc = (s: string, offset: number): bigint => MAX_U64 - readU64(s, offset);

/** One octet enum. */
export function u8(n: number): string {
	if (!Number.isInteger(n) || n < 0 || n > 255) throw new RangeError(`u8 out of range: ${n}`);
	return ch(n);
}

/**
 * WTF-8 of a JavaScript string: valid surrogate pairs become 4-octet UTF-8 and lone surrogates
 * become `ED A0..BF xx`. `TextEncoder` must not be used: it replaces lone surrogates with U+FFFD.
 */
export function wtf8(text: string): number[] {
	const out: number[] = [];
	for (let i = 0; i < text.length; i++) {
		let cp = text.charCodeAt(i);
		if (cp >= 0xd800 && cp <= 0xdbff && i + 1 < text.length) {
			const lo = text.charCodeAt(i + 1);
			if (lo >= 0xdc00 && lo <= 0xdfff) {
				cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
				i++;
			}
		}
		if (cp < 0x80) out.push(cp);
		else if (cp < 0x800) out.push(0xc0 | (cp >> 6), 0x80 | (cp & 0x3f));
		else if (cp < 0x10000) out.push(0xe0 | (cp >> 12), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
		else out.push(0xf0 | (cp >> 18), 0x80 | ((cp >> 12) & 0x3f), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
	}
	return out;
}

/**
 * `str(s)`: the short form is the escaped WTF-8 (each 0x00 becomes `00 FF`) followed by a 0x00
 * terminator, used when that is at most 1024 octets. Otherwise the long form `FE ‖ SHA-256(WTF-8(s))`
 * (33 octets), which only supports exact matches. A short form never starts with 0xFE.
 */
export function str(text: string): string {
	const bytes = wtf8(text);
	let s = "";
	for (const b of bytes) {
		s += ch(b);
		if (b === 0) s += ch(0xff);
	}
	s += ch(0);
	if (s.length <= STR_SHORT_MAX) return s;
	return ch(0xfe) + createHash("sha256").update(Uint8Array.from(bytes)).digest().toString("latin1");
}

/** True when `str(text)` uses the digest form, so the full string must be verified elsewhere. */
export const isLongStr = (text: string): boolean => str(text).charCodeAt(0) === 0xfe;

/** Prefix successor: strip trailing 0xFF octets, then increment the last octet. */
export function strinc(prefix: string): string {
	let q = prefix;
	while (q.length > 0 && q.charCodeAt(q.length - 1) === 0xff) q = q.slice(0, -1);
	if (q.length === 0) throw new RangeError("strinc of an empty or all-0xFF prefix");
	return q.slice(0, -1) + ch(q.charCodeAt(q.length - 1) + 1);
}

/** The binary string immediately after `k` in octet order (`k ‖ 00`). */
export const keySuccessor = (k: string): string => k + ch(0);

/** Canonical unpadded base64url (RFC 4648 §5) of a binary string. */
export const b64 = (key: string): string => Buffer.from(key, "latin1").toString("base64url");

const B64URL = /^[A-Za-z0-9_-]+$/;

/**
 * Decode canonical unpadded base64url to a binary string, or return undefined when the text is
 * not canonical (padding, non-zero pad bits, foreign characters, empty, or an impossible length).
 */
export function unb64(text: string): string | undefined {
	if (text.length === 0 || !B64URL.test(text) || text.length % 4 === 1) return undefined;
	const raw = Buffer.from(text, "base64url");
	if (raw.toString("base64url") !== text) return undefined;
	return raw.toString("latin1");
}

/** Number of octets of a binary string key. */
export const keyOctets = (key: string): number => key.length;

/** Binary string ↔ bytes. */
export const binaryToBytes = (s: string): Uint8Array => Uint8Array.from(Buffer.from(s, "latin1"));
export const bytesToBinary = (b: Uint8Array): string => Buffer.from(b).toString("latin1");
