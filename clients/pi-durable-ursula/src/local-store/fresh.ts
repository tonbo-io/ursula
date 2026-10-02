// Complete-at-mint coverage (design §7.3). A key is fresh-covered when one of its ID-typed
// components (marked † in §4.3) is at least `F_fresh`. Because no committed key had such a component
// at open, and every later write is written through, the cache holds `state(tail)` exactly on the
// fresh set.
//
// `isFreshRange(lo, hi, F)` decides whether EVERY key of `[lo, hi)` is fresh-covered; it is defined so
// that `isFreshKey(k, F) = isFreshRange(k, k‖00, F)` and every key of a fresh range is itself fresh
// (write-through and reads agree on the fresh set). The rule picks an ID component whose offset is
// fixed by `lo`'s leading components; with `P` the octets before it:
// - ascending `u64(id)`: `lo ≥ P‖u64(F)` and `hi ≤ strinc(P)`;
// - descending `u64desc(id)` (entry IDs): `hi ≤ P‖u64(2^64−F)`, i.e. below the first key whose ID is < F.
// Keys of a family that are shorter than its schema (never written by the Pi layer) are classified by
// the same octet comparison, which keeps the two functions consistent.
import { TAG } from "../families.ts";
import { MAX_U64, strinc, u64 } from "../tuple.ts";

/** Component kinds of a family key, after the tag octet. `id`/`iddesc` are the † components. */
type Component = "id" | "iddesc" | "u64" | "u8" | "str";

const SCHEMA: ReadonlyMap<number, readonly Component[]> = new Map<number, readonly Component[]>([
	[TAG.m, ["str"]],
	[TAG.x, ["id"]],
	[TAG.c, ["id"]],
	[TAG.coc, ["id", "id"]],
	[TAG.cot, ["id", "id"]],
	[TAG.e, ["id", "iddesc"]],
	[TAG.eh, ["id", "iddesc"]],
	[TAG.t, ["id"]],
	[TAG.ts, ["u8", "id"]],
	[TAG.tc, ["id", "id"]],
	// IDs are ≤ 2^53, so the octet after the kind's terminator is 00, never an FF escape.
	[TAG.tk, ["str", "id"]],
	[TAG.s, ["id"]],
	[TAG.ss, ["u8", "id"]],
	[TAG.sr, ["id", "str"]],
	[TAG.sc, ["id", "id"]],
	[TAG.d, ["id"]],
	// `d.a` stops at the owner: `str(key)` followed by `u64desc(createdAt)` cannot be split
	// unambiguously (a terminator 00 followed by an FF octet reads as an escaped NUL), so the
	// trailing document ID is never used.
	[TAG.da, ["u8", "id"]],
	[TAG.ds, ["u8", "id", "id"]],
	[TAG.db, ["id", "u64"]],
	[TAG.dr, ["id", "u64"]],
]);

/** Length of the `str()` component starting at `offset`, or undefined when `key` ends inside it. */
function strLength(key: string, offset: number): number | undefined {
	if (offset >= key.length) return undefined;
	if (key.charCodeAt(offset) === 0xfe) return offset + 33 <= key.length ? 33 : undefined;
	for (let i = offset; i < key.length; i++) {
		if (key.charCodeAt(i) !== 0) continue;
		if (i + 1 < key.length && key.charCodeAt(i + 1) === 0xff) {
			i++;
			continue;
		}
		return i + 1 - offset;
	}
	return undefined;
}

/** Offsets (and directions) of the ID components whose offset `lo`'s leading octets determine. */
function idOffsets(lo: string): { offset: number; desc: boolean; afterStr: boolean }[] {
	const out: { offset: number; desc: boolean; afterStr: boolean }[] = [];
	if (lo.length === 0) return out;
	const schema = SCHEMA.get(lo.charCodeAt(0));
	if (schema === undefined) return out;
	let offset = 1;
	let afterStr = false;
	for (const c of schema) {
		if (offset > lo.length) break;
		if (c === "id" || c === "iddesc") out.push({ offset, desc: c === "iddesc", afterStr });
		afterStr = c === "str";
		const len = c === "u8" ? 1 : c === "str" ? strLength(lo, offset) : 8;
		if (len === undefined) break;
		offset += len;
	}
	return out;
}

/** True when every key of `[lo, hi)` is fresh-covered at floor `F` (`hi` undefined: unbounded). */
export function isFreshRange(lo: string, hi: string | undefined, F: number): boolean {
	if (hi === undefined || !(lo < hi) || !Number.isFinite(F)) return false;
	const f = BigInt(Math.max(0, Math.ceil(F)));
	if (f > MAX_U64) return false;
	for (const { offset, desc, afterStr } of idOffsets(lo)) {
		const P = lo.slice(0, offset);
		// After a `str()` the octet at `offset` decides where the string ended (`00 FF` is an
		// escape), so only keys whose next octet is below FF parse with the same prefix P.
		const bound = afterStr ? P + "\u00ff" : strinc(P);
		if (desc) {
			// IDs ≥ F are raw components ≤ 2^64−1−F; the first excluded key is P‖u64(2^64−F).
			const limit = f === 0n ? bound : P + u64(MAX_U64 - f + 1n);
			if (hi <= limit) return true;
		} else if (lo >= P + u64(f) && hi <= bound) {
			return true;
		}
	}
	return false;
}

/** True when `key` is fresh-covered at floor `F`. */
export const isFreshKey = (key: string, F: number): boolean => isFreshRange(key, `${key}\u0000`, F);
