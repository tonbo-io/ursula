// Tuple layer and key-family vectors (design §4.2, §4.3, §4.6): byte-identical to the model.
import type { DocumentRecord, StorageWrite } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { K } from "../src/families.ts";
import { type OwnerClaim, planClaim, planCloseMarker, planCommit } from "../src/planner.ts";
import { FullResidentStateStore } from "../src/state-store.ts";
import { b64, isLongStr, str, strinc, u64, u64desc, unb64, wtf8 } from "../src/tuple.ts";
import { KEYS, RECORDS } from "./vectors.ts";

const BUILD: Record<string, () => string> = {
	"e/1/~127": () => K.e(1, 127),
	"x/127": () => K.x(127),
	"s/128": () => K.s(128),
	"s.s/placed/128": () => K.ss(2, 128),
	"x/128": () => K.x(128),
	"s.c/1/128": () => K.sc(1, 128),
	's.r/1/"req-1"': () => K.sr(1, "req-1"),
	"t/129": () => K.t(129),
	"t.s/pending/129": () => K.ts(1, 129),
	"x/129": () => K.x(129),
	"t.c/1/129": () => K.tc(1, 129),
	't.k/"pi.generation"/129': () => K.tk("pi.generation", 129),
	"d.b/4/~103": () => K.db(4, 103),
	"d.b/4/~102": () => K.db(4, 102),
	"strinc(d.b/4/)": () => strinc(K.db(4)),
	"d.r/4/0": () => K.dr(4, 0),
	"d.r/4/103": () => K.dr(4, 103),
	"d.r/4/104": () => K.dr(4, 104),
	"m/next_id": () => K.m("next_id"),
	"m/format": () => K.m("format"),
	"m/owner": () => K.m("owner"),
	't.k/"\\ud800"/5': () => K.tk("\ud800", 5),
	't.k/"\\ud801"/5': () => K.tk("\ud801", 5),
	's.r/1/<2000 x "R">': () => K.sr(1, "R".repeat(2000)),
	'd.a/session/0/"pi.agent"/single/~2/3': () =>
		K.da({ id: 3, kind: "pi.agent", scope: { kind: "session" }, createdAt: 2 } as unknown as DocumentRecord),
};

describe("key vectors", () => {
	for (const [label, expected] of KEYS) {
		it(label, () => {
			const build = BUILD[label];
			expect(build, `no builder for ${label}`).toBeDefined();
			expect(b64((build as () => string)())).toBe(expected);
		});
	}
});

describe("tuple primitives", () => {
	it("u64 / u64desc order", () => {
		expect(u64(1) < u64(2)).toBe(true);
		expect(u64desc(1) > u64desc(2)).toBe(true);
		expect(u64(Number.MAX_SAFE_INTEGER + 1)).toBe(u64(2n ** 53n));
		expect(() => u64(-1)).toThrow(RangeError);
		expect(() => u64(2n ** 64n)).toThrow(RangeError);
	});
	it("WTF-8 keeps lone surrogates distinct and encodes pairs as UTF-8", () => {
		expect(wtf8("\ud800")).toEqual([0xed, 0xa0, 0x80]);
		expect(wtf8("\ud801")).toEqual([0xed, 0xa0, 0x81]);
		expect(wtf8("😀")).toEqual([...Buffer.from("😀", "utf8")]);
		expect(str("\ud800")).not.toBe(str("\ufffd"));
	});
	it("escapes NUL as 00 FF and terminates with 00", () => {
		expect(str("k")).toBe("k\u0000");
		expect(str("k\u0000z")).toBe("k\u0000\u00ffz\u0000");
		expect(str("")).toBe("\u0000");
		// NUL-extension: str("k") is a prefix of str("k\0z") (the documented non-prefix-freeness).
		expect(str("k\u0000z").startsWith(str("k"))).toBe(true);
	});
	it("switches to FE ‖ SHA-256 above 1024 octets", () => {
		expect(isLongStr("a".repeat(1023))).toBe(false);
		expect(str("a".repeat(1023)).length).toBe(1024);
		expect(isLongStr("a".repeat(1024))).toBe(true);
		expect(str("a".repeat(1024)).length).toBe(33);
		expect(str("a".repeat(1024)).charCodeAt(0)).toBe(0xfe);
		// 512 NULs escape to 1024 octets + terminator: long form.
		expect(isLongStr("\u0000".repeat(512))).toBe(true);
	});
	it("strinc strips trailing FF", () => {
		expect(strinc("a\u00ff\u00ff")).toBe("b");
		expect(() => strinc("\u00ff")).toThrow(RangeError);
	});
	it("canonical base64url only", () => {
		expect(unb64("AA")).toBe("\u0000");
		expect(unb64("AB")).toBeUndefined(); // non-zero pad bits
		expect(unb64("AA==")).toBeUndefined();
		expect(unb64("+/")).toBeUndefined();
		expect(unb64("")).toBeUndefined();
		expect(unb64("A")).toBeUndefined();
		expect(b64("\u00ff\u00fe")).toBe("__4");
	});
});

describe("record vectors (§4.6)", () => {
	const owner: OwnerClaim = {
		epoch: 42,
		nonce: "c0ffee00112233445566778899aabbcc",
		host: "worker-9",
		pid: 812,
		opened_at_ms: 1790000360000,
		mode: "fail-if-active",
	};
	it("genesis", () => {
		const genesis = planClaim(0, { epoch: 0, nonce: "9f2c4e1a7b3d5f60a1b2c3d4e5f60718", host: "worker-7", pid: 4711, opened_at_ms: 1790000000000, mode: "fence" });
		expect(genesis.text).toBe(RECORDS.GENESIS);
	});
	it("takeover claim and close marker", () => {
		expect(planClaim(42, owner).text).toBe(RECORDS.CLAIM);
		expect(planCloseMarker(42, owner, 1790000720000).text).toBe(RECORDS.CLOSE);
	});

	const docStore = (): FullResidentStateStore => {
		const store = new FullResidentStateStore();
		const record = { id: 4, kind: "pi.live", scope: { kind: "session" }, createdAt: 5 };
		store.apply(0, [
			{ op: "p", key: K.d(4), value: JSON.stringify({ record, version: 1 }) },
			{ op: "p", key: K.x(4), value: '{"t":"d"}' },
		]);
		return store;
	};

	it("admission commit at N = 103, epoch 17 is byte-identical", () => {
		const writes: StorageWrite[] = [
			{ type: "entry", value: { id: 127, conversationId: 1, kind: "pi.user", model: [{ role: "user", content: "hi" }] } as never },
			{ type: "submission", value: { id: 128, conversationId: 1, requestId: "req-1", type: "input", status: "placed", entry: 127 } as never },
			{
				type: "task",
				value: { id: 129, conversationId: 1, kind: "pi.generation", version: 1, input: {}, background: false, abortRequested: false, state: { status: "pending", checkpoint: {} } } as never,
			},
			{ type: "document.change", id: 4 as never, content: { kind: "base", version: 1, value: { generation: null } } },
		];
		const plan = docStore().readSync((view) => planCommit(view, writes, { seq: 103, epoch: 17, nextId: 130, persistedNextId: 127 }));
		expect(plan.text).toBe(RECORDS.ADMISSION);
		expect(plan.bytes.length).toBe(1222);
		expect(plan.persistedNextId).toBe(130);
	});

	it("streaming partial commit is one op", () => {
		const writes: StorageWrite[] = [
			{ type: "document.change", id: 4 as never, content: { kind: "delta", version: 1, ops: [["a", ["live", "generation", "message", "text"], "hello wor"]] as never } },
		];
		const plan = docStore().readSync((view) => planCommit(view, writes, { seq: 104, epoch: 17, nextId: 130, persistedNextId: 130 }));
		expect(plan.text).toBe(RECORDS.PARTIAL);
	});
});
