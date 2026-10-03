// Snapshots and retention. A long run writes far more log than the per-group hot limit admits
// (~68 MB of frames trip it without a cold tier: run this file with URSULA_COLD=none to pin that),
// while the owner snapshots and trims, so the retained stream stays bounded. Then a fresh host and a
// host left below the retention both rebuild from snapshot + tail, byte-identical to the owner's
// file, and the takeover after the trim (the claims before the snapshot are gone) still fences the
// old owner.
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { attach, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { dump, integrity, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

const MiB = 1 << 20;
const ROUNDS = 600;

async function head(url: string): Promise<{ tail: number; retained: number; snapshot: number }> {
	const r = await fetch(url, { method: "HEAD" });
	expect(r.status).toBe(200);
	const n = (h: string): number => Number(r.headers.get(h) ?? "0");
	return { tail: n("stream-next-offset"), retained: n("stream-retained-offset"), snapshot: n("stream-snapshot-offset") };
}

/** Checkpoints `file` into its db file (plain connection) and returns its bytes and dump. */
function settle(file: string): { bytes: Buffer; dump: Record<string, string[]> } {
	const db = openPlain(file);
	try {
		expect(integrity(db)).toBe("ok");
		db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
		return { bytes: readFileSync(file), dump: dump(db) };
	} finally {
		db.close();
	}
}

it("snapshots + retention bound the stream; fresh and lagging hosts rebuild from snapshot + tail; the takeover fences", async () => {
	const url = ursulaUrl() + streamPath();
	// A host attached at the start and then left behind, far below the eventual retention.
	const lagging = freshFile();
	attach(lagging, url);
	const l = openPlain(lagging);
	l.exec("CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB)");
	l.close();
	const laggingOffset = status(lagging).offset;

	// The owner: 600 commits of four incompressible 64 KiB rows over 48 keys, ~160 MB of log onto
	// a ~3.5 MB database; then it idles while another host takes over, and tries one more commit.
	const steps: string[] = [];
	for (let i = 0; i < ROUNDS; i++) {
		const rows = [0, 1, 2, 3].map((j) => `(${(i * 4 + j) % 48}, randomblob(65536))`);
		steps.push(`INSERT OR REPLACE INTO t VALUES ${rows.join(", ")}`);
	}
	steps.push("@sleep:30000", "INSERT INTO t VALUES (1000, 'zombie')");
	const file = freshFile();
	const owner = runChild(file, url, steps, { CHILD_EXIT: "1" });
	await owner.waitFor((x) => x.step === ROUNDS && x.phase === "start", 600_000);
	expect(owner.lines.filter((x) => x.ok === false)).toEqual([]);

	// Every commit went through, the stream holds far more than the hot limit, and once the
	// snapshot thread has caught up with the (now idle) owner the retained part is a small multiple
	// of the snapshot threshold T (8 MiB by default): the latest snapshot is within T of the tail
	// (a due snapshot is taken within one commit), and retention trails it by one snapshot, whose
	// gap is T plus what the writer appended while the previous snapshot was in flight. Measured
	// before it settles, the tail can be several snapshot cycles ahead of a burst-fast writer.
	let h = await head(url);
	for (let i = 0, still = 0; i < 200 && still < 5; i++) {
		await new Promise((r) => setTimeout(r, 100));
		const next = await head(url);
		still = next.snapshot === h.snapshot && next.retained === h.retained && next.retained > laggingOffset ? still + 1 : 0;
		h = next;
	}
	console.log(`settled: tail ${h.tail}, snapshot ${h.snapshot}, retained ${h.retained}`);
	expect(h.tail).toBeGreaterThan(150 * MiB);
	expect(h.retained).toBeGreaterThan(laggingOffset);
	expect(h.snapshot).toBeGreaterThanOrEqual(h.retained);
	expect(h.tail - h.snapshot).toBeLessThan(9 * MiB);
	expect(h.tail - h.retained).toBeLessThan(40 * MiB);
	expect((await fetch(`${url}?offset=0`)).status).toBe(410);

	// A fresh host takes over: snapshot + tail, then its claim fences the owner.
	const fresh = freshFile();
	const offset = attach(fresh, url);
	expect(offset).toBeGreaterThan(h.snapshot);
	const zombie = await owner.waitFor((x) => x.step === ROUNDS + 1 && x.ok !== undefined, 60_000);
	expect(zombie.ok).toBe(false);
	const done = await owner.waitFor((x) => x.done === true);
	expect(done).toMatchObject({ poisoned: true });
	expect(done.snapshots).toBeGreaterThanOrEqual(5);
	expect(status(fresh).epoch).toBe((done.epoch ?? 0) + 1);
	expect((await owner.exited).code).toBe(0);

	// Byte-identical to the owner's own file, which never saw the zombie row.
	const ownerFile = settle(file);
	expect(ownerFile.dump["t"]?.length).toBe(48);
	const rebuilt = readFileSync(fresh);
	expect(rebuilt.equals(ownerFile.bytes)).toBe(true);

	// The lagging host's file is below the retention: re-attaching installs the snapshot.
	attach(lagging, url);
	expect(readFileSync(lagging).equals(ownerFile.bytes)).toBe(true);
	expect(settle(lagging).dump).toEqual(ownerFile.dump);
}, 900_000);
