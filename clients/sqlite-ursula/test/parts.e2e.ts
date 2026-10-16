// PART runs (design: Appends): a commit above the batch limit, min(request limit, 4 MiB), goes out
// as a run of PART frames, one per append, and readers apply only a complete run. Against the e2e
// server (test/stack.ts):
// - Under a lowered limit (URSULA_VFS_MAX_REQUEST_BYTES=65536) a 1 MiB transaction commits, synced()
//   resolves, and a fresh build equals it. A 9 MiB transaction at the default limit goes out in
//   parts of 4 MiB.
// - A crash between parts (URSULA_VFS_ABORT_AT=part_sent:1) leaves part 0 in the stream: a fresh
//   build holds none of the transaction, and the reopen sends it again whole.
// - Model only: a commit logged as one COMMIT while the stream is cut off, then a limit lowered
//   below it, re-cuts after the heal: the replicator claims the next epoch and sends it in parts.
import { afterEach, describe, expect, it } from "vitest";
import { Child, directUrl, fromStream, freshDir, gate, killChildren, onModel, parts, stream } from "./kit.ts";

afterEach(killChildren);

/** Rows of 64 KiB of random bytes: samples of each, so equal rows are equal bytes but for chance. */
const ROWS_SQL = "SELECT i, length(b) AS n, hex(substr(b, 1, 64)) AS head, hex(substr(b, 30000, 64)) AS mid, hex(substr(b, -64)) AS tail FROM t ORDER BY i";
const BLOB = 64 << 10;

/** One transaction inserting `rows` rows of 64 KiB of random bytes, `from` on. */
function insert(from: number, rows: number): string {
	const values = Array.from({ length: rows }, (_, k) => `(${from + k}, randomblob(${BLOB}))`).join(", ");
	return `BEGIN; INSERT INTO t VALUES ${values}; COMMIT`;
}

async function synced(c: Child, ms = 60_000): Promise<void> {
	expect((await c.send({ synced: ms })).synced).toMatchObject({ synced: true });
}

interface Status {
	epoch: number | null;
	reset: unknown;
}

async function status(c: Child): Promise<Status> {
	return (await c.send({ status: true })).status as Status;
}

describe("PART runs", () => {
	it("a 1 MiB transaction above a 64 KiB request limit commits in parts", async () => {
		const s = stream("parts-64k");
		const owner = new Child(s.url, freshDir(), { URSULA_VFS_MAX_REQUEST_BYTES: "65536" });
		await owner.opened();
		await owner.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)");
		await owner.exec(insert(1, 16));
		await synced(owner);
		const local = await owner.rows(ROWS_SQL);
		expect(local).toHaveLength(16);
		expect((await status(owner)).reset).toBeNull();
		await owner.stop();
		expect(await parts(s.path)).toBeGreaterThanOrEqual(16);
		expect(await fromStream(s.url, ROWS_SQL)).toEqual(local);
	});

	it("a 9 MiB transaction at the default limit commits in 4 MiB parts", async () => {
		const s = stream("parts-9m");
		const owner = new Child(s.url, freshDir());
		await owner.opened();
		await owner.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)");
		await owner.exec(insert(1, 144));
		await synced(owner);
		const local = await owner.rows(ROWS_SQL);
		expect(local).toHaveLength(144);
		await owner.stop();
		expect(await parts(s.path)).toBe(3);
		expect(await fromStream(s.url, ROWS_SQL)).toEqual(local);
	});

	it("a crash between parts leaves no part of the transaction applied, and the reopen sends it whole", async () => {
		const s = stream("parts-crash");
		const dir = freshDir();
		const setup = new Child(s.url, dir);
		await setup.opened();
		await setup.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)");
		await setup.exec("INSERT INTO t VALUES (1, x'01'), (2, x'02')");
		await synced(setup);
		const before = await setup.rows(ROWS_SQL);
		await setup.stop();

		const doomed = new Child(s.url, dir, { URSULA_VFS_ABORT_AT: "part_sent:1", URSULA_VFS_MAX_REQUEST_BYTES: "65536" });
		await doomed.opened();
		// The commit is local at once; the kill comes before its second part goes out, maybe
		// before the answer reaches us.
		await doomed.send({ exec: insert(3, 16) }).catch(() => undefined);
		const { signal } = await doomed.exit;
		expect(signal, "the child ended without the kill at part_sent:1").toBe("SIGKILL");
		expect(await parts(s.path), "part 0 landed before the kill").toBeGreaterThanOrEqual(1);
		// A reader applies only a complete run.
		expect(await fromStream(s.url, ROWS_SQL)).toEqual(before);

		const reopened = new Child(s.url, dir);
		await reopened.opened();
		const local = await reopened.rows(ROWS_SQL);
		expect(local).toHaveLength(18);
		await synced(reopened);
		const st = await status(reopened);
		expect(st.reset).toBeNull();
		expect(await reopened.rows("PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
		await reopened.stop();
		expect(await fromStream(s.url, ROWS_SQL)).toEqual(local);
	});

	it.runIf(onModel())("a commit logged whole re-cuts under the next epoch when the limit falls below it", async () => {
		const g = await gate(directUrl());
		try {
			const s = stream("parts-recut");
			const owner = new Child(`${g.url}${s.path}`, freshDir());
			await owner.opened();
			await owner.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)");
			await synced(owner);
			const epoch = (await status(owner)).epoch;
			expect(epoch).not.toBeNull();

			await mode(g.control, "refuse");
			// Under the default limit the commit is one COMMIT frame, its append refused at the gate.
			await owner.exec(insert(1, 16));
			const limited = await fetch(`${directUrl()}/__model/limits?max_request_bytes=65536`, { method: "PUT" });
			expect(limited.status).toBe(204);
			await mode(g.control, "forward");

			await synced(owner);
			const st = await status(owner);
			expect(st.reset).toBeNull();
			expect(st.epoch).toBe((epoch as number) + 1);
			const local = await owner.rows(ROWS_SQL);
			expect(local).toHaveLength(16);
			await owner.stop();
			expect(await parts(s.path)).toBeGreaterThanOrEqual(16);
			expect(await fromStream(s.url, ROWS_SQL)).toEqual(local);
		} finally {
			await fetch(`${directUrl()}/__model/limits?max_request_bytes=${32 << 20}`, { method: "PUT" });
			await g.stop();
		}
	});
});

async function mode(control: string, m: "forward" | "refuse"): Promise<void> {
	const r = await fetch(`${control}/mode`, { method: "PUT", body: JSON.stringify({ mode: m }) });
	expect(r.status).toBe(204);
}
