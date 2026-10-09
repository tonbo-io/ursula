// Same-minor compatibility (docs/architecture/sqlite-vfs.md §7): the previous published patch of
// this minor (SQLITE_URSULA_VFS_PREVIOUS) and this build read each other's frames, snapshots and
// sidecars, and fence each other's owners. Skipped without SQLITE_URSULA_VFS_PREVIOUS; CI sets it to
// the previous patch's prebuilt extension, or to this build when the minor has none yet.
import { writeFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { freshFile } from "./helpers.ts";
import { type Child, runChild, streamPath, ursulaUrl, vfsPath } from "./kit.ts";

const previous = process.env.SQLITE_URSULA_VFS_PREVIOUS;
const DONE = { CHILD_EXIT: "1" };
const xs = (child: Child, step: number): string[] => (child.lines.find((l) => l.step === step && l.rows !== undefined)?.rows ?? []).map((r) => String(r.x));
const ok = (child: Child, step: number): boolean | undefined => child.lines.find((l) => l.step === step && l.ok !== undefined)?.ok;

describe.skipIf(previous === undefined || previous.length === 0)("same-minor compatibility", () => {
	const versions = { previous: previous ?? "", current: vfsPath() };
	for (const [from, to] of [
		["previous", "current"],
		["current", "previous"],
	] as const) {
		it(`${to} takes over a stream ${from} wrote, on a new host and on the same one`, async () => {
			const url = ursulaUrl() + streamPath();
			const fileW = freshFile();
			const go = `${fileW}.go`;
			// The writer outgrows its database with rewrites, so a snapshot is due (a 1-byte minimum),
			// commits past it, then waits.
			const steps = [
				"CREATE TABLE t(x TEXT)",
				"CREATE TABLE pad(k INTEGER PRIMARY KEY, v TEXT)",
				"INSERT INTO t VALUES ('w1')",
				"INSERT INTO t VALUES ('w2')",
				...Array.from({ length: 30 }, () => "INSERT OR REPLACE INTO pad VALUES (1, hex(randomblob(600)))"),
				"@sleep:1500",
				"INSERT INTO t VALUES ('w3')",
				`@wait:${go}`,
				"INSERT INTO t VALUES ('w4')",
			];
			const w3 = steps.indexOf("INSERT INTO t VALUES ('w3')");
			const w4 = steps.length - 1;
			const w = runChild(fileW, url, steps, { ...DONE, URSULA_VFS_SNAPSHOT_MIN_BYTES: "1" }, versions[from]);
			await w.waitFor((l) => l.step === w3 && l.ok === true);
			const head = await fetch(url, { method: "HEAD" });
			expect(head.headers.get("stream-snapshot-offset")).not.toBeNull();

			// A new host: installs the writer's snapshot, replays its frames and claims, then waits
			// before its first commit.
			const fileT = freshFile();
			const goT = `${fileT}.go`;
			const t = runChild(fileT, url, ["@query:SELECT x FROM t ORDER BY x", `@wait:${goT}`, "INSERT INTO t VALUES ('t1')", "@query:SELECT x FROM t ORDER BY x"], DONE, versions[to]);
			await t.waitFor((l) => l.step === 1 && l.phase === "start");

			// The writer commits after the claim and before the new owner's first commit, so only the
			// claim's epoch fences it: a `Producer-Id` that changed within the minor would let this
			// commit through (or, after the new owner's commit, fence it by `Stream-Seq` instead).
			writeFileSync(go, "");
			expect((await w.exited).code).toBe(0);
			expect(ok(w, w4)).toBe(false);
			const wDone = w.lines.find((l) => l.done);
			expect(wDone).toMatchObject({ poisoned: true, fenced: true });
			expect(wDone?.reason).toMatch(/superseded/);

			writeFileSync(goT, "");
			expect((await t.exited).code).toBe(0);
			expect(xs(t, 0)).toEqual(["w1", "w2", "w3"]);
			expect(ok(t, 2)).toBe(true);
			expect(xs(t, 3)).toEqual(["t1", "w1", "w2", "w3"]);
			expect(t.lines.find((l) => l.done)?.installed).not.toBe("-1");

			// The writer's version reads the frames the new owner wrote.
			const r = runChild(freshFile(), url, ["@query:SELECT x FROM t ORDER BY x"], DONE, versions[from]);
			expect((await r.exited).code).toBe(0);
			expect(xs(r, 0)).toEqual(["t1", "w1", "w2", "w3"]);

			// The same host: the new version trusts the writer's local files and sidecar (no snapshot
			// install), catches up, and commits.
			const s = runChild(fileW, url, ["@query:SELECT x FROM t ORDER BY x", "INSERT INTO t VALUES ('s1')"], DONE, versions[to]);
			expect((await s.exited).code).toBe(0);
			expect(xs(s, 0)).toEqual(["t1", "w1", "w2", "w3"]);
			expect(ok(s, 1)).toBe(true);
			const done = s.lines.find((l) => l.done);
			expect(done?.installed).toBe("-1");
			expect(done?.local).not.toBe("-1");
		});
	}
});
