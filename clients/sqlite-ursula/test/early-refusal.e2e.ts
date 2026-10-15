// A refusal that `ursula gateway` decides from a write's head alone (a refused token, an authorizer's
// denial, a tenant's rate limit) reaches a commit larger than one read of its body. The gateway reads
// the body before it sends such a refusal (#540). Before 0.7.4 it answered at once and closed the
// connection with the body unread, so the extension, which sends the whole body before it reads the
// answer, met a failed write or a reset instead: an unknown outcome, and a commit whose token was
// refused poisoned the file once the retry budget ran out. Here the owner's token file turns bad
// before a commit of 8 MiB of random bytes, through a gateway that checks tokens: every attempt gets
// the gateway's 401, the commit fails alone as `unauthorized`, and the next commit, with a good token
// again, succeeds. The commit is larger than the loopback holds while the gateway reads nothing: a
// 2 MiB one got the 401 on CI's runners even from a gateway that closed the connection unread.
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, expect, inject, it } from "vitest";
import { attach } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { AuthGateway, type Child, type ChildLine, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

/** SQLITE_IOERR_AUTH: a commit refused before it reached the stream. */
const COMMIT_REFUSED = 7178;

const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-early-refusal-"));
let gateway: AuthGateway | undefined;

beforeAll(async () => {
	gateway = await AuthGateway.start(
		inject("ursulaNodes").map((node) => node.url),
		dir,
	);
});

afterAll(() => {
	gateway?.close();
	rmSync(dir, { recursive: true, force: true });
});

const step = (child: Child, n: number): ChildLine | undefined => child.lines.find((l) => l.step === n && l.ok !== undefined);

const xs = (file: string): string[] => {
	const db = openPlain(file);
	try {
		return (db.prepare("SELECT x FROM t ORDER BY rowid").all() as { x: string }[]).map((r) => r.x);
	} finally {
		db.close();
	}
};

// Quarantined for 0.8: the server no longer sends `x-ursula-not-applied`, so
// this driver cannot learn why a write was refused. The 0.8 VFS driver (#590
// and successors) replaces it and owns this test.
it.skip("an 8 MiB commit whose token the gateway refuses before reading its body fails alone as unauthorized, and the next commit succeeds", async () => {
	const g = gateway as AuthGateway;
	const path = streamPath();
	const file = freshFile();
	const token = join(dir, `token-${randomUUID()}`);
	writeFileSync(token, `${g.mint()}\n`);
	const [refusing, accepting] = [`${file}.refusing`, `${file}.accepting`];
	const steps = [
		"CREATE TABLE t(x TEXT)",
		"CREATE TABLE b(x BLOB)",
		"INSERT INTO t VALUES ('a1')",
		"@status",
		`@wait:${refusing}`,
		// 8 MiB of random bytes: a commit frame of about 8 MiB, which zstd cannot shrink.
		`BEGIN; ${"INSERT INTO b VALUES (randomblob(1048576)); ".repeat(8)}INSERT INTO t VALUES ('refused'); COMMIT`,
		"@status",
		`@wait:${accepting}`,
		"INSERT INTO t VALUES ('a2')",
	];
	const child = runChild(file, g.url + path, steps, { URSULA_VFS_TOKEN_FILE: token, URSULA_VFS_RETRY_MS: "2000", CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === 4 && l.phase === "start");
	writeFileSync(token, "not-a-token\n");
	writeFileSync(refusing, "");
	await child.waitFor((l) => l.step === 7 && l.phase === "start");
	writeFileSync(token, `${g.mint()}\n`);
	writeFileSync(accepting, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);

	const accepted = step(child, 3)?.status;
	expect(step(child, 5), child.stderr()).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 6)?.status).toMatchObject({
		poisoned: false,
		fenced: false,
		writes_refused: true,
		refused_kind: "unauthorized",
		offset: accepted?.offset,
		epoch: accepted?.epoch,
	});
	expect(step(child, 8)?.ok).toBe(true);
	expect(done).toMatchObject({ poisoned: false, fenced: false, writes_refused: false, refused_kind: null, refused_reason: null });
	const log = child.stderr();
	// Every attempt of the refused commit got the gateway's 401, none a closed connection.
	const reasons = [...log.matchAll(/event=append_(?:retry|gave_up) stream=\S+ attempt=\d+ elapsed_ms=\d+ reason=(\S+)/g)].map((m) => m[1]);
	expect(reasons.length, log).toBeGreaterThan(1);
	expect(new Set(reasons), log).toEqual(new Set(["401"]));
	expect(log).toMatch(/event=writes_refused file=\S+ stream=\S+ during=commit kind=unauthorized /);
	expect(log).not.toMatch(/event=poisoned/);

	// The stream holds the accepted commits alone.
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["a1", "a2"]);
});
