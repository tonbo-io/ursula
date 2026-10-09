// A layer in front of the stream (an authorizer, a quota or billing service) may refuse writes with
// 402 Payment Required. Nothing of a refused write reached the stream, so it fails alone: the commit
// is rolled back but the file is not poisoned, reads go on from the local file, and the next commit
// goes out as usual (same epoch, same claim) and succeeds once the layer accepts writes again. An
// attach the layer refuses claims nothing, and a read-only attach reads the database without writing
// the stream at all. The layer here is `gate.mjs`: the server never answers 402 itself.
import { writeFileSync } from "node:fs";
import { afterAll, beforeAll, expect, it } from "vitest";
import { attach, openUrsulaPiStorage, status, UrsulaPaymentRequiredError, UrsulaReplicationError } from "../src/index.ts";
import { openHarness, textTurn } from "./harness-kit.ts";
import { ctx, freshFile } from "./helpers.ts";
import { type Child, type ChildLine, Gate, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

/** The layer's explanation, as a quota or billing service might send it. */
const REASON = '{"error":"payment_required","detail":"quota exhausted"}';
/** SQLITE_IOERR_AUTH: a commit refused with 402. */
const COMMIT_REFUSED = 7178;
/** SQLITE_IOERR_WRITE: a commit that poisons the file. */
const POISONED = 778;

let gate: Gate;
beforeAll(async () => {
	gate = await Gate.start(ursulaUrl());
});
afterAll(async () => {
	await gate.close();
});

const xs = (file: string): string[] => {
	const db = openPlain(file);
	try {
		return (db.prepare("SELECT x FROM t ORDER BY x").all() as { x: string }[]).map((r) => r.x);
	} finally {
		db.close();
	}
};
const step = (child: Child, n: number): ChildLine | undefined => child.lines.find((l) => l.step === n && l.ok !== undefined);
const tail = async (url: string): Promise<string | null> => (await fetch(url, { method: "HEAD" })).headers.get("stream-next-offset");

it("a commit refused with 402 fails alone: reads go on, and the next commit succeeds once the layer accepts writes, without a new attach", async () => {
	const path = streamPath();
	const file = freshFile();
	const [refusing, accepting] = [`${file}.refusing`, `${file}.accepting`];
	const steps = [
		"CREATE TABLE t(x TEXT)",
		"INSERT INTO t VALUES ('a1')",
		// The refused commit then starts a new WAL generation (its header is refused with it).
		"PRAGMA wal_checkpoint(TRUNCATE)",
		"@status",
		`@wait:${refusing}`,
		"INSERT INTO t VALUES ('a2')",
		"@query:SELECT x FROM t ORDER BY x",
		"@status",
		"BEGIN; INSERT INTO t VALUES ('a3'); INSERT INTO t VALUES ('a4'); COMMIT",
		`@wait:${accepting}`,
		"INSERT INTO t VALUES ('a5')",
		"@query:SELECT x FROM t ORDER BY x",
	];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === 4 && l.phase === "start");
	await gate.refuse(REASON);
	writeFileSync(refusing, "");
	await child.waitFor((l) => l.step === 9 && l.phase === "start");
	await gate.refuse(null);
	writeFileSync(accepting, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);

	const before = step(child, 3)?.status;
	expect(step(child, 5)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 6)?.rows).toEqual([{ x: "a1" }]);
	const refused = step(child, 7)?.status;
	expect(refused).toMatchObject({ poisoned: false, fenced: false, read_only: false, payment_required: true, payment_reason: REASON });
	// Nothing of the refused commit moved the owner: same epoch, same offset.
	expect([refused?.epoch, refused?.offset]).toEqual([before?.epoch, before?.offset]);
	expect(step(child, 8)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 10)?.ok).toBe(true);
	expect(step(child, 11)?.rows).toEqual([{ x: "a1" }, { x: "a5" }]);
	expect(done).toMatchObject({ poisoned: false, fenced: false, payment_required: false, payment_reason: null, epoch: before?.epoch });
	expect(done.attempts).toEqual([1, 1, 1]);
	expect(child.stderr()).toMatch(/event=payment_required file=\S+ stream=\S+ during=commit reason=/);
	expect(child.stderr()).toMatch(/event=writes_resumed file=\S+ stream=\S+$/m);
	expect(child.stderr()).not.toMatch(/event=poisoned/);

	// The local files still match their sidecar: attached again on this boot, they are trusted.
	attach(file, ursulaUrl() + path);
	expect(status(file)).toMatchObject({ local: done.offset, installed: "-1" });
	expect(xs(file)).toEqual(["a1", "a5"]);
	// The stream holds the accepted commits alone.
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["a1", "a5"]);
});

// The commit's first attempt reaches the stream but its answer is lost, and the retries meet the 402:
// the commit may be in the stream, so the refusal proves nothing. It is retried until an answer
// settles it: once the layer forwards it again, the stream answers it as a duplicate of the first.
it("an append whose answer was lost and whose retries meet a 402 is retried until its outcome is known", async () => {
	const path = streamPath();
	const file = freshFile();
	const armed = `${file}.armed`;
	const child = runChild(file, gate.url + path, ["CREATE TABLE t(x TEXT)", `@wait:${armed}`, "INSERT INTO t VALUES ('once')"], { CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	const before = await gate.counts();
	await gate.dropNextPostThenRefuse(REASON, 2);
	writeFileSync(armed, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	const after = await gate.counts();
	expect([after.dropped - before.dropped, after.refused - before.refused]).toEqual([1, 2]);
	expect(step(child, 2)?.ok).toBe(true);
	expect(done).toMatchObject({ poisoned: false, payment_required: false, attempts: [1, 4] });
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["once"]);
});

// The fence rules do not change: after a refusal, an owner that another one claimed over is fenced and
// poisoned at its next commit, as ever.
it("an owner refused with 402 is still fenced by a newer owner's claim", async () => {
	const path = streamPath();
	const file = freshFile();
	const [refusing, claimed] = [`${file}.refusing`, `${file}.claimed`];
	const child = runChild(file, gate.url + path, ["CREATE TABLE t(x TEXT)", `@wait:${refusing}`, "INSERT INTO t VALUES ('a1')", `@wait:${claimed}`, "INSERT INTO t VALUES ('a2')"], {
		CHILD_EXIT: "1",
	});
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	await gate.refuse(REASON);
	writeFileSync(refusing, "");
	await child.waitFor((l) => l.step === 3 && l.phase === "start");
	await gate.refuse(null);
	attach(freshFile(), ursulaUrl() + path);
	writeFileSync(claimed, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	expect(step(child, 2)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 4)).toMatchObject({ ok: false, errcode: POISONED });
	expect(done).toMatchObject({ poisoned: true, fenced: true });
	expect(done.reason).toMatch(/superseded.*\(403\)/);
});

it("an attach the layer refuses throws UrsulaPaymentRequiredError, and a read-only attach reads the database without writing the stream", async () => {
	const path = streamPath();
	const url = ursulaUrl() + path;
	const ownerFile = freshFile();
	attach(ownerFile, url);
	const owner = openPlain(ownerFile);
	owner.exec("CREATE TABLE t(x TEXT)");
	owner.exec("INSERT INTO t VALUES ('r1')");
	const end = await tail(url);

	await gate.refuse(REASON);
	const file = freshFile();
	let error: unknown;
	try {
		attach(file, gate.url + path);
	} catch (e) {
		error = e;
	}
	expect(error).toBeInstanceOf(UrsulaPaymentRequiredError);
	expect((error as Error).message).toMatch(/claim \S+: 402 Payment Required/);
	expect((error as Error).message).toContain(REASON);
	// The failed attach leaves the file refused, as any failed attach does.
	expect(() => openPlain(file)).toThrow(/unable to open database file/);

	// Read-only: the same file catches up and opens; writes fail at once, and nothing is appended.
	expect(attach(file, gate.url + path, { readOnly: true })).toBe(end);
	const reader = openPlain(file);
	expect((reader.prepare("SELECT x FROM t").all() as { x: string }[]).map((r) => r.x)).toEqual(["r1"]);
	expect(() => reader.exec("INSERT INTO t VALUES ('r2')")).toThrow(/attempt to write a readonly database/);
	expect(() => reader.exec("BEGIN; CREATE TABLE u(y); COMMIT")).toThrow(/attempt to write a readonly database/);
	expect((reader.prepare("SELECT x FROM t").all() as { x: string }[]).map((r) => r.x)).toEqual(["r1"]);
	expect(status(file)).toMatchObject({ read_only: true, poisoned: false, payment_required: false, offset: end });
	reader.close();
	expect(await tail(url)).toBe(end);
	expect((await gate.counts()).refused).toBeGreaterThanOrEqual(1);
	await gate.refuse(null);

	// The owner was not fenced; the read-only file holds the stream as of its attach, and catches up
	// when attached again.
	owner.exec("INSERT INTO t VALUES ('r3')");
	expect(status(ownerFile)).toMatchObject({ poisoned: false, fenced: false });
	owner.close();
	expect(xs(file)).toEqual(["r1"]);
	attach(file, url, { readOnly: true });
	expect(xs(file)).toEqual(["r1", "r3"]);
	// And attached as an owner again, it commits.
	attach(file, url);
	const again = openPlain(file);
	again.exec("INSERT INTO t VALUES ('r4')");
	expect(status(file)).toMatchObject({ read_only: false, poisoned: false });
	again.close();
});

it("a read-only attach of a missing stream fails and creates nothing", async () => {
	const url = ursulaUrl() + streamPath();
	expect(() => attach(freshFile(), url, { readOnly: true })).toThrow(/missing .*a read-only attach never creates a stream/);
	expect((await fetch(url, { method: "HEAD" })).status).toBe(404);
});

// A layer that refuses only snapshot publishes (PUT): the refusal shows in `payment_required`, is no
// snapshot failure, and the snapshot is published after the next acknowledged commit.
it("a snapshot refused with 402 waits for the next acknowledged commit, and is no snapshot failure", async () => {
	const path = streamPath();
	const file = freshFile();
	const [refusing, accepting] = [`${file}.refusing`, `${file}.accepting`];
	const rows = Array.from({ length: 20 }, () => "INSERT OR REPLACE INTO t VALUES (1, hex(randomblob(800)))");
	const steps = [
		"CREATE TABLE t(k INTEGER PRIMARY KEY, x TEXT)",
		`@wait:${refusing}`,
		...rows,
		"@sleep:1500",
		"@status",
		`@wait:${accepting}`,
		"INSERT INTO t VALUES (2, 'after')",
		"@sleep:1500",
	];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1", URSULA_VFS_SNAPSHOT_MIN_BYTES: "1" });
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	await gate.refuse(REASON, { methods: ["PUT"] });
	writeFileSync(refusing, "");
	await child.waitFor((l) => l.step === rows.length + 4 && l.phase === "start");
	await gate.refuse(null);
	writeFileSync(accepting, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	const refused = step(child, rows.length + 3)?.status;
	expect(refused).toMatchObject({ poisoned: false, payment_required: true, payment_reason: REASON, snapshot: "-1", snapshot_failures: 0, snapshot_error: null });
	expect(child.stderr()).toMatch(/event=payment_required file=\S+ stream=\S+ during="snapshot publish"/);
	expect(done).toMatchObject({ poisoned: false, payment_required: false });
	expect(done.snapshot).not.toBe("-1");
	expect(done.health).toMatchObject({ snapshot_failures: 0, snapshot_error: null });
});

it("Pi: a refused commit rejects with UrsulaPaymentRequiredError, and the storage commits again once the layer accepts", async () => {
	const path = streamPath();
	const storage = await openUrsulaPiStorage(freshFile(), gate.url + path);
	await gate.refuse(REASON);
	const error = await openHarness(storage).then(
		() => undefined,
		(e: unknown) => e,
	);
	await gate.refuse(null);
	expect(error).toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(error).not.toBeInstanceOf(UrsulaReplicationError);
	expect((error as UrsulaPaymentRequiredError).reason).toBe(REASON);
	const { harness, root } = await openHarness(storage);
	await textTurn(root, 1);
	await harness.close(ctx);
	await storage.close(ctx);
});

it("Pi: a read-only storage reads what an owner wrote, and refuses its commits", async () => {
	const url = ursulaUrl() + streamPath();
	const owner = await openUrsulaPiStorage(freshFile(), url);
	const opened = await openHarness(owner);
	await textTurn(opened.root, 1);
	await opened.harness.close(ctx);
	await owner.close(ctx);
	const end = await tail(url);

	const file = freshFile();
	const reader = await openUrsulaPiStorage(file, url, { readOnly: true });
	const conversations = await reader.scanConversations({}, 10, undefined, ctx);
	expect(conversations.items.length).toBeGreaterThan(0);
	expect(status(file)).toMatchObject({ read_only: true });
	// The harness opens without a commit (its root exists); a turn needs one.
	const { harness, root } = await openHarness(reader);
	const error = await textTurn(root, 2).then(
		() => undefined,
		(e: unknown) => e,
	);
	expect(error).toMatchObject({ errcode: 8, message: "attempt to write a readonly database" });
	await harness.close(ctx);
	await reader.close(ctx);
	expect(await tail(url)).toBe(end);
});
