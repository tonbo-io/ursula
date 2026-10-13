// Writes refused before they reach the stream for other reasons than a 402: answers marked
// `x-ursula-not-applied` (the server's admission while it is under write pressure, a gateway that
// cannot authorize a request, an authorizer's 403) and vouched for by a gateway with
// `x-ursula-not-applied-checked`, and a token refused until the retry budget runs out. Nothing of
// such a write was applied, so it fails alone, as a refused 402 does: the commit is rolled back but
// the file is not poisoned, reads go on, and the next commit goes out as usual and succeeds once the
// refusals stop. An attempt whose outcome is unknown among the refusals still poisons: the commit may
// be in the stream. So does a marker without the vouch, and so does the commit after a refusal for a
// commit the stream applied. The refusing layer is `gate.mjs`, except for a commit frame over the
// extension's frame limit, which the extension refuses itself without sending it.
import { writeFileSync } from "node:fs";
import { afterAll, afterEach, beforeAll, expect, it } from "vitest";
import { attach, instrument, status, UrsulaPaymentRequiredError, UrsulaReplicationError, UrsulaWriteRefusedError } from "../src/index.ts";
import { openUrsulaPiStorage } from "../src/pi.ts";
import { openHarness, textTurn } from "./harness-kit.ts";
import { ctx, freshFile } from "./helpers.ts";
import { type Child, type ChildLine, Gate, integrity, openPlain, recordingMeter, runChild, streamPath, ursulaUrl } from "./kit.ts";

/** SQLITE_IOERR_AUTH: a commit refused before it reached the stream. */
const COMMIT_REFUSED = 7178;
/** SQLITE_IOERR_WRITE: a commit that poisons the file. */
const POISONED = 778;
/** SQLITE_ERROR: an attach that failed for any other reason than a refusal. */
const SQLITE_ERROR = 1;
/** The vouch with which a gateway (Ursula's, from 0.7.3 on) makes the marker count. */
const VOUCH = { "x-ursula-not-applied-checked": "1" };
/** The server's answer while its Raft log is over its hard limit, marked as applying nothing, through a gateway that vouches for the marker. */
const PRESSURE = { status: 503, headers: { "x-ursula-not-applied": "RaftLogPressure", ...VOUCH, "retry-after": "1" } };
/** The same answer as a node sends it, or a gateway before 0.7.3 relays it: the marker alone. */
const UNVOUCHED_PRESSURE = { status: 503, headers: { "x-ursula-not-applied": "RaftLogPressure", "retry-after": "1" } };
const PRESSURE_BODY = '{"error":"RaftLogPressure"}';
/** An authorizer's denial, marked as applying nothing the way Ursula's gateway marks it. */
const DENIED = { status: 403, headers: { "x-ursula-not-applied": "Forbidden", ...VOUCH } };
/** A retry budget that runs out after two attempts answered `Retry-After: 1`. */
const SHORT_BUDGET = { URSULA_VFS_RETRY_MS: "1500" };

let gate: Gate;
beforeAll(async () => {
	gate = await Gate.start(ursulaUrl());
});
// A test that fails midway leaves the gate refusing: the next test starts with it forwarding.
afterEach(async () => {
	await gate.reset();
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
const errcode = (error: unknown): unknown => (error as { errcode?: unknown } | undefined)?.errcode;

it("a commit refused under write pressure, or with its token refused, until the budget runs out fails alone, and the next commit succeeds once the refusals stop", async () => {
	const path = streamPath();
	const file = freshFile();
	const [pressure, token, accepting] = [`${file}.pressure`, `${file}.token`, `${file}.accepting`];
	const steps = [
		"CREATE TABLE t(x TEXT)",
		"INSERT INTO t VALUES ('a1')",
		"@status",
		`@wait:${pressure}`,
		"INSERT INTO t VALUES ('a2')",
		"@query:SELECT x FROM t ORDER BY x",
		"@status",
		`@wait:${token}`,
		"BEGIN; INSERT INTO t VALUES ('a3'); INSERT INTO t VALUES ('a4'); COMMIT",
		"@status",
		`@wait:${accepting}`,
		"INSERT INTO t VALUES ('a5')",
	];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1", ...SHORT_BUDGET });
	await child.waitFor((l) => l.step === 3 && l.phase === "start");
	await gate.refuse(PRESSURE_BODY, PRESSURE);
	writeFileSync(pressure, "");
	await child.waitFor((l) => l.step === 7 && l.phase === "start");
	await gate.refuse("", { status: 401 });
	writeFileSync(token, "");
	await child.waitFor((l) => l.step === 10 && l.phase === "start");
	await gate.refuse(null);
	writeFileSync(accepting, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);

	const before = step(child, 2)?.status;
	expect(step(child, 4)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 5)?.rows).toEqual([{ x: "a1" }]);
	const underPressure = step(child, 6)?.status;
	expect(underPressure).toMatchObject({
		poisoned: false,
		fenced: false,
		writes_refused: true,
		refused_kind: "not_applied",
		refused_reason: "RaftLogPressure",
		payment_required: false,
		payment_reason: null,
	});
	// Nothing of the refused commit moved the owner: same epoch, same offset.
	expect([underPressure?.epoch, underPressure?.offset]).toEqual([before?.epoch, before?.offset]);
	expect(step(child, 8)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 9)?.status).toMatchObject({ poisoned: false, writes_refused: true, refused_kind: "unauthorized", refused_reason: null, offset: before?.offset });
	expect(step(child, 11)?.ok).toBe(true);
	expect(done).toMatchObject({ poisoned: false, fenced: false, writes_refused: false, refused_kind: null, refused_reason: null, epoch: before?.epoch });
	expect(done.attempts).toEqual([1, 1, 1]);
	const log = child.stderr();
	expect(log).toMatch(/event=writes_refused file=\S+ stream=\S+ during=commit kind=not_applied reason="503 \(not applied: RaftLogPressure\)"/);
	expect(log).toMatch(/event=writes_refused file=\S+ stream=\S+ during=commit kind=unauthorized reason=/);
	expect(log).toMatch(/event=writes_resumed file=\S+ stream=\S+$/m);
	expect(log).not.toMatch(/event=poisoned/);

	// The local files still match their sidecar, and the stream holds the accepted commits alone.
	attach(file, ursulaUrl() + path);
	expect(status(file)).toMatchObject({ local: done.offset, installed: "-1" });
	expect(xs(file)).toEqual(["a1", "a5"]);
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["a1", "a5"]);
});

// The commit's first attempt reaches the stream but its answer is lost, and every retry is refused
// as applying nothing until the budget runs out: the refusals say nothing about the first attempt,
// which may be (and here is) in the stream. The owner is poisoned, never refused: sent again with
// the same producer sequence, the next commit would be acknowledged as a duplicate of this one.
it("an append whose answer was lost and whose retries are refused as applying nothing still poisons", async () => {
	const path = streamPath();
	const file = freshFile();
	const armed = `${file}.armed`;
	const child = runChild(file, gate.url + path, ["CREATE TABLE t(x TEXT)", `@wait:${armed}`, "INSERT INTO t VALUES ('once')"], { CHILD_EXIT: "1", ...SHORT_BUDGET });
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	const before = await gate.counts();
	await gate.dropNextPostThenRefuse(PRESSURE_BODY, 100, PRESSURE);
	writeFileSync(armed, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	const after = await gate.counts();
	expect(after.dropped - before.dropped).toBe(1);
	expect(after.refused - before.refused).toBeGreaterThanOrEqual(1);
	expect(step(child, 2)).toMatchObject({ ok: false, errcode: POISONED });
	expect(done).toMatchObject({ poisoned: true, fenced: false, writes_refused: false, refused_kind: null });
	expect(done.reason).toMatch(/outcome unknown/);
	// The lost attempt was applied: a rebuild from the stream holds it, once.
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["once"]);
});

// A layer that breaks the rule for refusals: it passes a commit on, the stream applies it, and the layer
// answers with a vouched refusal. The commit fails alone, as the refusal says it may. The next commit
// goes out with the same producer sequence and is answered as the refused commit's duplicate: it poisons
// instead of being acknowledged without its pages, and a replica built from the stream holds the refused
// transaction, not the next one.
it("a refusal for a commit the stream applied is caught at the next commit, which poisons", async () => {
	const path = streamPath();
	const file = freshFile();
	const [armed, accepting] = [`${file}.armed`, `${file}.accepting`];
	const steps = ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('f1')", `@wait:${armed}`, "INSERT INTO t VALUES ('f2')", "@status", `@wait:${accepting}`, "INSERT INTO t VALUES ('f3')"];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1", ...SHORT_BUDGET });
	await child.waitFor((l) => l.step === 2 && l.phase === "start");
	const before = await gate.counts();
	await gate.forwardNextPostThenRefuse(PRESSURE_BODY, 100, PRESSURE);
	writeFileSync(armed, "");
	await child.waitFor((l) => l.step === 5 && l.phase === "start");
	await gate.reset();
	writeFileSync(accepting, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	const after = await gate.counts();
	expect(after.replaced - before.replaced).toBe(1);
	// The node applied the refused commit as a fresh append.
	expect(after.replacedStatuses.slice(before.replacedStatuses.length)).toEqual([200]);
	expect(step(child, 3)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 4)?.status).toMatchObject({ poisoned: false, writes_refused: true, refused_kind: "not_applied", refused_reason: "RaftLogPressure" });
	expect(step(child, 6)).toMatchObject({ ok: false, errcode: POISONED });
	expect(done).toMatchObject({ poisoned: true, fenced: false, writes_refused: false, refused_kind: null });
	expect(done.reason).toMatch(/^a commit refused as never applied may have been applied after all: the next commit, at epoch \d+ seq \d+ after \S+, was answered as a duplicate \(receipt \S+\), so the stream holds either the refused commit \(.+\) or this one \(.+\), and the local file holds neither: attach the database again/);
	expect(child.stderr()).toMatch(/event=poisoned file=\S+ stream=\S+ fenced=false reason="a commit refused as never applied may have been applied after all/);
	// The stream holds the refused transaction, and not the next one.
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["f1", "f2"]);
});

// The gap that check leaves (design doc §2): when an attempt of the next commit got no answer before the
// duplicate's answer, that answer is taken for the lost attempt's own. The next commit is acknowledged
// without its pages, and the file and the stream diverge without an error. The extension logs
// `refused_commit_unchecked`, the one trace of this case.
it("a duplicate's answer after a lost attempt of the commit after a false refusal is acknowledged, with a warning", async () => {
	const path = streamPath();
	const file = freshFile();
	const [armed, accepting] = [`${file}.armed`, `${file}.accepting`];
	const steps = ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('f1')", `@wait:${armed}`, "INSERT INTO t VALUES ('f2')", `@wait:${accepting}`, "INSERT INTO t VALUES ('f3')", "@query:SELECT x FROM t ORDER BY x"];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1", ...SHORT_BUDGET });
	await child.waitFor((l) => l.step === 2 && l.phase === "start");
	const before = await gate.counts();
	await gate.forwardNextPostThenRefuse(PRESSURE_BODY, 100, PRESSURE);
	writeFileSync(armed, "");
	await child.waitFor((l) => l.step === 4 && l.phase === "start");
	// The next commit's first attempt reaches the node, which answers it as the refused commit's
	// duplicate, and that answer is lost. Its second attempt gets a 503 that proves nothing, and its third
	// reaches the node again.
	await gate.reset();
	await gate.dropNextPostThenRefuse("", 1, { status: 503 });
	writeFileSync(accepting, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	const after = await gate.counts();
	expect(after.replacedStatuses.slice(before.replacedStatuses.length)).toEqual([200]);
	expect(after.droppedStatuses.slice(before.droppedStatuses.length)).toEqual([204]);
	expect(step(child, 3)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 5)).toMatchObject({ ok: true });
	expect(done).toMatchObject({ poisoned: false, fenced: false, writes_refused: false });
	expect(done.attempts?.at(-1)).toBe(3);
	expect(child.stderr()).toMatch(/event=refused_commit_unchecked file=\S+ stream=\S+ epoch=\d+ seq=\d+ offset=\S+ receipt=\S+/);
	// Diverged: the file holds the next transaction, and the stream the refused one.
	expect(step(child, 6)?.rows).toEqual([{ x: "f1" }, { x: "f3" }]);
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["f1", "f2"]);
});

// The marker counts only with a gateway's vouch: a node sends it alone, and a gateway before 0.7.3
// relays it alone on an answer it got by following a redirect, which a node can send after it proposed
// the write. A commit refused with the marker alone until the budget runs out poisons, as a commit
// whose outcome is unknown does, and an attach whose claim a 403 marked alone denies fails as before
// 0.7.3, not with UrsulaWriteRefusedError.
it("a refusal marked without a gateway's vouch proves nothing: the commit poisons, and the attach fails as before", async () => {
	const path = streamPath();
	const file = freshFile();
	const armed = `${file}.armed`;
	const child = runChild(file, gate.url + path, ["CREATE TABLE t(x TEXT)", `@wait:${armed}`, "INSERT INTO t VALUES ('u1')"], { CHILD_EXIT: "1", ...SHORT_BUDGET });
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	await gate.refuse(PRESSURE_BODY, UNVOUCHED_PRESSURE);
	writeFileSync(armed, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	expect(step(child, 2)).toMatchObject({ ok: false, errcode: POISONED });
	expect(done).toMatchObject({ poisoned: true, fenced: false, writes_refused: false, refused_kind: null });
	expect(done.reason).toMatch(/outcome unknown/);
	expect(child.stderr()).not.toMatch(/event=writes_refused/);

	await gate.refuse("write access denied", { status: 403, headers: { "x-ursula-not-applied": "Forbidden" } });
	const reader = freshFile();
	const before = await gate.counts();
	let error: unknown;
	try {
		attach(reader, gate.url + path);
	} catch (e) {
		error = e;
	}
	expect(error).not.toBeInstanceOf(UrsulaWriteRefusedError);
	expect(errcode(error)).toBe(SQLITE_ERROR);
	// The claim may be in the stream, and may have fenced the owner: attach again.
	expect(error).toMatchObject({ name: "UrsulaReattachError", kind: "outcome_unknown", action: "reattach" });
	// The claim was the request refused: its append failed as unauthorized, and it was the one write.
	expect((error as Error).message).toMatch(/append \S+: 403( .*)?: the endpoint refused this client's credentials/);
	expect((await gate.counts()).refused - before.refused).toBe(1);
	// The gate refused every attempt of the commit: the stream holds the table alone.
	attach(reader, gate.url + path, { readOnly: true });
	expect(xs(reader)).toEqual([]);
});

// An authorizer's 403 marked `x-ursula-not-applied` and vouched for (as Ursula's gateway marks its
// denials) is final and applied nothing: the commit fails alone at once, without waiting out the
// budget. An attach whose claim it refuses claims nothing and throws UrsulaWriteRefusedError, and a
// read-only attach reads the database meanwhile. Without the marker a 403 says nothing about the
// stream (a filtering proxy can send one after the stream applied the write): the commit poisons, and
// the attach fails as before.
it("an authorizer's marked 403 fails a commit alone at once, and an attach it refuses throws UrsulaWriteRefusedError", async () => {
	const path = streamPath();
	const file = freshFile();
	const [refusing, accepting, unmarked] = [`${file}.refusing`, `${file}.accepting`, `${file}.unmarked`];
	const steps = ["CREATE TABLE t(x TEXT)", `@wait:${refusing}`, "INSERT INTO t VALUES ('d1')", "@status", `@wait:${accepting}`, "INSERT INTO t VALUES ('d2')", `@wait:${unmarked}`, "INSERT INTO t VALUES ('d3')"];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	await gate.refuse("write access denied", DENIED);
	writeFileSync(refusing, "");
	await child.waitFor((l) => l.step === 4 && l.phase === "start");
	await gate.refuse(null);
	writeFileSync(accepting, "");
	await child.waitFor((l) => l.step === 6 && l.phase === "start");
	await gate.refuse("write access denied", { status: 403 });
	writeFileSync(unmarked, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	expect(step(child, 2)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 3)?.status).toMatchObject({ poisoned: false, fenced: false, writes_refused: true, refused_kind: "forbidden", refused_reason: "write access denied" });
	expect(step(child, 5)?.ok).toBe(true);
	expect(step(child, 7)).toMatchObject({ ok: false, errcode: POISONED });
	expect(done).toMatchObject({ poisoned: true, fenced: false, writes_refused: false, refused_kind: null, attempts: [1, 1] });
	expect(done.reason).toMatch(/append \S+: 403/);
	expect(child.stderr()).not.toMatch(/event=append_retry/);

	await gate.refuse("write access denied", DENIED);
	const reader = freshFile();
	let error: unknown;
	try {
		attach(reader, gate.url + path);
	} catch (e) {
		error = e;
	}
	expect(error).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(error).not.toBeInstanceOf(UrsulaPaymentRequiredError);
	// The extension names the attach's failure `refused`, and its message what refused it.
	expect(error).toMatchObject({ name: "UrsulaWriteRefusedError", kind: "refused", action: "retry", reason: null });
	expect((error as Error).message).toMatch(/claim \S+: 403 write access denied: an authorizer denied it/);
	expect(errcode((error as Error).cause)).toBe(COMMIT_REFUSED);
	expect(() => openPlain(reader)).toThrow(/unable to open database file/);
	await gate.refuse("write access denied", { status: 403 });
	let unmarkedError: unknown;
	try {
		attach(reader, gate.url + path);
	} catch (e) {
		unmarkedError = e;
	}
	expect(unmarkedError).not.toBeInstanceOf(UrsulaWriteRefusedError);
	expect(errcode(unmarkedError)).toBe(SQLITE_ERROR);
	attach(reader, gate.url + path, { readOnly: true });
	expect(xs(reader)).toEqual(["d2"]);
});

// The refusal also shows in `instrument()`'s `sqlite_ursula.writes_refused` gauge: 1 while writes are
// refused, 0 once a commit succeeds.
it("Pi: a commit an authorizer refuses with a marked 403 rejects with UrsulaWriteRefusedError, and the storage commits again once it is allowed", async () => {
	const file = freshFile();
	const storage = await openUrsulaPiStorage(file, gate.url + streamPath());
	const { meter, observe } = recordingMeter();
	const stop = instrument(file, meter, { intervalMs: 60_000 });
	expect(observe("sqlite_ursula.writes_refused")).toBe(0);
	await gate.refuse("write access denied", DENIED);
	const error = await openHarness(storage).then(
		() => undefined,
		(e: unknown) => e,
	);
	expect(observe("sqlite_ursula.writes_refused")).toBe(1);
	await gate.refuse(null);
	expect(error).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(error).not.toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(error).not.toBeInstanceOf(UrsulaReplicationError);
	expect(error).toMatchObject({ name: "UrsulaWriteRefusedError", kind: "forbidden", action: "retry", reason: "write access denied" });
	const { harness, root } = await openHarness(storage);
	await textTurn(root, 1);
	expect(observe("sqlite_ursula.writes_refused")).toBe(0);
	stop();
	await harness.close(ctx);
	await storage.close(ctx);
});

// A transaction whose commit frame is larger than one request may be (over URSULA_VFS_MAX_FRAME_BYTES,
// 32 MiB by default, the server's own limit) is never sent: it fails alone as `too_large`, the file
// is not poisoned, and the next, smaller commit succeeds. Random blobs do not compress, so 40 MiB of
// them make a frame over the limit.
it("a transaction too large for one request fails alone without being sent, and the next commit succeeds", async () => {
	const path = streamPath();
	const file = freshFile();
	const [ready, checked] = [`${file}.ready`, `${file}.checked`];
	const big = Array.from({ length: 40 }, () => "INSERT INTO b VALUES (randomblob(1048576));").join(" ");
	const steps = [
		"CREATE TABLE t(x TEXT)",
		"CREATE TABLE b(x BLOB)",
		"INSERT INTO t VALUES ('a1')",
		"@status",
		`@wait:${ready}`,
		`BEGIN; ${big} COMMIT`,
		"@status",
		`@wait:${checked}`,
		"INSERT INTO t VALUES ('a2')",
	];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === 4 && l.phase === "start");
	const before = await gate.counts();
	writeFileSync(ready, "");
	await child.waitFor((l) => l.step === 7 && l.phase === "start");
	// Nothing went out while the transaction was refused.
	expect(await gate.counts()).toEqual(before);
	writeFileSync(checked, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);

	const accepted = step(child, 3)?.status;
	expect(step(child, 5)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 6)?.status).toMatchObject({
		poisoned: false,
		fenced: false,
		writes_refused: true,
		refused_kind: "too_large",
		refused_reason: expect.stringMatching(/^\d+-byte frame over the 33554432-byte frame limit$/),
		offset: accepted?.offset,
		epoch: accepted?.epoch,
	});
	expect(step(child, 8)?.ok).toBe(true);
	expect(done).toMatchObject({ poisoned: false, fenced: false, writes_refused: false, refused_kind: null, refused_reason: null });
	const log = child.stderr();
	expect(log).toMatch(/event=writes_refused file=\S+ stream=\S+ during=commit kind=too_large reason="\d+-byte commit frame over the 33554432-byte frame limit/);
	expect(log).not.toMatch(/event=poisoned/);

	// The stream holds the accepted commits alone.
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["a1", "a2"]);
	const db = openPlain(copy);
	try {
		expect(db.prepare("SELECT count(*) AS n FROM b").get()).toEqual({ n: 0 });
	} finally {
		db.close();
	}
	// The refused transaction spilled into the WAL before COMMIT (40 MiB, far over SQLite's page
	// cache), and its rollback dropped those frames: the owner's own files are whole, attach resumes
	// from them, and they hold the same rows.
	attach(file, ursulaUrl() + path);
	expect(status(file)).toMatchObject({ poisoned: false, writes_refused: false, local: done.offset });
	const own = openPlain(file);
	try {
		expect(integrity(own)).toBe("ok");
		expect(own.prepare("SELECT count(*) AS n FROM b").get()).toEqual({ n: 0 });
	} finally {
		own.close();
	}
	expect(xs(file)).toEqual(["a1", "a2"]);
});

it("URSULA_VFS_MAX_FRAME_BYTES lowers the frame limit for a smaller limit in front of the stream", async () => {
	const path = streamPath();
	const file = freshFile();
	const [ready, checked] = [`${file}.ready`, `${file}.checked`];
	// 2 MiB of random bytes in one transaction, over a 1 MiB limit.
	const steps = [
		"CREATE TABLE b(x BLOB)",
		`@wait:${ready}`,
		"BEGIN; INSERT INTO b VALUES (randomblob(1048576)); INSERT INTO b VALUES (randomblob(1048576)); COMMIT",
		"@status",
		`@wait:${checked}`,
		"INSERT INTO b VALUES (randomblob(1024))",
	];
	const child = runChild(file, gate.url + path, steps, { URSULA_VFS_MAX_FRAME_BYTES: "1048576", CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	const before = await gate.counts();
	writeFileSync(ready, "");
	await child.waitFor((l) => l.step === 4 && l.phase === "start");
	// Nothing went out while the transaction was refused.
	expect(await gate.counts()).toEqual(before);
	writeFileSync(checked, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	expect(step(child, 2)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 3)?.status).toMatchObject({
		poisoned: false,
		refused_kind: "too_large",
		refused_reason: expect.stringMatching(/^\d+-byte frame over the 1048576-byte frame limit$/),
	});
	expect(step(child, 5)?.ok).toBe(true);
	expect(done).toMatchObject({ poisoned: false, writes_refused: false });
});
