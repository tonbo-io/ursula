// `open()`'s token provider against the e2e server (test/stack.ts): the package calls it before
// the open's first request, again ahead of each token's expiry and at once after a 401, shares one
// call between concurrent opens, whatever functions they passed, stops once no binding of the thread
// is bound or an open with a string replaced it, and never keeps the process alive. Each call is
// handed the token held as `previous`, and an answer equal to it is retried. Its failures are typed. The 401s are scripted faults (the
// model's, or test/gate.mjs's in front of Ursula), since neither server checks tokens. In front of
// Ursula the gate records each request's `Authorization` header (`sentTokens`), so there the suite
// also checks which token each request carried. The token is one per process, so each test closes
// its databases and waits until the provider stopped (`closeAll`): the next test starts with none held.
import { spawn } from "node:child_process";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { type BearerToken, loadUrsulaVfs, open, type TokenProvider, type UrsulaDatabase, UrsulaSetupError, UrsulaUnavailableError } from "../src/index.ts";
import { ext, fault, freshDir, rejection, retired, sentTokens, stream } from "./kit.ts";

loadUrsulaVfs(ext());

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * A provider of tokens `<label>-<n>` that last `lifetimeMs`, when each of its calls started, and the
 * `previous` each was handed. `before` runs first in each call, with the call's number from 1: it
 * may wait, or throw to fail the call.
 */
function provider(
	label: string,
	lifetimeMs: number,
	before: (n: number) => void | Promise<void> = () => {},
): { fn: TokenProvider; calls: number[]; previous: (string | undefined)[] } {
	const calls: number[] = [];
	const previous: (string | undefined)[] = [];
	const fn: TokenProvider = async (options) => {
		calls.push(Date.now());
		previous.push(options.previous);
		const n = calls.length;
		await before(n);
		return { token: `${label}-${n}`, expiresAt: Date.now() + lifetimeMs };
	};
	return { fn, calls, previous };
}

/** The number `n` of the token `<label>-<n>` an `Authorization` header carries, or NaN. */
const nth = (label: string, authorization: string | null | undefined): number => {
	const prefix = `Bearer ${label}-`;
	return authorization?.startsWith(prefix) === true ? Number(authorization.slice(prefix.length)) : Number.NaN;
};

/** Waits until `done()` holds, for at most `ms`. */
async function until(what: string, done: () => boolean, ms = 10_000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!done()) {
		expect(Date.now(), what).toBeLessThan(deadline);
		await sleep(20);
	}
}

/** Closes `dbs` and waits until each retired and the pump told the provider, which stops then. */
async function closeAll(...dbs: UrsulaDatabase[]): Promise<void> {
	for (const db of dbs) db.close();
	for (const db of dbs) await retired(db.boundPath);
	await sleep(500);
}

/** Runs test/api-child.ts in `token` mode: its output, exit code and how long it took to exit after its output. */
function child(url: string, dir: string): Promise<{ out: string; code: number | null; ms: number }> {
	let started = Date.now();
	const proc = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "api-child.ts"), url, dir, "token", ""], { stdio: ["ignore", "pipe", "inherit"] });
	let out = "";
	proc.stdout.on("data", (chunk: Buffer) => {
		out += chunk.toString();
		started = Date.now();
	});
	return new Promise((resolve) => proc.once("exit", (code) => resolve({ out, code, ms: Date.now() - started })));
}

describe("a token provider", () => {
	// First, while no other binding of the suite sends requests that could take the scripted 401s.
	it("is called again for the next open after an open was refused with 401", async () => {
		const p = provider("bind", 3_600_000);
		const refused = stream("token-bind-401");
		// The open's first request and its resend with the token read again are both refused: the
		// bind blocks `unauthorized`, and the extension reports no condition for a binding not bound.
		await fault("status 401", 2);
		const error = await rejection(open(refused.url, { dir: freshDir(), token: p.fn, signal: AbortSignal.timeout(3_000) }));
		expect(error).toBeInstanceOf(UrsulaUnavailableError);
		expect(error).toMatchObject({ kind: "unauthorized", action: "fix" });
		// The refused open dropped the token and asked for the next one, which the next open uses.
		await until("a call after the refused open", () => p.calls.length === 2);
		// The first call held no token, and the call after the 401 is handed the refused one.
		expect(p.previous).toEqual([undefined, "bind-1"]);
		const next = stream("token-bind-401-next");
		const db = await open(next.url, { dir: freshDir(), token: p.fn });
		expect(p.calls).toHaveLength(2);
		const before = await sentTokens(refused.path);
		const after = await sentTokens(next.path);
		if (before !== undefined && after !== undefined) {
			expect(before.length).toBeGreaterThanOrEqual(2);
			expect(before.every((r) => r.authorization === "Bearer bind-1")).toBe(true);
			expect(after.length).toBeGreaterThan(0);
			expect(after.every((r) => r.authorization === "Bearer bind-2")).toBe(true);
		}
		await closeAll(db);
	});

	it("is called before the open's first request, again ahead of each expiry, and no more once no binding uses it", async () => {
		const s = stream("token-refresh");
		const p = provider("refresh", 3_000);
		const db = await open(s.url, { dir: freshDir(), token: p.fn });
		expect(p.calls).toHaveLength(1);
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		// After 70 to 80% of each 3 s token: 2.1 to 2.4 s after it arrived.
		await until("the first refresh", () => p.calls.length >= 2, 5_000);
		await until("the second refresh", () => p.calls.length >= 3, 5_000);
		const [first, second] = p.calls as [number, number];
		expect(second - first).toBeGreaterThanOrEqual(2_000);
		// No token before the first call, then each scheduled refresh is handed the current token.
		expect(p.previous.slice(0, 3)).toEqual([undefined, "refresh-1", "refresh-2"]);
		db.exec("INSERT INTO t VALUES (2)");
		await db.synced();
		const sent = await sentTokens(s.path);
		if (sent !== undefined) {
			// The open's first request carries the first token, and the append after the refreshes a later one.
			expect(sent[0]?.authorization).toBe("Bearer refresh-1");
			expect(sent.every((r) => nth("refresh", r.authorization) >= 1)).toBe(true);
			expect(nth("refresh", sent.filter((r) => r.method === "POST").at(-1)?.authorization)).toBeGreaterThanOrEqual(3);
		}
		const path = db.boundPath;
		db.close();
		await retired(path);
		const calls = p.calls.length;
		await sleep(3_000);
		expect(p.calls).toHaveLength(calls);
	});

	it("is called once for concurrent opens, and a later open with the same function reuses its token", async () => {
		const dir = freshDir();
		const p = provider("shared", 3_600_000, () => sleep(300));
		const streams = [1, 2, 3, 4].map((n) => stream(`token-shared-${n}`));
		const dbs = await Promise.all(streams.slice(0, 3).map((s) => open(s.url, { dir, token: p.fn })));
		expect(p.calls).toHaveLength(1);
		dbs.push(await open((streams[3] as { url: string }).url, { dir, token: p.fn }));
		expect(p.calls).toHaveLength(1);
		for (const s of streams) {
			const sent = await sentTokens(s.path);
			if (sent !== undefined) expect(sent[0]?.authorization).toBe("Bearer shared-1");
		}
		await closeAll(...dbs);
	});

	it("serves concurrent opens with different functions from one call, and runs the last one next", async () => {
		const dir = freshDir();
		const called: string[] = [];
		// A new function on each call, as an inline arrow is.
		const inline = (label: string) => async (): Promise<BearerToken> => {
			called.push(label);
			await sleep(300);
			return { token: `inline-${label}`, expiresAt: Date.now() + 2_000 };
		};
		const streams = ["a", "b", "c"].map((label) => ({ label, s: stream(`token-inline-${label}`) }));
		const dbs = await Promise.all(streams.map(({ label, s }) => open(s.url, { dir, token: inline(label) })));
		// The first open's call was in flight when the others named their functions: it served all three.
		expect(called).toEqual(["a"]);
		for (const { s } of streams) {
			const sent = await sentTokens(s.path);
			if (sent !== undefined) expect(sent[0]?.authorization).toBe("Bearer inline-a");
		}
		// The refresh runs the function the last open named.
		await until("a refresh", () => called.length >= 2, 5_000);
		expect(called[1]).toBe("c");
		await closeAll(...dbs);
	});

	it("serves the token it holds at once, expired too, while its refresh fails", async () => {
		const s = stream("token-held");
		const dir = freshDir();
		const p = provider("held", 1_500, (n) => {
			if (n > 1) throw new Error("the exchange is down");
		});
		const db = await open(s.url, { dir, token: p.fn });
		await until("a failed refresh", () => p.calls.length >= 2, 5_000);
		// Past the token's expiry: a connection on the bound binding still needs no network.
		await sleep(1_000);
		const started = Date.now();
		const again = await open(s.url, { dir, token: p.fn, signal: AbortSignal.timeout(5_000) });
		expect(Date.now() - started).toBeLessThan(1_000);
		await closeAll(again, db);
	});

	it("waits for the refresh in flight rather than serve an expired token while it is healthy", async () => {
		const dir = freshDir();
		// Every refresh takes 2 s, longer than what is left of the 1.5 s token when it starts.
		const p = provider("slow", 1_500, (n) => (n > 1 ? sleep(2_000) : undefined));
		const a = await open(stream("token-slow-a").url, { dir, token: p.fn });
		await until("the refresh in flight", () => p.calls.length >= 2, 5_000);
		await sleep(600);
		// The token held expired and the refresh has not answered: the open waits for it.
		const b = stream("token-slow-b");
		const db = await open(b.url, { dir, token: p.fn, signal: AbortSignal.timeout(5_000) });
		expect(p.calls).toHaveLength(2);
		const sent = await sentTokens(b.path);
		if (sent !== undefined) {
			expect(sent.length).toBeGreaterThan(0);
			expect(sent[0]?.authorization).toBe("Bearer slow-2");
		}
		await closeAll(db, a);
	});

	it("is called again at once when a request is refused with 401", async () => {
		const s = stream("token-401");
		const p = provider("refused", 3_600_000);
		const db = await open(s.url, { dir: freshDir(), token: p.fn });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		expect(p.calls).toHaveLength(1);
		// The append and its resend with the token read again are both refused: replication blocks
		// `unauthorized`, long before the token's refresh is due.
		await fault("status 401", 2, "POST");
		db.exec("INSERT INTO t VALUES (1)");
		await until("a call after the 401", () => p.calls.length === 2);
		// The blocked append goes out again within 30 s, under the new token.
		await db.synced();
		expect(p.calls).toHaveLength(2);
		// The call after the 401 is handed the refused token.
		expect(p.previous).toEqual([undefined, "refused-1"]);
		const sent = await sentTokens(s.path);
		if (sent !== undefined) {
			const posts = sent.filter((r) => r.method === "POST").map((r) => r.authorization);
			// The append refused twice went out under the first token, and its resend under the second.
			expect(posts.at(-1)).toBe("Bearer refused-2");
			expect(posts.slice(-3, -1)).toEqual(["Bearer refused-1", "Bearer refused-1"]);
		}
		await closeAll(db);
	});

	it("is retried when it hands back the refused token after a 401, and its next new token unblocks replication", async () => {
		const s = stream("token-401-cached");
		const previous: (string | undefined)[] = [];
		// A cache that hands its token out again, refused or not, until its second refill.
		const cached: TokenProvider = async (options) => {
			previous.push(options.previous);
			return { token: previous.length <= 2 ? "cached-1" : "cached-2", expiresAt: Date.now() + 3_600_000 };
		};
		const db = await open(s.url, { dir: freshDir(), token: cached });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		await fault("status 401", 2, "POST");
		db.exec("INSERT INTO t VALUES (1)");
		// The second call hands back the refused token: a failed call, retried after 0.5 to 1 s.
		await until("a retry after the refused token came back", () => previous.length === 3);
		await db.synced();
		expect(previous).toEqual([undefined, "cached-1", "cached-1"]);
		const sent = await sentTokens(s.path);
		if (sent !== undefined) {
			const posts = sent.filter((r) => r.method === "POST").map((r) => r.authorization);
			// The refused token never went out again: the blocked append's resend carried the new one.
			expect(posts.at(-1)).toBe("Bearer cached-2");
			expect(posts.slice(-3, -1)).toEqual(["Bearer cached-1", "Bearer cached-1"]);
		}
		await closeAll(db);
	});

	it("fails the open with its own error when it fails for good, and is retried after any other failure", async () => {
		const refused = new UrsulaUnavailableError("unauthorized: the exchange refused the key", "unauthorized");
		expect(refused.action).toBe("fix");
		const bad = provider("bad", 60_000, () => {
			throw refused;
		});
		expect(await rejection(open(stream("token-refused").url, { dir: freshDir(), token: bad.fn }))).toBe(refused);
		expect(bad.calls).toHaveLength(1);

		const flaky = provider("flaky", 3_600_000, (n) => {
			if (n < 3) throw new Error(`the exchange is down (${n})`);
		});
		const db = await open(stream("token-flaky").url, { dir: freshDir(), token: flaky.fn });
		expect(flaky.calls).toHaveLength(3);
		await closeAll(db);
	});

	it("fails the open at once with `unauthorized`, `fix`, when it throws another error that says `fix`, as that error's cause", async () => {
		// Any object whose action is `fix`, as an error of a library that cannot extend UrsulaError.
		const revoked = { action: "fix", message: "the key was revoked" };
		const p = provider("revoked", 60_000, () => {
			throw revoked;
		});
		const started = Date.now();
		const error = await rejection(open(stream("token-revoked").url, { dir: freshDir(), token: p.fn, signal: AbortSignal.timeout(10_000) }));
		expect(Date.now() - started).toBeLessThan(5_000);
		expect(error).toBeInstanceOf(UrsulaUnavailableError);
		expect(error).toMatchObject({ kind: "unauthorized", action: "fix" });
		expect((error as UrsulaUnavailableError).cause).toBe(revoked);
		expect((error as UrsulaUnavailableError).message).toContain("the key was revoked");
		// Failed for good: no retry until the next open.
		await sleep(2_000);
		expect(p.calls).toHaveLength(1);
	});

	it("fails the open at its signal with `unavailable`, `retry`, when it handed out no token", async () => {
		const down = new Error("the exchange is down");
		const p = provider("down", 60_000, () => {
			throw down;
		});
		const error = await rejection(open(stream("token-down").url, { dir: freshDir(), token: p.fn, signal: AbortSignal.timeout(2_000) }));
		expect(error).toBeInstanceOf(UrsulaUnavailableError);
		expect(error).toMatchObject({ kind: "unavailable", action: "retry" });
		expect((error as UrsulaUnavailableError).cause).toBe(down);
		expect(p.calls.length).toBeGreaterThan(1);
	});

	it("fails the open with `invalid_argument` when it hands out a token that cannot be sent", async () => {
		const answers: unknown[] = [
			{ token: "two words", expiresAt: Date.now() + 60_000 },
			{ token: "", expiresAt: Date.now() + 60_000 },
			{ token: "café", expiresAt: Date.now() + 60_000 },
			{ token: "token", expiresAt: "soon" },
			{ token: "token", expiresAt: new Date(Number.NaN) },
			// Seconds since the epoch, as a JWT's exp is.
			{ token: "token", expiresAt: Math.floor(Date.now() / 1000) + 3600 },
		];
		for (const answer of answers) {
			const error = await rejection(open(stream("token-unusable").url, { dir: freshDir(), token: async () => answer as BearerToken }));
			expect(error, JSON.stringify(answer)).toBeInstanceOf(UrsulaSetupError);
			expect(error).toMatchObject({ kind: "invalid_argument", action: "fix" });
		}
	});

	it("stops once an open with a string token replaced it, and the opens waiting for it go on under the string", async () => {
		const dir = freshDir();
		const p = provider("then-string", 2_000, () => sleep(500));
		const waiting = stream("token-then-string");
		const [a, b] = await Promise.all([open(waiting.url, { dir, token: p.fn }), open(stream("string-after-token").url, { dir, token: "a-string-token" })]);
		const sent = await sentTokens(waiting.path);
		if (sent !== undefined) expect(sent[0]?.authorization).toBe("Bearer a-string-token");
		// The string open stopped the provider in the same tick, before its call started: it never ran.
		expect(p.calls).toHaveLength(0);
		await sleep(3_000);
		expect(p.calls).toHaveLength(0);
		await closeAll(a, b);
	});

	it("hands the bindings it served to the provider of a later open, which shares the token held", async () => {
		const dir = freshDir();
		const first = provider("first", 2_000);
		const second = provider("second", 2_000);
		const s = stream("token-first");
		const a = await open(s.url, { dir, token: first.fn });
		const b = await open(stream("token-second").url, { dir, token: second.fn });
		// The token held served the second open: no call of the second function yet.
		expect(second.calls).toHaveLength(0);
		const path = b.boundPath;
		b.close();
		await retired(path);
		// `a` still needs a fresh token: the second provider keeps refreshing, the first never runs again.
		await until("a refresh for the first binding", () => second.calls.length >= 3, 8_000);
		expect(first.calls).toHaveLength(1);
		a.exec("CREATE TABLE t(x)");
		await a.synced();
		const sent = await sentTokens(s.path);
		if (sent !== undefined) expect(nth("second", sent.filter((r) => r.method === "POST").at(-1)?.authorization)).toBeGreaterThanOrEqual(3);
		await closeAll(a);
	});

	it("keeps refreshing while a binding opened with a string token is bound", async () => {
		const dir = freshDir();
		const p = provider("any", 2_000);
		const s = stream("token-string-bound");
		const a = await open(s.url, { dir, token: "a-string-token" });
		const b = await open(stream("token-provider-bound").url, { dir, token: p.fn });
		const path = b.boundPath;
		b.close();
		await retired(path);
		// The provider's tokens are the process's, so `a` carries them now: it keeps them fresh.
		await until("a refresh for the binding opened with a string", () => p.calls.length >= 3, 8_000);
		a.exec("CREATE TABLE t(x)");
		await a.synced();
		const sent = await sentTokens(s.path);
		if (sent !== undefined) expect(nth("any", sent.filter((r) => r.method === "POST").at(-1)?.authorization)).toBeGreaterThanOrEqual(3);
		await closeAll(a);
	});

	it("never keeps the process alive: an idle process exits on its own", async () => {
		const r = await child(stream("token-idle").url, freshDir());
		expect(r.out).toBe("token\n");
		expect(r.code).toBe(0);
		expect(r.ms).toBeLessThan(2_000);
	});
});
