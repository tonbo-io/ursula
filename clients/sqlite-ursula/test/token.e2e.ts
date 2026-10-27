// `open()`'s token provider against the e2e server (test/stack.ts): the package calls it before
// the open's first request, again ahead of each token's expiry and at once after a 401, shares one
// call between concurrent opens, stops once no binding uses it or another open's token replaced
// it, and never keeps the process alive. Its failures are typed. The 401s are scripted faults (the
// model's, or test/gate.mjs's in front of Ursula), since neither server checks tokens. In front of
// Ursula the gate records each request's `Authorization` header (`sentTokens`), so there the suite
// also checks which token each request carried.
import { spawn } from "node:child_process";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { type BearerToken, loadUrsulaVfs, open, UrsulaSetupError, UrsulaUnavailableError } from "../src/index.ts";
import { ext, fault, freshDir, rejection, retired, sentTokens, stream } from "./kit.ts";

loadUrsulaVfs(ext());

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * A provider of tokens `<label>-<n>` that last `lifetimeMs`, and when each of its calls started.
 * `before` runs first in each call, with the call's number from 1: it may wait, or throw to fail the call.
 */
function provider(label: string, lifetimeMs: number, before: (n: number) => void | Promise<void> = () => {}): { fn: () => Promise<BearerToken>; calls: number[] } {
	const calls: number[] = [];
	const fn = async (): Promise<BearerToken> => {
		calls.push(Date.now());
		const n = calls.length;
		await before(n);
		return { token: `${label}-${n}`, expiresAt: Date.now() + lifetimeMs };
	};
	return { fn, calls };
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
		db.close();
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
		for (const db of dbs) db.close();
	});

	it("serves concurrent opens with different functions the last one's token", async () => {
		const dir = freshDir();
		const called: string[] = [];
		// A new function on each call, as an inline arrow is.
		const inline = (label: string) => async (): Promise<BearerToken> => {
			called.push(label);
			await sleep(300);
			return { token: `inline-${label}`, expiresAt: Date.now() + 3_600_000 };
		};
		const streams = ["a", "b", "c"].map((label) => ({ label, s: stream(`token-inline-${label}`) }));
		const dbs = await Promise.all(streams.map(({ label, s }) => open(s.url, { dir, token: inline(label) })));
		// Each open replaced the provider before the last one's call answered: that call served all three.
		expect(called).toEqual(["a", "b", "c"]);
		for (const { s } of streams) {
			const sent = await sentTokens(s.path);
			if (sent !== undefined) expect(sent[0]?.authorization).toBe("Bearer inline-c");
		}
		for (const db of dbs) db.close();
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
		again.close();
		db.close();
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
		const sent = await sentTokens(s.path);
		if (sent !== undefined) {
			const posts = sent.filter((r) => r.method === "POST").map((r) => r.authorization);
			// The append refused twice went out under the first token, and its resend under the second.
			expect(posts.at(-1)).toBe("Bearer refused-2");
			expect(posts.slice(-3, -1)).toEqual(["Bearer refused-1", "Bearer refused-1"]);
		}
		db.close();
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
		db.close();
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
		const calls = p.calls.length;
		expect(calls).toBe(1);
		await sleep(3_000);
		expect(p.calls).toHaveLength(calls);
		a.close();
		b.close();
	});

	it("hands the bindings it served to the provider of a later open", async () => {
		const dir = freshDir();
		const first = provider("first", 2_000);
		const second = provider("second", 2_000);
		const s = stream("token-first");
		const a = await open(s.url, { dir, token: first.fn });
		const b = await open(stream("token-second").url, { dir, token: second.fn });
		const path = b.boundPath;
		b.close();
		await retired(path);
		// `a` still needs a fresh token: the second provider keeps refreshing, the first never runs again.
		const calls = first.calls.length;
		await until("a refresh for the first binding", () => second.calls.length >= 3, 6_000);
		expect(first.calls).toHaveLength(calls);
		a.exec("CREATE TABLE t(x)");
		await a.synced();
		const sent = await sentTokens(s.path);
		if (sent !== undefined) expect(nth("second", sent.filter((r) => r.method === "POST").at(-1)?.authorization)).toBeGreaterThanOrEqual(3);
		a.close();
	});

	it("never keeps the process alive: an idle process exits on its own", async () => {
		const r = await child(stream("token-idle").url, freshDir());
		expect(r.out).toBe("token\n");
		expect(r.code).toBe(0);
		expect(r.ms).toBeLessThan(2_000);
	});
});
