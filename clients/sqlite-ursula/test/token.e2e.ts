// `open()`'s token provider against the e2e server (test/stack.ts): the package calls it before
// the open's first request, again ahead of each token's expiry and at once after a 401, shares one
// call between concurrent opens, stops once no binding uses it or another open's token replaced
// it, and never keeps the process alive. Its failures are typed. Neither server checks tokens, so
// the suite shows when the package calls the provider and what it makes of the answers. That
// requests carry the token the package applies is the extension's (`ursula_set_token`).
import { spawn } from "node:child_process";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { type BearerToken, loadUrsulaVfs, open, UrsulaSetupError, UrsulaUnavailableError } from "../src/index.ts";
import { ext, fault, freshDir, onModel, rejection, retired, stream } from "./kit.ts";

loadUrsulaVfs(ext());

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * A provider of tokens that last `lifetimeMs`, and when each of its calls started. `before` runs
 * first in each call, with the call's number from 1: it may wait, or throw to fail the call.
 */
function provider(lifetimeMs: number, before: (n: number) => void | Promise<void> = () => {}): { fn: () => Promise<BearerToken>; calls: number[] } {
	const calls: number[] = [];
	const fn = async (): Promise<BearerToken> => {
		calls.push(Date.now());
		const n = calls.length;
		await before(n);
		return { token: `token-${n}`, expiresAt: Date.now() + lifetimeMs };
	};
	return { fn, calls };
}

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
	it("is called before the open's first request, again ahead of each expiry, and no more once no binding uses it", async () => {
		const s = stream("token-refresh");
		const p = provider(3_000);
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
		const path = db.boundPath;
		db.close();
		await retired(path);
		const calls = p.calls.length;
		await sleep(3_000);
		expect(p.calls).toHaveLength(calls);
	});

	it("is called once for concurrent opens, and a later open reuses its token", async () => {
		const dir = freshDir();
		const p = provider(3_600_000, () => sleep(300));
		const dbs = await Promise.all([1, 2, 3].map((n) => open(stream(`token-shared-${n}`).url, { dir, token: p.fn })));
		expect(p.calls).toHaveLength(1);
		dbs.push(await open(stream("token-shared-later").url, { dir, token: p.fn }));
		expect(p.calls).toHaveLength(1);
		for (const db of dbs) db.close();
	});

	it.runIf(onModel())("is called again at once when a request is refused with 401", async () => {
		const p = provider(3_600_000);
		const db = await open(stream("token-401").url, { dir: freshDir(), token: p.fn });
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
		db.close();
	});

	it("fails the open with its own error when it fails for good, and is retried after any other failure", async () => {
		const refused = new UrsulaUnavailableError("unauthorized: the exchange refused the key", "unauthorized");
		expect(refused.action).toBe("fix");
		const bad = provider(60_000, () => {
			throw refused;
		});
		expect(await rejection(open(stream("token-refused").url, { dir: freshDir(), token: bad.fn }))).toBe(refused);
		expect(bad.calls).toHaveLength(1);

		const flaky = provider(3_600_000, (n) => {
			if (n < 3) throw new Error(`the exchange is down (${n})`);
		});
		const db = await open(stream("token-flaky").url, { dir: freshDir(), token: flaky.fn });
		expect(flaky.calls).toHaveLength(3);
		db.close();
	});

	it("fails the open at its signal with `unauthorized`, `retry`, when it handed out no token", async () => {
		const down = new Error("the exchange is down");
		const p = provider(60_000, () => {
			throw down;
		});
		const error = await rejection(open(stream("token-down").url, { dir: freshDir(), token: p.fn, signal: AbortSignal.timeout(2_000) }));
		expect(error).toBeInstanceOf(UrsulaUnavailableError);
		expect(error).toMatchObject({ kind: "unauthorized", action: "retry" });
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
		];
		for (const answer of answers) {
			const error = await rejection(open(stream("token-unusable").url, { dir: freshDir(), token: async () => answer as BearerToken }));
			expect(error, JSON.stringify(answer)).toBeInstanceOf(UrsulaSetupError);
			expect(error).toMatchObject({ kind: "invalid_argument", action: "fix" });
		}
	});

	it("stops once an open with a string token replaced it", async () => {
		const dir = freshDir();
		const p = provider(2_000);
		const a = await open(stream("token-then-string").url, { dir, token: p.fn });
		const b = await open(stream("string-after-token").url, { dir, token: "a-string-token" });
		const calls = p.calls.length;
		await sleep(3_000);
		expect(p.calls).toHaveLength(calls);
		a.close();
		b.close();
	});

	it("hands the bindings it served to the provider of a later open", async () => {
		const dir = freshDir();
		const first = provider(2_000);
		const second = provider(2_000);
		const a = await open(stream("token-first").url, { dir, token: first.fn });
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
		a.close();
	});

	it("never keeps the process alive: an idle process exits on its own", async () => {
		const r = await child(stream("token-idle").url, freshDir());
		expect(r.out).toBe("token\n");
		expect(r.code).toBe(0);
		expect(r.ms).toBeLessThan(2_000);
	});
});
