// `@tonbo/sqlite-ursula/tonbo`'s `open` by name, through the real open against the e2e server
// (test/stack.ts). test/tonbo-api.ts stands in for the Management API and the token endpoint, as
// their published contracts document them, and serves databases whose URLs are streams of the
// e2e server. In front of Ursula, the gate records which token each stream request carried. The
// process holds one token across providers, so each test waits until the bindings it opened retired
// and the provider stopped: the next test starts with none held.
import { spawn } from "node:child_process";
import { chmodSync, mkdirSync, symlinkSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs } from "../src/index.ts";
import { open as openByName, TonboError, UrsulaSetupError, UrsulaUnavailableError } from "../src/tonbo.ts";
import { ext, freshDir, rejection, retired, sentTokens, stream } from "./kit.ts";
import { type FixtureDatabase, TonboApi } from "./tonbo-api.ts";

loadUrsulaVfs(ext());

/** The examples of the Tonbo CLI's credentials contract, version 1, as it lists them. */
const USABLE_LOGINS: readonly Record<string, unknown>[] = [
	{ version: 1, account_id: "10000000-0000-4000-8000-000000000001", key_id: "20000000-0000-4000-8000-000000000002", key: "tbo_secret", expires_at: "2027-04-24T00:00:00.000Z" },
	{ version: 1, account_id: "10000000-0000-4000-8000-000000000001", key_id: "20000000-0000-4000-8000-000000000002", key: "tbo_secret", expires_at: null },
];
const UNUSABLE_LOGINS: readonly Record<string, unknown>[] = [
	{ access_token: "access", refresh_token: "refresh", token_type: "bearer" },
	{ version: 2, account_id: "10000000-0000-4000-8000-000000000001", key_id: "20000000-0000-4000-8000-000000000002", key: "tbo_secret", expires_at: null },
	{ version: 1, account_id: "10000000-0000-4000-8000-000000000001", key_id: "20000000-0000-4000-8000-000000000002", key: "not-a-personal-key", expires_at: null },
	{ version: 1, account_id: "", key_id: "20000000-0000-4000-8000-000000000002", key: "tbo_secret", expires_at: null },
	{ version: 1, account_id: "10000000-0000-4000-8000-000000000001", key_id: "20000000-0000-4000-8000-000000000002", key: "tbo_secret", expires_at: "not a date" },
	{ version: 1, account_id: "10000000-0000-4000-8000-000000000001", key_id: "20000000-0000-4000-8000-000000000002", key: "tbo_secret" },
];

/** The variables the tests set: each test starts with none of them but the origins and a config home of its own. */
const VARIABLES = ["TONBO_API_KEY", "TONBO_API_ORIGIN", "TONBO_AUTH_ORIGIN", "XDG_CONFIG_HOME", "URSULA_VFS_TOKEN_FILE"] as const;

let api: TonboApi;
let saved: Record<string, string | undefined> = {};
/** The paths the test bound, which it closes: each retires before the next test. */
const bound: string[] = [];

/** The `/tonbo` entry's `open`, recording the path it bound. */
const open: typeof openByName = async (nameOrUrl, options) => {
	const db = await openByName(nameOrUrl, options);
	bound.push(db.boundPath);
	return db;
};

beforeAll(async () => {
	api = await TonboApi.start();
});
afterAll(() => api.stop());
beforeEach(() => {
	saved = Object.fromEntries(VARIABLES.map((v) => [v, process.env[v]]));
	for (const v of VARIABLES) delete process.env[v];
	// No `tonbo login` unless a test writes one.
	process.env.XDG_CONFIG_HOME = freshDir();
	process.env.TONBO_API_ORIGIN = api.origin;
	process.env.TONBO_AUTH_ORIGIN = api.origin;
	api.reset();
});
afterEach(async () => {
	const paths = bound.splice(0);
	for (const path of paths) await retired(path);
	// The pump delivers the retirements, and the provider stops: the next test holds no token.
	if (paths.length > 0) await new Promise((r) => setTimeout(r, 500));
	for (const v of VARIABLES) {
		const value = saved[v];
		if (value === undefined) delete process.env[v];
		else process.env[v] = value;
	}
});

/** A database of the stand-in on a stream of its own: its name, URL and stream path. */
function database(label: string, state: FixtureDatabase["state"] = "active", extra: Partial<FixtureDatabase> = {}): { name: string; url: string; path: string } {
	const s = stream(label);
	const name = s.path.split("/").at(-1) as string;
	api.databases.set(name, { url: s.url, state, ...extra });
	return { name, ...s };
}

/** Writes the login file `tonbo login` saves, with `mode`: its path. */
function login(content: unknown, mode = 0o600): string {
	const dir = join(process.env.XDG_CONFIG_HOME as string, "tonbo");
	mkdirSync(dir, { recursive: true, mode: 0o700 });
	const path = join(dir, "credentials.json");
	writeFileSync(path, JSON.stringify(content), { mode });
	chmodSync(path, mode);
	return path;
}

/** A server that answers every request with `status` and `headers`, and records each as `METHOD path`. */
async function bystander(status: number, headers: Record<string, string> = {}): Promise<{ origin: string; hits: string[]; stop: () => Promise<void> }> {
	const hits: string[] = [];
	const server = createServer((req, res) => {
		hits.push(`${req.method} ${req.url}`);
		req.resume();
		res.writeHead(status, headers).end();
	});
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const stop = async (): Promise<void> => {
		server.closeAllConnections();
		await new Promise((resolve) => server.close(resolve));
	};
	return { origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, hits, stop };
}

/** The error an open rejects with: a {@link UrsulaSetupError} `invalid_argument` whose message ends with `fix`. */
async function invalidArgument(promise: Promise<unknown>, fix: string): Promise<UrsulaSetupError> {
	const error = await rejection(promise);
	expect(error).toBeInstanceOf(UrsulaSetupError);
	expect(error).toMatchObject({ kind: "invalid_argument", action: "fix" });
	expect((error as UrsulaSetupError).message.endsWith(fix)).toBe(true);
	return error as UrsulaSetupError;
}

/** The error an open rejects with: a {@link TonboError} of `kind`, action `fix`, whose message ends with `fix`. */
async function tonboError(promise: Promise<unknown>, kind: TonboError["kind"], fix: string): Promise<TonboError> {
	const error = await rejection(promise);
	expect(error).toBeInstanceOf(TonboError);
	expect(error).toMatchObject({ kind, action: "fix", fix });
	expect((error as TonboError).message.endsWith(fix)).toBe(true);
	return error as TonboError;
}

describe("open by name", () => {
	it("looks the name up with the API key and opens the stream with a Streams token exchanged for it", async () => {
		const d = database("tonbo-name");
		const dir = freshDir();
		const db = await open(d.name, { dir, apiKey: "tbo_name" });
		expect(db.name).toBe(d.path);
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		expect(api.of("management")).toMatchObject([{ name: d.name, authorization: "Bearer tbo_name" }]);
		const exchanges = api.of("token");
		expect(exchanges).toMatchObject([{ authorization: "Bearer tbo_name", audience: "https://streams.tonbo.dev" }]);
		const issued = exchanges[0]?.issued as string;
		const sent = await sentTokens(d.path);
		if (sent !== undefined) {
			expect(sent.length).toBeGreaterThan(0);
			expect(sent.every((r) => r.authorization === `Bearer ${issued}`)).toBe(true);
		}
		// A later open looks the name up again and reuses the Streams token.
		const again = await open(d.name, { dir, apiKey: "tbo_name" });
		expect(again.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		expect(api.of("management")).toHaveLength(2);
		expect(api.of("token")).toHaveLength(1);
		again.close();
		db.close();
	});

	it("exchanges an Agent key with no audience for the Management API, keeps that token, and never sends it a Streams token", async () => {
		const d = database("tonbo-agent");
		const dir = freshDir();
		// The answer names another token endpoint: the key never goes there.
		const elsewhere = await bystander(200);
		api.tokenEndpoint = `${elsewhere.origin}/api/iam/token`;
		const db = await open(d.name, { dir, apiKey: "agent_one" });
		expect(elsewhere.hits).toEqual([]);
		await elsewhere.stop();
		const management = api.of("token").filter((s) => s.audience === null);
		expect(management).toMatchObject([{ authorization: "Bearer agent_one" }]);
		const jwt = `Bearer ${management[0]?.issued as string}`;
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer agent_one", jwt]);
		expect(api.of("token").filter((s) => s.audience === "https://streams.tonbo.dev")).toHaveLength(1);
		const again = await open(d.name, { dir, apiKey: "agent_one" });
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer agent_one", jwt, jwt]);
		expect(api.of("token")).toHaveLength(2);
		expect(api.of("management").some((s) => s.authorization?.startsWith("Bearer streams-"))).toBe(false);
		again.close();
		db.close();
	});

	it("exchanges an Agent key again when the Management API refuses the token it holds, and retries once", async () => {
		const d = database("tonbo-agent-revoked");
		const dir = freshDir();
		(await open(d.name, { dir, apiKey: "agent_two" })).close();
		const first = api.of("token").find((s) => s.audience === null)?.issued as string;
		api.forget(first);
		(await open(d.name, { dir, apiKey: "agent_two" })).close();
		const exchanged = api.of("token").filter((s) => s.audience === null);
		expect(exchanged).toHaveLength(2);
		const second = exchanged[1]?.issued as string;
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer agent_two", `Bearer ${first}`, `Bearer ${first}`, `Bearer ${second}`]);
	});

	it("takes the key from apiKey, then TONBO_API_KEY, then the tonbo login file", async () => {
		const d = database("tonbo-sources");
		const dir = freshDir();
		login(USABLE_LOGINS[1]);
		process.env.TONBO_API_KEY = "tbo_env";
		(await open(d.name, { dir, apiKey: "tbo_option" })).close();
		(await open(d.name, { dir })).close();
		delete process.env.TONBO_API_KEY;
		(await open(d.name, { dir })).close();
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer tbo_option", "Bearer tbo_env", "Bearer tbo_secret"]);
	});

	it("reads the contract's usable logins, and refuses its unusable and expired ones with invalid_api_key", async () => {
		const d = database("tonbo-logins");
		const dir = freshDir();
		for (const content of USABLE_LOGINS) {
			login(content);
			const expiresAt = content.expires_at;
			if (typeof expiresAt === "string" && Date.parse(expiresAt) <= Date.now()) {
				await tonboError(open(d.name, { dir }), "invalid_api_key", "Run: tonbo login");
				continue;
			}
			(await open(d.name, { dir })).close();
			expect(api.of("management").at(-1)?.authorization).toBe("Bearer tbo_secret");
		}
		api.reset();
		for (const content of [...UNUSABLE_LOGINS, { ...USABLE_LOGINS[1], expires_at: "2020-01-01T00:00:00.000Z" }]) {
			login(content);
			const error = await tonboError(open(d.name, { dir }), "invalid_api_key", "Run: tonbo login");
			expect(error.message).not.toContain("tbo_secret");
		}
		expect(api.seen).toEqual([]);
	});

	it.skipIf(process.platform === "win32")("refuses a login file that is a symlink, not a regular file, or open to group or others, and reads no other source", async () => {
		const d = database("tonbo-refused");
		const path = login(USABLE_LOGINS[1], 0o640);
		const shellWord = /^[\w@%+=:,./-]+$/.test(path) ? path : `'${path.replaceAll("'", "'\\''")}'`;
		await tonboError(open(d.name, { dir: freshDir() }), "credentials_refused", `Run: chmod 600 ${shellWord}`);
		const target = join(freshDir(), "elsewhere.json");
		writeFileSync(target, JSON.stringify(USABLE_LOGINS[1]), { mode: 0o600 });
		chmodSync(target, 0o600);
		const linked = freshDir();
		process.env.XDG_CONFIG_HOME = linked;
		mkdirSync(join(linked, "tonbo"), { mode: 0o700 });
		symlinkSync(target, join(linked, "tonbo", "credentials.json"));
		const symlink = await rejection(open(d.name, { dir: freshDir() }));
		expect(symlink).toBeInstanceOf(TonboError);
		expect(symlink).toMatchObject({ kind: "credentials_refused", action: "fix" });
		const directory = freshDir();
		process.env.XDG_CONFIG_HOME = directory;
		mkdirSync(join(directory, "tonbo", "credentials.json"), { recursive: true });
		expect(await rejection(open(d.name, { dir: freshDir() }))).toMatchObject({ kind: "credentials_refused", action: "fix" });
		expect(api.seen).toEqual([]);
	});

	it("fails missing_credentials without a key, unless a URL is opened with a token", async () => {
		const d = database("tonbo-missing");
		await tonboError(open(d.name, { dir: freshDir() }), "missing_credentials", "Set TONBO_API_KEY, or run: tonbo login");
		// A URL needs the key only for Streams tokens.
		await tonboError(open(d.url, { dir: freshDir() }), "missing_credentials", "Set TONBO_API_KEY, or run: tonbo login");
		const db = await open(d.url, { dir: freshDir(), token: "explicit-url" });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		db.close();
		expect(api.seen).toEqual([]);
	});

	it("maps 404 and a database being deleted to database_not_found, and 401 and 403 to invalid_api_key", async () => {
		const missing = await tonboError(open("tonbo-nope", { dir: freshDir(), apiKey: "tbo_answers" }), "database_not_found", "Run: tonbo database create tonbo-nope");
		expect(missing.message).toMatch(/^database_not_found: database "tonbo-nope" not found \(request req-\d+\)\. Run: tonbo database create tonbo-nope$/);
		expect(missing.requestId).toMatch(/^req-\d+$/);
		for (const state of ["deleting", "deleted"] as const) {
			const d = database(`tonbo-${state}`, state);
			await tonboError(open(d.name, { dir: freshDir(), apiKey: "tbo_answers" }), "database_not_found", `Run: tonbo database create ${d.name}`);
		}
		const d = database("tonbo-keys");
		await tonboError(open(d.name, { dir: freshDir(), apiKey: "tbo_revoked" }), "invalid_api_key", "Pass a valid apiKey");
		process.env.TONBO_API_KEY = "tbo_revoked";
		await tonboError(open(d.name, { dir: freshDir() }), "invalid_api_key", "Set TONBO_API_KEY to a valid key, or unset it and run: tonbo login");
		const forbidden = database("tonbo-forbidden", "active", { forbidden: true });
		const error = await tonboError(open(forbidden.name, { dir: freshDir(), apiKey: "tbo_answers" }), "invalid_api_key", `Use an API key that can read database "${forbidden.name}"`);
		expect(error.message).toContain(`can't read database "${forbidden.name}"`);
		expect(error.message).not.toContain("tbo_");
	});

	it("waits for a database being created, honors Retry-After, and gives up at the signal", async () => {
		const created = database("tonbo-creating", "creating", { activeAfter: 2 });
		(await open(created.name, { dir: freshDir(), apiKey: "tbo_waits" })).close();
		expect(api.of("management")).toHaveLength(3);
		api.reset();
		const limited = database("tonbo-limited");
		api.scripted.push({ status: 429, retryAfter: "1" });
		const started = Date.now();
		(await open(limited.name, { dir: freshDir(), apiKey: "tbo_waits" })).close();
		expect(Date.now() - started).toBeGreaterThanOrEqual(900);
		expect(api.of("management")).toHaveLength(2);
		const stuck = database("tonbo-stuck", "creating");
		const error = await rejection(open(stuck.name, { dir: freshDir(), apiKey: "tbo_waits", signal: AbortSignal.timeout(1_500) }));
		expect(error).toBeInstanceOf(UrsulaUnavailableError);
		expect(error).toMatchObject({ kind: "unavailable", action: "retry" });
		api.scripted.push({ status: 429, retryAfter: "1" });
		const limitedOut = await rejection(open(limited.name, { dir: freshDir(), apiKey: "tbo_waits", signal: AbortSignal.timeout(500) }));
		expect(limitedOut).toMatchObject({ kind: "rate_limited", action: "retry" });
		// `Retry-After: 0` waits the backoff all the same: no lookup storm.
		api.reset();
		for (let i = 0; i < 100; i += 1) api.scripted.push({ status: 429, retryAfter: "0" });
		const storm = await rejection(open(limited.name, { dir: freshDir(), apiKey: "tbo_waits", signal: AbortSignal.timeout(1_500) }));
		expect(storm).toMatchObject({ kind: "rate_limited", action: "retry" });
		expect(api.of("management").length).toBeLessThanOrEqual(5);
	});

	it("backs off on 5xx and an API that does not answer, and gives up unavailable or unreachable at the signal", async () => {
		const d = database("tonbo-backoff");
		api.scripted.push({ status: 503 });
		(await open(d.name, { dir: freshDir(), apiKey: "tbo_backoff" })).close();
		expect(api.of("management")).toHaveLength(2);
		for (let i = 0; i < 100; i += 1) api.scripted.push({ status: 503 });
		const down = await rejection(open(d.name, { dir: freshDir(), apiKey: "tbo_backoff", signal: AbortSignal.timeout(1_500) }));
		expect(down).toBeInstanceOf(UrsulaUnavailableError);
		expect(down).toMatchObject({ kind: "unavailable", action: "retry" });
		api.reset();
		const gone = await bystander(200);
		await gone.stop();
		const unreachable = await rejection(open(d.name, { dir: freshDir(), apiKey: "tbo_backoff", apiOrigin: gone.origin, signal: AbortSignal.timeout(1_500) }));
		expect(unreachable).toBeInstanceOf(UrsulaUnavailableError);
		expect(unreachable).toMatchObject({ kind: "unreachable", action: "retry" });
		expect(api.seen).toEqual([]);
	});

	it("fails invalid_api_key when the token endpoint refuses the key a Streams token", async () => {
		const d = database("tonbo-nostreams");
		await tonboError(open(d.name, { dir: freshDir(), apiKey: "tbo_nostreams" }), "invalid_api_key", "Pass a valid apiKey");
		expect(api.of("management")).toHaveLength(1);
		expect(api.of("token")).toMatchObject([{ audience: "https://streams.tonbo.dev" }]);
	});

	it("lets an explicit token serve the Streams requests", async () => {
		const d = database("tonbo-explicit");
		const db = await open(d.name, { dir: freshDir(), apiKey: "tbo_explicit", token: "explicit-1" });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		expect(api.of("management")).toHaveLength(1);
		expect(api.of("token")).toEqual([]);
		const sent = await sentTokens(d.path);
		if (sent !== undefined) {
			expect(sent.length).toBeGreaterThan(0);
			expect(sent.every((r) => r.authorization === "Bearer explicit-1")).toBe(true);
		}
		db.close();
	});

	// In a process of its own: the extension reads the file only while no open of the process set a token.
	it("lets URSULA_VFS_TOKEN_FILE serve the Streams requests of a process that set no token", async () => {
		const d = database("tonbo-token-file");
		const file = join(freshDir(), "token");
		writeFileSync(file, "from-the-file");
		const proc = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "tonbo-child.ts"), d.name, freshDir()], {
			stdio: ["ignore", "pipe", "inherit"],
			env: { ...process.env, TONBO_API_KEY: "tbo_file", URSULA_VFS_TOKEN_FILE: file },
		});
		let out = "";
		proc.stdout.on("data", (chunk: Buffer) => {
			out += chunk.toString();
		});
		const code = await new Promise((resolve) => proc.once("exit", resolve));
		expect({ out, code }).toEqual({ out: "synced\n", code: 0 });
		expect(api.of("management")).toMatchObject([{ name: d.name, authorization: "Bearer tbo_file" }]);
		expect(api.of("token")).toEqual([]);
		const sent = await sentTokens(d.path);
		if (sent !== undefined) {
			expect(sent.length).toBeGreaterThan(0);
			expect(sent.every((r) => r.authorization === "Bearer from-the-file")).toBe(true);
		}
	});

	it("takes origins from the options over the environment, and never sends a key over plain http to another host", async () => {
		const d = database("tonbo-origins");
		process.env.TONBO_API_ORIGIN = "http://127.0.0.1:1";
		(await open(d.name, { dir: freshDir(), apiKey: "tbo_origins", apiOrigin: api.origin })).close();
		expect(api.of("management")).toHaveLength(1);
		const error = await rejection(open(d.name, { dir: freshDir(), apiKey: "tbo_origins", apiOrigin: "http://api.example.com" }));
		expect(error).toBeInstanceOf(UrsulaSetupError);
		expect(error).toMatchObject({ kind: "invalid_argument", action: "fix" });
		expect(api.of("management")).toHaveLength(1);
		// A stream URL over plain http to another host gets no Streams token for the key.
		const exposed = database("tonbo-exposed", "active", { url: "http://streams.example.com/sqlite-e2e/exposed" });
		for (const target of [exposed.name, "http://streams.example.com/sqlite-e2e/exposed"]) {
			const refused = await rejection(open(target, { dir: freshDir(), apiKey: "tbo_origins", apiOrigin: api.origin }));
			expect(refused).toBeInstanceOf(UrsulaSetupError);
			expect(refused).toMatchObject({ kind: "invalid_url", action: "fix" });
		}
		expect(api.of("management")).toHaveLength(2);
		// The one Streams token is the first open's.
		expect(api.of("token")).toHaveLength(1);
	});

	it("names the origin to check when the token endpoint's origin is unusable or answers outside its contract", async () => {
		const d = database("tonbo-auth-origin");
		process.env.TONBO_AUTH_ORIGIN = "http://auth.example.com";
		const plain = await rejection(open(d.name, { dir: freshDir(), apiKey: "tbo_auth" }));
		expect(plain).toBeInstanceOf(UrsulaSetupError);
		expect(plain).toMatchObject({ kind: "invalid_argument", action: "fix" });
		expect((plain as Error).message).toContain("TONBO_AUTH_ORIGIN");
		expect(api.seen).toEqual([]);
		const wrong = await bystander(404);
		process.env.TONBO_AUTH_ORIGIN = wrong.origin;
		await invalidArgument(open(d.name, { dir: freshDir(), apiKey: "tbo_auth" }), "Check authOrigin (or TONBO_AUTH_ORIGIN)");
		expect(wrong.hits).toEqual(["POST /api/iam/token"]);
		expect(api.of("management")).toHaveLength(1);
		await wrong.stop();
	});

	it("never follows a redirect, which would carry the key to another origin", async () => {
		const d = database("tonbo-redirect");
		const redirect = await bystander(302, { location: `${api.origin}/v1/databases/${d.name}` });
		const error = await invalidArgument(open(d.name, { dir: freshDir(), apiKey: "tbo_redirect", apiOrigin: redirect.origin }), "Check apiOrigin (or TONBO_API_ORIGIN)");
		expect(error.message).toContain("redirect");
		expect(redirect.hits).toEqual([`GET /v1/databases/${d.name}`]);
		expect(api.seen).toEqual([]);
		await redirect.stop();
	});
});
