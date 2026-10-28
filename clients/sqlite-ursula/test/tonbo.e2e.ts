// `@tonbo/sqlite-ursula/tonbo`'s `open` by name, through the real open against the e2e server
// (test/stack.ts). test/tonbo-api.ts stands in for the Management API and the token endpoint, as
// their published contracts document them, and serves databases whose URLs are streams of the
// e2e server. In front of Ursula, the gate records which token each stream request carried. The
// process holds one token across providers, so each test waits until the bindings it opened retired
// and the provider stopped: the next test starts with none held. `resolveDatabase` runs with the `/pi`
// entry's `openUrsulaPiStorage`, the opener it exists for, and the `/tonbo/pi` entry runs both in one
// call under Pi's own Harness. The extension's `tonbo_resolve`, the same resolution for hosts of its
// SQL functions, runs against the same stand-in: on this thread with `timeout_ms` 0 and its `resolved`
// event, blocking in worker threads of this process (test/tonbo-resolve-thread.ts), which join the
// resolves of this thread, and blocking in a process of its own (test/tonbo-resolve-child.ts), whose
// stream requests carry the Streams token the resolve fetched for the key.
import { spawn } from "node:child_process";
import { chmodSync, mkdirSync, readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { Worker } from "node:worker_threads";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import type { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs } from "../src/index.ts";
import { openUrsulaPiStorage } from "../src/pi.ts";
import { open as openByName, type ResolvedDatabase, resolveDatabase, TonboError, UrsulaSetupError, UrsulaUnavailableError } from "../src/tonbo.ts";
import { openUrsulaPiStorage as openPiByName, TonboError as TonboPiError } from "../src/tonbo-pi.ts";
import { ext, freshDir, rejection, retired, sentTokens, stream } from "./kit.ts";
import { type FixtureDatabase, TonboApi } from "./tonbo-api.ts";

loadUrsulaVfs(ext());

/** The examples of the Tonbo CLI's credentials contract, version 1, as it lists them: the extension's unit tests read the same files. */
const contractExamples = (file: string): readonly Record<string, unknown>[] =>
	readFileSync(join(import.meta.dirname, "../../sqlite-vfs/src/tonbo/vectors", file), "utf8")
		.split("\n")
		.filter((line) => line.trim().length > 0)
		.map((line) => JSON.parse(line) as Record<string, unknown>);
const USABLE_LOGINS = contractExamples("credentials-v1-usable.jsonl");
const UNUSABLE_LOGINS = contractExamples("credentials-v1-unusable.jsonl");

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

/** The path a Pi storage bound. */
const boundOf = (storage: SqliteStorage): string => (storage as unknown as { db: { database: { location(): string } } }).db.database.location();

/** Pi storage on what `resolveDatabase` handed out, recording the path it bound. */
async function piStorage(resolved: ResolvedDatabase): Promise<SqliteStorage> {
	const storage = await openUrsulaPiStorage(resolved.url, { dir: freshDir(), token: resolved.token });
	bound.push(boundOf(storage));
	return storage;
}

/** The SQLite facade under a Pi storage: the wrapped node driver. */
const facade = (storage: SqliteStorage): { exec(sql: string): Promise<void>; all<T extends object>(sql: string): Promise<T[]> } =>
	(storage as unknown as { db: { exec(sql: string): Promise<void>; all<T extends object>(sql: string): Promise<T[]> } }).db;

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

describe("resolveDatabase", () => {
	it("resolves a name into the stream URL and a token provider, with which Pi storage opens and keeps its own Streams token", async () => {
		const d = database("tonbo-pi");
		const resolved = await resolveDatabase(d.name, { apiKey: "tbo_pi" });
		expect(resolved.url).toBe(d.url);
		expect(typeof resolved.token).toBe("function");
		expect(api.of("management")).toMatchObject([{ name: d.name, authorization: "Bearer tbo_pi" }]);
		// The open calls the provider: the resolve exchanged nothing for the stream.
		expect(api.of("token")).toEqual([]);
		const storage = await piStorage(resolved);
		const db = facade(storage);
		await db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		expect(await db.all("SELECT x FROM t")).toEqual([{ x: 1 }]);
		const exchanges = api.of("token");
		expect(exchanges).toMatchObject([{ authorization: "Bearer tbo_pi", audience: "https://streams.tonbo.dev" }]);
		const issued = exchanges[0]?.issued as string;
		const sent = await sentTokens(d.path);
		if (sent !== undefined) {
			expect(sent.length).toBeGreaterThan(0);
			expect(sent.every((r) => r.authorization === `Bearer ${issued}`)).toBe(true);
		}
		await storage.close(BACKGROUND_CONTEXT);
		expect(api.of("management")).toHaveLength(1);
	});

	it("fails as open does: missing_credentials, database_not_found, invalid_url, and invalid_api_key from the open that calls the provider", async () => {
		const d = database("tonbo-pi-errors");
		await tonboError(resolveDatabase(d.name), "missing_credentials", "Set TONBO_API_KEY, or run: tonbo login");
		await tonboError(resolveDatabase("tonbo-pi-nope", { apiKey: "tbo_pi_errors" }), "database_not_found", "Run: tonbo database create tonbo-pi-nope");
		const exposed = database("tonbo-pi-exposed", "active", { url: "http://streams.example.com/sqlite-e2e/exposed" });
		const refused = await rejection(resolveDatabase(exposed.name, { apiKey: "tbo_pi_errors" }));
		expect(refused).toBeInstanceOf(UrsulaSetupError);
		expect(refused).toMatchObject({ kind: "invalid_url", action: "fix" });
		// The lookup passes, and the open's call of the provider is refused a Streams token.
		const resolved = await resolveDatabase(d.name, { apiKey: "tbo_nostreams" });
		expect(resolved.url).toBe(d.url);
		await tonboError(openUrsulaPiStorage(resolved.url, { dir: freshDir(), token: resolved.token }), "invalid_api_key", "Pass a valid apiKey");
		expect(api.of("token")).toMatchObject([{ audience: "https://streams.tonbo.dev" }]);
	});

	it("takes a URL as it is, and passes an explicit token through with no key and no request", async () => {
		const d = database("tonbo-pi-url");
		expect(await resolveDatabase(d.url, { token: "explicit-pi" })).toEqual({ url: d.url, token: "explicit-pi" });
		expect(await resolveDatabase(new URL(d.url), { token: "explicit-pi" })).toEqual({ url: d.url, token: "explicit-pi" });
		expect(api.seen).toEqual([]);
	});
});

describe("the /tonbo/pi entry", () => {
	it("opens Pi storage by name in one call for Harness.open, whose close closes it, and its Streams requests carry the token exchanged for the key", async () => {
		const d = database("tonbo-pi-name");
		const storage = await openPiByName(d.name, { apiKey: "tbo_pi_name", dir: freshDir() });
		const path = boundOf(storage);
		bound.push(path);
		await facade(storage).exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		expect(api.of("management")).toMatchObject([{ name: d.name, authorization: "Bearer tbo_pi_name" }]);
		const exchanges = api.of("token");
		expect(exchanges).toMatchObject([{ authorization: "Bearer tbo_pi_name", audience: "https://streams.tonbo.dev" }]);
		const issued = exchanges[0]?.issued as string;
		const sent = await sentTokens(d.path);
		if (sent !== undefined) {
			expect(sent.length).toBeGreaterThan(0);
			expect(sent.every((r) => r.authorization === `Bearer ${issued}`)).toBe(true);
		}
		const harness = await Harness.open(storage, { models: createModels(), registry: createRegistry() }, BACKGROUND_CONTEXT);
		await harness.close(BACKGROUND_CONTEXT);
		// The Harness closed the storage: its binding retires with no close of the test's own.
		await retired(path);
		expect(api.of("management")).toHaveLength(1);
	});

	it("fails as the /tonbo entry does, with its error classes, before any Streams token is exchanged", async () => {
		expect(TonboPiError).toBe(TonboError);
		const d = database("tonbo-pi-name-errors");
		await tonboError(openPiByName(d.name, { dir: freshDir() }), "missing_credentials", "Set TONBO_API_KEY, or run: tonbo login");
		await tonboError(openPiByName("tonbo-pi-name-nope", { apiKey: "tbo_pi_name_errors", dir: freshDir() }), "database_not_found", "Run: tonbo database create tonbo-pi-name-nope");
		expect(api.of("token")).toEqual([]);
	});
});

/** One of the extension's SQL functions on this thread's control connection: its JSON answer, or the extension's own error, unclassified. */
function raw<T>(sql: string, ...args: (string | number)[]): T {
	const row = loadUrsulaVfs(ext()).prepare(sql).get(...args) as Record<string, string>;
	return JSON.parse(Object.values(row)[0] as string) as T;
}

/** `tonbo_resolve`'s answer. */
interface Resolution {
	readonly done: boolean;
	readonly url?: string;
	readonly name?: string | null;
	readonly request?: number;
}

/** The event cursor now: events after it are the ones a test waits for. */
const eventCursor = (): number => raw<{ cursor: number }>("SELECT ursula_events(0, 0)").cursor;

/** A `resolved` event of `ursula_events`. */
interface ResolvedEvent {
	readonly event: "resolved";
	readonly request: number;
	readonly ok: boolean;
}

/** Waits for the `resolved` event of `request` after `cursor`: it, or `"gap"` when events were dropped. */
async function resolvedEvent(cursor: number, request: number | undefined): Promise<ResolvedEvent | "gap"> {
	const deadline = Date.now() + 30_000;
	for (;;) {
		const got = raw<{ cursor: number; events: { event: string; request?: number; ok?: boolean }[] }>("SELECT ursula_events(?, 0)", cursor);
		cursor = got.cursor;
		if (got.events.some((e) => e.event === "gap")) return "gap";
		const resolved = got.events.find((e) => e.event === "resolved" && e.request === request);
		if (resolved !== undefined) return resolved as ResolvedEvent;
		expect(Date.now(), `request ${request} never resolved`).toBeLessThan(deadline);
		await new Promise((r) => setTimeout(r, 10));
	}
}

/**
 * The extension's `tonbo_resolve(nameOrUrl, options, 0)`, as a host that must not block calls it: it
 * returns at once, the test waits for its `resolved` event (the stand-in answers on this thread
 * meanwhile), and a call with the same arguments takes the outcome, the answer or the extension's
 * error, or names another request to wait for when the one it waited for was given up.
 */
async function tonboResolve(nameOrUrl: string, options: Record<string, unknown> = {}): Promise<Resolution> {
	const json = JSON.stringify(options);
	const cursor = eventCursor();
	let pending = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", nameOrUrl, json);
	while (!pending.done) {
		await resolvedEvent(cursor, pending.request);
		pending = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", nameOrUrl, json);
	}
	return pending;
}

/** What a blocking `tonbo_resolve` in a worker thread of this process came to: its answer, or its error's message. */
interface Blocked {
	readonly answer?: Resolution;
	readonly error?: string;
}

/**
 * The extension's `tonbo_resolve(nameOrUrl, options, timeoutMs)`, blocking, in a worker thread of this
 * process (test/tonbo-resolve-thread.ts), so it joins the resolves of this thread's calls while the
 * stand-in answers on this one. `calling` settles as the thread makes the call, `done` with its outcome.
 */
function blockingResolve(nameOrUrl: string, options: Record<string, unknown>, timeoutMs: number | bigint): { calling: Promise<void>; done: Promise<Blocked> } {
	const worker = new Worker(join(import.meta.dirname, "tonbo-resolve-thread.ts"), {
		workerData: { ext: ext(), nameOrUrl, options: JSON.stringify(options), timeoutMs },
		execArgv: ["--no-warnings"],
	});
	let called: () => void = () => {};
	const calling = new Promise<void>((resolve) => {
		called = resolve;
	});
	const done = new Promise<Blocked>((resolve, reject) => {
		worker.on("message", (m: "calling" | Blocked) => {
			if (m === "calling") called();
			else resolve(m);
		});
		worker.once("error", reject);
	});
	return { calling, done };
}

/** Waits until `ready` holds, checked every 10 ms for at most 30 s. */
async function until(ready: () => boolean, what: string): Promise<void> {
	const deadline = Date.now() + 30_000;
	while (!ready()) {
		expect(Date.now(), what).toBeLessThan(deadline);
		await new Promise((r) => setTimeout(r, 10));
	}
}

/** What `ursula_cancel` of a resolve reports it ended with. */
interface CancelReport {
	readonly kind: string;
	readonly action: string;
	readonly message: string;
	readonly fix: string | null;
	readonly request_id: string | null;
}

/** The extension's error `promise` rejects with: `tonbo_resolve: <kind>: ...`, ending with `. <fix>` when one is given. */
async function resolveError(promise: Promise<unknown>, kind: string, fix?: string): Promise<string> {
	const error = await rejection(promise);
	expect(error).toBeInstanceOf(Error);
	const message = (error as Error).message;
	expect(message.startsWith(`tonbo_resolve: ${kind}: `), message).toBe(true);
	if (fix !== undefined) expect(message.endsWith(`. ${fix}`), message).toBe(true);
	return message;
}

// The extension's own resolution, for hosts of its SQL functions. The process holds one API key for
// Streams tokens: a key refused gives way, and the tests that fetch a Streams token in this process
// share `tbo_extension`, so the refused key's test runs first. The others give the Streams requests a
// token of their own (`own_token`).
describe("the extension's tonbo_resolve", () => {
	it("fails invalid_api_key when the token endpoint refuses the key a Streams token, before it returns", async () => {
		const d = database("ext-nostreams");
		const message = await resolveError(tonboResolve(d.name, { api_key: "tbo_nostreams" }), "invalid_api_key", "Pass a valid API key");
		// The token endpoint's problem details: its code and request id.
		expect(message).toMatch(/^tonbo_resolve: invalid_api_key: the Tonbo token endpoint refused the API key from the API key option \(unauthenticated\) \(request req-\d+\)\. Pass a valid API key$/);
		expect(api.of("management")).toMatchObject([{ name: d.name, authorization: "Bearer tbo_nostreams" }]);
		expect(api.of("token")).toMatchObject([{ authorization: "Bearer tbo_nostreams", audience: "https://streams.tonbo.dev" }]);
	});

	it("resolves a name with the key and holds the key's first Streams token when it returns, and refuses another key meanwhile", async () => {
		const d = database("ext-name");
		expect(await tonboResolve(d.name, { api_key: "tbo_extension" })).toEqual({ done: true, url: d.url, name: d.name });
		expect(api.of("management")).toMatchObject([{ name: d.name, authorization: "Bearer tbo_extension" }]);
		expect(api.of("token")).toMatchObject([{ authorization: "Bearer tbo_extension", audience: "https://streams.tonbo.dev" }]);
		// A URL looks nothing up, and the token held serves at once.
		expect(await tonboResolve(d.url, { api_key: "tbo_extension" })).toEqual({ done: true, url: d.url, name: null });
		expect(api.of("management")).toHaveLength(1);
		expect(api.of("token")).toHaveLength(1);
		// One key per process: another is refused while a host may open with this one's token.
		const other = await resolveError(tonboResolve(d.url, { api_key: "tbo_other" }), "invalid_argument");
		expect(other).toContain("another API key");
		expect(api.of("token")).toHaveLength(1);
		// A URL with a token of the host's own needs no key and sends nothing.
		expect(raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.url, JSON.stringify({ own_token: true }))).toEqual({ done: true, url: d.url, name: null });
		expect(api.seen).toHaveLength(2);
	});

	it("exchanges an Agent key with no audience for the Management API, holds that token, and exchanges again when it is refused", async () => {
		const d = database("ext-agent");
		// The answer names another token endpoint: the key never goes there.
		const elsewhere = await bystander(200);
		api.tokenEndpoint = `${elsewhere.origin}/api/iam/token`;
		const options = { api_key: "agent_ext", own_token: true };
		expect(await tonboResolve(d.name, options)).toEqual({ done: true, url: d.url, name: d.name });
		expect(elsewhere.hits).toEqual([]);
		await elsewhere.stop();
		const exchanged = (): string[] => api.of("token").map((s) => s.issued as string);
		expect(api.of("token")).toMatchObject([{ authorization: "Bearer agent_ext", audience: null }]);
		const first = `Bearer ${exchanged()[0] as string}`;
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer agent_ext", first]);
		await tonboResolve(d.name, options);
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer agent_ext", first, first]);
		api.forget(exchanged()[0] as string);
		await tonboResolve(d.name, options);
		expect(api.of("token")).toHaveLength(2);
		const second = `Bearer ${exchanged()[1] as string}`;
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer agent_ext", first, first, first, second]);
		expect(api.of("token").every((s) => s.audience === null)).toBe(true);
	});

	it("takes the key from api_key, then TONBO_API_KEY, then the contract's usable logins, and refuses the others unread", async () => {
		const d = database("ext-sources");
		const options = { own_token: true };
		login(USABLE_LOGINS[1]);
		process.env.TONBO_API_KEY = "tbo_env";
		await tonboResolve(d.name, { ...options, api_key: "tbo_option" });
		await tonboResolve(d.name, options);
		delete process.env.TONBO_API_KEY;
		await tonboResolve(d.name, options);
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer tbo_option", "Bearer tbo_env", "Bearer tbo_secret"]);
		for (const content of USABLE_LOGINS) {
			login(content);
			const expiresAt = content.expires_at;
			if (typeof expiresAt === "string" && Date.parse(expiresAt) <= Date.now()) {
				await resolveError(tonboResolve(d.name, options), "invalid_api_key", "Run: tonbo login");
				continue;
			}
			await tonboResolve(d.name, options);
			expect(api.of("management").at(-1)?.authorization).toBe("Bearer tbo_secret");
		}
		api.reset();
		for (const content of [...UNUSABLE_LOGINS, { ...USABLE_LOGINS[1], expires_at: "2020-01-01T00:00:00.000Z" }]) {
			login(content);
			const message = await resolveError(tonboResolve(d.name, options), "invalid_api_key", "Run: tonbo login");
			expect(message).not.toContain("tbo_secret");
		}
		const path = login(USABLE_LOGINS[1], 0o640);
		await resolveError(tonboResolve(d.name, options), "credentials_refused", `Run: chmod 600 ${path}`);
		const target = join(freshDir(), "elsewhere.json");
		writeFileSync(target, JSON.stringify(USABLE_LOGINS[1]), { mode: 0o600 });
		chmodSync(target, 0o600);
		const linked = freshDir();
		process.env.XDG_CONFIG_HOME = linked;
		mkdirSync(join(linked, "tonbo"), { mode: 0o700 });
		symlinkSync(target, join(linked, "tonbo", "credentials.json"));
		await resolveError(tonboResolve(d.name, options), "credentials_refused", "Replace it with the file itself, or remove it, then run: tonbo login");
		const directory = freshDir();
		process.env.XDG_CONFIG_HOME = directory;
		mkdirSync(join(directory, "tonbo", "credentials.json"), { recursive: true });
		await resolveError(tonboResolve(d.name, options), "credentials_refused", "Remove it, then run: tonbo login");
		process.env.XDG_CONFIG_HOME = freshDir();
		await resolveError(tonboResolve(d.name, options), "missing_credentials", "Set TONBO_API_KEY, or run: tonbo login");
		expect(api.seen).toEqual([]);
	});

	it("maps 404 and a database being deleted to database_not_found, and 401 and 403 to invalid_api_key, with the API's request id", async () => {
		const options = { api_key: "tbo_answers", own_token: true };
		const missing = await resolveError(tonboResolve("ext-nope", options), "database_not_found", "Run: tonbo database create ext-nope");
		expect(missing).toMatch(/^tonbo_resolve: database_not_found: database "ext-nope" not found \(request req-\d+\)\. Run: tonbo database create ext-nope$/);
		for (const state of ["deleting", "deleted"] as const) {
			const d = database(`ext-${state}`, state);
			await resolveError(tonboResolve(d.name, options), "database_not_found", `Run: tonbo database create ${d.name}`);
		}
		const d = database("ext-keys");
		await resolveError(tonboResolve(d.name, { api_key: "tbo_revoked", own_token: true }), "invalid_api_key", "Pass a valid API key");
		process.env.TONBO_API_KEY = "tbo_revoked";
		await resolveError(tonboResolve(d.name, { own_token: true }), "invalid_api_key", "Set TONBO_API_KEY to a valid key, or unset it and run: tonbo login");
		const forbidden = database("ext-forbidden", "active", { forbidden: true });
		const message = await resolveError(tonboResolve(forbidden.name, options), "invalid_api_key", `Use an API key that can read database "${forbidden.name}"`);
		expect(message).toContain(`can't read database "${forbidden.name}"`);
		expect(message).not.toContain("tbo_");
		await resolveError(tonboResolve("", options), "invalid_name");
	});

	it("waits for a database being created, honors Retry-After but never less than the backoff, and gives up with the last condition", async () => {
		const options = { api_key: "tbo_waits", own_token: true };
		const created = database("ext-creating", "creating", { activeAfter: 2 });
		expect((await tonboResolve(created.name, options)).url).toBe(created.url);
		expect(api.of("management")).toHaveLength(3);
		api.reset();
		const limited = database("ext-limited");
		api.scripted.push({ status: 429, retryAfter: "1" });
		const started = Date.now();
		await tonboResolve(limited.name, options);
		expect(Date.now() - started).toBeGreaterThanOrEqual(900);
		expect(api.of("management")).toHaveLength(2);
		const stuck = database("ext-stuck", "creating");
		await resolveError(tonboResolve(stuck.name, { ...options, timeout_ms: 1_500 }), "unavailable");
		api.scripted.push({ status: 429, retryAfter: "1" });
		await resolveError(tonboResolve(limited.name, { ...options, timeout_ms: 500 }), "rate_limited");
		// `Retry-After: 0` waits the backoff all the same: no lookup storm.
		api.reset();
		for (let i = 0; i < 100; i += 1) api.scripted.push({ status: 429, retryAfter: "0" });
		await resolveError(tonboResolve(limited.name, { ...options, timeout_ms: 1_500 }), "rate_limited");
		expect(api.of("management").length).toBeLessThanOrEqual(5);
		api.reset();
		for (let i = 0; i < 100; i += 1) api.scripted.push({ status: 503 });
		await resolveError(tonboResolve(limited.name, { ...options, timeout_ms: 1_500 }), "unavailable");
		api.reset();
		const gone = await bystander(200);
		await gone.stop();
		await resolveError(tonboResolve(limited.name, { ...options, api_origin: gone.origin, timeout_ms: 1_500 }), "unreachable");
		expect(api.seen).toEqual([]);
	});

	it("never follows a redirect, and never sends the key or its Streams tokens over plain http to another host", async () => {
		const d = database("ext-redirect");
		const redirect = await bystander(302, { location: `${api.origin}/v1/databases/${d.name}` });
		const options = { api_key: "tbo_redirect", own_token: true };
		const message = await resolveError(tonboResolve(d.name, { ...options, api_origin: redirect.origin }), "invalid_argument", "Check the API origin option or TONBO_API_ORIGIN");
		expect(message).toContain("redirect");
		expect(redirect.hits).toEqual([`GET /v1/databases/${d.name}`]);
		await redirect.stop();
		const plainApi = await resolveError(tonboResolve(d.name, { ...options, api_origin: "http://api.example.com" }), "invalid_argument");
		expect(plainApi).toContain("the API origin option (or TONBO_API_ORIGIN)");
		process.env.TONBO_AUTH_ORIGIN = "http://auth.example.com";
		const plainAuth = await resolveError(tonboResolve(d.name, options), "invalid_argument");
		expect(plainAuth).toContain("TONBO_AUTH_ORIGIN");
		process.env.TONBO_AUTH_ORIGIN = api.origin;
		expect(api.seen).toEqual([]);
		const exposed = database("ext-exposed", "active", { url: "http://streams.example.com/sqlite-e2e/exposed" });
		for (const target of [exposed.name, "http://streams.example.com/sqlite-e2e/exposed"]) {
			await resolveError(tonboResolve(target, { api_key: "tbo_extension" }), "invalid_url", "Open an https:// URL, or give the open a token of its own");
		}
		expect(api.of("management")).toHaveLength(1);
		expect(api.of("token")).toEqual([]);
		// A URL the open would refuse is refused before any request, its key never sent.
		api.reset();
		for (const target of ["https://", "https://streams.example.com/s?x=1"]) {
			await resolveError(tonboResolve(target, { api_key: "tbo_extension" }), "invalid_url");
		}
		const nowhere = database("ext-nowhere", "active", { url: "https://" });
		await resolveError(tonboResolve(nowhere.name, { api_key: "tbo_extension" }), "invalid_url");
		expect(api.of("token")).toEqual([]);
	});

	it("sends any key, one it was not passed included, to the origins the options name over the environment's", async () => {
		const d = database("ext-origins");
		const elsewhere = await bystander(200);
		process.env.TONBO_API_ORIGIN = elsewhere.origin;
		process.env.TONBO_API_KEY = "tbo_ambient";
		await tonboResolve(d.name, { own_token: true, api_origin: api.origin });
		delete process.env.TONBO_API_KEY;
		login(USABLE_LOGINS[1]);
		await tonboResolve(d.name, { own_token: true, api_origin: api.origin });
		await tonboResolve(d.name, { own_token: true, api_key: "tbo_option", api_origin: api.origin });
		expect(elsewhere.hits).toEqual([]);
		await elsewhere.stop();
		expect(api.of("management").map((s) => s.authorization)).toEqual(["Bearer tbo_ambient", "Bearer tbo_secret", "Bearer tbo_option"]);
	});

	// Worker threads of this process make the blocking calls, so this thread serves the stand-in meanwhile.
	it("joins the calls with the same arguments, blocking or not, into one resolve, and each takes its answer", async () => {
		const d = database("ext-join", "creating", { activeAfter: 5 });
		const options = { api_key: "tbo_join", own_token: true };
		const cursor = eventCursor();
		const first = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, JSON.stringify(options));
		expect(first.done).toBe(false);
		const a = blockingResolve(d.name, options, 20_000);
		const b = blockingResolve(d.name, options, 20_000);
		const answer = { done: true, url: d.url, name: d.name };
		expect(await a.done).toEqual({ answer });
		expect(await b.done).toEqual({ answer });
		expect(await resolvedEvent(cursor, first.request)).toEqual({ event: "resolved", request: first.request, ok: true });
		expect(raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, JSON.stringify(options))).toEqual(answer);
		// One resolve: `creating` five times, then active.
		expect(api.of("management")).toHaveLength(6);
		// A timeout too large for the clock waits as long as the resolve takes.
		api.reset();
		expect(await blockingResolve(d.name, options, 9223372036854775807n).done).toEqual({ answer });
		expect(api.of("management")).toHaveLength(1);
	});

	it("leaves a resolve another call is owed when a blocking call's time runs out, and the other call takes its answer", async () => {
		const d = database("ext-leave", "creating", { activeAfter: 5 });
		const options = { api_key: "tbo_leave", own_token: true };
		const json = JSON.stringify(options);
		const cursor = eventCursor();
		const first = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, json);
		const blocked = await blockingResolve(d.name, options, 200).done;
		// It gives up after its time and the grace, with the condition the resolve met last.
		expect(blocked.error).toBe(`tonbo_resolve: unavailable: the lookup of database "${d.name}" gave up when its time ran out: database "${d.name}" is creating`);
		expect(await resolvedEvent(cursor, first.request)).toEqual({ event: "resolved", request: first.request, ok: true });
		expect(raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, json)).toEqual({ done: true, url: d.url, name: d.name });
		expect(api.of("management")).toHaveLength(6);
	});

	it("gives a resolve up when the call that cancels was the last one owed it, still ends it with its resolved event, and a call after it starts anew", async () => {
		const d = database("ext-cancel", "creating");
		const options = { api_key: "tbo_cancel", own_token: true };
		const json = JSON.stringify(options);
		const cursor = eventCursor();
		const first = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, json);
		// Another host's call with the same arguments joins it.
		expect(raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, json)).toEqual(first);
		await until(() => api.of("management").length >= 2, "the lookup went again");
		const report = raw<CancelReport>("SELECT ursula_cancel(?)", first.request as number);
		expect(report).toEqual({
			kind: "unavailable",
			action: "retry",
			message: `unavailable: the lookup of database "${d.name}" gave up when it was cancelled: database "${d.name}" is creating`,
			fix: null,
			request_id: null,
		});
		// The joined host hears the end, and its call again starts another resolve.
		expect(await resolvedEvent(cursor, first.request)).toEqual({ event: "resolved", request: first.request, ok: false });
		const again = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, json);
		expect(again.done).toBe(false);
		expect(again.request).not.toBe(first.request);
		expect(() => raw("SELECT ursula_cancel(?)", first.request as number)).toThrow(/^ursula_cancel: not_bound: the request \d+ is not an open or a resolve in progress$/);
		(api.databases.get(d.name) as FixtureDatabase).state = "active";
		expect(await tonboResolve(d.name, options)).toEqual({ done: true, url: d.url, name: d.name });
	});

	it("goes on with a resolve a blocking call waits for when another call cancels, and reports what an ended one came to", async () => {
		const d = database("ext-cancel-wait", "creating", { activeAfter: 5 });
		const options = { api_key: "tbo_cancel_wait", own_token: true };
		const json = JSON.stringify(options);
		const first = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, json);
		const blocked = blockingResolve(d.name, options, 20_000);
		await blocked.calling;
		await until(() => api.of("management").length >= 2, "the lookup went again");
		const report = raw<CancelReport>("SELECT ursula_cancel(?)", first.request as number);
		expect(report).toMatchObject({ kind: "unavailable", action: "retry", fix: null, request_id: null });
		expect(report.message).toContain("gave up when it was cancelled");
		expect(await blocked.done).toEqual({ answer: { done: true, url: d.url, name: d.name } });
		expect(api.of("management")).toHaveLength(6);
		// Ended before the cancel: the failure it ended with, or null for an answer.
		const cursor = eventCursor();
		const missing = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", "ext-cancel-nope", json);
		expect(await resolvedEvent(cursor, missing.request)).toEqual({ event: "resolved", request: missing.request, ok: false });
		const failed = raw<CancelReport>("SELECT ursula_cancel(?)", missing.request as number);
		expect(failed).toMatchObject({ kind: "database_not_found", action: "fix", fix: "Run: tonbo database create ext-cancel-nope" });
		expect(failed.request_id).toMatch(/^req-\d+$/);
		expect(failed.message).toBe(`database_not_found: database "ext-cancel-nope" not found (request ${failed.request_id}). Run: tonbo database create ext-cancel-nope`);
		const found = raw<Resolution>("SELECT tonbo_resolve(?, ?, 0)", d.name, json);
		expect(await resolvedEvent(cursor, found.request)).toEqual({ event: "resolved", request: found.request, ok: true });
		expect(raw<CancelReport | null>("SELECT ursula_cancel(?)", found.request as number)).toBeNull();
		// The cancel took the outcome: the next call looks the name up again.
		api.reset();
		expect(await tonboResolve(d.name, options)).toEqual({ done: true, url: d.url, name: d.name });
		expect(api.of("management")).toHaveLength(1);
	});

	// In a process of its own, where no token was set: the key's token is the one requests carry.
	it("resolves by name in a process of its own, blocking, and the stream's requests carry the Streams token it fetched", async () => {
		const d = database("ext-child");
		const proc = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "tonbo-resolve-child.ts"), d.name, freshDir()], {
			stdio: ["ignore", "pipe", "inherit"],
			env: { ...process.env, TONBO_API_KEY: "tbo_child" },
		});
		let out = "";
		proc.stdout.on("data", (chunk: Buffer) => {
			out += chunk.toString();
		});
		const code = await new Promise((resolve) => proc.once("exit", resolve));
		expect(code).toBe(0);
		expect(JSON.parse(out)).toEqual({ resolved: { done: true, url: d.url, name: d.name }, synced: true });
		expect(api.of("management")).toMatchObject([{ name: d.name, authorization: "Bearer tbo_child" }]);
		const exchanges = api.of("token");
		expect(exchanges).toMatchObject([{ authorization: "Bearer tbo_child", audience: "https://streams.tonbo.dev" }]);
		const issued = exchanges[0]?.issued as string;
		const sent = await sentTokens(d.path);
		if (sent !== undefined) {
			expect(sent.length).toBeGreaterThan(0);
			expect(sent.every((r) => r.authorization === `Bearer ${issued}`)).toBe(true);
		}
	});
});
