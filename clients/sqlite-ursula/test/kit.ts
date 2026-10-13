import { type ChildProcess, spawn } from "node:child_process";
import { generateKeyPairSync, type KeyObject, randomUUID, sign } from "node:crypto";
import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { createServer, request, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { dirname, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { expect, inject } from "vitest";
import type { AttachStatus, MeterLike, MetricAttributes } from "../src/index.ts";

let counter = 0;
/** A fresh stream URL on the spawned node (bucket created by the global setup). */
export const streamPath = (): string => `/sqlite-e2e/vfs-${process.pid}-${Date.now().toString(36)}-${counter++}`;
export const ursulaUrl = (): string => inject("ursulaUrl");

/**
 * What a refusal that keeps the local files asks for before the path attaches again: the db file and
 * its -wal moved aside, as a pair that still opens as a plain SQLite file.
 */
export function moveAside(file: string): void {
	renameSync(file, `${file}.aside`);
	if (existsSync(`${file}-wal`)) renameSync(`${file}-wal`, `${file}.aside-wal`);
}

/** The offset a sidecar's text records: its first token. */
export const sidecarOffset = (sidecar: string): string => sidecar.split(" ")[0] ?? "";

/** Offsets are the server's opaque strings, ordered as strings: asserts that `a` is past `b`. */
export function expectPast(a: string, b: string): void {
	expect(a > b, `${a} > ${b}`).toBe(true);
}

export function vfsPath(): string {
	const p = process.env.SQLITE_URSULA_VFS;
	if (p === undefined || p.length === 0) throw new Error("set SQLITE_URSULA_VFS");
	return p;
}

/** Opens `file` the way an application would: plain node:sqlite, WAL. */
export function openPlain(file: string): DatabaseSync {
	const db = new DatabaseSync(file);
	db.exec("PRAGMA journal_mode=WAL");
	return db;
}

export const integrity = (db: DatabaseSync): string => (db.prepare("PRAGMA integrity_check").get() as { integrity_check: string }).integrity_check;

/** A meter for `instrument()` that keeps what it is given; `observe` runs a gauge's callback and returns what it observed. */
export function recordingMeter() {
	const records = new Map<string, number[]>();
	const callbacks = new Map<string, (result: { observe(value: number, attributes?: MetricAttributes): void }) => void>();
	const keep = (name: string, value: number): void => {
		const kept = records.get(name);
		if (kept === undefined) records.set(name, [value]);
		else kept.push(value);
	};
	const meter: MeterLike = {
		createHistogram: (name) => ({ record: (value) => keep(name, value) }),
		createCounter: (name) => ({ add: (value) => keep(name, value) }),
		createObservableGauge: (name) => ({
			addCallback: (callback) => callbacks.set(name, callback),
			removeCallback: () => callbacks.delete(name),
		}),
	};
	const observe = (name: string): number | undefined => {
		let seen: number | undefined;
		callbacks.get(name)?.({ observe: (value) => (seen = value) });
		return seen;
	};
	return { meter, records, callbacks, observe };
}

/** Every table (FTS shadow tables included) and the schema, rows sorted. */
export function dump(db: DatabaseSync): Record<string, string[]> {
	const out: Record<string, string[]> = {};
	const tables = db.prepare("SELECT name, sql FROM sqlite_schema WHERE type = 'table' ORDER BY name").all() as { name: string; sql: string | null }[];
	out["sqlite_schema"] = (db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY name").all() as object[]).map((r) => JSON.stringify(r));
	for (const { name, sql } of tables) {
		if (sql !== null && /^CREATE VIRTUAL TABLE/i.test(sql)) continue;
		out[name] = (db.prepare(`SELECT * FROM "${name}"`).all() as object[])
			.map((r) => JSON.stringify(r, (_k, v: unknown) => (v instanceof Uint8Array ? Buffer.from(v).toString("hex") : typeof v === "bigint" ? v.toString() : v)))
			.sort();
	}
	return out;
}

export const walContains = (file: string, marker: string): boolean => existsSync(`${file}-wal`) && readFileSync(`${file}-wal`).includes(Buffer.from(marker));

export type ChildLine = {
	attached?: string;
	step?: number;
	phase?: string;
	ok?: boolean;
	error?: string;
	errcode?: number;
	done?: boolean;
	/** On `done`: append attempts per commit, whether the file is poisoned (fenced, and why), its stream offset. */
	attempts?: number[];
	poisoned?: boolean;
	fenced?: boolean;
	reason?: string | null;
	offset?: string;
	epoch?: number;
	/** On `done`: whether the latest write was refused with 402 (and the layer's explanation), and whether the file is attached read-only. */
	payment_required?: boolean;
	payment_reason?: string | null;
	read_only?: boolean;
	/** On `done`: whether the latest write was refused before it reached the stream, what refused it and why. */
	writes_refused?: boolean;
	refused_kind?: string | null;
	refused_reason?: string | null;
	/** On a `@status` step: `ursula_status`. */
	status?: AttachStatus;
	/** On `done`: snapshots published, the latest known snapshot and the retention this owner set. */
	snapshots?: number;
	snapshot?: string;
	retained?: string;
	/** On `done`: the snapshot attach installed and the offset of the local files it trusted. */
	installed?: string;
	local?: string;
	/** On a `@query:` step: the rows. */
	rows?: Record<string, unknown>[];
	/** On `done`: the owner's health fields from `ursula_status`. */
	health?: Pick<AttachStatus, "commits" | "append_retries" | "attach_ms" | "log_bytes" | "snapshot_due_bytes" | "snapshot_age_ms" | "snapshot_failures" | "snapshot_error">;
};

export interface Child {
	readonly proc: ChildProcess;
	readonly lines: ChildLine[];
	waitFor(pred: (l: ChildLine) => boolean, ms?: number): Promise<ChildLine>;
	readonly exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
	stderr(): string;
}

const childScript = join(dirname(fileURLToPath(import.meta.url)), "child.mjs");
const gateScript = join(dirname(fileURLToPath(import.meta.url)), "gate.mjs");

/** Runs `child.mjs` with this build's extension, or with `ext`'s. A child that a `waitFor` gives up on is killed, so it does not outlive the test. */
export function runChild(file: string, url: string, sqls: readonly string[], env: Record<string, string> = {}, ext = vfsPath()): Child {
	const proc = spawn(process.execPath, [childScript, ext, file, url, ...sqls], { env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] });
	const lines: ChildLine[] = [];
	let err = "";
	let buf = "";
	const waiters: { pred: (l: ChildLine) => boolean; res: (l: ChildLine) => void }[] = [];
	proc.stdout?.on("data", (chunk: Buffer) => {
		buf += chunk.toString();
		for (let i = buf.indexOf("\n"); i >= 0; i = buf.indexOf("\n")) {
			const line = JSON.parse(buf.slice(0, i)) as ChildLine;
			buf = buf.slice(i + 1);
			lines.push(line);
			for (const w of [...waiters]) if (w.pred(line)) w.res(line);
		}
	});
	proc.stderr?.on("data", (chunk: Buffer) => {
		err += chunk.toString();
	});
	const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((res) => proc.once("exit", (code, signal) => res({ code, signal })));
	return {
		proc,
		lines,
		exited,
		stderr: () => err,
		waitFor: (pred, ms = 30_000) => {
			const hit = lines.find(pred);
			if (hit !== undefined) return Promise.resolve(hit);
			return new Promise((res, rej) => {
				const timer = setTimeout(() => {
					proc.kill("SIGKILL");
					rej(new Error(`child: timed out; lines ${JSON.stringify(lines)}; stderr ${err}`));
				}, ms);
				waiters.push({ pred, res: (l) => (clearTimeout(timer), res(l)) });
				void exited.then((x) => setTimeout(() => rej(new Error(`child exited ${JSON.stringify(x)}; lines ${JSON.stringify(lines)}; stderr ${err}`)), 50));
			});
		},
	};
}

/**
 * HTTP proxy to the node. After `stallAfter` forwarded POSTs it holds every further POST unanswered and
 * unforwarded; after `dropAfter` forwarded POSTs it forwards the next one, waits for the node's answer
 * and then cuts the client connection instead of answering (an append with an unknown outcome); the
 * POST numbered `limitAt` is answered 429 with `Retry-After: 1` without being forwarded. Every POST it
 * forwards waits `delayMs` first.
 */
export class StallProxy {
	private readonly server: Server;
	private posts = 0;
	stallAfter = Number.POSITIVE_INFINITY;
	dropAfter = Number.POSITIVE_INFINITY;
	limitAt = Number.POSITIVE_INFINITY;
	delayMs = 0;
	/** POSTs whose answer was dropped. */
	dropped = 0;
	readonly stalled: Promise<void>;
	private onStall!: () => void;
	private onPost: (() => void)[] = [];
	readonly url: string;

	private constructor(server: Server, url: string) {
		this.server = server;
		this.url = url;
		this.stalled = new Promise((r) => {
			this.onStall = r;
		});
	}

	static async start(target: string): Promise<StallProxy> {
		const t = new URL(target);
		let proxy: StallProxy | undefined;
		const server = createServer((req, res) => {
			const p = proxy as StallProxy;
			let drop = false;
			let delay = 0;
			if (req.method === "POST") {
				for (const resolve of p.onPost.splice(0)) resolve();
				delay = p.delayMs;
				const n = p.posts++;
				if (n >= p.stallAfter) {
					p.onStall();
					return; // in flight forever: never forwarded, never answered
				}
				drop = n === p.dropAfter;
				if (n === p.limitAt) {
					req.resume();
					res.writeHead(429, { "retry-after": "1" }).end("rate limited");
					return;
				}
			}
			const forward = (): void => {
				const up = request({ host: t.hostname, port: t.port, method: req.method, path: req.url, headers: req.headers }, (ur) => {
					if (drop) {
						p.dropped++;
						ur.resume();
						ur.on("end", () => res.socket?.destroy());
						return;
					}
					res.writeHead(ur.statusCode ?? 502, ur.headers);
					ur.pipe(res);
				});
				up.on("error", () => res.destroy());
				req.pipe(up);
			};
			if (delay > 0) setTimeout(forward, delay);
			else forward();
		});
		const port = await new Promise<number>((res) =>
			server.listen(0, "127.0.0.1", () => {
				const a = server.address();
				res(typeof a === "object" && a !== null ? a.port : 0);
			}),
		);
		proxy = new StallProxy(server, `http://127.0.0.1:${port}`);
		return proxy;
	}

	reset(): void {
		this.posts = 0;
		this.stallAfter = Number.POSITIVE_INFINITY;
		this.dropAfter = Number.POSITIVE_INFINITY;
		this.limitAt = Number.POSITIVE_INFINITY;
		this.delayMs = 0;
	}

	/** Resolves when the proxy receives its next POST, before it forwards, holds or delays it. */
	nextPost(): Promise<void> {
		return new Promise((resolve) => this.onPost.push(resolve));
	}

	async close(): Promise<void> {
		this.server.closeAllConnections();
		await new Promise<void>((r) => this.server.close(() => r()));
	}
}

/** How the gate answers the writes it refuses: `402` without extra headers unless set. */
export type Refusal = { status?: number; headers?: Record<string, string> };

/** What `gate.mjs` did so far (see {@link Gate.counts}). */
export type GateCounts = { refused: number; forwarded: number; dropped: number; replaced: number; droppedStatuses: number[]; replacedStatuses: number[] };

/**
 * An HTTP layer in front of the node that refuses writes on demand (`gate.mjs`), with `402 Payment
 * Required` by default, the way an authorizer or a quota or billing service may, or with any status
 * and headers. It runs in a process of its own, so this process can attach and commit through it
 * synchronously.
 */
export class Gate {
	private readonly proc: ChildProcess;
	readonly url: string;

	private constructor(proc: ChildProcess, url: string) {
		this.proc = proc;
		this.url = url;
	}

	static async start(target: string): Promise<Gate> {
		const proc = spawn(process.execPath, [gateScript, target], { stdio: ["ignore", "pipe", "inherit"] });
		const port = await new Promise<number>((res, rej) => {
			let buf = "";
			proc.stdout?.on("data", (chunk: Buffer) => {
				buf += chunk.toString();
				const i = buf.indexOf("\n");
				if (i >= 0) res((JSON.parse(buf.slice(0, i)) as { port: number }).port);
			});
			proc.once("exit", (code) => rej(new Error(`the gate exited (${code})`)));
		});
		return new Gate(proc, `http://127.0.0.1:${port}`);
	}

	/**
	 * Answers writes `status` (`402` by default) with `headers` and `body` from now on, without
	 * forwarding them: `limit` of them at most, and only those with one of `methods` (POST and PUT by
	 * default), with `over`, a body of more than `over` bytes, and with `path`, a path that contains
	 * it. `null` forwards them again.
	 */
	async refuse(body: string | null, options: { limit?: number; methods?: string[]; over?: number; path?: string } & Refusal = {}): Promise<void> {
		await this.control({ refuse: body, ...options });
	}

	/** Forwards the next POST but cuts its answer (its outcome stays unknown to the client), then refuses the next `limit` writes with `body` (and `refusal`'s status and headers, see `refuse`). */
	async dropNextPostThenRefuse(body: string, limit: number, refusal: Refusal = {}): Promise<void> {
		await this.control({ dropNextPost: { refuse: body, limit, ...refusal } });
	}

	/**
	 * Forwards the next POST and answers it with `body` (and `refusal`'s status and headers, see `refuse`)
	 * in place of the node's answer, then refuses the next `limit` writes the same way. It breaks the rule
	 * for refusals: the client is told that a write the stream applied was not applied.
	 */
	async forwardNextPostThenRefuse(body: string, limit: number, refusal: Refusal = {}): Promise<void> {
		await this.control({ forwardNextPost: { refuse: body, limit, ...refusal } });
	}

	/** Forwards every request again, and drops an armed `dropNextPostThenRefuse` or `forwardNextPostThenRefuse`. */
	async reset(): Promise<void> {
		await this.control({ refuse: null, dropNextPost: null, forwardNextPost: null });
	}

	/** Requests refused, forwarded, forwarded with their answer cut, and forwarded with their answer replaced by a refusal, so far, and the node's status for each answer cut or replaced, in order. */
	async counts(): Promise<GateCounts> {
		const r = await fetch(`${this.url}/__gate`);
		return (await r.json()) as GateCounts;
	}

	private async control(body: object): Promise<void> {
		const r = await fetch(`${this.url}/__gate`, { method: "PUT", body: JSON.stringify(body) });
		if (!r.ok) throw new Error(`gate control: ${r.status}`);
	}

	async close(): Promise<void> {
		if (this.proc.exitCode !== null || this.proc.signalCode !== null) return;
		const exited = new Promise((r) => this.proc.once("exit", r));
		this.proc.kill();
		await exited;
	}
}

const listen = (server: Server): Promise<number> =>
	new Promise((res) => server.listen(0, "127.0.0.1", () => res((server.address() as AddressInfo).port)));

const b64 = (o: object): string => Buffer.from(JSON.stringify(o)).toString("base64url");

/**
 * An `ursula gateway` in front of `upstreams`, with `args` after its own (`--max-request-body-bytes`,
 * say), started in `cwd`: up once it answers. The caller kills `proc`.
 */
export async function startGateway(upstreams: readonly string[], args: readonly string[] = [], cwd?: string): Promise<{ url: string; proc: ChildProcess }> {
	const probe = createServer();
	const port = await listen(probe);
	await new Promise((res) => probe.close(res));
	const all = ["gateway", "--listen", `127.0.0.1:${port}`, "--raft-group-count", process.env.E2E_GROUPS ?? "4"];
	for (const upstream of upstreams) all.push("--upstream", upstream);
	const proc = spawn(resolve(process.env.URSULA_BIN ?? ""), [...all, ...args], { cwd, stdio: "ignore" });
	const url = `http://127.0.0.1:${port}`;
	for (let i = 0; ; i++) {
		const up = await fetch(`${url}/`).then(
			() => true,
			() => false,
		);
		if (up) return { url, proc };
		if (i > 100 || proc.exitCode !== null) {
			proc.kill("SIGKILL");
			throw new Error("the gateway did not start");
		}
		await new Promise((r) => setTimeout(r, 100));
	}
}

/**
 * An `ursula gateway` in front of `upstreams` that checks bearer tokens: RFC 9068 access tokens from
 * a JWKS this process serves. The bucket `sqlite-e2e` belongs to the tokens `mint` makes, and the
 * gateway hides its streams from a request without one (404).
 */
export class AuthGateway {
	static readonly audience = "https://streams.e2e";
	readonly url: string;
	private readonly issuer: string;
	private readonly key: KeyObject;
	private readonly proc: ChildProcess;
	private readonly jwks: Server;

	private constructor(url: string, issuer: string, key: KeyObject, proc: ChildProcess, jwks: Server) {
		this.url = url;
		this.issuer = issuer;
		this.key = key;
		this.proc = proc;
		this.jwks = jwks;
	}

	/** Starts the gateway, with `args` after its own, its JWKS and its policy file (written to `dir`). */
	static async start(upstreams: readonly string[], dir: string, args: readonly string[] = []): Promise<AuthGateway> {
		const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
		const jwks = createServer((_, res) => {
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify({ keys: [{ ...publicKey.export({ format: "jwk" }), kid: "e2e", alg: "RS256", use: "sig" }] }));
		});
		const issuer = `http://127.0.0.1:${await listen(jwks)}`;
		const policy = join(dir, `policy-${randomUUID()}.toml`);
		writeFileSync(policy, `[[bucket]]\nid = "sqlite-e2e"\nowners = [{ issuer = "${issuer}", subject = "vfs-e2e" }]\n`);
		const auth = ["--auth-issuer", issuer, "--auth-audience", AuthGateway.audience, "--auth-policy", policy, "--auth-jwks-url", `${issuer}/jwks.json`];
		try {
			const { url, proc } = await startGateway(upstreams, [...auth, ...args], dir);
			return new AuthGateway(url, issuer, privateKey, proc, jwks);
		} catch (e) {
			jwks.close();
			throw e;
		}
	}

	/** An access token the gateway accepts, valid for an hour. */
	mint(): string {
		const now = Math.floor(Date.now() / 1000);
		const head = b64({ alg: "RS256", typ: "at+jwt", kid: "e2e" });
		const claims = b64({ iss: this.issuer, sub: "vfs-e2e", aud: AuthGateway.audience, client_id: "vfs-e2e", iat: now, exp: now + 3600, jti: randomUUID() });
		return `${head}.${claims}.${sign("sha256", Buffer.from(`${head}.${claims}`), this.key).toString("base64url")}`;
	}

	close(): void {
		this.proc.kill("SIGKILL");
		this.jwks.close();
	}
}
