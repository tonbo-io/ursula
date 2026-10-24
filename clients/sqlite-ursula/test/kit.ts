import { type ChildProcess, spawn } from "node:child_process";
import { appendFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { expect, inject } from "vitest";
import { call } from "../src/control.ts";

/** The base URL of the server the suite runs against (test/stack.ts). */
export const ursulaUrl = (): string => inject("ursulaUrl");

/**
 * The server with no gate in front, for the reads and writes a test makes itself: they never take
 * a fault scripted for the database's requests.
 */
export const directUrl = (): string => inject("ursulaDirect");

/** Whether the suite runs against the model server (E2E_SERVER=model). */
export const onModel = (): boolean => inject("ursulaServer") === "model";

/** The extension the suites load. */
export const ext = (): string => {
	const path = process.env.SQLITE_URSULA_VFS;
	if (path === undefined || path.length === 0) throw new Error("set SQLITE_URSULA_VFS");
	return path;
};

let n = 0;
/** A stream of its own for each test, in the bucket `sqlite-e2e` (test/stack.ts creates it): its URL and its path on the server. */
export function stream(label: string): { url: string; path: string } {
	n += 1;
	const path = `/sqlite-e2e/${label}-${process.pid}-${Date.now().toString(36)}-${n}`;
	return { url: `${ursulaUrl()}${path}`, path };
}

/** A new cache root. */
export const freshDir = (): string => mkdtempSync(join(tmpdir(), "sqlite-ursula-e2e-"));

/**
 * Every byte of the stream at `path`: the model's `/__model/frames`, or a read of the stream from
 * `from` (its start by default, otherwise a frame boundary such as its retained offset) to its tail.
 */
export async function streamBytes(path: string, from = "-1"): Promise<Uint8Array> {
	if (onModel()) return new Uint8Array(await (await fetch(`${directUrl()}/__model/frames?path=${path}`)).arrayBuffer());
	const chunks: Uint8Array[] = [];
	for (let offset = from; ; ) {
		const r = await fetch(`${directUrl()}${path}?offset=${offset}`);
		expect(r.status, `read ${path} from ${offset}`).toBe(200);
		const chunk = new Uint8Array(await r.arrayBuffer());
		chunks.push(chunk);
		const next = r.headers.get("stream-next-offset");
		expect(next, `read ${path}: no Stream-Next-Offset`).not.toBeNull();
		if (r.headers.has("stream-up-to-date") || chunk.length === 0 || next === offset) break;
		offset = next as string;
	}
	return new Uint8Array(Buffer.concat(chunks));
}

/** Rows of 64 KiB of random bytes (table `t(i, b)`): samples of each, so equal rows are equal bytes but for chance. */
export const ROWS_SQL = "SELECT i, length(b) AS n, hex(substr(b, 1, 64)) AS head, hex(substr(b, 30000, 64)) AS mid, hex(substr(b, -64)) AS tail FROM t ORDER BY i";
const BLOB = 64 << 10;

/** One transaction inserting `rows` rows of 64 KiB of random bytes into `t(i, b)`, `from` on. */
export function insert(from: number, rows: number): string {
	const values = Array.from({ length: rows }, (_, k) => `(${from + k}, randomblob(${BLOB}))`).join(", ");
	return `BEGIN; INSERT INTO t VALUES ${values}; COMMIT`;
}

/** The `USQ2` frames the stream at `path` holds, by kind (1 claim, 2 commit). */
export async function frames(path: string): Promise<{ claims: number; commits: number }> {
	const bytes = await streamBytes(path);
	const view = new DataView(bytes.buffer);
	const count = { claims: 0, commits: 0 };
	for (let at = 0; at < bytes.length; ) {
		expect(new TextDecoder().decode(bytes.subarray(at, at + 4))).toBe("USQ2");
		const kind = bytes[at + 4];
		const len = view.getUint32(at + 5, true);
		if (kind === 1) count.claims += 1;
		if (kind === 2) count.commits += 1;
		at += 13 + len;
	}
	return count;
}

/** The stream's retained offset, tail and latest snapshot offset (`null` with none), from a HEAD. */
export async function head(path: string): Promise<{ retained: number; tail: number; snapshot: number | null }> {
	const r = await fetch(`${directUrl()}${path}`, { method: "HEAD" });
	expect(r.status).toBe(200);
	const tail = r.headers.get("stream-next-offset");
	expect(tail).not.toBeNull();
	const snapshot = r.headers.get("stream-snapshot-offset");
	return { retained: Number(r.headers.get("stream-retained-offset") ?? "0"), tail: Number(tail), snapshot: snapshot === null ? null : Number(snapshot) };
}

/**
 * A `USQ2` frame decoded as far as its owner (design: Frames): a CLAIM names its epoch and
 * activation, a COMMIT or PART its commit id `(lineage, position)`. Ids are lowercase hex, as
 * `ursula_status` spells a lineage.
 */
export type OwnedFrame = { kind: "claim"; epoch: number; activation: string } | { kind: "commit" | "part"; lineage: string; position: number };

/** The frames the stream at `path` holds as far as it is retained, in stream order, each with its owner. */
export async function ownedFrames(path: string): Promise<OwnedFrame[]> {
	const { retained } = await head(path);
	const bytes = await streamBytes(path, retained === 0 ? "-1" : String(retained));
	const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	const hex = (from: number): string => Buffer.from(bytes.subarray(from, from + 16)).toString("hex");
	const out: OwnedFrame[] = [];
	for (let at = 0; at < bytes.length; at += 13 + view.getUint32(at + 5, true)) {
		expect(new TextDecoder().decode(bytes.subarray(at, at + 4))).toBe("USQ2");
		const kind = bytes[at + 4];
		const body = at + 13;
		if (kind === 1) out.push({ kind: "claim", epoch: Number(view.getBigUint64(body, true)), activation: hex(body + 8) });
		else if (kind === 2 || kind === 3) out.push({ kind: kind === 2 ? "commit" : "part", lineage: hex(body), position: Number(view.getBigUint64(body + 16, true)) });
		else throw new Error(`frame kind ${kind} at byte ${at} of ${path}`);
	}
	return out;
}

/** No commit id `(lineage, position)` is in the stream at `path` twice, as far as it is retained, and it holds one at least. */
export async function commitsOnce(path: string): Promise<void> {
	const ids = (await ownedFrames(path)).flatMap((f) => (f.kind === "commit" ? [`${f.lineage}:${f.position}`] : []));
	expect(ids.length).toBeGreaterThan(0);
	expect(ids.filter((id, i) => ids.indexOf(id) !== i)).toEqual([]);
}

/** Reports a line to the console and, in CI, to the job summary. */
export function report(line: string): void {
	process.stderr.write(`${line}\n`);
	const summary = process.env.GITHUB_STEP_SUMMARY;
	if (summary !== undefined && summary.length > 0) appendFileSync(summary, `${line}\n`);
}

/** The `USQ2` PART frames (kind 3) the stream at `path` holds. */
export async function parts(path: string): Promise<number> {
	const bytes = await streamBytes(path);
	const view = new DataView(bytes.buffer);
	let count = 0;
	for (let at = 0; at < bytes.length; at += 13 + view.getUint32(at + 5, true)) {
		if (bytes[at + 4] === 3) count += 1;
	}
	return count;
}

/** Where {@link fault} scripts a fault: the model's `/__model/faults`, or the gate's `/faults` (test/stack.ts). */
export const faultsUrl = (): string => inject("ursulaFaults");

/**
 * Scripts a fault for the next `times` requests of `method` to the server: at the model
 * (clients/sqlite-vfs/src/model/faults.rs), or at the gate in front of Ursula (test/gate.mjs), which
 * scripts `delay <ms>` only.
 */
export async function fault(line: string, times = 1, method?: string): Promise<void> {
	const query = `times=${times}${method === undefined ? "" : `&method=${method}`}`;
	const r = await fetch(`${faultsUrl()}?${query}`, { method: "POST", body: line });
	expect(r.status, await r.text()).toBe(204);
}

/** An append to the stream at `path` by another writer: `body` with no producer headers. */
export async function foreignAppend(path: string, body: string): Promise<void> {
	const r = onModel()
		? await fetch(`${directUrl()}/__model/foreign?path=${path}`, { method: "POST", body })
		: await fetch(`${directUrl()}${path}`, { method: "POST", headers: { "content-type": "application/octet-stream" }, body });
	expect(r.ok, `${r.status} ${await r.text()}`).toBe(true);
}

/**
 * Waits until the binding of the db file at `path` retired: after its last connection closed it
 * drains first, and an open of its stream before then takes it back, files as they were.
 */
export async function retired(path: string): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (call<{ state: string }>("SELECT ursula_status(?)", path).state !== "retired") {
		expect(Date.now(), `${path} never retired`).toBeLessThan(deadline);
		await new Promise((r) => setTimeout(r, 20));
	}
}

/** Closes the stream at `path`: the model's `/__model/close`, or a close-only append. */
export async function closeStream(path: string): Promise<void> {
	const r = onModel()
		? await fetch(`${directUrl()}/__model/close?path=${path}`, { method: "POST" })
		: await fetch(`${directUrl()}${path}`, {
				method: "POST",
				headers: { "content-type": "application/vnd.ursula.sqlite-vfs.v2", "stream-closed": "true" },
				body: "",
			});
	expect(r.ok, `${r.status} ${await r.text()}`).toBe(true);
}

/** The error `promise` rejects with (it must reject). */
export async function rejection(promise: Promise<unknown>): Promise<unknown> {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error("it resolved");
}

/** The error `f` throws (it must throw). */
export function thrown(f: () => unknown): unknown {
	try {
		f();
	} catch (error) {
		return error;
	}
	throw new Error("it did not throw");
}

/** `ursula_open`'s answer for a bound database. */
export interface Opened {
	done: boolean;
	path: string;
	name: string;
	state: string;
}

/** Every child process a test started and has not stopped: {@link killChildren} kills them. */
const children = new Set<Child>();

/** Kills every child process still running: register it with `afterEach`. */
export async function killChildren(): Promise<void> {
	for (const c of [...children]) await c.stop("SIGKILL");
}

/**
 * A Node process of a test: test/`script` (test/core-child.mjs by default) with the extension, `url`,
 * `dir` and `args` as its arguments, read one JSON line at a time. core-child.mjs binds `url` in
 * `dir` and answers one JSON command per line.
 */
export class Child {
	private readonly proc: ChildProcess;
	private readonly queue: string[] = [];
	private waiting: ((line: string | undefined) => void) | undefined;
	private ended = false;
	/** How the process ended. */
	readonly exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;

	constructor(url: string, dir: string, env: NodeJS.ProcessEnv = {}, script = "core-child.mjs", args: readonly string[] = []) {
		this.proc = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, script), ext(), url, dir, ...args], {
			stdio: ["pipe", "pipe", "inherit"],
			env: { ...process.env, ...env },
		});
		this.exit = new Promise((r) => this.proc.once("exit", (code, signal) => r({ code, signal })));
		// A child that exited on its own closed its stdin: a later write fails, harmlessly.
		this.proc.stdin?.on("error", () => {});
		const lines = createInterface({ input: this.proc.stdout as NodeJS.ReadableStream });
		lines.on("line", (line) => this.deliver(line));
		lines.on("close", () => {
			this.ended = true;
			this.deliver(undefined);
		});
		children.add(this);
	}

	private deliver(line: string | undefined): void {
		const waiting = this.waiting;
		this.waiting = undefined;
		if (waiting !== undefined) waiting(line);
		else if (line !== undefined) this.queue.push(line);
	}

	/** The next line, or `undefined` once the process closed its stdout. */
	async line(timeoutMs = 60_000): Promise<Record<string, unknown> | undefined> {
		const queued = this.queue.shift();
		if (queued !== undefined) return JSON.parse(queued) as Record<string, unknown>;
		if (this.ended) return undefined;
		const line = await new Promise<string | undefined>((res, rej) => {
			const timer = setTimeout(() => rej(new Error("the child did not answer")), timeoutMs);
			this.waiting = (l) => {
				clearTimeout(timer);
				res(l);
			};
		});
		return line === undefined ? undefined : (JSON.parse(line) as Record<string, unknown>);
	}

	private async next(): Promise<Record<string, unknown>> {
		const line = await this.line();
		if (line === undefined) throw new Error("the child exited");
		return line;
	}

	/** The open's outcome, the first line, as it came. */
	first(): Promise<Record<string, unknown>> {
		return this.next();
	}

	/** The open's outcome, the first line. */
	async opened(): Promise<Opened> {
		const reply = await this.next();
		expect(reply.ok, JSON.stringify(reply)).toBe(true);
		return reply.opened as Opened;
	}

	async send(cmd: Record<string, unknown>): Promise<Record<string, unknown>> {
		this.proc.stdin?.write(`${JSON.stringify(cmd)}\n`);
		return this.next();
	}

	async exec(sql: string): Promise<void> {
		const reply = await this.send({ exec: sql });
		expect(reply.ok, JSON.stringify(reply)).toBe(true);
	}

	async rows(sql: string): Promise<unknown[]> {
		const reply = await this.send({ all: sql });
		expect(reply.ok, JSON.stringify(reply)).toBe(true);
		return reply.rows as unknown[];
	}

	/**
	 * Sends `signal` to the process and returns at once: `SIGSTOP` freezes it with its sockets
	 * open, as a paused VM, and `SIGCONT` wakes it. {@link stop} still ends it.
	 */
	signal(signal: NodeJS.Signals): void {
		this.proc.kill(signal);
	}

	/** Exits, cleanly (`exit`) or killed (`SIGKILL`). */
	async stop(how: "exit" | "SIGKILL" = "exit"): Promise<void> {
		if (this.proc.exitCode === null && this.proc.signalCode === null) {
			if (how === "exit") this.proc.stdin?.write(`${JSON.stringify({ exit: true })}\n`);
			else this.proc.kill("SIGKILL");
			await this.exit;
		}
		children.delete(this);
	}
}

/** The rows `sql` returns from a fresh build of the stream: a new process with a new cache root, and `env`. */
export async function fromStream(url: string, sql: string, env: NodeJS.ProcessEnv = {}): Promise<unknown[]> {
	const reader = new Child(url, freshDir(), env);
	await reader.opened();
	const rows = await reader.rows(sql);
	await reader.stop();
	return rows;
}

/** A process a test started that prints one JSON line once it listens and exits when its stdin closes. */
async function listening(bin: string, args: readonly string[]): Promise<{ line: Record<string, unknown>; stop: () => Promise<void> }> {
	const proc = spawn(bin, [...args], { stdio: ["pipe", "pipe", "inherit"] });
	const exited = new Promise((r) => proc.once("exit", r));
	// A binary that cannot start (missing, not executable) fails here instead of hanging.
	const failed = new Promise<never>((_, reject) => proc.once("error", (error) => reject(new Error(`${bin} did not start: ${error.message}`))));
	const lines = createInterface({ input: proc.stdout as NodeJS.ReadableStream })[Symbol.asyncIterator]();
	const first = await Promise.race([lines.next(), failed]);
	if (first.done === true) throw new Error(`${bin} exited before it listened`);
	const stop = async (): Promise<void> => {
		if (proc.exitCode === null && proc.signalCode === null) {
			proc.stdin?.end();
			await exited;
		}
	};
	return { line: JSON.parse(first.value as string) as Record<string, unknown>, stop };
}

/**
 * A model server of its own, of the deployment `clusterId`, drawing its history ids from `seed`:
 * the binary test/stack.ts runs, from URSULA_VFS_MODEL or the crate's release build.
 */
export async function modelServer(clusterId: string, seed: number): Promise<{ url: string; stop: () => Promise<void> }> {
	const bin = process.env.URSULA_VFS_MODEL ?? join(import.meta.dirname, "../../sqlite-vfs/target/release/ursula-vfs-model");
	const { line, stop } = await listening(bin, ["--exit-with-stdin", "--cluster-id", clusterId, "--seed", String(seed)]);
	return { url: `http://127.0.0.1:${line.port as number}`, stop };
}

/** test/gate.mjs in front of `url`: its proxy URL, its control URL and a stop. */
export async function gate(url: string): Promise<{ url: string; control: string; stop: () => Promise<void> }> {
	const { line, stop } = await listening(process.execPath, ["--no-warnings", join(import.meta.dirname, "gate.mjs"), url]);
	return { url: `http://127.0.0.1:${line.port as number}`, control: `http://127.0.0.1:${line.control as number}`, stop };
}
