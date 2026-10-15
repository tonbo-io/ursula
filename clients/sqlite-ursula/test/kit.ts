import { type ChildProcess, spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { expect, inject } from "vitest";

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
 * its start to its tail.
 */
async function streamBytes(path: string): Promise<Uint8Array> {
	if (onModel()) return new Uint8Array(await (await fetch(`${directUrl()}/__model/frames?path=${path}`)).arrayBuffer());
	const chunks: Uint8Array[] = [];
	for (let offset = "-1"; ; ) {
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

/** The rows `sql` returns from a fresh build of the stream: a new process with a new cache root. */
export async function fromStream(url: string, sql: string): Promise<unknown[]> {
	const reader = new Child(url, freshDir());
	await reader.opened();
	const rows = await reader.rows(sql);
	await reader.stop();
	return rows;
}
