// Child-process helpers for the e2e stack and the drills: spawn with a log file and a bounded tail,
// stop with SIGTERM then SIGKILL, free ports, readiness polling.
import { type ChildProcess, spawn } from "node:child_process";
import { createWriteStream, existsSync } from "node:fs";
import { createServer } from "node:net";
import { resolve } from "node:path";

/** The ursula binary under test: URSULA_BIN, falling back to the repo's release build. */
export function ursulaBinary(): string {
	const fromEnv = process.env.URSULA_BIN;
	if (fromEnv !== undefined && fromEnv.length > 0) return resolve(fromEnv);
	const local = resolve(import.meta.dirname, "../../../../target/release/ursula");
	if (existsSync(local)) return local;
	throw new Error("set URSULA_BIN to an ursula binary (cargo build --release -p ursula --bin ursula)");
}

export function freePort(): Promise<number> {
	return new Promise((res, rej) => {
		const server = createServer();
		server.once("error", rej);
		server.listen(0, "127.0.0.1", () => {
			const address = server.address();
			const port = typeof address === "object" && address !== null ? address.port : 0;
			server.close(() => res(port));
		});
	});
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** A spawned child process with its log file and a bounded in-memory log tail. */
export class Proc {
	readonly child: ChildProcess;
	readonly name: string;
	private tail = "";

	constructor(name: string, bin: string, args: readonly string[], options: { cwd: string; log?: string; env?: NodeJS.ProcessEnv }) {
		this.name = name;
		this.child = spawn(bin, [...args], { cwd: options.cwd, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, ...options.env } });
		const file = options.log === undefined ? undefined : createWriteStream(options.log, { flags: "a" });
		const keep = (chunk: Buffer): void => {
			this.tail = (this.tail + chunk.toString()).slice(-20_000);
			file?.write(chunk);
		};
		this.child.stdout?.on("data", keep);
		this.child.stderr?.on("data", keep);
		this.child.once("exit", () => file?.end());
	}

	get running(): boolean {
		return this.child.exitCode === null && this.child.signalCode === null;
	}

	logs(): string {
		return this.tail;
	}

	/** SIGTERM, then SIGKILL after `graceMs`. */
	async stop(graceMs = 5000): Promise<void> {
		if (!this.running) return;
		const exited = new Promise((r) => this.child.once("exit", r));
		this.child.kill("SIGTERM");
		const timer = setTimeout(() => this.child.kill("SIGKILL"), graceMs);
		await exited;
		clearTimeout(timer);
	}

	/** SIGKILL: a crash. */
	async kill(): Promise<void> {
		if (!this.running) return;
		const exited = new Promise((r) => this.child.once("exit", r));
		this.child.kill("SIGKILL");
		await exited;
	}
}

/** Polls `url` until it answers 2xx, failing early if `proc` exits. */
export async function waitReady(url: string, proc: Proc | undefined, timeoutMs = 60_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	let last = "";
	while (Date.now() < deadline) {
		if (proc !== undefined && !proc.running) throw new Error(`${proc.name} exited (${proc.child.exitCode ?? proc.child.signalCode}) before ${url} was ready:\n${proc.logs()}`);
		try {
			const r = await fetch(url, { signal: AbortSignal.timeout(1000) });
			if (r.ok) return;
			last = `${r.status}`;
		} catch (error) {
			last = String(error);
		}
		await sleep(100);
	}
	throw new Error(`${url} did not become ready within ${timeoutMs} ms (${last}):\n${proc?.logs() ?? ""}`);
}

/** Polls `check` until it returns a non-undefined value. */
export async function until<T>(what: string, check: () => Promise<T | undefined> | T | undefined, timeoutMs = 60_000, intervalMs = 100): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const value = await check();
		if (value !== undefined) return value;
		if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}`);
		await sleep(intervalMs);
	}
}
