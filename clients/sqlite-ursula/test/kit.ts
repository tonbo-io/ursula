import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { createServer, request, type Server } from "node:http";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { inject } from "vitest";

let counter = 0;
/** A fresh stream URL on the spawned node (bucket created by the global setup). */
export const streamPath = (): string => `/sqlite-e2e/vfs-${process.pid}-${Date.now().toString(36)}-${counter++}`;
export const ursulaUrl = (): string => inject("ursulaUrl");

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
	attached?: number;
	step?: number;
	phase?: string;
	ok?: boolean;
	error?: string;
	errcode?: number;
	done?: boolean;
	/** On `done`: append attempts per commit, whether the file is poisoned, its stream offset. */
	attempts?: number[];
	poisoned?: boolean;
	offset?: number;
	epoch?: number;
	/** On `done`: snapshots published, the latest known snapshot and the retention this owner set. */
	snapshots?: number;
	snapshot?: number;
	retained?: number;
};

export interface Child {
	readonly proc: ChildProcess;
	readonly lines: ChildLine[];
	waitFor(pred: (l: ChildLine) => boolean, ms?: number): Promise<ChildLine>;
	readonly exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
	stderr(): string;
}

const childScript = join(dirname(fileURLToPath(import.meta.url)), "child.mjs");

export function runChild(file: string, url: string, sqls: readonly string[], env: Record<string, string> = {}): Child {
	const proc = spawn(process.execPath, [childScript, vfsPath(), file, url, ...sqls], { env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] });
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
				const timer = setTimeout(() => rej(new Error(`child: timed out; lines ${JSON.stringify(lines)}; stderr ${err}`)), ms);
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
 * POST numbered `limitAt` is answered 429 with `Retry-After: 1` without being forwarded.
 */
export class StallProxy {
	private readonly server: Server;
	private posts = 0;
	stallAfter = Number.POSITIVE_INFINITY;
	dropAfter = Number.POSITIVE_INFINITY;
	limitAt = Number.POSITIVE_INFINITY;
	/** POSTs whose answer was dropped. */
	dropped = 0;
	readonly stalled: Promise<void>;
	private onStall!: () => void;
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
			if (req.method === "POST") {
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
	}

	async close(): Promise<void> {
		this.server.closeAllConnections();
		await new Promise<void>((r) => this.server.close(() => r()));
	}
}
