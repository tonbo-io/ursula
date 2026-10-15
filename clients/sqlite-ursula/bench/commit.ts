// Commit latency on the VFS against B1 (M1 exit criterion: the VFS's commit p50 within 0.5 ms of
// B1's; docs/architecture/sqlite-vfs.md §9 defines B1). Both arms run the same workload in one
// process, alternating rounds so drift and noise fall on both alike:
// - B1: plain node:sqlite on a local file, no extension (WAL, synchronous=NORMAL). It is opened
//   before the extension loads, so it stays on SQLite's own unix VFS.
// - VFS: the package's `open()` against the model server (ursula-vfs-model, started here), which
//   replicates in the background meanwhile. Its commit is local, so `synced()` is not timed.
// A commit is a Pi-shaped transaction: BEGIN, an entry row of about 4.5 KB of text, its
// conversation's head row, COMMIT, timed from BEGIN to COMMIT's return.
//
//   SQLITE_URSULA_VFS=<extension> [URSULA_VFS_MODEL=<model binary>] node bench/commit.ts
//
// Settings: BENCH_ROUNDS (10), BENCH_COMMITS per round and arm (500), BENCH_WARMUP commits per arm
// (300), BENCH_PACE_MS between commits (0, flat out), BENCH_MAX_DELTA_MS (0.5): the run fails when
// p50_vfs - p50_B1 exceeds it. Prints one JSON summary, and appends a table to GITHUB_STEP_SUMMARY.
import { type ChildProcess, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import { loadUrsulaVfs, open } from "../src/index.ts";

const setting = (name: string, fallback: number): number => Number(process.env[name] ?? fallback);
const ROUNDS = setting("BENCH_ROUNDS", 10);
const COMMITS = setting("BENCH_COMMITS", 500);
const WARMUP = setting("BENCH_WARMUP", 300);
const PACE_MS = setting("BENCH_PACE_MS", 0);
const MAX_DELTA_MS = setting("BENCH_MAX_DELTA_MS", 0.5);
const ext = process.env.SQLITE_URSULA_VFS;
if (ext === undefined || ext.length === 0) throw new Error("set SQLITE_URSULA_VFS to the extension");
const modelBin = resolve(process.env.URSULA_VFS_MODEL ?? join(import.meta.dirname, "../../sqlite-vfs/target/release/ursula-vfs-model"));

/** The model server: its URL, once it printed its port. */
async function startModel(): Promise<{ url: string; proc: ChildProcess }> {
	const proc = spawn(modelBin, ["--exit-with-stdin"], { stdio: ["pipe", "pipe", "inherit"] });
	process.once("exit", () => proc.kill("SIGKILL"));
	const lines = createInterface({ input: proc.stdout as NodeJS.ReadableStream })[Symbol.asyncIterator]();
	const first = await lines.next();
	if (first.done === true) throw new Error(`${modelBin} exited before it printed its port`);
	return { url: `http://127.0.0.1:${(JSON.parse(first.value) as { port: number }).port}`, proc };
}

const WORDS = "the a session tool call result assistant user message content text token stream commit entry durable replay".split(" ");
/** An entry body of about 4.5 KB, JSON-shaped like a Pi entry, from a seeded generator. */
function body(n: number): string {
	let x = n * 2654435761;
	const words: string[] = [];
	let length = 0;
	while (length < 4400) {
		x = (x * 1103515245 + 12345) % 2147483648;
		const word = WORDS[x % WORDS.length] as string;
		words.push(word);
		length += word.length + 1;
	}
	return JSON.stringify({ type: "message", role: n % 2 === 0 ? "user" : "assistant", id: randomBytes(8).toString("hex"), text: words.join(" ") });
}

/** One database's prepared workload. */
class Arm {
	readonly name: string;
	readonly db: DatabaseSync;
	readonly samples: number[] = [];
	private readonly begin: StatementSync;
	private readonly entry: StatementSync;
	private readonly head: StatementSync;
	private readonly commit: StatementSync;
	private n = 0;
	constructor(name: string, db: DatabaseSync) {
		this.name = name;
		this.db = db;
		db.exec("PRAGMA synchronous=NORMAL");
		db.exec("CREATE TABLE IF NOT EXISTS entries(id INTEGER PRIMARY KEY, conv INTEGER NOT NULL, body TEXT NOT NULL)");
		db.exec("CREATE INDEX IF NOT EXISTS entries_conv ON entries(conv, id)");
		db.exec("CREATE TABLE IF NOT EXISTS heads(conv INTEGER PRIMARY KEY, seq INTEGER NOT NULL)");
		this.begin = db.prepare("BEGIN IMMEDIATE");
		this.entry = db.prepare("INSERT INTO entries(conv, body) VALUES (?, ?)");
		this.head = db.prepare("INSERT INTO heads(conv, seq) VALUES (?, ?) ON CONFLICT(conv) DO UPDATE SET seq = excluded.seq");
		this.commit = db.prepare("COMMIT");
	}
	/** Runs `count` commits, keeping their latencies unless `warmup`. */
	async run(count: number, warmup = false): Promise<void> {
		for (let k = 0; k < count; k++) {
			this.n += 1;
			const text = body(this.n);
			const conv = this.n % 8;
			const started = performance.now();
			this.begin.run();
			this.entry.run(conv, text);
			this.head.run(conv, this.n);
			this.commit.run();
			const elapsed = performance.now() - started;
			if (!warmup) this.samples.push(elapsed);
			if (PACE_MS > 0) await new Promise((r) => setTimeout(r, PACE_MS));
			else if (k % 50 === 49) await new Promise((r) => setImmediate(r));
		}
	}
}

const pct = (xs: readonly number[], p: number): number => {
	const s = Float64Array.from(xs).sort();
	return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] ?? Number.NaN;
};
const round3 = (x: number): number => Math.round(x * 1000) / 1000;
const dist = (xs: readonly number[]) => ({ p50: round3(pct(xs, 50)), p90: round3(pct(xs, 90)), p99: round3(pct(xs, 99)), max: round3(Math.max(...xs)) });

// B1 first, before the extension loads.
const plainDir = mkdtempSync(join(tmpdir(), "sqlite-ursula-bench-b1-"));
const plain = new DatabaseSync(join(plainDir, "b1.db"));
plain.exec("PRAGMA journal_mode=WAL");
const b1 = new Arm("B1", plain);

const model = await startModel();
const control = loadUrsulaVfs(ext);
const vfsDb = await open(`${model.url}/bench/commit-${process.pid}`, { dir: mkdtempSync(join(tmpdir(), "sqlite-ursula-bench-vfs-")) });
const vfs = new Arm("VFS", vfsDb);
const stats = (): { commits: { log_us: number; vfs_us: number; bytes: number; pages: number }[] } =>
	JSON.parse(Object.values(control.prepare("SELECT ursula_stats(?)").get(vfsDb.boundPath) as Record<string, string>)[0] as string);

await b1.run(WARMUP, true);
await vfs.run(WARMUP, true);
await vfsDb.synced();
stats();
const hook: { log_us: number; vfs_us: number; bytes: number; pages: number }[] = [];
for (let r = 0; r < ROUNDS; r++) {
	// Alternate which arm goes first.
	for (const arm of r % 2 === 0 ? [b1, vfs] : [vfs, b1]) await arm.run(COMMITS);
	hook.push(...stats().commits);
}
await vfsDb.synced({ signal: AbortSignal.timeout(60_000) });
const { commits, reset } = vfsDb.status();
if (reset !== null || commits.acknowledged !== commits.logged) throw new Error(`the VFS database did not end acknowledged: ${JSON.stringify(vfsDb.status())}`);
vfsDb.close();
plain.close();
model.proc.stdin?.end();

const summary = {
	commits_per_arm: b1.samples.length,
	b1_ms: dist(b1.samples),
	vfs_ms: dist(vfs.samples),
	delta_p50_ms: round3(pct(vfs.samples, 50) - pct(b1.samples, 50)),
	max_delta_p50_ms: MAX_DELTA_MS,
	// The VFS's own part of a commit (capture, encode, the log write), and the log write alone.
	vfs_hook_ms: dist(hook.map((c) => c.vfs_us / 1000)),
	log_write_ms: dist(hook.map((c) => c.log_us / 1000)),
	frame_bytes_p50: pct(
		hook.map((c) => c.bytes),
		50,
	),
	pages_p50: pct(
		hook.map((c) => c.pages),
		50,
	),
	node: process.version,
	platform: `${process.platform}-${process.arch}`,
};
const ok = summary.delta_p50_ms <= MAX_DELTA_MS;
console.log(JSON.stringify(summary, null, 2));
const out = process.env.GITHUB_STEP_SUMMARY;
if (out !== undefined && out.length > 0) {
	const row = (name: string, d: ReturnType<typeof dist>): string => `| ${name} | ${d.p50} | ${d.p90} | ${d.p99} | ${d.max} |`;
	appendFileSync(
		out,
		[
			`### Commit latency, VFS against B1 (${summary.commits_per_arm} commits per arm, ${summary.platform}, Node ${summary.node})`,
			"",
			"| arm | p50 ms | p90 ms | p99 ms | max ms |",
			"| --- | --- | --- | --- | --- |",
			row("B1 (plain node:sqlite, WAL, NORMAL)", summary.b1_ms),
			row("VFS (open(), commit only)", summary.vfs_ms),
			row("VFS hook (capture, encode, log)", summary.vfs_hook_ms),
			"",
			`p50_vfs - p50_B1 = ${summary.delta_p50_ms} ms (at most ${MAX_DELTA_MS} ms): ${ok ? "met" : "NOT met"}`,
			"",
		].join("\n"),
	);
}
if (!ok) {
	console.error(`p50_vfs - p50_B1 = ${summary.delta_p50_ms} ms, above ${MAX_DELTA_MS} ms`);
	process.exitCode = 1;
}
