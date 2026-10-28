// Commit latency on the VFS against B1 (design: Verification and Performance, and a CI gate:
// the VFS's commit p50 within 0.5 ms of B1's, plain SQLite). B1 here is plain node:sqlite with B1's
// settings: the 0.7 measurements (docs/architecture/sqlite-vfs.md, Performance) ran Pi's own
// storage with the same settings on an EKS pod, which this bench does not reproduce. Both arms run
// the same workload in one process, alternating rounds so drift and noise fall on both alike:
// - B1: plain node:sqlite on a local file, no extension (WAL, synchronous=NORMAL). It is opened
//   before the extension loads, so it stays on SQLite's own unix VFS.
// - VFS: the package's `open()` against the model server (ursula-vfs-model, started here), or the
//   Ursula at BENCH_URL, which it replicates to in the background meanwhile. Its commit is local, so
//   the rounds do not wait for `synced()`.
// A commit is a Pi-shaped transaction: BEGIN, an entry row of about 4.5 KB of text, its
// conversation's head row, COMMIT, timed from BEGIN to COMMIT's return.
//
// With BENCH_HOLD=1 the VFS arm runs with a host directory holding the host's `hold` file, which no
// host takes (design: Hosts): each write transaction then tries the hold before its WAL write lock.
// The directory is a temporary one the test hook URSULA_VFS_HOST_DIR names, set before the
// extension loads.
//
// A last phase times `synced()` on the VFS: one commit at a time, each followed by
// `await db.synced()`, timed from COMMIT's return to `synced()` resolving. This is how long a host
// waits before an external effect, such as a Pi commit resolving, with nothing else in flight.
//
//   SQLITE_URSULA_VFS=<extension> [URSULA_VFS_MODEL=<model binary> | BENCH_URL=<ursula URL>] node bench/commit.ts
//
// Settings: BENCH_ROUNDS (10), BENCH_COMMITS per round and arm (500), BENCH_WARMUP commits per arm
// (300), BENCH_PACE_MS between commits (0, flat out), BENCH_SYNCED commits in the `synced()` phase
// (500), BENCH_HOLD (0), BENCH_MAX_DELTA_MS (0.5): the run fails when p50_vfs - p50_B1 exceeds it. The `synced()`
// timings are reported, never a failure. Prints one JSON summary, and appends a table to GITHUB_STEP_SUMMARY.
import { type ChildProcess, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readlinkSync, realpathSync, writeFileSync } from "node:fs";
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
const SYNCED = setting("BENCH_SYNCED", 500);
const MAX_DELTA_MS = setting("BENCH_MAX_DELTA_MS", 0.5);
const HOLD = setting("BENCH_HOLD", 0) === 1;
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
	/** Runs one commit, and returns how long it took in ms. */
	commitOne(): number {
		this.n += 1;
		const text = body(this.n);
		const conv = this.n % 8;
		const started = performance.now();
		this.begin.run();
		this.entry.run(conv, text);
		this.head.run(conv, this.n);
		this.commit.run();
		return performance.now() - started;
	}
	/** Runs `count` commits, keeping their latencies unless `warmup`. */
	async run(count: number, warmup = false): Promise<void> {
		for (let k = 0; k < count; k++) {
			const elapsed = this.commitOne();
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

const benchUrl = (process.env.BENCH_URL ?? "").replace(/\/+$/, "");
const model = benchUrl === "" ? await startModel() : undefined;
/** The stream's bucket: Ursula's must exist (and its ids are 4 to 64 bytes). */
const bucket = model === undefined ? `${benchUrl}/sqlite-bench` : `${model.url}/bench`;
if (model === undefined) {
	const r = await fetch(bucket, { method: "PUT" });
	if (!r.ok && r.status !== 409) throw new Error(`create the bucket ${bucket}: ${r.status} ${await r.text()}`);
}
/** The host directory of BENCH_HOLD. */
let holdHost: string | undefined;
if (HOLD) {
	// The hook is read only when the compiled-in host directory does not exist.
	if (existsSync("/var/lib/sqlite-ursula/host")) throw new Error("BENCH_HOLD needs a host without /var/lib/sqlite-ursula/host");
	holdHost = join(realpathSync(mkdtempSync(join(tmpdir(), "sqlite-ursula-bench-host-"))), "host");
	mkdirSync(holdHost);
	writeFileSync(join(holdHost, "hold"), "");
	process.env.URSULA_VFS_HOST_DIR = holdHost;
}
const control = loadUrsulaVfs(ext);
const vfsDb = await open(`${bucket}/commit-${process.pid}`, { dir: mkdtempSync(join(tmpdir(), "sqlite-ursula-bench-vfs-")) });
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
if (holdHost !== undefined) {
	// The arm measured the hold path: the open recorded its cache directory in the host directory,
	// and the extension holds `hold` open, which it opens at the first write that may wait and keeps
	// (a file it cannot open logs `write_hold_failed` and is never waited at).
	if (!readdirSync(holdHost).some((n) => n.startsWith("dir-"))) throw new Error(`BENCH_HOLD: the open recorded nothing in ${holdHost}`);
	const holdFile = join(holdHost, "hold");
	const held = readdirSync("/proc/self/fd").some((fd) => {
		try {
			return readlinkSync(join("/proc/self/fd", fd)) === holdFile;
		} catch {
			return false;
		}
	});
	if (!held) throw new Error(`BENCH_HOLD: the extension did not open ${holdFile}: see write_hold_failed on stderr`);
}
// The synced() phase: one commit at a time, its synced() timed from COMMIT's return.
const synced: number[] = [];
for (let k = 0; k < SYNCED; k++) {
	vfs.commitOne();
	const started = performance.now();
	await vfsDb.synced({ signal: AbortSignal.timeout(60_000) });
	synced.push(performance.now() - started);
}
const { commits, reset } = vfsDb.status();
if (reset !== null || commits.acknowledged !== commits.logged) throw new Error(`the VFS database did not end acknowledged: ${JSON.stringify(vfsDb.status())}`);
vfsDb.close();
plain.close();
model?.proc.stdin?.end();

const summary = {
	commits_per_arm: b1.samples.length,
	b1_ms: dist(b1.samples),
	vfs_ms: dist(vfs.samples),
	delta_p50_ms: round3(pct(vfs.samples, 50) - pct(b1.samples, 50)),
	max_delta_p50_ms: MAX_DELTA_MS,
	// From COMMIT's return to synced() resolving, one commit at a time.
	synced_commits: synced.length,
	synced_ms: dist(synced),
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
	server: model === undefined ? "ursula" : "model",
	// A hold file present and not taken (BENCH_HOLD).
	hold: HOLD,
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
			`### Commit latency, VFS${HOLD ? " with a hold file present" : ""} against B1 (${summary.commits_per_arm} commits per arm, ${summary.server} server, ${summary.platform}, Node ${summary.node})`,
			"",
			"| arm | p50 ms | p90 ms | p99 ms | max ms |",
			"| --- | --- | --- | --- | --- |",
			row("B1 (plain node:sqlite, WAL, NORMAL)", summary.b1_ms),
			row(`VFS (open(), commit only${HOLD ? ", hold file present" : ""})`, summary.vfs_ms),
			row("VFS hook (capture, encode, log)", summary.vfs_hook_ms),
			row(`VFS synced() after a commit (${summary.synced_commits}, one at a time)`, summary.synced_ms),
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
