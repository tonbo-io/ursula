// Back-to-back large transactions (docs/architecture/sqlite-vfs.md §9): a 5 MB table, 2,000-row
// updates (frames of about 1 MB), no pause. Reports commit latency, snapshots taken, and the
// retained log (tail minus retention, from the stream's HEAD every 10 s; this tool reads the
// server's offsets as numbers, which the VFS itself never does).
//
//   node bench/bigtx.ts <base url> <label> <duration s> <out dir>
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { attach, drainStats, status } from "../src/index.ts";
import { freshFile } from "../test/helpers.ts";

if (process.argv.length < 6) throw new Error("usage: bigtx.ts <base url> <label> <duration s> <out dir>");
const [base, label, durationArg, outDir] = process.argv.slice(2) as [string, string, string, string];
const url = `${base.replace(/\/+$/, "")}/vfs-bench/${label}-${Date.now().toString(36)}`;
const file = freshFile();
attach(file, url);
const db = new DatabaseSync(file);
db.exec("PRAGMA journal_mode=WAL");
db.exec("CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT)");
db.exec("BEGIN");
for (let k = 0; k < 5000; k++) db.prepare("INSERT INTO t VALUES (?, hex(randomblob(500)))").run(k);
db.exec("COMMIT");
drainStats(file);
const update = db.prepare("UPDATE t SET v = hex(randomblob(500)) WHERE k >= ? AND k < ?");
const retained: number[] = [];
const sampler = setInterval(async () => {
	const r = await fetch(url, { method: "HEAD" });
	const tail = Number(r.headers.get("stream-next-offset"));
	const from = Number(r.headers.get("stream-retained-offset") ?? 0);
	if (Number.isFinite(tail) && Number.isFinite(from)) retained.push(tail - Math.max(0, from));
}, 10_000);
const end = performance.now() + Number(durationArg) * 1000;
const lat: number[] = [];
let written = 0;
let snapshots = 0;
for (let i = 0; performance.now() < end; i++) {
	const from = (i * 2000) % 5000;
	const at = performance.now();
	db.exec("BEGIN");
	update.run(from, from + 2000);
	db.exec("COMMIT");
	lat.push(performance.now() - at);
	const stats = drainStats(file);
	written += stats.commits.reduce((a, c) => a + c.bytes, 0);
	snapshots += stats.snapshots.length;
	// Let the sampler's fetch and the snapshot thread's progress through the event loop.
	await new Promise((r) => setImmediate(r));
}
clearInterval(sampler);
const s = Float64Array.from(lat).sort();
const pct = (p: number): number => Math.round(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] as number);
const st = status(file);
const summary = {
	label,
	url,
	commits: lat.length,
	commit_ms: { p50: pct(50), p99: pct(99), max: Math.round(s[s.length - 1] as number) },
	stream_bytes_written: written,
	snapshots: snapshots + drainStats(file).snapshots.length,
	retained_log_bytes: { max: Math.max(0, ...retained), last: retained.at(-1) ?? null, samples: retained.length },
	poisoned: st.poisoned,
	reason: st.reason,
};
mkdirSync(join(outDir, label), { recursive: true });
writeFileSync(join(outDir, label, "summary.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary));
process.exit(0);
