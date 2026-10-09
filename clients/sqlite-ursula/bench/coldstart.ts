// Cold start (docs/architecture/sqlite-vfs.md §9): build a database of about `mb` MiB on a fresh
// stream, then attach fresh files to it, as a new host would (snapshot install plus the tail since
// it), three times. A freshly built database has no snapshot yet (one is due once the log outgrows
// the database), so attach replays the whole log; with `snapshot`, rows are rewritten after the
// build until a snapshot is published, and attach installs it.
//
//   node bench/coldstart.ts <base url> <label> <mb> <out dir> [snapshot]
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { attach, drainStats, status } from "../src/index.ts";
import { freshFile } from "../test/helpers.ts";

if (process.argv.length < 6) throw new Error("usage: coldstart.ts <base url> <label> <mb> <out dir> [snapshot]");
const [base, label, mbArg, outDir, mode] = process.argv.slice(2) as [string, string, string, string, string?];
const url = `${base.replace(/\/+$/, "")}/vfs-bench/${label}-${Date.now().toString(36)}`;
const target = Number(mbArg) * 1024 * 1024;
const file = freshFile();
attach(file, url);
const db = new DatabaseSync(file);
db.exec("PRAGMA journal_mode=WAL");
db.exec("CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB)");
// About 1 MiB per commit, rows a third random (snapshots compress about 3:1, as Pi data did).
const insert = db.prepare("INSERT INTO t(v) VALUES (randomblob(300) || zeroblob(724))");
const buildStart = performance.now();
let size = 0;
while (size < target) {
	db.exec("BEGIN");
	for (let i = 0; i < 1000; i++) insert.run();
	db.exec("COMMIT");
	size = (db.prepare("SELECT page_count * page_size AS n FROM pragma_page_count(), pragma_page_size()").get() as { n: number }).n;
}
const buildMs = performance.now() - buildStart;
const rows = (db.prepare("SELECT max(k) AS n FROM t").get() as { n: number }).n;
const rewrite = db.prepare("UPDATE t SET v = randomblob(300) || zeroblob(724) WHERE k > ? AND k <= ?");
let churned = 0;
if (mode === "snapshot") {
	for (let k = 0; status(file).snapshot === "-1"; k = (k + 1000) % rows) {
		rewrite.run(k, k + 1000);
		churned++;
	}
}
// Give the last snapshot time to publish, as a host restarting later would find it.
await new Promise((r) => setTimeout(r, 15_000));
const snapshots = drainStats(file).snapshots;
const s = status(file);
db.close();
// Each attach's time, what it replayed (`log_bytes`: the whole log, or the tail after the snapshot
// it installed) and the snapshot it installed (`"-1"`: none).
const attaches: { ms: number; log_bytes: number; installed: string }[] = [];
for (let i = 0; i < 3; i++) {
	const fresh = freshFile();
	const at = performance.now();
	attach(fresh, url);
	const ms = Math.round(performance.now() - at);
	const { log_bytes, installed } = status(fresh);
	attaches.push({ ms, log_bytes, installed });
	rmSync(fresh, { force: true });
}
const attachMs = attaches.map((a) => a.ms);
const latest = snapshots.at(-1);
const summary = {
	label,
	url,
	db_bytes: size,
	build_s: Math.round(buildMs / 1000),
	rewrite_commits: churned,
	snapshots: snapshots.length,
	latest_snapshot: latest === undefined ? null : { body_bytes: latest.bytes, db_bytes: latest.raw, publish_ms: Math.round(latest.total_us / 1000) },
	snapshot_offset: s.snapshot,
	attach_ms: { first: attachMs[0], best: Math.min(...attachMs), all: attachMs },
	replayed: attaches.map(({ log_bytes, installed }) => ({ log_bytes, installed })),
};
mkdirSync(join(outDir, label), { recursive: true });
writeFileSync(join(outDir, label, "summary.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary));
process.exit(0);
