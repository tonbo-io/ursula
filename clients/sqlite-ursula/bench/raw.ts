// The raw append floor (B3 in docs/architecture/sqlite-vfs.md §9): closed-loop writers, one stream
// each, 5 to 7 KiB random bodies (the VFS's compressed frames are about that size) with producer
// headers, no SQLite.
//
//   node bench/raw.ts <base url> <label> <writers> <warm-up s> <duration s> <out dir> [leader]
//
// With `leader`, a writer follows the first 307 to its stream's leader and keeps sending there.
import { randomBytes } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

if (process.argv.length < 8) throw new Error("usage: raw.ts <base url> <label> <writers> <warm-up s> <duration s> <out dir> [leader]");
const [base, label, writersArg, warmup, duration, outDir, mode] = process.argv.slice(2) as [string, string, string, string, string, string, string?];
const run = Date.now().toString(36);
const started = performance.now();
const measureFrom = started + Number(warmup) * 1000;
const end = measureFrom + Number(duration) * 1000;
let status503 = 0;
let otherErrors = 0;

async function writer(i: number): Promise<number[]> {
	let url = `${base.replace(/\/+$/, "")}/vfs-bench/${label}-${run}-${i}`;
	const created = await fetch(url, { method: "PUT", headers: { "content-type": "application/octet-stream" }, redirect: "follow" });
	if (!created.ok) throw new Error(`create ${url}: ${created.status}`);
	const samples: number[] = [];
	for (let seq = 0; performance.now() < end; seq++) {
		const body = randomBytes(5 * 1024 + Math.floor(Math.random() * 2048));
		const headers = { "content-type": "application/octet-stream", "producer-id": `raw-${i}`, "producer-epoch": "0", "producer-seq": `${seq}` };
		for (;;) {
			const at = performance.now();
			const r = await fetch(url, { method: "POST", headers, body, redirect: "manual" });
			await r.arrayBuffer();
			if (r.status === 307 && mode === "leader") {
				url = new URL(r.headers.get("location") ?? url, url).toString();
				continue;
			}
			if (r.status === 503 || r.status === 429) {
				status503++;
				await new Promise((res) => setTimeout(res, 20));
				continue;
			}
			if (!r.ok) {
				otherErrors++;
				throw new Error(`append ${url}: ${r.status}`);
			}
			if (at >= measureFrom) samples.push(performance.now() - at);
			break;
		}
	}
	return samples;
}

const all = (await Promise.all(Array.from({ length: Number(writersArg) }, (_, i) => writer(i)))).flat();
const s = Float64Array.from(all).sort();
const pct = (p: number): number => Math.round((s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] ?? Number.NaN) * 100) / 100;
const summary = {
	label,
	writers: Number(writersArg),
	mode: mode ?? "gateway",
	appends: all.length,
	append_ms: { p50: pct(50), p99: pct(99), p999: pct(99.9), max: Math.round((s[s.length - 1] ?? Number.NaN) * 100) / 100 },
	appends_per_s: Math.round((all.length / Number(duration)) * 10) / 10,
	retried_503_429: status503,
	other_errors: otherErrors,
};
mkdirSync(join(outDir, label), { recursive: true });
writeFileSync(join(outDir, label, "summary.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary));
