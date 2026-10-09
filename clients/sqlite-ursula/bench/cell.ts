// One benchmark cell: `owners` Pi databases, each in its own process (bench/owner.ts), on fresh
// streams under `<base url>/<bucket>/<label>-<i>`, then the pooled numbers.
//
//   node bench/cell.ts <base url> <label> <owners> <pace ms> <warm-up s> <duration s> <out dir>
//
// Writes `<out dir>/<label>/owner-<i>.json` per owner and `<out dir>/<label>/summary.json`, and
// prints the summary.
import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

if (process.argv.length < 9) throw new Error("usage: cell.ts <base url> <label> <owners> <pace ms> <warm-up s> <duration s> <out dir>");
const [base, label, ownersArg, pace, warmup, duration, outDir] = process.argv.slice(2) as [string, string, string, string, string, string, string];
const owners = Number(ownersArg);
const dir = join(outDir, label);
mkdirSync(dir, { recursive: true });
const files = join(tmpdir(), `vfs-bench-${label}-${Date.now().toString(36)}`);
mkdirSync(files, { recursive: true });
const ownerScript = join(dirname(fileURLToPath(import.meta.url)), "owner.ts");
const run = Date.now().toString(36);

const exits = await Promise.all(
	Array.from({ length: owners }, (_, i) => {
		const url = `${base.replace(/\/+$/, "")}/vfs-bench/${label}-${run}-${i}`;
		const child = spawn(process.execPath, [ownerScript, url, join(files, `db-${i}.sqlite`), pace, warmup, duration, join(dir, `owner-${i}.json`)], {
			stdio: ["ignore", "inherit", "inherit"],
		});
		return new Promise<number>((res) => child.on("exit", (code) => res(code ?? -1)));
	}),
);

interface Owner {
	readonly turns: number;
	readonly failed: string | null;
	readonly status: { readonly poisoned: boolean; readonly fenced: boolean; readonly reason: string | null };
	readonly samples: [number, number][];
	readonly vfs: [number, number, number, number, number, number][];
}
const results: Owner[] = [];
for (let i = 0; i < owners; i++) {
	try {
		results.push(JSON.parse(readFileSync(join(dir, `owner-${i}.json`), "utf8")) as Owner);
	} catch {
		// An owner that died before writing its result: counted in `exits`.
	}
}
const pct = (xs: number[], p: number): number => {
	if (xs.length === 0) return Number.NaN;
	const s = Float64Array.from(xs).sort();
	return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] as number;
};
const r2 = (n: number): number => Math.round(n * 100) / 100;
/** The largest value (a spread would exceed the argument limit for a million samples). */
const max = (xs: number[]): number => xs.reduce((a, b) => (b > a ? b : a), Number.NEGATIVE_INFINITY);
const lat = results.flatMap((o) => o.samples.map(([, ms]) => ms));
const vfs = results.flatMap((o) => o.vfs);
const seconds = Number(duration);
const perOwner = results.map((o) => o.samples.length / seconds);
const summary = {
	label,
	owners,
	pace: Number(pace),
	duration_s: seconds,
	exited_nonzero: exits.filter((c) => c !== 0).length,
	poisoned: results.filter((o) => o.status.poisoned).length,
	failures: results.flatMap((o) => (o.failed === null ? [] : [o.failed])).slice(0, 5),
	commits: lat.length,
	commit_ms: { p50: r2(pct(lat, 50)), p99: r2(pct(lat, 99)), p999: r2(pct(lat, 99.9)), max: r2(max(lat)) },
	commits_per_s: { total: r2(lat.length / seconds), per_owner_median: r2(pct(perOwner, 50)) },
	vfs: {
		commits: vfs.length,
		retried: vfs.filter((c) => c[3] > 1).length,
		max_attempts: Math.max(0, max(vfs.map((c) => c[3]))),
		append_ms_p50: r2(pct(vfs.map((c) => c[4] / 1000), 50)),
		append_ms_p99: r2(pct(vfs.map((c) => c[4] / 1000), 99)),
		bytes_per_commit_p50: pct(vfs.map((c) => c[0]), 50),
		pages_per_commit_p50: pct(vfs.map((c) => c[2]), 50),
		zstd_ratio: r2(vfs.reduce((a, c) => a + c[1], 0) / Math.max(1, vfs.reduce((a, c) => a + c[0], 0))),
	},
};
writeFileSync(join(dir, "summary.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary));
