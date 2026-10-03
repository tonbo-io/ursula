// Benchmark (sanity/regression check: bytes per turn, pages per commit, compression ratio, and no
// hot-limit trip in the default config; real performance runs on EKS with S3): a real Pi Harness on Pi's official node driver over the VFS, text and tool turns until
// the stream holds 1k and then 10k commits; after each phase the storage closes and a fresh file is
// rebuilt from the stream (a cold attach), then the original file is re-attached (it takes the
// stream back) and the next phase continues. Reports Pi commit latency (Storage.commit wall time), the
// VFS's numbers per commit (stream bytes, raw bytes, pages, append and hook time), checkpoints, and
// what the slowest Pi commits spent their time on.
import { appendFileSync } from "node:fs";
import type { Storage, StorageWrite } from "@earendil-works/pi-durable";
import { it } from "vitest";
import { attach, drainStats, openUrsulaPiStorage, status, type VfsCommitStat, type VfsSnapshotStat } from "../src/index.ts";
import { openHarness, textTurn, toolTurn } from "./harness-kit.ts";
import { ctx, freshFile } from "./helpers.ts";
import { streamPath, ursulaUrl } from "./kit.ts";

const pct = (xs: readonly number[], p: number): number => {
	if (xs.length === 0) return Number.NaN;
	const s = [...xs].sort((a, b) => a - b);
	return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] as number;
};
const f = (n: number): string => n.toFixed(2);
const ms = (us: number): number => us / 1000;
const dist = (xs: readonly number[]): string => `p50 ${f(pct(xs, 50))}, p99 ${f(pct(xs, 99))}, max ${f(Math.max(...xs))}`;
const kindOf = (writes: readonly StorageWrite[]): string => [...new Set(writes.map((w) => w.type))].sort().join("+") || "empty";

interface Sample {
	readonly ms: number;
	readonly kind: string;
	readonly commits: readonly VfsCommitStat[];
	readonly checkpointsUs: readonly number[];
}

it("benchmark: Pi on the ursula VFS against real Ursula", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	const samples: Sample[] = [];
	let commits = 0;
	let turn = 0;
	const out: string[] = [];
	const snapshots: VfsSnapshotStat[] = [];
	const drain = (file: string) => {
		const s = drainStats(file);
		snapshots.push(...s.snapshots);
		return s;
	};
	for (const target of [1000, 10_000]) {
		const storage = await openUrsulaPiStorage(file, url);
		commits += drain(file).commits.length;
		const timed = new Proxy(storage, {
			get(t, prop) {
				const value = Reflect.get(t, prop, t) as unknown;
				if (prop === "commit") {
					return async (writes: readonly StorageWrite[], c: typeof ctx) => {
						commits += drain(file).commits.length;
						const started = performance.now();
						try {
							return await t.commit(writes, c);
						} finally {
							const elapsed = performance.now() - started;
							const s = drain(file);
							commits += s.commits.length;
							samples.push({ ms: elapsed, kind: kindOf(writes), commits: s.commits, checkpointsUs: s.checkpoints_us });
						}
					};
				}
				return typeof value === "function" ? value.bind(t) : value;
			},
		}) as Storage;
		const { harness, root } = await openHarness(timed);
		while (commits < target) {
			if (turn % 3 === 2) await toolTurn(root, turn);
			else await textTurn(root, turn);
			turn++;
			commits += drain(file).commits.length;
		}
		await harness.close(ctx);
		const { snapshot, retained } = status(file);
		await storage.close(ctx);
		const started = performance.now();
		attach(freshFile(), url);
		out.push(`cold rebuild of a fresh file from ${commits} commits (${turn} turns; snapshot at ${snapshot}, retention ${retained}): ${f(performance.now() - started)} ms`);
	}

	const steady = samples.slice(10);
	const lat = steady.map((s) => s.ms);
	const recs = steady.flatMap((s) => s.commits);
	const all = samples.flatMap((s) => s.commits);
	const ckpts = samples.flatMap((s) => s.checkpointsUs);
	const slowest = [...steady].sort((a, b) => b.ms - a.ms).slice(0, 5);
	out.unshift(
		`Pi commits: ${samples.length}, VFS commits: ${all.length} (${f(all.length / samples.length)} per Pi commit)`,
		`Pi commit latency (Storage.commit) ms ${dist(lat)}`,
		`append requests alone ms ${dist(recs.map((r) => ms(r.append_us)))}; retried appends ${recs.filter((r) => r.attempts > 1).length}`,
		`VFS commit hook (frame build + append + local WAL write) ms ${dist(recs.map((r) => ms(r.vfs_us)))}`,
		`checkpoints: ${ckpts.length}, ms ${dist(ckpts.map(ms))}`,
		...slowest.map(
			(s) =>
				`slow Pi commit ${f(s.ms)} ms (${s.kind}): hook ${s.commits.map((r) => f(ms(r.vfs_us))).join("+")} ms, append ${s.commits.map((r) => f(ms(r.append_us))).join("+")} ms, checkpoint ${s.checkpointsUs.map((c) => f(ms(c))).join("+") || "none"} ms`,
		),
	);
	const byKind = new Map<string, Sample[]>();
	for (const s of samples) byKind.set(s.kind, [...(byKind.get(s.kind) ?? []), s]);
	for (const [k, ss] of [...byKind].sort((a, b) => b[1].length - a[1].length)) {
		const sum = (g: (r: VfsCommitStat) => number) => ss.map((s) => s.commits.reduce((a, r) => a + g(r), 0));
		const [bytes, raw, pages] = [sum((r) => r.bytes), sum((r) => r.raw), sum((r) => r.pages)];
		out.push(`Pi commit ${k} (n=${ss.length}): stream bytes p50 ${pct(bytes, 50)}, p99 ${pct(bytes, 99)}, max ${Math.max(...bytes)}; raw p50 ${pct(raw, 50)}; pages p50 ${pct(pages, 50)}, p99 ${pct(pages, 99)}, max ${Math.max(...pages)}`);
	}
	if (snapshots.length > 0) {
		out.push(
			`snapshots: ${snapshots.length}, body bytes p50 ${pct(snapshots.map((s) => s.bytes), 50)} (db ${pct(snapshots.map((s) => s.raw), 50)}); page copy ms ${dist(snapshots.map((s) => ms(s.copy_us)))}; whole snapshot ms ${dist(snapshots.map((s) => ms(s.total_us)))}`,
		);
	}
	const total = all.reduce((a, r) => a + r.bytes, 0);
	const raw = all.reduce((a, r) => a + r.raw, 0);
	const pages = all.map((r) => r.pages);
	out.push(`pages per commit p50 ${pct(pages, 50)}, p99 ${pct(pages, 99)}, max ${Math.max(...pages)}`);
	out.push(`total stream bytes: ${total} (${f(total / all.length)} per commit, ${f(total / turn)} per turn); raw ${raw} (zstd ratio ${f(raw / total)})`);
	const report = out.join("\n");
	console.log(`\n=== sqlite-ursula VFS benchmark (single node, memory WAL, cold ${process.env.URSULA_COLD ?? "memory"}) ===\n${report}\n`);
	const summary = process.env.GITHUB_STEP_SUMMARY;
	if (summary !== undefined) appendFileSync(summary, `### sqlite-ursula VFS benchmark (cold ${process.env.URSULA_COLD ?? "memory"})\n\n\`\`\`\n${report}\n\`\`\`\n`);
}, 1_800_000);
