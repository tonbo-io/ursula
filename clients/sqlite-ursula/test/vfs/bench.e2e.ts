// Test 5: the spike-1 benchmark shape over the VFS — a real Pi Harness on Pi's official node driver
// (unmodified), text and tool turns until the stream holds 1k and then 10k records; Pi commit latency
// (Storage.commit wall time), the VFS's own numbers per record, and cold rebuilds of a fresh file.
import { appendFileSync } from "node:fs";
import type { Storage, StorageWrite } from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { expect, it } from "vitest";
import { attach, drainStats, type VfsCommitStat } from "../../src/vfs.ts";
import { openHarness, textTurn, toolTurn } from "../harness-kit.ts";
import { ctx, freshFile } from "../helpers.ts";
import { streamPath, ursulaUrl } from "./kit.ts";

const pct = (xs: readonly number[], p: number): number => {
	if (xs.length === 0) return Number.NaN;
	const s = [...xs].sort((a, b) => a - b);
	return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] as number;
};
const f = (n: number): string => n.toFixed(2);
const kindOf = (writes: readonly StorageWrite[]): string => [...new Set(writes.map((w) => w.type))].sort().join("+") || "empty";

interface Sample {
	readonly ms: number;
	readonly kind: string;
	readonly records: readonly VfsCommitStat[];
}

it("benchmark: Pi on the ursula VFS against real Ursula", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const storage = await openNodeSqliteStorage(file);
	let records = drainStats(file).length; // schema migrations
	const samples: Sample[] = [];
	const timed = new Proxy(storage, {
		get(target, prop) {
			const value = Reflect.get(target, prop, target) as unknown;
			if (prop === "commit") {
				return async (writes: readonly StorageWrite[], c: typeof ctx) => {
					records += drainStats(file).length;
					const started = performance.now();
					try {
						return await target.commit(writes, c);
					} finally {
						const ms = performance.now() - started;
						const recs = drainStats(file);
						records += recs.length;
						samples.push({ ms, kind: kindOf(writes), records: recs });
					}
				};
			}
			return typeof value === "function" ? value.bind(target) : value;
		},
	}) as Storage;
	const { harness, root } = await openHarness(timed);
	const out: string[] = [];
	let turn = 0;
	for (const target of [1000, 10_000]) {
		while (records < target) {
			if (turn % 3 === 2) await toolTurn(root, turn);
			else await textTurn(root, turn);
			turn++;
			records += drainStats(file).length;
		}
		const started = performance.now();
		const tail = attach(freshFile(), url);
		const ms = performance.now() - started;
		expect(tail).toBeGreaterThanOrEqual(records);
		out.push(`cold rebuild of a fresh file from ${tail} records (${turn} turns): ${f(ms)} ms`);
	}
	await harness.close(ctx);
	await storage.close(ctx);

	const steady = samples.slice(10);
	const lat = steady.map((s) => s.ms);
	const recs = steady.flatMap((s) => s.records);
	const all = samples.flatMap((s) => s.records);
	out.unshift(
		`Pi commits: ${samples.length}, records: ${records} (${f(all.length / samples.length)} per Pi commit)`,
		`Pi commit latency (Storage.commit) p50 ${f(pct(lat, 50))} ms, p99 ${f(pct(lat, 99))} ms, max ${f(Math.max(...lat))} ms`,
		`append request alone             p50 ${f(pct(recs.map((r) => r.append_us / 1000), 50))} ms, p99 ${f(pct(recs.map((r) => r.append_us / 1000), 99))} ms`,
		`VFS commit hook (append + WAL write + fsync) p50 ${f(pct(recs.map((r) => r.vfs_us / 1000), 50))} ms, p99 ${f(pct(recs.map((r) => r.vfs_us / 1000), 99))} ms`,
	);
	const byKind = new Map<string, Sample[]>();
	for (const s of samples) byKind.set(s.kind, [...(byKind.get(s.kind) ?? []), s]);
	for (const [k, ss] of [...byKind].sort((a, b) => b[1].length - a[1].length)) {
		const bytes = ss.map((s) => s.records.reduce((a, r) => a + r.bytes, 0));
		const pages = ss.map((s) => s.records.reduce((a, r) => a + r.pages, 0));
		out.push(`Pi commit ${k} (n=${ss.length}): record bytes p50 ${pct(bytes, 50)}, p99 ${pct(bytes, 99)}, max ${Math.max(...bytes)}; pages p50 ${pct(pages, 50)}, p99 ${pct(pages, 99)}, max ${Math.max(...pages)}`);
	}
	const total = all.reduce((a, r) => a + r.bytes, 0);
	const pages = all.map((r) => r.pages);
	out.push(`pages per record p50 ${pct(pages, 50)}, p99 ${pct(pages, 99)}, max ${Math.max(...pages)}`);
	out.push(`total stream bytes: ${total} (${f(total / all.length)} per record, ${f(total / turn)} per turn)`);
	const report = out.join("\n");
	console.log(`\n=== sqlite-ursula VFS benchmark (single node, memory WAL) ===\n${report}\n`);
	const summary = process.env.GITHUB_STEP_SUMMARY;
	if (summary !== undefined) appendFileSync(summary, `### sqlite-ursula VFS benchmark\n\n\`\`\`\n${report}\n\`\`\`\n`);
}, 1_800_000);
