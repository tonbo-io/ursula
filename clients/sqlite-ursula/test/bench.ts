// Benchmark shared by the e2e suite (real Ursula) and a local smoke run (fake): a real Pi Harness
// over SqliteStorage over ReplicatedSqlite, text and tool turns until the stream holds `target`
// records; per-commit latency and record bytes by kind; cold rebuild of a fresh file at each target.
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { type CommitInfo, ReplicatedSqlite } from "../src/replicated.ts";
import type { WalStream } from "../src/stream.ts";
import { openHarness, textTurn, toolTurn } from "./harness-kit.ts";
import { ctx, freshFile } from "./helpers.ts";

const pct = (xs: readonly number[], p: number): number => {
	if (xs.length === 0) return Number.NaN;
	const s = [...xs].sort((a, b) => a - b);
	return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] as number;
};
const f = (n: number): string => n.toFixed(2);

/** The kind of a Pi commit: the tables it wrote (durable_metadata, written by every commit, omitted). */
function kind(tables: readonly string[]): string {
	return tables.filter((t) => t !== "durable_metadata").sort().join("+") || "metadata only";
}

export async function runBench(stream: () => WalStream, targets: readonly number[]): Promise<string> {
	const commits: CommitInfo[] = [];
	const s = stream();
	const db = await ReplicatedSqlite.open(freshFile(), s, { onCommit: (info) => commits.push(info) });
	const storage = await SqliteStorage.open(db);
	const { harness, root } = await openHarness(storage);
	const out: string[] = [];
	let turn = 0;
	for (const target of targets) {
		while (db.nextRecord < target) {
			if (turn % 3 === 2) await toolTurn(root, turn);
			else await textTurn(root, turn);
			turn++;
		}
		const records = db.nextRecord;
		const started = performance.now();
		const rebuilt = await ReplicatedSqlite.open(freshFile(), stream());
		const rebuildMs = performance.now() - started;
		if (rebuilt.nextRecord < records) throw new Error(`rebuild reached ${rebuilt.nextRecord} < ${records}`);
		await rebuilt.close();
		out.push(`cold rebuild of a fresh file from ${records} records (${turn} turns): ${f(rebuildMs)} ms`);
	}
	await harness.close(ctx);
	await storage.close(ctx);
	const steady = commits.slice(10); // skip schema creation and warm-up
	const lat = steady.map((c) => c.latencyMs);
	const app = steady.map((c) => c.appendMs);
	out.unshift(
		`commits: ${commits.length}`,
		`commit latency (whole transaction) p50 ${f(pct(lat, 50))} ms, p99 ${f(pct(lat, 99))} ms, max ${f(Math.max(...lat))} ms`,
		`append request alone            p50 ${f(pct(app, 50))} ms, p99 ${f(pct(app, 99))} ms`,
	);
	const byKind = new Map<string, CommitInfo[]>();
	for (const c of commits) byKind.set(kind(c.tables), [...(byKind.get(kind(c.tables)) ?? []), c]);
	for (const [k, cs] of [...byKind].sort((a, b) => b[1].length - a[1].length)) {
		const bytes = cs.map((c) => c.bytes);
		const raw = cs.map((c) => c.changesetBytes);
		out.push(`record bytes, ${k} (n=${cs.length}): p50 ${pct(bytes, 50)}, p99 ${pct(bytes, 99)}, max ${Math.max(...bytes)} (changeset p50 ${pct(raw, 50)})`);
	}
	const total = commits.reduce((a, c) => a + c.bytes, 0);
	out.push(`total stream bytes: ${total} (${f(total / commits.length)} per record, ${f(total / turn)} per turn)`);
	return out.join("\n");
}
