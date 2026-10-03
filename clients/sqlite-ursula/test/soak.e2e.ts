// Memory soak (runs only with SOAK_SECONDS set): many SQLite-VFS-shaped owners writing flat out
// through the gateway, the request shape of the sqlite-ursula-vfs extension without SQLite in the
// loop — binary appends of ~7 KB incompressible frames under the idempotent producer, a snapshot
// PUT (0.1–0.5 MB) once ~1 MB of log has accumulated since the last one, then a retention advance
// to the previous snapshot. Samples every node's RSS and a few server gauges every 5 s, writes them
// to SOAK_OUT (JSONL) and, with SOAK_MAX_RSS_MB, fails when a node's RSS goes past it.
//
// Overload must surface as pushback (503/429 + retry) on the client, never as server memory growth:
// the owners back off on 429/503 and retry with the same producer sequence, as the extension does.
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync } from "node:fs";
import { inject, it } from "vitest";
import { ursulaUrl } from "./kit.ts";

const seconds = Number(process.env.SOAK_SECONDS ?? 0);
const owners = Number(process.env.SOAK_OWNERS ?? 128);
const frameBytes = Number(process.env.SOAK_FRAME_BYTES ?? 7 * 1024);
const snapshotEvery = Number(process.env.SOAK_SNAPSHOT_EVERY_BYTES ?? 1024 * 1024);
const maxRssMb = Number(process.env.SOAK_MAX_RSS_MB ?? 0);
// Stops the load early (keeping the runner alive) once a node passes this RSS.
const abortRssMb = Number(process.env.SOAK_ABORT_RSS_MB ?? 0);
const out = process.env.SOAK_OUT;

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
// A pool of incompressible bodies (zstd frames do not compress further).
const pool = Array.from({ length: 64 }, () => randomBytes(frameBytes));
const snapshotPool = Array.from({ length: 8 }, (_, i) => randomBytes(100 * 1024 + i * 57 * 1024));

const counts = new Map<string, number>();
const bump = (key: string): void => {
	counts.set(key, (counts.get(key) ?? 0) + 1);
};

async function put(url: string, body: Uint8Array | undefined, stop: () => boolean): Promise<number> {
	let backoff = 20;
	for (;;) {
		let status = 0;
		try {
			const r = await fetch(url, { method: "PUT", headers: { "content-type": "application/octet-stream" }, body, signal: AbortSignal.timeout(30_000) });
			status = r.status;
			await r.arrayBuffer();
		} catch {
			status = -1;
		}
		bump(`put ${status}`);
		if (status !== -1 && status !== 429 && status < 500) return status;
		if (stop()) return status;
		await sleep(backoff);
		backoff = Math.min(backoff * 2, 1000);
	}
}

async function owner(base: string, index: number, stop: () => boolean): Promise<void> {
	const url = `${base}/sqlite-e2e/soak-${process.pid}-${index}`;
	const created = await put(url, undefined, stop);
	if (created >= 300 && created !== 409) throw new Error(`create ${url}: ${created}`);
	let seq = 0;
	let offset = 0;
	let snapshot = 0;
	let previousSnapshot = 0;
	let sinceSnapshot = 0;
	let backoff = 20;
	while (!stop()) {
		const body = pool[(index + seq) % pool.length] as Buffer;
		let status = 0;
		let next: string | null = null;
		try {
			const r = await fetch(url, {
				method: "POST",
				headers: { "content-type": "application/octet-stream", "producer-id": "soak", "producer-epoch": "1", "producer-seq": `${seq}` },
				body,
				signal: AbortSignal.timeout(30_000),
			});
			status = r.status;
			next = r.headers.get("stream-next-offset");
			await r.arrayBuffer();
		} catch {
			status = -1;
		}
		bump(`append ${status}`);
		if (status >= 200 && status < 300) {
			seq++;
			backoff = 20;
			if (next !== null) offset = Number(next);
			sinceSnapshot += body.length;
		} else if (status === -1 || status === 429 || status >= 500) {
			// Pushback or unknown outcome: retry the same producer sequence.
			await sleep(backoff);
			backoff = Math.min(backoff * 2, 1000);
			continue;
		} else {
			throw new Error(`append ${url} seq ${seq}: ${status}`);
		}
		if (sinceSnapshot >= snapshotEvery && offset > snapshot) {
			sinceSnapshot = 0;
			const body = snapshotPool[seq % snapshotPool.length] as Buffer;
			const s = await put(`${url}/snapshot/${offset}`, body, stop);
			if (s >= 200 && s < 300) {
				previousSnapshot = snapshot;
				snapshot = offset;
				if (previousSnapshot > 0) await put(`${url}/retention/${previousSnapshot}`, undefined, stop);
			}
		}
	}
}

function rssMb(pid: number): number {
	try {
		return Number(execFileSync("ps", ["-o", "rss=", "-p", `${pid}`]).toString().trim()) / 1024;
	} catch {
		return Number.NaN;
	}
}

type Json = Record<string, unknown>;
async function gauges(url: string): Promise<Json> {
	try {
		const m = (await (await fetch(`${url}/__ursula/metrics`, { signal: AbortSignal.timeout(5000) })).json()) as Json;
		const groups = (m["raft_groups"] as Json[] | undefined) ?? [];
		let logEntries = 0;
		let logBytes = 0;
		for (const g of groups) {
			logEntries += Number(g["last_log_index"] ?? 0) - Number(g["purged_index"] ?? 0);
			logBytes += Number(g["log_bytes_since_snapshot"] ?? 0);
		}
		const picked: Json = { raft_log_entries_unpurged: logEntries, raft_log_bytes_since_snapshot: logBytes };
		for (const [k, v] of Object.entries(m)) {
			if (typeof v !== "number") continue;
			if (/inflight|hot|pending|queue|backlog|uncommitted|rejected|overload|cold_flush|_live_|in_memory/.test(k)) picked[k] = v;
		}
		return picked;
	} catch (error) {
		return { error: String(error) };
	}
}

it.skipIf(seconds <= 0)(
	"soak: VFS-shaped owners stay within bounded server memory",
	async () => {
		const nodes = inject("ursulaNodes");
		const base = ursulaUrl();
		const started = Date.now();
		let aborted = false;
		const stop = (): boolean => aborted || Date.now() - started > seconds * 1000;
		const work = Array.from({ length: owners }, (_, i) => owner(base, i, stop));
		let maxSeen = 0;
		let sampling = true;
		const sampler = (async () => {
			let lastAppends = 0;
			while (sampling) {
				const t = Math.round((Date.now() - started) / 1000);
				const appends = [...counts].filter(([k]) => k.startsWith("append 2")).reduce((a, [, v]) => a + v, 0);
				const rss = nodes.map((n) => rssMb(n.pid));
				for (const r of rss) if (Number.isFinite(r)) maxSeen = Math.max(maxSeen, r);
				if (abortRssMb > 0 && maxSeen > abortRssMb) aborted = true;
				const g = await Promise.all(nodes.map((n) => gauges(n.url)));
				const row = { t, rss_mb: rss.map((r) => Math.round(r)), appends_per_s: Math.round((appends - lastAppends) / 5), counts: Object.fromEntries(counts), gauges: g };
				lastAppends = appends;
				console.log(`soak t=${t}s rss_mb=${JSON.stringify(row.rss_mb)} appends/s=${row.appends_per_s} counts=${JSON.stringify(row.counts)}`);
				if (out !== undefined) appendFileSync(out, `${JSON.stringify(row)}\n`);
				await sleep(5000);
			}
		})();
		const results = await Promise.allSettled(work);
		sampling = false;
		await sampler;
		const failed = results.filter((r) => r.status === "rejected");
		if (failed.length > 0) throw new Error(`${failed.length} owners failed: ${String((failed[0] as PromiseRejectedResult).reason)}`);
		if (aborted) console.log(`soak aborted early: node RSS ${Math.round(maxSeen)} MB past SOAK_ABORT_RSS_MB`);
		if (maxRssMb > 0 && maxSeen > maxRssMb) throw new Error(`node RSS reached ${Math.round(maxSeen)} MB (limit ${maxRssMb} MB)`);
	},
	(seconds + 300) * 1000,
);
