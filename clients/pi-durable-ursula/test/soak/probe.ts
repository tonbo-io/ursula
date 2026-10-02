// Soak sampling (bounded-stream-state §7.5, keyed streams §10 M4): per-group state gauges and Raft
// log/snapshot sizes from every node's `/__ursula/metrics`, node RSS, the keyed indexer's lag and S3
// requests, and S3 requests by API from MinIO's Prometheus endpoint.
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { Stack } from "../stack/cluster.ts";

const run = promisify(execFile);

/** One group replica's gauges (GroupStateGauges plus the Raft log figures). */
export interface GroupSample {
	readonly group: number;
	readonly leader: boolean;
	readonly gauges: Record<string, number>;
	readonly logBytesSinceSnapshot: number;
	readonly lastSnapshotBytes: number;
	readonly snapshotIndex: number | null;
}

export interface NodeSample {
	readonly id: number;
	readonly rssBytes: number;
	readonly corruptions: number;
	readonly keyedStateRequests: Record<string, number>;
	readonly groups: GroupSample[];
	readonly error?: string;
}

export interface IndexerSample {
	readonly publishes: number;
	readonly compactionPublishes: number;
	readonly casConflicts: number;
	readonly gcBacklog: number;
	readonly namespaces: number;
	readonly lagTotal: number;
	readonly lagMax: number;
	readonly runsMax: number;
	readonly s3: S3Classes;
	readonly perNamespace: { key: string; incarnation: number; through: number; next: number; lag: number; s3: S3Classes }[];
	readonly error?: string;
}

export interface S3Classes {
	readonly put: number;
	readonly get: number;
	readonly head: number;
	readonly list: number;
	readonly delete: number;
}

const num = (v: unknown): number => (typeof v === "number" ? v : 0);

async function json(url: string, timeoutMs = 10_000): Promise<Record<string, unknown>> {
	const r = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
	if (!r.ok) throw new Error(`${url}: ${r.status}`);
	return (await r.json()) as Record<string, unknown>;
}

/**
 * Asks every node to snapshot every group it hosts (`POST /__ursula/raft/{g}/snapshot`, which waits
 * for the build). Groups go one at a time per node: a trigger that lands on a running build does
 * not start another. Returns the answers' statuses.
 */
export async function forceSnapshots(stack: Stack): Promise<{ node: number; group: number; status: number }[]> {
	const answers = await Promise.all(
		stack.nodes.map(async (node) => {
			const out: { node: number; group: number; status: number }[] = [];
			for (let g = 0; g < stack.groupCount; g++) {
				const status = await fetch(`${node.adminUrl}/__ursula/raft/${g}/snapshot`, { method: "POST", signal: AbortSignal.timeout(30_000) })
					.then(async (r) => {
						await r.arrayBuffer();
						return r.status;
					})
					.catch(() => 0);
				out.push({ node: node.id, group: g, status });
			}
			return out;
		}),
	);
	return answers.flat();
}

/** Resident set size of a process in bytes (`ps`), 0 when it is gone. */
export async function rssOf(pid: number | undefined): Promise<number> {
	if (pid === undefined) return 0;
	try {
		const { stdout } = await run("ps", ["-o", "rss=", "-p", String(pid)]);
		return Number(stdout.trim()) * 1024 || 0;
	} catch {
		return 0;
	}
}

export async function sampleNodes(stack: Stack): Promise<NodeSample[]> {
	return Promise.all(
		stack.nodes.map(async (node): Promise<NodeSample> => {
			try {
				const m = await json(`${node.adminUrl}/__ursula/metrics`);
				const raft = new Map<number, Record<string, unknown>>();
				for (const g of (m.raft_groups as Record<string, unknown>[] | undefined) ?? []) raft.set(num(g.raft_group_id), g);
				const groups: GroupSample[] = [];
				const gauges = m.group_state_gauges;
				for (const g of Array.isArray(gauges) ? (gauges as Record<string, unknown>[]) : []) {
					if (g.hosted !== true || g.error !== undefined) continue;
					const id = num(g.raft_group_id);
					const r = raft.get(id) ?? {};
					const values: Record<string, number> = {};
					for (const [k, v] of Object.entries(g)) if (typeof v === "number") values[k] = v;
					groups.push({
						group: id,
						leader: r.current_leader === node.id,
						gauges: values,
						logBytesSinceSnapshot: num(r.log_bytes_since_snapshot),
						lastSnapshotBytes: num(r.last_snapshot_bytes),
						snapshotIndex: typeof r.snapshot_index === "number" ? r.snapshot_index : null,
					});
				}
				const ksr: Record<string, number> = {};
				for (const [k, v] of Object.entries((m.keyed_state_requests as Record<string, unknown>) ?? {})) if (typeof v === "number") ksr[k] = v;
				return { id: node.id, rssBytes: num(m.process_rss_bytes) || (await rssOf(node.proc?.child.pid)), corruptions: num(m.record_coordinate_corruptions), keyedStateRequests: ksr, groups };
			} catch (error) {
				return { id: node.id, rssBytes: 0, corruptions: 0, keyedStateRequests: {}, groups: [], error: String(error) };
			}
		}),
	);
}

const classes = (s: Record<string, unknown> | undefined): S3Classes => ({
	put: num(s?.put),
	get: num(s?.get),
	head: num(s?.head),
	list: num(s?.list),
	delete: num(s?.delete),
});

export async function sampleIndexer(url: string): Promise<IndexerSample> {
	try {
		const m = await json(`${url}/__ursula/indexer/metrics`);
		return {
			publishes: num(m.publishes),
			compactionPublishes: num(m.compaction_publishes),
			casConflicts: num(m.cas_conflicts),
			gcBacklog: num(m.gc_backlog),
			namespaces: num(m.namespaces),
			lagTotal: num(m.lag_records_total),
			lagMax: num(m.lag_records_max),
			runsMax: num(m.runs_max),
			s3: classes(m.s3_requests as Record<string, unknown>),
			perNamespace: ((m.namespace_detail as Record<string, unknown>[] | undefined) ?? []).map((n) => ({
				key: `${String(n.bucket)}/${String(n.key)}`,
				incarnation: num(n.incarnation),
				through: num(n.through_record),
				next: num(n.source_next),
				lag: num(n.lag_records),
				s3: classes(n.s3_requests as Record<string, unknown>),
			})),
		};
	} catch (error) {
		const zero = { put: 0, get: 0, head: 0, list: 0, delete: 0 };
		return { publishes: 0, compactionPublishes: 0, casConflicts: 0, gcBacklog: 0, namespaces: 0, lagTotal: 0, lagMax: 0, runsMax: 0, s3: zero, perNamespace: [], error: String(error) };
	}
}

/** `minio_s3_requests_total` by API (summed over servers), or undefined without a MinIO we spawned. */
export async function sampleMinio(endpoint: string): Promise<Record<string, number> | undefined> {
	try {
		const r = await fetch(`${endpoint}/minio/v2/metrics/cluster`, { signal: AbortSignal.timeout(10_000) });
		if (!r.ok) return undefined;
		const out: Record<string, number> = {};
		for (const line of (await r.text()).split("\n")) {
			const m = /^minio_s3_requests_total\{([^}]*)\}\s+([0-9.e+]+)/.exec(line);
			if (m === null) continue;
			const api = /api="([^"]*)"/.exec(m[1] ?? "")?.[1] ?? "unknown";
			out[api] = (out[api] ?? 0) + Number(m[2]);
		}
		return out;
	} catch {
		return undefined;
	}
}

/** PUT class (PUT, COPY, POST, LIST: billed at the PUT rate), GET class (GET, HEAD), and free DELETEs. */
export function classifyS3(byApi: Record<string, number>): { put: number; get: number; delete: number } {
	let put = 0;
	let get = 0;
	let del = 0;
	for (const [api, n] of Object.entries(byApi)) {
		const a = api.toLowerCase();
		if (a.startsWith("delete")) del += n;
		else if (a.startsWith("get") || a.startsWith("head")) get += n;
		else put += n;
	}
	return { put, get, delete: del };
}
