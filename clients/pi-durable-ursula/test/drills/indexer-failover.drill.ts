// Drill: keyed indexer active/standby failover (node keyed-state proxy, `keyed_state.indexer_urls`
// in failover order). One node and two indexers on one object store (S3 when available, else the
// filesystem); drills run one file at a time, so the timing is not disturbed by other stacks.
//
// Live bounded owners commit while the primary indexer is SIGKILLed: owners keep committing with
// zero poison, their keyed reads (flush-waits raising E) are served by the standby, and the
// Stream-Keyed-Through each stream serves never decreases. The primary then restarts on its port and
// the node moves traffic back to it.
import { afterAll, expect, it } from "vitest";
import { Stack } from "../stack/cluster.ts";
import { delta, OwnerFleet } from "../stack/fleet.ts";
import { sleep, until } from "../stack/proc.ts";
import { s3Available } from "../stack/s3.ts";

interface PodMetrics {
	readonly url: string;
	readonly healthy: boolean;
	readonly requests: number;
	readonly failures: number;
}
interface UpstreamMetrics {
	readonly active_pod: number | null;
	readonly failovers: number;
	readonly unhealthy_pods: number;
	readonly pods: readonly PodMetrics[];
}

let stack: Stack | undefined;
let fleet: OwnerFleet | undefined;
let stopPolling = false;
afterAll(async () => {
	stopPolling = true;
	await fleet?.stop();
	await stack?.stop();
});

async function upstream(): Promise<UpstreamMetrics> {
	const node = stack?.nodes[0];
	if (node === undefined) throw new Error("no node");
	const r = await fetch(`${node.adminUrl}/__ursula/metrics`, { signal: AbortSignal.timeout(10_000) });
	const metrics = (await r.json()) as { keyed_state_upstream: UpstreamMetrics };
	return metrics.keyed_state_upstream;
}

/** Every owner's E (overlay floor) reaches the tail it had when this was called. */
async function untilECatchesUp(what: string, timeoutMs = 60_000): Promise<void> {
	const owners = fleet?.owners ?? [];
	const targets = owners.map((owner) => owner.storage?.tail ?? 0);
	await until(what, () => (owners.every((owner, i) => (owner.storage?.localStore?.overlayFloor ?? 0) >= (targets[i] ?? 0)) ? true : undefined), timeoutMs, 100);
}

it("primary indexer killed mid-run: the standby serves, D never decreases, traffic returns to the restarted primary", async () => {
	stack = await Stack.start({ s3: s3Available(), standbyIndexer: true });
	await stack.createBucket("failover");
	fleet = new OwnerFleet({
		baseUrl: stack.url,
		bucket: "failover",
		owners: 4,
		commitIntervalMs: 50,
		payloadBytes: 256,
		// Flush-waits often, so E moves through keyed-state reads all the time.
		timing: { keyedWaitMs: 5_000, flushMaxRecords: 5, flushMaxAgeMs: 500, flushBackoffMaxMs: 500, backoffMaxMs: 500 },
	});
	fleet.start();

	// Served D per stream, polled without min_through_record: it must never decrease.
	const served = new Map<string, number>();
	const decreases: string[] = [];
	const nodeUrl = stack.url;
	const poll = (async () => {
		while (!stopPolling) {
			for (const owner of fleet?.owners ?? []) {
				try {
					const r = await fetch(`${nodeUrl}/${owner.stream}/keyed-state?limit=1`, { signal: AbortSignal.timeout(10_000) });
					await r.arrayBuffer();
					const through = Number(r.headers.get("stream-keyed-through") ?? Number.NaN);
					if (r.status !== 200 || !Number.isFinite(through)) continue;
					const before = served.get(owner.stream) ?? 0;
					if (through < before) decreases.push(`${owner.stream}: ${before} -> ${through}`);
					served.set(owner.stream, Math.max(before, through));
				} catch {
					// Not-yet-created streams and transient errors are skipped.
				}
			}
			await sleep(50);
		}
	})();

	const owners = fleet.owners;
	await until("every owner to commit", () => (owners.every((owner) => owner.counters.commits >= 5) ? true : undefined), 60_000, 100);
	await untilECatchesUp("E to follow the tail on the primary");
	let metrics = await upstream();
	expect(metrics.active_pod).toBe(0);
	expect(metrics.pods.length).toBe(2);
	expect(metrics.pods[0]?.requests).toBeGreaterThan(0);

	// Kill the primary mid-run.
	const before = fleet.totals();
	const standbyBefore = metrics.pods[1]?.requests ?? 0;
	await stack.stopIndexer(true);
	await sleep(1_000);
	await untilECatchesUp("E to keep moving on the standby");
	await sleep(2_000);
	const during = delta(fleet.totals(), before);
	metrics = await upstream();
	expect(metrics.active_pod).toBe(1);
	expect(metrics.failovers).toBeGreaterThan(0);
	expect(metrics.pods[0]?.healthy).toBe(false);
	expect(metrics.pods[1]?.requests ?? 0).toBeGreaterThan(standbyBefore);
	expect(during.commits).toBeGreaterThan(owners.length * 5);
	expect(during.poison, fleet.errors()).toBe(0);

	// Restart the primary on its port: the prober restores it and traffic moves back.
	await stack.restartIndexer();
	await until("the primary to be active again", async () => ((await upstream()).active_pod === 0 ? true : undefined), 30_000, 200);
	const back = await upstream();
	await untilECatchesUp("E to follow the tail on the restarted primary");
	await sleep(1_000);
	metrics = await upstream();
	expect(metrics.active_pod).toBe(0);
	expect(metrics.pods[0]?.requests ?? 0).toBeGreaterThan(back.pods[0]?.requests ?? 0);
	// No read went to the standby once the primary was back.
	expect(metrics.pods[1]?.requests).toBe(back.pods[1]?.requests);

	stopPolling = true;
	await poll;
	await fleet.stop();
	const totals = delta(fleet.totals(), before);
	const problems = await fleet.verify(stack.url);
	expect(problems, problems.join("\n")).toEqual([]);
	expect(totals.poison, fleet.errors()).toBe(0);
	expect(totals.faulted, fleet.errors()).toBe(0);
	expect(decreases).toEqual([]);
	expect(served.size).toBe(owners.length);
}, 240_000);
