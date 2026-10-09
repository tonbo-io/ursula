// One Pi database for the EKS benchmark (docs/architecture/sqlite-vfs.md §9): Pi Durable's official
// SqliteStorage on a replicated file, a real Harness with the faux model, turns of text, text,
// tool (5 Pi commits per turn). Each database runs in its own process, as an application would:
// a commit blocks the process until Ursula acknowledges it.
//
//   node bench/owner.ts <stream url> <file> <pace ms, 0: flat out> <warm-up s> <duration s> <out.json>
//
// Records the wall time of every Storage.commit after the warm-up (ms, with its start time), the
// VFS's per-commit numbers, and the owner's status at the end.
import { writeFileSync } from "node:fs";
import type { Storage, StorageWrite } from "@earendil-works/pi-durable";
import { drainStats, openUrsulaPiStorage, status, UrsulaReplicationError, type VfsCommitStat } from "../src/index.ts";
import { openHarness, textTurn, toolTurn } from "../test/harness-kit.ts";
import { ctx } from "../test/helpers.ts";

if (process.argv.length < 8) throw new Error("usage: owner.ts <url> <file> <pace ms> <warm-up s> <duration s> <out.json>");
const [url, file, paceArg, warmupArg, durationArg, out] = process.argv.slice(2) as [string, string, string, string, string, string];
const pace = Number(paceArg);
const started = performance.now();
const measureFrom = started + Number(warmupArg) * 1000;
const end = measureFrom + Number(durationArg) * 1000;

/** Commit latency samples: [start ms since the measurement began, wall time ms]. */
const samples: [number, number][] = [];
/** The process's RSS, MiB, once a minute: [s since start, MiB]. */
const rss: [number, number][] = [];
const rssTimer = setInterval(() => rss.push([Math.round((performance.now() - started) / 1000), Math.round(process.memoryUsage().rss / 1048576)]), 60_000);
const vfs: VfsCommitStat[] = [];
let failed: string | undefined;

const storage = await openUrsulaPiStorage(file, url);
drainStats(file);
const timed = new Proxy(storage, {
	get(t, prop) {
		const value = Reflect.get(t, prop, t) as unknown;
		if (prop === "commit") {
			return async (writes: readonly StorageWrite[], c: typeof ctx) => {
				const at = performance.now();
				try {
					return await t.commit(writes, c);
				} finally {
					const ms = performance.now() - at;
					const stats = drainStats(file);
					if (at >= measureFrom) {
						samples.push([at - measureFrom, ms]);
						vfs.push(...stats.commits);
					}
				}
			};
		}
		return typeof value === "function" ? value.bind(t) : value;
	},
}) as Storage;
const { harness, root } = await openHarness(timed);

let turn = 0;
/** A turn a killed predecessor left open resumes first and takes the faux answer meant for this
 * process's first turn, which then settles unanswered: an artifact of the faux model. */
let resumed = 0;
try {
	while (performance.now() < end) {
		const turnStart = performance.now();
		try {
			if (turn % 3 === 2) await toolTurn(root, turn);
			else await textTurn(root, turn);
		} catch (error) {
			if (turn !== 0 || !String(error).includes("settled unanswered")) throw error;
			resumed++;
		}
		turn++;
		if (pace > 0) {
			// Agent pace: one turn per `pace` ms.
			const wait = turnStart + pace - performance.now();
			if (wait > 0) await new Promise((r) => setTimeout(r, wait));
		}
	}
} catch (error) {
	failed = error instanceof UrsulaReplicationError ? `${error.message} (fenced: ${error.fenced})` : String(error);
}
clearInterval(rssTimer);
const s = status(file);
writeFileSync(
	out,
	JSON.stringify({
		file,
		url,
		pace,
		turns: turn,
		resumed,
		failed: failed ?? null,
		status: s,
		rss,
		samples,
		vfs: vfs.map((c) => [c.bytes, c.raw, c.pages, c.attempts, c.append_us, c.vfs_us]),
	}),
);
await harness.close(ctx).catch(() => {});
await storage.close(ctx).catch(() => {});
process.exit(failed === undefined && !s.poisoned ? 0 : 1);
