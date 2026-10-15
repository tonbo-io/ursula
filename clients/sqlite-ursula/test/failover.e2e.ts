// Leader failover on a 3-node Ursula cluster (E2E_SERVER=ursula E2E_NODES=3, test/stack.ts): the
// node that leads the stream's Raft group is killed with SIGKILL while the database has commits it
// sent and the stream has not acknowledged. Commits stay local and go on, the requests the dead
// leader leaves unanswered are sent again (the gateway's 5xx, a 503 without a leader, a 307 of a
// node that no longer leads), and once the group elected a new leader the database catches up in
// full with no reset, each commit in the stream exactly once. The killed node is then started
// again, so the cluster is whole for the suites after this one.
//
// It needs the cluster's control server, so it does not run against a single node or the model.
import { afterEach, describe, expect, inject, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, type UrsulaResetError } from "../src/index.ts";
import { ext, frames, fromStream, freshDir, killChildren, stream } from "./kit.ts";

const control = inject("ursulaControl");
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

interface Node {
	id: number;
	url: string;
	running: boolean;
}

async function nodes(): Promise<Node[]> {
	return (await (await fetch(`${control}/nodes`)).json()) as Node[];
}

async function act(id: number, action: "kill" | "start"): Promise<void> {
	const r = await fetch(`${control}/nodes/${id}/${action}`, { method: "POST" });
	expect(r.status, await r.text()).toBe(200);
}

/**
 * Who `node` says leads the group of the stream at `path`, from a live read, which only the leader
 * serves: itself (`200`), the node a follower redirects to (`307` with `x-ursula-raft-leader-id`),
 * or `undefined` (no leader known, or no answer). A HEAD does not tell: a follower that knows the
 * leader forwards it and answers `200` itself.
 */
async function leaderSeenBy(node: Node, path: string): Promise<number | undefined> {
	const abort = new AbortController();
	const timer = setTimeout(() => abort.abort(), 2000);
	try {
		const r = await fetch(`${node.url}${path}?offset=now&live=sse`, { redirect: "manual", signal: abort.signal });
		if (r.status === 200) return node.id;
		const named = r.headers.get("x-ursula-raft-leader-id");
		return r.status === 307 && named !== null ? Number(named) : undefined;
	} catch {
		return undefined;
	} finally {
		clearTimeout(timer);
		// The leader's answer is an endless event stream.
		abort.abort();
	}
}

/** The leader of the stream's group, once every running node agrees on one that runs, within 30 s. */
async function leader(path: string): Promise<number> {
	const deadline = Date.now() + 30_000;
	let seen: (number | undefined)[] = [];
	for (;;) {
		const running = (await nodes()).filter((n) => n.running);
		seen = await Promise.all(running.map((n) => leaderSeenBy(n, path)));
		const first = seen[0];
		if (first !== undefined && seen.every((id) => id === first) && running.some((n) => n.id === first)) return first;
		if (Date.now() > deadline) throw new Error(`the nodes never agreed on a leader of ${path}: ${JSON.stringify(seen)}`);
		await sleep(100);
	}
}

const opened = new Set<UrsulaDatabase>();
afterEach(killChildren);
afterEach(async () => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
	// Every node runs again, whatever happened.
	if (control !== "") for (const node of await nodes()) if (!node.running) await act(node.id, "start");
});

describe.skipIf(control === "")("leader failover (needs E2E_SERVER=ursula E2E_NODES=3: skipped otherwise)", () => {
	it("kills the stream's leader with commits unacknowledged: commits go on, no reset, a full catch-up on the new leader", { timeout: 180_000 }, async () => {
		loadUrsulaVfs(ext());
		const s = stream("failover");
		const db = await open(s.url, { dir: freshDir() });
		opened.add(db);
		const resets: UrsulaResetError[] = [];
		db.on("reset", (e) => resets.push(e));
		db.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, body TEXT)");
		const insert = db.prepare("INSERT INTO t VALUES (?, ?)");
		let i = 0;
		const commit = (): void => {
			i += 1;
			insert.run(i, `row ${i} `.repeat(16));
		};
		for (let k = 0; k < 20; k++) commit();
		await db.synced();

		const old = await leader(s.path);
		// Commits replicating while the leader dies: no synced() between them, and the kill goes out
		// right after one, with some still unacknowledged.
		let pendingAtKill = 0;
		for (let k = 0; k < 20 && pendingAtKill === 0; k++) {
			commit();
			pendingAtKill = db.status().commits.pending;
		}
		expect(pendingAtKill, "no commit was unacknowledged when the leader was killed").toBeGreaterThan(0);
		await act(old, "kill");
		const killedAt = Date.now();
		let retrying = false;
		while (Date.now() - killedAt < 5_000) {
			commit();
			const st = db.status();
			expect(st.reset).toBeNull();
			if (st.replication.condition === "retrying") retrying = true;
			await sleep(50);
		}
		// An election takes 1.5 to 3 s: commits every 50 ms meet the gap.
		expect(retrying, "status never said retrying while the group had no leader").toBe(true);
		await db.synced({ signal: AbortSignal.timeout(90_000) });
		const caughtUp = Date.now() - killedAt;
		const next = await leader(s.path);
		expect(next).not.toBe(old);
		const { commits, replication, reset } = db.status();
		expect(commits.acknowledged).toBe(commits.logged);
		expect(commits).toMatchObject({ pending: 0, pending_bytes: 0 });
		expect(replication.condition).toBeNull();
		expect(reset).toBeNull();
		expect(resets).toEqual([]);

		// The old leader comes back as a follower, and the database goes on.
		await act(old, "start");
		commit();
		await db.synced({ signal: AbortSignal.timeout(60_000) });
		expect(resets).toEqual([]);
		// One claim and every commit exactly once (the CREATE TABLE and `i` inserts): nothing the dead
		// leader left unanswered was lost or applied twice.
		expect(await frames(s.path)).toEqual({ claims: 1, commits: i + 1 });
		const summary = "SELECT count(*) AS n, max(i) AS max, group_concat(i || ':' || body, ',') AS rows FROM (SELECT * FROM t ORDER BY i)";
		const local = db.prepare(summary).all();
		expect(local).toMatchObject([{ n: i, max: i }]);
		db.close();
		expect(await fromStream(s.url, summary)).toEqual(local);
		process.stderr.write(`failover: node${old} killed with ${pendingAtKill} commits unacknowledged, node${next} leads, ${i} commits, caught up ${caughtUp} ms after the kill\n`);
	});
});
