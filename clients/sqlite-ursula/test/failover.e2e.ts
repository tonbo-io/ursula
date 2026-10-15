// Leader failover on a 3-node Ursula cluster (E2E_SERVER=ursula E2E_NODES=3, test/stack.ts): the
// node that leads the stream's Raft group is killed with SIGKILL while the database commits. Commits
// stay local and go on, the requests the dead leader leaves unanswered are sent again (the gateway's
// 5xx, a 503 without a leader, a 307 of a node that no longer leads), and once the group elected a
// new leader the database catches up in full with no reset. The killed node is then started again,
// so the cluster is whole for the suites after this one.
//
// It needs the cluster's control server, so it does not run against a single node or the model.
import { afterEach, describe, expect, inject, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, type UrsulaResetError } from "../src/index.ts";
import { ext, fromStream, freshDir, stream } from "./kit.ts";

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

/** The running node that leads the group of the stream at `path`: the one whose HEAD is not a redirect. */
async function leader(path: string): Promise<Node> {
	const deadline = Date.now() + 30_000;
	for (;;) {
		for (const node of (await nodes()).filter((n) => n.running)) {
			try {
				const r = await fetch(`${node.url}${path}`, { method: "HEAD", redirect: "manual", signal: AbortSignal.timeout(2000) });
				if (r.status === 200) return node;
			} catch {
				// Not answering yet: try the next.
			}
		}
		if (Date.now() > deadline) throw new Error(`no node leads ${path} within 30 s`);
		await sleep(100);
	}
}

const opened = new Set<UrsulaDatabase>();
afterEach(async () => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
	// Every node runs again, whatever happened.
	if (control !== "") for (const node of await nodes()) if (!node.running) await act(node.id, "start");
});

describe.skipIf(control === "")("leader failover (needs E2E_SERVER=ursula E2E_NODES=3: skipped otherwise)", () => {
	it("kills the stream's leader during commits: commits go on, no reset, a full catch-up on the new leader", { timeout: 180_000 }, async () => {
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
		// Commits in flight while the leader dies: no synced() between them.
		for (let k = 0; k < 20; k++) {
			commit();
			await sleep(5);
		}
		await act(old.id, "kill");
		const killedAt = Date.now();
		let retrying = false;
		while (Date.now() - killedAt < 5_000) {
			commit();
			const st = db.status();
			expect(st.reset).toBeNull();
			if (st.replication.condition === "retrying") retrying = true;
			await sleep(50);
		}
		await db.synced({ signal: AbortSignal.timeout(90_000) });
		const caughtUp = Date.now() - killedAt;
		const next = await leader(s.path);
		expect(next.id).not.toBe(old.id);
		const { commits, replication, reset } = db.status();
		expect(commits.acknowledged).toBe(commits.logged);
		expect(commits).toMatchObject({ pending: 0, pending_bytes: 0 });
		expect(replication.condition).toBeNull();
		expect(reset).toBeNull();
		expect(resets).toEqual([]);

		// The old leader comes back as a follower, and the database goes on.
		await act(old.id, "start");
		commit();
		await db.synced({ signal: AbortSignal.timeout(60_000) });
		expect(resets).toEqual([]);
		const summary = "SELECT count(*) AS n, max(i) AS max, group_concat(i || ':' || body, ',') AS rows FROM (SELECT * FROM t ORDER BY i)";
		const local = db.prepare(summary).all();
		expect(local).toMatchObject([{ n: i, max: i }]);
		db.close();
		expect(await fromStream(s.url, summary)).toEqual(local);
		process.stderr.write(`failover: node${old.id} killed, node${next.id} leads, ${i} commits, retrying seen ${retrying}, caught up ${caughtUp} ms after the kill\n`);
	});
});
