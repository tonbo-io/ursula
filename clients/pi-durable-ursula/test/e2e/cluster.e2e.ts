// The keyed stack on a 3-node cluster behind the gateway (E2E_NODES=3): keyed-state read on a
// follower (served from its replica, equal to the leader's), an owner talking to a follower node
// directly (its appends answered with a 307 to the stream's leader), and an owner takeover across
// nodes. The whole e2e suite also runs through the gateway in this mode.
import { describe, expect, it } from "vitest";
import { FencedError } from "../../src/errors.ts";
import { encodeRecord } from "../../src/keyed-batch.ts";
import { KEYED_CONTENT_TYPE } from "../../src/protocol.ts";
import { ctx } from "../helpers.ts";
import { freshStream, openHttp, type StackInfo, stackInfo } from "./env.ts";

const info = stackInfo();
const conv = (id: number) => [{ type: "conversation" as const, value: { id } as never }];

async function until(check: () => boolean, ms = 30_000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!check()) {
		if (Date.now() > deadline) throw new Error("condition not reached in time");
		await new Promise((r) => setTimeout(r, 25));
	}
}

/**
 * The stream's Raft leader, found without writing: an append with a stale `Stream-Record-Match`
 * answers 412 on the leader and 307 (to the leader) on a follower.
 */
async function streamLeader(stream: string): Promise<StackInfo["nodes"][number] | undefined> {
	const body = encodeRecord(0, []);
	const answers = await Promise.all(
		info.nodes.map(async (node) => {
			const r = await fetch(`${node.url}/${stream}`, {
				method: "POST",
				headers: { "content-type": KEYED_CONTENT_TYPE, "stream-record-match": "0" },
				body,
				redirect: "manual",
			});
			await r.text();
			return { node, status: r.status, location: r.headers.get("location") };
		}),
	);
	const leaders = answers.filter((a) => a.status === 412);
	expect(leaders.length, JSON.stringify(answers.map((a) => [a.node.id, a.status]))).toBe(1);
	const leader = leaders[0]?.node;
	for (const follower of answers.filter((a) => a.node !== leader)) {
		expect(follower.status).toBe(307);
		expect(follower.location).toContain(new URL(leader?.url ?? "http://x").host);
	}
	return leader;
}

describe.runIf(info.nodes.length >= 3)("keyed stack on a 3-node cluster", () => {
	it("keyed-state on a follower: served from the replica, equal to the leader's; writes redirect to the leader", async () => {
		const stream = freshStream();
		const s = await openHttp(stream);
		for (let i = 0; i < 3; i++) await s.commit(conv(i + 1), ctx);
		const tail = s.tail;
		await s.close(ctx);
		const leader = await streamLeader(stream);
		expect(leader).toBeDefined();
		const query = `keyed-state?limit=100&min_through_record=${tail}&timeout_ms=20000`;
		const answers = await Promise.all(
			info.nodes.map(async (node) => {
				// A follower that has not applied the tail yet answers 400 with its record tail (the
				// owner retries that), and 503 is retryable (other e2e files restart the shared
				// indexer concurrently); poll until it serves.
				for (let i = 0; ; i++) {
					const r = await fetch(`${node.url}/${stream}/${query}`, { redirect: "manual" });
					const body = await r.text();
					if ((r.status !== 400 && r.status !== 503) || i > 200) return { node, status: r.status, through: r.headers.get("stream-keyed-through"), body };
					await new Promise((res) => setTimeout(res, 50));
				}
			}),
		);
		for (const answer of answers) {
			expect(answer.status, `node ${answer.node.id}`).toBe(200);
			expect(Number(answer.through)).toBeGreaterThanOrEqual(tail);
		}
		const bodies = new Set(answers.map((a) => a.body));
		expect(bodies.size).toBe(1);
	});

	it("an owner talking to a follower node directly commits (307 to the leader) and reads keyed-state there", async () => {
		const stream = freshStream();
		const first = await openHttp(stream);
		await first.commit(conv(1), ctx);
		await first.close(ctx);
		const leader = await streamLeader(stream);
		const follower = info.nodes.find((node) => node.id !== leader?.id);
		expect(follower).toBeDefined();
		const s = await openHttp(stream, { timing: { flushMaxRecords: 2, flushBackoffMaxMs: 200 } }, follower?.url);
		for (let i = 2; i <= 6; i++) await s.commit(conv(i), ctx);
		expect((await s.scanConversations({}, 100, undefined, ctx)).items.map((c) => c.id)).toEqual([1, 2, 3, 4, 5, 6]);
		await until(() => (s.localStore?.overlayFloor ?? 0) >= 5);
		expect(s.flushMetrics?.floorsRaised).toBeGreaterThan(0);
		expect(s.poison).toBeUndefined();
		await s.close(ctx);
		const reopened = await openHttp(stream, {}, follower?.url);
		expect((await reopened.scanConversations({}, 100, undefined, ctx)).items.length).toBe(6);
		await reopened.close(ctx);
	});

	it("owner takeover across nodes: fence on another node fences the active owner", async () => {
		const stream = freshStream();
		const [n1, n2] = info.nodes;
		const a = await openHttp(stream, {}, n1?.url);
		for (let i = 0; i < 4; i++) await a.commit(conv(10 + i), ctx);
		const b = await openHttp(stream, { mode: "fence" }, n2?.url);
		expect(b.epoch).toBeGreaterThan(a.epoch);
		await expect(a.commit(conv(99), ctx)).rejects.toBeInstanceOf(FencedError);
		await b.commit(conv(20), ctx);
		expect((await b.scanConversations({}, 100, undefined, ctx)).items.map((c) => c.id)).toEqual([10, 11, 12, 13, 20]);
		await b.close(ctx);
	});
});
