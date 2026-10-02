// A real Pi Harness on the bounded owner (design §10 M3 exit): after reopening a harness with
// history and one warm-up turn, steady-state text and tool turns issue zero remote reads on the
// Session line, and no task is ever faulted, under every indexer mode. The keyed-state transport is
// instrumented (harness-kit.ts), and the owner's own metric agrees.
import type { Conversation, Harness } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import type { FakeUrsula } from "../src/fake/index.ts";
import type { UrsulaStorage } from "../src/storage.ts";
import { BOUNDED_TIMING, fakeFor, INDEXER_MODES } from "./bounded-helpers.ts";
import { CountingKeyedState, faultedTasks, openHarness, textTurn, toolTurn } from "./harness-kit.ts";
import { ctx, freshPath, openOn } from "./helpers.ts";

async function open(fake: FakeUrsula, path: string): Promise<{ storage: UrsulaStorage; counter: CountingKeyedState; harness: Harness; root: Conversation }> {
	const counter = new CountingKeyedState(fake.keyedStateTransport(path));
	const storage = await openOn(fake, path, { stateStore: "bounded", keyedState: counter, timing: BOUNDED_TIMING });
	return { storage, counter, ...(await openHarness(storage)) };
}

describe("steady-state turns on the bounded owner", () => {
	for (const mode of INDEXER_MODES) {
		it(`issue zero Session-line remote reads after warm-up, with no faulted task (indexer ${mode})`, async () => {
			const fake = fakeFor(mode);
			const path = freshPath();
			// History written by a previous owner.
			{
				const s = await open(fake, path);
				await textTurn(s.root, 1);
				await toolTurn(s.root, 2);
				await textTurn(s.root, 3);
				await s.harness.close(ctx);
				await s.storage.close(ctx);
			}
			const s = await open(fake, path);
			// Warm-up: the first turns after open may read remotely.
			await textTurn(s.root, 4);
			await toolTurn(s.root, 5);
			const before = s.counter.sessionLine;
			const remoteBefore = s.storage.metrics().sessionLineRemoteReads;
			for (let n = 6; n < 12; n++) {
				if (n % 2 === 0) await textTurn(s.root, n);
				else await toolTurn(s.root, n);
			}
			// Open and warm-up did read keyed-state; the flush loop raised E unless the indexer is paused.
			expect(before).toBeGreaterThan(0);
			if (mode !== "paused") expect(s.storage.localStore?.overlayFloor ?? 0).toBeGreaterThan(0);
			expect(s.counter.sessionLine - before).toBe(0);
			expect(s.storage.metrics().sessionLineRemoteReads - remoteBefore).toBe(0);
			await s.harness.waitForIdle(ctx);
			const tasks = await faultedTasks(s.storage);
			expect(tasks.total).toBeGreaterThan(0);
			expect(tasks.faulted).toEqual([]);
			expect(s.storage.poison).toBeUndefined();
			await s.harness.close(ctx);
			await s.storage.close(ctx);
		});
	}
});
