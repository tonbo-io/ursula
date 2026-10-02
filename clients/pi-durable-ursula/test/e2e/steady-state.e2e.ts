// M3 exit "zero remote reads on the line for steady-state text and tool turns" (design §10 M3,
// §11.10) against the real keyed stack: a Pi Harness on the bounded owner over HTTP reopens a log
// with history, warms up with one text and one tool turn, and then runs steady-state turns. The
// keyed-state transport is instrumented, and the owner's Session-line metric must agree.
import { describe, expect, it } from "vitest";
import { HttpKeyedStateTransport } from "../../src/http.ts";
import { CountingKeyedState, faultedTasks, openHarness, textTurn, toolTurn } from "../harness-kit.ts";
import { ctx } from "../helpers.ts";
import { baseUrl, freshStream, openHttp } from "./env.ts";

/** Flush every few records so the owner relies on the indexer (E rises during the test). */
const TIMING = { flushMaxRecords: 4, flushMaxAgeMs: 100, flushBackoffMaxMs: 200 } as const;

async function until(check: () => boolean, ms = 30_000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!check()) {
		if (Date.now() > deadline) throw new Error("condition not reached in time");
		await new Promise((r) => setTimeout(r, 25));
	}
}

describe("steady-state turns on the real keyed stack", () => {
	it("issue zero Session-line remote reads after warm-up, with no faulted task", async () => {
		const stream = freshStream();
		{
			const storage = await openHttp(stream, { timing: TIMING });
			const { harness, root } = await openHarness(storage);
			await textTurn(root, 1);
			await toolTurn(root, 2);
			await textTurn(root, 3);
			await harness.close(ctx);
			await storage.close(ctx);
		}
		const counter = new CountingKeyedState(new HttpKeyedStateTransport({ baseUrl: baseUrl(), stream }));
		const storage = await openHttp(stream, { keyedState: counter, timing: TIMING });
		expect(storage.localStore).toBeDefined();
		const { harness, root } = await openHarness(storage);
		await textTurn(root, 4);
		await toolTurn(root, 5);
		// The indexer catches up and the flush loop drops the overlay prefix: steady-state reads must
		// stay local with E above the history.
		const local = storage.localStore;
		const warm = storage.tail;
		await until(() => (local?.overlayFloor ?? 0) >= warm);
		const before = counter.sessionLine;
		const metricBefore = storage.metrics().sessionLineRemoteReads;
		for (let n = 6; n < 16; n++) {
			if (n % 2 === 0) await textTurn(root, n);
			else await toolTurn(root, n);
		}
		expect(counter.sessionLine - before).toBe(0);
		expect(storage.metrics().sessionLineRemoteReads - metricBefore).toBe(0);
		// The flush loop keeps raising E through the real indexer.
		const tail = storage.tail;
		await until(() => (local?.overlayFloor ?? 0) >= tail);
		expect(storage.metrics().floorsRaised).toBeGreaterThan(1);
		await harness.waitForIdle(ctx);
		const tasks = await faultedTasks(storage);
		expect(tasks.total).toBeGreaterThan(0);
		expect(tasks.faulted).toEqual([]);
		expect(storage.poison).toBeUndefined();
		await harness.close(ctx);
		await storage.close(ctx);
	});
});
