// Shared settings for the bounded-owner suites (design §10 M3): the four indexer modes of the fake,
// a 4 KiB cache, and timing that makes the flush loop fire every few records.
import { FakeUrsula, faults } from "../src/fake/index.ts";
import type { Timing, UrsulaStorage, UrsulaStorageOptions } from "../src/storage.ts";
import { openOn } from "./helpers.ts";

export type IndexerMode = "normal" | "paused" | "aggressive" | "flaky";
export const INDEXER_MODES: readonly IndexerMode[] = ["normal", "paused", "aggressive", "flaky"];

/** Flush-waits fire every 3 records; keyed-state waits are short so a paused indexer answers 204 fast. */
export const BOUNDED_TIMING: Partial<Timing> = {
	keyedWaitMs: 40,
	flushMaxRecords: 3,
	flushMaxAgeMs: 200,
	flushBackoffMaxMs: 20,
	readDeadlineMs: 3000,
};

let seed = 1;

/** A fake Ursula whose keyed-state indexer behaves per `mode`; `flaky` is `normal` plus random 503s, 429s and resets. */
export function fakeFor(mode: IndexerMode, s = seed++): FakeUrsula {
	const fake = new FakeUrsula({ indexer: mode === "flaky" ? "normal" : mode, seed: s });
	if (mode === "flaky") {
		fake.fault = faults.random(
			s,
			0.25,
			[
				faults.status(503, { "retry-after": "0" }),
				faults.status(503),
				faults.status(429),
				faults.status(500),
				faults.dropRequest,
				faults.dropResponse,
			],
			["scan"],
		);
	}
	return fake;
}

export function openBounded(fake: FakeUrsula, path: string, options: Partial<UrsulaStorageOptions> = {}): Promise<UrsulaStorage> {
	return openOn(fake, path, {
		stateStore: "bounded",
		cacheBudgetBytes: 4096,
		...options,
		timing: { ...BOUNDED_TIMING, ...options.timing },
	});
}
