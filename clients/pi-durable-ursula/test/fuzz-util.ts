import type { Storage } from "@earendil-works/pi-durable";
import { FakeUrsula } from "../src/fake/index.ts";
import type { UrsulaStorage, UrsulaStorageOptions } from "../src/storage.ts";
import { BOUNDED_TIMING, fakeFor } from "./bounded-helpers.ts";
import { ctx, openOn } from "./helpers.ts";

/** Which owner a fuzz seed runs against. */
export interface OwnerVariant {
	readonly name: string;
	readonly fake: () => FakeUrsula;
	readonly options: Partial<UrsulaStorageOptions>;
	/** Fraction of the configured trials this variant runs. */
	readonly share: number;
}

export const VARIANTS: readonly OwnerVariant[] = [
	{ name: "bounded", fake: () => new FakeUrsula(), options: {}, share: 1 },
	{
		name: "bounded, 4 KiB cache, flaky lagging indexer",
		fake: () => fakeFor("flaky"),
		options: { stateStore: "bounded", cacheBudgetBytes: 4096, timing: BOUNDED_TIMING },
		// Every snapshot re-reads most of the state through the 4 KiB cache and a faulty indexer (up
		// to ~1,000 keyed-state reads per step late in a trial), so this variant runs few trials.
		share: 0.03,
	},
	{ name: "full-resident", fake: () => new FakeUrsula(), options: { stateStore: "full-resident" }, share: 0.5 },
];

/** Deterministic LCG, as in the original model fuzzers. */
export function rng(seed: number): { rnd: () => number; pick: <T>(xs: readonly T[]) => T } {
	let s = seed;
	const rnd = (): number => {
		s = (s * 1103515245 + 12345) & 0x7fffffff;
		return s / 0x7fffffff;
	};
	return { rnd, pick: <T>(xs: readonly T[]): T => xs[Math.floor(rnd() * xs.length)] as T };
}

/**
 * Reopen the owner. Half the time the old owner closes first (fail-if-active open); otherwise it is
 * abandoned without a close marker and the new owner fences it (crash takeover).
 */
export async function reopen(
	fake: FakeUrsula,
	path: string,
	current: UrsulaStorage,
	graceful: boolean,
	options: Partial<UrsulaStorageOptions> = {},
): Promise<UrsulaStorage> {
	if (graceful) {
		await current.close(ctx);
		return openOn(fake, path, options);
	}
	const next = await openOn(fake, path, { ...options, mode: "fence" });
	await current.close(ctx); // the fenced zombie's close marker must fail and not disturb the new owner
	return next;
}

export const trials = (name: string, fallback: number): number => Number(process.env[name] ?? fallback);

export type { Storage };
