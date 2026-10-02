import type { Storage } from "@earendil-works/pi-durable";
import type { FakeUrsula } from "../src/fake/index.ts";
import type { UrsulaStorage } from "../src/storage.ts";
import { ctx, openOn } from "./helpers.ts";

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
export async function reopen(fake: FakeUrsula, path: string, current: UrsulaStorage, graceful: boolean): Promise<UrsulaStorage> {
	if (graceful) {
		await current.close(ctx);
		return openOn(fake, path);
	}
	const next = await openOn(fake, path, { mode: "fence" });
	await current.close(ctx); // the fenced zombie's close marker must fail and not disturb the new owner
	return next;
}

export const trials = (name: string, fallback: number): number => Number(process.env[name] ?? fallback);

export type { Storage };
