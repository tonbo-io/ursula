// Pi's storage conformance suite on the bounded owner (design §10 M3 exit): 23 cases ×
// {direct, close+reopen after every commit} × indexer modes {normal, paused, aggressive, flaky} ×
// a 4 KiB cache, against the fake Ursula.
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { INDEXER_MODES, fakeFor, openBounded } from "./bounded-helpers.ts";
import { ctx, freshPath, ReopeningStorage } from "./helpers.ts";

for (const mode of INDEXER_MODES) {
	registerStorageConformance({ describe, expect, it }, `bounded owner direct (indexer ${mode}, 4 KiB cache)`, async (use) => {
		const fake = fakeFor(mode);
		const storage = await openBounded(fake, freshPath());
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
		expect(storage.poison).toBeUndefined();
	});

	registerStorageConformance({ describe, expect, it }, `bounded owner close+reopen after every commit (indexer ${mode}, 4 KiB cache)`, async (use) => {
		const fake = fakeFor(mode);
		const path = freshPath();
		const storage = await ReopeningStorage.open(() => openBounded(fake, path));
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});
}
