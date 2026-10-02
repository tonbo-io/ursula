// Pi's own storage conformance suite: 23 cases × {direct, close+reopen after every commit
// including failed commits} × both open modes × {bounded (default), full-resident} state stores,
// against the fake Ursula. The bounded store's indexer-mode × 4 KiB-cache matrix is in
// bounded-conformance.test.ts.
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { ctx, FakeUrsula, freshPath, openOn, ReopeningStorage } from "./helpers.ts";

for (const [mode, stateStore] of [
	["fail-if-active", "auto"],
	["fence", "auto"],
	["fail-if-active", "full-resident"],
	["fence", "full-resident"],
] as const) {
	const label = stateStore === "auto" ? mode : `${mode}, ${stateStore}`;
	registerStorageConformance({ describe, expect, it }, `UrsulaStorage direct (${label})`, async (use) => {
		const fake = new FakeUrsula();
		const storage = await openOn(fake, freshPath(), { mode, stateStore });
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});

	registerStorageConformance({ describe, expect, it }, `UrsulaStorage close+reopen after every commit (${label})`, async (use) => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const storage = await ReopeningStorage.open(() => openOn(fake, path, { mode, stateStore }));
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});
}
