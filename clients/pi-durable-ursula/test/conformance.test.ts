// Pi's own storage conformance suite: 23 cases × {direct, close+reopen after every commit
// including failed commits} × both open modes, against the fake Ursula.
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { ctx, FakeUrsula, freshPath, openOn, ReopeningStorage } from "./helpers.ts";

for (const mode of ["fail-if-active", "fence"] as const) {
	registerStorageConformance({ describe, expect, it }, `UrsulaStorage direct (${mode})`, async (use) => {
		const fake = new FakeUrsula();
		const storage = await openOn(fake, freshPath(), { mode });
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});

	registerStorageConformance({ describe, expect, it }, `UrsulaStorage close+reopen after every commit (${mode})`, async (use) => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const storage = await ReopeningStorage.open(() => openOn(fake, path, { mode }));
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});
}
