// Pi's own storage conformance suite on the full-resident store: 23 cases × {direct,
// close+reopen after every commit including failed commits}, against the fake Ursula. The open
// mode only matters across a reopen, so direct runs one mode and close+reopen runs both. The
// bounded store's matrix (indexer modes × a 4 KiB cache) is in bounded-conformance.test.ts.
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { ctx, FakeUrsula, freshPath, openOn, ReopeningStorage } from "./helpers.ts";

registerStorageConformance({ describe, expect, it }, "UrsulaStorage direct (full-resident)", async (use) => {
	const fake = new FakeUrsula();
	const storage = await openOn(fake, freshPath(), { stateStore: "full-resident" });
	try {
		await use(storage);
	} finally {
		await storage.close(ctx);
	}
});

for (const mode of ["fail-if-active", "fence"] as const) {
	registerStorageConformance({ describe, expect, it }, `UrsulaStorage close+reopen after every commit (${mode}, full-resident)`, async (use) => {
		const fake = new FakeUrsula();
		const path = freshPath();
		const storage = await ReopeningStorage.open(() => openOn(fake, path, { mode, stateStore: "full-resident" }));
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});
}
