// Pi's storage conformance suite (23 cases) against a real single-node ursula through the HTTP
// transports (design §10 M1 exit): direct, and close+reopen after every commit in both open modes
// (the open mode only matters across a reopen).
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { ctx, ReopeningStorage } from "../helpers.ts";
import { freshStream, openHttp } from "./env.ts";

registerStorageConformance({ describe, expect, it }, "ursula e2e direct", async (use) => {
	const storage = await openHttp(freshStream());
	try {
		await use(storage);
	} finally {
		await storage.close(ctx);
	}
});

for (const mode of ["fail-if-active", "fence"] as const) {
	registerStorageConformance({ describe, expect, it }, `ursula e2e close+reopen after every commit (${mode})`, async (use) => {
		const stream = freshStream();
		const storage = await ReopeningStorage.open(() => openHttp(stream, { mode }));
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});
}
