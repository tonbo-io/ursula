// Pi's storage conformance suite (23 cases) × {direct, close+reopen after every commit} × both open
// modes, against a real single-node ursula through the HTTP transports (design §10 M1 exit).
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { ctx, ReopeningStorage } from "../helpers.ts";
import { freshStream, openHttp } from "./env.ts";

for (const mode of ["fail-if-active", "fence"] as const) {
	registerStorageConformance({ describe, expect, it }, `ursula e2e direct (${mode})`, async (use) => {
		const storage = await openHttp(freshStream(), { mode });
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});

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
