// Pi's storage conformance on its official node driver over the VFS (openUrsulaPiStorage), in three
// modes: direct, close+reopen on the same file after every commit (re-attach catches up from the
// sidecar), and reopen on a fresh empty file after every commit (every reopen rebuilds the whole
// database from the stream).
import type { Storage } from "@earendil-works/pi-durable";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { loadUrsulaVfs, openUrsulaPiStorage } from "../src/index.ts";
import { ctx, freshFile, ReopeningStorage } from "./helpers.ts";
import { streamPath, ursulaUrl } from "./kit.ts";

loadUrsulaVfs();
const run = (name: string, make: () => Promise<Storage>) =>
	registerStorageConformance({ describe, expect, it }, `vfs: ${name}`, async (use) => {
		const storage = await make();
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});

run("direct", () => openUrsulaPiStorage(freshFile(), ursulaUrl() + streamPath()));
run("close+reopen after every commit", () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	return ReopeningStorage.open(() => openUrsulaPiStorage(file, url));
});
run("reopen on a fresh empty file after every commit", () => {
	const url = ursulaUrl() + streamPath();
	return ReopeningStorage.open(() => openUrsulaPiStorage(freshFile(), url));
});
