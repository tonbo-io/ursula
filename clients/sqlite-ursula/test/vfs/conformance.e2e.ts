// Test 4: Pi's official node driver, unmodified (`openNodeSqliteStorage(path)`), over the default
// "ursula" VFS — Pi's storage conformance in the three modes of spike 1.
import type { Storage } from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { attach, loadUrsulaVfs } from "../../src/vfs.ts";
import { ctx, freshFile, ReopeningStorage } from "../helpers.ts";
import { streamPath, ursulaUrl } from "./kit.ts";

loadUrsulaVfs();
const open = (file: string, url: string): Promise<Storage> => {
	attach(file, url);
	return openNodeSqliteStorage(file);
};
const run = (name: string, make: () => Promise<Storage>) =>
	registerStorageConformance({ describe, expect, it }, `vfs: ${name}`, async (use) => {
		const storage = await make();
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});

run("direct", () => open(freshFile(), ursulaUrl() + streamPath()));
run("close+reopen after every commit", () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	return ReopeningStorage.open(() => open(file, url));
});
run("reopen on a fresh empty file after every commit", () => {
	const url = ursulaUrl() + streamPath();
	return ReopeningStorage.open(() => open(freshFile(), url));
});
