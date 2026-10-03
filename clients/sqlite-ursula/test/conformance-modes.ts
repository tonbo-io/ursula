// Pi's 23 storage conformance cases in three modes over any WalStream factory: direct, close+reopen
// on the same file after every commit, and reopen on a fresh empty file after every commit (every
// reopen rebuilds the whole database from the stream).
import type { Storage } from "@earendil-works/pi-durable";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { openPiStorage } from "../src/pi.ts";
import type { WalStream } from "../src/stream.ts";
import { ctx, freshFile, ReopeningStorage } from "./helpers.ts";

export function registerModes(label: string, newStream: () => WalStream): void {
	const run = (name: string, open: () => Promise<Storage>) =>
		registerStorageConformance({ describe, expect, it }, `${label}: ${name}`, async (use) => {
			const storage = await open();
			try {
				await use(storage);
			} finally {
				await storage.close(ctx);
			}
		});
	run("direct", () => openPiStorage(freshFile(), newStream()));
	run("close+reopen after every commit", () => {
		const stream = newStream();
		const file = freshFile();
		return ReopeningStorage.open(() => openPiStorage(file, stream));
	});
	run("reopen on a fresh empty file after every commit", () => {
		const stream = newStream();
		return ReopeningStorage.open(() => openPiStorage(freshFile(), stream));
	});
}
