// A token change sends what a `401` blocked again at once (design: Transport), against the e2e
// server (test/stack.ts) behind test/gate.mjs, which answers `401` to every request without the
// token it requires. A bound database's replication blocks `unauthorized` once the gate requires
// another token, and resumes far within the 30 s retry of a blocked condition once the process has
// that token: set with `ursula_set_token`, or written to the file `URSULA_VFS_TOKEN_FILE` names,
// even when another database's requests read the changed file first.
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase } from "../src/index.ts";
import { ext, freshDir, gate, stream, ursulaUrl } from "./kit.ts";

/** The token file. Read once, when the extension first needs it: before it loads. */
const TOKEN_FILE = join(freshDir(), "token");
process.env.URSULA_VFS_TOKEN_FILE = TOKEN_FILE;

/** How long a resend after a token change may take: far below the 30 s retry of a blocked request. */
const RESUME_MS = 10_000;

let g: Awaited<ReturnType<typeof gate>>;

beforeAll(async () => {
	g = await gate(ursulaUrl());
});

afterAll(async () => {
	await g?.stop();
});

/** Makes the gate answer `401` to every request without `token` (`null`: none required). */
async function requireToken(token: string | null): Promise<void> {
	const r = await fetch(`${g.control}/auth`, { method: "PUT", body: JSON.stringify({ token }) });
	expect(r.status, await r.text()).toBe(204);
}

/** Waits until the database's replication is blocked `unauthorized`, for at most 30 s. */
async function blockedUnauthorized(db: UrsulaDatabase): Promise<void> {
	const deadline = Date.now() + 30_000;
	while (db.status().replication.condition !== "unauthorized") {
		if (Date.now() > deadline) throw new Error(`never blocked unauthorized: ${JSON.stringify(db.status().replication)}`);
		await new Promise((r) => setTimeout(r, 50));
	}
}

/** Opens a stream through the gate, commits a table, and blocks a second commit with a `401`. */
async function blockedDatabase(label: string, token: string | undefined, next: string): Promise<UrsulaDatabase> {
	const s = stream(label);
	const db = await open(`${g.url}${s.path}`, { dir: freshDir(), token });
	db.exec("CREATE TABLE t(i INTEGER PRIMARY KEY)");
	await db.synced();
	await requireToken(next);
	db.exec("INSERT INTO t VALUES (1)");
	await blockedUnauthorized(db);
	return db;
}

describe("a token change after a 401", () => {
	it("resends at once with the token ursula_set_token sets", async () => {
		const control = loadUrsulaVfs(ext());
		await requireToken("set-token-1");
		const db = await blockedDatabase("token-set", "set-token-1", "set-token-22");
		try {
			const started = Date.now();
			control.prepare("SELECT ursula_set_token('', ?)").get("set-token-22");
			await db.synced();
			expect(Date.now() - started).toBeLessThan(RESUME_MS);
			expect(db.status().replication.condition).toBeNull();
		} finally {
			db.close();
			control.prepare("SELECT ursula_set_token('', '')").get();
			await requireToken(null);
		}
	});

	it("resends at once with the token a changed token file holds, whoever reads it first", async () => {
		const control = loadUrsulaVfs(ext());
		// Back to the token file.
		control.prepare("SELECT ursula_set_token('', '')").get();
		writeFileSync(TOKEN_FILE, "file-token-1\n");
		await requireToken("file-token-1");
		const db = await blockedDatabase("token-file", undefined, "file-token-22");
		let other: UrsulaDatabase | undefined;
		try {
			const started = Date.now();
			writeFileSync(TOKEN_FILE, "file-token-22\n");
			// Another database's open reads the changed file before the driver's own check does.
			other = await open(`${g.url}${stream("token-file-other").path}`, { dir: freshDir() });
			await db.synced();
			expect(Date.now() - started).toBeLessThan(RESUME_MS);
			expect(db.status().replication.condition).toBeNull();
		} finally {
			other?.close();
			db.close();
			await requireToken(null);
		}
	});
});
