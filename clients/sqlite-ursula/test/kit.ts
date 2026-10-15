import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, inject } from "vitest";

/** The base URL of the server the suite runs against (test/stack.ts). */
export const ursulaUrl = (): string => inject("ursulaUrl");

/** Whether the suite runs against the model server (E2E_SERVER=model). */
export const onModel = (): boolean => inject("ursulaServer") === "model";

/** The extension the suites load. */
export const ext = (): string => {
	const path = process.env.SQLITE_URSULA_VFS;
	if (path === undefined || path.length === 0) throw new Error("set SQLITE_URSULA_VFS");
	return path;
};

let n = 0;
/** A stream of its own for each test: its URL and its path on the server. */
export function stream(label: string): { url: string; path: string } {
	n += 1;
	const path = `/e2e/${label}-${process.pid}-${Date.now().toString(36)}-${n}`;
	return { url: `${ursulaUrl()}${path}`, path };
}

/** A new cache root. */
export const freshDir = (): string => mkdtempSync(join(tmpdir(), "sqlite-ursula-e2e-"));

/** The `USQ2` frames the model holds for `path`, by kind (1 claim, 2 commit). */
export async function frames(path: string): Promise<{ claims: number; commits: number }> {
	const r = await fetch(`${ursulaUrl()}/__model/frames?path=${path}`);
	const bytes = new Uint8Array(await r.arrayBuffer());
	const view = new DataView(bytes.buffer);
	const count = { claims: 0, commits: 0 };
	for (let at = 0; at < bytes.length; ) {
		expect(new TextDecoder().decode(bytes.subarray(at, at + 4))).toBe("USQ2");
		const kind = bytes[at + 4];
		const len = view.getUint32(at + 5, true);
		if (kind === 1) count.claims += 1;
		if (kind === 2) count.commits += 1;
		at += 13 + len;
	}
	return count;
}

/** Scripts a fault of the model (clients/sqlite-vfs/src/model/faults.rs) for its next `times` requests of `method`. */
export async function fault(line: string, times = 1, method?: string): Promise<void> {
	const query = `times=${times}${method === undefined ? "" : `&method=${method}`}`;
	const r = await fetch(`${ursulaUrl()}/__model/faults?${query}`, { method: "POST", body: line });
	expect(r.status, await r.text()).toBe(204);
}

/** The error `promise` rejects with (it must reject). */
export async function rejection(promise: Promise<unknown>): Promise<unknown> {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error("it resolved");
}

/** The error `f` throws (it must throw). */
export function thrown(f: () => unknown): unknown {
	try {
		f();
	} catch (error) {
		return error;
	}
	throw new Error("it did not throw");
}
