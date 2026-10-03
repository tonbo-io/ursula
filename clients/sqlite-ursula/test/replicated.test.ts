import { describe, expect, it } from "vitest";
import { type CrashPoint, FencedError, PoisonedError, ReplicatedSqlite } from "../src/replicated.ts";
import { FakeUrsula } from "../src/stream.ts";
import { freshFile } from "./helpers.ts";

const SCHEMA = "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL) STRICT";
const rows = (db: ReplicatedSqlite) => db.all<{ k: string; v: string }>("SELECT k, v FROM kv ORDER BY k");

async function put(db: ReplicatedSqlite, k: string, v: string): Promise<void> {
	await db.transaction(async (tx) => {
		await tx.run("INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v", k, v);
	});
}

describe("ReplicatedSqlite", () => {
	it("round trip: a fresh empty file on host B rebuilds host A's rows", async () => {
		const fake = new FakeUrsula();
		const a = await ReplicatedSqlite.open(freshFile(), fake.stream("/b/rt"));
		await a.exec(SCHEMA);
		for (let i = 0; i < 250; i++) await put(a, `k${i % 100}`, `v${i}`);
		await a.run("DELETE FROM kv WHERE k = ?", "k7");
		const expected = await rows(a);
		await a.close();
		const b = await ReplicatedSqlite.open(freshFile(), fake.stream("/b/rt"));
		expect(await rows(b)).toEqual(expected);
		expect(b.nextRecord).toBe(fake.streams.get("/b/rt")?.length);
		await b.close();
	});

	for (const [point, present] of [
		["before-append", false],
		["before-commit", true],
		[undefined, true],
	] as const) {
		it(`crash ${point ?? "after COMMIT"}: the write is ${present ? "present" : "absent"} after reopen`, async () => {
			const fake = new FakeUrsula();
			const file = freshFile();
			let armed: CrashPoint | undefined;
			const a = await ReplicatedSqlite.open(file, fake.stream("/b/c"), { crash: (p) => p === armed });
			await a.exec(SCHEMA);
			await put(a, "before", "1");
			armed = point;
			if (point === undefined) {
				await put(a, "x", "1");
				a.raw.close(); // die after COMMIT, without a clean close
			} else {
				await expect(put(a, "x", "1")).rejects.toThrow(`crashed at ${point}`);
			}
			const reopened = await ReplicatedSqlite.open(file, fake.stream("/b/c"));
			const keys = (await rows(reopened)).map((r) => r.k);
			expect(keys).toEqual(present ? ["before", "x"] : ["before"]);
			await reopened.close();
		});
	}

	it("fencing: a stale owner's commit fails, rolls back and poisons; the new owner is unaffected", async () => {
		const fake = new FakeUrsula();
		const a = await ReplicatedSqlite.open(freshFile(), fake.stream("/b/f"));
		await a.exec(SCHEMA);
		await put(a, "a", "1");
		const b = await ReplicatedSqlite.open(freshFile(), fake.stream("/b/f"));
		await put(b, "b", "1");
		await expect(put(a, "a", "2")).rejects.toBeInstanceOf(FencedError);
		expect(await a.all("SELECT k FROM kv").catch((e: unknown) => e)).toBeInstanceOf(PoisonedError);
		const local = a.raw.prepare("SELECT k, v FROM kv ORDER BY k").all();
		expect(local.map((r) => r.v)).toEqual(["1"]); // rolled back: a=1 only
		await a.close();
		await put(b, "c", "1");
		expect((await rows(b)).map((r) => `${r.k}=${r.v}`)).toEqual(["a=1", "b=1", "c=1"]);
		await b.close();
		const c = await ReplicatedSqlite.open(freshFile(), fake.stream("/b/f"));
		expect((await rows(c)).map((r) => `${r.k}=${r.v}`)).toEqual(["a=1", "b=1", "c=1"]);
		await c.close();
	});
});
