// An arbitrary schema through plain node:sqlite, rebuilt byte-identical from the stream; and the
// write-transaction scope of the WAL overlay with two connections on one file.
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { attach, drainStats, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { dump, integrity, openPlain, streamPath, ursulaUrl } from "./kit.ts";

it("replicates any schema transparently and rebuilds a byte-identical file", () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const db = openPlain(file);
	db.exec("CREATE TABLE nopk(a, b)");
	db.exec("CREATE TABLE kv(k TEXT PRIMARY KEY, v BLOB) WITHOUT ROWID");
	db.exec("CREATE VIRTUAL TABLE docs USING fts5(title, body)");
	const ins = db.prepare("INSERT INTO nopk VALUES (?, ?)");
	for (let i = 0; i < 50; i++) ins.run(i % 7, `row ${i}`); // 50 autocommit transactions
	db.exec("BEGIN");
	for (let i = 0; i < 200; i++) db.prepare("INSERT INTO kv VALUES (?, ?)").run(`k${i}`, Buffer.alloc(100, i));
	db.exec("COMMIT");
	db.exec("INSERT INTO docs VALUES ('apple pie', 'a recipe with apples'), ('pear', 'nothing about apples'), ('fig', 'figs only')");
	db.exec("ALTER TABLE nopk ADD COLUMN c DEFAULT 'x'");
	db.exec("CREATE INDEX nopk_a ON nopk(a)");
	db.exec("UPDATE nopk SET c = 'y' WHERE a = 3");
	db.exec("DELETE FROM nopk WHERE a = 5");
	db.exec("DELETE FROM kv WHERE k LIKE 'k1%'");
	// Cache spill: a 10-page cache and a ~2 MB transaction, so frames reach the WAL (the overlay)
	// before the commit frame, and spilled pages are rewritten in place before it.
	db.exec("PRAGMA cache_size = 10");
	db.exec("BEGIN");
	const big = db.prepare("INSERT INTO nopk VALUES (?, ?, ?)");
	for (let i = 0; i < 4000; i++) big.run(1000 + i, "z".repeat(500), `spill ${i}`);
	db.exec("UPDATE nopk SET b = 'rewritten' WHERE a % 10 = 0");
	db.exec("COMMIT");
	db.exec("PRAGMA cache_size = -2000");
	const { commits } = drainStats(file);
	const spill = commits.at(-1);
	console.log(`spill transaction frame: ${spill?.pages} pages, ${spill?.raw} raw bytes, ${spill?.bytes} on the stream; ${commits.length} commits`);
	expect(integrity(db)).toBe("ok");
	const fts = db.prepare("SELECT title FROM docs WHERE docs MATCH 'apples' ORDER BY title").all();
	const before = dump(db);
	const offset = status(file).offset;
	db.close(); // last connection: checkpoint + WAL removed

	const rebuilt = freshFile();
	expect(attach(rebuilt, url)).toBeGreaterThan(offset); // plus the rebuild's own claim
	const identical = Buffer.compare(readFileSync(file), readFileSync(rebuilt)) === 0;
	console.log(`db file ${readFileSync(file).length} bytes; rebuilt ${readFileSync(rebuilt).length} bytes; byte-identical: ${identical}`);
	const r = openPlain(rebuilt);
	expect(integrity(r)).toBe("ok");
	expect(r.prepare("SELECT title FROM docs WHERE docs MATCH 'apples' ORDER BY title").all()).toEqual(fts);
	expect(dump(r)).toEqual(before);
	r.close();
	expect(identical).toBe(true);
});

// Regression (#322 review, P1-2): the overlay was per WAL handle and outlived a rolled-back
// transaction, so a connection that spilled and rolled back later read its stale frames instead of
// the frames another connection committed at the same WAL offsets.
it("a rolled-back spill never shadows another connection's commit", () => {
	const file = freshFile();
	attach(file, ursulaUrl() + streamPath());
	const a = openPlain(file);
	const b = openPlain(file);
	a.exec("CREATE TABLE t(x TEXT)");
	a.exec("PRAGMA cache_size = 10");
	const xs = (): string[] => (a.prepare("SELECT x FROM t WHERE x LIKE 'b%' ORDER BY x").all() as { x: string }[]).map((r) => r.x);
	for (let i = 0; i < 3; i++) {
		a.exec("BEGIN");
		a.exec("WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 2000) INSERT INTO t SELECT 'a' || hex(zeroblob(250)) FROM c");
		a.exec("ROLLBACK");
		b.exec(`INSERT INTO t VALUES ('b${i}')`);
		expect(xs()).toEqual(Array.from({ length: i + 1 }, (_, j) => `b${j}`));
		expect(integrity(a)).toBe("ok");
	}
	a.close();
	b.close();
});
