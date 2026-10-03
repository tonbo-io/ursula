// Test 1: an arbitrary schema through plain node:sqlite — no primary keys, WITHOUT ROWID, FTS5, DDL
// (ALTER TABLE, CREATE INDEX), deletes, and one transaction big enough to spill the page cache into
// the WAL before its commit — then a rebuild of a fresh file from the stream alone.
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { attach, drainStats } from "../../src/vfs.ts";
import { freshFile } from "../helpers.ts";
import { dump, integrity, openPlain, streamPath, ursulaUrl } from "./kit.ts";

it("replicates any schema transparently and rebuilds a byte-identical file", () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	expect(attach(file, url)).toBe(0);
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
	// Cache spill: a 10-page cache and a ~2 MB transaction, so frames reach the WAL (our buffer)
	// before the commit frame, and spilled pages are rewritten in place before it.
	db.exec("PRAGMA cache_size = 10");
	db.exec("BEGIN");
	const big = db.prepare("INSERT INTO nopk VALUES (?, ?, ?)");
	for (let i = 0; i < 4000; i++) big.run(1000 + i, "z".repeat(500), `spill ${i}`);
	db.exec("UPDATE nopk SET b = 'rewritten' WHERE a % 10 = 0");
	db.exec("COMMIT");
	db.exec("PRAGMA cache_size = -2000");
	const stats = drainStats(file);
	const spill = stats.at(-1);
	console.log(`spill transaction record: ${spill?.pages} pages, ${spill?.bytes} bytes; ${stats.length} records total`);
	expect(integrity(db)).toBe("ok");
	const fts = db.prepare("SELECT title FROM docs WHERE docs MATCH 'apples' ORDER BY title").all();
	const before = dump(db);
	db.close(); // last connection: checkpoint + WAL removed

	const rebuilt = freshFile();
	const tail = attach(rebuilt, url);
	expect(tail).toBe(stats.length + 0);
	const identical = Buffer.compare(readFileSync(file), readFileSync(rebuilt)) === 0;
	console.log(`db file ${readFileSync(file).length} bytes; rebuilt ${readFileSync(rebuilt).length} bytes; byte-identical: ${identical}`);
	const r = openPlain(rebuilt);
	expect(integrity(r)).toBe("ok");
	expect(r.prepare("SELECT title FROM docs WHERE docs MATCH 'apples' ORDER BY title").all()).toEqual(fts);
	expect(dump(r)).toEqual(before);
	r.close();
	expect(identical).toBe(true);
});
