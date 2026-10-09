// The soak's verification (bench/soak.ts): attaches the owner's file, so it catches up and holds
// the stream at its offset, then rebuilds a fresh copy from the stream, and compares the two:
// `PRAGMA integrity_check` on both, and a hash of every table's rows. The fresh copy's attach
// appends a claim frame (so its offset is past the owner's) and fences the owner's file, which
// nothing writes afterwards; the owner's next segment attaches it again.
//
//   node bench/verify.ts <stream url> <owner file> <copy file> <out.json>
import { createHash } from "node:crypto";
import { rmSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { attach, status } from "../src/index.ts";

if (process.argv.length < 6) throw new Error("usage: verify.ts <url> <owner file> <copy file> <out.json>");
const [url, file, copy, out] = process.argv.slice(2) as [string, string, string, string];

function digest(path: string): { integrity: string; tables: Record<string, string> } {
	const db = new DatabaseSync(path, { readOnly: true });
	try {
		const integrity = (db.prepare("PRAGMA integrity_check").get() as { integrity_check: string }).integrity_check;
		const tables: Record<string, string> = {};
		const names = db.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all() as { name: string }[];
		for (const { name } of names) {
			const hash = createHash("sha256");
			for (const row of db.prepare(`SELECT * FROM "${name.replaceAll('"', '""')}" ORDER BY 1`).iterate()) {
				hash.update(JSON.stringify(row, (_, v) => (typeof v === "bigint" ? v.toString() : v instanceof Uint8Array ? Buffer.from(v).toString("hex") : v)));
			}
			tables[name] = hash.digest("hex");
		}
		return { integrity, tables };
	} finally {
		db.close();
	}
}

try {
	attach(file, url);
	const offset = status(file).offset;
	attach(copy, url);
	const copyOffset = status(copy).offset;
	const [a, b] = [digest(file), digest(copy)];
	const same = a.integrity === "ok" && b.integrity === "ok" && JSON.stringify(a.tables) === JSON.stringify(b.tables);
	writeFileSync(out, JSON.stringify({ same, offset, copy_offset: copyOffset, integrity: [a.integrity, b.integrity], tables: Object.keys(a.tables).length, differing: Object.keys(a.tables).filter((t) => a.tables[t] !== b.tables[t]) }));
} catch (error) {
	writeFileSync(out, JSON.stringify({ same: false, error: String(error) }));
} finally {
	rmSync(copy, { force: true });
	rmSync(`${copy}-wal`, { force: true });
	rmSync(`${copy}-shm`, { force: true });
	rmSync(`${copy}-ursula`, { force: true });
	rmSync(`${copy}-ursula.lock`, { force: true });
}
process.exit(0);
