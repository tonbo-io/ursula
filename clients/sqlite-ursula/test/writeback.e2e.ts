// The local files are not fsynced on the commit path, so a write-back error underneath a running
// process (EIO from the device, a thin-provisioned volume out of space) can drop a WAL page the
// kernel already took: SQLite, which does not check WAL frames on a read, would read the older
// bytes back, and the owner's next commits would carry them into the stream. Every WAL page this
// process wrote is checked when it is read back. A page changed underneath the process stands in
// for the lost write-back here: the read fails, the owner is poisoned (not fenced), and the stream
// keeps exactly what was acknowledged.
import { closeSync, openSync, writeSync } from "node:fs";
import { expect, it } from "vitest";
import { attach, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { openPlain, streamPath, ursulaUrl } from "./kit.ts";

it("a WAL page that reads back other bytes than were written poisons the owner before it commits", () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const writer = openPlain(file);
	writer.exec("CREATE TABLE t(x TEXT)");
	writer.exec("INSERT INTO t VALUES ('acked')");
	// Frame 1's page (after the 32-byte WAL header and the 24-byte frame header), as if its
	// write-back had failed and an older block came back.
	const fd = openSync(`${file}-wal`, "r+");
	writeSync(fd, Buffer.alloc(4096), 0, 4096, 32 + 24);
	closeSync(fd);
	// A new connection reads page 1 from that frame.
	expect(() => openPlain(file)).toThrow(/disk I\/O error/);
	expect(status(file)).toMatchObject({ poisoned: true, fenced: false });
	expect(status(file).reason).toMatch(/reads back other bytes/);
	expect(() => writer.exec("INSERT INTO t VALUES ('after')")).toThrow(/disk I\/O error/);
	const copy = freshFile();
	attach(copy, url);
	const db = openPlain(copy);
	expect(db.prepare("SELECT x FROM t").all()).toEqual([{ x: "acked" }]);
	db.close();
});
