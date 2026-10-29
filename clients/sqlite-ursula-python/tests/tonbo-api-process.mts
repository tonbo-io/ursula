// The Node suite's stand-in for the Tonbo APIs (clients/sqlite-ursula/test/tonbo-api.ts), unchanged,
// in a process of its own for tests/test_tonbo.py, run by Node from this source (type stripping). It
// prints `{"origin"}` once it listens, then answers each JSON command on a line of stdin with one
// JSON line, and stops at the end of stdin:
//
// - `{"database": name, "url", "state", "activeAfter"?, "forbidden"?}` serves database `name`.
// - `{"scripted": {"status", "retryAfter"?}}` queues an answer the next lookup gives first.
// - `{"reset": true}` forgets what it saw and what was scripted.
// - `{"seen": true}` answers `{"seen": [...]}`, the requests it received.
import { createInterface } from "node:readline";
import { type FixtureDatabase, TonboApi } from "../../sqlite-ursula/test/tonbo-api.ts";

interface Command {
	readonly database?: string;
	readonly scripted?: { status: number; retryAfter?: string };
	readonly reset?: boolean;
	readonly seen?: boolean;
}

const api = await TonboApi.start();
process.stdout.write(`${JSON.stringify({ origin: api.origin })}\n`);
for await (const line of createInterface({ input: process.stdin })) {
	if (line.trim().length === 0) continue;
	const command = JSON.parse(line) as Command & Partial<FixtureDatabase>;
	if (command.database !== undefined) {
		const { database, ...fixture } = command;
		api.databases.set(database, fixture as FixtureDatabase);
	}
	if (command.scripted !== undefined) api.scripted.push(command.scripted);
	if (command.reset === true) api.reset();
	process.stdout.write(`${JSON.stringify(command.seen === true ? { seen: api.seen } : { ok: true })}\n`);
}
await api.stop();
