// An owner's health for operators: `ursula_status` reports commits, retries, the log since the
// latest snapshot against the size a snapshot is taken at, the snapshot's age and failures, and the
// attach time; `instrument()` turns them into OpenTelemetry-shaped metrics; and the extension logs
// one logfmt line per event on stderr.
import { expect, it } from "vitest";
import { attach, instrument, type MeterLike, type MetricAttributes, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

/** A meter that keeps what it is given. */
function recordingMeter() {
	const records = new Map<string, number[]>();
	const callbacks = new Map<string, (result: { observe(value: number, attributes?: MetricAttributes): void }) => void>();
	const keep = (name: string, value: number) => records.set(name, [...(records.get(name) ?? []), value]);
	const meter: MeterLike = {
		createHistogram: (name) => ({ record: (value) => keep(name, value) }),
		createCounter: (name) => ({ add: (value) => keep(name, value) }),
		createObservableGauge: (name) => ({
			addCallback: (callback) => callbacks.set(name, callback),
			removeCallback: () => callbacks.delete(name),
		}),
	};
	const observe = (name: string): number | undefined => {
		let seen: number | undefined;
		callbacks.get(name)?.({ observe: (value) => (seen = value) });
		return seen;
	};
	return { meter, records, callbacks, observe };
}

it("status reports an owner's health, and instrument() reports it as metrics", () => {
	const file = freshFile();
	attach(file, ursulaUrl() + streamPath());
	const { meter, records, callbacks, observe } = recordingMeter();
	const stop = instrument(file, meter, { intervalMs: 60_000 });
	const db = openPlain(file);
	db.exec("CREATE TABLE t(x TEXT)");
	for (let i = 0; i < 5; i++) db.exec(`INSERT INTO t VALUES ('r${i}')`);
	db.close();
	const s = status(file);
	expect(s).toMatchObject({ commits: 6, append_retries: 0, snapshot_failures: 0, snapshot_error: null, snapshot_age_ms: null });
	expect(s.attach_ms).toBeGreaterThanOrEqual(0);
	expect(s.log_bytes).toBeGreaterThan(0);
	expect(s.snapshot_due_bytes).toBeGreaterThanOrEqual(s.log_bytes);
	expect(observe("sqlite_ursula.log.bytes")).toBe(s.log_bytes);
	expect(observe("sqlite_ursula.poisoned")).toBe(0);
	expect(observe("sqlite_ursula.snapshot.age")).toBeUndefined();
	stop();
	expect(records.get("sqlite_ursula.commit.duration")).toHaveLength(6);
	expect(records.get("sqlite_ursula.append.duration")).toHaveLength(6);
	expect(records.get("sqlite_ursula.append.retries")).toBeUndefined();
	expect((records.get("sqlite_ursula.stream.bytes") ?? []).reduce((a, b) => a + b, 0)).toBeGreaterThan(0);
	expect(callbacks.size).toBe(0);
});

it("snapshots show in the age and failure fields, and a poisoned owner logs a logfmt event", async () => {
	const url = ursulaUrl() + streamPath();
	// One row rewritten: the database stays small while the log outgrows it, so snapshots are taken.
	const rows = Array.from({ length: 20 }, () => "INSERT OR REPLACE INTO t VALUES (1, hex(randomblob(800)))");
	const child = runChild(freshFile(), url, ["CREATE TABLE t(k INTEGER PRIMARY KEY, x TEXT)", ...rows, "@sleep:2000", "INSERT INTO t VALUES (100, 'after')"], {
		URSULA_VFS_SNAPSHOT_MIN_BYTES: "1",
		CHILD_EXIT: "1",
	});
	await child.waitFor((l) => l.step === rows.length + 1 && l.phase === "start");
	// A new owner claims the stream while the child sleeps: the child's next commit is fenced.
	attach(freshFile(), url);
	const done = await child.waitFor((l) => l.done === true);
	expect(done.poisoned).toBe(true);
	expect(done.health?.commits).toBe(rows.length + 1);
	expect(done.health?.snapshot_age_ms).not.toBeNull();
	expect(done.health).toMatchObject({ snapshot_failures: 0, snapshot_error: null });
	await child.exited;
	expect(child.stderr()).toMatch(/^sqlite-ursula-vfs level=warn event=poisoned file=\S+ stream=\S+ fenced=true reason="fenced: epoch \d+ superseded by Some\(\d+\) \(403\)"$/m);
});
