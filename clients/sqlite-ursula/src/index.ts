// SQLite replicated through an Ursula stream by the sqlite-ursula-vfs loadable extension
// (clients/sqlite-vfs). The extension registers the "ursula" VFS as SQLite's default; after
// `attach(path, streamUrl)` every connection that opens `path` (plain node:sqlite, Pi's official node
// driver) is replicated: each commit is appended to the stream before it reaches the local WAL.
//
// The package's root entry. It depends on nothing outside Node.js: Pi Durable's storage is the
// `@tonbo/sqlite-ursula/pi` entry (pi.ts), whose `@earendil-works/pi-durable` is an optional peer
// dependency. Nothing reachable from here may import it (test/entries.e2e.ts).
import { DatabaseSync } from "node:sqlite";
import { errcode, SQLITE_AUTH, SQLITE_IOERR_AUTH, UrsulaPaymentRequiredError, UrsulaWriteRefusedError, type WriteRefusedKind } from "./errors.ts";
import { prebuiltExtension } from "./platform.ts";

export { UrsulaPaymentRequiredError, UrsulaReplicationError, UrsulaWriteRefusedError, type WriteRefusedKind } from "./errors.ts";

let control: DatabaseSync | undefined;

/**
 * Loads the extension once per process and returns the control connection. The library is `path`,
 * else SQLITE_URSULA_VFS when set, else the prebuilt one for this platform
 * (`@tonbo/sqlite-ursula-<platform>`, an optional dependency).
 */
export function loadUrsulaVfs(path = process.env.SQLITE_URSULA_VFS): DatabaseSync {
	if (control !== undefined) return control;
	const library = path === undefined || path.length === 0 ? prebuiltExtension() : path;
	const db = new DatabaseSync(":memory:", { allowExtension: true });
	db.loadExtension(library);
	control = db;
	return db;
}

export interface AttachOptions {
	/**
	 * Attach without claiming the stream. Nothing is ever appended (no claim, commit or snapshot), so
	 * no owner is fenced, and a layer in front of the stream that refuses writes does not stop it.
	 * Every write to the file fails with SQLITE_READONLY ("attempt to write a readonly database").
	 * A failed COMMIT leaves no transaction open, but a statement of an explicit transaction whose
	 * changes outgrow the page cache fails before COMMIT, and SQLite leaves that transaction open:
	 * run ROLLBACK after such an error (until then even reads on the connection fail).
	 * The file holds the stream as of the attach: attach again to catch up. A missing stream is an
	 * error, never created.
	 */
	readonly readOnly?: boolean;
}

/**
 * Catches `file` up from the stream (installing the stream's latest snapshot first when the file is
 * missing or behind it), claims the stream for this process (fencing every earlier owner)
 * and attaches the file. No connection to `file` may be open, the process may hold no descriptor
 * on it, its `-wal`, `-shm` or `-journal` outside the extension (such as a connection opened before
 * the extension was loaded, a read-only one included), and no other process may have it open. The
 * process keeps a host lock on the file for its lifetime. Returns the stream offset the file
 * reflects (a {@link StreamOffset}). A file that has a sidecar (`<file>-ursula`, written by its first
 * attach) opens in a new process only once attached there. After a failed attach, the file opens in
 * this process only once an attach of it succeeds, whether or not it has a sidecar. An attach
 * refused up front with `in_use` (open connections, another thread attaching it, another process
 * attached to it or holding it open, a descriptor outside the extension) leaves the file, and its
 * binding, as they were. A process that opens the file while the attach runs fails it instead.
 *
 * Local files that may hold data in no stream are refused and kept, never rebuilt over
 * (`unreplicated_file`): a file never attached, one written by a connection outside the extension
 * (before, during or after an attach, as far as the files show it), one replaced or changed behind
 * the extension's back, or one a poison distrusted. The message names how to salvage the data, and
 * moving the file and its `-wal` aside lets the path attach again.
 *
 * A failed attach throws an error whose message starts with `ursula_attach: <kind>: `, a stable
 * name of what failed (such as `invalid_name`, `unreachable`, `in_use`,
 * `unreplicated_file`, `fenced`). The design doc lists the kinds and what to do about each.
 *
 * With `{ readOnly: true }` it claims nothing (see {@link AttachOptions.readOnly}). An attach that a
 * layer in front of the stream refuses with `402 Payment Required` throws
 * {@link UrsulaPaymentRequiredError}, and one whose claim is refused before it reaches the stream in
 * another way (see {@link WriteRefusedKind}) throws {@link UrsulaWriteRefusedError}. Either way nothing
 * was claimed: attach again later, and a read-only attach can still read the database meanwhile.
 */
export function attach(file: string, streamUrl: string, options: AttachOptions = {}): StreamOffset {
	const sql = options.readOnly === true ? "SELECT ursula_attach(?, ?, 'read_only') AS n" : "SELECT ursula_attach(?, ?) AS n";
	try {
		const row = loadUrsulaVfs().prepare(sql).get(file, streamUrl) as { n: string };
		return row.n;
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		if (errcode(error) === SQLITE_AUTH) throw new UrsulaPaymentRequiredError(message, null, error);
		if (errcode(error) === SQLITE_IOERR_AUTH) throw new UrsulaWriteRefusedError(message, null, null, error);
		throw error;
	}
}

/**
 * Sets the bearer token every request of the extension carries from now on, process-wide (for an
 * endpoint behind `ursula gateway --auth-*`). `null` goes back to the file named by
 * `URSULA_VFS_TOKEN_FILE`, which is read again whenever it changes and after a refused token (401).
 *
 * Call it with a fresh token before the current one expires. node:sqlite is synchronous, so a
 * commit whose token is refused retries on the thread that would call `setToken`: it waits out the
 * retry budget (`URSULA_VFS_RETRY_MS`, 30 s by default) and fails alone, as a write refused before
 * it reached the stream (`refused_kind` `unauthorized`): set a fresh token and commit again. For
 * tokens that expire on their own schedule, prefer the token file.
 */
export function setToken(token: string | null): void {
	loadUrsulaVfs().prepare("SELECT ursula_set_token(?)").get(token ?? "");
}

/**
 * A stream offset as the server wrote it (`Stream-Next-Offset`): opaque. Compare two offsets of one
 * stream only as strings (lexicographically, e.g. `a < b`); never parse or do arithmetic on them.
 * `"-1"` is the beginning of the stream, and stands for "none" where an offset may be absent.
 */
export type StreamOffset = string;

export interface AttachStatus {
	/** Stream offset after the last acknowledged commit. */
	readonly offset: StreamOffset;
	/** This owner's producer epoch; read-only, the highest epoch claimed up to `offset`. */
	readonly epoch: number;
	/** Every later commit fails until the file is re-attached. */
	readonly poisoned: boolean;
	/** Poisoned, and a newer owner claimed the stream, the stream was deleted and recreated, or another writer appended with a higher `Stream-Seq` (the first failure or a later one). Never reset until the file is attached again. */
	readonly fenced: boolean;
	/** Why it is poisoned: the first failure, which a later one never replaces. */
	readonly reason: string | null;
	/** Attached with `{ readOnly: true }`: nothing is ever appended, and every write fails with SQLITE_READONLY. */
	readonly read_only: boolean;
	/** The latest refusal that a layer in front of the stream answered is a `402 Payment Required` (a commit failed with {@link UrsulaPaymentRequiredError}, or a snapshot waits). It does not poison the file, and the next acknowledged commit clears it. A transaction refused as `too_large`, never sent or refused by a limit in front, and a commit that was `unreachable` leave it as it was: they say nothing about the layer's answer to other writes. Always `false` while `poisoned`, since a poisoned file needs a re-attach whatever the layer does. */
	readonly payment_required: boolean;
	/** The layer's explanation: the `402` response's body (its first 256 bytes, then `...` when it is longer), or the token of `x-ursula-not-applied` when the body is empty (`null` when it sent neither, or without a refusal). */
	readonly payment_reason: string | null;
	/** The latest write was refused before it reached the stream, so nothing of it was applied (a commit failed with {@link UrsulaWriteRefusedError}, or a snapshot waits). It does not poison the file, and the next acknowledged commit clears it. Always `false` while `poisoned`. A `402` that is the latest refusal shows here and in `payment_required`. Absent with an extension before 0.7.3 (an older library named by `SQLITE_URSULA_VFS`), like the two fields below. */
	readonly writes_refused?: boolean;
	/** What refused the latest write (see {@link WriteRefusedKind}); `null` without a refusal. Absent with an extension before 0.7.3. */
	readonly refused_kind?: WriteRefusedKind | null;
	/** Why: the refusing layer's response body (its first 256 bytes, then `...` when it is longer), or the token of `x-ursula-not-applied` (such as `RaftLogPressure`) when the body is empty, and always for `not_applied`. For `too_large`, the frame's size and the limit (such as `41943104-byte frame over the 33554432-byte frame limit`), or the token of a `413` from a limit in front. For `unreachable`, the last attempt's failure (such as `timeout: resolve`). `null` when the layer sent neither, or without a refusal. Absent with an extension before 0.7.3. */
	readonly refused_reason?: string | null;
	/** Offset of the latest snapshot known readable (published and read back, or found at attach); `"-1"` for none. */
	readonly snapshot: StreamOffset;
	/** Retention this owner advanced the stream to (`"-1"`: none yet). */
	readonly retained: StreamOffset;
	/** Stream offset of the local state attach started from; `"-1"` when it rebuilt the file from nothing: a fresh host, or local files it discarded because it could not check them against the stream and they hold nothing the stream lacks (written in another boot, for another incarnation of the stream after it was deleted and recreated, by an older version, or a WAL that ends before the sidecar's claim, such as after a crash or a local failure right after a commit). Files that may hold data in no stream are refused instead (`unreplicated_file`), never discarded. */
	readonly local: StreamOffset;
	/** Offset of the snapshot attach installed (`"-1"`: none). */
	readonly installed: StreamOffset;
	/** How long the attach took, in milliseconds. */
	readonly attach_ms: number;
	/** Commits acknowledged since the attach. */
	readonly commits: number;
	/** Append attempts beyond the first (unknown outcomes, 429, 503, a refused token), over those commits. */
	readonly append_retries: number;
	/** Bytes of log since the latest snapshot. A rebuild replays them, so they should stay near `snapshot_due_bytes`. */
	readonly log_bytes: number;
	/** The log a snapshot is taken at: the database's size, or `URSULA_VFS_SNAPSHOT_MIN_BYTES` if larger. */
	readonly snapshot_due_bytes: number;
	/** Milliseconds since this owner last saw a snapshot published; `null` for none since the attach. */
	readonly snapshot_age_ms: number | null;
	/** Snapshot attempts that failed in a row (0 once one succeeds), and the last failure. After a failure the next attempt waits, from 100 ms and twice as long after each further one, up to an hour while the body is too large and five minutes otherwise, give or take a fifth, unless the database shrinks by more than a quarter (a `VACUUM`). After an outage the first commit that succeeds brings the next attempt forward once. A retention move that fails is not counted: it is logged as `retention_failed` and tried again after the next snapshot. */
	readonly snapshot_failures: number;
	readonly snapshot_error: string | null;
	/** Compressed size of the latest snapshot body this owner built, published or not (`null` for none since the attach). Ursula's gateway refuses a body above its `--max-request-body-bytes` (32 MiB by default) with `413`. Absent with an extension before 0.7.3. */
	readonly snapshot_bytes?: number | null;
	/**
	 * Why writes fail now, `null` while they go through. `kind` is a stable name of what failed (the
	 * design doc lists them), `action` what to do: `retry` (nothing reached the stream, the file stays
	 * usable), `reattach` (attach the file again, which replays the stream), `stop` (another owner
	 * holds the stream) or `fix` (change the URL, the setup, the transaction or the files: after
	 * `foreign_write` or `local_damage` every attach refuses the local files until they are moved
	 * aside, and the message says how to salvage their data). `message` is the reason (`reason`'s text
	 * while poisoned). Absent with an extension before 0.7.5.
	 */
	readonly error?: { readonly kind: string; readonly action: "retry" | "reattach" | "stop" | "fix"; readonly message: string } | null;
}

export function status(file: string): AttachStatus {
	const row = loadUrsulaVfs().prepare("SELECT ursula_status(?) AS s").get(file) as { s: string };
	return JSON.parse(row.s) as AttachStatus;
}

/** One replicated commit, as measured inside the VFS. */
export interface VfsCommitStat {
	/** Frame bytes on the stream (compressed). */
	readonly bytes: number;
	/** Uncompressed record bytes. */
	readonly raw: number;
	/** Page images in the record. */
	readonly pages: number;
	/** Append requests (more than one after an unknown outcome). */
	readonly attempts: number;
	/** The append requests alone. */
	readonly append_us: number;
	/** Commit hook: record build + append + local WAL write. */
	readonly vfs_us: number;
}

/** One published (and read back) snapshot. */
export interface VfsSnapshotStat {
	/** Stream offset it reflects. */
	readonly offset: StreamOffset;
	/** Body bytes (compressed). */
	readonly bytes: number;
	/** Database bytes. */
	readonly raw: number;
	/** Page copy under the read transaction. */
	readonly copy_us: number;
	/** Checkpoint, copy, compression, publish and read-back. */
	readonly total_us: number;
}

/**
 * What the extension dropped since the last drain to keep at most 8,192 of each kind. The dropped
 * commits' frame bytes and append retries are summed, so frame byte and retry totals built from the
 * drains count every commit. Their durations are lost.
 */
export interface VfsDropped {
	readonly commits: number;
	/** The dropped commits' frame bytes. */
	readonly commit_bytes: number;
	/** The dropped commits' append attempts beyond the first. */
	readonly append_retries: number;
	readonly checkpoints: number;
	readonly snapshots: number;
}

export interface VfsStats {
	readonly commits: VfsCommitStat[];
	/** Checkpoints of the file since the last drain (time holding the checkpoint lock). */
	readonly checkpoints_us: number[];
	readonly snapshots: VfsSnapshotStat[];
	/** What was dropped since the last drain. An extension before 0.7.3 reports none: it kept up to 1,000,000 commits and checkpoints and 100,000 snapshots between drains, and dropped newer ones without counting them. */
	readonly dropped?: VfsDropped;
}

/**
 * Drains the per-commit, per-checkpoint and per-snapshot stats of an attached file: everything kept
 * since the last drain, oldest first. The extension keeps the newest 8,192 of each kind between
 * drains, 512 KiB of commits, so a file nobody drains holds a bounded amount. `dropped` counts the
 * rest.
 */
export function drainStats(file: string): VfsStats {
	const row = loadUrsulaVfs().prepare("SELECT ursula_stats(?) AS s").get(file) as { s: string };
	return JSON.parse(row.s) as VfsStats;
}

/** Attributes on a measurement, as OpenTelemetry takes them. */
export type MetricAttributes = Readonly<Record<string, string | number | boolean>>;

/**
 * The part of an OpenTelemetry `Meter` that {@link instrument} uses. A `Meter` from
 * `@opentelemetry/api` satisfies it; the package does not depend on OpenTelemetry.
 */
export interface MeterLike {
	createHistogram(name: string, options?: { description?: string; unit?: string }): { record(value: number, attributes?: MetricAttributes): void };
	createCounter(name: string, options?: { description?: string; unit?: string }): { add(value: number, attributes?: MetricAttributes): void };
	createObservableGauge(
		name: string,
		options?: { description?: string; unit?: string },
	): {
		addCallback(callback: (result: { observe(value: number, attributes?: MetricAttributes): void }) => void): void;
		removeCallback(callback: (result: { observe(value: number, attributes?: MetricAttributes): void }) => void): void;
	};
}

/**
 * Reports an attached file's health as metrics on `meter`, until the returned function is called:
 *
 * - histograms `sqlite_ursula.commit.duration` (the VFS's commit hook: frame build, append, local
 *   WAL write) and `sqlite_ursula.append.duration`, in ms;
 * - counters `sqlite_ursula.append.retries`, `sqlite_ursula.stream.bytes` and
 *   `sqlite_ursula.commit.unmeasured` (commits whose durations the histograms did not get: each
 *   drain hands over the newest 8,192 commits at most, while `status(file).commits` counts all);
 * - gauges `sqlite_ursula.log.bytes`, `sqlite_ursula.snapshot.due_bytes`, `sqlite_ursula.snapshot.age`
 *   (ms), `sqlite_ursula.snapshot.failures`, `sqlite_ursula.snapshot.bytes`,
 *   `sqlite_ursula.poisoned`, `sqlite_ursula.fenced`, `sqlite_ursula.payment_required` and
 *   `sqlite_ursula.writes_refused` (0 or 1). An extension before 0.7.3 does not report
 *   `snapshot.bytes` or `writes_refused`, so they are not measured.
 *
 * Every measurement carries `sqlite.file` and `attributes`. The per-commit numbers come from
 * {@link drainStats} every `intervalMs` (1 s by default), so do not call `drainStats` on the same
 * file elsewhere while it runs. The extension keeps the newest 8,192 commits between two drains, so
 * at the default interval the histograms see every commit up to about 8,000 commits a second. A file
 * that commits faster needs a shorter `intervalMs`.
 */
export function instrument(file: string, meter: MeterLike, options: { intervalMs?: number; attributes?: MetricAttributes } = {}): () => void {
	const attributes: MetricAttributes = { "sqlite.file": file, ...options.attributes };
	const commit = meter.createHistogram("sqlite_ursula.commit.duration", { unit: "ms", description: "VFS commit hook: frame build, append, local WAL write" });
	const append = meter.createHistogram("sqlite_ursula.append.duration", { unit: "ms", description: "Append requests of a commit" });
	const retries = meter.createCounter("sqlite_ursula.append.retries", { description: "Append attempts beyond the first" });
	const bytes = meter.createCounter("sqlite_ursula.stream.bytes", { unit: "By", description: "Frame bytes appended to the stream" });
	const unmeasured = meter.createCounter("sqlite_ursula.commit.unmeasured", { description: "Commits whose durations were dropped between two drains" });
	const drain = (): void => {
		let stats: VfsStats;
		try {
			stats = drainStats(file);
		} catch {
			return; // not attached (any more)
		}
		for (const c of stats.commits) {
			commit.record(c.vfs_us / 1000, attributes);
			append.record(c.append_us / 1000, attributes);
			if (c.attempts > 1) retries.add(c.attempts - 1, attributes);
			bytes.add(c.bytes, attributes);
		}
		// Commits dropped between two drains still count, though their durations are lost.
		const dropped = stats.dropped;
		if (dropped !== undefined) {
			if (dropped.append_retries > 0) retries.add(dropped.append_retries, attributes);
			if (dropped.commit_bytes > 0) bytes.add(dropped.commit_bytes, attributes);
			if (dropped.commits > 0) unmeasured.add(dropped.commits, attributes);
		}
	};
	const gauges: [string, string | undefined, (s: AttachStatus) => number | null][] = [
		["sqlite_ursula.log.bytes", "By", (s) => s.log_bytes],
		["sqlite_ursula.snapshot.due_bytes", "By", (s) => s.snapshot_due_bytes],
		["sqlite_ursula.snapshot.age", "ms", (s) => s.snapshot_age_ms],
		["sqlite_ursula.snapshot.failures", undefined, (s) => s.snapshot_failures],
		["sqlite_ursula.snapshot.bytes", "By", (s) => s.snapshot_bytes ?? null],
		["sqlite_ursula.poisoned", undefined, (s) => (s.poisoned ? 1 : 0)],
		["sqlite_ursula.fenced", undefined, (s) => (s.fenced ? 1 : 0)],
		["sqlite_ursula.payment_required", undefined, (s) => (s.payment_required ? 1 : 0)],
		// An extension before 0.7.3 does not report it: no measurement rather than a false 0.
		["sqlite_ursula.writes_refused", undefined, (s) => (s.writes_refused === undefined ? null : s.writes_refused ? 1 : 0)],
	];
	const observed = gauges.map(([name, unit, value]) => {
		const gauge = meter.createObservableGauge(name, unit === undefined ? {} : { unit });
		const callback = (result: { observe(value: number, attributes?: MetricAttributes): void }): void => {
			let s: AttachStatus;
			try {
				s = status(file);
			} catch {
				return;
			}
			const v = value(s);
			if (v !== null) result.observe(v, attributes);
		};
		gauge.addCallback(callback);
		return () => gauge.removeCallback(callback);
	});
	const timer = setInterval(drain, options.intervalMs ?? 1_000);
	timer.unref();
	return () => {
		clearInterval(timer);
		drain();
		for (const remove of observed) remove();
	};
}
