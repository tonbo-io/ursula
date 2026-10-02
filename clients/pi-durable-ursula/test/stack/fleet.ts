// A fleet of live bounded owners for the drills: each owner opens its own harness stream and
// commits one Pi entry (with a Session-line read of it) per step until stopped. Poison reopens the
// owner in `fence` mode, as a host does; `StorageRejected` is what Pi turns into a faulted task, so
// it is counted separately. Every append body is captured, so `verify()` can check that each
// acknowledged commit is stored byte-for-byte at its Seq (design §3.3, §6.3).
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { type EntryId, StorageRejected, type StorageWrite } from "@earendil-works/pi-durable";
import { httpTransports } from "../../src/http.ts";
import { type Timing, UrsulaStorage } from "../../src/storage.ts";
import type { LogTransport } from "../../src/transport.ts";
import { sleep } from "./proc.ts";

const ctx = BACKGROUND_CONTEXT;

export interface FleetOptions {
	/** Node or gateway URL. */
	readonly baseUrl: string;
	readonly bucket: string;
	readonly owners: number;
	/** Pause between an owner's commits (default 50 ms). */
	readonly commitIntervalMs?: number;
	/** Text bytes per entry (default 256). */
	readonly payloadBytes?: number;
	/** Overlay hard cap (default the owner's 256 MiB). */
	readonly overlayCapBytes?: number;
	readonly timing?: Partial<Timing>;
	/** Stream name prefix (default `drill`). */
	readonly prefix?: string;
}

export interface FleetCounters {
	commits: number;
	/** Commits or reads that poisoned the owner. */
	poison: number;
	/** `StorageRejected`: a faulted Pi task. */
	faulted: number;
	/** Opens that failed (retried). */
	openFailures: number;
	reopens: number;
	/** Keyed-state reads the owners had to make on the Session line. */
	remoteReads: number;
}

/** One live owner. */
export class FleetOwner {
	readonly stream: string;
	storage: UrsulaStorage | undefined;
	readonly counters: FleetCounters = { commits: 0, poison: 0, faulted: 0, openFailures: 0, reopens: 0, remoteReads: 0 };
	readonly errors: string[] = [];
	/** Seq → the bytes of the acknowledged commit at that Seq. */
	readonly acked = new Map<number, Uint8Array>();
	private readonly sent = new Map<number, Uint8Array>();
	private readonly log: LogTransport;
	private readonly keyedState: ReturnType<typeof httpTransports>["keyedState"];
	private stopped = false;
	/** While paused, an owner without an open storage does not (re)open. */
	paused = false;
	private loop: Promise<void> | undefined;
	private opened = false;
	private readonly options: FleetOptions;

	constructor(options: FleetOptions, stream: string) {
		this.options = options;
		this.stream = stream;
		const t = httpTransports({ baseUrl: options.baseUrl, stream });
		this.keyedState = t.keyedState;
		this.log = {
			head: () => t.log.head(),
			create: () => t.log.create(),
			append: (body, match) => {
				this.sent.set(match, body);
				return t.log.append(body, match);
			},
			readRecords: (from, o) => t.log.readRecords(from, o),
		};
	}

	/** `tail − E`: records the owner holds in its overlay. */
	get overlayRecords(): number {
		const s = this.storage;
		const local = s?.localStore;
		if (s === undefined || local === undefined) return 0;
		return s.tail - local.overlayFloor;
	}

	get overlayBytes(): number {
		return this.storage?.localStore?.overlayBytes ?? 0;
	}

	private note(error: unknown): void {
		this.errors.push(error instanceof Error ? `${error.name}: ${error.message}` : String(error));
		if (this.errors.length > 50) this.errors.shift();
	}

	private async open(): Promise<UrsulaStorage> {
		const storage = await UrsulaStorage.open({
			log: this.log,
			keyedState: this.keyedState,
			stateStore: "bounded",
			mode: this.opened ? "fence" : "fail-if-active",
			host: "drill",
			pid: process.pid,
			...(this.options.overlayCapBytes === undefined ? {} : { overlayCapBytes: this.options.overlayCapBytes }),
			...(this.options.timing === undefined ? {} : { timing: this.options.timing }),
		});
		if (this.opened) this.counters.reopens++;
		this.opened = true;
		return storage;
	}

	start(): void {
		this.loop = this.run();
	}

	private async run(): Promise<void> {
		const text = "x".repeat(this.options.payloadBytes ?? 256);
		let step = 0;
		while (!this.stopped) {
			if (this.storage === undefined) {
				if (this.paused) {
					await sleep(100);
					continue;
				}
				try {
					this.storage = await this.open();
					if (this.storage.tail <= this.storage.epoch + 1 && (await this.storage.conversation(1 as never, ctx)) === undefined) {
						await this.commit([{ type: "conversation", value: { id: 1 } as never }]);
					}
				} catch (error) {
					this.counters.openFailures++;
					this.note(error);
					await this.drop();
					await sleep(500);
				}
				continue;
			}
			try {
				const s = this.storage;
				const id = await s.mintId<EntryId>();
				// Mixed JSON: nested values, unicode, exponent numbers (P1 keeps the text as sent).
				await this.commit([
					{
						type: "entry",
						value: { id, conversationId: 1, kind: "drill", data: { step: step++, owner: this.stream, text, nested: { a: [1, 2.5, "é"], b: null } } } as never,
					},
				]);
				const back = await s.entry(id, ctx);
				if (back === undefined) throw new Error(`entry ${id} not visible after its commit`);
				this.counters.remoteReads = s.localStore?.metrics.remoteReads ?? 0;
			} catch (error) {
				this.note(error);
				if (error instanceof StorageRejected) this.counters.faulted++;
				else this.counters.poison++;
				await this.drop();
			}
			await sleep(this.options.commitIntervalMs ?? 50);
		}
	}

	private async commit(writes: StorageWrite[]): Promise<void> {
		const s = this.storage;
		if (s === undefined) throw new Error("not open");
		const seq = Number(await s.commit(writes, ctx));
		const body = this.sent.get(seq);
		if (body === undefined) throw new Error(`commit at ${seq} has no captured body`);
		this.acked.set(seq, body);
		this.counters.commits++;
	}

	/** Drops a poisoned or failed storage (best-effort close). */
	private async drop(): Promise<void> {
		const s = this.storage;
		this.storage = undefined;
		if (s !== undefined) await s.close(ctx).catch(() => undefined);
	}

	/** Forgets acknowledged commits at or above `tail` (lost by a restore to an older backup). */
	rewind(tail: number): void {
		for (const seq of [...this.acked.keys()]) if (seq >= tail) this.acked.delete(seq);
	}

	async stop(): Promise<void> {
		this.stopped = true;
		await this.loop;
		await this.drop();
	}

	/** Every acknowledged commit, read back from the log at `baseUrl`, must equal the bytes sent. */
	async verify(baseUrl: string): Promise<string[]> {
		const { log } = httpTransports({ baseUrl, stream: this.stream });
		const problems: string[] = [];
		const stored = new Map<number, Uint8Array>();
		let at = 0;
		for (;;) {
			const page = await log.readRecords(at, { maxRecords: 500, leader: true });
			if (page.status !== 200) {
				problems.push(`${this.stream}: read at ${at} answered ${page.status} ${page.message ?? ""}`);
				return problems;
			}
			// Page until a read at the tail returns no records.
			if (page.records.length === 0) break;
			const start = Number(page.headers["stream-record-start"] ?? at);
			page.records.forEach((record, i) => stored.set(start + i, record));
			at = start + page.records.length;
		}
		for (const [seq, body] of this.acked) {
			const got = stored.get(seq);
			if (got === undefined) problems.push(`${this.stream}: acknowledged record ${seq} is missing`);
			else if (Buffer.compare(Buffer.from(got), Buffer.from(body)) !== 0) problems.push(`${this.stream}: record ${seq} differs from the acknowledged bytes`);
		}
		return problems;
	}
}

/** N owners on fresh streams in one bucket. */
export class OwnerFleet {
	readonly owners: FleetOwner[];
	readonly options: FleetOptions;

	constructor(options: FleetOptions) {
		this.options = options;
		const run = `${process.pid}-${Date.now().toString(36)}`;
		this.owners = Array.from({ length: options.owners }, (_, i) => new FleetOwner(options, `${options.bucket}/${options.prefix ?? "drill"}-${run}-${i}`));
	}

	start(): void {
		for (const owner of this.owners) owner.start();
	}

	async stop(): Promise<void> {
		await Promise.all(this.owners.map((owner) => owner.stop()));
	}

	/** Owners without an open storage stay closed until `resume()`. */
	pause(): void {
		for (const owner of this.owners) owner.paused = true;
	}

	resume(): void {
		for (const owner of this.owners) owner.paused = false;
	}

	totals(): FleetCounters {
		const t: FleetCounters = { commits: 0, poison: 0, faulted: 0, openFailures: 0, reopens: 0, remoteReads: 0 };
		for (const owner of this.owners) {
			for (const k of Object.keys(t) as (keyof FleetCounters)[]) t[k] += owner.counters[k];
		}
		return t;
	}

	/** Recent errors of every owner, for failure messages. */
	errors(): string {
		return this.owners
			.filter((owner) => owner.errors.length > 0)
			.map((owner) => `${owner.stream}:\n  ${owner.errors.slice(-5).join("\n  ")}`)
			.join("\n");
	}

	/** Every owner open, and its overlay at most `maxRecords` records (E caught up). */
	caughtUp(maxRecords: number): boolean {
		return this.owners.every((owner) => owner.storage !== undefined && owner.overlayRecords <= maxRecords);
	}

	maxOverlayBytes(): number {
		return Math.max(0, ...this.owners.map((owner) => owner.overlayBytes));
	}

	maxOverlayRecords(): number {
		return Math.max(0, ...this.owners.map((owner) => owner.overlayRecords));
	}

	async verify(baseUrl: string): Promise<string[]> {
		return (await Promise.all(this.owners.map((owner) => owner.verify(baseUrl)))).flat();
	}
}

/** Counter deltas between two `totals()` snapshots. */
export function delta(after: FleetCounters, before: FleetCounters): FleetCounters {
	const out = { ...after };
	for (const k of Object.keys(out) as (keyof FleetCounters)[]) out[k] = after[k] - before[k];
	return out;
}
