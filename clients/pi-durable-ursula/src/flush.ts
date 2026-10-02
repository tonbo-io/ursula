// Background flush loop of the bounded owner (design §7.9, error policy §7.6, fencing §7.8).
//
// One flush-wait at a time, never on the Session line: `GET keyed-state?key=m/owner&
// min_through_record=tail&timeout_ms=60000`. A flush-wait that returns `D_pub` raises the overlay
// floor, `E := max(E, min(D_pub, tail))` (§7.2), and checks `m/owner`: a foreign nonce in a state
// that includes records after our claim poisons with `FencedError`. Every transient outcome (no
// response, 204, 429, 5xx, a lagging node's 400) retries indefinitely with backoff capped at 30 s and
// never poisons; only a foreign owner, a 404, or a 401/403 does.
//
// It fires when the overlay holds ≥ 4 MiB or ≥ 5,000 records, when its oldest record is 10 min old,
// or when a commit is waiting because the overlay reached its hard cap (§7.5).
import { FencedError } from "./errors.ts";
import { K, META } from "./families.ts";
import type { LocalStore } from "./local-store/index.ts";
import type { OwnerClaim } from "./planner.ts";
import { H, intHeader, retryAfterMs } from "./protocol.ts";
import type { KeyedScanOutcome, KeyedStateTransport } from "./transport.ts";
import { TransportError } from "./transport.ts";
import { b64 } from "./tuple.ts";

export interface FlushTiming {
	/** Overlay bytes that trigger a flush-wait (§7.9). */
	readonly flushMaxBytes: number;
	/** Overlay records that trigger a flush-wait (§7.9). */
	readonly flushMaxRecords: number;
	/** Age of the oldest overlay record that triggers a flush-wait (§7.9). */
	readonly flushMaxAgeMs: number;
	/** `timeout_ms` of a flush-wait. */
	readonly flushWaitTimeoutMs: number;
	readonly backoffBaseMs: number;
	/** Backoff cap of flush-wait retries (§7.6: 30 s). */
	readonly flushBackoffMaxMs: number;
}

export interface FlushHost {
	readonly local: LocalStore;
	readonly keyedState: KeyedStateTransport;
	readonly owner: OwnerClaim;
	readonly timing: FlushTiming;
	now(): number;
	/** The in-flight commit's landing (append + apply), or undefined when none is in flight. */
	inflight(): Promise<void> | undefined;
	/** Poison the storage (terminal outcomes only). */
	poison(error: Error): void;
}

export class FlushLoop {
	private readonly host: FlushHost;
	/** Apply time of each overlay record, oldest first (ordinal, ms). */
	private readonly applied: { ordinal: number; at: number }[] = [];
	private stopped = false;
	private urgent = false;
	/** Wakes the loop while it idles (waiting for a trigger). */
	private wakeIdle: (() => void) | undefined;
	/** Wakes the loop while it backs off between flush-wait attempts. */
	private wakeSleep: (() => void) | undefined;
	private timer: ReturnType<typeof setTimeout> | undefined;
	private progressWaiters: (() => void)[] = [];
	private readonly done: Promise<void>;
	/** Metrics (§7.6): flush-waits issued, retried, and floors raised. */
	readonly metrics = { flushWaits: 0, retries: 0, floorsRaised: 0 };

	constructor(host: FlushHost) {
		this.host = host;
		// Records already in the overlay at open (replay, claim) start their age clock now.
		const now = host.now();
		for (let o = host.local.overlayFloor; o < host.local.tail; o++) this.applied.push({ ordinal: o, at: now });
		this.done = this.run();
	}

	/** A record was applied to the overlay: start its age clock; the loop re-evaluates its triggers. */
	noteApplied(ordinal: number): void {
		this.applied.push({ ordinal, at: this.host.now() });
		const w = this.wakeIdle;
		this.wakeIdle = undefined;
		w?.();
	}

	/** Flush now (a commit waits at the overlay cap). Resolves after the next floor raise or stop. */
	urge(): Promise<void> {
		this.urgent = true;
		const p = new Promise<void>((resolve) => this.progressWaiters.push(resolve));
		this.kick();
		return p;
	}

	/** Stop the loop. An in-flight flush-wait is abandoned, not awaited. */
	stop(): void {
		this.stopped = true;
		this.kick();
		this.release();
	}

	/** Resolves when the loop has exited (tests). */
	stopped$(): Promise<void> {
		return this.done;
	}

	private due(): boolean {
		const { local, timing } = this.host;
		if (local.overlayRecords === 0) return false;
		if (this.urgent || local.overlayBytes >= timing.flushMaxBytes || local.overlayRecords >= timing.flushMaxRecords) return true;
		const oldest = this.oldestAt();
		return oldest !== undefined && this.host.now() - oldest >= timing.flushMaxAgeMs;
	}

	private oldestAt(): number | undefined {
		const floor = this.host.local.overlayFloor;
		while (this.applied.length > 0 && (this.applied[0] as { ordinal: number }).ordinal < floor) this.applied.shift();
		return this.applied[0]?.at;
	}

	private kick(): void {
		const idle = this.wakeIdle;
		const sleeping = this.wakeSleep;
		this.wakeIdle = undefined;
		this.wakeSleep = undefined;
		idle?.();
		sleeping?.();
	}

	private release(): void {
		const waiters = this.progressWaiters;
		this.progressWaiters = [];
		for (const w of waiters) w();
	}

	/** Sleep until woken, or until the oldest record reaches the age threshold. */
	private idle(): Promise<void> {
		return new Promise<void>((resolve) => {
			this.wakeIdle = resolve;
			const oldest = this.oldestAt();
			if (oldest !== undefined) {
				const ms = Math.max(1, oldest + this.host.timing.flushMaxAgeMs - this.host.now());
				this.timer = setTimeout(() => this.kick(), ms);
				this.timer.unref?.();
			}
		}).finally(() => {
			if (this.timer !== undefined) clearTimeout(this.timer);
			this.timer = undefined;
		});
	}

	private sleep(ms: number): Promise<void> {
		return new Promise<void>((resolve) => {
			this.wakeSleep = resolve;
			this.timer = setTimeout(() => this.kick(), ms);
			this.timer.unref?.();
		}).finally(() => {
			if (this.timer !== undefined) clearTimeout(this.timer);
			this.timer = undefined;
		});
	}

	private async run(): Promise<void> {
		while (!this.stopped) {
			// A poisoned store is never flushed again.
			if (this.host.local.poisoned !== undefined) break;
			if (!this.due()) {
				await this.idle();
				continue;
			}
			try {
				await this.flushOnce();
			} catch (error) {
				this.host.poison(error instanceof Error ? error : new Error(String(error)));
				this.stopped = true;
			}
		}
		this.release();
	}

	/** One flush-wait, retried until it raises the floor or the loop stops. */
	private async flushOnce(): Promise<void> {
		const { local, keyedState, owner, timing } = this.host;
		let attempt = 0;
		while (!this.stopped) {
			const r = local.tail;
			this.metrics.flushWaits++;
			let outcome: KeyedScanOutcome | undefined;
			try {
				outcome = await keyedState.scan({ key: b64(K.m(META.owner)), minThroughRecord: r, timeoutMs: timing.flushWaitTimeoutMs });
			} catch (error) {
				if (!(error instanceof TransportError)) throw error;
			}
			if (this.stopped) return;
			const s = outcome?.status;
			if (outcome !== undefined && s === 200 && outcome.through !== undefined) {
				let through = outcome.through;
				// A page above the local tail: wait for the in-flight commit; with none, another writer exists.
				while (through > local.tail) {
					const wait = this.host.inflight();
					if (wait === undefined) {
						throw new FencedError(`keyed-state reflects record ${through - 1} above the local tail ${local.tail}: another writer exists`);
					}
					await wait.catch(() => undefined);
					if (this.stopped) return;
				}
				if (through > owner.epoch) {
					const row = outcome.rows[0];
					const current = row === undefined ? undefined : (JSON.parse(row.value) as Partial<OwnerClaim>);
					if (current?.nonce !== owner.nonce) {
						throw new FencedError(`another owner claimed the log (m/owner ${row === undefined ? "absent" : `epoch ${String(current?.epoch)}`} at D = ${through})`);
					}
				}
				through = Math.min(through, local.tail);
				if (through > local.overlayFloor) {
					local.advanceFloor(through);
					this.metrics.floorsRaised++;
				}
				this.urgent = false;
				this.release();
				return;
			}
			if (s === 404) throw new Error(`the log is no longer served: flush-wait answered 404${outcome?.message ? ` (${outcome.message})` : ""}`);
			if (s === 401 || s === 403) throw new Error(`flush-wait was not authorized (${s}); refresh credentials and reopen`);
			// Every other outcome is transient for a flush-wait (§7.6): retry indefinitely.
			attempt++;
			this.metrics.retries++;
			const backoff = Math.min(timing.flushBackoffMaxMs, timing.backoffBaseMs * 2 ** Math.min(attempt, 30));
			const retryAfter = outcome === undefined ? undefined : retryAfterMs(outcome.headers);
			// A 204 already waited `timeout_ms`; re-issue at once unless the server asked for a pause.
			const ms = retryAfter ?? (s === 204 && intHeader(outcome?.headers ?? {}, H.keyedThrough) !== undefined ? 0 : backoff * (0.5 + Math.random() / 2));
			if (ms > 0) await this.sleep(Math.min(ms, timing.flushBackoffMaxMs));
		}
	}
}
