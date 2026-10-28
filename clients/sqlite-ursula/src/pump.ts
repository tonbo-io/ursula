// The pump (design: Pump): this thread's view of the extension's events. Calls into the extension
// on this thread never wait (timeout 0): `ursula_open`, `tonbo_resolve` and `ursula_synced` either
// answer at once or hand back a request or a ticket, which an event later resolves. One worker per
// JavaScript thread (pump-worker.ts) waits for those events and posts them here, where they
// complete `open()` and the `/tonbo` entry's resolve, resolve or reject `synced()` and fire the
// `reset` listeners.
//
// `synced()` tickets resolve through their binding's watermark (`synced {path, through}`): every
// ticket of the path up to `through` resolved. The extension keeps only the latest watermark of a
// binding in its queue, so any number of waiters costs one event. A ticket fails alone
// (`sync_failed`) only when it was abandoned or its binding was not bound, and all of a binding's
// tickets fail with its `reset` event.
//
// The token provider (token.ts) follows the events through `watch`: a 401 and a retired binding. A
// failed worker is replaced while a ticket is pending or a watcher wants the events.
//
// The worker is unref'd while nothing waits, so an idle process exits as if it were not there, and
// ref'd while an `open()` or a `synced()` is pending, so `await db.synced()` never lets the process
// exit with the wait unresolved.
import { Worker } from "node:worker_threads";
import { call, loadedLibrary } from "./control.ts";
import { type Described, errorOf, type ResetKind, UrsulaResetError, type UrsulaErrorAction, UrsulaSetupError } from "./errors.ts";

/** A database whose `reset` listeners the pump fires: `UrsulaDatabase`. */
export interface ResetTarget {
	/** The path `ursula_open` bound. */
	readonly boundPath: string;
	/** The database was reset: called once per reset event of its path, after its pending `synced()` calls rejected. */
	resetBy(error: UrsulaResetError): void;
	/** Its reset, from `ursula_status`, when it is one; `undefined` otherwise or when the status cannot be read. */
	currentReset(): UrsulaResetError | undefined;
}

/** One event of `ursula_events` (clients/sqlite-vfs/src/status.rs `events_json`). */
export type PumpEvent =
	| { event: "opened" | "resolved"; request: number; ok: boolean }
	| { event: "synced"; path: string | null; through: number }
	| ({ event: "sync_failed"; ticket: number } & Described)
	| ({ event: "reset"; path: string | null } & Described)
	| { event: "gap"; dropped: number }
	| { event: "condition"; path: string | null; condition: string | null }
	| { event: "acked" | "retired"; path: string | null };

interface Ticket {
	readonly path: string;
	resolve(): void;
	reject(error: unknown): void;
}

/** The error of the reset the extension `named` (a `reset` event, `status().reset`), with its message, for `synced()` and the listeners. */
export const resetError = (named: Described): UrsulaResetError =>
	new UrsulaResetError(named.message, named.kind as ResetKind, undefined, named.action as UrsulaErrorAction);

/**
 * The open databases of this thread by bound path, whose listeners the pump fires. Held weakly: a
 * database the application dropped is collected, and its binding retires.
 */
const targets = new Map<string, Set<WeakRef<ResetTarget>>>();
/** The pending `synced()` tickets of this thread, by ticket: kept across pumps. */
const tickets = new Map<number, Ticket>();
/** How many `open()` and `synced()` calls wait: the worker is ref'd while any does. */
let waiting = 0;
let current: Pump | undefined;
let exiting = false;
/** Pumps that failed in a row, before any delivered events. */
let failures = 0;
/** After this many failures in a row, the pump stays down and its waits fail. */
const MAX_FAILURES = 3;
/** The stop flags of this thread's pump workers, set when the thread exits. */
const stops = new Set<Int32Array>();
let exitListener = false;
/** What else follows the events, and whether it wants them now: the token provider (token.ts). */
const watchers = new Map<(event: PumpEvent) => void, () => boolean>();

/**
 * Calls `watcher` with every event this thread's pumps deliver, after the pump handled it. While
 * `wanted()` holds, a failed pump is replaced, and the replacement hands the watcher a `gap` for the
 * events between the two.
 */
export function watch(watcher: (event: PumpEvent) => void, wanted: () => boolean): void {
	watchers.set(watcher, wanted);
}

/** Hands `event` to every watcher: a watcher's failure is its own. */
function notify(event: PumpEvent): void {
	for (const watcher of watchers.keys()) {
		try {
			watcher(event);
		} catch {
			// The rest of the batch applies.
		}
	}
}

/** Fires `target`'s listeners on the resets of its path. */
export function track(target: ResetTarget): void {
	let set = targets.get(target.boundPath);
	if (set === undefined) {
		set = new Set();
		targets.set(target.boundPath, set);
	}
	set.add(new WeakRef(target));
}

export function untrack(target: ResetTarget): void {
	const set = targets.get(target.boundPath);
	if (set === undefined) return;
	for (const ref of set) {
		const t = ref.deref();
		if (t === undefined || t === target) set.delete(ref);
	}
	if (set.size === 0) targets.delete(target.boundPath);
}

/** The live databases of `path`, or of every path; collected ones are dropped. */
function live(path?: string): ResetTarget[] {
	const found: ResetTarget[] = [];
	for (const [p, set] of path === undefined ? targets : [[path, targets.get(path) ?? new Set<WeakRef<ResetTarget>>()] as const]) {
		for (const ref of set) {
			const t = ref.deref();
			if (t === undefined) set.delete(ref);
			else found.push(t);
		}
		if (set.size === 0) targets.delete(p);
	}
	return found;
}

function hold(): void {
	waiting += 1;
	if (waiting === 1) current?.ref();
}

function release(): void {
	waiting -= 1;
	if (waiting === 0) current?.unref();
}

/** Asks for every pending ticket again (`ursula_synced`, timeout 0), under a new ticket. */
function reissue(): void {
	const pending = [...tickets.values()];
	tickets.clear();
	for (const t of pending) {
		try {
			const again = call<{ ticket: number }>("SELECT ursula_synced(?, 0)", t.path);
			tickets.set(again.ticket, t);
		} catch (error) {
			t.reject(error);
		}
	}
}

/**
 * Waits for `ticket` of `ursula_synced` on `path`: rejects with the reset error, or with
 * `signal.reason` when `signal` fires. The caller started the pump before it asked for the ticket.
 */
export function waitTicket(path: string, ticket: number, signal: AbortSignal | undefined): Promise<void> {
	return new Promise((resolve, reject) => {
		const abort = (): void => {
			for (const [id, t] of tickets) if (t === entry) tickets.delete(id);
			settle();
			reject(signal?.reason);
		};
		const settle = (): void => {
			signal?.removeEventListener("abort", abort);
			release();
		};
		const entry: Ticket = {
			path,
			resolve: () => {
				settle();
				resolve();
			},
			reject: (error) => {
				settle();
				reject(error);
			},
		};
		hold();
		tickets.set(ticket, entry);
		signal?.addEventListener("abort", abort, { once: true });
	});
}

class Pump {
	readonly #worker: Worker;
	/**
	 * Pending opens and resolves by request: each waiter is woken once by the request's `opened` or
	 * `resolved` event, a gap, or the pump's end.
	 */
	readonly #requests = new Map<number, Set<() => void>>();
	#failed = false;

	constructor(onExit: () => void) {
		// From the cursor at this moment: every request and ticket handed out after it is answered by
		// an event the worker sees, and no event of an earlier binding is taken for a later one's.
		const { cursor } = call<{ cursor: number }>("SELECT ursula_events(0, 0)");
		const here = import.meta.url;
		const file = new URL(here.endsWith(".ts") ? "./pump-worker.ts" : "./pump-worker.js", here);
		const stop = new Int32Array(new SharedArrayBuffer(4));
		this.#worker = new Worker(file, { workerData: { library: loadedLibrary(), cursor, stop }, stdout: false, stderr: false });
		// Node joins every worker before the process exits: the worker leaves its loop once told.
		stops.add(stop);
		if (!exitListener) {
			exitListener = true;
			process.once("exit", () => {
				exiting = true;
				for (const flag of stops) Atomics.store(flag, 0, 1);
			});
		}
		this.#worker.once("exit", () => stops.delete(stop));
		this.#worker.on("message", (message: { events?: PumpEvent[]; error?: string }) => {
			if (message.events !== undefined) {
				failures = 0;
				this.handle(message.events);
			}
			if (message.error !== undefined) this.#fail(onExit);
		});
		this.#worker.on("error", () => this.#fail(onExit));
		this.#worker.on("exit", () => this.#fail(onExit));
		// After the listeners: adding a "message" listener refs the worker's port.
		if (waiting === 0) this.#worker.unref();
	}

	ref(): void {
		this.#worker.ref();
	}

	unref(): void {
		this.#worker.unref();
	}

	/**
	 * Waits until `request` of `ursula_open` or `tonbo_resolve` may have ended: its `opened` or
	 * `resolved` event, a gap in the events, or the pump's end, after which the caller calls again
	 * with the same arguments. Rejects with `signal.reason` when `signal` fires: the caller then
	 * cancels the request.
	 */
	completed(request: number, signal: AbortSignal): Promise<void> {
		return new Promise((resolve, reject) => {
			let set = this.#requests.get(request);
			if (set === undefined) {
				set = new Set();
				this.#requests.set(request, set);
			}
			const waiters = set;
			const done = (): void => {
				signal.removeEventListener("abort", abort);
				release();
			};
			const wake = (): void => {
				done();
				resolve();
			};
			const abort = (): void => {
				waiters.delete(wake);
				if (waiters.size === 0 && this.#requests.get(request) === waiters) this.#requests.delete(request);
				done();
				reject(signal.reason);
			};
			hold();
			waiters.add(wake);
			signal.addEventListener("abort", abort, { once: true });
		});
	}

	/** A batch of events, in order. A gap comes first in its batch: what it may have dropped is asked for again before the rest applies. */
	handle(events: readonly PumpEvent[]): void {
		for (const event of events) {
			switch (event.event) {
				case "opened":
				case "resolved":
					this.#wake(event.request);
					break;
				case "synced":
					for (const [id, t] of [...tickets]) {
						if (t.path === event.path && id <= event.through) {
							tickets.delete(id);
							t.resolve();
						}
					}
					break;
				case "sync_failed": {
					const t = tickets.get(event.ticket);
					tickets.delete(event.ticket);
					t?.reject(errorOf(event, event.message, undefined));
					break;
				}
				case "reset":
					this.#reset(event.path, event);
					break;
				case "gap":
					this.#gap();
					break;
				default:
					break;
			}
			notify(event);
		}
	}

	#wake(request: number): void {
		const waiters = this.#requests.get(request);
		this.#requests.delete(request);
		for (const wake of waiters ?? []) wake();
	}

	/** A reset of `path` (null: a binding the process no longer has): its pending `synced()` calls reject, then its listeners run. */
	#reset(path: string | null, named: Described): void {
		const hit: [ResetTarget, UrsulaResetError][] = [];
		if (path === null) {
			for (const target of live()) {
				const error = target.currentReset();
				if (error !== undefined) hit.push([target, error]);
			}
		} else {
			for (const target of live(path)) hit.push([target, resetError(named)]);
		}
		for (const [id, t] of [...tickets]) {
			if (path === null ? hit.some(([target]) => target.boundPath === t.path) : t.path === path) {
				tickets.delete(id);
				t.reject(resetError(named));
			}
		}
		// After the rejections' handlers ran: the listeners see what a `synced()` caller saw first.
		if (hit.length > 0) {
			setImmediate(() => {
				for (const [target, error] of hit) target.resetBy(error);
			});
		}
	}

	/**
	 * Events fell out of the queue before the worker read them. Every pending ticket is asked for
	 * again under a new ticket, which no watermark of the batch covers, so a ticket whose failure
	 * the gap dropped never passes for resolved. Opens and resolves ask again, and resets are read
	 * from status.
	 */
	#gap(): void {
		for (const request of [...this.#requests.keys()]) this.#wake(request);
		reissue();
		for (const target of live()) {
			const error = target.currentReset();
			if (error !== undefined) setImmediate(() => target.resetBy(error));
		}
	}

	/**
	 * The worker is gone: pending opens and resolves ask again, and pending tickets move to a new
	 * pump, which also starts while a watcher wants the events. After {@link MAX_FAILURES} failures
	 * in a row the tickets fail instead, and so does every later call.
	 */
	#fail(onExit: () => void): void {
		if (this.#failed) return;
		this.#failed = true;
		failures += 1;
		onExit();
		for (const request of [...this.#requests.keys()]) this.#wake(request);
		void this.#worker.terminate();
		setImmediate(() => {
			if (exiting || (tickets.size === 0 && ![...watchers.values()].some((wanted) => wanted()))) return;
			try {
				pump();
			} catch (error) {
				const pending = [...tickets.values()];
				tickets.clear();
				for (const t of pending) t.reject(error);
				return;
			}
			reissue();
			notify({ event: "gap", dropped: 0 });
		});
	}
}

/** This thread's pump, started at the first call that needs it. */
export function pump(): Pump {
	if (current === undefined) {
		if (failures >= MAX_FAILURES) {
			throw new UrsulaSetupError(`internal: the sqlite-ursula pump worker failed ${failures} times in a row: check the extension, then restart the process`, "internal");
		}
		const started: Pump = new Pump(() => {
			if (current === started) current = undefined;
		});
		current = started;
	}
	return current;
}
