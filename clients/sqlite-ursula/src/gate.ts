// The output gate as a helper (design: Gates): `gate` runs a unit of work and hands its outcome on
// only once `synced()`, called after the work settled, resolved on every database given. A
// returned WHATWG `Response` comes back with its body gated chunk by chunk. `synced()` is the gate:
// this module only decides when to call it and what to hold back, and calls nothing else of the
// package.
import type { UrsulaDatabase } from "./database.ts";
import { findWrapped, UrsulaError } from "./errors.ts";

/** {@link gate}'s options. */
export interface GateOptions {
	/**
	 * Stops waiting for the outcome when it fires, as `synced()`'s signal does: the gate rejects with
	 * `signal.reason`, and the commits keep replicating. It does not bound a returned body once the
	 * gate resolved: cancel the body to stop it.
	 */
	readonly signal?: AbortSignal | undefined;
}

/** How many bytes of a gated body are read ahead of the reader, held or released. */
const READ_AHEAD_BYTES = 64 * 1024;

/**
 * Runs `work` and settles with its outcome only once a `synced()` call made after `work` settled
 * resolved on every database in `db` (in parallel), so nothing outside the process acts on a
 * result, or an error, that rests on an unacknowledged commit or on ownership this process is about
 * to lose. A result that only read waits too: it may have read another call's pending commit.
 *
 * - An {@link UrsulaError} thrown by `work` is rethrown at once, also when a library wrapped it (as
 *   a `cause`, or in an AggregateError), as `work` threw it: it carries no application state. A
 *   refused write committed nothing, and a reset is what the wait would report. Every other error
 *   waits as a result does.
 * - A reset during the wait rejects with the `UrsulaResetError`, and the outcome is dropped.
 * - When `work` returns a global `Response` with a body, the gate resolves with a `Response` of the
 *   same status and headers, whose body passes each chunk, and its end or error, on only once a
 *   `synced()` call made after the chunk was read resolved. A call covers every chunk read while
 *   the previous one was in flight, so a stream pays one round trip per batch. It reads ahead at
 *   most 64 KiB plus one chunk. A reset errors the body, and cancelling the body ends its waits.
 * - `signal` bounds the wait for the outcome only. When it fires first (or fired before the call,
 *   when `work` does not run) the gate rejects with `signal.reason`, and a returned body is
 *   cancelled.
 *
 * Effects inside `work` before it returns, such as an outbound call or a manual acknowledgement,
 * are not gated: `await db.synced()` before them.
 */
export async function gate<T>(db: UrsulaDatabase | readonly UrsulaDatabase[], work: () => T | Promise<T>, options: GateOptions = {}): Promise<Awaited<T>> {
	const dbs = (Array.isArray(db) ? db : [db]) as readonly UrsulaDatabase[];
	if (dbs.length === 0) throw new TypeError("gate needs at least one database");
	const { signal } = options;
	signal?.throwIfAborted();
	let outcome: { ok: true; value: Awaited<T> } | { ok: false; error: unknown };
	try {
		outcome = { ok: true, value: await work() };
	} catch (error) {
		if (findWrapped(error, (next) => (next instanceof UrsulaError ? next : undefined)) !== undefined) throw error;
		outcome = { ok: false, error };
	}
	const release = new Release(dbs);
	const abort = (): void => release.stop(signal?.reason);
	try {
		signal?.throwIfAborted();
		signal?.addEventListener("abort", abort, { once: true });
		await release.hold();
	} catch (error) {
		const dropped: unknown = outcome.ok ? outcome.value : undefined;
		if (dropped instanceof Response) dropped.body?.cancel(error).catch(() => {});
		throw error;
	} finally {
		signal?.removeEventListener("abort", abort);
	}
	if (!outcome.ok) throw outcome.error;
	const value: unknown = outcome.value;
	if (value instanceof Response && value.body !== null) {
		const body = gatedBody(value.body, release);
		return new Response(body, { status: value.status, statusText: value.statusText, headers: value.headers }) as Awaited<T>;
	}
	return outcome.value;
}

/**
 * The release queue of one gate: outputs wait in the order they were produced, and each is released
 * by the first `synced()` call on every database that started after it was held. At most one call
 * is in flight; outputs held meanwhile wait for the next. Once a wait failed or the gate stopped,
 * nothing more is released.
 */
class Release {
	readonly #dbs: readonly UrsulaDatabase[];
	/** Ends the call in flight: a stop, or a failure on one database that ends the others' waits. */
	readonly #stop = new AbortController();
	/** Outputs held since the call in flight started, which it does not cover. */
	#next: PromiseWithResolvers<void>[] = [];
	#busy = false;
	#failure: { error: unknown } | undefined;

	constructor(dbs: readonly UrsulaDatabase[]) {
		this.#dbs = dbs;
	}

	/** Resolves once a `synced()` call made after this call resolved on every database. */
	hold(): Promise<void> {
		if (this.#failure !== undefined) return Promise.reject(this.#failure.error);
		const held = Promise.withResolvers<void>();
		this.#next.push(held);
		if (!this.#busy) this.#call();
		return held.promise;
	}

	/** Rejects what is held, and every later hold, with `reason`, and ends the call in flight. */
	stop(reason: unknown): void {
		this.#fail(reason, []);
	}

	#call(): void {
		// The cutoff: every output in `batch` was held before the calls below start.
		const batch = this.#next;
		this.#next = [];
		this.#busy = true;
		const { signal } = this.#stop;
		let waits: Promise<unknown>;
		try {
			waits = Promise.all(this.#dbs.map((db) => db.synced({ signal })));
		} catch (error) {
			waits = Promise.reject(error);
		}
		waits.then(
			() => {
				this.#busy = false;
				if (this.#failure !== undefined) {
					for (const held of batch) held.reject(this.#failure.error);
					return;
				}
				for (const held of batch) held.resolve();
				if (this.#next.length > 0) this.#call();
			},
			(error: unknown) => {
				this.#busy = false;
				this.#fail(error, batch);
			},
		);
	}

	#fail(error: unknown, batch: readonly PromiseWithResolvers<void>[]): void {
		this.#failure ??= { error };
		const { error: first } = this.#failure;
		for (const held of [...batch, ...this.#next]) held.reject(first);
		this.#next = [];
		this.#stop.abort(first);
	}
}

/**
 * `source` read ahead into `release`: each chunk, and the end or error, passes on only once its
 * hold resolved, in order. A failed hold errors the body and cancels `source`.
 */
function gatedBody(source: ReadableStream<Uint8Array>, release: Release): ReadableStream<Uint8Array> {
	const reader = source.getReader();
	/** Bytes read from `source` and not yet released. */
	let held = 0;
	/** The gated body closed, errored or was cancelled: nothing more passes on. */
	let ended = false;
	let wake: (() => void) | undefined;
	const nudge = (): void => {
		const woken = wake;
		wake = undefined;
		woken?.();
	};
	return new ReadableStream<Uint8Array>(
		{
			start(controller) {
				const fail = (error: unknown): void => {
					if (ended) return;
					ended = true;
					controller.error(error);
					reader.cancel(error).catch(() => {});
					release.stop(error);
					nudge();
				};
				const read = async (): Promise<void> => {
					for (;;) {
						// Held and released-but-unread bytes stay within the read-ahead.
						while (!ended && held >= (controller.desiredSize ?? 0)) await new Promise<void>((resolve) => (wake = resolve));
						if (ended) return;
						let next: Awaited<ReturnType<typeof reader.read>>;
						try {
							next = await reader.read();
						} catch (error) {
							// The body's own error: an output, held as its chunks are.
							release.hold().then(() => fail(error), fail);
							return;
						}
						if (ended) return;
						if (next.done) {
							release.hold().then(() => {
								if (ended) return;
								ended = true;
								controller.close();
							}, fail);
							return;
						}
						const chunk: unknown = next.value;
						if (!(chunk instanceof Uint8Array)) {
							// Not a byte chunk: an error of the body, held as its own errors are.
							const error = new TypeError("gate: a Response body chunk is not a Uint8Array");
							release.hold().then(() => fail(error), fail);
							return;
						}
						held += chunk.byteLength;
						release.hold().then(() => {
							held -= chunk.byteLength;
							if (ended) return;
							controller.enqueue(chunk);
							nudge();
						}, fail);
					}
				};
				void read();
			},
			pull() {
				nudge();
			},
			cancel(reason) {
				ended = true;
				release.stop(reason);
				nudge();
				return reader.cancel(reason);
			},
		},
		{ highWaterMark: READ_AHEAD_BYTES, size: (chunk) => chunk.byteLength },
	);
}
