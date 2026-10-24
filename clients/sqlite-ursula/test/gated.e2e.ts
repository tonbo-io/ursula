// `gate` against the e2e server (test/stack.ts): a unit of work's outcome, and each chunk of a
// returned Response's body, pass on only after a synced() call made after them resolved, on every
// database given. Errors wait too, except the package's own; a reset or the signal drops the
// outcome; a stream pays one round trip per batch, not per chunk.
import { afterEach, describe, expect, it } from "vitest";
import { gate, loadUrsulaVfs, open, type UrsulaDatabase, UrsulaResetError, UrsulaWriteRefusedError } from "../src/index.ts";
import { Child, ext, fault, frames, freshDir, killChildren, rejection, stream } from "./kit.ts";

loadUrsulaVfs(ext());
afterEach(killChildren);

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
const text = new TextEncoder();

/** `promise`, and whether it has settled yet. */
function watch<T>(promise: Promise<T>): { promise: Promise<T>; settled: () => boolean } {
	let settled = false;
	const done = promise.finally(() => {
		settled = true;
	});
	// Observed here, awaited by the test.
	done.catch(() => {});
	return { promise: done, settled: () => settled };
}

/** A database on a stream of its own with an empty table `t(x)`, synced. */
async function table(label: string, options: { maxPendingBytes?: number } = {}): Promise<{ db: UrsulaDatabase; path: string; url: string }> {
	const s = stream(label);
	const db = await open(s.url, { dir: freshDir(), ...options });
	db.exec("CREATE TABLE t(x)");
	await db.synced();
	return { db, path: s.path, url: s.url };
}

/** Another process opens the stream at `url` (test/core-child.mjs), which takes it over. */
async function takeOver(url: string): Promise<void> {
	const taker = new Child(url, freshDir());
	await taker.opened();
}

/** Every chunk of `body`, as text, and the error it ended with, if any. */
async function drain(body: ReadableStream<Uint8Array>, each: (chunk: string) => void = () => {}): Promise<{ chunks: string[]; error: unknown }> {
	const reader = body.getReader();
	const chunks: string[] = [];
	const decoder = new TextDecoder();
	try {
		for (;;) {
			const { done, value } = await reader.read();
			if (done) return { chunks, error: undefined };
			const chunk = decoder.decode(value);
			chunks.push(chunk);
			each(chunk);
		}
	} catch (error) {
		return { chunks, error };
	}
}

describe("gate", () => {
	it("G1: settles only once the stream acknowledged the commit work made, where awaiting work alone does not", async () => {
		const { db } = await table("gate-commit");
		await fault("delay 1000", 1, "POST");
		const started = Date.now();
		let n = 0;
		const work = (): string => {
			n += 1;
			db.prepare("INSERT INTO t VALUES (?)").run(n);
			return `saved ${n}`;
		};
		// Negative control: the commit is local, so work alone settles at once, unacknowledged.
		expect(await (async () => work())()).toBe("saved 1");
		expect(Date.now() - started).toBeLessThan(300);
		expect(db.status().commits.pending).toBeGreaterThan(0);
		const gated = watch(gate(db, work));
		await sleep(300);
		expect(gated.settled()).toBe(false);
		expect(await gated.promise).toBe("saved 2");
		expect(Date.now() - started).toBeGreaterThanOrEqual(900);
		const { commits } = db.status();
		expect(commits.acknowledged).toBeGreaterThanOrEqual(commits.logged);
		db.close();
	});

	it("G2: holds a read-only result that read another call's pending commit until that commit is acknowledged", async () => {
		const { db } = await table("gate-read");
		await fault("delay 1000", 1, "POST");
		const started = Date.now();
		db.exec("INSERT INTO t VALUES (1)");
		const target = db.status().commits.logged;
		const gated = watch(gate(db, () => db.prepare("SELECT count(*) AS n FROM t").get()));
		await sleep(300);
		expect(gated.settled()).toBe(false);
		expect(await gated.promise).toEqual({ n: 1 });
		expect(Date.now() - started).toBeGreaterThanOrEqual(900);
		expect(db.status().commits.acknowledged).toBeGreaterThanOrEqual(target);
		db.close();
	});

	it("G3: holds work's own error as it holds a result, and rethrows a backlog_full refusal at once, also when a library wrapped it", async () => {
		const { db } = await table("gate-error");
		await fault("delay 1000", 1, "POST");
		const started = Date.now();
		const own = new Error("the application's own error");
		const failed = watch(
			gate(db, () => {
				db.exec("INSERT INTO t VALUES (1)");
				throw own;
			}),
		);
		await sleep(300);
		expect(failed.settled()).toBe(false);
		expect(await rejection(failed.promise)).toBe(own);
		expect(Date.now() - started).toBeGreaterThanOrEqual(900);
		db.close();

		const bounded = await table("gate-refused", { maxPendingBytes: 1 });
		// The next append's answer comes late: its commit stays pending, and holds the backlog full.
		await fault("delay 1500", 1, "POST");
		bounded.db.exec("INSERT INTO t VALUES (1)");
		const refusedAt = Date.now();
		const refused = await rejection(gate(bounded.db, () => bounded.db.exec("INSERT INTO t VALUES (2)")));
		expect(Date.now() - refusedAt).toBeLessThan(300);
		expect(refused).toBeInstanceOf(UrsulaWriteRefusedError);
		expect(refused).toMatchObject({ kind: "backlog_full", action: "retry" });
		// Wrapped as drizzle wraps a driver's error (DrizzleQueryError's cause), and in an AggregateError
		// (a failed ROLLBACK after it): rethrown at once, as work threw it.
		const wraps: ((refusal: unknown) => Error)[] = [
			(refusal) => new Error("Failed query: INSERT INTO t VALUES (3)", { cause: refusal }),
			(refusal) => new AggregateError([new Error("cannot rollback - no transaction is active"), refusal], "COMMIT failed"),
		];
		for (const wrap of wraps) {
			let wrapper: Error | undefined;
			const wrappedAt = Date.now();
			const wrapped = await rejection(
				gate(bounded.db, () => {
					try {
						bounded.db.exec("INSERT INTO t VALUES (3)");
					} catch (refusal) {
						wrapper = wrap(refusal);
						throw wrapper;
					}
				}),
			);
			expect(Date.now() - wrappedAt).toBeLessThan(300);
			expect(wrapper).toBeDefined();
			expect(wrapped).toBe(wrapper);
		}
		await bounded.db.synced();
		expect(bounded.db.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		bounded.db.close();
	});

	it("G4: rejects with the reset when another process took the stream over before the release, never with the result", async () => {
		const { db, url } = await table("gate-takeover");
		let heard = 0;
		const reset = new Promise<UrsulaResetError>((resolve) =>
			db.on("reset", (error) => {
				heard += 1;
				resolve(error);
			}),
		);
		const error = await rejection(
			gate(db, async () => {
				db.exec("INSERT INTO t VALUES (1)");
				// The takeover lands after the commit and before the gate's synced() call.
				await takeOver(url);
				return "result";
			}),
		);
		expect(error).toBeInstanceOf(UrsulaResetError);
		expect(error).toMatchObject({ kind: "fenced", action: "stop" });
		expect(await reset).toMatchObject({ kind: "fenced" });
		await sleep(100);
		expect(heard).toBe(1);
		// A later gate on the reset database fails the same way, and work's outcome never passes.
		expect(await rejection(gate(db, () => "later"))).toMatchObject({ kind: "fenced" });
		db.close();
	});

	it("G5: rejects with its signal's reason, cancelling a returned body, while the commits keep replicating", async () => {
		const { db, path } = await table("gate-signal");
		await fault("delay 1000", 1, "POST");
		const error = await rejection(gate(db, () => db.exec("INSERT INTO t VALUES (1)"), { signal: AbortSignal.timeout(100) }));
		expect(error).toBeInstanceOf(DOMException);
		expect((error as DOMException).name).toBe("TimeoutError");

		await fault("delay 1000", 1, "POST");
		let cancelled: unknown;
		const body = new ReadableStream<Uint8Array>({
			pull: (controller) => controller.enqueue(text.encode("never")),
			cancel: (reason) => {
				cancelled = reason;
			},
		});
		const reason = new Error("gave up");
		const controller = new AbortController();
		const withBody = gate(
			db,
			() => {
				db.exec("INSERT INTO t VALUES (2)");
				return new Response(body);
			},
			{ signal: controller.signal },
		);
		await sleep(100);
		controller.abort(reason);
		expect(await rejection(withBody)).toBe(reason);
		expect(cancelled).toBe(reason);
		// A signal that fired before the call: work never runs.
		let ran = false;
		const before = new Error("before");
		expect(
			await rejection(
				gate(
					db,
					() => {
						ran = true;
					},
					{ signal: AbortSignal.abort(before) },
				),
			),
		).toBe(before);
		expect(ran).toBe(false);

		await db.synced();
		expect(await frames(path)).toEqual({ claims: 1, commits: 3 });
		db.close();
	});

	it("G6: passes a returned Response's head, and each chunk, on only after the commit before it is acknowledged, in order", async () => {
		const { db } = await table("gate-body");
		const chunks = 8;
		// The head's append is answered late. Only the first chunk's commit, which the source's high-water
		// mark pulls while `work` builds the Response, queues behind it; later chunks are not delayed, and an
		// ungated chunk would reach the reader within microseconds of its commit.
		await fault("delay 500", 1, "POST");
		/** The position of the commit made before chunk i (the head's: `positions[0]`). */
		const positions: number[] = [];
		const commit = (i: number): void => {
			db.prepare("INSERT INTO t VALUES (?)").run(i);
			positions[i] = db.status().commits.logged;
		};
		let i = 0;
		const source = new ReadableStream<Uint8Array>({
			pull(controller) {
				i += 1;
				if (i > chunks) {
					controller.close();
					return;
				}
				commit(i);
				controller.enqueue(text.encode(`${i};`));
			},
		});
		const response = await gate(db, () => {
			commit(0);
			return new Response(source, { status: 201, statusText: "Created", headers: { "content-type": "text/plain", "x-gated": "yes" } });
		});
		expect(db.status().commits.acknowledged).toBeGreaterThanOrEqual(positions[0] as number);
		expect(response).toBeInstanceOf(Response);
		expect(response.status).toBe(201);
		expect(response.statusText).toBe("Created");
		expect(response.headers.get("content-type")).toBe("text/plain");
		expect(response.headers.get("x-gated")).toBe("yes");
		const late: string[] = [];
		const { chunks: got, error } = await drain(response.body as ReadableStream<Uint8Array>, (chunk) => {
			const n = Number(chunk.slice(0, -1));
			if (db.status().commits.acknowledged < (positions[n] as number)) late.push(chunk);
		});
		expect(error).toBeUndefined();
		expect(late).toEqual([]);
		expect(got).toEqual(Array.from({ length: chunks }, (_, k) => `${k + 1};`));
		db.close();
	});

	it("G7: gates 10,000 chunks with one synced() call per batch, not per chunk", async () => {
		const { db } = await table("gate-batch");
		let calls = 0;
		const synced = db.synced.bind(db);
		db.synced = (options) => {
			calls += 1;
			return synced(options);
		};
		const chunks = 10_000;
		let i = 0;
		const source = new ReadableStream<Uint8Array>({
			pull(controller) {
				if (i === chunks) {
					controller.close();
					return;
				}
				i += 1;
				controller.enqueue(text.encode(`${String(i).padStart(7, "0")};`));
			},
		});
		const started = Date.now();
		const response = await gate(db, () => {
			db.exec("INSERT INTO t VALUES (1)");
			return new Response(source);
		});
		const { chunks: got, error } = await drain(response.body as ReadableStream<Uint8Array>);
		expect(error).toBeUndefined();
		expect(got).toHaveLength(chunks);
		expect(got[chunks - 1]).toBe(`${String(chunks).padStart(7, "0")};`);
		expect(Date.now() - started).toBeLessThan(30_000);
		// 80 KB through a 64 KiB read-ahead: a few batches, where a call per chunk makes 10,001.
		expect(calls).toBeLessThan(100);
		db.close();
	});

	it("G8: errors a body with the reset when another process takes the stream over in the middle of it", async () => {
		const { db, url } = await table("gate-body-reset");
		const first = Promise.withResolvers<void>();
		let pulls = 0;
		const source = new ReadableStream<Uint8Array>({
			async pull(controller) {
				pulls += 1;
				if (pulls === 1) {
					db.exec("INSERT INTO t VALUES (1)");
					controller.enqueue(text.encode("before"));
					return;
				}
				if (pulls === 2) {
					// Once the reader has the first chunk, another process takes the stream over.
					await first.promise;
					await takeOver(url);
					controller.enqueue(text.encode("after"));
					return;
				}
				controller.close();
			},
		});
		const response = await gate(db, () => new Response(source));
		const { chunks, error } = await drain(response.body as ReadableStream<Uint8Array>, () => first.resolve());
		expect(chunks).toEqual(["before"]);
		expect(error).toBeInstanceOf(UrsulaResetError);
		expect(error).toMatchObject({ kind: "fenced" });
		db.close();
	});

	it("G9: with two databases, settles only once both acknowledged", async () => {
		const idle = await table("gate-idle");
		const busy = await table("gate-busy");
		expect(await rejection(gate([], () => "nothing"))).toBeInstanceOf(TypeError);
		await fault("delay 1000", 1, "POST");
		const started = Date.now();
		busy.db.exec("INSERT INTO t VALUES (1)");
		const both = watch(gate([idle.db, busy.db], () => "both"));
		// The idle database alone releases at once.
		expect(await gate(idle.db, () => "idle")).toBe("idle");
		expect(Date.now() - started).toBeLessThan(300);
		expect(both.settled()).toBe(false);
		expect(await both.promise).toBe("both");
		expect(Date.now() - started).toBeGreaterThanOrEqual(900);
		const { commits } = busy.db.status();
		expect(commits.acknowledged).toBeGreaterThanOrEqual(commits.logged);
		idle.db.close();
		busy.db.close();
	});

	it("G10: holds a body's end, and its error, until the commit before them is acknowledged", async () => {
		const { db } = await table("gate-body-end");
		const failure = new Error("the body's own error");
		for (const ending of ["end", "error"] as const) {
			const go = Promise.withResolvers<void>();
			let position = 0;
			let committedAt = 0;
			const source = new ReadableStream<Uint8Array>({
				async pull(controller) {
					await go.promise;
					db.exec("INSERT INTO t VALUES (1)");
					position = db.status().commits.logged;
					committedAt = Date.now();
					if (ending === "end") controller.close();
					else controller.error(failure);
				},
			});
			const response = await gate(db, () => new Response(source));
			// The commit before the end or the error is answered late.
			await fault("delay 1000", 1, "POST");
			go.resolve();
			const { chunks, error } = await drain(response.body as ReadableStream<Uint8Array>);
			expect(Date.now() - committedAt).toBeGreaterThanOrEqual(900);
			expect(db.status().commits.acknowledged).toBeGreaterThanOrEqual(position);
			expect(chunks).toEqual([]);
			expect(error).toBe(ending === "end" ? undefined : failure);
		}
		db.close();
	});

	it("G11: a body cancelled while its synced() call is in flight cancels the source with the reason and leaves no call pending", async () => {
		const { db } = await table("gate-body-cancel");
		/** The synced() calls not settled yet. */
		let inFlight = 0;
		const synced = db.synced.bind(db);
		db.synced = (options) => {
			inFlight += 1;
			return synced(options).finally(() => {
				inFlight -= 1;
			});
		};
		const go = Promise.withResolvers<void>();
		const pulled = Promise.withResolvers<void>();
		let cancelled: unknown;
		let pulls = 0;
		const source = new ReadableStream<Uint8Array>({
			async pull(controller) {
				pulls += 1;
				// One chunk, then a source that has nothing more yet.
				if (pulls > 1) return new Promise<void>(() => {});
				await go.promise;
				db.exec("INSERT INTO t VALUES (1)");
				controller.enqueue(text.encode("held"));
				pulled.resolve();
			},
			cancel(reason) {
				cancelled = reason;
			},
		});
		const response = await gate(db, () => new Response(source));
		expect(inFlight).toBe(0);
		// The chunk's commit is answered late, so its synced() call is in flight when the body is cancelled.
		await fault("delay 1000", 1, "POST");
		go.resolve();
		await pulled.promise;
		await sleep(100);
		expect(inFlight).toBe(1);
		const cancelledAt = Date.now();
		const reason = new Error("the client went away");
		await (response.body as ReadableStream<Uint8Array>).cancel(reason);
		expect(cancelled).toBe(reason);
		await sleep(100);
		expect(inFlight).toBe(0);
		expect(Date.now() - cancelledAt).toBeLessThan(500);
		await db.synced();
		db.close();
	});

	it("G12: errors a body with a TypeError when its source enqueues a chunk that is not bytes", async () => {
		const { db } = await table("gate-body-type");
		const source = new ReadableStream<unknown>({
			start(controller) {
				controller.enqueue("text");
				controller.close();
			},
		});
		const response = await gate(db, () => new Response(source as ReadableStream<Uint8Array>));
		const { chunks, error } = await drain(response.body as ReadableStream<Uint8Array>);
		expect(chunks).toEqual([]);
		expect(error).toBeInstanceOf(TypeError);
		db.close();
	});
});
