// In-memory fake of the Ursula surface the owner uses: keyed stream create/HEAD, appends with
// `Stream-Record-Match` (P1 normalization, P2 validation, 413 body cap), record-aware reads with P7
// `max_bytes` and long-poll, and the keyed-state resource (P3) with real fold semantics and a
// controllable publish frontier. Every request passes a fault hook first (§11.7 fault proxy).
import { type KeyedOp, KeyedBatchError, fromUtf8, normalizeJsonMessage, parseKeyedBatch, utf8 } from "../keyed-batch.ts";
import { OrderedMap } from "../ordered-map.ts";
import {
	EXT_KEYED_BATCH,
	EXT_KEYED_STATE,
	EXT_RECORD_COORDINATES,
	H,
	type Headers,
	KEYED_CONTENT_TYPE,
	LIMITS,
} from "../protocol.ts";
import { foldRecord, type Row } from "../state-store.ts";
import {
	type HttpOutcome,
	type KeyedRow,
	type KeyedScanOutcome,
	type KeyedScanRequest,
	type KeyedStateTransport,
	type LogTransport,
	type ReadRecordsOptions,
	type ReadRecordsOutcome,
	TransportError,
} from "../transport.ts";
import { b64, keySuccessor, unb64 } from "../tuple.ts";

export type FakeOp = "head" | "create" | "append" | "read" | "scan";

export interface FakeRequest {
	/** Global request counter, starting at 0. */
	readonly index: number;
	readonly op: FakeOp;
	readonly path: string;
	readonly match?: number;
	readonly from?: number;
	readonly body?: Uint8Array;
	readonly scan?: KeyedScanRequest;
	readonly readOptions?: ReadRecordsOptions;
}

export type Fault =
	/** The request never reaches the server: nothing applied, the client sees a connection error. */
	| { readonly type: "drop-request" }
	/** The server applies the request; the response is lost (client sees a timeout). */
	| { readonly type: "drop-response" }
	/** Wait, then process normally. */
	| { readonly type: "delay"; readonly ms: number }
	/** The client times out at once; the server applies the request `ms` later. */
	| { readonly type: "delay-apply"; readonly ms: number }
	/** The request is delivered twice; the client sees the second response. */
	| { readonly type: "duplicate" }
	/** Answer with a synthetic status, optionally after applying the request for real. */
	| {
			readonly type: "respond";
			readonly status: number;
			readonly headers?: Headers;
			readonly apply?: boolean;
			readonly message?: string;
	  };

export type FaultHook = (request: FakeRequest) => Fault | undefined;

export interface FakeUrsulaOptions {
	/** Advertise `keyed-batch-v1` on keyed streams (default true). False models an ungated node. */
	readonly advertiseKeyedBatch?: boolean;
	/** Advertise `keyed-state-v1` and support P7 `max_bytes` (default true). */
	readonly advertiseKeyedState?: boolean;
	/** Keyed-state publish policy: the `D` to publish for a request (default: the tail). */
	readonly publishTarget?: (tail: number, minThroughRecord: number | undefined) => number;
}

interface Waiter {
	readonly resolve: () => void;
}

class FakeStream {
	readonly contentType: string;
	readonly texts: string[] = [];
	readonly parsed: KeyedOp[][] = [];
	readonly keyed: boolean;
	/** The published projection, `state(published)`. */
	readonly projection = new OrderedMap<Row>();
	published = 0;
	appendWaiters: Waiter[] = [];
	publishWaiters: Waiter[] = [];
	deleted = false;
	constructor(contentType: string) {
		this.contentType = contentType;
		this.keyed = contentType === KEYED_CONTENT_TYPE;
	}
	get tail(): number {
		return this.texts.length;
	}
	publishTo(d: number): void {
		const target = Math.min(d, this.tail);
		while (this.published < target) {
			foldRecord(this.projection, this.published, this.parsed[this.published] as KeyedOp[]);
			this.published++;
		}
		const waiters = this.publishWaiters;
		this.publishWaiters = [];
		for (const w of waiters) w.resolve();
	}
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Wait until `ready()` holds or `ms` elapses, waking on the stream's waiter list. */
async function waitFor(push: (w: Waiter) => void, ready: () => boolean, ms: number): Promise<void> {
	const deadline = Date.now() + ms;
	while (!ready()) {
		const left = deadline - Date.now();
		if (left <= 0) return;
		await new Promise<void>((resolve) => {
			const timer = setTimeout(resolve, left);
			push({
				resolve: () => {
					clearTimeout(timer);
					resolve();
				},
			});
		});
	}
}

export class FakeUrsula {
	readonly streams = new Map<string, FakeStream>();
	readonly requests: FakeRequest[] = [];
	fault: FaultHook | undefined;
	private readonly options: FakeUrsulaOptions;
	private counter = 0;
	private readonly pending = new Set<Promise<unknown>>();

	constructor(options: FakeUrsulaOptions = {}) {
		this.options = options;
	}

	// ------------------------------------------------------------ transports

	logTransport(path: string): LogTransport {
		return {
			head: () => this.handle({ op: "head", path }, () => this.head(path)),
			create: () => this.handle({ op: "create", path }, () => this.create(path, KEYED_CONTENT_TYPE)),
			append: (body, match) => this.handle({ op: "append", path, body, match }, () => this.append(path, body, match)),
			readRecords: (from, readOptions = {}) =>
				this.handle({ op: "read", path, from, readOptions }, () => this.read(path, from, readOptions)),
		};
	}

	keyedStateTransport(path: string): KeyedStateTransport {
		return { scan: (scan) => this.handle({ op: "scan", path, scan }, () => this.scan(path, scan)) };
	}

	// ------------------------------------------------------------ test helpers

	/** Create a stream with any content type (for example a non-keyed JSON stream). */
	createStream(path: string, contentType: string): void {
		if (!this.streams.has(path)) this.streams.set(path, new FakeStream(contentType));
	}

	/** Stored message texts of a stream. */
	records(path: string): readonly string[] {
		return this.streams.get(path)?.texts ?? [];
	}

	/** Append as a third party, bypassing faults. Returns the HTTP outcome. */
	appendRaw(path: string, text: string, match?: number): Promise<HttpOutcome> {
		return this.append(path, utf8(text), match);
	}

	/** Publish keyed state up to `d` (default: the tail). */
	publish(path: string, d?: number): void {
		const s = this.streams.get(path);
		if (s !== undefined) s.publishTo(d ?? s.tail);
	}

	/** `state(D)` of the published projection, as base64url rows. */
	projectionRows(path: string): { key: string; record: number; value: string }[] {
		const s = this.streams.get(path);
		if (s === undefined) return [];
		return [...s.projection.entries()].map(([k, r]) => ({ key: b64(k), record: r.record, value: r.value }));
	}

	deleteStream(path: string): void {
		const s = this.streams.get(path);
		if (s !== undefined) {
			s.deleted = true;
			this.streams.delete(path);
		}
	}

	/** Wait for every delayed application to land. */
	async settle(): Promise<void> {
		while (this.pending.size > 0) await Promise.allSettled([...this.pending]);
	}

	// ------------------------------------------------------------ fault layer

	private async handle<T extends HttpOutcome>(
		info: Omit<FakeRequest, "index">,
		run: () => Promise<T>,
	): Promise<T> {
		const request: FakeRequest = { ...info, index: this.counter++ };
		this.requests.push(request);
		const fault = this.fault?.(request);
		if (fault === undefined) return run();
		switch (fault.type) {
			case "drop-request":
				throw new TransportError("connection", `fake: request ${request.index} dropped`);
			case "drop-response":
				await run();
				throw new TransportError("timeout", `fake: response ${request.index} dropped`);
			case "delay":
				await sleep(fault.ms);
				return run();
			case "delay-apply": {
				const p = sleep(fault.ms).then(run);
				this.pending.add(p);
				void p.finally(() => this.pending.delete(p)).catch(() => undefined);
				throw new TransportError("timeout", `fake: request ${request.index} timed out (applies in ${fault.ms} ms)`);
			}
			case "duplicate":
				await run();
				return run();
			case "respond": {
				if (fault.apply === true) await run();
				return {
					status: fault.status,
					headers: fault.headers ?? {},
					message: fault.message ?? `fake: injected ${fault.status}`,
					records: [],
					rows: [],
				} as unknown as T;
			}
		}
	}

	// ------------------------------------------------------------ server semantics

	private extHeaders(s: FakeStream): Record<string, string> {
		const tokens = [EXT_RECORD_COORDINATES];
		if (s.keyed && this.options.advertiseKeyedBatch !== false) tokens.push(EXT_KEYED_BATCH);
		if (s.keyed && this.options.advertiseKeyedState !== false) tokens.push(EXT_KEYED_STATE);
		return { [H.extensions]: tokens.join(", ") };
	}

	private async head(path: string): Promise<HttpOutcome> {
		const s = this.streams.get(path);
		if (s === undefined) return { status: 404, headers: {}, message: "stream not found" };
		return {
			status: 200,
			headers: { ...this.extHeaders(s), [H.recordFirst]: "0", [H.recordNext]: String(s.tail), "content-type": s.contentType },
		};
	}

	private async create(path: string, contentType: string): Promise<HttpOutcome> {
		const existing = this.streams.get(path);
		if (existing !== undefined) {
			if (existing.contentType !== contentType) return { status: 409, headers: {}, message: "content type mismatch" };
			return { status: 200, headers: { ...this.extHeaders(existing), [H.recordFirst]: "0", [H.recordNext]: String(existing.tail) } };
		}
		const s = new FakeStream(contentType);
		this.streams.set(path, s);
		return { status: 201, headers: { ...this.extHeaders(s), [H.recordFirst]: "0", [H.recordNext]: "0" } };
	}

	private async append(path: string, body: Uint8Array, match: number | undefined): Promise<HttpOutcome> {
		const s = this.streams.get(path);
		if (s === undefined) return { status: 404, headers: {}, message: "stream not found" };
		if (body.length > LIMITS.maxRecordBytes) return { status: 413, headers: {}, message: "body too large" };
		let text: string;
		let ops: KeyedOp[] = [];
		try {
			text = normalizeJsonMessage(fromUtf8(body));
			if (s.keyed) ops = parseKeyedBatch(text);
		} catch (error) {
			if (error instanceof KeyedBatchError) return { status: error.status, headers: this.extHeaders(s), message: error.message };
			throw error;
		}
		if (match !== undefined && match !== s.tail) {
			return { status: 412, headers: { ...this.extHeaders(s), [H.recordNext]: String(s.tail) }, message: "record match failed" };
		}
		const start = s.tail;
		s.texts.push(text);
		s.parsed.push(ops);
		const waiters = s.appendWaiters;
		s.appendWaiters = [];
		for (const w of waiters) w.resolve();
		return {
			status: 204,
			headers: { ...this.extHeaders(s), [H.recordStart]: String(start), [H.recordNext]: String(start + 1) },
		};
	}

	private async read(path: string, from: number, options: ReadRecordsOptions): Promise<ReadRecordsOutcome> {
		const s = this.streams.get(path);
		if (s === undefined) return { status: 404, headers: {}, records: [], message: "stream not found" };
		if (options.maxBytes !== undefined && this.options.advertiseKeyedState === false) {
			return { status: 400, headers: {}, records: [], message: "max_bytes is not allowed on record-aware reads" };
		}
		if (from > s.tail) return { status: 400, headers: { [H.recordNext]: String(s.tail) }, records: [], message: "record beyond tail" };
		if (from === s.tail && options.longPollMs !== undefined) {
			await waitFor(
				(w) => s.appendWaiters.push(w),
				() => s.tail > from || s.deleted,
				options.longPollMs,
			);
			if (s.deleted) return { status: 404, headers: {}, records: [], message: "stream deleted" };
			if (s.tail === from) {
				return {
					status: 204,
					headers: { ...this.extHeaders(s), [H.recordStart]: String(from), [H.recordNext]: String(from), [H.upToDate]: "true" },
					records: [],
				};
			}
		}
		const records: Uint8Array[] = [];
		let bytes = 0;
		let i = from;
		while (i < s.tail) {
			if (options.maxRecords !== undefined && records.length >= options.maxRecords) break;
			const rec = utf8(s.texts[i] as string);
			if (options.maxBytes !== undefined && records.length > 0 && bytes + rec.length + 1 > options.maxBytes) break;
			records.push(rec);
			bytes += rec.length + 1;
			i++;
		}
		const headers: Record<string, string> = {
			...this.extHeaders(s),
			[H.recordFirst]: "0",
			[H.recordStart]: String(from),
			[H.recordNext]: String(i),
		};
		if (i === s.tail) headers[H.upToDate] = "true";
		return { status: 200, headers, records };
	}

	private async scan(path: string, req: KeyedScanRequest): Promise<KeyedScanOutcome> {
		const bad = (message: string): KeyedScanOutcome => ({ status: 400, headers: {}, rows: [], message });
		const decode = (k: string | undefined): string | undefined | null => {
			if (k === undefined) return undefined;
			const key = unb64(k);
			return key === undefined || key.length > LIMITS.maxKeyOctets ? null : key;
		};
		const key = decode(req.key);
		const start = decode(req.start);
		const after = decode(req.after);
		const end = decode(req.end);
		if (key === null || start === null || after === null || end === null) return bad("invalid key");
		if (req.key !== undefined && (req.start ?? req.after ?? req.end ?? req.limit) !== undefined) {
			return bad("key excludes start, after, end and limit");
		}
		if (req.start !== undefined && req.after !== undefined) return bad("start and after are mutually exclusive");
		const limit = req.limit ?? LIMITS.defaultScanLimit;
		if (!Number.isInteger(limit) || limit < 1 || limit > LIMITS.maxScanLimit) return bad("invalid limit");
		const s = this.streams.get(path);
		if (s === undefined || !s.keyed || this.options.advertiseKeyedState === false) {
			return { status: 404, headers: {}, rows: [], message: "not a keyed stream" };
		}
		const r = req.minThroughRecord;
		if (r !== undefined && r > s.tail) {
			return { status: 400, headers: { [H.recordNext]: String(s.tail) }, rows: [], message: "min_through_record beyond tail" };
		}
		const target = this.options.publishTarget?.(s.tail, r) ?? s.tail;
		if (target > s.published) s.publishTo(target);
		const ext = { [H.extensions]: EXT_KEYED_STATE, "cache-control": "no-store" };
		if (r !== undefined && s.published < r) {
			const timeout = Math.min(Math.max(req.timeoutMs ?? LIMITS.defaultTimeoutMs, 1), LIMITS.maxTimeoutMs);
			await waitFor(
				(w) => s.publishWaiters.push(w),
				() => s.published >= r,
				timeout,
			);
			if (s.published < r) return { status: 204, headers: { ...ext, [H.keyedThrough]: String(s.published) }, rows: [], through: s.published };
		}
		const through = s.published;
		const rows: KeyedRow[] = [];
		let truncated = false;
		if (key !== undefined) {
			const row = s.projection.get(key);
			if (row !== undefined) rows.push({ key: req.key as string, record: row.record, value: row.value });
		} else {
			const lo = start ?? (after !== undefined ? keySuccessor(after) : "");
			let budget = 0;
			for (const [k, row] of s.projection.range(lo, end)) {
				if (rows.length >= limit) {
					truncated = true;
					break;
				}
				const line = `{"key":"${b64(k)}","record":${row.record},"value":${row.value}}\n`.length;
				if (rows.length > 0 && budget + line > LIMITS.scanResponseBudget) {
					truncated = true;
					break;
				}
				budget += line;
				rows.push({ key: b64(k), record: row.record, value: row.value });
			}
		}
		const headers: Record<string, string> = { ...ext, [H.keyedThrough]: String(through) };
		const last = rows.at(-1);
		if (truncated && last !== undefined) headers[H.keyedAfter] = last.key;
		return { status: 200, headers, rows, through, ...(truncated && last !== undefined ? { after: last.key } : {}) };
	}
}
