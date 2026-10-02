// HTTP implementations of LogTransport and KeyedStateTransport over `fetch` (Node 22).
//
// Both are 1:1 with the Ursula HTTP API (design §3.3, §3.6, §5.3, §5.4): every response status is
// returned as a value, and only "no response" failures throw `TransportError` (connection error,
// timeout). A response whose body breaks the protocol (malformed keyed rows, a record count that
// disagrees with the record coordinates) throws a plain `Error`: retrying cannot fix it.
import { EXT_KEYED_STATE, extensionTokens, H, type Headers, KEYED_CONTENT_TYPE, intHeader } from "./protocol.ts";
import type {
	HttpOutcome,
	KeyedRow,
	KeyedScanOutcome,
	KeyedScanRequest,
	KeyedStateTransport,
	LogTransport,
	ReadRecordsOptions,
	ReadRecordsOutcome,
} from "./transport.ts";
import { TransportError } from "./transport.ts";

export interface HttpTransportOptions {
	/** Node or gateway base URL, for example `http://127.0.0.1:4437`. */
	readonly baseUrl: string;
	/** Stream path `{bucket}/{stream}` (or `{bucket}/{affinity}/{stream}`), each segment unencoded. */
	readonly stream: string;
	/** Sent as `Authorization: Bearer <token>`. */
	readonly token?: string;
	/** Per-request timeout in ms, on top of any server-side wait the request asks for (default 30 s). */
	readonly timeoutMs?: number;
	/** `max_records` page used when `maxBytes` is asked for but the node has not advertised P7 (default 1000). */
	readonly fallbackPageRecords?: number;
	/** Override for tests. */
	readonly fetch?: typeof fetch;
}

const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_FALLBACK_PAGE_RECORDS = 1000;
const LF = 0x0a;

/** `{baseUrl}/{bucket}/{stream}` with every path segment percent-encoded. */
export function streamUrl(baseUrl: string, stream: string): string {
	const base = baseUrl.replace(/\/+$/, "");
	const segments = stream.split("/").filter((s) => s.length > 0);
	if (segments.length < 2) throw new TypeError(`stream path must be {bucket}/{stream}, got ${JSON.stringify(stream)}`);
	return `${base}/${segments.map(encodeURIComponent).join("/")}`;
}

function lowercaseHeaders(headers: globalThis.Headers): Record<string, string> {
	const out: Record<string, string> = {};
	headers.forEach((value, name) => {
		out[name.toLowerCase()] = value;
	});
	return out;
}

/** Split an NDJSON body into its lines, without their LF, as views over the original bytes. */
export function splitRecords(body: Uint8Array): Uint8Array[] {
	const out: Uint8Array[] = [];
	let start = 0;
	for (let i = 0; i < body.length; i++) {
		if (body[i] === LF) {
			out.push(body.subarray(start, i));
			start = i + 1;
		}
	}
	if (start < body.length) out.push(body.subarray(start));
	return out;
}

const strictUtf8 = new TextDecoder("utf-8", { fatal: true });
const ROW_PREFIX = /^\{"key":"([A-Za-z0-9_-]+)","record":(0|[1-9]\d*),"value":/;

/**
 * Parse a keyed-rows NDJSON body (P3.3). Each line is `{"key":"<k>","record":<r>,"value":<v>}` with
 * members in exactly that order, so the value's stored text is the slice between the prefix and the
 * closing brace: it is never `JSON.parse`d, which would rewrite number text and reorder members.
 */
export function parseKeyedRows(body: Uint8Array): KeyedRow[] {
	let text: string;
	try {
		text = strictUtf8.decode(body);
	} catch (error) {
		throw new Error("keyed-state: the response body is not valid UTF-8", { cause: error });
	}
	const rows: KeyedRow[] = [];
	let lineNo = 0;
	for (const line of text.split("\n")) {
		if (line.length === 0) continue;
		const m = ROW_PREFIX.exec(line);
		const record = m === null ? undefined : Number(m[2]);
		if (m === null || !line.endsWith("}") || record === undefined || !Number.isSafeInteger(record) || line.length < m[0].length + 2) {
			throw new Error(`keyed-state: malformed row at line ${lineNo}`);
		}
		rows.push({ key: m[1] as string, record, value: line.slice(m[0].length, -1) });
		lineNo++;
	}
	return rows;
}

/** Shared request plumbing: URL, auth, timeouts and outcome mapping. */
class HttpClient {
	readonly url: string;
	private readonly token: string | undefined;
	private readonly timeoutMs: number;
	private readonly fetchFn: typeof fetch;

	constructor(url: string, options: HttpTransportOptions) {
		this.url = url;
		this.token = options.token;
		this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
		this.fetchFn = options.fetch ?? globalThis.fetch;
	}

	/** Issue one request; resolves with the status, lowercase headers and the whole body. */
	async request(
		method: string,
		url: string,
		init: { headers?: Record<string, string>; body?: Uint8Array; waitMs?: number } = {},
	): Promise<{ status: number; headers: Headers; body: Uint8Array }> {
		const headers: Record<string, string> = { ...init.headers };
		if (this.token !== undefined) headers.authorization = `Bearer ${this.token}`;
		const budget = this.timeoutMs + (init.waitMs ?? 0);
		const signal = AbortSignal.timeout(budget);
		try {
			const response = await this.fetchFn(url, {
				method,
				headers,
				signal,
				redirect: "follow",
				...(init.body === undefined ? {} : { body: init.body }),
			});
			const body = new Uint8Array(await response.arrayBuffer());
			return { status: response.status, headers: lowercaseHeaders(response.headers), body };
		} catch (error) {
			if (signal.aborted) throw new TransportError("timeout", `${method} ${url}: no response within ${budget} ms`, { cause: error });
			throw new TransportError("connection", `${method} ${url}: ${describeError(error)}`, { cause: error });
		}
	}
}

function describeError(error: unknown): string {
	if (!(error instanceof Error)) return String(error);
	const cause = (error as { cause?: unknown }).cause;
	return cause instanceof Error ? `${error.message}: ${cause.message}` : error.message;
}

function outcome(status: number, headers: Headers, body: Uint8Array): HttpOutcome {
	if (status >= 200 && status < 300) return { status, headers };
	const message = new TextDecoder().decode(body).trim();
	return message.length > 0 ? { status, headers, message } : { status, headers };
}

/** `LogTransport` over HTTP: one keyed stream's log. */
export class HttpLogTransport implements LogTransport {
	private readonly http: HttpClient;
	private readonly fallbackPageRecords: number;
	/** Whether a response for this stream advertised `keyed-state-v1` (and so P7 `max_bytes`). */
	private p7 = false;

	constructor(options: HttpTransportOptions) {
		this.http = new HttpClient(streamUrl(options.baseUrl, options.stream), options);
		this.fallbackPageRecords = options.fallbackPageRecords ?? DEFAULT_FALLBACK_PAGE_RECORDS;
	}

	get url(): string {
		return this.http.url;
	}

	private observe(headers: Headers): void {
		if (extensionTokens(headers).has(EXT_KEYED_STATE)) this.p7 = true;
	}

	async head(): Promise<HttpOutcome> {
		const r = await this.http.request("HEAD", this.url);
		this.observe(r.headers);
		return outcome(r.status, r.headers, r.body);
	}

	async create(): Promise<HttpOutcome> {
		const r = await this.http.request("PUT", this.url, { headers: { "content-type": KEYED_CONTENT_TYPE } });
		this.observe(r.headers);
		return outcome(r.status, r.headers, r.body);
	}

	async append(body: Uint8Array, match: number): Promise<HttpOutcome> {
		const r = await this.http.request("POST", this.url, {
			headers: { "content-type": KEYED_CONTENT_TYPE, "stream-record-match": String(match) },
			body,
		});
		this.observe(r.headers);
		return outcome(r.status, r.headers, r.body);
	}

	async readRecords(from: number, options: ReadRecordsOptions = {}): Promise<ReadRecordsOutcome> {
		const q = new URLSearchParams({ record: String(from) });
		let maxRecords = options.maxRecords;
		if (options.maxBytes !== undefined) {
			// P7 is only safe once the node advertised it; otherwise page by records.
			if (this.p7) q.set("max_bytes", String(options.maxBytes));
			else maxRecords ??= this.fallbackPageRecords;
		}
		if (maxRecords !== undefined) q.set("max_records", String(maxRecords));
		if (options.longPollMs !== undefined) {
			q.set("live", "long-poll");
			q.set("timeout_ms", String(options.longPollMs));
		}
		if (options.leader === true) q.set("consistency", "leader");
		const r = await this.http.request("GET", `${this.url}?${q}`, { waitMs: options.longPollMs ?? 0 });
		this.observe(r.headers);
		if (r.status !== 200) return { ...outcome(r.status, r.headers, r.body), records: [] };
		const records = splitRecords(r.body);
		const start = intHeader(r.headers, H.recordStart);
		const next = intHeader(r.headers, H.recordNext);
		if (start !== undefined && next !== undefined && next - start !== records.length) {
			throw new Error(`record read from ${from}: ${records.length} records in the body but coordinates [${start}, ${next})`);
		}
		return { status: r.status, headers: r.headers, records };
	}
}

/** `KeyedStateTransport` over HTTP: `GET {stream}/keyed-state` (P3). */
export class HttpKeyedStateTransport implements KeyedStateTransport {
	private readonly http: HttpClient;

	constructor(options: HttpTransportOptions) {
		this.http = new HttpClient(`${streamUrl(options.baseUrl, options.stream)}/keyed-state`, options);
	}

	get url(): string {
		return this.http.url;
	}

	async scan(request: KeyedScanRequest): Promise<KeyedScanOutcome> {
		const q = new URLSearchParams();
		if (request.key !== undefined) q.set("key", request.key);
		if (request.start !== undefined) q.set("start", request.start);
		if (request.after !== undefined) q.set("after", request.after);
		if (request.end !== undefined) q.set("end", request.end);
		if (request.limit !== undefined) q.set("limit", String(request.limit));
		if (request.minThroughRecord !== undefined) q.set("min_through_record", String(request.minThroughRecord));
		if (request.timeoutMs !== undefined) q.set("timeout_ms", String(request.timeoutMs));
		const query = q.size > 0 ? `?${q}` : "";
		// The server waits at most timeout_ms (default 1000) and only with min_through_record.
		const waitMs = request.minThroughRecord === undefined ? 0 : (request.timeoutMs ?? 1000);
		const r = await this.http.request("GET", `${this.url}${query}`, { waitMs });
		const through = intHeader(r.headers, H.keyedThrough);
		const base = { status: r.status, headers: r.headers, ...(through === undefined ? {} : { through }) };
		if (r.status === 204) return { ...base, rows: [] };
		if (r.status !== 200) return { ...outcome(r.status, r.headers, r.body), ...(through === undefined ? {} : { through }), rows: [] };
		const rows = parseKeyedRows(r.body);
		const after = r.headers[H.keyedAfter];
		return { ...base, rows, ...(after === undefined ? {} : { after }) };
	}
}

/** Both transports for one stream. */
export function httpTransports(options: HttpTransportOptions): { log: HttpLogTransport; keyedState: HttpKeyedStateTransport } {
	return { log: new HttpLogTransport(options), keyedState: new HttpKeyedStateTransport(options) };
}
