// Transport interfaces. Each method maps 1:1 to one HTTP request of the Ursula API; the owner
// classifies outcomes itself (§3.3, §7.6), so transports return every HTTP status as a value and
// throw only for failures where no response arrived (connection error, timeout, abort).
//
// Header names are lowercase. Keys on the keyed-state surface are canonical base64url text, exactly
// as on the wire; record bodies are raw bytes.
import type { Headers } from "./protocol.ts";

/** No HTTP response was received: the request may or may not have been applied. */
export class TransportError extends Error {
	readonly kind: "connection" | "timeout" | "aborted";
	constructor(kind: "connection" | "timeout" | "aborted", message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "TransportError";
		this.kind = kind;
	}
}

export interface HttpOutcome {
	readonly status: number;
	readonly headers: Headers;
	/** Plain-text error body, when the server sent one. */
	readonly message?: string;
}

export interface ReadRecordsOptions {
	/** P7 `max_bytes`: only when the node advertises `keyed-state-v1`. */
	readonly maxBytes?: number;
	/** `max_records`. */
	readonly maxRecords?: number;
	/** `live=long-poll&timeout_ms=…`: wait for a record at `from` when `from` is the tail. */
	readonly longPollMs?: number;
	/** `consistency=leader`. */
	readonly leader?: boolean;
}

export interface ReadRecordsOutcome extends HttpOutcome {
	/** Complete stored messages starting at `Stream-Record-Start`, without their trailing LF. */
	readonly records: readonly Uint8Array[];
}

/** One keyed stream's log: `{bucket}/{stream}`. */
export interface LogTransport {
	/** `HEAD {log}`. */
	head(): Promise<HttpOutcome>;
	/** `PUT {log}` with `Content-Type: application/json; profile=keyed-batch-v1` and no body. */
	create(): Promise<HttpOutcome>;
	/** `POST {log}` with the keyed content type and `Stream-Record-Match: match`. */
	append(body: Uint8Array, match: number): Promise<HttpOutcome>;
	/** `GET {log}?record=from[&max_bytes][&max_records][&live=long-poll&timeout_ms][&consistency=leader]`. */
	readRecords(from: number, options?: ReadRecordsOptions): Promise<ReadRecordsOutcome>;
}

export interface KeyedScanRequest {
	/** Point read; excludes start/after/end/limit. */
	readonly key?: string;
	readonly start?: string;
	readonly after?: string;
	readonly end?: string;
	readonly limit?: number;
	readonly minThroughRecord?: number;
	readonly timeoutMs?: number;
}

export interface KeyedRow {
	/** Canonical base64url. */
	readonly key: string;
	readonly record: number;
	/** The value's stored JSON text. */
	readonly value: string;
}

export interface KeyedScanOutcome extends HttpOutcome {
	readonly rows: readonly KeyedRow[];
	/** `Stream-Keyed-Through` (200 and 204). */
	readonly through?: number;
	/** `Stream-Keyed-After` when the range was truncated. */
	readonly after?: string;
}

/** `GET {log}/keyed-state?…` (P3). */
export interface KeyedStateTransport {
	scan(request: KeyedScanRequest): Promise<KeyedScanOutcome>;
}
