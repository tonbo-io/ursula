// Wire constants of the Ursula protocol surface the owner uses (design §5, §13 Q3/Q5).

/** Content type that activates `keyed-batch-v1` on a stream (P2, §13 Q3 option a). */
export const KEYED_CONTENT_TYPE = "application/json; profile=keyed-batch-v1";
/** Media type of keyed-state row bodies (P3.3). */
export const KEYED_ROWS_MEDIA_TYPE = "application/vnd.durable-stream-keyed-rows+ndjson";

export const EXT_KEYED_BATCH = "keyed-batch-v1";
export const EXT_KEYED_STATE = "keyed-state-v1";
export const EXT_RECORD_COORDINATES = "json-record-coordinates-v1";

/** Response header names, lowercase (transports normalize header names to lowercase). */
export const H = {
	extensions: "stream-extensions",
	recordFirst: "stream-record-first",
	recordStart: "stream-record-start",
	recordNext: "stream-record-next",
	upToDate: "stream-up-to-date",
	keyedThrough: "stream-keyed-through",
	keyedAfter: "stream-keyed-after",
	retryAfter: "retry-after",
} as const;

/** Server limits mirrored by owner pre-checks (§3.3 step 2, §5.1.3, P2.8). */
export const LIMITS = {
	/** Request body cap: one commit record. */
	maxRecordBytes: 32 * 1024 * 1024,
	/** JSON nesting depth per message. */
	maxJsonDepth: 127,
	/** Octets per key. */
	maxKeyOctets: 4096,
	/** keyed-state `limit` bounds and default. */
	maxScanLimit: 1000,
	defaultScanLimit: 100,
	/** keyed-state response budget (uncompressed body). */
	scanResponseBudget: 4 * 1024 * 1024,
	/** keyed-state `timeout_ms` clamp and default. */
	maxTimeoutMs: 60_000,
	defaultTimeoutMs: 1000,
} as const;

export type Headers = Readonly<Record<string, string>>;

/** Parse a comma-separated `Stream-Extensions` header into tokens. */
export function extensionTokens(headers: Headers): Set<string> {
	const raw = headers[H.extensions];
	if (raw === undefined) return new Set();
	return new Set(
		raw
			.split(",")
			.map((t) => t.trim())
			.filter((t) => t.length > 0),
	);
}

/** Parse a non-negative safe integer header, or undefined when absent or malformed. */
export function intHeader(headers: Headers, name: string): number | undefined {
	const raw = headers[name];
	if (raw === undefined || !/^\d+$/.test(raw)) return undefined;
	const n = Number(raw);
	return Number.isSafeInteger(n) ? n : undefined;
}

/** `Retry-After` in milliseconds (delta-seconds form only), or undefined when absent. */
export function retryAfterMs(headers: Headers): number | undefined {
	const raw = headers[H.retryAfter];
	if (raw === undefined) return undefined;
	const s = Number(raw.trim());
	return Number.isFinite(s) && s >= 0 ? s * 1000 : 0;
}
