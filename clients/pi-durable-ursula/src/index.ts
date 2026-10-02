// @tonbo-io/pi-durable-ursula: Pi Durable Storage on Ursula keyed streams.
//
// Module map:
// - tuple.ts       — tuple layer (§4.2): u64, u64desc, u8, WTF-8 `str` with digest long form, strinc, base64url.
// - families.ts    — Pi key families (§4.3) and status/scope codes.
// - keyed-batch.ts — keyed-batch-v1 encoding, P1 JSON normalization, P2 grammar validation, op parsing.
// - ordered-map.ts — sorted binary-key map used by the state store and the fake.
// - state-store.ts — StateStore/StateView contract and the full-resident M1 store.
// - local-store/   — bounded StateStore (§7.2–§7.5): overlay + range cache, fresh floor, eviction, skip list.
// - bounded.ts     — bounded-owner policies over the Pi schema: fetch widening (§4.5) and open preload (§3.6, §7.4).
// - flush.ts       — background flush loop (§7.9): flush-waits raise E, detect takeovers; transient failures never poison.
// - pi-layer.ts    — read plans of the Storage methods (§4.5) over a StateView.
// - planner.ts     — StorageWrite → ops (§4.4), MemoryStorage-ported validation, pre-checks, claims.
// - transport.ts   — LogTransport / KeyedStateTransport interfaces (1:1 with the HTTP API).
// - http.ts        — HttpLogTransport / HttpKeyedStateTransport over fetch (NDJSON records, raw keyed rows).
// - storage.ts     — UrsulaStorage: bounded or full-resident state, commit outcome policy, open/claim, close, poison.
// - errors.ts      — FencedError, OwnershipActive, OwnershipContention, ClaimTimeout, OpenRefused.
// - fake/          — in-memory fake Ursula with fault injection (tests and local development).
export { ClaimTimeout, FencedError, OpenRefused, OwnershipActive, OwnershipContention } from "./errors.ts";
export { K, TAG } from "./families.ts";
export {
	HttpKeyedStateTransport,
	HttpLogTransport,
	type HttpTransportOptions,
	httpTransports,
	parseKeyedRows,
	splitRecords,
	streamUrl,
} from "./http.ts";
export { encodeRecord, type KeyedOp, KeyedBatchError, normalizeJsonMessage, parseKeyedBatch } from "./keyed-batch.ts";
export { EXT_KEYED_BATCH, EXT_KEYED_STATE, H, KEYED_CONTENT_TYPE, KEYED_ROWS_MEDIA_TYPE, LIMITS } from "./protocol.ts";
export { LocalStore, type LocalStoreOptions } from "./local-store/index.ts";
export { FullResidentStateStore, type Row, type StateStore, type StateView, ViewAbort } from "./state-store.ts";
export {
	type Clock,
	DEFAULT_TIMING,
	type OpenMode,
	type OwnerAlert,
	type StateStoreKind,
	systemClock,
	type Timing,
	UrsulaStorage,
	type UrsulaStorageOptions,
} from "./storage.ts";
export type {
	HttpOutcome,
	KeyedRow,
	KeyedScanOutcome,
	KeyedScanRequest,
	KeyedStateTransport,
	LogTransport,
	ReadRecordsOptions,
	ReadRecordsOutcome,
} from "./transport.ts";
export { TransportError } from "./transport.ts";
export { b64, str, strinc, u64, u64desc, u8, unb64 } from "./tuple.ts";
