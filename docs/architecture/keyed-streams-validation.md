# Keyed Streams Design Validation

Status: Phase 0 (M0a) decision and executable-model gate

Design: `docs/architecture/keyed-streams-pi-durable.md`

Normative text: `docs/web/src/content/docs/pages/specs/extensions.mdx`, sections "JSON Message Text" (P1), 6.6 (P7), 9.1 (P2), 9.2 (P3), 9.3 (correctness requirements), 9.4 (tuple layer, informative), and the amended Section 3 (bootstrap)

Related: `docs/architecture/json-record-coordinates-validation.md` (the record-coordinates gate this one extends)

## Decisions

The protocol questions in design §13 are settled:

- Q0: records are writer-declared `keyed-batch-v1` batches. There is no server-side extraction spec and no user code in Ursula.
- Q1: keyed logs are never trimmed. The projection is a cache rebuilt from record 0.
- Q2: JSON Message Text (P1) is the default for every `application/json` stream. It is an observable change and ships with a release note.
- Q3: activation is `application/json; profile=keyed-batch-v1`. The names are `keyed-batch-v1`, `keyed-state-v1`, the resource `keyed-state`, the headers `Stream-Keyed-Through` and `Stream-Keyed-After`, the rows media type `application/vnd.durable-stream-keyed-rows+ndjson`, and the parameter `min_through_record`.
- Q4 and Q5 are owner-side (default open mode `fail-if-active` with W = 3 s; 32 MiB per commit, JSON depth ≤ 127, overlay cap 256 MiB). Only the depth limit and the request body limit appear in the protocol.
- Bootstrap: Option B, honest partial. `/bootstrap` stays read-only and never reads the cold tier. It returns either every retained message after the snapshot point, or the snapshot alone with `Stream-Next-Offset` at the snapshot point and `Stream-Up-To-Date: false`. It never skips a message and never puts two messages in one part.

## Gate

Production work on the keyed data path (U3 onward) starts only after the reference model `crates/ursula-index/src/keyed/fold.rs` passes every vector in `crates/ursula-index/tests/vectors/keyed_batch_v1.json`. As with the record-coordinates oracle, the model is deliberately small and independent of the engine: a grammar check and a `BTreeMap` fold. Later engine, HTTP and TypeScript tests compare against it instead of restating implementation details.

The gate proves these properties.

**Grammar (spec §9.1.2, §9.1.3):**

1. A message is accepted if and only if it matches the grammar. The model reports the zero-based index of the first failing message after array flattening, and a request with any failing message is rejected as a whole.
2. `ops` is found after unescaping: `"ops"` is the `ops` member, and two members that unescape to `ops` are rejected. A message without `ops` is rejected; any other member is accepted and ignored.
3. An unpaired-surrogate escape in a top-level member name is rejected; the same escape in a member name inside a value is accepted.
4. Op codes are compared after unescaping (`"p"` is `p`). The arities are exact: `p` takes a key and a value, `d` a key, `x` two keys. Any other code or arity is rejected.
5. Keys are canonical unpadded base64url of 1..4096 octets, checked on raw characters: `AB` (non-zero pad bits), `AA==` (padding), `+/` (standard alphabet), the empty string, any backslash escape, and 4097 octets are rejected; 1 and 4096 octets are accepted.
6. A range delete with `start ≥ end` in unsigned octet order is rejected.
7. An empty `ops` array is accepted and changes nothing. A put of `null` is accepted and makes the key visible.

**Fold (spec §9.1.4):**

8. `state(0)` is empty. `state(D)` applies records `0 … D−1` in order and the ops of each record in array order.
9. A put in record `r` sets `k → (r, value text)`; a later put replaces both. A point delete removes the key. A range delete removes exactly the keys in `[start, end)` in unsigned octet order, including keys put earlier in the same record and excluding keys put later in the same record.
10. A row's `record` is the ordinal of the record of the last applied put. A re-put in a later record moves it; a delete followed by a put in the same record gives the put's record.
11. Value text is carried byte-for-byte as stored under P1: member order, duplicate members, `1.50e3`, `1e400` and lone-surrogate escapes survive the fold.
12. Members other than `ops` never affect the fold. The fold never produces a key that no op declares.
13. Keys order by their decoded unsigned octets, not by their base64url text: `-w` (0xFB) and `_w` (0xFF) sort after `aA` (0x68), although `-` and `_` sort before `a` in ASCII.

**Reads over the model (spec §9.2):**

14. A range `[lo, hi)` with `start`, `after` and `end` returns exactly the visible keys of `state(D)` in that range, in ascending octet order; `lo ≥ hi` returns nothing.
15. With `limit`, a page returns at most `limit` rows and names its last key; paging with `after` until no last key is named visits every key in the range exactly once at a fixed `D`.
16. With a byte budget, a page never exceeds the budget unless it holds exactly one row.

**JSON Message Text (spec "JSON Message Text"):**

17. `stored == minify(input)` on every vector: insignificant whitespace is removed outside strings, nothing else changes, and one LF terminates each message.
18. Depth 127 is accepted and depth 128 is rejected for a bare message; a message at depth 127 inside an array body is accepted, because depth is measured per message after flattening.

**Tuple layer (spec §9.4, informative):**

19. The tuple vectors in spec §9.4 reproduce, including the two lone-surrogate keys and the long (digest) form.
20. The NUL-extension pairs `"k"` / `"k\u0000z"` and `""` / `"\u0000"` encode so that the shorter is a prefix of the longer, which is why scans compare full strings.

Vectors are data, not code: each vector names its input messages (as raw text, so escapes and whitespace survive), the expected acceptance or failing index and reason class, and, for accepted logs, the expected `key → (record, value text)` map at one or more `D`. The TypeScript overlay fold (§7.2 of the design) and the Rust indexer fold (U14, U16) run the same file, which is how I27 (identical maps on any log prefix) is checked.

## HTTP vectors at the real boundary

The model gate does not touch HTTP. These vectors run against a real node from the milestone named; each one is a request, the expected status, the expected headers, and, where it applies, the expected body bytes or rows.

### M1: write path, P1 and P2

Every P2 negative vector runs on all four write paths: the creating `PUT` body, `POST`, an `append-batch` frame, and a `$transaction` operation.

- Every grammar vector from the gate that the model rejects: `422`, nothing committed (the record tail and `Stream-Next-Offset` are unchanged), and a plain-text body naming the message index (`frame <f> message <i>` for `append-batch`).
- An `append-batch` request whose second frame is ungrammatical: `422` for the whole request, and the first frame is not committed.
- Precedence: invalid JSON with the keyed content type is `400`, not `422`; an ungrammatical batch sent with the keyed content type to an absent stream or to a plain `application/json` stream is `422`, not `404` or `409`; a grammatical batch to an absent stream is `404`; a grammatical batch whose content type does not match the stream's is `409`; a grammatical batch with a stale `Stream-Record-Match` is `412`.
- Activation: `Application/JSON;Profile=keyed-batch-v1` creates a keyed stream; `profile="keyed-batch-v1"` and an extra parameter do not. A plain `application/json` append to a keyed stream is `409`.
- Advertisement: `HEAD`, the create response, append responses and record-aware reads of a keyed stream carry `keyed-batch-v1` and `json-record-coordinates-v1`; a plain JSON stream never carries `keyed-batch-v1`.
- `$transaction`: an operation `content_type` that differs from the stream's only in case or whitespace is accepted (Section 1.8 normalization).
- P1 fidelity on a plain `application/json` stream and on a keyed stream: member order, duplicate members, `1.50e3`, `1e400` and `\ud800` round-trip through offset reads, record reads, the envelope view, SSE and bootstrap parts; bodies of depth 127/128/129 give `2xx`/`400`/`400` as bare messages and `2xx`/`2xx`/`400` as one-element arrays (whose message is one level shallower); stored bytes equal `minify(input)` byte for byte.
- Keyed creates are refused below group feature level 1.
- The official Durable Streams conformance suite stays at 300/300 (the M0b gate).

### M1: bootstrap (Option B)

These are regression tests for the two bootstrap bugs (design §6.5) and pin the amended Section 3.

- No snapshot, all messages hot: an empty first part, one part per message in order, `Stream-Next-Offset` = tail, `Stream-Up-To-Date: true`.
- Snapshot at S, every message after S hot: the snapshot part, one part per message in `[S, tail)`, `Stream-Next-Offset` = tail, `Stream-Up-To-Date: true`.
- Messages after S already flushed to the cold tier (snapshot offset beyond the retained offset, the case that drops updates today): the snapshot part and no update parts, `Stream-Next-Offset` = S, `Stream-Up-To-Date: false`; an ordinary read from S returns every message in `[S, tail)`, so bootstrap plus catch-up equals a full replay.
- No snapshot and a cold prefix (the case that returns the cold suffix as one part today): an empty first part, no update parts, `Stream-Next-Offset` = the first retained offset, `Stream-Up-To-Date: false`.
- In every case: no part holds more than one message, no message is skipped or duplicated between S and `Stream-Next-Offset`, and the request performs no object-store read (asserted with a counting object store).
- JSON streams: each update part is one record, consistent with Section 6.10.

### M2: P3 keyed-state and P7 byte-bounded reads

- Rows: a log built from the gate vectors, read at each published `D` with `key`, `start`, `after`, `end` and `limit`, equals the model's map restricted to the range; the body is `application/vnd.durable-stream-keyed-rows+ndjson` with members `key`, `record`, `value` in that order and `value` byte-identical to the stored text.
- A point read of an invisible key: `200` with an empty body.
- Headers: `Stream-Keyed-Through` and `Cache-Control: no-store` on every `200` and `204`; `Stream-Keyed-After` whenever `limit` or the 4 MiB budget stops a page; a first row larger than 4 MiB is returned whole.
- Parameters: `key` combined with a range parameter, `start` with `after`, a repeated parameter, a non-canonical key, `limit=0` and `limit=1001`, and an unparseable `min_through_record` are `400`; `lo ≥ hi` is an empty `200`; `timeout_ms=0`, `timeout_ms=999999` and `timeout_ms=abc` are clamped or defaulted.
- Waiting: `min_through_record` above the published `D` and at most `N` returns `D ≥ r` once ingested, or `204` with `Stream-Keyed-Through` below `r` at the timeout; `min_through_record` above `N` is `400` with `Stream-Record-Next: N`.
- Monotonicity: across concurrent publish and compaction, no `200` or `204` for one incarnation carries a smaller `D` than an earlier response.
- Namespace absent: `state(0)`, `Stream-Keyed-Through: 0`.
- Keyed state ahead of the source (`D > N`): `503` with `Retry-After`.
- Methods: `HEAD`, `PUT`, `POST` and `DELETE` are `405` with `Allow: GET`.
- Not keyed or absent: `404`, without `keyed-state-v1`; every other response carries `keyed-state-v1`, and `HEAD` and create of a keyed stream advertise it when the resource is served.
- Incarnation: delete and recreate the same path within one millisecond (frozen clock); the new stream serves `state(0)` or its own state, never the old one.
- Retention past what ingestion needs: `500` naming a record ordinal for any `min_through_record` above the held `D`.
- P7: `record=r&max_bytes=b` returns the longest run of complete records whose stored bytes (LF included) fit `b`, and at least one record; combined with `max_records`, whichever stops first; with `record_view=envelope`, the same records as the default view (envelope framing does not count); `Stream-Record-Next` and `Stream-Next-Offset` agree on the continuation; offset reads keep base-protocol `max_bytes`.
- Affinity form: `/{bucket}/{affinity}/{stream}/keyed-state` serves the same resource, and an affinity stream named `keyed-state` cannot be created once the reservation is active.

The DST `http-protocol-surface` seed families extend to P1, P2 and P3.

## Implementation acceptance

The production implementation is accepted only when the gate vectors run at the HTTP boundary as listed above, and the engine model test (design §11.4) shows every published namespace equals `fold(log[0, D))` across flush, compaction, GC and reads, including tombstone-only parts. Applier conformance (design §11.5, I27) runs the Rust indexer fold, the TypeScript overlay fold and the reference model over the same logs and requires identical maps.

## Implementation audit

Filled in as each milestone lands.

| Contract | Production path | Evidence |
| --- | --- | --- |
| P1 text fidelity | `ursula/src/render.rs` minify and flatten (U1); envelope splices stored bytes (U2) | M0b proptest (10^6 cases), DS conformance 300/300, depth vectors |
| P2 grammar and 422 | shared validator `ursula-index/src/keyed/batch.rs` (U3) at all four write paths (U4) | gate vectors; M1 negative vectors on four paths; fuzz |
| Activation and advertisement | `normalize_content_type` and `profile_of` in `ursula-shard` (U5) | M1 activation and advertisement vectors |
| `$transaction` content-type normalization | `ursula/src/lib.rs` (U10) | M1 transaction vector |
| Bootstrap Option B | bootstrap handler and read plan | M1 bootstrap regression tests with a counting object store |
| P3 keyed-state | node proxy (U7), indexer API and engine (U13–U17) | M2 vectors; engine model test; two-pod CAS race |
| P7 byte-bounded reads | `ursula/src/lib.rs`, `ursula-runtime` read plan (U6) | M2 P7 vectors; sparse-mark differential (C1) |
| Fold equivalence (I27) | Rust fold, TypeScript overlay fold, reference model | applier conformance property test |
