# ursula-wasm

`ursula-wasm` embeds Wasmtime for short, pure, per-stream reducers. Components receive opaque reducer state, an intent, and stream coordinates, then return new opaque state, records to append, and an optional response.

The ABI intentionally exposes no WASI imports, network, filesystem, clock, randomness, or cross-stream access. Ursula supplies time and stream coordinates as explicit input, bounds guest memory and fuel, and replicates the materialized reduction rather than executing guest code on followers.
