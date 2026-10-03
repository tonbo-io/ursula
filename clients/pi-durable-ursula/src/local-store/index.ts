// Bounded owner state (design §7.2–§7.5): overlay [E, tail) + range cache over a skip list,
// complete-at-mint fresh floor, eviction with pinning (large values first, LRU ranges shrunk from
// their cold end, adjacent unpinned ranges coalesced), and the overlay alert. UrsulaStorage uses it as its bounded store.
export { LocalStore, type LocalStoreOptions, OVERLAY_ALERT_BYTES } from "./local-store.ts";
