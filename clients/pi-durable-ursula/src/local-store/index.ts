// Bounded owner state (design §7.2–§7.5): overlay [E, tail) + range cache over a skip list,
// complete-at-mint fresh floor, eviction with pinning (large values first, LRU ranges shrunk from
// their cold end, adjacent unpinned ranges coalesced), and the overlay alert. UrsulaStorage uses it as its bounded store.
export { isFreshKey, isFreshRange } from "./fresh.ts";
export { type CachedRow, LARGE_VALUE_BYTES, LocalStore, type LocalStoreOptions, OVERLAY_ALERT_BYTES, ROW_OVERHEAD } from "./local-store.ts";
export { SkipList } from "./skip-list.ts";
