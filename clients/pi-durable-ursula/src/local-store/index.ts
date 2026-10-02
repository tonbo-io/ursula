// Bounded owner state (design §7.2–§7.5): overlay [E, tail) + range cache over a skip list,
// complete-at-mint fresh floor, LRU eviction with pinning. UrsulaStorage uses it as its bounded store.
export { isFreshKey, isFreshRange } from "./fresh.ts";
export { type CachedRow, LocalStore, type LocalStoreOptions, ROW_OVERHEAD } from "./local-store.ts";
export { SkipList } from "./skip-list.ts";
