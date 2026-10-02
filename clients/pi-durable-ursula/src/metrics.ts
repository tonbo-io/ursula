// Owner metrics (design §7.6 "Owner metrics", §10 M3): remote reads on the Session line and their
// latency, pinned bytes, and poison, fence and contention counts.
//
// One `OwnerMetrics` may be shared by every open of a host (`UrsulaStorageOptions.metrics`), because
// some events (ownership contention, refused opens) happen before a storage exists. Gauges (pinned
// and cached bytes) belong to one storage and are read through `UrsulaStorage.metrics()`.

/** A latency recorder: totals plus a ring of the most recent samples for quantiles. */
export class LatencyRecorder {
	count = 0;
	totalMs = 0;
	maxMs = 0;
	private readonly ring: number[] = [];
	private next = 0;
	private readonly capacity: number;

	constructor(capacity = 4096) {
		this.capacity = capacity;
	}

	record(ms: number): void {
		this.count++;
		this.totalMs += ms;
		if (ms > this.maxMs) this.maxMs = ms;
		if (this.ring.length < this.capacity) this.ring.push(ms);
		else this.ring[this.next] = ms;
		this.next = (this.next + 1) % this.capacity;
	}

	/** Quantile `q` (0..1) of the retained samples (nearest rank), or undefined without samples. */
	quantile(q: number): number | undefined {
		if (this.ring.length === 0) return undefined;
		const sorted = [...this.ring].sort((a, b) => a - b);
		const rank = Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1));
		return sorted[rank];
	}

	snapshot(): LatencySnapshot {
		return {
			count: this.count,
			meanMs: this.count === 0 ? 0 : this.totalMs / this.count,
			p50Ms: this.quantile(0.5) ?? 0,
			p99Ms: this.quantile(0.99) ?? 0,
			maxMs: this.maxMs,
		};
	}
}

export interface LatencySnapshot {
	readonly count: number;
	readonly meanMs: number;
	readonly p50Ms: number;
	readonly p99Ms: number;
	readonly maxMs: number;
}

/** Counters of one owner, or of every owner of a host when shared. */
export class OwnerMetrics {
	/** keyed-state reads issued by Storage reads after open: the Session line (M3 gate: 0 in steady state). */
	sessionLineRemoteReads = 0;
	/** keyed-state reads issued by open (the `m/` read excluded): preload and the format/owner checks. */
	openRemoteReads = 0;
	/** Remote reads that met a transient outcome and were retried (§7.6). */
	remoteReadRetries = 0;
	/** Latency of every remote read, open and Session line. */
	readonly remoteReadLatency = new LatencyRecorder();
	/** Storages poisoned (each counts once, whatever poisoned it). */
	poisons = 0;
	/** Poisons by `FencedError`: another owner claimed the log. */
	fences = 0;
	/** Opens that lost a claim race (`OwnershipContention`). */
	contentions = 0;
	/** `fail-if-active` opens refused because the current owner is active (`OwnershipActive`). */
	activeRefusals = 0;
	/** Claims still ambiguous at their deadline (`ClaimTimeout`). */
	claimTimeouts = 0;
	/** Overlay alerts (§7.5: the overlay grew past 64 MiB). */
	overlayAlerts = 0;
	/** Commits that waited for the flush loop at the overlay hard cap (§7.5). */
	overlayCapWaits = 0;
}

/** `UrsulaStorage.metrics()`: the shared counters plus this storage's gauges. */
export interface OwnerMetricsSnapshot {
	readonly sessionLineRemoteReads: number;
	readonly openRemoteReads: number;
	readonly remoteReadRetries: number;
	readonly remoteReadLatency: LatencySnapshot;
	readonly poisons: number;
	readonly fences: number;
	readonly contentions: number;
	readonly activeRefusals: number;
	readonly claimTimeouts: number;
	readonly overlayAlerts: number;
	readonly overlayCapWaits: number;
	/** Bytes of the overlay `[E, tail)`: the pinned set (§7.2). 0 for the full-resident store. */
	readonly pinnedBytes: number;
	readonly pinnedRecords: number;
	/** Bytes of the range cache (§7.5). */
	readonly cacheBytes: number;
	/** `E` and the tail. */
	readonly overlayFloor: number;
	readonly tail: number;
	/** Flush-waits issued and retried, and floors raised (§7.9). */
	readonly flushWaits: number;
	readonly flushRetries: number;
	readonly floorsRaised: number;
}
