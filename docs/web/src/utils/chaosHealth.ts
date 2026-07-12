// Shared health-history bucketing for the chaos test — used by both the
// chaos test page and the homepage strip so the two instruments can never
// disagree on the same data.

export type StatusLevel =
  | "operational"
  | "maintenance"
  | "degraded_performance"
  | "partial_outage"
  | "major_outage"
  | "unknown";

export type HealthHistoryPoint = {
  time: string | null;
  status: StatusLevel | string;
  running_nodes?: number;
  metrics_ok?: number;
  full_raft_nodes?: number;
  append_success_delta?: number | null;
  append_error_delta?: number | null;
  integrity_status?: StatusLevel | string;
  active_fault?: string | null;
  reasons?: string[];
};

export type HealthHistoryCell = HealthHistoryPoint & {
  bucket_start_time: string | null;
};

export const STATUS_RANK: Record<string, number> = {
  operational: 0,
  maintenance: 1,
  degraded_performance: 2,
  partial_outage: 3,
  major_outage: 4,
};

export function statusWorse(a: string | null | undefined, b: string | null | undefined): string {
  const aRank = STATUS_RANK[a ?? ""] ?? -1;
  const bRank = STATUS_RANK[b ?? ""] ?? -1;
  return aRank >= bRank ? (a ?? "unknown") : (b ?? "unknown");
}

export function bucketHistory(
  history: HealthHistoryPoint[],
  bucketMs: number,
  maxBucketCount: number,
  nowMs: number,
): HealthHistoryCell[] {
  const currentBucketStart = Math.floor(nowMs / bucketMs) * bucketMs;
  const cells: Array<HealthHistoryCell & { sampleCount: number }> = [];
  for (let i = maxBucketCount - 1; i >= 0; i--) {
    const start = currentBucketStart - i * bucketMs;
    const end = i === 0 ? nowMs : start + bucketMs;
    cells.push({
      bucket_start_time: new Date(start).toISOString(),
      time: new Date(end).toISOString(),
      status: "unknown",
      sampleCount: 0,
    });
    const bucket = cells[cells.length - 1];
    for (const point of history) {
      const tsText = point.time ?? "";
      const ts = tsText ? new Date(tsText).getTime() : NaN;
      if (Number.isNaN(ts)) continue;
      const isHourlySummary = /^\d{4}-\d{2}-\d{2}T\d{2}:00:00Z$/.test(tsText);
      const belongsToBucket = isHourlySummary ? ts > start && ts <= end : ts >= start && ts < end;
      if (!belongsToBucket) continue;
      if (bucket.sampleCount === 0) {
        bucket.status = point.status;
        bucket.reasons = point.reasons;
      } else {
        const winner = statusWorse(bucket.status, point.status);
        if (winner !== bucket.status) {
          bucket.status = winner;
          bucket.reasons = point.reasons;
        }
      }
      bucket.sampleCount += 1;
    }
  }
  return cells;
}
