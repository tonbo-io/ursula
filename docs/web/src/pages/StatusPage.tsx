import { useEffect, useMemo, useRef, useState } from "react";

import Footer from "../components/Footer";
import Header from "../components/Header";

type StatusLevel = "operational" | "degraded_performance" | "partial_outage" | "major_outage" | "maintenance" | "unknown";

type ChaosNode = {
  name: string;
  role: "node" | "client" | string;
  instance_id?: string;
  instance_state?: string;
  service_state?: string;
  metrics_state?: string;
  accepted_appends?: number;
  applied_mutations?: number;
  leader_groups?: number;
  raft_groups?: number;
  last_error?: string | null;
};

type ChaosEvent = {
  time: string | null;
  level: "info" | "warn" | "error" | string;
  message: string;
};

type HealthHistoryPoint = {
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

type TopologyReplica = {
  node_id?: number | null;
  node_name?: string | null;
  role?: string | null;
  committed_index?: number | null;
  last_applied_index?: number | null;
};

type TopologyGroup = {
  raft_group_id: number;
  leader_id?: number | null;
  leader_name?: string | null;
  voter_ids?: number[];
  voter_names?: string[];
  learner_ids?: number[];
  replicas?: TopologyReplica[];
};

type ChaosTopology = {
  nodes?: Array<{
    node_id?: number | null;
    name?: string | null;
    instance_state?: string | null;
    metrics_state?: string | null;
    availability_zone?: string | null;
    region?: string | null;
  }>;
  raft_groups?: TopologyGroup[];
};

type ChaosInjection = {
  id: number;
  scenario?: string;
  expected_result?: string;
  allow_next_revert?: boolean;
  fault_apply_ok?: boolean | null;
  node_id?: string | number | null;
  node_name?: string | null;
  target_nodes?: string[];
  status?: string;
  stop_requested_at?: string | null;
  stopped_at?: string | null;
  injected_at?: string | null;
  start_requested_at?: string | null;
  recovered_at?: string | null;
  recover_after?: string | null;
  recovery_ms?: number | null;
  outage_ms?: number | null;
  recovery_slo_secs?: number | null;
  slo_met?: boolean | null;
  slo_missed_at?: string | null;
  timeline?: Array<{
    time: string | null;
    status: string;
    message: string;
  }>;
};

type ChaosCoverage = {
  scenario_count?: number;
  configured_count?: number;
  covered_count?: number;
  pending?: string[];
  scenarios?: Record<
    string,
    {
      configured?: boolean;
      attempts?: number;
      recovered?: number;
      detected?: number;
      failed?: number;
      active?: number;
      last_status?: string | null;
      last_run_at?: string | null;
    }
  >;
};

type WorkloadProbeCoverage = {
  covered?: boolean;
  passing?: boolean;
  enabled?: boolean;
  success?: number;
  errors?: number;
  attempts?: number;
  events?: number;
  bytes?: number;
  probe_success?: number;
  probe_errors?: number;
  background_publishes?: number;
  background_uploads?: number;
};

type WorkloadCoverage = {
  probes?: Record<string, WorkloadProbeCoverage>;
};

type ChaosStatus = {
  schema_version: number;
  overall: StatusLevel;
  started_at: string | null;
  updated_at: string | null;
  summary: string;
  health?: {
    expected_nodes?: number;
    expected_raft_groups?: number;
    running_nodes?: number;
    metrics_ok?: number;
    full_raft_nodes?: number;
    append_success_delta?: number | null;
    append_error_delta?: number | null;
    workload_progressing?: boolean;
    workload_clean?: boolean;
    quorum_healthy?: boolean;
    reasons?: string[];
  };
  history?: HealthHistoryPoint[];
  topology?: ChaosTopology;
  workload: {
    append_target_per_second: number;
    append_success_total: number;
    append_error_total: number;
    reader_success_total?: number;
    reader_error_total?: number;
    backpressure_probe_enabled?: boolean;
    producer_count?: number;
    payload_sizes?: number[];
    last_append_offset: number | null;
    stream?: string | null;
    stream_count?: number | null;
    coverage?: WorkloadCoverage;
  };
  integrity: {
    status: StatusLevel;
    checked_at: string | null;
    verified_offsets: number;
    mismatch_count: number;
    setsum_mismatch_count?: number;
    verify_counts?: Record<string, number>;
    verify_errors?: Record<string, number>;
    expected_live_setsum?: string;
    server?: {
      node?: string;
      live_setsum?: string | null;
      total_setsum?: string | null;
      evicted_records?: number | null;
      live_start_offset?: number | null;
      live_records?: number | null;
      total_records?: number | null;
    } | null;
    last_error: string | null;
  };
  chaos: {
    enabled: boolean;
    active_fault: string | null;
    last_fault: string | null;
    next_fault_after: string | null;
    fault_profile?: string;
    fault_scenarios?: string[];
    coverage?: ChaosCoverage;
    recovery_slo_secs?: number;
    injection_count?: number;
    injections?: ChaosInjection[];
  };
  nodes: ChaosNode[];
  events: ChaosEvent[];
};

const STATUS_URL =
  (import.meta.env.VITE_CHAOS_STATUS_URL as string | undefined) ||
  "https://ursula-chaos-status-tonbo.s3.amazonaws.com/status.json";
const REFRESH_OPTIONS = [
  { label: "10s", value: 10_000 },
  { label: "30s", value: 30_000 },
  { label: "1m", value: 60_000 },
  { label: "5m", value: 300_000 },
];
const HISTORY_LEGEND: Array<{ status: StatusLevel; label: string }> = [
  { status: "operational", label: "healthy" },
  { status: "degraded_performance", label: "fault active" },
  { status: "major_outage", label: "outage" },
  { status: "unknown", label: "unknown" },
];

function formatTime(value: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  });
}

function formatShortTime(value: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatRelative(value: string | null | undefined) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  if (Number.isNaN(ts)) return null;
  const diffMs = Date.now() - ts;
  if (diffMs < 0) return `in ${formatDuration(Math.abs(diffMs))}`;
  const seconds = Math.round(diffMs / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

function formatScheduleDelta(value: string | null | undefined) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  if (Number.isNaN(ts)) return null;
  const diffMs = ts - Date.now();
  return diffMs >= 0 ? `in ${formatDuration(diffMs)}` : `${formatDuration(Math.abs(diffMs))} overdue`;
}

function formatDuration(ms: number) {
  if (ms < 1000) return `${ms}ms`;
  const seconds = Math.round(ms / 1000);
  if (seconds < 90) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remSeconds = seconds % 60;
  if (minutes < 60) return remSeconds ? `${minutes}m ${remSeconds}s` : `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return remMinutes ? `${hours}h ${remMinutes}m` : `${hours}h`;
}

function formatRunningFor(ms: number): { primary: string; secondary: string | null } {
  if (ms < 60_000) {
    return { primary: `${Math.max(0, Math.floor(ms / 1000))}s`, secondary: null };
  }
  const minutes = Math.floor(ms / 60_000);
  if (minutes < 60) {
    return { primary: `${minutes}m`, secondary: null };
  }
  const hours = Math.floor(minutes / 60);
  const remMin = minutes % 60;
  if (hours < 24) {
    return { primary: `${hours}h`, secondary: remMin ? `${remMin}m` : null };
  }
  const days = Math.floor(hours / 24);
  const remHours = hours % 24;
  return { primary: `${days}d`, secondary: remHours ? `${remHours}h` : null };
}

function statusLabel(status: string) {
  return status.replace(/_/g, " ");
}

function StatusPill({ status, label }: { status: StatusLevel | string; label?: string }) {
  const normalized = status || "unknown";
  return (
    <span className={`status-pill status-pill-${normalized}`}>
      <span className="status-pill-dot" aria-hidden="true" />
      {label ?? statusLabel(normalized)}
    </span>
  );
}

function HistoryCell({ point, bucketLabel }: { point: HealthHistoryPoint; bucketLabel: string }) {
  const status = point.status || "unknown";
  const parts = [bucketLabel, statusLabel(status)];
  if (point.reasons?.length) parts.push(...point.reasons);
  return (
    <span
      aria-label={parts.join(" · ")}
      className={`history-day history-day-${status}`}
      title={parts.join(" · ")}
    />
  );
}

function numberValue(value: number | null | undefined) {
  return typeof value === "number" ? value.toLocaleString() : "-";
}

function formatBytesShort(value: number) {
  if (value < 1024) return `${value} B`;
  const units = ["KB", "MB", "GB"];
  let scaled = value / 1024;
  let unitIndex = 0;
  while (scaled >= 1024 && unitIndex < units.length - 1) {
    scaled /= 1024;
    unitIndex += 1;
  }
  const digits = scaled >= 10 || scaled === Math.floor(scaled) ? 0 : 1;
  return `${scaled.toFixed(digits)} ${units[unitIndex]}`;
}

const STATUS_RANK: Record<string, number> = {
  operational: 0,
  maintenance: 1,
  degraded_performance: 2,
  partial_outage: 3,
  major_outage: 4,
};

function statusWorse(a: string | null | undefined, b: string | null | undefined): string {
  const aRank = STATUS_RANK[a ?? ""] ?? -1;
  const bRank = STATUS_RANK[b ?? ""] ?? -1;
  return aRank >= bRank ? (a ?? "unknown") : (b ?? "unknown");
}

function bucketHistory(
  history: HealthHistoryPoint[],
  bucketMs: number,
  bucketCount: number,
  nowMs: number,
): HealthHistoryPoint[] {
  const cells: Array<HealthHistoryPoint & { sampleCount: number }> = [];
  for (let i = bucketCount - 1; i >= 0; i--) {
    const end = nowMs - i * bucketMs;
    const start = end - bucketMs;
    cells.push({
      time: new Date(end).toISOString(),
      status: "unknown",
      sampleCount: 0,
    });
    const bucket = cells[cells.length - 1];
    for (const point of history) {
      const ts = point.time ? new Date(point.time).getTime() : NaN;
      if (Number.isNaN(ts) || ts < start || ts >= end) continue;
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

function appendRateSamples(history: HealthHistoryPoint[]): Array<{ time: string | null; rate: number }> {
  const samples: Array<{ time: string | null; rate: number }> = [];
  for (let i = 1; i < history.length; i++) {
    const prev = history[i - 1];
    const cur = history[i];
    const delta = cur.append_success_delta;
    if (typeof delta !== "number") continue;
    const prevTime = new Date(prev.time ?? "").getTime();
    const curTime = new Date(cur.time ?? "").getTime();
    if (Number.isNaN(prevTime) || Number.isNaN(curTime) || curTime <= prevTime) continue;
    samples.push({ time: cur.time, rate: delta / ((curTime - prevTime) / 1000) });
  }
  return samples;
}

function isStatusStale(status: ChaosStatus | null, refreshMs: number) {
  if (!status?.updated_at) return false;
  const updatedAt = new Date(status.updated_at).getTime();
  if (Number.isNaN(updatedAt)) return false;
  return Date.now() - updatedAt > Math.max(90_000, refreshMs * 3);
}

function injectionDurationMs(injection: ChaosInjection): number | null {
  const start = injection.stopped_at ?? injection.stop_requested_at;
  const end = injection.recovered_at ?? null;
  if (!start || !end) return null;
  const startMs = new Date(start).getTime();
  const endMs = new Date(end).getTime();
  if (Number.isNaN(startMs) || Number.isNaN(endMs) || endMs < startMs) return null;
  return endMs - startMs;
}

function injectionPill(injection: ChaosInjection): { status: StatusLevel; label: string } {
  switch (injection.status) {
    case "recovered":
      return { status: "operational", label: "recovered" };
    case "detected":
      return { status: "maintenance", label: "detected" };
    case "stopping":
    case "stopped":
    case "starting":
      return { status: "maintenance", label: injection.status };
    default:
      return { status: "degraded_performance", label: injection.status ?? "in progress" };
  }
}

type PhaseKey = "stopping" | "down" | "recovering";

function injectionPhases(injection: ChaosInjection): Array<{ key: PhaseKey; label: string; ms: number }> {
  const segments: Array<{ key: PhaseKey; label: string; from: string | null; to: string | null }> = [
    { key: "stopping", label: "stopping", from: injection.stop_requested_at ?? null, to: injection.stopped_at ?? null },
    { key: "down", label: "down", from: injection.stopped_at ?? null, to: injection.start_requested_at ?? null },
    { key: "recovering", label: "recovering", from: injection.start_requested_at ?? null, to: injection.recovered_at ?? null },
  ];
  const out: Array<{ key: PhaseKey; label: string; ms: number }> = [];
  for (const seg of segments) {
    if (!seg.from || !seg.to) continue;
    const fromMs = new Date(seg.from).getTime();
    const toMs = new Date(seg.to).getTime();
    if (Number.isNaN(fromMs) || Number.isNaN(toMs) || toMs <= fromMs) continue;
    out.push({ key: seg.key, label: seg.label, ms: toMs - fromMs });
  }
  return out;
}

function probeCount(probe: WorkloadProbeCoverage) {
  return probe.success ?? probe.probe_success ?? probe.events ?? probe.attempts ?? probe.background_publishes ?? 0;
}

function probeLabel(name: string) {
  switch (name) {
    case "cold_write_backpressure":
      return "cold backpressure";
    case "producer_semantics":
      return "producer semantics";
    case "read_availability":
      return "read availability";
    case "cold_flush":
      return "cold flush";
    default:
      return name.replace(/_/g, " ");
  }
}

function eventLevelClass(level: string) {
  switch (level) {
    case "error":
      return "status-event-error";
    case "warn":
      return "status-event-warn";
    default:
      return "status-event-info";
  }
}

function Sparkline({
  samples,
  target,
}: {
  samples: Array<{ time: string | null; rate: number }>;
  target: number;
}) {
  if (samples.length < 2) {
    return <div className="status-sparkline-empty">No rate samples yet.</div>;
  }
  const width = 600;
  const height = 72;
  const peak = Math.max(target, ...samples.map((s) => s.rate)) || 1;
  const ceiling = peak * 1.1;
  const stepX = width / (samples.length - 1);
  const points = samples
    .map((s, i) => `${(i * stepX).toFixed(2)},${(height - (s.rate / ceiling) * height).toFixed(2)}`)
    .join(" ");
  const targetY = target > 0 ? height - (target / ceiling) * height : null;
  return (
    <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" className="status-sparkline">
      {targetY != null ? (
        <line x1={0} x2={width} y1={targetY} y2={targetY} className="status-sparkline-target" />
      ) : null}
      <polyline points={points} className="status-sparkline-line" />
    </svg>
  );
}

function PhaseBar({ phases }: { phases: Array<{ key: PhaseKey; label: string; ms: number }> }) {
  if (phases.length === 0) {
    return <div className="injection-phases injection-phases-pending">awaiting first event</div>;
  }
  return (
    <div className="injection-phases">
      {phases.map((phase, i) => (
        <div
          className={`injection-phase injection-phase-${phase.key}`}
          key={`${phase.key}-${i}`}
          style={{ flex: phase.ms }}
          title={`${phase.label} · ${formatDuration(phase.ms)}`}
        >
          <span className="injection-phase-duration">{formatDuration(phase.ms)}</span>
        </div>
      ))}
    </div>
  );
}

function TopologyCanvas({ topology }: { topology?: ChaosTopology }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container) return;

    const draw = () => {
      const rect = container.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      const width = Math.max(320, Math.floor(rect.width));
      const height = width < 620 ? 440 : 340;
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      canvas.style.height = `${height}px`;

      const ctx = canvas.getContext("2d");
      if (!ctx) return;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);

      const muted = "#928374";
      const text = "#f9f5d7";
      const secondary = "#d5c4a1";
      const voter = "rgba(168, 153, 132, 0.32)";
      const danger = "#fb4934";
      const groups = topology?.raft_groups ?? [];
      const nodes = (topology?.nodes ?? []).filter((node) => typeof node.node_id === "number");

      if (nodes.length === 0 || groups.length === 0) {
        ctx.fillStyle = muted;
        ctx.font = "13px IBM Plex Sans, sans-serif";
        ctx.fillText("Topology appears after the chaos agent publishes Raft metrics.", 18, 32);
        return;
      }

      const nodeWatermarks = new Map<number, number>();
      groups.forEach((group) => {
        (group.replicas ?? []).forEach((replica) => {
          if (typeof replica.node_id !== "number" || typeof replica.last_applied_index !== "number") {
            return;
          }
          nodeWatermarks.set(
            replica.node_id,
            Math.max(nodeWatermarks.get(replica.node_id) ?? 0, replica.last_applied_index),
          );
        });
      });

      const nodePositions = new Map<
        number,
        { x: number; y: number; label: string; watermark: number | null; az: string | null }
      >();
      const nodeLayout =
        width < 620
          ? [
              { x: width * 0.5, y: 56 },
              { x: width * 0.2, y: height - 72 },
              { x: width * 0.8, y: height - 72 },
            ]
          : [
              { x: 92, y: 64 },
              { x: width - 92, y: 64 },
              { x: width / 2, y: height - 64 },
            ];
      nodes
        .slice()
        .sort((a, b) => (a.node_id ?? 0) - (b.node_id ?? 0))
        .forEach((node, index) => {
          const position = nodeLayout[index] ?? {
            x: width / 2 + Math.cos(index) * 120,
            y: height / 2 + Math.sin(index) * 120,
          };
          nodePositions.set(node.node_id as number, {
            x: position.x,
            y: position.y,
            label: `node ${node.node_id}`,
            watermark: nodeWatermarks.get(node.node_id as number) ?? null,
            az: typeof node.availability_zone === "string" ? node.availability_zone : null,
          });
        });

      const groupPositions = groups.map((group, index) => {
        const columns = width < 620 ? 2 : Math.min(3, Math.max(1, groups.length));
        const rows = Math.ceil(groups.length / columns);
        const col = index % columns;
        const row = Math.floor(index / columns);
        const startX = width * 0.3;
        const endX = width * 0.7;
        const x = columns === 1 ? width / 2 : startX + ((endX - startX) * col) / (columns - 1);
        const y = height * 0.36 + (row - (rows - 1) / 2) * 52;
        return { group, x, y };
      });

      const nodeRadius = 24;
      const groupRadius = 14;

      ctx.lineCap = "round";
      groupPositions.forEach(({ group, x, y }) => {
        (group.replicas ?? []).forEach((replica) => {
          const nodeId = replica.node_id;
          if (typeof nodeId !== "number") return;
          const node = nodePositions.get(nodeId);
          if (!node) return;
          const isLeader = replica.role === "leader";
          const dx = node.x - x;
          const dy = node.y - y;
          const dist = Math.hypot(dx, dy) || 1;
          const ux = dx / dist;
          const uy = dy / dist;
          const startX = x + ux * groupRadius;
          const startY = y + uy * groupRadius;
          const endX = node.x - ux * nodeRadius;
          const endY = node.y - uy * nodeRadius;
          ctx.strokeStyle = isLeader ? "rgba(184, 187, 38, 0.7)" : voter;
          ctx.lineWidth = isLeader ? 1.6 : 0.8;
          ctx.beginPath();
          ctx.moveTo(startX, startY);
          const midY = (startY + endY) / 2;
          ctx.bezierCurveTo(startX, midY, endX, midY, endX, endY);
          ctx.stroke();
        });
      });

      ctx.textAlign = "center";
      nodePositions.forEach((node) => {
        ctx.fillStyle = text;
        ctx.font = "600 13px IBM Plex Sans, sans-serif";
        ctx.fillText(node.label, node.x, node.y - 10);
        ctx.fillStyle = secondary;
        ctx.font = "11px IBM Plex Mono, monospace";
        ctx.fillText(
          node.watermark == null ? "applied -" : `applied ${node.watermark.toLocaleString()}`,
          node.x,
          node.y + 4,
        );
        if (node.az) {
          ctx.fillStyle = muted;
          ctx.font = "11px IBM Plex Mono, monospace";
          ctx.fillText(node.az, node.x, node.y + 18);
        }
      });

      groupPositions.forEach(({ group, x, y }) => {
        ctx.fillStyle = group.leader_id == null ? danger : text;
        ctx.font = "600 13px IBM Plex Sans, sans-serif";
        ctx.fillText(`g${group.raft_group_id}`, x, y - 2);
        ctx.fillStyle = muted;
        ctx.font = "11px IBM Plex Mono, monospace";
        ctx.fillText(
          group.leader_name ? `L n${group.leader_id}` : "no leader",
          x,
          y + 14,
        );
      });
    };

    draw();
    const resizeObserver = new ResizeObserver(draw);
    resizeObserver.observe(container);
    return () => resizeObserver.disconnect();
  }, [topology]);

  return (
    <div className="topology-canvas-wrap" ref={containerRef}>
      <canvas
        aria-label="Raft topology diagram"
        className="topology-canvas"
        ref={canvasRef}
      />
    </div>
  );
}

function StatusPage() {
  const [status, setStatus] = useState<ChaosStatus | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [refreshMs, setRefreshMs] = useState(30_000);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    let closed = false;

    async function load() {
      try {
        const response = await fetch(`${STATUS_URL}?t=${Date.now()}`, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`status endpoint returned ${response.status}`);
        }
        const nextStatus = (await response.json()) as ChaosStatus;
        if (!closed) {
          setStatus(nextStatus);
          setLoadError(null);
          setNow(Date.now());
        }
      } catch (error) {
        if (!closed) {
          setLoadError(error instanceof Error ? error.message : String(error));
        }
      }
    }

    void load();
    const timer = window.setInterval(() => void load(), refreshMs);
    return () => {
      closed = true;
      window.clearInterval(timer);
    };
  }, [refreshMs]);

  useEffect(() => {
    const tick = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(tick);
  }, []);

  const stale = isStatusStale(status, refreshMs);
  const displayedOverall = stale ? "major_outage" : status?.overall ?? "unknown";
  const HEALTH_BUCKET_MS = 60 * 60 * 1000;
  const HEALTH_BUCKET_COUNT = 7 * 24;
  const healthHistory = useMemo(
    () => bucketHistory(status?.history ?? [], HEALTH_BUCKET_MS, HEALTH_BUCKET_COUNT, now),
    [status, now],
  );
  const rateSamples = useMemo(() => appendRateSamples(status?.history ?? []).slice(-80), [status]);
  const injections = useMemo(
    () => status?.chaos.injections?.slice().reverse().slice(0, 3) ?? [],
    [status],
  );
  const events = useMemo(() => status?.events?.slice().reverse().slice(0, 10) ?? [], [status]);

  const updatedRelative = formatRelative(status?.updated_at);
  void now;

  const startedAtMs = status?.started_at ? new Date(status.started_at).getTime() : NaN;
  const runtime = Number.isFinite(startedAtMs) ? formatRunningFor(Math.max(0, now - startedAtMs)) : null;

  const chaosActive = Boolean(status?.chaos.enabled && status?.chaos.active_fault);
  const heroPillLabel =
    !stale &&
    chaosActive &&
    (displayedOverall === "degraded_performance" || displayedOverall === "partial_outage")
      ? "fault active"
      : undefined;

  const expectedNodes = status?.health?.expected_nodes;
  const runningNodes = status?.health?.running_nodes;
  const nodeSummary =
    typeof runningNodes === "number" && typeof expectedNodes === "number" && expectedNodes > 0
      ? `${runningNodes} / ${expectedNodes}`
      : "-";
  const streamCount = status?.workload.stream_count ?? null;
  const recentRate = rateSamples.length > 0 ? Math.round(rateSamples[rateSamples.length - 1].rate) : null;
  const producerCount = status?.workload.producer_count ?? null;
  const payloadSizes = status?.workload.payload_sizes ?? null;
  const payloadRange = useMemo(() => {
    if (!payloadSizes || payloadSizes.length === 0) return null;
    const min = Math.min(...payloadSizes);
    const max = Math.max(...payloadSizes);
    return min === max ? formatBytesShort(min) : `${formatBytesShort(min)}..${formatBytesShort(max)}`;
  }, [payloadSizes]);
  const coverageScenarios = Object.entries(status?.chaos.coverage?.scenarios ?? {}).sort(
    ([left], [right]) => left.localeCompare(right),
  );
  const verifyModes = useMemo(() => {
    const ok = status?.integrity.verify_counts ?? {};
    const errors = status?.integrity.verify_errors ?? {};
    const names = Array.from(new Set([...Object.keys(ok), ...Object.keys(errors)])).sort();
    return names.map((name) => ({
      name,
      ok: ok[name] ?? 0,
      errors: errors[name] ?? 0,
    }));
  }, [status]);
  const workloadProbes = useMemo(() => {
    return Object.entries(status?.workload.coverage?.probes ?? {}).sort(([left], [right]) =>
      left.localeCompare(right),
    );
  }, [status]);
  const topologyPlacement = useMemo(() => {
    const regions = new Set<string>();
    const azs = new Set<string>();
    for (const node of status?.topology?.nodes ?? []) {
      if (typeof node.region === "string" && node.region) regions.add(node.region);
      if (typeof node.availability_zone === "string" && node.availability_zone) azs.add(node.availability_zone);
    }
    if (regions.size === 0 && azs.size === 0) return null;
    if (regions.size === 1) {
      const [region] = regions;
      return `${region} · ${azs.size} AZ${azs.size === 1 ? "" : "s"}`;
    }
    if (regions.size > 1) return `${regions.size} regions · ${azs.size} AZs`;
    return `${azs.size} AZ${azs.size === 1 ? "" : "s"}`;
  }, [status]);

  return (
    <>
      <Header
        navItems={[
          { label: "Docs", href: "/docs" },
          { label: "Blog", href: "/blog" },
          { label: "Benchmark", href: "/benchmark" },
          { label: "Chaos Test", href: "/chaos-test", active: true },
        ]}
        version={__URSULA_VERSION__}
        githubUrl="https://github.com/tonbo-io/ursula"
      />

      <main className="status-page">
        <section className="status-hero">
          <div className="status-hero-top">
            <div className="status-hero-title">
              <div className="status-brand">24/7 reliability test</div>
              <h1>Chaos Test</h1>
            </div>
            <div className="status-hero-pill">
              {runtime ? (
                <div className="status-hero-runtime" title={`started ${formatTime(status?.started_at ?? null)}`}>
                  <span className="status-hero-runtime-label">continuously running for</span>
                  <span className="status-hero-runtime-value">
                    {runtime.primary}
                    {runtime.secondary ? <span className="status-hero-runtime-sub">{runtime.secondary}</span> : null}
                  </span>
                </div>
              ) : null}
              <StatusPill status={displayedOverall} label={heroPillLabel} />
            </div>
          </div>
          <p className="status-summary">
            {status?.summary ?? "Waiting for the EC2 chaos runner to publish live test data."}
          </p>
          <p className="status-hero-blurb">
            A 3-node Ursula cluster runs on EC2 around the clock. The runner mixes writes, readers,
            old-offset checks, producer fencing probes, burst phases, and scheduled fault scenarios.
            The page reports what is covered by the current run and whether the cluster returns to
            full health inside the recovery SLO.
          </p>
          <div className="status-hero-controls">
            <div className="status-hero-meta">
              {streamCount ? <>{streamCount.toLocaleString()} streams</> : "-"}
              {producerCount ? <> · {producerCount.toLocaleString()} producers</> : null}
              {payloadRange ? <> · payloads {payloadRange}</> : null}
              {status?.chaos.recovery_slo_secs ? <> · recovery SLO {status.chaos.recovery_slo_secs}s</> : null}
            </div>
            <div className="status-hero-refresh">
              {updatedRelative ? (
                <span className="status-updated-relative" title={formatTime(status?.updated_at ?? null)}>
                  updated {updatedRelative}
                </span>
              ) : null}
              <label className="status-refresh-control">
                <span>Refresh</span>
                <select
                  aria-label="Chaos test refresh interval"
                  className="status-refresh-select"
                  value={refreshMs}
                  onChange={(event) => setRefreshMs(Number(event.target.value))}
                >
                  {REFRESH_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          </div>
        </section>

        {stale ? (
          <div className="status-warning">Status feed stale. Last update {updatedRelative ?? "long ago"}.</div>
        ) : null}
        {loadError ? <div className="status-warning">Refresh failed: {loadError}</div> : null}

        <section className="status-section">
          <div className="status-section-heading">
            <h2>
              Health
              <span className="status-section-subtitle">1 h per bar</span>
            </h2>
            <div className="status-history-legend" role="list">
              {HISTORY_LEGEND.map((entry) => (
                <span className="status-history-legend-item" key={entry.status} role="listitem">
                  <span aria-hidden="true" className={`status-history-legend-swatch history-day-${entry.status}`} />
                  {entry.label}
                </span>
              ))}
            </div>
          </div>
          {healthHistory.length === 0 ? (
            <div className="status-empty">No samples yet.</div>
          ) : (
            <>
              <div className="history-grid status-history-grid">
                {healthHistory.map((point, index) => {
                  const end = point.time ? new Date(point.time) : null;
                  const start = end ? new Date(end.getTime() - HEALTH_BUCKET_MS) : null;
                  const bucketLabel =
                    start && end
                      ? `${start.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })}–${end.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })}`
                      : "-";
                  return (
                    <HistoryCell
                      bucketLabel={bucketLabel}
                      key={`${point.time ?? "sample"}-${index}`}
                      point={point}
                    />
                  );
                })}
              </div>
              <div className="status-history-axis">
                <span>7d ago</span>
                <span>now</span>
              </div>
            </>
          )}
        </section>

        <section className="status-section">
          <div className="status-section-heading">
            <h2>
              Topology
              {topologyPlacement ? (
                <span className="status-section-subtitle">{topologyPlacement}</span>
              ) : null}
            </h2>
            <div className="topology-legend">
              <span>
                <span className="topology-legend-line topology-legend-line-leader" aria-hidden="true" />
                leader
              </span>
              <span>
                <span className="topology-legend-line topology-legend-line-voter" aria-hidden="true" />
                voter
              </span>
              <span>
                <span className="topology-legend-swatch topology-legend-swatch-noleader" aria-hidden="true" />
                no leader
              </span>
            </div>
          </div>
          <TopologyCanvas topology={status?.topology} />
        </section>

        <section className="status-section">
          <div className="status-section-heading">
            <h2>Workload</h2>
            <div className="status-section-stats">
              <span>
                <em>nodes</em>
                {nodeSummary}
              </span>
              <span>
                <em>target</em>
                {numberValue(status?.workload.append_target_per_second)}/s
              </span>
              <span>
                <em>recent</em>
                {recentRate == null ? "-" : `${recentRate.toLocaleString()}/s`}
              </span>
              <span>
                <em>total</em>
                {numberValue(status?.workload.append_success_total)}
              </span>
              <span className={
                (status?.workload.append_error_total ?? 0) > 0 ? "status-section-stat-bad" : undefined
              }>
                <em>errors</em>
                {numberValue(status?.workload.append_error_total)}
              </span>
              <span>
                <em>reads</em>
                {numberValue(status?.workload.reader_success_total)}
              </span>
              <span className={
                (status?.workload.reader_error_total ?? 0) > 0 ? "status-section-stat-bad" : undefined
              }>
                <em>read errors</em>
                {numberValue(status?.workload.reader_error_total)}
              </span>
            </div>
          </div>
          <div className="status-sparkline-wrap">
            <Sparkline samples={rateSamples} target={status?.workload.append_target_per_second ?? 0} />
          </div>
          {workloadProbes.length > 0 ? (
            <div className="workload-coverage-row" role="list" aria-label="workload coverage">
              {workloadProbes.map(([name, probe]) => {
                const disabled = probe.enabled === false;
                const state = disabled
                  ? "disabled"
                  : probe.passing === false
                    ? "failed"
                    : probe.covered
                      ? "covered"
                      : "pending";
                const count = probeCount(probe);
                const errorCount = probe.errors ?? probe.probe_errors ?? 0;
                const title = disabled
                  ? `${probeLabel(name)} is disabled in this run`
                  : `${probeLabel(name)}: ${probe.covered ? "covered" : "not covered"}${errorCount ? ` · ${errorCount} errors` : ""}`;
                return (
                  <span
                    className={`workload-probe workload-probe-${state}`}
                    key={name}
                    role="listitem"
                    title={title}
                  >
                    <span aria-hidden="true" className={`workload-probe-dot workload-probe-dot-${state}`} />
                    {probeLabel(name)}
                    {count > 0 ? <em>{count.toLocaleString()}</em> : null}
                  </span>
                );
              })}
            </div>
          ) : null}
        </section>

        <section className="status-section">
          <div className="status-section-heading">
            <h2>Integrity</h2>
            <div className="status-section-stats">
              <StatusPill status={status?.integrity.status ?? "unknown"} />
              <span>
                <em>verified</em>
                {numberValue(status?.integrity.verified_offsets)}
              </span>
              <span className={
                (status?.integrity.setsum_mismatch_count ?? 0) > 0 ? "status-section-stat-bad" : undefined
              }>
                <em>mismatches</em>
                {numberValue(status?.integrity.setsum_mismatch_count)}
              </span>
              <span>
                <em>checked</em>
                {formatRelative(status?.integrity.checked_at) ?? "-"}
              </span>
            </div>
          </div>
          {status?.integrity.last_error ? (
            <div className="status-callout">{status.integrity.last_error}</div>
          ) : null}
          {verifyModes.length > 0 ? (
            <div className="integrity-modes-row" role="list" aria-label="read check modes">
              {verifyModes.map((mode) => {
                const bad = mode.errors > 0;
                return (
                  <span
                    className={`integrity-mode${bad ? " integrity-mode-bad" : ""}`}
                    key={mode.name}
                    role="listitem"
                    title={`${mode.name.replace(/_/g, " ")}: ${mode.ok.toLocaleString()} ok${bad ? `, ${mode.errors.toLocaleString()} errors` : ""}`}
                  >
                    <span
                      aria-hidden="true"
                      className={`integrity-mode-dot ${bad ? "integrity-mode-dot-bad" : "integrity-mode-dot-ok"}`}
                    />
                    <span className="integrity-mode-name">{mode.name.replace(/_/g, " ")}</span>
                    <em>{mode.ok.toLocaleString()}</em>
                    {bad ? <strong>{mode.errors.toLocaleString()} err</strong> : null}
                  </span>
                );
              })}
            </div>
          ) : null}
        </section>

        <section className="status-section">
          <div className="status-section-heading">
            <h2>Fault injection</h2>
            <div className="status-section-stats">
              <span className={status?.chaos.enabled ? undefined : "status-section-stat-dim"}>
                <em>state</em>
                {status?.chaos.enabled ? "enabled" : "disabled"}
              </span>
              <span>
                <em>profile</em>
                {status?.chaos.fault_profile ?? "custom"}
              </span>
              <span>
                <em>runs</em>
                {numberValue(status?.chaos.injection_count)}
              </span>
              <span title={formatTime(status?.chaos.next_fault_after ?? null)}>
                <em>next</em>
                {formatScheduleDelta(status?.chaos.next_fault_after) ?? "-"}
              </span>
              <span className="injection-phase-legend" aria-label="phase legend">
                <span className="injection-phase injection-phase-stopping" aria-hidden="true" />
                stopping
                <span className="injection-phase injection-phase-down" aria-hidden="true" />
                down
                <span className="injection-phase injection-phase-recovering" aria-hidden="true" />
                recovering
              </span>
            </div>
          </div>
          {coverageScenarios.length > 0 ? (
            <div className="fault-coverage-row" role="list" aria-label="fault scenario coverage">
              {coverageScenarios.map(([scenario, entry]) => {
                const attempts = entry.attempts ?? 0;
                const failed = entry.failed ?? 0;
                const active = entry.active ?? 0;
                const recovered = entry.recovered ?? 0;
                const detected = entry.detected ?? 0;
                const state =
                  failed > 0
                    ? "failed"
                    : active > 0
                      ? "active"
                      : attempts > 0
                        ? "covered"
                        : "pending";
                const tooltip =
                  attempts === 0
                    ? `${scenario.replace(/_/g, " ")}: not yet exercised`
                    : `${scenario.replace(/_/g, " ")}: ${attempts} run${attempts === 1 ? "" : "s"} · ${recovered} recovered · ${detected} detected · ${failed} failed${active ? ` · ${active} active` : ""}`;
                return (
                  <span
                    className={`fault-coverage-pill fault-coverage-pill-${state}`}
                    key={scenario}
                    role="listitem"
                    title={tooltip}
                  >
                    <span
                      aria-hidden="true"
                      className={`fault-coverage-dot fault-coverage-dot-${state}`}
                    />
                    {scenario.replace(/_/g, " ")}
                    {attempts > 0 ? <em>{attempts}</em> : null}
                  </span>
                );
              })}
            </div>
          ) : null}
          <div className="injection-list">
            {injections.length > 0 ? (
              injections.map((injection) => {
                const pill = injectionPill(injection);
                const durationMs = injectionDurationMs(injection);
                const phases = injectionPhases(injection);
                const targetLabel = injection.target_nodes?.length
                  ? injection.target_nodes.join(", ")
                  : injection.node_name ?? `node ${injection.node_id ?? "-"}`;
                return (
                  <article className="injection-item" key={injection.id}>
                    <div className="injection-item-header">
                      <div className="injection-item-title">
                        <span className="injection-id">#{injection.id}</span>
                        {injection.scenario ? <span>{injection.scenario.replace(/_/g, " ")}</span> : null}
                        <span className="injection-target-name">{targetLabel}</span>
                        {injection.expected_result === "revert_detection" ? <span>revert detection</span> : null}
                        {durationMs != null ? (
                          <span className="injection-duration">{formatDuration(durationMs)}</span>
                        ) : null}
                        {typeof injection.slo_met === "boolean" ? (
                          <span className={injection.slo_met ? undefined : "status-section-stat-bad"}>
                            SLO {injection.slo_met ? "met" : "missed"}
                          </span>
                        ) : null}
                      </div>
                      <StatusPill status={pill.status} label={pill.label} />
                    </div>
                    <PhaseBar phases={phases} />
                    {(injection.timeline?.length ?? 0) > 0 ? (
                      <details className="injection-timeline-details">
                        <summary>Timeline</summary>
                        <div className="injection-timeline">
                          {(injection.timeline ?? []).map((event, index) => (
                            <div className="injection-timeline-step" key={`${event.time ?? "event"}-${index}`}>
                              <span>{event.status}</span>
                              <time>{formatShortTime(event.time)}</time>
                              <p>{event.message}</p>
                            </div>
                          ))}
                        </div>
                      </details>
                    ) : null}
                  </article>
                );
              })
            ) : (
              <div className="injection-empty">
                No injection has run yet. Next fault is scheduled for{" "}
                <time title={formatTime(status?.chaos.next_fault_after ?? null)}>
                  {formatTime(status?.chaos.next_fault_after ?? null)}
                </time>
                .
              </div>
            )}
          </div>
        </section>

        <section className="status-section">
          <div className="status-section-heading">
            <h2>Events</h2>
          </div>
          <div className="status-event-list">
            {events.length > 0 ? (
              events.map((event, index) => (
                <div
                  className={`status-event ${eventLevelClass(event.level)}`}
                  key={`${event.time ?? "event"}-${index}`}
                  title={formatTime(event.time)}
                >
                  <span className="status-event-dot" aria-hidden="true" />
                  <time>{formatShortTime(event.time)}</time>
                  <p>{event.message}</p>
                </div>
              ))
            ) : (
              <div className="status-empty">No events recorded yet.</div>
            )}
          </div>
        </section>
      </main>

      <Footer />
    </>
  );
}

export default StatusPage;
