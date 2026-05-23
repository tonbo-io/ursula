import { useEffect, useMemo, useState } from "react";

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

type ChaosStatus = {
  schema_version: number;
  overall: StatusLevel;
  started_at: string | null;
  updated_at: string | null;
  summary: string;
  workload: {
    append_target_per_second: number;
    append_success_total: number;
    append_error_total: number;
    last_append_offset: number | null;
  };
  integrity: {
    status: StatusLevel;
    checked_at: string | null;
    verified_offsets: number;
    mismatch_count: number;
    last_error: string | null;
  };
  chaos: {
    enabled: boolean;
    active_fault: string | null;
    last_fault: string | null;
    next_fault_after: string | null;
  };
  nodes: ChaosNode[];
  events: ChaosEvent[];
};

const STATUS_URL =
  (import.meta.env.VITE_CHAOS_STATUS_URL as string | undefined) ||
  "https://ursula-chaos-status-tonbo.s3.amazonaws.com/status.json";

function formatTime(value: string | null) {
  if (!value) return "not published";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  });
}

function statusLabel(status: string) {
  return status.replace(/_/g, " ");
}

function StatusPill({ status }: { status: StatusLevel | string }) {
  const normalized = status || "unknown";
  return (
    <span className={`status-pill status-pill-${normalized}`}>
      <span className="status-pill-dot" aria-hidden="true" />
      {statusLabel(normalized)}
    </span>
  );
}

function numberValue(value: number | null | undefined) {
  return typeof value === "number" ? value.toLocaleString() : "-";
}

function StatusPage() {
  const [status, setStatus] = useState<ChaosStatus | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

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
        }
      } catch (error) {
        if (!closed) {
          setLoadError(error instanceof Error ? error.message : String(error));
        }
      }
    }

    void load();
    const timer = window.setInterval(() => void load(), 30_000);
    return () => {
      closed = true;
      window.clearInterval(timer);
    };
  }, []);

  const recentEvents = useMemo(() => status?.events.slice(0, 8) ?? [], [status]);

  return (
    <>
      <Header
        navItems={[
          { label: "Docs", href: "/docs" },
          { label: "Blog", href: "/blog" },
          { label: "Benchmark", href: "/benchmark" },
          { label: "Status", href: "/status", active: true },
        ]}
        version={__URSULA_VERSION__}
        githubUrl="https://github.com/tonbo-io/ursula"
      />

      <main className="status-page">
        <section className="status-hero">
          <div className="status-brand">Ursula 24/7 chaos test</div>
          <div className="status-hero-main">
            <div>
              <h1>{status?.summary ?? "Loading chaos status"}</h1>
              <p>
                Continuous client load against a three-node EC2 cluster with randomized single-node
                stop and recovery cycles.
              </p>
            </div>
            <StatusPill status={status?.overall ?? "unknown"} />
          </div>
          <dl className="status-timestamps">
            <div>
              <dt>Started</dt>
              <dd>{formatTime(status?.started_at ?? null)}</dd>
            </div>
            <div>
              <dt>Updated</dt>
              <dd>{formatTime(status?.updated_at ?? null)}</dd>
            </div>
          </dl>
        </section>

        {loadError ? <div className="status-warning">Unable to refresh status: {loadError}</div> : null}

        <section className="status-section">
          <div className="status-section-heading">
            <h2>Cluster</h2>
            <p>{status?.nodes.length ?? 0} instances</p>
          </div>
          <div className="component-table">
            <div className="component-row component-row-header">
              <div>Component</div>
              <div>Instance</div>
              <div>Service</div>
              <div>Raft</div>
              <div>Writes</div>
            </div>
            {(status?.nodes ?? []).map((node) => (
              <div className="component-row" key={`${node.role}-${node.name}`}>
                <div className="component-name">
                  {node.name}
                  <span>{node.role}</span>
                </div>
                <div>{node.instance_state ?? "-"}</div>
                <div>{node.service_state ?? node.metrics_state ?? "-"}</div>
                <div>
                  {numberValue(node.leader_groups)} / {numberValue(node.raft_groups)}
                </div>
                <div>{numberValue(node.accepted_appends)}</div>
              </div>
            ))}
          </div>
        </section>

        <section className="status-section">
          <div className="status-section-heading">
            <h2>Workload and Integrity</h2>
            <p>append pressure plus readback checks</p>
          </div>
          <div className="history-summary">
            <div>
              Target append rate
              <span>{numberValue(status?.workload.append_target_per_second)} / sec</span>
            </div>
            <div>
              Successful appends
              <span>{numberValue(status?.workload.append_success_total)}</span>
            </div>
            <div>
              Verified offsets
              <span>{numberValue(status?.integrity.verified_offsets)}</span>
            </div>
            <div>
              Integrity
              <span>
                <StatusPill status={status?.integrity.status ?? "unknown"} />
              </span>
            </div>
          </div>
        </section>

        <section className="status-section">
          <div className="status-section-heading">
            <h2>Fault Injection</h2>
            <p>{status?.chaos.enabled ? "enabled" : "disabled"}</p>
          </div>
          <div className="history-summary">
            <div>
              Active fault
              <span>{status?.chaos.active_fault ?? "none"}</span>
            </div>
            <div>
              Last fault
              <span>{status?.chaos.last_fault ?? "none"}</span>
            </div>
            <div>
              Next fault after
              <span>{formatTime(status?.chaos.next_fault_after ?? null)}</span>
            </div>
            <div>
              Last integrity check
              <span>{formatTime(status?.integrity.checked_at ?? null)}</span>
            </div>
          </div>
        </section>

        <section className="status-section">
          <div className="status-section-heading">
            <h2>Recent Events</h2>
            <p>{recentEvents.length} entries</p>
          </div>
          <div className="incident-list">
            {recentEvents.map((event, index) => (
              <article className="incident-item" key={`${event.time ?? "event"}-${index}`}>
                <div className="incident-item-header">
                  <h3>{event.message}</h3>
                  <StatusPill status={event.level === "error" ? "major_outage" : event.level === "warn" ? "degraded_performance" : "operational"} />
                </div>
                <dl className="incident-meta">
                  <div>
                    <dt>Time</dt>
                    <dd>{formatTime(event.time)}</dd>
                  </div>
                  <div>
                    <dt>Level</dt>
                    <dd>{event.level}</dd>
                  </div>
                </dl>
              </article>
            ))}
          </div>
        </section>
      </main>

      <Footer />
    </>
  );
}

export default StatusPage;
