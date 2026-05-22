import { useState } from "react";

import Footer from "../components/Footer";
import Header from "../components/Header";

type BackendKey = "ursula" | "durable" | "s2";

const backends: Record<
  BackendKey,
  { label: string; color: string; note: string }
> = {
  ursula: {
    label: "Ursula",
    color: "#83a598",
    note: "3 × c7g.4xlarge, Raft quorum + S3 cold flush",
  },
  durable: {
    label: "Durable Streams",
    color: "#fb4934",
    note: "1 × c7g.4xlarge, file-durable on EBS",
  },
  s2: {
    label: "S2 Lite",
    color: "#fabd2f",
    note: "1 × c7g.4xlarge, S3 backend",
  },
};

const backendOrder: BackendKey[] = ["ursula", "durable", "s2"];

type ScenarioPoint = {
  x: number;
  y: number;
  label: string;
  tooltip: string[];
};

type Scenario = {
  key: string;
  title: string;
  subtitle: string;
  xLabel: string;
  yLabel: string;
  yUnit: string;
  lowerIsBetter: boolean;
  xLevels: number[];
  series: Partial<Record<BackendKey, ScenarioPoint[]>>;
  annotation?: string;
};

// -----------------------------------------------------------------------------
// MEASURED DATA (apples-to-apples, ursula-bench, c7g cluster, 2026-05-22)
// -----------------------------------------------------------------------------

const multiStreamThroughput: Scenario = {
  key: "ms-throughput",
  title: "Multi-stream write - aggregate throughput",
  subtitle:
    "N independent streams, one writer per stream, 256 B payload, 30 s. All three systems run with persistent backends: Ursula commits to a 3-voter Raft quorum with S3 cold flush enabled, Durable Streams runs file-durable storage on EBS, and S2 Lite runs against S3.",
  xLabel: "concurrent active streams",
  yLabel: "aggregate commits / s",
  yUnit: "ops/s",
  lowerIsBetter: false,
  xLevels: [100, 500, 2000],
  series: {
    ursula: [
      {
        x: 100,
        y: 28697,
        label: "28.7k",
        tooltip: ["100 streams", "28,697 ops/s", "p99 7.4 ms"],
      },
      {
        x: 500,
        y: 41552,
        label: "41.6k",
        tooltip: ["500 streams", "41,552 ops/s", "p99 26.0 ms"],
      },
      {
        x: 2000,
        y: 38772,
        label: "38.8k",
        tooltip: ["2k streams", "38,772 ops/s", "p99 101.1 ms"],
      },
    ],
    durable: [
      {
        x: 100,
        y: 2760,
        label: "2.8k",
        tooltip: ["100 streams", "2,760 ops/s", "p99 79.7 ms", "file-durable on EBS"],
      },
      {
        x: 500,
        y: 3400,
        label: "3.4k",
        tooltip: ["500 streams", "3,400 ops/s", "p99 471.8 ms"],
      },
      {
        x: 2000,
        y: 3626,
        label: "3.6k",
        tooltip: ["2k streams", "3,626 ops/s", "p99 994.3 ms"],
      },
    ],
    s2: [
      {
        x: 100,
        y: 1416,
        label: "1.4k",
        tooltip: ["100 streams", "1,416 ops/s", "p99 160.1 ms", "S3 PUT bound"],
      },
      {
        x: 500,
        y: 6042,
        label: "6.0k",
        tooltip: ["500 streams", "6,042 ops/s", "p99 234.6 ms"],
      },
      {
        x: 2000,
        y: 12157,
        label: "12.2k",
        tooltip: ["2k streams", "12,157 ops/s", "p99 370.4 ms"],
      },
    ],
  },
  annotation:
    "Ursula keeps every append on a 3-voter Raft quorum while asynchronously flushing cold chunks to S3; this run uploaded ~675 MiB through that background path. Durable Streams is shown on a real EBS-backed data directory; earlier tmpfs-backed file-durable numbers are excluded.",
};

const multiStreamLatency: Scenario = {
  key: "ms-latency",
  title: "Multi-stream write - p99 latency",
  subtitle: "Same workload. Lower is better.",
  xLabel: "concurrent active streams",
  yLabel: "p99 append latency",
  yUnit: "ms",
  lowerIsBetter: true,
  xLevels: [100, 500, 2000],
  series: {
    ursula: [
      {
        x: 100,
        y: 7.4,
        label: "7.4 ms",
        tooltip: ["100 streams", "p50 3.0 ms", "p99 7.4 ms", "p999 51.5 ms"],
      },
      {
        x: 500,
        y: 26.0,
        label: "26 ms",
        tooltip: ["500 streams", "p50 11.5 ms", "p99 26.0 ms", "p999 46.9 ms"],
      },
      {
        x: 2000,
        y: 101.1,
        label: "101 ms",
        tooltip: ["2k streams", "p50 50.0 ms", "p99 101.1 ms", "p999 152.7 ms"],
      },
    ],
    durable: [
      {
        x: 100,
        y: 79.7,
        label: "80 ms",
        tooltip: ["100 streams", "p50 35.3 ms", "p99 79.7 ms"],
      },
      {
        x: 500,
        y: 471.8,
        label: "472 ms",
        tooltip: ["500 streams", "p50 119.4 ms", "p99 471.8 ms"],
      },
      {
        x: 2000,
        y: 994.3,
        label: "994 ms",
        tooltip: ["2k streams", "p50 505.9 ms", "p99 994.3 ms"],
      },
    ],
    s2: [
      {
        x: 100,
        y: 160.1,
        label: "160 ms",
        tooltip: [
          "100 streams",
          "p50 59.9 ms",
          "p99 160 ms",
          "S3 latency bound",
        ],
      },
      {
        x: 500,
        y: 234.6,
        label: "235 ms",
        tooltip: ["500 streams", "p50 74.6 ms", "p99 235 ms"],
      },
      {
        x: 2000,
        y: 370.4,
        label: "370 ms",
        tooltip: ["2k streams", "p50 155.5 ms", "p99 370 ms"],
      },
    ],
  },
  annotation:
    "S2 Lite's per-append latency is dominated by the S3 PUT round-trip. Durable Streams pays local EBS fdatasync on the file-durable path; Ursula pays the cross-node quorum cost plus background cold-flush pressure and remains below both at every measured concurrency.",
};

const fanoutLatency: Scenario = {
  key: "fanout",
  title: "SSE fan-out - per-event delivery p99",
  subtitle:
    "One stream, one writer at 50 events / s, N concurrent SSE subscribers. End-to-end publish-to-receive latency measured at each subscriber.",
  xLabel: "concurrent SSE subscribers on one stream",
  yLabel: "p99 fan-out latency",
  yUnit: "ms",
  lowerIsBetter: true,
  xLevels: [50, 200, 500, 1000],
  series: {
    ursula: [
      {
        x: 50,
        y: 1.5,
        label: "1.5 ms",
        tooltip: ["50 subs", "74.8k delivered", "p99 1.5 ms"],
      },
      {
        x: 200,
        y: 4.1,
        label: "4.1 ms",
        tooltip: ["200 subs", "297.9k delivered", "p99 4.1 ms"],
      },
      {
        x: 500,
        y: 5.1,
        label: "5.1 ms",
        tooltip: ["500 subs", "720.6k delivered", "p99 5.1 ms"],
      },
      {
        x: 1000,
        y: 8.3,
        label: "8.3 ms",
        tooltip: ["1000 subs", "1.43M delivered", "p99 8.3 ms"],
      },
    ],
    durable: [
      {
        x: 50,
        y: 3.1,
        label: "3.1 ms",
        tooltip: ["50 subs", "75k delivered", "p99 3.1 ms", "file-durable on EBS"],
      },
      {
        x: 200,
        y: 3.4,
        label: "3.4 ms",
        tooltip: ["200 subs", "298.7k delivered", "p99 3.4 ms"],
      },
      {
        x: 500,
        y: 4.6,
        label: "4.6 ms",
        tooltip: ["500 subs", "747.1k delivered", "p99 4.6 ms"],
      },
      {
        x: 1000,
        y: 6.5,
        label: "6.5 ms",
        tooltip: ["1000 subs", "1.44M delivered", "p99 6.5 ms"],
      },
    ],
    s2: [
      {
        x: 50,
        y: 101.2,
        label: "101 ms",
        tooltip: ["50 subs", "27.9k delivered", "p99 101 ms", "S3 batch flush"],
      },
      {
        x: 200,
        y: 100.8,
        label: "101 ms",
        tooltip: ["200 subs", "112.9k delivered", "p99 101 ms"],
      },
      {
        x: 500,
        y: 112.2,
        label: "112 ms",
        tooltip: ["500 subs", "272.5k delivered", "p99 112 ms"],
      },
      {
        x: 1000,
        y: 111.7,
        label: "112 ms",
        tooltip: ["1000 subs", "559.5k delivered", "p99 112 ms"],
      },
    ],
  },
  annotation:
    "Ursula and Durable Streams both keep fan-out p99 in single-digit milliseconds through 1,000 subscribers. S2 Lite remains around 100 ms because the S3-backed path dominates the live-tail floor in this setup.",
};

const replayLatency: Scenario = {
  key: "replay-latency",
  title: "Catch-up replay - p99 latency",
  subtitle:
    "N clients, each on its own stream pre-filled with 200 events × 1 KiB. Ursula uses GET /bootstrap (snapshot + tail-since-snapshot); DS and S2 Lite replay the full log in this harness.",
  xLabel: "concurrent clients (each on a unique stream)",
  yLabel: "p99 latency among ok clients",
  yUnit: "ms",
  lowerIsBetter: true,
  xLevels: [100, 500, 1000],
  series: {
    ursula: [
      {
        x: 100,
        y: 96.3,
        label: "96 ms",
        tooltip: [
          "100 / 100 ok",
          "p99 96.3 ms",
          "172 KB body (snapshot 64 KB + tail)",
        ],
      },
      {
        x: 500,
        y: 229.8,
        label: "230 ms",
        tooltip: ["500 / 500 ok", "p99 229.8 ms"],
      },
      {
        x: 1000,
        y: 253.2,
        label: "253 ms",
        tooltip: ["1000 / 1000 ok", "p99 253.2 ms"],
      },
    ],
    durable: [
      {
        x: 100,
        y: 10.4,
        label: "10 ms",
        tooltip: ["100 / 100 ok", "p99 10.4 ms", "200 KB body (full log)"],
      },
      {
        x: 500,
        y: 215.8,
        label: "216 ms",
        tooltip: ["500 / 500 ok", "p99 215.8 ms"],
      },
      {
        x: 1000,
        y: 365.8,
        label: "366 ms",
        tooltip: ["1000 / 1000 ok", "p99 365.8 ms"],
      },
    ],
    s2: [
      {
        x: 100,
        y: 353.5,
        label: "354 ms",
        tooltip: ["100 / 100 ok", "p99 353.5 ms", "471 KB body"],
      },
      {
        x: 500,
        y: 371.5,
        label: "371 ms",
        tooltip: ["500 / 500 ok", "p99 371.5 ms"],
      },
      {
        x: 1000,
        y: 794.1,
        label: "794 ms",
        tooltip: ["1000 / 1000 ok", "p99 794.1 ms"],
      },
    ],
  },
  annotation:
    "At 1,000 concurrent clients, Ursula has the lowest replay p99 (253 ms) and the smallest response body (172 KB), ahead of Durable Streams at 366 ms and S2 Lite at 794 ms.",
};

// -----------------------------------------------------------------------------
// Trend chart
// -----------------------------------------------------------------------------

type TooltipState = {
  xPct: number;
  yPct: number;
  placement: "above" | "below";
  title: string;
  lines: string[];
} | null;

function fmtY(v: number, unit: string) {
  if (unit === "ops/s") {
    if (v >= 1000) return `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}k`;
    return `${v.toFixed(0)}`;
  }
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k`;
  if (v >= 100) return v.toFixed(0);
  if (v >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

function fmtX(v: number) {
  if (v >= 1000) return `${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k`;
  return v.toString();
}

function TrendChart({ scenario }: { scenario: Scenario }) {
  const [tooltip, setTooltip] = useState<TooltipState>(null);
  const width = 760;
  const height = 320;
  const left = 72;
  const right = 24;
  const top = 32;
  const bottom = 64;
  const chartWidth = width - left - right;
  const chartHeight = height - top - bottom;

  const allY = backendOrder.flatMap((b) =>
    (scenario.series[b] ?? []).map((p) => p.y),
  );
  const yMax = Math.max(...allY) * 1.12 || 1;
  const yMin = 0;

  const xMin = Math.log2(Math.max(1, scenario.xLevels[0]));
  const xMax = Math.log2(
    Math.max(2, scenario.xLevels[scenario.xLevels.length - 1]),
  );

  const xPos = (v: number) =>
    left +
    ((Math.log2(Math.max(1, v)) - xMin) / (xMax - xMin || 1)) * chartWidth;
  const yPos = (v: number) =>
    top + chartHeight - ((v - yMin) / (yMax - yMin || 1)) * chartHeight;

  const tickFractions = [0, 0.25, 0.5, 0.75, 1];

  return (
    <div
      className="benchmark-trend-frame"
      onPointerLeave={() => setTooltip(null)}
    >
      <svg
        className="benchmark-trend-panel"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${scenario.title} trend`}
      >
        <rect
          className="benchmark-trend-bg"
          x="0"
          y="0"
          width={width}
          height={height}
          rx="8"
        />
        <text className="benchmark-trend-title" x={left} y="22">
          {scenario.yLabel}
        </text>
        <text
          className="benchmark-trend-better"
          x={width - right}
          y="22"
          textAnchor="end"
        >
          {scenario.lowerIsBetter ? "lower is better" : "higher is better"} ·
          log x, linear y
        </text>

        {tickFractions.map((f) => {
          const py = top + chartHeight - f * chartHeight;
          const v = yMin + f * (yMax - yMin);
          return (
            <g key={f}>
              <line
                className="benchmark-trend-grid-line"
                x1={left}
                x2={width - right}
                y1={py}
                y2={py}
              />
              <text
                className="benchmark-trend-axis-text"
                x={left - 10}
                y={py + 4}
                textAnchor="end"
              >
                {fmtY(v, scenario.yUnit)}
              </text>
            </g>
          );
        })}

        {scenario.xLevels.map((lvl) => {
          const px = xPos(lvl);
          return (
            <g key={lvl}>
              <line
                className="benchmark-trend-grid-line"
                x1={px}
                x2={px}
                y1={top}
                y2={top + chartHeight}
              />
              <text
                className="benchmark-trend-axis-text"
                x={px}
                y={height - 34}
                textAnchor="middle"
              >
                {fmtX(lvl)}
              </text>
            </g>
          );
        })}
        <text
          className="benchmark-trend-axis-label"
          x={left + chartWidth / 2}
          y={height - 12}
          textAnchor="middle"
        >
          {scenario.xLabel}
        </text>

        {backendOrder.map((b) => {
          const pts = scenario.series[b];
          if (!pts) return null;
          const color = backends[b].color;
          const path = pts.map((p) => `${xPos(p.x)},${yPos(p.y)}`).join(" ");
          return (
            <g key={b}>
              <polyline
                fill="none"
                points={path}
                stroke={color}
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth="3"
              />
              {pts.map((p) => {
                const px = xPos(p.x);
                const py = yPos(p.y);
                const xPct = Math.min(86, Math.max(14, (px / width) * 100));
                const yPct = (py / height) * 100;
                const placement: "above" | "below" =
                  py < 90 ? "below" : "above";
                return (
                  <g
                    key={p.x}
                    className="benchmark-trend-point-group"
                    tabIndex={0}
                    onPointerEnter={() =>
                      setTooltip({
                        xPct,
                        yPct,
                        placement,
                        title: `${backends[b].label} @ ${p.x.toLocaleString()}`,
                        lines: p.tooltip,
                      })
                    }
                    onFocus={() =>
                      setTooltip({
                        xPct,
                        yPct,
                        placement,
                        title: `${backends[b].label} @ ${p.x.toLocaleString()}`,
                        lines: p.tooltip,
                      })
                    }
                    onBlur={() => setTooltip(null)}
                  >
                    <circle
                      className="benchmark-trend-hit"
                      cx={px}
                      cy={py}
                      r="14"
                    />
                    <circle
                      className="benchmark-trend-point"
                      cx={px}
                      cy={py}
                      r="5"
                      fill={color}
                    />
                  </g>
                );
              })}
            </g>
          );
        })}
      </svg>

      {tooltip && (
        <div
          className={`benchmark-trend-tooltip ${tooltip.placement}`}
          style={{ left: `${tooltip.xPct}%`, top: `${tooltip.yPct}%` }}
        >
          <strong>{tooltip.title}</strong>
          {tooltip.lines.map((line) => (
            <span key={line}>{line}</span>
          ))}
        </div>
      )}

      <div
        style={{
          display: "flex",
          gap: 18,
          flexWrap: "wrap",
          marginTop: 10,
          fontSize: 12,
          color: "#a89984",
        }}
      >
        {backendOrder.map((b) =>
          scenario.series[b] ? (
            <span key={b}>
              <span
                style={{
                  display: "inline-block",
                  width: 16,
                  height: 3,
                  background: backends[b].color,
                  verticalAlign: "middle",
                  marginRight: 6,
                }}
              />
              {backends[b].label}
            </span>
          ) : null,
        )}
      </div>
    </div>
  );
}

function ScenarioBlock({ scenario }: { scenario: Scenario }) {
  return (
    <article className="benchmark-result-card">
      <div className="benchmark-result-card-head">
        <h3>{scenario.title}</h3>
        <span>
          {scenario.lowerIsBetter ? "lower is better" : "higher is better"}
        </span>
      </div>
      <p style={{ color: "#bdae93" }}>{scenario.subtitle}</p>
      <TrendChart scenario={scenario} />
      {scenario.annotation && (
        <p
          style={{
            padding: "12px 16px",
            borderLeft: `3px solid ${backends.ursula.color}`,
            background: "rgba(131, 166, 152, 0.08)",
            color: "#ebdbb2",
            fontSize: 14,
            lineHeight: 1.55,
          }}
        >
          {scenario.annotation}
        </p>
      )}
    </article>
  );
}

function BenchmarkPage() {
  return (
    <>
      <Header
        navItems={[
          { label: "Docs", href: "/docs" },
          { label: "Blog", href: "/blog" },
          { label: "Benchmark", href: "/benchmark", active: true },
        ]}
        version={__URSULA_VERSION__}
        githubUrl="https://github.com/tonbo-io/ursula"
      />

      <main className="benchmark-page">
        <div className="benchmark-hero">
          <header>
            <h1>OSS HTTP Streams Benchmark</h1>
            <p className="benchmark-lead">
              A comparison of Ursula, Durable Streams, and S2 Lite across
              multi-stream writes, catch-up replay, and SSE live tail.
            </p>
          </header>
          <aside className="benchmark-scoreboard" aria-label="Headline gaps">
            <article className="benchmark-score ursula">
              <div className="benchmark-score-name">
                Multi-stream write @ 500 streams
              </div>
              <div className="benchmark-score-pair">
                <div>
                  <b>41.6k</b>
                  <span>Ursula ops/s</span>
                </div>
                <div>
                  <b>6.9×</b>
                  <span>vs S2 Lite (S3)</span>
                </div>
              </div>
              <div className="benchmark-score-foot">
                <div>
                  <b>3.4k</b>
                  <span>DS single-node EBS file-durable</span>
                </div>
                <div>
                  <b>3 voter</b>
                  <span>quorum on every commit</span>
                </div>
              </div>
            </article>
            <article className="benchmark-score s2">
              <div className="benchmark-score-name">
                SSE fan-out @ 1000 subscribers
              </div>
              <div className="benchmark-score-pair">
                <div>
                  <b>8.3 ms</b>
                  <span>Ursula p99</span>
                </div>
                <div>
                  <b>13×</b>
                  <span>vs S2 Lite p99</span>
                </div>
              </div>
              <div className="benchmark-score-foot">
                <div>
                  <b>6.5 ms</b>
                  <span>DS p99</span>
                </div>
                <div>
                  <b>112 ms</b>
                  <span>S2 Lite p99</span>
                </div>
              </div>
            </article>
            <article className="benchmark-score durable">
              <div className="benchmark-score-name">
                Catch-up replay @ 1,000 clients
              </div>
              <div className="benchmark-score-pair">
                <div>
                  <b>1.8×</b>
                  <span>vs DS p99</span>
                </div>
                <div>
                  <b>3.1×</b>
                  <span>vs S2 Lite p99</span>
                </div>
              </div>
              <div className="benchmark-score-foot">
                <div>
                  <b>253 ms</b>
                  <span>Ursula p99</span>
                </div>
                <div>
                  <b>snapshot</b>
                  <span>172 KB body vs 200 KB (DS) / 471 KB (S2 Lite)</span>
                </div>
              </div>
            </article>
          </aside>
        </div>

        <section className="benchmark-section">
          <h2>What was measured</h2>
          <p>
            All three systems answered the exact same three workloads from the
            exact same client binary. The bench client picks a backend with{" "}
            <code>--api-style ursula|durable|s2</code> and switches its HTTP
            plumbing (URLs, body shape, auth headers) so the workload itself is
            identical across backends.
          </p>
          <div className="benchmark-deploy-grid">
            <article>
              <h3>
                <span
                  style={{
                    display: "inline-block",
                    width: 10,
                    height: 10,
                    borderRadius: "50%",
                    background: backends.ursula.color,
                    marginRight: 6,
                  }}
                />
                Ursula
              </h3>
              <ul>
                <li>3 × c7g.4xlarge, one voter per AZ</li>
                <li>256 Raft groups, 16 cores per node</li>
                <li>Every commit replicates to a majority quorum (2 of 3)</li>
                <li>S3 cold flush enabled; ~675 MiB uploaded in this run</li>
                <li>Bench targets all 3 nodes via round-robin</li>
              </ul>
            </article>
            <article>
              <h3>
                <span
                  style={{
                    display: "inline-block",
                    width: 10,
                    height: 10,
                    borderRadius: "50%",
                    background: backends.durable.color,
                    marginRight: 6,
                  }}
                />
                Durable Streams
              </h3>
              <ul>
                <li>1 × c7g.4xlarge, single Rust server process</li>
                <li>durable-streams-server v0.3.0</li>
                <li>file-durable storage on the root EBS volume</li>
                <li>Capacity limits raised above workload size; replay uses ?offset=-1</li>
              </ul>
            </article>
            <article>
              <h3>
                <span
                  style={{
                    display: "inline-block",
                    width: 10,
                    height: 10,
                    borderRadius: "50%",
                    background: backends.s2.color,
                    marginRight: 6,
                  }}
                />
                S2 Lite
              </h3>
              <ul>
                <li>1 × c7g.4xlarge, single S2 Lite process</li>
                <li>s2-cli v0.33.0 (s2-lite)</li>
                <li>S3 backend (S3 Standard, same region)</li>
                <li>S2 Lite's own API, not Durable Streams protocol</li>
              </ul>
            </article>
          </div>
          <p>
            <strong>All three backends are persistent in this run.</strong>{" "}
            Ursula commits each write to a 3-voter Raft quorum across three
            c7g.4xlarge nodes and runs background S3 cold flush; Durable
            Streams' file-backed store fsyncs to the root EBS volume on a single node;
            S2 Lite writes through to S3 on a single node. Ursula append
            acknowledgements are not gated by the S3 flush, but this run did
            exercise that background path. This is the durable-vs-durable
            comparison. Aggregate throughput reflects Ursula getting 3× the
            hardware in exchange for delivering quorum-replicated durability
            across AZs that the other two do not provide here.
          </p>
          <p>
            The OS file descriptor limit was set to 65,535 on the client and
            servers. With S2 Lite artificially constrained to 256 fds, the same
            harness reproduces connection failures as <code>Too many open files</code>;
            those failures are excluded from the headline results.
          </p>
          <p>
            Durable Streams' <code>max_memory_bytes</code> is a hard payload
            capacity limit, not an eviction cache. It is raised here only to
            avoid benchmark-induced 413 responses; the data directory is on EBS,
            not <code>/tmp</code> tmpfs.
          </p>
        </section>

        <section className="benchmark-section">
          <h2>Multi-stream write</h2>
          <p>
            The question this scenario answers: when many streams are writing
            concurrently, does the system commit them in parallel or does some
            shared point serialize them? Ursula's bet is multi-Raft sharding
            across nodes and cores.
          </p>
          <div style={{ display: "grid", gap: 24 }}>
            <ScenarioBlock scenario={multiStreamThroughput} />
            <ScenarioBlock scenario={multiStreamLatency} />
          </div>
        </section>

        <section className="benchmark-section">
          <h2>SSE fan-out</h2>
          <p>
            One popular document with N concurrent SSE viewers and a steady-rate
            publisher. The bet: a server with an O(unique-request) wake path
            delivers each event to all viewers in one round; a naive O(N) wake
            loop or storage-backed tail path can add latency as subscriber
            count grows.
          </p>
          <ScenarioBlock scenario={fanoutLatency} />
        </section>

        <section className="benchmark-section">
          <h2>Catch-up replay</h2>
          <p>
            After a deploy or a network blip, many clients reconnect - each to
            its own document. Each client wants "give me the full current state
            of this stream". The mechanism differs by system: Ursula uses{" "}
            <code>/bootstrap</code> which returns a snapshot plus the tail since
            that snapshot, while DS and S2 Lite must replay the full log because
            neither ships a matching snapshot endpoint in this harness.
          </p>
          <ScenarioBlock scenario={replayLatency} />
        </section>

        <section className="benchmark-section">
          <h2>Takeaways</h2>
          <ul style={{ color: "#ebdbb2", lineHeight: 1.6, paddingLeft: 20 }}>
            <li>
              <strong>Write throughput</strong> at 500 streams: Ursula 41.6k
              quorum commits/s vs S2 Lite 6.0k; Durable Streams reaches 3.4k
              on its single-node EBS file-durable path.
            </li>
            <li>
              <strong>SSE fan-out</strong> at 1,000 subscribers: Ursula 8.3 ms
              p99 and DS 6.5 ms p99 both stay in single-digit milliseconds; S2
              Lite is 112 ms p99 on the S3-backed path.
            </li>
            <li>
              <strong>Catch-up replay</strong> at 1,000 clients: Ursula has
              the lowest p99 at 253 ms and the smallest response body at 172 KB.
            </li>
            <li>
              <strong>Caveats:</strong> Ursula uses 3 × c7g vs DS / S2 Lite's 1 ×
              c7g (deployment-shape comparison, not per-CPU). DS numbers use
              file-durable on EBS with capacity limits raised above the benchmark
              footprint, and S2 Lite uses S3 Standard with a 65,535-fd process
              limit.
            </li>
          </ul>
        </section>

        <section className="benchmark-section">
          <h2>Durability and availability posture</h2>
          <p>
            Throughput and latency are only fair to compare if the durability
            properties are clear. Here is what each system actually guarantees
            in this benchmark's configuration. Ursula pays a quorum round-trip
            on every commit; S2 Lite pays an S3 PUT; the file-durable Durable
            Streams server writes to a single EBS volume.
          </p>
          <div className="benchmark-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>System</th>
                  <th>Committed data lives on</th>
                  <th>One instance lost</th>
                  <th>One AZ lost</th>
                  <th>Approx. annual data-loss probability</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <th>
                    <span
                      style={{
                        display: "inline-block",
                        width: 10,
                        height: 10,
                        borderRadius: "50%",
                        background: backends.ursula.color,
                        marginRight: 6,
                      }}
                    />
                    Ursula
                  </th>
                  <td>3 Raft voters across us-east-1a / 1b / 1c</td>
                  <td>service stays up; data preserved (2/3 quorum)</td>
                  <td>service stays up; data preserved (2/3 quorum)</td>
                  <td>
                    ~10<sup>−7</sup> (needs concurrent loss of 2 voters across
                    AZs before recovery)
                  </td>
                </tr>
                <tr>
                  <th>
                    <span
                      style={{
                        display: "inline-block",
                        width: 10,
                        height: 10,
                        borderRadius: "50%",
                        background: backends.durable.color,
                        marginRight: 6,
                      }}
                    />
                    Durable Streams (file-durable)
                  </th>
                  <td>local disk on one EBS volume, one instance, one AZ</td>
                  <td>
                    service down + acknowledged data potentially unrecoverable
                  </td>
                  <td>
                    service down + acknowledged data potentially unrecoverable
                  </td>
                  <td>
                    ~10<sup>−5</sup> (bounded by single EBS volume / instance
                    failure rate)
                  </td>
                </tr>
                <tr>
                  <th>
                    <span
                      style={{
                        display: "inline-block",
                        width: 10,
                        height: 10,
                        borderRadius: "50%",
                        background: backends.s2.color,
                        marginRight: 6,
                      }}
                    />
                    S2 Lite (S3)
                  </th>
                  <td>
                    S3 Standard (cross-AZ replicated by S3, 11-nines object
                    durability)
                  </td>
                  <td>
                    service down until restart; committed data preserved on S3
                  </td>
                  <td>
                    service down until restart; committed data preserved on S3
                  </td>
                  <td>
                    ~10<sup>−11</sup> per object (S3 durability), service
                    availability bounded by single instance
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <p>
            Three different shapes of "durable". Ursula gives you replicated{" "}
            <em>availability</em> too - the cluster keeps serving on instance or
            AZ loss. Durable Streams is the weakest on availability: data on one
            disk, service on one process. S2 Lite has the best raw
            object-storage durability but its service front-end is
            single-instance, so an instance failure means downtime even though
            the data is intact. Read the throughput and latency numbers above
            with this in mind: Ursula is paying for that quorum replication on
            every write.
          </p>
        </section>

        <section className="benchmark-section">
          <h2>Reproduce</h2>
          <div className="benchmark-code-block">
            <pre>{`# 1. Build the bench client and the Ursula HTTP server
cargo build --release -p ursula -p ursula-bench

# 2. Bring up each backend on identical hardware
export URSULA_COLD_BACKEND=s3
export URSULA_COLD_S3_BUCKET=<s3-bucket>
export URSULA_COLD_S3_REGION=<region>
export URSULA_COLD_FLUSH_MIN_HOT_BYTES=65536
export URSULA_COLD_FLUSH_MAX_BYTES=65536
python3 scripts/ursula_ec2.py --config <manifest>.json start
~/.cargo/bin/s2 lite --bucket <s3-bucket> --path s2-lite --port 4439
durable-streams-server --profile dev --config ds-ebs-file-durable.toml

# 3. Run the same three scenarios against each
for api in ursula durable s2; do
 ursula-bench multi-stream --target http://NODE:PORT --api-style "$api" \\
 --streams 500 --duration-secs 30 --payload-bytes 256
 ursula-bench fan-out --target http://NODE:PORT --api-style "$api" \\
 --subscribers 1000 --writer-rate 50 --duration-secs 30
done

# 4. Replay (apples-to-apples on all three backends)
for api in ursula durable s2; do
 ursula-bench bootstrap --target http://NODE:PORT --api-style "$api" \\
 --clients 1000 --pre-events 200 --per-client-stream
done`}</pre>
          </div>
        </section>
      </main>

      <Footer />
    </>
  );
}

export default BenchmarkPage;
