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
    note: "3 × c7g.4xlarge, Raft quorum",
  },
  durable: {
    label: "Durable Streams (ref)",
    color: "#fb4934",
    note: "1 × c7g.4xlarge, file-backed store",
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
// MEASURED DATA (apples-to-apples, ursula-bench, c7g cluster, 2026-05-19)
// -----------------------------------------------------------------------------

const multiStreamThroughput: Scenario = {
  key: "ms-throughput",
  title: "Multi-stream write - aggregate throughput",
  subtitle:
    "N independent streams, one writer per stream, 256 B payload, 30 s. All three systems run with persistent backends: Ursula commits to a 3-voter Raft quorum, Durable Streams runs its file-backed store, S2 Lite runs against S3.",
  xLabel: "concurrent active streams",
  yLabel: "aggregate commits / s",
  yUnit: "ops/s",
  lowerIsBetter: false,
  xLevels: [100, 500, 2000],
  series: {
    ursula: [
      {
        x: 100,
        y: 24177,
        label: "24.2k",
        tooltip: ["100 streams", "24,177 ops/s", "p99 8.6 ms"],
      },
      {
        x: 500,
        y: 35241,
        label: "35.2k",
        tooltip: ["500 streams", "35,241 ops/s", "p99 32.6 ms"],
      },
      {
        x: 2000,
        y: 29395,
        label: "29.4k",
        tooltip: ["2k streams", "29,395 ops/s", "p99 139.6 ms"],
      },
    ],
    durable: [
      {
        x: 100,
        y: 6488,
        label: "6.5k",
        tooltip: ["100 streams", "6,488 ops/s", "p99 21.7 ms", "file-backed"],
      },
      {
        x: 500,
        y: 5991,
        label: "6.0k",
        tooltip: ["500 streams", "5,991 ops/s", "p99 136.6 ms"],
      },
      {
        x: 2000,
        y: 5221,
        label: "5.2k",
        tooltip: ["2k streams", "5,221 ops/s", "p99 497.7 ms", "262 errors"],
      },
    ],
    s2: [
      {
        x: 100,
        y: 1342,
        label: "1.3k",
        tooltip: ["100 streams", "1,342 ops/s", "p99 157.8 ms", "S3 PUT bound"],
      },
      {
        x: 500,
        y: 6816,
        label: "6.8k",
        tooltip: ["500 streams", "6,816 ops/s", "p99 179.6 ms"],
      },
      {
        x: 2000,
        y: 11733,
        label: "11.7k",
        tooltip: ["2k streams", "11,733 ops/s", "p99 366.8 ms"],
      },
    ],
  },
  annotation:
    "With all three running on persistent storage Ursula peaks above 35k commits/s, vs ~6k for the file-backed Durable Streams reference and 1.3k–11.7k for S3-backed S2 Lite. Ursula's multi-Raft layout puts writes for different streams on different cores and different nodes in parallel, whereas the two single-process systems are bounded by their single durable-write loop.",
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
        y: 8.6,
        label: "8.6 ms",
        tooltip: ["100 streams", "p50 3.7 ms", "p99 8.6 ms", "p999 50 ms"],
      },
      {
        x: 500,
        y: 32.6,
        label: "33 ms",
        tooltip: ["500 streams", "p50 13.3 ms", "p99 32.6 ms", "p999 70 ms"],
      },
      {
        x: 2000,
        y: 139.6,
        label: "140 ms",
        tooltip: ["2k streams", "p50 65.5 ms", "p99 139.6 ms", "p999 201 ms"],
      },
    ],
    durable: [
      {
        x: 100,
        y: 21.7,
        label: "22 ms",
        tooltip: ["100 streams", "p50 15.1 ms", "p99 21.7 ms"],
      },
      {
        x: 500,
        y: 136.6,
        label: "137 ms",
        tooltip: ["500 streams", "p50 109 ms", "p99 137 ms"],
      },
      {
        x: 2000,
        y: 497.7,
        label: "498 ms",
        tooltip: ["2k streams", "p50 413 ms", "p99 498 ms"],
      },
    ],
    s2: [
      {
        x: 100,
        y: 157.8,
        label: "158 ms",
        tooltip: [
          "100 streams",
          "p50 65.8 ms",
          "p99 158 ms",
          "S3 latency bound",
        ],
      },
      {
        x: 500,
        y: 179.6,
        label: "180 ms",
        tooltip: ["500 streams", "p50 58.1 ms", "p99 180 ms"],
      },
      {
        x: 2000,
        y: 366.8,
        label: "367 ms",
        tooltip: ["2k streams", "p50 161 ms", "p99 367 ms"],
      },
    ],
  },
  annotation:
    "S2 Lite's per-append latency is dominated by the S3 PUT round-trip (~50–100 ms baseline). Durable Streams' file-backed store starts comparable to Ursula at low concurrency but its single event loop falls behind quickly. Ursula's p99 grows with concurrency the same way the others' do, but stays at the bottom of the range while replicating to quorum.",
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
        y: 3.5,
        label: "3.5 ms",
        tooltip: ["50 subs", "75k delivered", "p99 3.5 ms"],
      },
      {
        x: 200,
        y: 4.4,
        label: "4.4 ms",
        tooltip: ["200 subs", "298k delivered", "p99 4.4 ms"],
      },
      {
        x: 500,
        y: 4.9,
        label: "4.9 ms",
        tooltip: ["500 subs", "718k delivered", "p99 4.9 ms"],
      },
      {
        x: 1000,
        y: 6.1,
        label: "6.1 ms",
        tooltip: ["1000 subs", "1.4M delivered", "p99 6.1 ms"],
      },
    ],
    durable: [
      {
        x: 50,
        y: 35.0,
        label: "35 ms",
        tooltip: ["50 subs", "63.8k delivered", "p99 35 ms", "file-backed"],
      },
      {
        x: 200,
        y: 197.8,
        label: "198 ms",
        tooltip: ["200 subs", "37.2k delivered", "p99 198 ms"],
      },
      {
        x: 500,
        y: 516.1,
        label: "516 ms",
        tooltip: ["500 subs", "38k delivered", "p99 516 ms"],
      },
      {
        x: 1000,
        y: 980.0,
        label: "980 ms",
        tooltip: ["1000 subs", "39.5k delivered", "p99 980 ms"],
      },
    ],
    s2: [
      {
        x: 50,
        y: 113.1,
        label: "113 ms",
        tooltip: ["50 subs", "27.5k delivered", "p99 113 ms", "S3 batch flush"],
      },
      {
        x: 200,
        y: 115.9,
        label: "116 ms",
        tooltip: ["200 subs", "109k delivered", "p99 116 ms"],
      },
      {
        x: 500,
        y: 96.4,
        label: "96 ms",
        tooltip: ["500 subs", "277k delivered", "p99 96 ms"],
      },
      {
        x: 1000,
        y: 111.3,
        label: "111 ms",
        tooltip: ["1000 subs", "550k delivered", "p99 111 ms"],
      },
    ],
  },
  annotation:
    "Ursula stays flat from 50 to 1,000 subscribers at 3.5–6.1 ms p99 - the watcher path is O(unique requests). S2 with S3 backend is flat at ~100 ms (dominated by S3 round-trip on the read side). Durable Streams' file-backed store shows the linear O(N) wake curve, ending at 980 ms p99 at 1,000 subscribers - a 160× gap vs Ursula.",
};

const replayCompletion: Scenario = {
  key: "replay-completion",
  title: "Catch-up replay - completion rate",
  subtitle:
    "N clients, each on its own stream pre-filled with 200 events × 1 KiB. Every client asks for the full stream contents. Ursula uses GET /bootstrap (snapshot + tail-since-snapshot); DS and S2 don't have a snapshot endpoint, so each client must read the full log. Y-axis is the share of clients that received valid data inside the 180 s timeout.",
  xLabel: "concurrent clients (each on a unique stream)",
  yLabel: "% of clients that successfully replayed",
  yUnit: "%",
  lowerIsBetter: false,
  xLevels: [100, 500, 1000],
  series: {
    ursula: [
      {
        x: 100,
        y: 100,
        label: "100%",
        tooltip: ["100 / 100 ok", "drain 0.01 s", "p99 6.8 ms"],
      },
      {
        x: 500,
        y: 100,
        label: "100%",
        tooltip: ["500 / 500 ok", "drain 0.25 s", "p99 271.6 ms"],
      },
      {
        x: 1000,
        y: 100,
        label: "100%",
        tooltip: ["1000 / 1000 ok", "drain 0.45 s", "p99 471 ms"],
      },
    ],
    durable: [
      {
        x: 100,
        y: 100,
        label: "100%",
        tooltip: ["100 / 100 ok", "drain 0.11 s", "p99 106.7 ms"],
      },
      {
        x: 500,
        y: 100,
        label: "100%",
        tooltip: ["500 / 500 ok", "drain 0.58 s", "p99 521.7 ms"],
      },
      {
        x: 1000,
        y: 100,
        label: "100%",
        tooltip: ["1000 / 1000 ok", "drain 1.01 s", "p99 828.4 ms"],
      },
    ],
    s2: [
      {
        x: 100,
        y: 100,
        label: "100%",
        tooltip: ["100 / 100 ok", "drain 0.04 s", "p99 35.4 ms"],
      },
      {
        x: 500,
        y: 97.4,
        label: "97.4%",
        tooltip: ["487 / 500 ok", "13 timeouts", "p99 346.9 ms among ok"],
      },
      {
        x: 1000,
        y: 65.9,
        label: "65.9%",
        tooltip: ["659 / 1000 ok", "341 timeouts", "p99 380 ms among ok"],
      },
    ],
  },
  annotation:
    "This is the metric that p99 latency was hiding. Ursula and Durable Streams deliver to every client at every load point we measured; S2 Lite on S3 drops to 97.4% at 500 concurrent replays and to 65.9% at 1,000 - one in three clients never gets the document. The remaining clients on S2 do come back at moderate p99 (380 ms) which made the latency-only chart look competitive, but production users see 341 broken sessions per 1,000 reconnects.",
};

const replayLatency: Scenario = {
  key: "replay-latency",
  title: "Catch-up replay - p99 latency among successful clients",
  subtitle:
    "Same workload, p99 measured only over clients that did receive their data. Failed clients are not in this distribution; see the completion-rate chart for what is missing.",
  xLabel: "concurrent clients (each on a unique stream)",
  yLabel: "p99 latency among ok clients",
  yUnit: "ms",
  lowerIsBetter: true,
  xLevels: [100, 500, 1000],
  series: {
    ursula: [
      {
        x: 100,
        y: 6.8,
        label: "6.8 ms",
        tooltip: [
          "100 / 100 ok",
          "p99 6.8 ms",
          "172 KB body (snapshot 64 KB + tail)",
        ],
      },
      {
        x: 500,
        y: 271.6,
        label: "272 ms",
        tooltip: ["500 / 500 ok", "p99 271.6 ms"],
      },
      {
        x: 1000,
        y: 471.0,
        label: "471 ms",
        tooltip: ["1000 / 1000 ok", "p99 471 ms"],
      },
    ],
    durable: [
      {
        x: 100,
        y: 106.7,
        label: "107 ms",
        tooltip: ["100 / 100 ok", "p99 106.7 ms", "200 KB body (full log)"],
      },
      {
        x: 500,
        y: 521.7,
        label: "522 ms",
        tooltip: ["500 / 500 ok", "p99 521.7 ms"],
      },
      {
        x: 1000,
        y: 828.4,
        label: "828 ms",
        tooltip: ["1000 / 1000 ok", "p99 828.4 ms"],
      },
    ],
    s2: [
      {
        x: 100,
        y: 35.4,
        label: "35 ms",
        tooltip: ["100 / 100 ok", "p99 35.4 ms", "471 KB body"],
      },
      {
        x: 500,
        y: 346.9,
        label: "347 ms",
        tooltip: ["487 / 500 ok - 13 timeouts not counted"],
      },
      {
        x: 1000,
        y: 380.2,
        label: "380 ms",
        tooltip: ["659 / 1000 ok - 341 timeouts not counted"],
      },
    ],
  },
  annotation:
    "Ursula's snapshot path keeps the body size smallest (172 KB) and the tail-among-ok latency lowest at 1,000 concurrent clients (471 ms vs DS 828 ms). S2's number looks competitive here but it is only describing the 65.9% of clients that did receive their replay - read this chart together with the completion-rate chart above.",
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
        githubUrl="https://github.com/opendurability/ursula"
      />

      <main className="benchmark-page">
        <div className="benchmark-hero">
          <header>
            <h1>Ursula vs Durable Streams vs S2</h1>
            <p className="benchmark-lead">
              Apples-to-apples runs of the same three real-world workload
              scenarios against three durable-stream implementations on
              identical AWS Graviton hardware. The bench client and the three
              target backends are all driven from one tool (
              <code>crates/ursula-bench</code>).
            </p>
          </header>
          <aside className="benchmark-scoreboard" aria-label="Headline gaps">
            <article className="benchmark-score ursula">
              <div className="benchmark-score-name">
                Multi-stream write @ 500 streams
              </div>
              <div className="benchmark-score-pair">
                <div>
                  <b>5.9×</b>
                  <span>vs DS (file)</span>
                </div>
                <div>
                  <b>5.2×</b>
                  <span>vs S2 (S3)</span>
                </div>
              </div>
              <div className="benchmark-score-foot">
                <div>
                  <b>35.2k</b>
                  <span>Ursula ops/s peak</span>
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
                  <b>160×</b>
                  <span>vs DS p99</span>
                </div>
                <div>
                  <b>18×</b>
                  <span>vs S2 p99</span>
                </div>
              </div>
              <div className="benchmark-score-foot">
                <div>
                  <b>6.1 ms</b>
                  <span>Ursula p99</span>
                </div>
                <div>
                  <b>980 ms</b>
                  <span>DS p99 (O(N) wake)</span>
                </div>
              </div>
            </article>
            <article className="benchmark-score durable">
              <div className="benchmark-score-name">
                Catch-up replay @ 1,000 clients
              </div>
              <div className="benchmark-score-pair">
                <div>
                  <b>2.4×</b>
                  <span>vs DS p99</span>
                </div>
                <div>
                  <b>100%</b>
                  <span>Ursula completion vs 66% on S2</span>
                </div>
              </div>
              <div className="benchmark-score-foot">
                <div>
                  <b>471 ms</b>
                  <span>Ursula p99 / 0.45 s drain</span>
                </div>
                <div>
                  <b>snapshot</b>
                  <span>172 KB body vs 200 KB (DS) / 471 KB (S2)</span>
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
                <li>Every commit replicates to quorum (3 of 3)</li>
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
                Durable Streams ref
              </h3>
              <ul>
                <li>1 × c7g.4xlarge, single Node.js process</li>
                <li>Reference impl from durable-streams/durable-streams</li>
                <li>FileBackedStreamStore (dataDir on local disk)</li>
                <li>/bootstrap not implemented; replay uses ?offset=-1</li>
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
                <li>S2's own API, not Durable Streams protocol</li>
              </ul>
            </article>
          </div>
          <p>
            <strong>All three backends are persistent in this run.</strong>{" "}
            Ursula commits each write to a 3-voter Raft quorum across three
            c7g.4xlarge nodes; Durable Streams' file-backed store fsyncs to
            local disk on a single node; S2 Lite writes through to S3 on a
            single node. This is the durable-vs-durable comparison. Aggregate
            throughput reflects Ursula getting 3× the hardware in exchange for
            delivering quorum-replicated durability across AZs that the other
            two do not provide here.
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
            loop degrades linearly.
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
            that snapshot, while DS and S2 must replay the full log because
            neither ships a snapshot endpoint. All three answer the same client
            question. Read these two charts together - the first one shows
            whether the system finished at all, the second shows how fast among
            the clients that did finish.
          </p>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(420px, 1fr))",
              gap: 24,
            }}
          >
            <ScenarioBlock scenario={replayCompletion} />
            <ScenarioBlock scenario={replayLatency} />
          </div>
        </section>

        <section className="benchmark-section">
          <h2>Takeaways</h2>
          <ul style={{ color: "#ebdbb2", lineHeight: 1.6, paddingLeft: 20 }}>
            <li>
              <strong>Write throughput</strong> at 500 streams: Ursula 35.2k vs
              DS 6.0k vs S2 6.8k (5.2–5.9× gap). Multi-Raft puts streams on
              different cores and voters in parallel.
            </li>
            <li>
              <strong>SSE fan-out</strong> at 1,000 subscribers: Ursula 6.1 ms
              p99 vs DS 980 ms (O(N) wake) vs S2 111 ms (S3 floor). 160× / 18×
              gaps.
            </li>
            <li>
              <strong>Catch-up replay</strong> at 1,000 clients: completion is
              the binding metric. Ursula and DS deliver 100%; S2 only 65.9%.
              Ursula's snapshot also keeps the body smallest (172 KB).
            </li>
            <li>
              <strong>Caveats:</strong> Ursula uses 3 × c7g vs DS / S2's 1 ×
              c7g (deployment-shape comparison, not per-CPU). S2 production
              uses S3 Express which would cut its latency at ~7× the per-GB
              cost.
            </li>
          </ul>
        </section>

        <section className="benchmark-section">
          <h2>Durability and availability posture</h2>
          <p>
            Throughput and latency are only fair to compare if the durability
            properties are clear. Here is what each system actually guarantees
            in this benchmark's configuration. Ursula pays a quorum round-trip
            on every commit; S2 pays an S3 PUT; the file-backed Durable Streams
            reference fsyncs to a single EBS volume.
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
                    Durable Streams ref (file)
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
            AZ loss. The DS reference is the weakest on both axes: data on one
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
cargo build --release -p ursula-http -p ursula-bench

# 2. Bring up each backend on identical hardware
python3 scripts/ursula_ec2.py --config <manifest>.json start
~/.cargo/bin/s2 lite --port 4438 # S2 Lite on one node
# DS: clone github.com/durable-streams/durable-streams + run example server

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
