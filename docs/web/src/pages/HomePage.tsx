import { useEffect, useState } from "react";

import EdgeStream from "../components/EdgeStream";
import Footer from "../components/Footer";
import Header from "../components/Header";
import { buildAppHref, isInternalAppPath, navigateTo } from "../utils/navigation";
import { bucketHistory, statusWorse, type HealthHistoryPoint } from "../utils/chaosHealth";

/** Requantize hourly cells into wider buckets, worst status wins.
    Phones show the same 7 days at 6-hour resolution instead of
    truncating the record. */
function coarsen(history: Array<string | null>, factor: number): Array<string | null> {
  const out: Array<string | null> = [];
  for (let i = 0; i < history.length; i += factor) {
    let worst: string | null = null;
    for (const st of history.slice(i, i + factor)) {
      if (st != null) worst = worst == null ? st : statusWorse(worst, st);
    }
    out.push(worst);
  }
  return out;
}

const CHAOS_STATUS_URL =
  (import.meta.env.VITE_CHAOS_STATUS_URL as string | undefined) ||
  (import.meta.env.DEV
    ? "/__chaos-proxy/status.json"
    : "https://ursula-chaos-status-tonbo.s3.amazonaws.com/status.json");

type LiveChaos = {
  startedAt: number;
  faults: number;
  corruptions: number;
  verified: number;
  overall: string;
  summary: string | null;
  updatedAt: number | null;
  history: Array<string | null>;
};

/** Same bucketing as the chaos test page — shared code, shared truth. */
function bucketHourly(points: unknown, now: number): Array<string | null> {
  const history = (Array.isArray(points) ? points : []) as HealthHistoryPoint[];
  return bucketHistory(history, 3_600_000, 168, now).map((cell) =>
    cell.status === "unknown" ? null : String(cell.status),
  );
}

/** The 24/7 chaos test feed — the product photographing itself. */
function useLiveChaos(): LiveChaos | null {
  const [live, setLive] = useState<LiveChaos | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const response = await fetch(`${CHAOS_STATUS_URL}?t=${Date.now()}`, { cache: "no-store" });
        if (!response.ok) return;
        const data = await response.json();
        const now = Date.now();
        const startedAt = data?.started_at ? new Date(data.started_at).getTime() : NaN;
        if (!Number.isFinite(startedAt)) return;
        const faults = data?.chaos?.injection_count;
        const corruptions = data?.integrity?.setsum_mismatch_count;
        if (typeof faults !== "number" || typeof corruptions !== "number") return;
        const verified = data?.integrity?.verified_offsets;
        const updatedAt = data?.updated_at ? new Date(data.updated_at).getTime() : NaN;
        if (cancelled) return;
        setLive({
          startedAt,
          faults,
          corruptions,
          verified: typeof verified === "number" ? verified : 0,
          overall: typeof data?.overall === "string" ? data.overall : "unknown",
          summary: typeof data?.summary === "string" ? data.summary : null,
          updatedAt: Number.isFinite(updatedAt) ? updatedAt : null,
          history: bucketHourly(data?.history, now),
        });
      } catch {
        // Static page keeps working without the live feed.
      }
    }

    void load();
    const timer = window.setInterval(() => void load(), 30_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  return live;
}

function formatAgo(sec: number) {
  if (sec < 90) return `${sec} s ago`;
  return `${Math.round(sec / 60)} m ago`;
}

const EMPTY_HISTORY: Array<string | null> = Array.from({ length: 168 }, () => null);

/** Transport clock: bright digits, small engraved units, ticking.
    With no feed it shows the LCD rest state, all dashes. */
function ClockDigits({ ms }: { ms: number | null }) {
  if (ms == null) {
    return (
      <>
        --<i>d</i>--<i>h</i>--<i>m</i>--<i>s</i>
      </>
    );
  }
  const total = Math.max(0, Math.floor(ms / 1000));
  const d = Math.floor(total / 86_400);
  const hh = String(Math.floor((total % 86_400) / 3_600)).padStart(2, "0");
  const mm = String(Math.floor((total % 3_600) / 60)).padStart(2, "0");
  const ss = String(total % 60).padStart(2, "0");
  return (
    <>
      {d}
      <i>d</i>
      {hh}
      <i>h</i>
      {mm}
      <i>m</i>
      {ss}
      <i>s</i>
    </>
  );
}

type LinkProps = {
  href: string;
  children: React.ReactNode;
  className?: string;
};

function AppLink({ href, children, className }: LinkProps) {
  const internal = isInternalAppPath(href);
  return (
    <a
      className={className}
      href={internal ? buildAppHref(href) : href}
      target={internal ? undefined : "_blank"}
      rel={internal ? undefined : "noopener noreferrer"}
      onClick={
        internal
          ? (event) => {
              event.preventDefault();
              navigateTo(href);
            }
          : undefined
      }
    >
      {children}
    </a>
  );
}

/* Miniature portraits, one per keep — the reader's own objects drawn
   TE-style: free-standing, one technique each, ink + one orange mark. */
function KeepHost() {
  // Two rack units reduced to geometry: aligned slabs, one slot each,
  // one power lamp. (The quorum glyph cascades; this one stacks.)
  return (
    <svg aria-hidden="true" fill="none" height="80" viewBox="0 0 80 80" width="80">
      <rect fill="currentColor" height="21" width="64" x="8" y="16" />
      <rect fill="var(--glyph-bg)" height="2.5" width="20" x="15" y="25" />
      <circle cx="63" cy="26.5" fill="var(--bg-accent)" r="3" />
      <rect fill="currentColor" height="21" width="64" x="8" y="43" />
      <rect fill="var(--glyph-bg)" height="2.5" width="20" x="15" y="52" />
    </svg>
  );
}

function KeepClock() {
  // A dial reduced to geometry: the disc, one tick at twelve, and the
  // needle. The deflection is the latency.
  return (
    <svg aria-hidden="true" fill="none" height="80" viewBox="0 0 80 80" width="80">
      <circle cx="40" cy="42" fill="currentColor" r="32" />
      <line stroke="var(--glyph-bg)" strokeWidth="2.5" x1="40" x2="40" y1="13" y2="19" />
      <line stroke="var(--bg-accent)" strokeLinecap="round" strokeWidth="3" x1="40" x2="52" y1="42" y2="24" />
      <circle cx="40" cy="42" fill="var(--glyph-bg)" r="3" />
    </svg>
  );
}

function KeepS3() {
  // One heavy cylinder; two thin curves — one knocked out, one orange
  return (
    <svg aria-hidden="true" fill="none" height="80" viewBox="0 0 80 80" width="80">
      <ellipse cx="40" cy="16" fill="currentColor" rx="25" ry="8" />
      <rect fill="currentColor" height="48" width="50" x="15" y="16" />
      <ellipse cx="40" cy="64" fill="currentColor" rx="25" ry="8" />
      {/* One wrapped band, filled between two arcs (a stroke's butt cap
          would sit perpendicular to the tangent instead of flush with
          the vertical silhouette). */}
      <path d="M15 39 a25 8 0 0 0 50 0 l0 4 a25 8 0 0 1 -50 0 Z" fill="var(--bg-accent)" />
    </svg>
  );
}

function KeepQuorum() {
  // Three replicas stacked front over back: 2:1 slabs in arithmetic sizes
  // (24/40/56), each overlapping the one behind (3px, 5px) so occlusion
  // carries the depth. No seams — the two solid slabs merge where they
  // touch. Corner radii scale with the slab (2/3/4). The hollow one,
  // furthest back, is the one you may lose.
  return (
    <svg aria-hidden="true" fill="none" height="80" viewBox="0 0 80 80" width="80">
      <rect fill="var(--glyph-bg)" height="12" stroke="currentColor" strokeWidth="2" width="24" x="48" y="14" />
      <rect fill="currentColor" height="20" width="40" x="28" y="23" />
      <rect fill="currentColor" height="28" stroke="var(--glyph-bg)" strokeWidth="2" width="56" x="8" y="38" />
      <circle cx="20" cy="52" fill="var(--bg-accent)" r="4" />
    </svg>
  );
}

const KEEP_GLYPHS: Record<string, () => JSX.Element> = {
  host: KeepHost,
  clock: KeepClock,
  s3: KeepS3,
  quorum: KeepQuorum,
};
const KEEPS = [
  {
    title: "Open-source self-hosting",
    body: "Apache-2.0, the complete server. Deploys as one binary, a Docker image, or a Helm chart.",
    glyph: "host",
  },
  {
    title: "Low write latency",
    body: "Appends commit in a quorum-replicated in-memory ring. No batching window, no S3 PUT on the write path.",
    glyph: "clock",
  },
  {
    title: "Plain S3 economics",
    body: "Cold tier on S3 Standard. No Express tier, no per-GB SaaS markup.",
    glyph: "s3",
  },
  {
    title: "Quorum-replicated durability",
    body: "Acknowledged writes survive a single-node failure.",
    glyph: "quorum",
  },
];

const ARCHITECTURE = [
  {
    term: "Thread-per-core × multi-Raft",
    body: "Each stream hashes to one Raft group and one owner core. Hot-path requests touch that core only. A slow follower in one group never stalls another.",
  },
  {
    term: "Hot ring",
    body: "Writes commit at Raft quorum in memory, so appends take single-digit milliseconds with no separate broker and no batched commits.",
  },
  {
    term: "S3 cold tier",
    body: "Older segments flush to S3 in the background. One GET transparently spans hot and cold.",
  },
];


function HomePage() {
  const live = useLiveChaos();

  // The instrument ticks: re-render once a second so the running clock and
  // the "updated" counter move like a transport display.
  const [, setTick] = useState(0);
  useEffect(() => {
    const timer = window.setInterval(() => setTick((v) => v + 1), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <>
      <Header
        navItems={[
          { label: "Docs", href: "/docs" },
          { label: "Blog", href: "/blog" },
          { label: "Benchmark", href: "/benchmark" },
          { label: "Chaos Test", href: "/chaos-test" },
        ]}
        version={__URSULA_VERSION__}
        githubUrl="https://github.com/tonbo-io/ursula"
      />

      <main className="home">
        {/* The protocol is simple enough to be the hero. Show it, don't describe it. */}
        <section className="home-hero">
          <div className="home-inner">
            <div className="home-hero-grid">
          <div className="home-hero-copy">
            <h1>
              Durable streams
              <br />
              over
              <br />
              <span className="hx">plain&nbsp;HTTP</span>,
              <br />
              backed by&nbsp;S3.
            </h1>
            <p>
              One durable timeline per document, session, or agent run. Replayable,
              tailable, self-hosted.
            </p>
            <div className="home-hero-links">
              <AppLink className="home-link-primary" href="/docs/quick-start">
                Quick start →
              </AppLink>
              <AppLink className="home-link" href="https://github.com/tonbo-io/ursula">
                GitHub →
              </AppLink>
            </div>
          </div>

          <figure className="home-terminal">
            <pre>
              <code>
                <span className="t-comment"># create a bucket, then a stream</span>
                {"\n"}
                <span className="t-prompt">$</span> curl -X PUT http://127.0.0.1:4437/demo
                {"\n"}
                <span className="t-prompt">$</span> curl -X PUT http://127.0.0.1:4437/demo/hello
                {"\n\n"}
                <span className="t-comment"># append, acknowledged at Raft quorum</span>
                {"\n"}
                <span className="t-prompt">$</span> curl -X POST http://127.0.0.1:4437/demo/hello \
                {"\n"}
                {"    "}--data-binary 'hello world'
                {"\n\n"}
                <span className="t-comment"># replay from any offset, or tail live</span>
                {"\n"}
                <span className="t-prompt">$</span> curl 'http://127.0.0.1:4437/demo/hello?offset=-1'
                {"\n"}
                <span className="t-output">hello world</span>
              </code>
            </pre>
            <figcaption className="home-claim">
              Any HTTP client is a valid client. No SDK.
            </figcaption>
          </figure>
            </div>
          </div>
        </section>

        <section className="home-band home-band-orange edge-ink">
          <EdgeStream />
          <div className="home-inner">
          <h2 className="home-label">Measured</h2>
          <div className="home-stat-hero">
            <span className="hsh-value">
              7.4<i className="hsh-unit">ms</i>
            </span>
            <span className="hsh-label">
              p99 append · 100 streams · quorum ack across 3 availability zones
            </span>
          </div>
          <dl className="home-stats home-stats-row">
            <div>
              <dd>41,552</dd>
              <dt>commits/s aggregate · 500 streams</dt>
            </div>
            <div>
              <dd>8.3 ms</dd>
              <dt>sse fan-out p99 · 1,000 subscribers, one stream</dt>
            </div>
            <div>
              <dd>253 ms</dd>
              <dt>catch-up replay p99 · 1,000 cold clients</dt>
            </div>
          </dl>
          <p className="home-footnote home-spec">
            measured 2026-05-22 · 3 × c7g.4xlarge · s3 cold flush on, ~675 MiB uploaded ·
            256 B payloads · one client binary
          </p>
          <p className="home-footnote home-exit">
            <AppLink className="home-link" href="/benchmark">
              Full method and data →
            </AppLink>
          </p>
          </div>
        </section>

        <section className="home-band">
          <div className="home-inner">
          <h2 className="home-label">What Ursula keeps</h2>
          <p className="home-lede">
            Every other server we evaluated for the Durable Streams Protocol gives up at least
            one of these four. Keeping all four is Ursula's reason to exist.
          </p>
          <ol className="home-keeps">
{KEEPS.map((keep, index) => (
              <li key={keep.title}>
                <i aria-hidden="true" className="keep-glyph">
                  {KEEP_GLYPHS[keep.glyph]?.()}
                </i>
                <div className="keep-entry">
                  <div className="keep-head">
                    <b className="keep-no">{String(index + 1).padStart(2, "0")}</b>
                    <h3>{keep.title}</h3>
                  </div>
                  <p>{keep.body}</p>
                </div>
              </li>
            ))}
          </ol>
          <p className="home-footnote home-exit">
            <AppLink className="home-link" href="/docs/why-ursula">
              Why Ursula →
            </AppLink>
          </p>
          </div>
        </section>

        {/* Proven — the green room: durability evidence, alive */}
        <section className="home-band home-band-green edge-paper">
          <EdgeStream />
          <div className="home-inner">
            <h2 className="home-label">Proven live</h2>
            <h3 className="hp-title">
              Survives single-node failure,
              <br />
              on record.
            </h3>
            <p className="home-lede">
              A 3-node cluster on EC2 takes faults around the clock: nodes killed, networks
              partitioned, clocks skewed. Every read is verified against a running
              checksum.
            </p>
            <div className="hp-cluster">
              <div className="hp-head">
                {/* A row of lamps, each labelled with what it indicates;
                    the prose detail rides along as title/aria-label. With
                    no feed the panel stands by: lamps unlit, feed amber,
                    dashes in the windows. */}
                <span className="hp-lamp" title={live?.summary ?? "no telemetry"}>
                  <i
                    className={`home-live-dot${
                      live == null
                        ? " home-live-dot-off"
                        : live.overall === "operational"
                          ? ""
                          : live.overall === "major_outage"
                            ? " home-live-dot-bad"
                            : " home-live-dot-warn"
                    }`}
                    role="img"
                    aria-label={live?.summary ?? "cluster status unknown"}
                  />
                  nodes
                </span>
                <span
                  className="hp-lamp"
                  title={live ? `${live.corruptions} data corruptions detected` : "no telemetry"}
                >
                  <i
                    className={`home-live-dot${
                      live == null ? " home-live-dot-off" : live.corruptions > 0 ? " home-live-dot-bad" : ""
                    }`}
                    role="img"
                    aria-label={
                      live ? `${live.corruptions} data corruptions detected` : "integrity unknown"
                    }
                  />
                  integrity
                </span>
                <span className="hp-lamp" title="telemetry freshness">
                  <i
                    className={`home-live-dot${
                      live?.updatedAt != null && Date.now() - live.updatedAt < 90_000
                        ? ""
                        : " home-live-dot-warn"
                    }`}
                    role="img"
                    aria-label="telemetry freshness"
                  />
                  feed
                </span>
                <span className="hp-updated">
                  chaos test · ec2 ·{" "}
                  {live?.updatedAt != null
                    ? `updated ${formatAgo(Math.max(0, Math.round((Date.now() - live.updatedAt) / 1000)))}`
                    : "feed unreachable"}
                </span>
              </div>
              <dl className="hp-meters">
                <div>
                  <dd><ClockDigits ms={live ? Date.now() - live.startedAt : null} /></dd>
                  <dt>running</dt>
                </div>
                <div>
                  <dd>{live ? live.faults : "----"}</dd>
                  <dt>faults injected</dt>
                </div>
                <div>
                  <dd>{live ? live.verified : "------"}</dd>
                  <dt>offsets verified</dt>
                </div>
                <div className={live && live.corruptions > 0 ? "hp-bad" : undefined}>
                  <dd>{live ? live.corruptions : "-"}</dd>
                  <dt>data corruptions</dt>
                </div>
              </dl>
              <div>
                <div
                  className="hp-strip hp-strip-fine"
                  role="img"
                  aria-label="Cluster health over the last 7 days, one cell per hour"
                >
                  {(live?.history ?? EMPTY_HISTORY).map((st, i) => (
                    <i key={i} className={`hp-cell${st ? ` hp-${st}` : ""}`} />
                  ))}
                </div>
                <div
                  className="hp-strip hp-strip-coarse"
                  role="img"
                  aria-label="Cluster health over the last 7 days, one cell per six hours"
                >
                  {coarsen(live?.history ?? EMPTY_HISTORY, 6).map((st, i) => (
                    <i key={i} className={`hp-cell${st ? ` hp-${st}` : ""}`} />
                  ))}
                </div>
                <div className="hp-axis">
                  <span>7 d ago</span>
                  <span>now</span>
                </div>
              </div>
            </div>
            <p className="home-footnote home-exit">
              <AppLink className="home-link" href="/chaos-test">
                Full record →
              </AppLink>
            </p>
          </div>
        </section>

        <section className="home-band home-band-recess">
          <div className="home-inner">
          <h2 className="home-label">How</h2>
          <h3 className="ha-title">
            Replicate in memory,
            <br />
            flush to S3 in the background.
          </h3>
          <div className="ha-scroll">
            <div
              className="lt-grid"
              role="img"
              aria-label="Write path as a printed circuit: an append enters at the input pin, runs through the owner core and reaches a 2-of-3 Raft quorum. The meter on the ACK block moves in single-digit milliseconds, and S3 is never on this path. From the owner core's hot ring, older segments drain down to S3 in the background. The read rail joins a hot tap after the quorum with the cold rail from S3, so one GET spans hot and cold."
            >
              <span className="lt lt-in">
                <b className="lt-name">append</b>
                <i className="lt-sub">http post</i>
              </span>
              <span className="lt lt-comp lt-w2 lt-drain">
                <em className="lt-box" aria-hidden="true" />
                <b className="lt-name">owner core</b>
                <i className="lt-sub">hot ring · in memory</i>
              </span>
              <span className="lt lt-comp lt-w2">
                <em className="lt-box" aria-hidden="true" />
                <b className="lt-name">raft quorum</b>
                <i className="lt-sub">2 of 3 voters</i>
              </span>
              <span className="lt lt-junction" aria-hidden="true" />
              <span className="lt lt-meter">
                <em className="lt-gauge" aria-hidden="true">
                  <u className="lt-needle" />
                </em>
                <b className="lt-name">ack</b>
                <i className="lt-sub">single-digit ms</i>
              </span>
              <span className="lt-slot" aria-hidden="true" />
              <span className="lt-slot" aria-hidden="true" />
              <span className="lt lt-branch" aria-hidden="true">
                <i className="lt-sub lt-vlab">background flush</i>
              </span>
              <span className="lt-slot" aria-hidden="true" />
              <span className="lt-slot" aria-hidden="true" />
              <span className="lt lt-branch" aria-hidden="true">
                <i className="lt-sub lt-vlab">hot</i>
              </span>
              <span className="lt-slot" aria-hidden="true" />
              <span className="lt-slot" aria-hidden="true" />
              <span className="lt lt-s3 lt-w2">
                <em className="lt-box" aria-hidden="true" />
                <b className="lt-name">s3 · cold tier</b>
                <i className="lt-sub">plain s3 standard</i>
              </span>
              <span className="lt lt-wire" aria-hidden="true">
                <i className="lt-sub lt-wlab">cold</i>
              </span>
              <span className="lt lt-wire" aria-hidden="true" />
              <span className="lt lt-junction lt-junction-up" aria-hidden="true" />
              <span className="lt lt-get">
                <b className="lt-name">get</b>
                <i className="lt-sub">one read · hot + cold</i>
              </span>
              <span className="lt-titleblock" aria-hidden="true">
                <b>ursula · write path</b>
                <i>{`v${__URSULA_VERSION__} · port 4437`}</i>
              </span>
            </div>
          </div>
          <dl className="home-arch">
            {ARCHITECTURE.map((row) => (
              <div key={row.term}>
                <dt>{row.term}</dt>
                <dd>{row.body}</dd>
              </div>
            ))}
          </dl>
          <p className="home-footnote home-exit">
            <AppLink className="home-link" href="/docs/architecture/overview">
              Architecture →
            </AppLink>
          </p>
          </div>
        </section>

        <section className="home-band home-band-yellow edge-dark">
          <EdgeStream />
          <div className="home-inner">
          <h2 className="home-label">When it's the wrong shape</h2>
          <h3 className="hw-title">
            Where do your clients run,
            <br />
            and what is one stream?
          </h3>
          <p className="home-lede">
            These two questions decide the fit. In-network services pushing a few
            high-throughput pipelines is the wrong shape:
          </p>
          <ul className="hw-routes">
            <li>
              <b>Kafka / Redpanda</b>
              <span>A few high-throughput topics, in-network consumer groups</span>
            </li>
            <li>
              <b>S2</b>
              <span>Managed service, willing to adopt a vendor API</span>
            </li>
            <li>
              <b>S3 directly</b>
              <span>Immutable blobs, no append or tail</span>
            </li>
            <li>
              <b>etcd</b>
              <span>Small consistent cluster-wide state</span>
            </li>
            <li className="hw-ours">
              <AppLink className="hw-ursula" href="/docs/why-ursula">
                <b>Ursula</b>
                <span>
                  Many small per-resource streams, open-internet HTTP clients, replay and live
                  tail in one primitive
                </span>
                <i aria-hidden="true">→</i>
              </AppLink>
            </li>
          </ul>
          <p className="home-footnote home-exit">
            <AppLink className="home-link" href="/docs/competitive-comparison">
              Full comparison →
            </AppLink>
          </p>
          </div>
        </section>

        <section className="home-band home-band-dark home-band-short edge-yellow">
          <EdgeStream />
          <div className="home-inner">
          <h2 className="home-label">Run it</h2>
          <div className="home-run">
            <pre>
              <code>
                <span className="t-comment"># single in-memory node on :4437</span>
                {"\n"}
                <span className="t-prompt">$</span> docker run --rm -p 4437:4437 ghcr.io/tonbo-io/ursula:{__URSULA_VERSION__}
              </code>
            </pre>
          </div>
          <p className="home-footnote home-exit">
            <AppLink className="home-link" href="/docs/quick-start">
              Quick start →
            </AppLink>
            <i className="home-sep" aria-hidden="true">·</i>
            <AppLink className="home-link" href="/docs/specs/durable-stream">
              Protocol spec →
            </AppLink>
            <i className="home-sep" aria-hidden="true">·</i>
            <AppLink className="home-link" href="/llms.txt">
              llms.txt →
            </AppLink>
          </p>
          </div>
        </section>
      </main>

      <Footer />
    </>
  );
}

export default HomePage;
