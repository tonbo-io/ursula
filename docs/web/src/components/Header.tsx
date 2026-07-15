import { buildAppHref, isInternalAppPath, navigateTo } from "../utils/navigation";

type HeaderNavItem = {
  label: string;
  href?: string;
  active?: boolean;
  caret?: boolean;
  disabled?: boolean;
  children?: { label: string; href: string }[];
};

type HeaderCta = {
  label: string;
  href: string;
};

type HeaderProps = {
  navItems: HeaderNavItem[];
  version?: string;
  githubUrl?: string;
  cta?: HeaderCta;
  className?: string;
};

/* Miniature portraits of the site's own objects, TE-style: free-standing,
   mixed technique, ink + one orange mark. Basic shapes only. */
function GlyphDocs() {
  return (
    <svg aria-hidden="true" fill="none" height="48" viewBox="0 0 48 48" width="48">
      <rect fill="var(--bg-elevated)" height="36" rx="3" stroke="currentColor" strokeWidth="1.5" width="45" x="1.5" y="6" />
      <line stroke="currentColor" strokeWidth="1.5" x1="5" x2="43" y1="17" y2="17" />
      <rect fill="var(--bg-elevated)" height="8" stroke="currentColor" strokeWidth="1.5" width="11" x="12" y="13" />
      <circle cx="31" cy="17" fill="currentColor" r="2" />
      <line stroke="currentColor" strokeWidth="1.5" x1="31" x2="31" y1="17" y2="29" />
      <rect fill="var(--bg-elevated)" height="6" stroke="currentColor" strokeWidth="1.5" width="10" x="26" y="29" />
      <rect fill="currentColor" height="4.5" width="9" x="5" y="33" />
    </svg>
  );
}

function GlyphProof() {
  // The Braun radio tuning window: a plain slab with a fan-shaped
  // cutout, one thin arc of scale, a horizontal needle and the orange
  // triangle marker. The silhouette does the talking.
  return (
    <svg aria-hidden="true" fill="none" height="48" viewBox="0 0 48 48" width="48">
      {/* Drawn on a 48 grid, mapped into the shared y6-42 content band;
          stroke widths compensate for the 0.75 scale. */}
      <g transform="translate(6 6) scale(0.75)">
        <rect fill="currentColor" height="48" rx="4" width="48" x="0" y="0" />
        <path
          d="M30.4 5.6 A24 24 0 0 1 27 44.8 L19.5 31.8 A9 9 0 0 0 20.8 17.1 Z"
          fill="var(--bg-base)"
        />
        <line stroke="var(--bg-accent)" strokeWidth="3" x1="21" x2="31.5" y1="24.5" y2="24.5" />
        <path d="M38 21.5 L38 28.5 L32.5 25 Z" fill="var(--bg-accent)" />
      </g>
    </svg>
  );
}

function GlyphLatest() {
  // The record, reduced to geometry: a disc, its spindle, and the
  // orange point on the groove that is playing right now.
  return (
    <svg aria-hidden="true" fill="none" height="48" viewBox="0 0 48 48" width="48">
      <circle cx="24" cy="24" fill="currentColor" r="18" />
      <circle cx="24" cy="24" fill="var(--bg-base)" r="3.5" />
      <circle cx="31.8" cy="16.2" fill="var(--bg-accent)" r="3" />
    </svg>
  );
}

const GLYPHS: Record<string, () => JSX.Element> = {
  comp: GlyphDocs,
  gauge: GlyphProof,
  stream: GlyphLatest,
};

type CatalogGroup = {
  key: string;
  label: string;
  href: string;
  glyph: "comp" | "gauge" | "stream";
  match: string[];
  sub: { label: string; href: string }[];
};

// The header is a catalog: every destination printed in the open — no
// dropdowns, nothing hidden. Each group wears a pictogram drawn from the
// site's own circuit vocabulary.
const CATALOG: CatalogGroup[] = [
  {
    key: "docs",
    label: "docs",
    href: "/docs",
    glyph: "comp",
    match: ["/docs"],
    sub: [
      { label: "quick start", href: "/docs/quick-start" },
      { label: "api reference", href: "/docs/api/overview" },
      { label: "protocol spec", href: "/docs/specs/durable-stream" },
    ],
  },
  {
    key: "proof",
    label: "proof",
    href: "/benchmark",
    glyph: "gauge",
    match: ["/benchmark", "/chaos-test"],
    sub: [
      { label: "benchmark", href: "/benchmark" },
      { label: "chaos test", href: "/chaos-test" },
    ],
  },
  {
    key: "latest",
    label: "latest",
    href: "/blog",
    glyph: "stream",
    match: ["/blog"],
    sub: [
      { label: "blog", href: "/blog" },
      { label: "github", href: "__github__" },
      { label: "llms.txt", href: "/llms.txt" },
    ],
  },
];

function Header({ navItems, version, githubUrl, className }: HeaderProps) {
  const activeHref = navItems.find((item) => item.active)?.href ?? "";

  const handleClick = (href: string) => (event: React.MouseEvent<HTMLElement>) => {
    if (!isInternalAppPath(href)) {
      return;
    }
    event.preventDefault();
    navigateTo(href);
  };

  const renderLink = (href: string, label: string, cls: string) => {
    const resolved = href === "__github__" ? (githubUrl ?? "https://github.com/tonbo-io/ursula") : href;
    const internal = isInternalAppPath(resolved);
    return (
      <a
        className={cls}
        href={internal ? buildAppHref(resolved) : resolved}
        target={internal ? undefined : "_blank"}
        rel={internal ? undefined : "noopener noreferrer"}
        onClick={handleClick(resolved)}
      >
        {label}
      </a>
    );
  };

  return (
    <header className={className ? `site-header ${className}` : "site-header"}>
      <a
        aria-label="Ursula"
        className="header-brand"
        href={buildAppHref("/")}
        onClick={handleClick("/")}
      >
        <span className="hb-name">ursula</span>
        <span className="hb-plate">
          durable streams
          <br />
          {version ? `v${version} · ` : ""}apache-2.0
        </span>
      </a>

      <nav aria-label="Primary" className="header-catalog">
        {CATALOG.map((group) => {
          const active =
            activeHref !== "" && group.match.some((m) => activeHref.startsWith(m));
          return (
            <div className={active ? "hgroup hgroup-active" : "hgroup"} key={group.key}>
              <span aria-hidden="true" className="hglyph">
                {GLYPHS[group.glyph]?.()}
              </span>
              <div className="hgroup-body">
                {renderLink(group.href, group.label, "hgroup-title")}
                <ul className="hgroup-sub">
                  {group.sub.map((item) => (
                    <li key={item.label}>{renderLink(item.href, item.label, "hgroup-link")}</li>
                  ))}
                </ul>
              </div>
            </div>
          );
        })}
      </nav>

      <p aria-hidden="true" className="header-statement">
        put creates a stream.
        <br />
        post appends at quorum.
        <br />
        get replays or tails.
      </p>

      <span aria-hidden="true" className="header-mark" />
    </header>
  );
}

export default Header;
