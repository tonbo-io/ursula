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
  // A Braun panel meter: knob on top, solid case, wide face window,
  // a fan of scale ticks, the needle leaning left off vertical.
  return (
    <svg aria-hidden="true" fill="none" height="48" viewBox="0 0 48 48" width="48">
      <rect fill="currentColor" height="5" rx="1" width="8" x="20" y="6" />
      <rect fill="currentColor" height="32" rx="4" width="44" x="2" y="10" />
      <rect fill="var(--bg-base)" height="19" rx="2.5" width="35" x="6.5" y="14.5" />
      <line stroke="currentColor" strokeWidth="1.5" x1="12.9" x2="15.1" y1="21" y2="23" />
      <line stroke="currentColor" strokeWidth="1.5" x1="17.9" x2="19.1" y1="17.3" y2="20" />
      <line stroke="currentColor" strokeWidth="1.5" x1="24" x2="24" y1="16" y2="19" />
      <line stroke="currentColor" strokeWidth="1.5" x1="30.1" x2="28.9" y1="17.3" y2="20" />
      <line stroke="currentColor" strokeWidth="1.5" x1="35.1" x2="32.9" y1="21" y2="23" />
      <line stroke="var(--bg-accent)" strokeLinecap="round" strokeWidth="2.5" x1="24" x2="15" y1="31" y2="20.3" />
      <circle cx="24" cy="31" fill="currentColor" r="2.5" />
    </svg>
  );
}

function GlyphLatest() {
  // Thin long rail, thin ticks, one massive block — and a bright cursor.
  // The block tops out at y6 so the silhouette matches docs/proof (y6–42).
  return (
    <svg aria-hidden="true" fill="none" height="48" viewBox="0 0 48 48" width="48">
      <line stroke="currentColor" strokeWidth="2" x1="2" x2="46" y1="41" y2="41" />
      <rect fill="currentColor" height="16" width="3" x="4" y="24" />
      <rect fill="currentColor" height="10" width="3" x="10" y="30" />
      <rect fill="currentColor" height="34" width="9" x="16" y="6" />
      <rect fill="currentColor" height="20" width="3" x="28" y="20" />
      <rect fill="currentColor" height="12" width="3" x="34" y="28" />
      <rect fill="var(--bg-accent)" height="27" width="3" x="40" y="13" />
      <circle cx="41.5" cy="9" fill="var(--bg-accent)" r="3" />
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
