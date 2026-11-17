import { useEffect, useRef, useState } from "react";
import { DOCS_VERSION, DOCS_VERSIONS } from "../../utils/navigation";

const CURRENT = DOCS_VERSIONS.find((version) =>
  DOCS_VERSION ? version.id === DOCS_VERSION : !version.archived,
);
const LATEST = DOCS_VERSIONS.find((version) => !version.archived);

// The same page in another version when it exists there, otherwise that
// version's root. Always the trailing-slash form Pages serves.
function versionHref(version: DocsVersionInfo, slug: string) {
  return version.slugs.includes(slug) ? `${version.root}/${slug}/` : `${version.root}/`;
}

function versionLabel(version: DocsVersionInfo) {
  return version.archived ? version.id : `${version.id} (latest)`;
}

const CHECK_ICON = (
  <svg viewBox="0 0 12 12" width="12" height="12" fill="none" aria-hidden="true">
    <path
      d="M2.5 6.5L5 9L9.5 3.5"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

// A disclosure that shows the current version and lists the others on demand.
// Links between versions are plain anchors: each version is its own build.
export function VersionSwitcher({ activeSlug }: { activeSlug: string }) {
  const [isOpen, setIsOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!isOpen) return;
    const onPointerDown = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setIsOpen(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setIsOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [isOpen]);

  if (DOCS_VERSIONS.length < 2 || !CURRENT) return null;

  return (
    <div
      ref={rootRef}
      className={`docs-version-switcher${isOpen ? " docs-version-switcher-open" : ""}`}
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) setIsOpen(false);
      }}
    >
      <button
        ref={triggerRef}
        type="button"
        className="docs-version-trigger"
        aria-expanded={isOpen}
        aria-controls="docs-version-menu"
        aria-label={`Docs version: ${versionLabel(CURRENT)}`}
        onClick={() => setIsOpen((value) => !value)}
      >
        <span className="docs-version-trigger-kicker" aria-hidden="true">
          Version
        </span>
        <span className="docs-version-trigger-label">{versionLabel(CURRENT)}</span>
        <span className="docs-version-trigger-icon" aria-hidden="true">
          <svg viewBox="0 0 12 12" width="12" height="12" fill="none">
            <path
              d="M2.5 4.5L6 8L9.5 4.5"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </span>
      </button>
      <ul id="docs-version-menu" className="docs-version-menu" hidden={!isOpen}>
        {DOCS_VERSIONS.map((version) => {
          const isCurrent = version === CURRENT;
          return (
            <li key={version.id}>
              <a
                className={isCurrent ? "docs-version-option docs-version-option-active" : "docs-version-option"}
                href={versionHref(version, activeSlug)}
                aria-current={isCurrent ? "true" : undefined}
              >
                <span>{versionLabel(version)}</span>
                {isCurrent ? CHECK_ICON : null}
              </a>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

// Shown on archived pages only. data-docs-version-exit marks the links that
// leave the version on purpose, so `docs-versions.mjs verify` can skip them.
export function VersionBanner({ activeSlug }: { activeSlug: string }) {
  if (!CURRENT?.archived || !LATEST) return null;

  return (
    <aside className="docs-callout docs-callout-warning docs-version-banner" data-pagefind-ignore="all">
      <p>
        These docs are for Ursula {CURRENT.id}.x ({CURRENT.release}). {LATEST.id} is a breaking
        release with no in-place upgrade. See{" "}
        <a href={CURRENT.upgrade ?? `${LATEST.root}/`} data-docs-version-exit="">
          Upgrading to {LATEST.id}
        </a>
        , or the{" "}
        <a href={versionHref(LATEST, activeSlug)} data-docs-version-exit="">
          current version of this page
        </a>
        .
      </p>
    </aside>
  );
}
