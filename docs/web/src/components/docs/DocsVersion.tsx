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

// Links between versions are plain anchors: each version is its own build.
export function VersionSwitcher({ activeSlug }: { activeSlug: string }) {
  if (DOCS_VERSIONS.length < 2) return null;

  return (
    <nav className="docs-version-switcher" aria-label="Docs version">
      {DOCS_VERSIONS.map((version) => {
        const isCurrent = version === CURRENT;
        return (
          <a
            key={version.id}
            className={isCurrent ? "docs-version-link docs-version-link-active" : "docs-version-link"}
            href={versionHref(version, activeSlug)}
            aria-current={isCurrent ? "true" : undefined}
          >
            {version.archived ? version.id : `${version.id} (latest)`}
          </a>
        );
      })}
    </nav>
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
