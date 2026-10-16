export const HOME_PATH = "/";
export const DOCS_PATH = "/docs";
export const DOCS_PAGE_PREFIX = "/docs/";
// The docs version this bundle renders: null for the latest docs at /docs,
// or an archived id such as "0.7" for the build served at /docs/0.7.
export const DOCS_VERSION = __DOCS_VERSION__;
export const DOCS_VERSIONS = __DOCS_VERSIONS__;
// Public root of the docs this bundle renders. Inside the app, docs routes
// stay "/docs/..." and buildAppHref maps them to this root.
export const DOCS_ROOT = DOCS_VERSION ? `${DOCS_PATH}/${DOCS_VERSION}` : DOCS_PATH;
export const DOCS_TITLE_SUFFIX = DOCS_VERSION ? `Ursula ${DOCS_VERSION}` : "Ursula";
export const PAGEFIND_BASE = DOCS_VERSION ? `${DOCS_ROOT}/pagefind` : "/pagefind";
export const BENCHMARK_PATH = "/benchmark";
export const CHAOS_TEST_PATH = "/chaos-test";
export const STATUS_PATH = "/status";
export const BLOG_PATH = "/blog";
export const BLOG_PAGE_PREFIX = "/blog/";

const basePath = import.meta.env.BASE_URL.replace(/\/$/, "");

const DOCS_ROUTE = /^\/docs(?=[/#?]|$)/;
const DOCS_ROOT_ROUTE = new RegExp(`^${DOCS_ROOT.replace(/\./g, "\\.")}(?=[/#?]|$)`);
const ARCHIVED_ROUTE = new RegExp(
  `^/docs/(${DOCS_VERSIONS.filter((version) => version.archived)
    .map((version) => version.id.replace(/\./g, "\\."))
    .join("|") || "(?!)"})(?=[/#?]|$)`,
);

// Maps an in-app docs route to the public path of this bundle's version.
function toPublicPath(path: string) {
  if (!DOCS_VERSION || DOCS_ROOT_ROUTE.test(path)) return path;
  return path.replace(DOCS_ROUTE, DOCS_ROOT);
}

export function buildAppHref(path: string) {
  const normalizedPath = path === HOME_PATH ? HOME_PATH : path.startsWith("/") ? path : `/${path}`;
  return `${basePath}${normalizedPath === HOME_PATH ? "/" : toPublicPath(normalizedPath)}`;
}

// Whether this bundle renders the path. Files (llms.txt) and other docs
// versions are served by a different build, so they take a full page load.
function isRenderedHere(path: string) {
  const pathname = path.split(/[?#]/)[0] ?? path;
  if (/\.[a-z][a-z0-9]*$/i.test(pathname)) return false;
  return DOCS_VERSION ? DOCS_ROUTE.test(pathname) : !ARCHIVED_ROUTE.test(pathname);
}

export function getCurrentAppPath(pathname?: string) {
  const resolvedPathname =
    pathname ?? (typeof window !== "undefined" ? window.location.pathname : HOME_PATH);
  const pathWithoutBase =
    basePath && resolvedPathname.startsWith(basePath)
      ? resolvedPathname.slice(basePath.length) || HOME_PATH
      : resolvedPathname;
  const trimmedPath = pathWithoutBase.replace(/\/+$/, "") || HOME_PATH;
  const normalizedPath = DOCS_VERSION
    ? trimmedPath.replace(DOCS_ROOT_ROUTE, DOCS_PATH)
    : trimmedPath;

  if (normalizedPath === DOCS_PATH) {
    return DOCS_PATH;
  }

  if (normalizedPath.startsWith(DOCS_PAGE_PREFIX) && normalizedPath !== DOCS_PATH) {
    return normalizedPath;
  }

  if (normalizedPath === BENCHMARK_PATH) {
    return BENCHMARK_PATH;
  }

  if (normalizedPath === CHAOS_TEST_PATH || normalizedPath === STATUS_PATH) {
    return CHAOS_TEST_PATH;
  }

  if (normalizedPath === BLOG_PATH) {
    return BLOG_PATH;
  }

  if (normalizedPath.startsWith(BLOG_PAGE_PREFIX) && normalizedPath !== BLOG_PATH) {
    return normalizedPath;
  }

  return HOME_PATH;
}

export function getBlogPostPath(slug: string) {
  return `${BLOG_PATH}/${slug}`;
}

export function getBlogSlugFromPath(path: string) {
  if (!path.startsWith(BLOG_PAGE_PREFIX)) {
    return null;
  }
  return path.slice(BLOG_PAGE_PREFIX.length) || null;
}

export function getDocsPagePath(slug: string) {
  return `${DOCS_PATH}/${slug}`;
}

export function getDocsSlugFromPath(path: string) {
  if (path === DOCS_PATH) {
    return "";
  }

  if (!path.startsWith(DOCS_PAGE_PREFIX)) {
    return null;
  }

  return path.slice(DOCS_PAGE_PREFIX.length) || null;
}

export function isInternalAppPath(path: string) {
  return path.startsWith("/");
}

export function navigateTo(path: string) {
  const nextHref = buildAppHref(path);
  if (!isRenderedHere(nextHref)) {
    window.location.assign(nextHref);
    return;
  }
  const currentHref = `${window.location.pathname}${window.location.search}${window.location.hash}`;

  if (currentHref === nextHref) {
    return;
  }

  window.history.pushState({}, "", nextHref);
  window.dispatchEvent(new PopStateEvent("popstate"));
  window.scrollTo({ top: 0, behavior: "auto" });
}
