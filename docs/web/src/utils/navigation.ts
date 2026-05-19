export const HOME_PATH = "/";
export const DOCS_PATH = "/docs";
export const DOCS_PAGE_PREFIX = "/docs/";
export const BENCHMARK_PATH = "/benchmark";
export const BLOG_PATH = "/blog";
export const BLOG_PAGE_PREFIX = "/blog/";

const basePath = import.meta.env.BASE_URL.replace(/\/$/, "");

export function buildAppHref(path: string) {
  const normalizedPath = path === HOME_PATH ? HOME_PATH : path.startsWith("/") ? path : `/${path}`;
  return `${basePath}${normalizedPath === HOME_PATH ? "/" : normalizedPath}`;
}

export function getCurrentAppPath(pathname?: string) {
  const resolvedPathname =
    pathname ?? (typeof window !== "undefined" ? window.location.pathname : HOME_PATH);
  const pathWithoutBase =
    basePath && resolvedPathname.startsWith(basePath)
      ? resolvedPathname.slice(basePath.length) || HOME_PATH
      : resolvedPathname;
  const normalizedPath = pathWithoutBase.replace(/\/+$/, "") || HOME_PATH;

  if (normalizedPath === DOCS_PATH) {
    return DOCS_PATH;
  }

  if (normalizedPath.startsWith(DOCS_PAGE_PREFIX) && normalizedPath !== DOCS_PATH) {
    return normalizedPath;
  }

  if (normalizedPath === BENCHMARK_PATH) {
    return BENCHMARK_PATH;
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
  const currentHref = `${window.location.pathname}${window.location.search}${window.location.hash}`;

  if (currentHref === nextHref) {
    return;
  }

  window.history.pushState({}, "", nextHref);
  window.dispatchEvent(new PopStateEvent("popstate"));
  window.scrollTo({ top: 0, behavior: "auto" });
}
