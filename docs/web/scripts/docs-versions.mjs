// Builds the archived docs versions listed in docs-versions.json.
//
//   node scripts/docs-versions.mjs fetch    pull each version's MDX pages from its tag
//                                           into archived-docs/<id>/pages
//   node scripts/docs-versions.mjs build    after the latest build: render each version
//                                           with the current site code and merge it into
//                                           dist/docs/<id>
//   node scripts/docs-versions.mjs verify   check the merged dist
//
// `npm run build:all` runs fetch, the latest build, then build. The tag is only
// read. Its pages are never checked in.

import { execFileSync } from "node:child_process";
import { appendFileSync, cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const DIST = join(ROOT, "dist");
const ORIGIN = "https://ursula.tonbo.io";
const PAGES_PATH = "docs/web/src/content/docs/pages";
const VERSIONS = JSON.parse(readFileSync(join(ROOT, "docs-versions.json"), "utf8")).archived;
for (const version of VERSIONS) {
  // The id becomes a path that fetch and build delete, so keep it to "<major>.<minor>".
  if (!/^\d+\.\d+$/.test(version.id)) throw new Error(`bad docs version id: ${version.id}`);
}

const archivedDir = (version) => join(ROOT, "archived-docs", version.id);

function run(command, args, options = {}) {
  return execFileSync(command, args, { cwd: ROOT, stdio: "inherit", ...options });
}

function listFiles(dir, suffix) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir, { recursive: true, encoding: "utf8" })
    .map((file) => file.split("\\").join("/"))
    .filter((file) => file.endsWith(suffix));
}

function slugsOf(version) {
  return listFiles(join(archivedDir(version), "pages"), ".mdx").map((file) =>
    file.replace(/\.mdx$/, "").replace(/\/index$/, ""),
  );
}

function fetchVersions() {
  for (const version of VERSIONS) {
    const tagRef = `refs/tags/${version.ref}`;
    try {
      run("git", ["rev-parse", "-q", "--verify", `${tagRef}^{commit}`], { stdio: "ignore" });
    } catch {
      // --depth=1 only keeps a CI checkout shallow. On a full clone it would make it shallow.
      const shallow =
        execFileSync("git", ["rev-parse", "--is-shallow-repository"], { cwd: ROOT, encoding: "utf8" }).trim() ===
        "true";
      run("git", ["fetch", "--no-tags", ...(shallow ? ["--depth=1"] : []), "origin", `${tagRef}:${tagRef}`]);
    }
    const pagesDir = join(archivedDir(version), "pages");
    rmSync(pagesDir, { recursive: true, force: true });
    mkdirSync(pagesDir, { recursive: true });
    // From the repository root: run in a subdirectory, git archive keeps only that subtree.
    const tar = execFileSync("git", ["archive", "--format=tar", `${tagRef}:${PAGES_PATH}`], {
      cwd: join(ROOT, "../.."),
      maxBuffer: 256 * 1024 * 1024,
    });
    run("tar", ["-x", "-C", pagesDir], { input: tar, stdio: ["pipe", "inherit", "inherit"] });
    console.log(`docs-versions: ${version.id} <- ${version.ref} (${slugsOf(version).length} pages)`);
  }
}

function buildVersions() {
  if (!existsSync(join(DIST, "index.html"))) {
    throw new Error('dist/ has no latest build; run "npm run build" first.');
  }
  const headers = [];
  for (const version of VERSIONS) {
    if (slugsOf(version).length === 0) {
      throw new Error(`no pages for ${version.id}; run "node scripts/docs-versions.mjs fetch" first.`);
    }
    const env = { ...process.env, URSULA_DOCS_VERSION: version.id };
    // vite.config.ts explicitly: Vite prefers a stale vite.config.js left by `tsc -b`.
    run("npx", ["vite", "build", "--config", "vite.config.ts"], { env });
    run("node", ["scripts/build-llm-docs.mjs", "--version", version.id]);
    const site = join("archived-docs", version.id, "dist");
    run("npx", [
      "pagefind",
      "--site",
      site,
      "--glob",
      `docs/${version.id}/**/*.html`,
      "--output-subdir",
      `docs/${version.id}/pagefind`,
    ]);
    const target = join(DIST, "docs", version.id);
    rmSync(target, { recursive: true, force: true });
    cpSync(join(ROOT, site, "docs", version.id), target, { recursive: true });
    // Covers the .md, llms and search files, which carry no robots meta.
    headers.push(`/docs/${version.id}/*`, "  X-Robots-Tag: noindex", "");
  }
  appendFileSync(join(DIST, "_headers"), `\n${headers.join("\n")}`);
}

// Site links in generated text (llms files and .md pages): absolute
// https://ursula.tonbo.io/docs/... URLs, markdown "](/docs/...)" and href="/docs/...".
function siteDocsLinks(text) {
  const pattern = new RegExp(`(?:${ORIGIN.replace(/\./g, "\\.")}|\\]\\(|href=")(/docs(?:[/#?][^\\s)"'<>\\]]*)?)`, "g");
  // A sentence can end right after a URL.
  return [...text.matchAll(pattern)].map((match) => match[1].replace(/[.,;:]+$/, ""));
}

// "/docs/0.7/api/read.md#x" with root "/docs/0.7" -> "api/read".
function slugOf(path, root) {
  return path
    .split(/[?#]/)[0]
    .slice(root.length)
    .replace(/^\/|\/$/g, "")
    .replace(/\.md$/, "");
}

function verifyVersions() {
  const failures = [];
  const check = (ok, message) => {
    if (!ok) failures.push(message);
  };
  const read = (path) => readFileSync(path, "utf8");
  const pageCount = (entry) => {
    if (!existsSync(entry)) return null;
    return Object.values(JSON.parse(read(entry)).languages).reduce((sum, lang) => sum + lang.page_count, 0);
  };
  const checkIndex = (entry, expected, label) => {
    const count = pageCount(entry);
    check(count !== null, `${label}: no search index at ${entry}`);
    check(count === null || count === expected, `${label}: search index has ${count} pages, expected ${expected}`);
  };
  const archivedRoots = VERSIONS.map((version) => `/docs/${version.id}`);
  const underRoot = (path, root) => path === root || (path.startsWith(root) && /^[/#?]/.test(path.slice(root.length)));
  const canonicalOf = (html) => html.match(/<link rel="canonical" href="([^"]*)"/)?.[1];
  // "install/index.html" -> ".../install/", the trailing-slash form Pages serves.
  const pageUrl = (root, file) => `${ORIGIN}${root}/${file.replace(/index\.html$/, "")}`;

  // Latest docs.
  const latestSlugs = new Set(listFiles(join(ROOT, "src/content/docs/pages"), ".mdx").map((file) =>
    file.replace(/\.mdx$/, "").replace(/\/index$/, ""),
  ));
  const latestPages = listFiles(join(DIST, "docs"), "index.html").filter(
    (file) => !archivedRoots.some((root) => underRoot(`/docs/${file}`, root)),
  );
  for (const file of latestPages) {
    const html = read(join(DIST, "docs", file));
    check(!html.includes('name="robots"'), `latest docs/${file} has a robots meta`);
    check(!html.includes("docs-version-banner"), `latest docs/${file} has the version banner`);
    const expected = pageUrl("/docs", file);
    check(canonicalOf(html) === expected, `latest docs/${file}: canonical ${canonicalOf(html)}, expected ${expected}`);
  }
  checkIndex(join(DIST, "pagefind", "pagefind-entry.json"), latestPages.length, "latest");
  const latestText = [
    "llms.txt",
    "llms-full.txt",
    ...listFiles(join(DIST, "docs"), ".md")
      .filter((file) => !archivedRoots.some((root) => underRoot(`/docs/${file}`, root)))
      .map((file) => `docs/${file}`),
  ];
  for (const file of latestText) {
    for (const path of siteDocsLinks(read(join(DIST, file)))) {
      const archived = archivedRoots.find((root) => underRoot(path, root));
      if (archived) {
        // Only llms.txt points readers at an older version's llms.txt.
        check(file === "llms.txt" && path === `${archived}/llms.txt`, `${file}: link ${path} leaves the latest docs`);
        continue;
      }
      const slug = slugOf(path, "/docs");
      check(slug === "" || latestSlugs.has(slug), `${file}: link ${path} has no latest page`);
    }
  }

  // _headers: the only noindex rules are the archived /docs/<id>/* prefixes.
  const headersPath = join(DIST, "_headers");
  const rules = [];
  for (const line of (existsSync(headersPath) ? read(headersPath) : "").split("\n")) {
    if (!line.trim() || line.trim().startsWith("#")) continue;
    if (/^\s/.test(line)) rules.at(-1)?.headers.push(line.trim());
    else rules.push({ path: line.trim(), headers: [] });
  }
  const robotsRules = rules.filter((rule) => rule.headers.some((header) => /^x-robots-tag\s*:/i.test(header)));
  for (const rule of robotsRules) {
    check(
      archivedRoots.some((root) => rule.path === `${root}/*`),
      `_headers: X-Robots-Tag rule for ${rule.path} is not an archived /docs/<id>/* prefix`,
    );
  }

  // Archived versions.
  for (const version of VERSIONS) {
    const root = `/docs/${version.id}`;
    const dir = join(DIST, "docs", version.id);
    const slugs = new Set(slugsOf(version));
    const pages = listFiles(dir, "index.html").filter((file) => !file.startsWith("pagefind/"));
    check(slugs.size > 0, `${version.id}: no fetched pages`);
    check(pages.length === slugs.size + 1, `${version.id}: ${pages.length} pages, expected ${slugs.size + 1}`);

    for (const file of pages) {
      const where = `${root}/${file}`;
      const html = read(join(dir, file));
      check(html.includes('<meta name="robots" content="noindex, follow"'), `${where}: no noindex meta`);
      const expected = pageUrl(root, file);
      check(canonicalOf(html) === expected, `${where}: canonical ${canonicalOf(html)}, expected ${expected}`);
      check(html.includes("docs-version-banner"), `${where}: no version banner`);
      for (const [, asset] of html.matchAll(/(?:src|href)="(\/[^"]*\.(?:js|css))"/g)) {
        check(asset.startsWith(`${root}/assets/`), `${where}: asset ${asset} outside ${root}/assets/`);
      }
      const article = html.match(/<article[\s\S]*?<\/article>/)?.[0] ?? "";
      for (const [tag] of article.matchAll(/<a\s[^>]*>/g)) {
        if (tag.includes("data-docs-version-exit")) continue;
        const href = tag.match(/href="([^"]*)"/)?.[1] ?? "";
        if (!/^\/docs([/#?]|$)/.test(href)) continue;
        if (!underRoot(href, root)) {
          check(false, `${where}: link ${href} leaves ${root}`);
          continue;
        }
        const slug = slugOf(href, root);
        check(slug === "" || slugs.has(slug), `${where}: link ${href} has no page`);
      }
    }

    checkIndex(join(dir, "pagefind", "pagefind-entry.json"), pages.length, version.id);
    for (const name of ["llms.txt", "llms-full.txt"]) {
      check(existsSync(join(dir, name)), `${version.id}: no ${root}/${name}`);
    }
    for (const file of listFiles(dir, ".md").concat(["llms.txt", "llms-full.txt"])) {
      if (!existsSync(join(dir, file))) continue;
      for (const path of siteDocsLinks(read(join(dir, file)))) {
        if (!underRoot(path, root)) {
          check(false, `${root}/${file}: link ${path} leaves ${root}`);
          continue;
        }
        const slug = slugOf(path, root);
        check(
          slug === "" || slug === "llms.txt" || slug === "llms-full.txt" || slugs.has(slug),
          `${root}/${file}: link ${path} has no page`,
        );
      }
    }
    check(
      robotsRules.some(
        (rule) => rule.path === `${root}/*` && rule.headers.some((header) => /^x-robots-tag\s*:\s*noindex$/i.test(header)),
      ),
      `${version.id}: no _headers noindex rule for ${root}/*`,
    );
  }

  if (failures.length > 0) {
    console.error(`docs-versions verify: ${failures.length} problem(s)`);
    for (const failure of failures.slice(0, 50)) console.error(`  ${failure}`);
    process.exit(1);
  }
  console.log(`docs-versions verify: ok (${latestPages.length} latest pages, ${VERSIONS.map((v) => v.id).join(", ")})`);
}

const commands = { fetch: fetchVersions, build: buildVersions, verify: verifyVersions };
const command = commands[process.argv[2]];
if (!command) {
  console.error("usage: node scripts/docs-versions.mjs fetch|build|verify");
  process.exit(2);
}
command();
