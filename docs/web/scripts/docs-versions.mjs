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
      run("git", ["fetch", "--no-tags", "--depth=1", "origin", `${tagRef}:${tagRef}`]);
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
  appendFileSync(join(DIST, "_headers"), `${headers.join("\n")}`);
}

function verifyVersions() {
  const failures = [];
  const check = (ok, message) => {
    if (!ok) failures.push(message);
  };
  const read = (path) => readFileSync(path, "utf8");
  const pageCount = (entry) =>
    Object.values(JSON.parse(read(entry)).languages).reduce((sum, lang) => sum + lang.page_count, 0);
  const archivedPrefixes = VERSIONS.map((version) => `${version.id}/`);

  const latestPages = listFiles(join(DIST, "docs"), "index.html").filter(
    (file) => !archivedPrefixes.some((prefix) => file.startsWith(prefix)),
  );
  for (const file of latestPages) {
    const html = read(join(DIST, "docs", file));
    check(!html.includes('name="robots"'), `latest docs/${file} has a robots meta`);
    check(!html.includes("docs-version-banner"), `latest docs/${file} has the version banner`);
  }
  check(
    pageCount(join(DIST, "pagefind", "pagefind-entry.json")) === latestPages.length,
    `latest search index does not match the ${latestPages.length} latest pages`,
  );
  const llmsFull = read(join(DIST, "llms-full.txt"));

  const headers = existsSync(join(DIST, "_headers")) ? read(join(DIST, "_headers")) : "";
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
      check(
        new RegExp(`<link rel="canonical" href="${ORIGIN}${root.replace(/\./g, "\\.")}/`).test(html),
        `${where}: canonical is not under ${root}/`,
      );
      check(html.includes("docs-version-banner"), `${where}: no version banner`);
      for (const [, asset] of html.matchAll(/(?:src|href)="(\/[^"]*\.(?:js|css))"/g)) {
        check(asset.startsWith(`${root}/assets/`), `${where}: asset ${asset} outside ${root}/assets/`);
      }
      const article = html.match(/<article[\s\S]*?<\/article>/)?.[0] ?? "";
      for (const [tag] of article.matchAll(/<a\s[^>]*>/g)) {
        if (tag.includes("data-docs-version-exit")) continue;
        const href = tag.match(/href="([^"]*)"/)?.[1] ?? "";
        const path = href.split(/[?#]/)[0];
        if (!/^\/docs(\/|$)/.test(path)) continue;
        if (path !== root && !path.startsWith(`${root}/`)) {
          check(false, `${where}: link ${href} leaves ${root}`);
          continue;
        }
        const slug = path.slice(root.length).replace(/^\/|\/$/g, "");
        check(slug === "" || slugs.has(slug), `${where}: link ${href} has no page`);
      }
    }

    check(
      pageCount(join(dir, "pagefind", "pagefind-entry.json")) === pages.length,
      `${version.id}: search index does not match its ${pages.length} pages`,
    );
    check(existsSync(join(dir, "llms.txt")), `${version.id}: no ${root}/llms.txt`);
    check(!llmsFull.includes(`${root}/`), `llms-full.txt links ${root}`);
    check(headers.includes(`${root}/*\n  X-Robots-Tag: noindex`), `${version.id}: no _headers noindex rule`);
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
