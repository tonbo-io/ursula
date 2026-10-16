import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import mdx from "@mdx-js/rollup";
import rehypeShiki from "@shikijs/rehype";
import rehypeSlug from "rehype-slug";
import remarkFrontmatter from "remark-frontmatter";
import remarkGfm from "remark-gfm";
import remarkMdxFrontmatter from "remark-mdx-frontmatter";
import { vitePrerenderPlugin } from "vite-prerender-plugin";

const __dirname = dirname(fileURLToPath(import.meta.url));
const workspaceCargo = readFileSync(resolve(__dirname, "../../Cargo.toml"), "utf8");
const workspaceVersion = workspaceCargo.match(/^version\s*=\s*"([^"]+)"/m)?.[1] ?? "0.0.0";

// Versioned docs: the default build renders the latest pages at /docs. With
// URSULA_DOCS_VERSION=<id> it renders the archived pages that
// `scripts/docs-versions.mjs fetch` pulled from that version's tag, for
// /docs/<id>. See docs-versions.json.
type ArchivedVersion = { id: string; ref: string; upgrade: string };
const archivedVersions: ArchivedVersion[] = JSON.parse(
  readFileSync(resolve(__dirname, "docs-versions.json"), "utf8"),
).archived;
const docsVersionId = process.env.URSULA_DOCS_VERSION || null;
const docsVersion = docsVersionId
  ? archivedVersions.find((version) => version.id === docsVersionId)
  : undefined;
if (docsVersionId && !docsVersion) {
  throw new Error(`URSULA_DOCS_VERSION=${docsVersionId} is not listed in docs-versions.json`);
}
const latestPagesDir = resolve(__dirname, "src/content/docs/pages");
const archivedPagesDir = (id: string) => resolve(__dirname, "archived-docs", id, "pages");

function listSlugs(dir: string): string[] {
  if (!existsSync(dir)) return [];
  return readdirSync(dir, { recursive: true, encoding: "utf8" })
    .filter((file) => file.endsWith(".mdx"))
    .map((file) => file.replace(/\\/g, "/").replace(/\.mdx$/, "").replace(/\/index$/, ""))
    .sort();
}

const docsVersions = [
  {
    id: workspaceVersion.split(".").slice(0, 2).join("."),
    release: workspaceVersion,
    root: "/docs",
    archived: false,
    slugs: listSlugs(latestPagesDir),
  },
  ...archivedVersions.map((version) => ({
    id: version.id,
    release: version.ref.replace(/^v/, ""),
    root: `/docs/${version.id}`,
    archived: true,
    upgrade: version.upgrade,
    slugs: listSlugs(archivedPagesDir(version.id)),
  })),
];

// Rams-style syntax palette on the graphite panel: three functional colors only.

export default defineConfig({
  define: {
    __URSULA_VERSION__: JSON.stringify(
      docsVersion ? docsVersion.ref.replace(/^v/, "") : workspaceVersion,
    ),
    __DOCS_VERSION__: JSON.stringify(docsVersion?.id ?? null),
    __DOCS_VERSIONS__: JSON.stringify(docsVersions),
  },
  resolve: {
    alias: {
      "@docs-pages": docsVersion ? archivedPagesDir(docsVersion.id) : latestPagesDir,
    },
  },
  // An archived build keeps base "/" (so routes stay real paths) and puts
  // its pages and assets under /docs/<id>. The deploy merges only that folder.
  publicDir: docsVersion ? false : "public",
  build: docsVersion
    ? {
        outDir: `archived-docs/${docsVersion.id}/dist`,
        assetsDir: `docs/${docsVersion.id}/assets`,
        emptyOutDir: true,
      }
    : undefined,
  server: {
    proxy: {
      "/__chaos-proxy/status.json": {
        target: "https://ursula-chaos-status-tonbo.s3.amazonaws.com",
        changeOrigin: true,
        rewrite: (path) => path.replace("/__chaos-proxy", ""),
      },
    },
  },
  plugins: [
    {
      enforce: "pre",
      ...mdx({
        providerImportSource: "@mdx-js/react",
        remarkPlugins: [
          remarkFrontmatter,
          [remarkMdxFrontmatter, { name: "frontmatter" }],
          remarkGfm,
        ],
        rehypePlugins: [
          rehypeSlug,
          [
            rehypeShiki,
            {
              theme: "gruvbox-dark-medium",
              defaultLanguage: "text",
              parseMetaString(meta: string) {
                const titleMatch = meta.match(/title="([^"]+)"/);
                if (titleMatch) {
                  return { "data-title": titleMatch[1] };
                }
                return null;
              },
            },
          ],
        ],
        mdExtensions: [],
        mdxExtensions: [".mdx"],
      }),
    },
    react({ include: /\.(jsx|tsx|mdx)$/ }),
    vitePrerenderPlugin({
      renderTarget: "#root",
      prerenderScript: "/src/prerender.tsx",
    }),
  ],
});
