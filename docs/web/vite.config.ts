import { readFileSync } from "node:fs";
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

// Rams-style syntax palette on the graphite panel: three functional colors only.
// Operative tokens (keywords, commands, tags) = signal yellow; data (strings,
// numbers, constants) = warm amber; structure names (keys, attributes) = olive.
// Comments and punctuation recede; everything else stays plain.
const braunGraphite = {
  name: "braun-graphite",
  type: "dark",
  colors: {
    "editor.background": "#26241f",
    "editor.foreground": "#edebdd",
  },
  tokenColors: [
    { settings: { foreground: "#edebdd" } },
    {
      scope: ["comment", "punctuation.definition.comment"],
      settings: { foreground: "#8b867a" },
    },
    {
      scope: [
        "keyword",
        "storage",
        "keyword.control",
        "entity.name.tag",
        "entity.name.function",
        "support.function",
        "markup.heading",
      ],
      settings: { foreground: "#f1b400" },
    },
    {
      scope: [
        "string",
        "punctuation.definition.string",
        "constant.numeric",
        "constant.language",
        "constant.character.escape",
        "constant.other",
      ],
      settings: { foreground: "#e0b16e" },
    },
    {
      scope: [
        "variable.other",
        "variable.parameter",
        "entity.other.attribute-name",
        "support.type.property-name",
        "meta.object-literal.key",
      ],
      settings: { foreground: "#a9b665" },
    },
    {
      scope: ["punctuation", "keyword.operator", "punctuation.separator", "meta.brace"],
      settings: { foreground: "#8b867a" },
    },
  ],
};

export default defineConfig({
  define: {
    __URSULA_VERSION__: JSON.stringify(workspaceVersion),
  },
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
              theme: braunGraphite,
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
