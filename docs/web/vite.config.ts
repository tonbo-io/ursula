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

export default defineConfig({
  define: {
    __URSULA_VERSION__: JSON.stringify(workspaceVersion),
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
              theme: "gruvbox-dark-hard",
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
