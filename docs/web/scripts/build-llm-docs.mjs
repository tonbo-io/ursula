// Generates agent-friendly endpoints from the source MDX:
//   dist/docs/<slug>.md   - per-page raw markdown
//   dist/llms.txt         - index with summaries
//   dist/llms-full.txt    - every page concatenated
//
// Runs after `vite build` in webpages/package.json. The transform unwraps
// the JSX components we use (Card / CardGroup / Steps / Step / Note / Tip /
// Warning / Info / CodeGroup / AccordionGroup / Accordion) into plain
// markdown so a coding agent fetching the .md sees real markdown links and
// real headings instead of JSX attributes.

import { readdir, readFile, writeFile, mkdir, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..");
const PAGES_DIR = join(REPO_ROOT, "src/content/docs/pages");
const PUBLIC_DIR = join(REPO_ROOT, "public");
const DIST_DIR = join(REPO_ROOT, "dist");
const ORIGIN = "https://ursula.tonbo.io";

const GROUP_ORDER = [
  "Getting Started",
  "Deploy",
  "Examples",
  "Concepts",
  "API Reference",
  "Operate",
  "Architecture",
  "Comparisons",
  "Protocol Specification",
  "Reference",
];

const SUMMARY =
  "Open-source Distributed Durable Streams over HTTP, backed by S3.";

const PAGE_ORDER = new Map([
  ["introduction", ["Getting Started", 1]],
  ["why-ursula", ["Getting Started", 2]],
  ["install", ["Getting Started", 3]],
  ["quick-start", ["Getting Started", 4]],
  ["clients", ["Getting Started", 5]],

  ["deploy-cluster", ["Deploy", 1]],
  ["configuration", ["Deploy", 2]],
  ["security", ["Deploy", 3]],

  ["examples/browser-telemetry", ["Examples", 1]],

  ["concepts/streams", ["Concepts", 1]],
  ["concepts/buckets", ["Concepts", 2]],
  ["concepts/offsets", ["Concepts", 3]],
  ["concepts/record-coordinates", ["Concepts", 4]],
  ["concepts/read-modes", ["Concepts", 5]],
  ["concepts/exactly-once-writes", ["Concepts", 6]],
  ["concepts/conditional-writes", ["Concepts", 7]],
  ["concepts/snapshots", ["Concepts", 8]],
  ["concepts/bootstrap", ["Concepts", 9]],
  ["concepts/durability-and-consistency", ["Concepts", 10]],
  ["concepts/binary-sse", ["Concepts", 11]],
  ["concepts/len-prefixed-framing", ["Concepts", 12]],

  ["api/overview", ["API Reference", 1]],
  ["api/create-bucket", ["API Reference", 2]],
  ["api/create-stream", ["API Reference", 3]],
  ["api/append", ["API Reference", 4]],
  ["api/read", ["API Reference", 5]],
  ["api/head-stream", ["API Reference", 6]],
  ["api/stream-attrs", ["API Reference", 7]],
  ["api/publish-snapshot", ["API Reference", 8]],
  ["api/read-snapshot", ["API Reference", 9]],
  ["api/bootstrap", ["API Reference", 10]],
  ["api/delete-stream", ["API Reference", 11]],

  ["cli", ["Operate", 1]],
  ["operations", ["Operate", 2]],
  ["observability", ["Operate", 3]],
  ["troubleshooting", ["Operate", 4]],

  ["architecture/overview", ["Architecture", 1]],

  ["specs/durable-stream", ["Protocol Specification", 1]],
  ["specs/extensions", ["Protocol Specification", 2]],

  ["competitive-comparison", ["Comparisons", 1]],
]);

function parseFrontmatter(source) {
  const match = source.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  if (!match) return { meta: {}, body: source };
  const meta = {};
  for (const rawLine of match[1].split("\n")) {
    const line = rawLine.trim();
    if (!line) continue;
    const fm = line.match(/^([A-Za-z0-9_-]+):\s*(.*)$/);
    if (!fm) continue;
    let value = fm[2].trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    meta[fm[1]] = value;
  }
  return { meta, body: match[2] };
}

function indentBlock(text, prefix) {
  return text
    .split("\n")
    .map((line) => (line.length === 0 ? prefix.replace(/\s+$/, "") : prefix + line))
    .join("\n");
}

// Strip the common leading whitespace from every non-blank line.
// MDX content inside JSX wrappers (<Steps>, <Step>, <CodeGroup>, ...) is
// indented for legibility; once the wrapper goes away we want the remaining
// markdown to sit at column 0 so fenced code blocks and lists render right.
function dedent(text) {
  const lines = text.split("\n");
  const indents = lines
    .filter((line) => line.trim().length > 0)
    .map((line) => line.match(/^[ \t]*/)[0].length);
  if (indents.length === 0) return text;
  const min = Math.min(...indents);
  if (min === 0) return text;
  return lines.map((line) => line.slice(min)).join("\n");
}

function transformMdxToMarkdown(body) {
  let text = body;

  // Code-fence titles: ```sh title="storage set"  →  **storage set** above the fence.
  // Preserve the leading indent of the opening fence so the surrounding block
  // stays consistently indented; dedent() in the wrapper unwraps then strips
  // the shared indent for the whole block at once.
  text = text.replace(
    /^([ \t]*)```([\w-]+)\s+title="([^"]+)"\s*\n/gm,
    (_match, indent, lang, title) =>
      `${indent}**${title}**\n\n${indent}\`\`\`${lang}\n`,
  );

  // <Card title="X" href="Y">Z</Card>  →  - [**X**](Y) - Z
  // <Card title="X">Z</Card>            →  - **X** - Z
  // Eat any leading horizontal whitespace on the line so siblings inside a
  // CardGroup (which are 2-space-indented in the MDX source for legibility)
  // don't render as a nested markdown list.
  text = text.replace(
    /^[ \t]*<Card\s+([^>]*?)>([\s\S]*?)<\/Card>/gm,
    (_match, attrs, inner) => {
      const titleMatch = attrs.match(/title="([^"]+)"/);
      const hrefMatch = attrs.match(/href="([^"]+)"/);
      const title = titleMatch ? titleMatch[1] : "";
      const href = hrefMatch ? hrefMatch[1] : null;
      const compactInner = inner.trim().replace(/\s+\n\s+/g, " ").replace(/\n+/g, " ");
      const head = href ? `[**${title}**](${href})` : `**${title}**`;
      return `- ${head}${compactInner ? " - " + compactInner : ""}`;
    },
  );

  // <CardGroup ...>X</CardGroup>  →  unwrap
  text = text.replace(/<CardGroup[^>]*>\s*([\s\S]*?)\s*<\/CardGroup>/g, "$1");

  // <Step title="X">Y</Step>  →  **X**\n\nY
  text = text.replace(
    /^[ \t]*<Step\s+title="([^"]+)"\s*>([\s\S]*?)<\/Step>/gm,
    (_match, title, inner) => {
      const dedented = dedent(inner).trim();
      return `**${title}**\n\n${dedented}\n`;
    },
  );

  // <Steps>X</Steps>  →  unwrap, dedent
  text = text.replace(/<Steps>\s*([\s\S]*?)\s*<\/Steps>/g, (_m, inner) => dedent(inner));

  // <Note>X</Note>      →  > [!NOTE]\n> X
  // <Tip>X</Tip>        →  > [!TIP]\n> X
  // <Warning>X</Warning>→  > [!WARNING]\n> X
  // <Info>X</Info>      →  > [!NOTE]\n> X      (GFM has no Info)
  const calloutMap = { Note: "NOTE", Tip: "TIP", Warning: "WARNING", Info: "NOTE" };
  for (const [tag, level] of Object.entries(calloutMap)) {
    const re = new RegExp(`<${tag}>\\s*([\\s\\S]*?)\\s*<\\/${tag}>`, "g");
    text = text.replace(re, (_match, inner) => {
      return `> [!${level}]\n` + indentBlock(inner.trim(), "> ");
    });
  }

  // <Accordion title="X">Y</Accordion>  →  ### X\n\nY
  text = text.replace(
    /^[ \t]*<Accordion\s+title="([^"]+)"\s*>([\s\S]*?)<\/Accordion>/gm,
    (_match, title, inner) => `### ${title}\n\n${dedent(inner).trim()}\n`,
  );

  // <AccordionGroup>X</AccordionGroup>  →  unwrap
  text = text.replace(/<AccordionGroup>\s*([\s\S]*?)\s*<\/AccordionGroup>/g, "$1");

  // <CodeGroup>X</CodeGroup>  →  unwrap + dedent (inner code fences keep their **Title**)
  text = text.replace(/<CodeGroup>\s*([\s\S]*?)\s*<\/CodeGroup>/g, (_m, inner) => dedent(inner));

  // Collapse runs of blank lines.
  text = text.replace(/\n{3,}/g, "\n\n");

  return text.trim() + "\n";
}

async function ensureDir(path) {
  if (!existsSync(path)) await mkdir(path, { recursive: true });
}

async function writeOutputFile(root, relativePath, content) {
  const path = join(root, relativePath);
  await ensureDir(dirname(path));
  await writeFile(path, content);
}

async function listMdxFiles(dir, prefix = "") {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    const absolute = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listMdxFiles(absolute, relative)));
    } else if (entry.isFile() && entry.name.endsWith(".mdx")) {
      files.push(relative);
    }
  }
  return files;
}

function inferSlug(file) {
  return file.replace(/\.mdx$/, "").replace(/\/index$/, "");
}

async function main() {
  await stat(DIST_DIR).catch(() => {
    throw new Error(`dist/ does not exist; run "vite build" first.`);
  });
  const outputRoots = [
    { label: "dist", dir: DIST_DIR },
    { label: "public", dir: PUBLIC_DIR },
  ];
  for (const output of outputRoots) {
    await ensureDir(join(output.dir, "docs"));
  }

  const files = await listMdxFiles(PAGES_DIR);
  const pages = [];
  for (const file of files) {
    const source = await readFile(join(PAGES_DIR, file), "utf8");
    const { meta, body } = parseFrontmatter(source);
    const slug = meta.slug ?? inferSlug(file);
    const orderConfig = PAGE_ORDER.get(slug);
    if (!meta.title) {
      console.warn(`skipping ${file}: missing title in frontmatter`);
      continue;
    }
    pages.push({
      slug,
      title: meta.title,
      description: meta.description ?? "",
      group: meta.group ?? orderConfig?.[0] ?? "Reference",
      order: parseInt(meta.order ?? String(orderConfig?.[1] ?? 999), 10),
      markdown: transformMdxToMarkdown(body),
    });
  }

  pages.sort((a, b) => {
    const ga = GROUP_ORDER.indexOf(a.group);
    const gb = GROUP_ORDER.indexOf(b.group);
    const groupKeyA = ga === -1 ? Number.MAX_SAFE_INTEGER : ga;
    const groupKeyB = gb === -1 ? Number.MAX_SAFE_INTEGER : gb;
    if (groupKeyA !== groupKeyB) return groupKeyA - groupKeyB;
    return a.order - b.order;
  });

  // Per-page .md files at /docs/<slug>.md
  for (const p of pages) {
    const out =
      `# ${p.title}\n\n` +
      (p.description ? `> ${p.description}\n\n` : "") +
      p.markdown;
    for (const output of outputRoots) {
      await writeOutputFile(output.dir, `docs/${p.slug}.md`, out);
    }
  }

  // llms.txt - agent-friendly index.
  const grouped = new Map();
  for (const p of pages) {
    const list = grouped.get(p.group) ?? [];
    list.push(p);
    grouped.set(p.group, list);
  }

  const llmsIndex = [
    `# Ursula`,
    ``,
    `> ${SUMMARY}`,
    ``,
    `Customer-facing documentation. Each page is also available as raw markdown at`,
    `${ORIGIN}/docs/<slug>.md (e.g. ${ORIGIN}/docs/quick-start.md). For all docs`,
    `concatenated, fetch ${ORIGIN}/llms-full.txt.`,
    ``,
  ];
  for (const groupName of GROUP_ORDER) {
    const list = grouped.get(groupName);
    if (!list || list.length === 0) continue;
    llmsIndex.push(`## ${groupName}`, ``);
    for (const p of list) {
      const desc = p.description ? `: ${p.description}` : "";
      llmsIndex.push(`- [${p.title}](${ORIGIN}/docs/${p.slug}.md)${desc}`);
    }
    llmsIndex.push(``);
  }
  for (const output of outputRoots) {
    await writeOutputFile(output.dir, "llms.txt", llmsIndex.join("\n"));
  }

  // llms-full.txt - every page concatenated.
  const llmsFull = [
    `# Ursula - full documentation`,
    ``,
    `> ${SUMMARY}`,
    ``,
    `Source: ${ORIGIN}`,
    ``,
  ];
  for (const p of pages) {
    llmsFull.push(`---`, ``, `# ${p.title}`, ``);
    if (p.description) llmsFull.push(`> ${p.description}`, ``);
    llmsFull.push(p.markdown.trim(), ``);
  }
  for (const output of outputRoots) {
    await writeOutputFile(output.dir, "llms-full.txt", llmsFull.join("\n"));
  }

  console.log(`build-llm-docs:`);
  for (const output of outputRoots) {
    console.log(`  ${output.label}/llms.txt`);
    console.log(`  ${output.label}/llms-full.txt`);
    console.log(`  ${output.label}/docs/<slug>.md (${pages.length} pages)`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
