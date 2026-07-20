import type { ComponentType } from "react";

export type DocsPageMeta = {
  slug: string;
  title: string;
  description?: string;
  group: string;
  order: number;
};

export type DocsPage = DocsPageMeta & {
  Component: ComponentType;
};

export type DocsGroup = {
  name: string;
  pages: DocsPage[];
};

type MdxModule = {
  default: ComponentType;
  frontmatter?: {
    slug?: string;
    title?: string;
    description?: string;
    group?: string;
    order?: number;
  };
};

const docsModules = import.meta.glob<MdxModule>("./pages/**/*.mdx", { eager: true });

const pageOrder: Record<string, { group: string; order: number; title?: string }> = {
  // Getting Started - orient, try, decide, call.
  introduction: { group: "Getting Started", order: 1, title: "Introduction" },
  "quick-start": { group: "Getting Started", order: 2 },
  "why-ursula": { group: "Getting Started", order: 3 },
  clients: { group: "Getting Started", order: 4 },

  // Guides - complete application-shaped examples.
  "guides/browser-telemetry": { group: "Guides", order: 1 },

  // Concepts - encounter order: primitive, organization, reading, writing, long-stream, transport.
  "concepts/streams": { group: "Concepts", order: 1 },
  "concepts/buckets": { group: "Concepts", order: 2 },
  "concepts/offsets": { group: "Concepts", order: 3 },
  "concepts/record-coordinates": { group: "Concepts", order: 4 },
  "concepts/read-modes": { group: "Concepts", order: 5 },
  "concepts/exactly-once-writes": { group: "Concepts", order: 6 },
  "concepts/conditional-writes": { group: "Concepts", order: 7 },
  "concepts/snapshots": { group: "Concepts", order: 8 },
  "concepts/bootstrap": { group: "Concepts", order: 9 },
  "concepts/durability-and-consistency": { group: "Concepts", order: 10 },
  "concepts/binary-sse": { group: "Concepts", order: 11 },
  "concepts/len-prefixed-framing": { group: "Concepts", order: 12 },

  // API Reference - typical call order: setup, hot path, snapshots/bootstrap, lifecycle, compatibility.
  "api/overview": { group: "API Reference", order: 1 },
  "api/create-bucket": { group: "API Reference", order: 2 },
  "api/create-stream": { group: "API Reference", order: 3 },
  "api/append": { group: "API Reference", order: 4 },
  "api/read": { group: "API Reference", order: 5 },
  "api/head-stream": { group: "API Reference", order: 6 },
  "api/stream-attrs": { group: "API Reference", order: 7 },
  "api/publish-snapshot": { group: "API Reference", order: 8 },
  "api/read-snapshot": { group: "API Reference", order: 9 },
  "api/bootstrap": { group: "API Reference", order: 10 },
  "api/delete-stream": { group: "API Reference", order: 11 },

  // Install & Deploy - get the binaries, get the cluster running.
  install: { group: "Install & Deploy", order: 1 },
  "run-locally": { group: "Install & Deploy", order: 2 },
  configuration: { group: "Install & Deploy", order: 3 },
  "configure-s3": { group: "Install & Deploy", order: 4 },
  "deploy-cluster": { group: "Install & Deploy", order: 5 },
  "deploy-kubernetes": { group: "Install & Deploy", order: 6 },
  security: { group: "Install & Deploy", order: 7 },

  // Operate - day-2 entrypoints. `ursulactl` is the first one to reach for.
  cli: { group: "Operate", order: 1, title: "ursulactl" },
  operations: { group: "Operate", order: 2 },
  observability: { group: "Operate", order: 3 },
  troubleshooting: { group: "Operate", order: 4 },

  // Architecture - internals for users who want to dig deeper.
  "architecture/overview": { group: "Architecture", order: 1 },

  // Protocol Specification - for protocol implementers.
  "specs/durable-stream": { group: "Protocol Specification", order: 1 },
  "specs/extensions": { group: "Protocol Specification", order: 2 },

  // Comparisons - positioning.
  "competitive-comparison": { group: "Comparisons", order: 1 },
};

function inferSlug(path: string) {
  return path
    .replace(/^\.\/pages\//, "")
    .replace(/\.mdx$/, "")
    .replace(/\/index$/, "");
}

function buildPage(path: string, mod: MdxModule): DocsPage {
  const fallbackSlug = inferSlug(path);
  const frontmatter = mod.frontmatter ?? {};

  const slug = frontmatter.slug ?? fallbackSlug;
  const override = pageOrder[slug];
  const title = override?.title ?? frontmatter.title;
  const group = override?.group ?? frontmatter.group ?? "Reference";
  const order = override?.order ?? frontmatter.order ?? 999;

  if (!title) {
    throw new Error(`Docs page is missing a title in frontmatter: ${path}`);
  }

  return {
    slug,
    title,
    description: frontmatter.description,
    group,
    order,
    Component: mod.default,
  };
}

const docsPages: DocsPage[] = Object.entries(docsModules)
  .map(([path, mod]) => buildPage(path, mod))
  .sort((a, b) => {
    if (a.group !== b.group) {
      return a.group.localeCompare(b.group);
    }
    return a.order - b.order;
  });

const groupOrder = [
  "Getting Started",
  "Architecture",
  "Comparisons",
  "Guides",
  "Concepts",
  "API Reference",
  "Install & Deploy",
  "Operate",
  "Protocol Specification",
  "Reference",
];

export function getAllDocsPages(): DocsPage[] {
  return docsPages;
}

export function getDocsPageBySlug(slug: string): DocsPage | undefined {
  if (slug === "") {
    return docsPages.find((page) => page.slug === "introduction") ?? docsPages[0];
  }
  return docsPages.find((page) => page.slug === slug);
}

export function getDocsGroups(): DocsGroup[] {
  const map = new Map<string, DocsPage[]>();

  for (const page of docsPages) {
    const list = map.get(page.group) ?? [];
    list.push(page);
    map.set(page.group, list);
  }

  return Array.from(map.entries())
    .map(([name, pages]) => ({
      name,
      pages: pages.sort((a, b) => a.order - b.order),
    }))
    .sort((a, b) => {
      const indexA = groupOrder.indexOf(a.name);
      const indexB = groupOrder.indexOf(b.name);
      if (indexA === -1 && indexB === -1) return a.name.localeCompare(b.name);
      if (indexA === -1) return 1;
      if (indexB === -1) return -1;
      return indexA - indexB;
    });
}
