import { renderToString } from "react-dom/server";
import App from "./App";
import { getAllBlogPosts, getBlogPostBySlug } from "./content/blog";
import { getAllDocsPages, getDocsPageBySlug } from "./content/docs";
import {
  BENCHMARK_PATH,
  BLOG_PAGE_PREFIX,
  BLOG_PATH,
  CHAOS_TEST_PATH,
  DOCS_PAGE_PREFIX,
  DOCS_PATH,
  HOME_PATH,
  STATUS_PATH,
} from "./utils/navigation";

const ORIGIN = "https://ursula.tonbo.io";
const DEFAULT_DESCRIPTION =
  "Open-source Distributed Durable Streams over HTTP, backed by S3.";
// Version query busts scraper caches (X keys cards by exact URL and has
// no manual refresh); bump it whenever the card image changes.
const DEFAULT_OG_IMAGE = `${ORIGIN}/og-default.png?v=20260713`;

type HeadData = {
  title: string;
  description: string;
  ogType: "website" | "article";
  ogImage: string;
  canonical: string;
};

function absoluteUrl(path: string) {
  return `${ORIGIN}${path === HOME_PATH ? "/" : path}`;
}

function getHeadForUrl(url: string): HeadData {
  const canonical = absoluteUrl(url);

  if (url === HOME_PATH) {
    return {
      title: "Ursula — Durable Streams over HTTP, backed by S3",
      description:
        "Self-hosted, distributed Durable Streams server. Quorum-replicated appends in single-digit milliseconds, cold data on plain S3, any HTTP client is a valid client.",
      ogType: "website",
      ogImage: DEFAULT_OG_IMAGE,
      canonical,
    };
  }

  if (url === DOCS_PATH || url.startsWith(DOCS_PAGE_PREFIX)) {
    const slug =
      url === DOCS_PATH ? "introduction" : url.slice(DOCS_PAGE_PREFIX.length) || "overview";
    const page = getDocsPageBySlug(slug);

    if (page) {
      return {
        title: `${page.title} | Ursula`,
        description: page.description ?? DEFAULT_DESCRIPTION,
        ogType: "website",
        ogImage: DEFAULT_OG_IMAGE,
        canonical,
      };
    }
  }

  if (url === BENCHMARK_PATH) {
    return {
      title: "OSS HTTP Streams Benchmark | Ursula",
      description: "Uniform HTTP benchmark comparing Ursula, Durable Streams, and S2 Lite S3 on EC2.",
      ogType: "website",
      ogImage: DEFAULT_OG_IMAGE,
      canonical,
    };
  }

  if (url === CHAOS_TEST_PATH || url === STATUS_PATH) {
    return {
      title: "Chaos Test | Ursula",
      description: "Live view of Ursula's continuous EC2 chaos test.",
      ogType: "website",
      ogImage: DEFAULT_OG_IMAGE,
      canonical,
    };
  }

  if (url === BLOG_PATH) {
    return {
      title: "Blog | Ursula",
      description: "News, design notes, and deep dives from the Ursula team.",
      ogType: "website",
      ogImage: DEFAULT_OG_IMAGE,
      canonical,
    };
  }

  if (url.startsWith(BLOG_PAGE_PREFIX)) {
    const slug = url.slice(BLOG_PAGE_PREFIX.length);
    const post = getBlogPostBySlug(slug);
    if (post) {
      return {
        title: `${post.title} | Ursula`,
        description: post.description,
        ogType: "article",
        ogImage: DEFAULT_OG_IMAGE,
        canonical,
      };
    }
  }

  return {
    title: "Ursula",
    description: DEFAULT_DESCRIPTION,
    ogType: "website",
    ogImage: DEFAULT_OG_IMAGE,
    canonical,
  };
}

function buildHeadElements(head: HeadData) {
  return new Set([
    { type: "meta", props: { name: "description", content: head.description } },
    { type: "link", props: { rel: "canonical", href: head.canonical } },
    { type: "meta", props: { property: "og:type", content: head.ogType } },
    { type: "meta", props: { property: "og:site_name", content: "Ursula" } },
    { type: "meta", props: { property: "og:title", content: head.title } },
    { type: "meta", props: { property: "og:description", content: head.description } },
    { type: "meta", props: { property: "og:url", content: head.canonical } },
    { type: "meta", props: { property: "og:image", content: head.ogImage } },
    { type: "meta", props: { name: "twitter:card", content: "summary_large_image" } },
    { type: "meta", props: { name: "twitter:title", content: head.title } },
    { type: "meta", props: { name: "twitter:description", content: head.description } },
    { type: "meta", props: { name: "twitter:image", content: head.ogImage } },
  ]);
}

export async function prerender(data: { url: string }) {
  const head = getHeadForUrl(data.url);
  const html = renderToString(<App initialUrl={data.url} />);

  const docsPageLinks = getAllDocsPages().map((page) => `${DOCS_PAGE_PREFIX}${page.slug}`);
  const blogPostLinks = getAllBlogPosts().map((post) => `${BLOG_PAGE_PREFIX}${post.slug}`);
  const links = new Set<string>([
    DOCS_PATH,
    BENCHMARK_PATH,
    CHAOS_TEST_PATH,
    STATUS_PATH,
    BLOG_PATH,
    ...docsPageLinks,
    ...blogPostLinks,
  ]);

  return {
    html,
    links,
    head: {
      lang: "en",
      title: head.title,
      elements: buildHeadElements(head),
    },
  };
}
