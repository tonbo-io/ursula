import type { ComponentType } from "react";

export type BlogPostMeta = {
  slug: string;
  title: string;
  description: string;
  date: string;
  author?: string;
};

export type BlogPost = BlogPostMeta & {
  Component: ComponentType;
};

type MdxModule = {
  default: ComponentType;
  frontmatter?: {
    slug?: string;
    title?: string;
    description?: string;
    date?: string;
    author?: string;
  };
};

const blogModules = import.meta.glob<MdxModule>("./posts/**/*.mdx", { eager: true });

function inferSlug(path: string) {
  return path.replace(/^\.\/posts\//, "").replace(/\.mdx$/, "");
}

function buildPost(path: string, mod: MdxModule): BlogPost {
  const fallbackSlug = inferSlug(path);
  const fm = mod.frontmatter ?? {};

  const slug = fm.slug ?? fallbackSlug;

  if (!fm.title) {
    throw new Error(`Blog post is missing a title in frontmatter: ${path}`);
  }
  if (!fm.date) {
    throw new Error(`Blog post is missing a date in frontmatter: ${path}`);
  }
  if (!fm.description) {
    throw new Error(`Blog post is missing a description in frontmatter: ${path}`);
  }

  return {
    slug,
    title: fm.title,
    description: fm.description,
    date: fm.date,
    author: fm.author,
    Component: mod.default,
  };
}

const blogPosts: BlogPost[] = Object.entries(blogModules)
  .map(([path, mod]) => buildPost(path, mod))
  .sort((a, b) => (a.date < b.date ? 1 : a.date > b.date ? -1 : 0));

export function getAllBlogPosts(): BlogPost[] {
  return blogPosts;
}

export function getBlogPostBySlug(slug: string): BlogPost | undefined {
  return blogPosts.find((post) => post.slug === slug);
}
