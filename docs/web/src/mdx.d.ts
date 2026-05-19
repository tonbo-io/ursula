declare module "*.mdx" {
  import type { ComponentType } from "react";

  export const frontmatter: Record<string, unknown>;
  const MDXComponent: ComponentType<Record<string, unknown>>;
  export default MDXComponent;
}

declare module "/pagefind/pagefind.js" {
  type PagefindSubResult = {
    url: string;
    title: string;
    excerpt: string;
  };

  type PagefindResultData = {
    url: string;
    excerpt: string;
    meta: { title?: string };
    sub_results: PagefindSubResult[];
  };

  type PagefindResult = {
    id: string;
    data: () => Promise<PagefindResultData>;
  };

  export function init(): Promise<void>;
  export function search(query: string): Promise<{ results: PagefindResult[] }>;
}
