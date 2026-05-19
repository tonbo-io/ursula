import { MDXProvider } from "@mdx-js/react";
import { useRef, type ReactNode } from "react";
import CodeBlockCopyButtons from "./CodeBlockCopyButtons";
import DocsSidebar from "./DocsSidebar";
import DocsTableOfContents from "./DocsTableOfContents";
import { mdxComponents } from "./MdxComponents";

type DocsLayoutProps = {
  activeSlug: string;
  title: string;
  description?: string;
  children: ReactNode;
};

function DocsLayout({ activeSlug, title, description, children }: DocsLayoutProps) {
  const contentRef = useRef<HTMLDivElement>(null);

  return (
    <main className="docs-page">
      <DocsSidebar activeSlug={activeSlug} />
      <article className="docs-content" data-pagefind-body>
        <header className="docs-content-header">
          <h1>{title}</h1>
          {description ? <p className="docs-content-description">{description}</p> : null}
        </header>
        <div ref={contentRef} className="docs-content-body markdown-content">
          <MDXProvider components={mdxComponents}>{children}</MDXProvider>
        </div>
      </article>
      <DocsTableOfContents key={activeSlug} contentRef={contentRef} />
      <CodeBlockCopyButtons key={`copy-${activeSlug}`} contentRef={contentRef} />
    </main>
  );
}

export default DocsLayout;
