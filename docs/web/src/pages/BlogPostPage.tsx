import { MDXProvider } from "@mdx-js/react";
import { useRef } from "react";
import CodeBlockCopyButtons from "../components/docs/CodeBlockCopyButtons";
import { mdxComponents } from "../components/docs/MdxComponents";
import Footer from "../components/Footer";
import Header from "../components/Header";
import type { BlogPost } from "../content/blog";

type BlogPostPageProps = {
  post: BlogPost;
};

function formatDate(date: string) {
  const parsed = new Date(`${date}T00:00:00Z`);
  if (Number.isNaN(parsed.valueOf())) return date;
  return parsed.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  });
}

function BlogPostPage({ post }: BlogPostPageProps) {
  const contentRef = useRef<HTMLDivElement>(null);
  const Content = post.Component;

  return (
    <>
      <Header
        navItems={[
          { label: "Docs", href: "/docs" },
          { label: "Blog", href: "/blog", active: true },
          { label: "Benchmark", href: "/benchmark" },
        ]}
        version={__URSULA_VERSION__}
        githubUrl="https://github.com/opendurability/ursula"
      />

      <main className="blog-page">
        <article className="blog-post" data-pagefind-body>
          <header className="blog-post-header">
            <p className="blog-post-date">{formatDate(post.date)}</p>
            <h1 className="blog-post-title">{post.title}</h1>
            {post.author ? <p className="blog-post-author">{post.author}</p> : null}
          </header>
          <div ref={contentRef} className="blog-post-body markdown-content">
            <MDXProvider components={mdxComponents}>
              <Content />
            </MDXProvider>
          </div>
        </article>
      </main>

      <CodeBlockCopyButtons key={`copy-${post.slug}`} contentRef={contentRef} />

      <Footer />
    </>
  );
}

export default BlogPostPage;
