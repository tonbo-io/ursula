import { useEffect, useState } from "react";
import { getBlogPostBySlug } from "./content/blog";
import { getDocsPageBySlug } from "./content/docs";
import BenchmarkPage from "./pages/BenchmarkPage";
import BlogIndexPage from "./pages/BlogIndexPage";
import BlogPostPage from "./pages/BlogPostPage";
import DocsPage from "./pages/DocsPage";
import {
  BENCHMARK_PATH,
  BLOG_PAGE_PREFIX,
  BLOG_PATH,
  DOCS_PAGE_PREFIX,
  DOCS_PATH,
  HOME_PATH,
  getCurrentAppPath,
} from "./utils/navigation";

type AppProps = {
  initialUrl?: string;
};

function resolveDocsSlug(path: string) {
  if (path === HOME_PATH || path === DOCS_PATH) return "introduction";
  if (path.startsWith(DOCS_PAGE_PREFIX)) return path.slice(DOCS_PAGE_PREFIX.length) || "overview";
  return "overview";
}

function App({ initialUrl }: AppProps) {
  const [currentPath, setCurrentPath] = useState(() => getCurrentAppPath(initialUrl));

  useEffect(() => {
    const onChange = () => setCurrentPath(getCurrentAppPath());
    window.addEventListener("popstate", onChange);
    return () => window.removeEventListener("popstate", onChange);
  }, []);

  useEffect(() => {
    if (currentPath === BENCHMARK_PATH) {
      document.title = "OSS HTTP Streams Benchmark | Ursula";
      return;
    }

    if (currentPath === BLOG_PATH) {
      document.title = "Blog | Ursula";
      return;
    }

    if (currentPath.startsWith(BLOG_PAGE_PREFIX)) {
      const slug = currentPath.slice(BLOG_PAGE_PREFIX.length);
      const post = getBlogPostBySlug(slug);
      document.title = post ? `${post.title} | Ursula` : "Ursula";
      return;
    }

    const slug = resolveDocsSlug(currentPath);
    const page = getDocsPageBySlug(slug);
    document.title = page ? `${page.title} | Ursula` : "Ursula";
  }, [currentPath]);

  if (currentPath === BENCHMARK_PATH) {
    return (
      <div className="page-shell">
        <BenchmarkPage />
      </div>
    );
  }

  if (currentPath === BLOG_PATH) {
    return (
      <div className="page-shell">
        <BlogIndexPage />
      </div>
    );
  }

  if (currentPath.startsWith(BLOG_PAGE_PREFIX)) {
    const slug = currentPath.slice(BLOG_PAGE_PREFIX.length);
    const post = getBlogPostBySlug(slug);
    if (post) {
      return (
        <div className="page-shell">
          <BlogPostPage post={post} />
        </div>
      );
    }
    return <div className="page-shell">Post not found.</div>;
  }

  const slug = resolveDocsSlug(currentPath);
  const page = getDocsPageBySlug(slug);

  if (!page) {
    return <div className="page-shell">Page not found.</div>;
  }

  return (
    <div className="page-shell">
      <DocsPage page={page} />
    </div>
  );
}

export default App;
