import { getAllBlogPosts } from "../content/blog";
import Footer from "../components/Footer";
import Header from "../components/Header";
import { buildAppHref, getBlogPostPath, navigateTo } from "../utils/navigation";

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

function BlogIndexPage() {
  const posts = getAllBlogPosts();

  const handleClick = (path: string) => (event: React.MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    navigateTo(path);
  };

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
        <header className="blog-page-header">
          <h1>Blog</h1>
          <p>News, design notes, and deep dives from the Ursula team.</p>
        </header>

        <ul className="blog-index-list">
          {posts.map((post) => {
            const path = getBlogPostPath(post.slug);
            return (
              <li key={post.slug} className="blog-index-item">
                <a
                  className="blog-index-link"
                  href={buildAppHref(path)}
                  onClick={handleClick(path)}
                >
                  <p className="blog-index-date">{formatDate(post.date)}</p>
                  <h2 className="blog-index-title">{post.title}</h2>
                  <p className="blog-index-description">{post.description}</p>
                </a>
              </li>
            );
          })}
        </ul>
      </main>

      <Footer />
    </>
  );
}

export default BlogIndexPage;
