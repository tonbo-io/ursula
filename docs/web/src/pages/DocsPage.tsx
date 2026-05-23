import type { DocsPage as DocsPageType } from "../content/docs";
import DocsLayout from "../components/docs/DocsLayout";
import Footer from "../components/Footer";
import Header from "../components/Header";

type DocsPageProps = {
  page: DocsPageType;
};

function DocsPage({ page }: DocsPageProps) {
  const Content = page.Component;

  return (
    <>
      <Header
        navItems={[
          { label: "Docs", href: "/docs", active: true },
          { label: "Blog", href: "/blog" },
          { label: "Benchmark", href: "/benchmark" },
          { label: "Status", href: "/status" },
        ]}
        version={__URSULA_VERSION__}
        githubUrl="https://github.com/tonbo-io/ursula"
      />

      <DocsLayout activeSlug={page.slug} title={page.title} description={page.description}>
        <Content />
      </DocsLayout>

      <Footer />
    </>
  );
}

export default DocsPage;
