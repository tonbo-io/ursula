import { buildAppHref, isInternalAppPath } from "../utils/navigation";

type FooterLink = {
  label: string;
  href?: string;
};

const footerColumns = [
  {
    title: "Docs",
    links: [
      { label: "Introduction", href: "/docs" },
      { label: "API reference", href: "/docs/api/overview" },
      { label: "Blog", href: "/blog" },
      { label: "Benchmark", href: "/benchmark" },
    ] satisfies FooterLink[],
  },
  {
    title: "Project",
    links: [
      { label: "Open Durability", href: "https://github.com/tonbo-io/ursula" },
      { label: "GitHub", href: "https://github.com/tonbo-io/ursula" },
    ] satisfies FooterLink[],
  },
  {
    title: "For agents",
    links: [
      { label: "llms.txt", href: "/llms.txt" },
      { label: "llms-full.txt", href: "/llms-full.txt" },
    ] satisfies FooterLink[],
  },
] as const;

function Footer() {
  return (
    <footer className="site-footer">
      <div className="footer-brand-column">
        <p className="footer-brand-title">Ursula</p>
        <p className="footer-copyright">Distributed Durable Streams over HTTP, backed by S3.</p>
        <p className="footer-typeplate">v{__URSULA_VERSION__} · apache-2.0</p>
        <div className="footer-built-by">
          <span className="footer-built-by-label">Built by</span>
          <a
            href="https://tonbo.io/"
            target="_blank"
            rel="noopener noreferrer"
            className="footer-built-by-link"
          >
            <img src="/assets/tonbo.png" alt="" width="20" height="20" />
            <span>Tonbo</span>
          </a>
        </div>
      </div>

      <div className="footer-nav-columns">
        {footerColumns.map((column) => (
          <div className="footer-nav-column" key={column.title}>
            <p className="footer-nav-title">{column.title}</p>
            <div className="footer-nav-links">
              {column.links.map((link) => (
                <a
                  key={link.label}
                  className="footer-nav-link"
                  href={link.href && isInternalAppPath(link.href) ? buildAppHref(link.href) : link.href}
                  target={link.href?.startsWith("http") ? "_blank" : undefined}
                  rel={link.href?.startsWith("http") ? "noopener noreferrer" : undefined}
                >
                  {link.label}
                </a>
              ))}
            </div>
          </div>
        ))}
      </div>
    </footer>
  );
}

export default Footer;
