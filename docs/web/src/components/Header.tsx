import { buildAppHref, isInternalAppPath, navigateTo } from "../utils/navigation";

type HeaderNavItem = {
  label: string;
  href?: string;
  active?: boolean;
  caret?: boolean;
  disabled?: boolean;
  children?: { label: string; href: string }[];
};

type HeaderCta = {
  label: string;
  href: string;
};

type HeaderProps = {
  navItems: HeaderNavItem[];
  version?: string;
  githubUrl?: string;
  cta?: HeaderCta;
  className?: string;
};

function Header({ navItems, version, githubUrl, cta, className }: HeaderProps) {
  const handleLinkClick = (href: string) => (event: React.MouseEvent<HTMLElement>) => {
    if (!isInternalAppPath(href)) {
      return;
    }

    event.preventDefault();
    navigateTo(href);
  };

  const showActions = Boolean(githubUrl) || Boolean(cta);

  return (
    <header className={className ? `site-header ${className}` : "site-header"}>
      <div className="header-primary">
        <a
          className="header-brand"
          aria-label="Ursula"
          href={buildAppHref("/")}
          onClick={handleLinkClick("/")}
        >
          <span className="header-brand-wordmark">
            <span className="header-brand-wordmark-accent">Ursula</span>
          </span>
        </a>

        {version ? (
          githubUrl ? (
            <a
              className="header-brand-version"
              href={`${githubUrl}/releases`}
              target="_blank"
              rel="noopener noreferrer"
              title="Releases"
            >
              v{version}
            </a>
          ) : (
            <span className="header-brand-version">v{version}</span>
          )
        ) : null}

        <nav className="nav-links" aria-label="Primary">
          {navItems.map((item) => {
            if (item.children && item.children.length > 0) {
              return (
                <div className="nav-link-group" key={item.label}>
                  <button
                    type="button"
                    className={
                      item.active
                        ? "nav-link nav-link-active nav-link-trigger"
                        : "nav-link nav-link-trigger"
                    }
                    aria-haspopup="menu"
                  >
                    <span>{item.label}</span>
                    <span className="nav-caret" aria-hidden="true" />
                  </button>
                  <div className="nav-dropdown-menu" role="menu">
                    {item.children.map((child) => (
                      <a
                        key={child.label}
                        className="nav-dropdown-item"
                        href={child.href}
                        role="menuitem"
                        target={isInternalAppPath(child.href) ? undefined : "_blank"}
                        rel={isInternalAppPath(child.href) ? undefined : "noopener noreferrer"}
                        onClick={handleLinkClick(child.href)}
                      >
                        {child.label}
                      </a>
                    ))}
                  </div>
                </div>
              );
            }

            if (item.href && !item.disabled) {
              return (
                <a
                  key={item.label}
                  className={item.active ? "nav-link nav-link-active" : "nav-link"}
                  href={isInternalAppPath(item.href) ? buildAppHref(item.href) : item.href}
                  onClick={handleLinkClick(item.href)}
                  aria-current={item.active ? "page" : undefined}
                >
                  <span>{item.label}</span>
                  {item.caret ? <span className="nav-caret" aria-hidden="true" /> : null}
                </a>
              );
            }

            return (
              <span
                key={item.label}
                className={
                  item.active
                    ? "nav-link nav-link-active nav-link-disabled"
                    : "nav-link nav-link-disabled"
                }
                aria-disabled="true"
              >
                <span>{item.label}</span>
                {item.caret ? <span className="nav-caret" aria-hidden="true" /> : null}
              </span>
            );
          })}
        </nav>
      </div>

      {showActions ? (
        <div className="header-actions">
          {githubUrl ? (
            <a
              className="header-github-link"
              href={githubUrl}
              target="_blank"
              rel="noopener noreferrer"
              aria-label="GitHub repository"
              title="GitHub"
            >
              <svg
                viewBox="0 0 24 24"
                width="20"
                height="20"
                fill="currentColor"
                aria-hidden="true"
              >
                <path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.4 3-.405 1.02.005 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12" />
              </svg>
            </a>
          ) : null}
          {cta ? (
            <a
              className="button button-header button-primary"
              href={cta.href}
              target="_blank"
              rel="noopener noreferrer"
            >
              {cta.label}
            </a>
          ) : null}
        </div>
      ) : null}
    </header>
  );
}

export default Header;
