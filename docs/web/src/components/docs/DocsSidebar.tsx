import { useMemo, useState } from "react";
import { getDocsGroups } from "../../content/docs";
import { buildAppHref, getDocsPagePath, isInternalAppPath, navigateTo } from "../../utils/navigation";
import SearchModal from "./SearchModal";

type DocsSidebarProps = {
  activeSlug: string;
};

function DocsSidebar({ activeSlug }: DocsSidebarProps) {
  const groups = getDocsGroups();
  const [isOpen, setIsOpen] = useState(false);

  const activePageTitle = useMemo(() => {
    for (const group of groups) {
      const found = group.pages.find((page) => page.slug === activeSlug);
      if (found) return found.title;
    }
    return "Docs";
  }, [groups, activeSlug]);

  const handleClick = (href: string) => (event: React.MouseEvent<HTMLAnchorElement>) => {
    if (!isInternalAppPath(href)) return;
    event.preventDefault();
    setIsOpen(false);
    navigateTo(href);
  };

  return (
    <aside
      className={`docs-sidebar${isOpen ? " docs-sidebar-open" : ""}`}
      aria-label="Docs navigation"
    >
      <button
        type="button"
        className="docs-sidebar-toggle"
        aria-expanded={isOpen}
        aria-controls="docs-sidebar-panel"
        onClick={() => setIsOpen((value) => !value)}
      >
        <span className="docs-sidebar-toggle-label">{activePageTitle}</span>
        <span className="docs-sidebar-toggle-icon" aria-hidden="true">
          <svg viewBox="0 0 12 12" width="12" height="12" fill="none">
            <path
              d="M2.5 4.5L6 8L9.5 4.5"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </span>
      </button>
      <div id="docs-sidebar-panel" className="docs-sidebar-panel">
        <SearchModal />
        <nav>
          {groups.map((group) => (
            <div className="docs-sidebar-group" key={group.name}>
              <p className="docs-sidebar-group-title">{group.name}</p>
              <ul className="docs-sidebar-group-list">
                {group.pages.map((page) => {
                  const href = getDocsPagePath(page.slug);
                  const isActive = page.slug === activeSlug;
                  return (
                    <li key={page.slug}>
                      <a
                        className={isActive ? "docs-sidebar-link docs-sidebar-link-active" : "docs-sidebar-link"}
                        href={buildAppHref(href)}
                        onClick={handleClick(href)}
                        aria-current={isActive ? "page" : undefined}
                      >
                        {page.title}
                      </a>
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </nav>
      </div>
    </aside>
  );
}

export default DocsSidebar;
