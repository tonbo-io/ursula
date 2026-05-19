import { getDocsGroups } from "../../content/docs";
import { buildAppHref, getDocsPagePath, isInternalAppPath, navigateTo } from "../../utils/navigation";
import SearchModal from "./SearchModal";

type DocsSidebarProps = {
  activeSlug: string;
};

function DocsSidebar({ activeSlug }: DocsSidebarProps) {
  const groups = getDocsGroups();

  const handleClick = (href: string) => (event: React.MouseEvent<HTMLAnchorElement>) => {
    if (!isInternalAppPath(href)) return;
    event.preventDefault();
    navigateTo(href);
  };

  return (
    <aside className="docs-sidebar" aria-label="Docs navigation">
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
    </aside>
  );
}

export default DocsSidebar;
