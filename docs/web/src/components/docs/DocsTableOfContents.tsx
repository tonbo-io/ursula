import { useEffect, useState, type RefObject } from "react";

type Heading = {
  id: string;
  text: string;
  level: 2 | 3;
};

type DocsTableOfContentsProps = {
  contentRef: RefObject<HTMLDivElement>;
};

function DocsTableOfContents({ contentRef }: DocsTableOfContentsProps) {
  const [headings, setHeadings] = useState<Heading[]>([]);
  const [activeId, setActiveId] = useState<string>("");

  // Scan the content area for h2/h3 headings once the MDX content has mounted.
  useEffect(() => {
    const container = contentRef.current;
    if (!container) return;

    const nodes = container.querySelectorAll<HTMLHeadingElement>("h2, h3");
    const collected: Heading[] = [];

    nodes.forEach((node) => {
      if (!node.id) return;
      collected.push({
        id: node.id,
        text: node.textContent ?? "",
        level: node.tagName === "H2" ? 2 : 3,
      });
    });

    setHeadings(collected);
    setActiveId(collected[0]?.id ?? "");
  }, [contentRef]);

  // Scroll-spy: highlight the heading currently near the top of the viewport.
  useEffect(() => {
    if (headings.length === 0) return;

    const container = contentRef.current;
    if (!container) return;

    const nodes = Array.from(
      container.querySelectorAll<HTMLHeadingElement>("h2, h3"),
    ).filter((node) => node.id);

    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((entry) => entry.isIntersecting)
          .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);

        if (visible.length > 0) {
          setActiveId((visible[0].target as HTMLElement).id);
          return;
        }

        // No heading intersecting the active region — fall back to the last one
        // whose top has already scrolled past the active band.
        const scrolledPast = entries
          .filter((entry) => entry.boundingClientRect.top < 0)
          .sort((a, b) => b.boundingClientRect.top - a.boundingClientRect.top);

        if (scrolledPast.length > 0) {
          setActiveId((scrolledPast[0].target as HTMLElement).id);
        }
      },
      {
        rootMargin: "-96px 0px -70% 0px",
        threshold: [0, 1],
      },
    );

    nodes.forEach((node) => observer.observe(node));
    return () => observer.disconnect();
  }, [headings, contentRef]);

  if (headings.length === 0) {
    return <aside className="docs-toc" aria-label="On this page" />;
  }

  const handleClick = (id: string) => (event: React.MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    const target = document.getElementById(id);
    if (!target) return;
    target.scrollIntoView({ behavior: "smooth", block: "start" });
    window.history.replaceState(null, "", `#${id}`);
    setActiveId(id);
  };

  return (
    <aside className="docs-toc" aria-label="On this page">
      <p className="docs-toc-title">On this page</p>
      <nav>
        <ul className="docs-toc-list">
          {headings.map((heading) => {
            const isActive = heading.id === activeId;
            return (
              <li key={heading.id} className={`docs-toc-item docs-toc-level-${heading.level}`}>
                <a
                  className={isActive ? "docs-toc-link docs-toc-link-active" : "docs-toc-link"}
                  href={`#${heading.id}`}
                  onClick={handleClick(heading.id)}
                >
                  {heading.text}
                </a>
              </li>
            );
          })}
        </ul>
      </nav>
    </aside>
  );
}

export default DocsTableOfContents;
