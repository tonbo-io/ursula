import {
  Children,
  Fragment,
  isValidElement,
  useId,
  useState,
  type ReactElement,
  type ReactNode,
} from "react";
import { buildAppHref, isInternalAppPath, navigateTo } from "../../utils/navigation";

type CardProps = {
  title: string;
  href?: string;
  icon?: ReactNode;
  children?: ReactNode;
};

export function Card({ title, href, icon, children }: CardProps) {
  const handleClick = (event: React.MouseEvent<HTMLAnchorElement>) => {
    if (!href || !isInternalAppPath(href)) return;
    event.preventDefault();
    navigateTo(href);
  };

  const content = (
    <>
      {icon ? <span className="docs-card-icon">{icon}</span> : null}
      <span className="docs-card-title">{title}</span>
      {children ? <span className="docs-card-body">{children}</span> : null}
    </>
  );

  if (href) {
    const isInternal = isInternalAppPath(href);
    return (
      <a
        className="docs-card"
        href={isInternal ? buildAppHref(href) : href}
        onClick={handleClick}
        target={isInternal ? undefined : "_blank"}
        rel={isInternal ? undefined : "noopener noreferrer"}
      >
        {content}
      </a>
    );
  }

  return <div className="docs-card">{content}</div>;
}

type CardGroupProps = {
  cols?: number;
  children?: ReactNode;
};

export function CardGroup({ cols = 2, children }: CardGroupProps) {
  return (
    <div
      className="docs-card-group"
      style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}
    >
      {children}
    </div>
  );
}

type CalloutProps = {
  children?: ReactNode;
};

function Callout({ tone, children }: CalloutProps & { tone: string }) {
  return <div className={`docs-callout docs-callout-${tone}`}>{children}</div>;
}

export function Note({ children }: CalloutProps) {
  return <Callout tone="note">{children}</Callout>;
}

export function Tip({ children }: CalloutProps) {
  return <Callout tone="tip">{children}</Callout>;
}

export function Warning({ children }: CalloutProps) {
  return <Callout tone="warning">{children}</Callout>;
}

export function Info({ children }: CalloutProps) {
  return <Callout tone="info">{children}</Callout>;
}

type StepsProps = {
  children?: ReactNode;
};

export function Steps({ children }: StepsProps) {
  return <ol className="docs-steps">{Children.toArray(children)}</ol>;
}

type StepProps = {
  title: string;
  children?: ReactNode;
};

export function Step({ title, children }: StepProps) {
  return (
    <li className="docs-step">
      <div className="docs-step-title">{title}</div>
      <div className="docs-step-body">{children}</div>
    </li>
  );
}

type CodeGroupProps = {
  children?: ReactNode;
};

type CodeGroupTab = {
  label: string;
  node: ReactElement;
};

function findDataTitle(node: ReactNode): string | undefined {
  // MDX wraps each fenced code block in a Fragment whose single child is the
  // <pre> emitted by rehype-shiki. The rehype-shiki parseMetaString hook in
  // vite.config.ts forwards `title="..."` from the fence onto the <pre> as
  // `data-title`. Walk the tree until we find a string-valued data-title.
  let found: string | undefined;
  Children.forEach(node, (child) => {
    if (found || !isValidElement(child)) return;
    const props = (child as ReactElement<Record<string, unknown>>).props ?? {};
    if (typeof props["data-title"] === "string") {
      found = props["data-title"] as string;
      return;
    }
    if ((child as ReactElement).type === Fragment && props.children !== undefined) {
      found = findDataTitle(props.children as ReactNode);
    }
  });
  return found;
}

function extractCodeGroupTabs(children: ReactNode): CodeGroupTab[] {
  const tabs: CodeGroupTab[] = [];
  Children.forEach(children, (child) => {
    if (!isValidElement(child)) return;
    const dataTitle = findDataTitle(child);
    tabs.push({
      label: dataTitle ?? `Tab ${tabs.length + 1}`,
      node: child,
    });
  });
  return tabs;
}

export function CodeGroup({ children }: CodeGroupProps) {
  const tabs = extractCodeGroupTabs(children);
  const [activeIndex, setActiveIndex] = useState(0);

  if (tabs.length === 0) {
    return null;
  }

  return (
    <div className="docs-code-group">
      <div className="docs-code-group-tabs" role="tablist">
        {tabs.map((tab, index) => (
          <button
            key={`${tab.label}-${index}`}
            type="button"
            role="tab"
            aria-selected={activeIndex === index}
            className={
              activeIndex === index
                ? "docs-code-group-tab docs-code-group-tab-active"
                : "docs-code-group-tab"
            }
            onClick={() => setActiveIndex(index)}
          >
            {tab.label}
          </button>
        ))}
      </div>
      {tabs.map((tab, index) => (
        <div
          key={`panel-${tab.label}-${index}`}
          role="tabpanel"
          hidden={activeIndex !== index}
          className="docs-code-group-panel"
        >
          {tab.node}
        </div>
      ))}
    </div>
  );
}

type AccordionGroupProps = {
  children?: ReactNode;
};

export function AccordionGroup({ children }: AccordionGroupProps) {
  return <div className="docs-accordion-group">{children}</div>;
}

type AccordionProps = {
  title: string;
  defaultOpen?: boolean;
  children?: ReactNode;
};

export function Accordion({ title, defaultOpen, children }: AccordionProps) {
  const [isOpen, setIsOpen] = useState(Boolean(defaultOpen));
  const headingId = useId();
  const panelId = useId();

  return (
    <div className={isOpen ? "docs-accordion docs-accordion-open" : "docs-accordion"}>
      <button
        type="button"
        id={headingId}
        aria-expanded={isOpen}
        aria-controls={panelId}
        className="docs-accordion-summary"
        onClick={() => setIsOpen((prev) => !prev)}
      >
        <span className="docs-accordion-caret" aria-hidden="true" />
        <span className="docs-accordion-title">{title}</span>
      </button>
      <div
        id={panelId}
        role="region"
        aria-labelledby={headingId}
        hidden={!isOpen}
        className="docs-accordion-body"
      >
        {children}
      </div>
    </div>
  );
}

type FieldProps = {
  name?: string;
  path?: string;
  query?: string;
  header?: string;
  body?: string;
  type?: string;
  required?: boolean;
  default?: string;
  children?: ReactNode;
};

function ApiField({ name, path, query, header, body, type, required, default: defaultValue, children }: FieldProps) {
  const label = name ?? path ?? query ?? header ?? body ?? "";
  const scope = path ? "path" : query ? "query" : header ? "header" : body ? "body" : "field";

  return (
    <div className="docs-api-field">
      <div className="docs-api-field-head">
        <code>{label}</code>
        <span className="docs-api-field-scope">{scope}</span>
        {type ? <span className="docs-api-field-type">{type}</span> : null}
        {required ? <span className="docs-api-field-required">required</span> : null}
        {defaultValue !== undefined ? (
          <span className="docs-api-field-default">default: {defaultValue || "\"\""}</span>
        ) : null}
      </div>
      {children ? <div className="docs-api-field-body">{children}</div> : null}
    </div>
  );
}

export function ParamField(props: FieldProps) {
  return <ApiField {...props} />;
}

export function ResponseField(props: FieldProps) {
  return <ApiField {...props} />;
}

type ExampleProps = {
  children?: ReactNode;
};

export function RequestExample({ children }: ExampleProps) {
  return <div className="docs-example docs-request-example">{children}</div>;
}

export function ResponseExample({ children }: ExampleProps) {
  return <div className="docs-example docs-response-example">{children}</div>;
}

type TabProps = {
  title: string;
  children?: ReactNode;
};

export function Tab({ children }: TabProps) {
  return <>{children}</>;
}

type TabEntry = {
  title: string;
  node: ReactElement;
};

function extractTabs(children: ReactNode): TabEntry[] {
  const tabs: TabEntry[] = [];
  Children.forEach(children, (child) => {
    if (!isValidElement(child)) return;
    const props = (child as ReactElement<TabProps>).props;
    tabs.push({ title: props.title ?? `Tab ${tabs.length + 1}`, node: child });
  });
  return tabs;
}

export function Tabs({ children }: { children?: ReactNode }) {
  const tabs = extractTabs(children);
  const [activeIndex, setActiveIndex] = useState(0);

  if (tabs.length === 0) {
    return null;
  }

  return (
    <div className="docs-tabs">
      <div className="docs-tabs-list" role="tablist">
        {tabs.map((tab, index) => (
          <button
            key={`${tab.title}-${index}`}
            type="button"
            role="tab"
            aria-selected={activeIndex === index}
            className={activeIndex === index ? "docs-tab docs-tab-active" : "docs-tab"}
            onClick={() => setActiveIndex(index)}
          >
            {tab.title}
          </button>
        ))}
      </div>
      {tabs.map((tab, index) => (
        <div
          key={`tab-panel-${tab.title}-${index}`}
          role="tabpanel"
          hidden={activeIndex !== index}
          className="docs-tab-panel"
        >
          {tab.node.props.children}
        </div>
      ))}
    </div>
  );
}

export function Expandable({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <details className="docs-expandable">
      <summary>{title}</summary>
      <div className="docs-expandable-body">{children}</div>
    </details>
  );
}

export const mdxComponents = {
  Card,
  CardGroup,
  Note,
  Tip,
  Warning,
  Info,
  Steps,
  Step,
  CodeGroup,
  AccordionGroup,
  Accordion,
  ParamField,
  ResponseField,
  RequestExample,
  ResponseExample,
  Tabs,
  Tab,
  Expandable,
};
