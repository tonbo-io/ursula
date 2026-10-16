/// <reference types="vite/client" />

type DocsVersionInfo = {
  /** "0.8" for the latest docs, or an archived id such as "0.7". */
  id: string;
  /** Release the pages come from, such as "0.7.6". */
  release: string;
  /** Public root, "/docs" or "/docs/<id>". */
  root: string;
  archived: boolean;
  /** Where readers of an archived version learn how to move off it. */
  upgrade?: string;
  /** Page slugs that exist in this version. */
  slugs: string[];
};

declare const __URSULA_VERSION__: string;
declare const __DOCS_VERSION__: string | null;
declare const __DOCS_VERSIONS__: DocsVersionInfo[];
