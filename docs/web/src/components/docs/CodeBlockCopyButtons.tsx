import { useEffect, type RefObject } from "react";

type CodeBlockCopyButtonsProps = {
  contentRef: RefObject<HTMLDivElement>;
};

const COPY_ICON_SVG = `
<svg class="docs-code-copy-icon docs-code-copy-idle" width="14" height="14" viewBox="0 0 20 20" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
  <path d="M12.5 3A1.5 1.5 0 0 1 14 4.5V6h1.5A1.5 1.5 0 0 1 17 7.5v8a1.5 1.5 0 0 1-1.5 1.5h-8A1.5 1.5 0 0 1 6 15.5V14H4.5A1.5 1.5 0 0 1 3 12.5v-8A1.5 1.5 0 0 1 4.5 3zm1.5 9.5a1.5 1.5 0 0 1-1.5 1.5H7v1.5a.5.5 0 0 0 .5.5h8a.5.5 0 0 0 .5-.5v-8a.5.5 0 0 0-.5-.5H14zM4.5 4a.5.5 0 0 0-.5.5v8a.5.5 0 0 0 .5.5h8a.5.5 0 0 0 .5-.5v-8a.5.5 0 0 0-.5-.5z"/>
</svg>
`;

const DONE_ICON_SVG = `
<svg class="docs-code-copy-icon docs-code-copy-done" width="14" height="14" viewBox="0 0 20 20" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
  <path d="M15.188 5.11a.5.5 0 0 1 .752.626l-.056.084-7.5 9a.5.5 0 0 1-.738.033l-3.5-3.5-.064-.078a.501.501 0 0 1 .693-.693l.078.064 3.113 3.113 7.15-8.58z"/>
</svg>
`;

function CodeBlockCopyButtons({ contentRef }: CodeBlockCopyButtonsProps) {
  useEffect(() => {
    const container = contentRef.current;
    if (!container) return;

    const cleanups: Array<() => void> = [];

    const pres = container.querySelectorAll<HTMLPreElement>("pre");

    pres.forEach((pre) => {
      if (pre.querySelector(".docs-code-copy")) return;

      pre.classList.add("has-copy-button");

      const button = document.createElement("button");
      button.type = "button";
      button.className = "docs-code-copy";
      button.setAttribute("aria-label", "Copy code");
      button.innerHTML = COPY_ICON_SVG + DONE_ICON_SVG;

      let resetTimer: number | null = null;

      const handleClick = async (event: MouseEvent) => {
        event.preventDefault();
        const code = pre.querySelector("code");
        const text = (code?.textContent ?? pre.textContent ?? "").replace(/\n$/, "");

        try {
          await navigator.clipboard.writeText(text);
        } catch {
          // Fallback for contexts without clipboard API (e.g. non-secure origins)
          const textarea = document.createElement("textarea");
          textarea.value = text;
          textarea.setAttribute("readonly", "");
          textarea.style.position = "absolute";
          textarea.style.left = "-9999px";
          document.body.appendChild(textarea);
          textarea.select();
          try {
            document.execCommand("copy");
          } catch {
            return;
          } finally {
            document.body.removeChild(textarea);
          }
        }

        button.classList.add("is-copied");
        if (resetTimer !== null) window.clearTimeout(resetTimer);
        resetTimer = window.setTimeout(() => {
          button.classList.remove("is-copied");
        }, 1500);
      };

      button.addEventListener("click", handleClick);
      pre.appendChild(button);

      cleanups.push(() => {
        button.removeEventListener("click", handleClick);
        if (resetTimer !== null) window.clearTimeout(resetTimer);
        button.remove();
        pre.classList.remove("has-copy-button");
      });
    });

    return () => {
      cleanups.forEach((fn) => fn());
    };
  }, [contentRef]);

  return null;
}

export default CodeBlockCopyButtons;
