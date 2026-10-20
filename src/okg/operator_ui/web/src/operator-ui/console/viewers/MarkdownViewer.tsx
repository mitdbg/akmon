// The Markdown viewer: react-markdown with remark-gfm (tables, task lists,
// strikethrough) and rehype-sanitize (GitHub's schema: no scripts, no
// event handlers, no raw HTML past the allow-list), tightened so an image
// can only come from the artifact's own origin. GitHub's schema allows
// `http:` and `https:` image sources, and `![](https://evil/x.png)` in an
// ingested document would fetch from the operator's browser, the same
// egress the HTML viewer's policy exists to stop. All three are MIT. The
// output is typed with the console's scale in styles/explore.css under
// `.okg-console__rendered`. Lazy, like the other viewers.
import { useEffect, useState, type ReactElement } from "react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeSanitize, { defaultSchema } from "rehype-sanitize";

import { fetchText, reasonOf, type OnViewerState } from "@operator-ui/console/viewers/viewerState";

/** GitHub's schema with no protocol allowed on `src`: only a relative
 *  reference survives sanitising. `sameOriginImage` then resolves it against
 *  the artifact and refuses anything that lands on another origin (a
 *  protocol-relative `//host/x.png` has no scheme and passes the schema). */
export const MARKDOWN_SANITIZE_SCHEMA = {
  ...defaultSchema,
  protocols: { ...defaultSchema.protocols, src: [] },
};

/** The image URL to draw, or null when it would leave the artifact's origin
 *  or is not http(s). `pageHref` is what a relative artifact URL resolves
 *  against (the page's own location in the browser). */
export function sameOriginImage(src: string | undefined, artifactUrl: string, pageHref: string): string | null {
  if (!src) return null;
  try {
    const base = new URL(artifactUrl, pageHref);
    const resolved = new URL(src, base);
    if (resolved.origin !== base.origin) return null;
    if (resolved.protocol !== "http:" && resolved.protocol !== "https:") return null;
    return resolved.href;
  } catch {
    return null;
  }
}

/** The rendered document. Exported so the sanitising can be tested without
 *  a fetch. */
export function RenderedMarkdown({ text, artifactUrl, pageHref }: { text: string; artifactUrl: string; pageHref: string }): ReactElement {
  return (
    <div className="okg-console__rendered" data-rendered-document="markdown" data-testid="explore-content-markdown">
      <Markdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[[rehypeSanitize, MARKDOWN_SANITIZE_SCHEMA]]}
        components={{
          a: ({ href, children }) => (
            <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>
          ),
          img: ({ src, alt }) => {
            const url = sameOriginImage(typeof src === "string" ? src : undefined, artifactUrl, pageHref);
            if (url === null) {
              return <span className="okg-console__rendered-image-withheld" data-image-withheld="">{alt || "image"} (not shown: not from this deployment)</span>;
            }
            return <img src={url} alt={alt ?? ""} />;
          },
        }}
      >
        {text}
      </Markdown>
    </div>
  );
}

export default function MarkdownViewer({ url, onState }: { url: string; onState: OnViewerState }): ReactElement | null {
  const [text, setText] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    setText(null);
    onState({ kind: "pending" });
    fetchText(url).then(
      (body) => { if (!cancelled) { setText(body); onState({ kind: "ready" }); } },
      (error: unknown) => { if (!cancelled) onState({ kind: "failed", reason: reasonOf(error) }); },
    );
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onState is stable for one url
  }, [url]);
  if (text === null) return null;
  return <RenderedMarkdown text={text} artifactUrl={url} pageHref={window.location.href} />;
}
