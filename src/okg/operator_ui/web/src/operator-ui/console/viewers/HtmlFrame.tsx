// The HTML viewer: the page's own text in a sandboxed frame with scripts
// off and a Content-Security-Policy that loads nothing from anywhere. The
// sandbox alone is not enough: Chromium refuses to RUN a script in a frame
// without `allow-scripts`, but still FETCHES its `src`, so a remote script
// tag in an ingested page would reach its origin. `default-src 'none'` in
// the document's own head stops the fetch; inline styles are allowed so the
// page keeps its look, and data: images so an inline figure still draws.
// scripts/operator-ui-design/audit-rules.mjs checks the sandbox attribute and the policy on every
// frame, and the capture fails on any request off the dev server.
import { useEffect, useState, type ReactElement } from "react";

import { fetchText, reasonOf, type OnViewerState } from "@operator-ui/console/viewers/viewerState";

export const FRAME_CSP = "default-src 'none'; style-src 'unsafe-inline'; img-src data:";

/** The page with the policy as the first child of its real head, and with
 *  nothing left that could undo it: every other `meta[http-equiv]` (a refresh
 *  would navigate the frame to a foreign origin) and every `base` (would
 *  rebase the page's URLs) is removed. The document is PARSED, not searched
 *  as text: a string splice after the first "<head>" was defeated by a
 *  comment, a title or an early link (viewers/htmlFrame.test.ts carries the
 *  four documents). The result is the parsed tree serialised with a doctype
 *  so the frame renders in standards mode. Exported for the unit test. */
export function withPolicy(html: string): string {
  const doc = new DOMParser().parseFromString(html, "text/html");
  for (const node of doc.querySelectorAll("meta[http-equiv], base")) node.remove();
  let head = doc.head as HTMLHeadElement | null;
  if (!head) {
    head = doc.createElement("head");
    doc.documentElement.prepend(head);
  }
  const meta = doc.createElement("meta");
  meta.setAttribute("http-equiv", "Content-Security-Policy");
  meta.setAttribute("content", FRAME_CSP);
  head.prepend(meta);
  return `<!doctype html>${doc.documentElement.outerHTML}`;
}

export default function HtmlFrame({
  url, title, onState,
}: { url: string; title: string; onState: OnViewerState }): ReactElement | null {
  const [document_, setDocument] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    setDocument(null);
    onState({ kind: "pending" });
    fetchText(url).then(
      (text) => { if (!cancelled) setDocument(withPolicy(text)); },
      (error: unknown) => { if (!cancelled) onState({ kind: "failed", reason: reasonOf(error) }); },
    );
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onState is stable for one url
  }, [url]);
  if (document_ === null) return null;
  return (
    <iframe
      className="okg-console__frame"
      data-testid="explore-content-html"
      data-csp={FRAME_CSP}
      title={title}
      srcDoc={document_}
      sandbox=""
      referrerPolicy="no-referrer"
      onLoad={() => onState({ kind: "ready", note: "scripts off" })}
    />
  );
}
