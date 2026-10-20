// The PDF viewer: pdf.js (pdfjs-dist, Apache-2.0) renders every page to a
// canvas in one scrollable column. The worker ships from this bundle as its
// own asset; nothing is loaded from a CDN. Lazy: this module is reached only
// through a dynamic import from the Content tab.
import { useEffect, useRef, useState, type ReactElement } from "react";
import { getDocument, GlobalWorkerOptions } from "pdfjs-dist";
import workerUrl from "pdfjs-dist/build/pdf.worker.min.mjs?url";

import { reasonOf, type OnViewerState } from "@operator-ui/console/viewers/viewerState";

GlobalWorkerOptions.workerSrc = workerUrl;

export interface PdfPosition {
  pageCount: number;
  currentPage: number;
}

export default function PdfViewer({
  url, onState, onPosition,
}: { url: string; onState: OnViewerState; onPosition: (position: PdfPosition) => void }): ReactElement {
  const column = useRef<HTMLDivElement>(null);
  const [pageCount, setPageCount] = useState(0);

  useEffect(() => {
    let cancelled = false;
    const host = column.current;
    if (!host) return undefined;
    host.replaceChildren();
    onState({ kind: "pending" });
    (async () => {
      const document_ = await getDocument({ url }).promise;
      if (cancelled) return;
      setPageCount(document_.numPages);
      onPosition({ pageCount: document_.numPages, currentPage: 1 });
      const width = Math.max(320, host.clientWidth - 2);
      for (let number = 1; number <= document_.numPages; number += 1) {
        const page = await document_.getPage(number);
        if (cancelled) return;
        const unscaled = page.getViewport({ scale: 1 });
        const scale = width / unscaled.width;
        const ratio = window.devicePixelRatio || 1;
        const viewport = page.getViewport({ scale: scale * ratio });
        const canvas = window.document.createElement("canvas");
        canvas.className = "okg-console__pdf-page";
        canvas.width = Math.floor(viewport.width);
        canvas.height = Math.floor(viewport.height);
        canvas.style.width = `${Math.floor(viewport.width / ratio)}px`;
        canvas.style.height = `${Math.floor(viewport.height / ratio)}px`;
        canvas.setAttribute("data-page", String(number));
        canvas.setAttribute("aria-label", `Page ${number} of ${document_.numPages}`);
        canvas.setAttribute("role", "img");
        const context = canvas.getContext("2d");
        if (!context) throw new Error("the browser gave no 2d canvas context");
        host.appendChild(canvas);
        await page.render({ canvasContext: context, viewport, canvas }).promise;
      }
      if (!cancelled) onState({ kind: "ready", note: `${document_.numPages} pages drawn` });
    })().catch((error: unknown) => {
      if (!cancelled) onState({ kind: "failed", reason: `pdf.js could not render ${url}: ${reasonOf(error)}` });
    });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the callbacks are stable for one url
  }, [url]);

  // The page nearest the top of the column is the current one.
  const onScroll = () => {
    const host = column.current;
    if (!host || !pageCount) return;
    const pages = [...host.querySelectorAll<HTMLCanvasElement>("canvas[data-page]")];
    const top = host.getBoundingClientRect().top;
    let current = 1;
    for (const canvas of pages) {
      if (canvas.getBoundingClientRect().top - top <= host.clientHeight / 2) {
        current = Number(canvas.getAttribute("data-page"));
      }
    }
    onPosition({ pageCount, currentPage: current });
  };

  return (
    <div
      className="okg-console__pdf"
      data-testid="explore-content-pdf"
      ref={column}
      onScroll={onScroll}
    />
  );
}
