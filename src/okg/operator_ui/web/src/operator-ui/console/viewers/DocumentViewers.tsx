// The four document viewers, behind one lazy entry point.
//
// Only the design harness mounts this. It is the sole module that imports
// PdfViewer, MarkdownViewer, CodeViewer and HtmlFrame, so the import
// boundary the live console holds is one edge: whether a build reaches
// this file. The harness's `explore_js` and `pdf_worker` budgets measure
// the viewers through it.
import type { DocumentViewerProps } from "@operator-ui/console/components/documentViewers";
import { Suspense, lazy, type ReactElement } from "react";

const PdfViewer = lazy(() => import("@operator-ui/console/viewers/PdfViewer"));
const MarkdownViewer = lazy(() => import("@operator-ui/console/viewers/MarkdownViewer"));
const CodeViewer = lazy(() => import("@operator-ui/console/viewers/CodeViewer"));
const HtmlFrame = lazy(() => import("@operator-ui/console/viewers/HtmlFrame"));

export default function DocumentViewers({
  kind, url, language, fileName, onState,
}: DocumentViewerProps): ReactElement {
  return (
    <Suspense fallback={<p className="okg-console__fact">Loading the viewer…</p>}>
      {kind === "pdf" ? <PdfViewer url={url} onState={onState} onPosition={() => undefined} />
        : kind === "markdown" ? <MarkdownViewer url={url} onState={onState} />
          : kind === "code" ? <CodeViewer url={url} language={language} onState={onState} />
            : <HtmlFrame url={url} title={`${fileName}, rendered without scripts`} onState={onState} />}
    </Suspense>
  );
}
