// The slot the four document viewers fill, and nothing that imports them.
//
// PDF, HTML, Markdown and code viewers are NOT part of the live console.
// The only byte store the record reads serve is `okg.visual_crop_blobs`,
// whose media types are image/png, image/jpeg, image/gif and image/webp
// (#2121), so no route serves bytes any of those four could draw, and
// carrying pdf.js, react-markdown and shiki into the console bundle would
// pay for a viewer that can never fire.
//
// They stay on disk for the design harness, which mounts them through this
// context. A module reachable from `routes/console/ConsoleRoute.tsx` may
// import THIS file; importing a viewer is held by
// tests/operator_ui/test_console_bundle_has_no_document_viewers.py.
import { createContext, useContext, type ReactElement } from "react";
import type { ViewerState } from "@operator-ui/console/viewers/viewerState";

/** Which viewer a non-image artifact would need. The live console renders
 *  none of them and says so by name. */
export type DocumentViewerKind = "pdf" | "markdown" | "code" | "html";

export interface DocumentViewerProps {
  kind: DocumentViewerKind;
  url: string;
  language: string | null;
  /** The file's name, for the HTML frame's accessible title. */
  fileName: string;
  onState: (state: ViewerState) => void;
}

/** Null in every build but the design harness's. */
export const DocumentViewersContext =
  createContext<((props: DocumentViewerProps) => ReactElement) | null>(null);

export function useDocumentViewers(): ((props: DocumentViewerProps) => ReactElement) | null {
  return useContext(DocumentViewersContext);
}

/** What the Content tab says in a build that mounts no document viewer.
 *  Named, not blank: the reader is told which condition fired. */
export function noDocumentViewerSentence(kind: DocumentViewerKind, mediaType: string): string {
  return `This console does not render ${mediaType} in the page: the only bytes these `
    + `reads serve are figure crops, which are images, so no ${kind} viewer ships with it. `
    + "Open the original or download it from the toolbar.";
}
