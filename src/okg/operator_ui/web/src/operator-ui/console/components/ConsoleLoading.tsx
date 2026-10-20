// The line a console view shows while its reads are in flight.
//
// It is a component rather than a bare <p> in five places because it was a
// bare <p> in five places: outside `.okg-console`, the line landed at x=0
// against the window edge, in the browser's default type, while the page it
// replaces sits in a 960px column. On first paint of any console view that
// is the whole page, so the first thing an operator saw was an unstyled
// sentence in the corner.
//
// Inside the same column and type as the page it stands in for, so the view
// does not jump sideways when the reads land.
//
// After three seconds the line counts ("Still reading this deployment
// (7 s)"), so a slow read is told apart from a page that stopped.
import type { ReactElement } from "react";

import { ReadingLine } from "@operator-ui/console/components/readRecovery";

export function ConsoleLoading({ what }: {
  /** What is being read, in the reader's words: "this deployment", or a
   *  producer's name. */
  what: string;
}): ReactElement {
  return (
    <main className="okg-console" data-testid="console-loading">
      <ReadingLine what={what} />
    </main>
  );
}
