// Rule 13: a read that failed says so, in place, by name.
//
// It lives in its own module because both the design harness and every console
// view need it, and a shared failure component that one of them forgot to use
// is how a missing read becomes a healthy-looking blank page. It imports
// nothing under @operator-ui/design-harness: it is shipping code.
//
// There are two failures with two different readers, and one behaviour:
//
//   - A read the FIXTURE does not define. Only the harness can produce it,
//     and its next step really is to add the read and regenerate. That advice
//     is kept here, and only here.
//   - Anything a live console can hit. `ReadFailure` beside this file says
//     what a live operator needs -- which route, what the server said, its
//     reason code, and what to check next -- so this delegates to it rather
//     than keeping a second, poorer version of the same words. That is why no
//     live failure can mention a `scripts/operator-ui-design/` file: the branch that names one is
//     unreachable unless the error is a FixtureReadMissing.
import type { ReactElement } from "react";

import { FixtureReadMissing } from "@operator-ui/console/api/readFailures";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";

export function LoudFailure({
  error, route, what, onReadAgain, retrying,
}: {
  error: unknown;
  /** The route that failed, when the caller knows it. */
  route?: string;
  /** What the page cannot show, in the reader's words. */
  what?: string;
  /** Re-issue the failed read; see ReadFailure. */
  onReadAgain?: (() => void) | null;
  retrying?: boolean;
}): ReactElement {
  if (!(error instanceof FixtureReadMissing)) {
    return (
      <ReadFailure
        route={route ?? "this view's read"}
        error={error}
        what={what ?? "This page"}
        onReadAgain={onReadAgain}
        retrying={retrying}
      />
    );
  }
  return (
    <section className="okg-design-failure" role="alert" data-testid="design-read-failed">
      <h2>A read this view needs is not in the fixture</h2>
      <p>{error.message}</p>
      <p className="okg-design-failure__next">
        Nothing was substituted for it. Add the read in
        {" "}<code>scripts/operator-ui-design/lib/deployments.mjs</code> and
        regenerate the fixtures.
      </p>
    </section>
  );
}

/**
 * A read that failed and took the WHOLE view with it.
 *
 * It renders where the view's own content would be: inside the console's
 * column, in the console's type. Returned bare -- which Home and Explore
 * did -- the block lands hard against the left edge of the window, outside
 * every rule the page is laid out by, and reads as a page that broke rather
 * than as a page saying what broke. Sources and Updates already wrapped
 * theirs; this is that wrapper, named once so the next view cannot forget it.
 *
 * `route` is not optional here. Without one the surface prints "this view's
 * read" in a mono face, and an operator told to check a read cannot tell
 * which read to check.
 */
export function ConsolePageFailure({
  error, route, what, testId = "console-page-failed", onReadAgain, retrying,
}: {
  error: unknown;
  route: string;
  what: string;
  testId?: string;
  onReadAgain?: (() => void) | null;
  retrying?: boolean;
}): ReactElement {
  return (
    <main className="okg-console" data-testid={testId}>
      <LoudFailure error={error} route={route} what={what} onReadAgain={onReadAgain} retrying={retrying} />
    </main>
  );
}
