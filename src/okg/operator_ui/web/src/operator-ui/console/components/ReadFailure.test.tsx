// What the failure surface may claim about a failed read.
//
// Three different things fail, and they need three different sentences. A
// read that never reached the server is the only one whose next step is
// "check that the read API is running"; printing that line for an answer the
// server DID give and this client refused sends an operator to look at a
// process that is up and serving. Partially addresses #2153.
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { ConsolePageFailure } from "@operator-ui/console/components/LoudFailure";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";
import { deploymentName, versionsPlaceholder } from "@operator-ui/console/views/TopBar";
import { SearchResponseError } from "@operator-ui/console/api/searchData";
import { ApiRequestError } from "@operator-ui/shared/api/apiClient";

const ROUTE = "/viz/api/console/records/search";
const render = (error: unknown) => renderToStaticMarkup(
  <ReadFailure route={ROUTE} error={error} what="These results" />,
);

const TRANSPORT = "The read did not reach the server.";
const NEXT_CLASS = "okg-console__read-failed-next";

describe("ReadFailure", () => {
  it("says a read never reached the server only when no response came back", () => {
    const markup = render(new ApiRequestError({ route: ROUTE }));
    expect(markup).toContain(TRANSPORT);
    expect(markup).toContain("Check that the read API is running.");
  });

  it("reports an answer this client refused as a refusal, not as an unreachable server", () => {
    // parseExploreSearch throws this when the read carries a zero-hit code
    // the build does not know. The server answered; the console refused the
    // answer. The transport sentence would be the opposite of what happened.
    const markup = render(new SearchResponseError('search zero-hit reason "ranked_out"'));
    expect(markup).toContain("The server returned an unreadable search zero-hit reason");
    expect(markup).toContain("ranked_out");
    expect(markup).not.toContain(TRANSPORT);
    expect(markup).not.toContain("Check that the read API is running.");
    // The step it does offer is the one the refusal supports: the console and
    // the read API disagree about the payload.
    expect(markup).toContain("this console refused its answer");
  });

  it("offers no next step for a failure it cannot classify", () => {
    // Neither a response nor a refusal of one. The detail is still printed;
    // an invented recovery step is not.
    const markup = render(new Error("boom"));
    expect(markup).toContain("boom");
    expect(markup).not.toContain(TRANSPORT);
    expect(markup).not.toContain(NEXT_CLASS);
  });

  it("keeps the server's own reason, status and next check when it gave them", () => {
    const markup = render(new ApiRequestError({
      route: ROUTE,
      status: 503,
      responseText: JSON.stringify({
        reason: "version_not_published",
        error: "No such published generation.",
        next_check: "choose a published version",
      }),
    }));
    expect(markup).toContain("503");
    expect(markup).toContain("version_not_published");
    expect(markup).toContain("No such published generation.");
    expect(markup).toContain("choose a published version");
    expect(markup).not.toContain(TRANSPORT);
  });
});

describe("a read that failed and took the whole view with it", () => {
  const boom = new ApiRequestError({ route: "/viz/api/console/versions", status: 503 });

  it("renders inside the console's own column, not bare against the window", () => {
    // Rendered bare, the block starts at the left edge of the window in the
    // page's own type, with no column and no way back: it reads as a page
    // that broke rather than as a page saying what broke.
    const markup = renderToStaticMarkup(
      <ConsolePageFailure error={boom} route="/viz/api/console/versions" what="This deployment" />,
    );
    expect(markup).toContain('<main class="okg-console"');
    expect(markup.indexOf('class="okg-console"')).toBeLessThan(markup.indexOf("okg-console__read-failed"));
  });

  it("names the route it read, and never the placeholder", () => {
    const markup = renderToStaticMarkup(
      <ConsolePageFailure error={boom} route="/viz/api/console/versions" what="This deployment" />,
    );
    expect(markup).toContain("/viz/api/console/versions");
    expect(markup).not.toContain("this view's read");
  });

  it("tells a picker still loading apart from one whose read refused", () => {
    // The shell's two pickers used to share one Promise.all, so a refusal
    // from either left BOTH reading "not read yet" -- including the one whose
    // read had answered. They are settled separately now, and a picker with
    // no value says which of the two states it is in. Neither is ever filled
    // with a value the read did not give. The separation itself is measured
    // live, in ui/live-capture.md; this holds the words.
    // Re-aimed onto the bar's own two helpers, which is where this rule
    // lives after the Sources branch re-did the split: each takes ONLY its
    // own read's failure, so neither picker can be emptied by the other's.
    expect(deploymentName(null)).toBe("not read yet");
    expect(deploymentName(boom)).toBe("not read");
    expect(versionsPlaceholder(null, null)).toBe("not read yet");
    expect(versionsPlaceholder({ payload: { versions: [], latest: null } }, null))
      .toBe("none published");
    expect(versionsPlaceholder(null, boom)).toBe("not read");
    // The answered read still shows what it found while the OTHER refused.
    expect(versionsPlaceholder({ payload: { versions: [], latest: null } }, null))
      .not.toBe(deploymentName(boom));
  });

  it("has the styling its classes ask for, in a stylesheet the console loads", () => {
    // The surface named four classes and the stylesheets defined none of
    // them, so every failed read on this console rendered as unstyled
    // paragraphs. A class the markup uses and no rule matches is the defect;
    // this asserts the rules exist rather than trusting a screenshot.
    const css = readFileSync(
      resolve(process.cwd(), "src/okg/operator_ui/web/src/operator-ui/console/styles/shell.css"),
      "utf8",
    );
    for (const rule of [
      ".okg-console__read-failed",
      ".okg-console__read-failed-headline",
      ".okg-console__read-failed-detail",
      ".okg-console__read-failed-next",
    ]) {
      expect(css, `${rule} is used by the failure surface and styled nowhere`).toContain(rule);
    }
  });
});
