// A failed read must say what failed, and must not give a live operator
// advice meant for the design harness.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { FixtureReadMissing } from "@operator-ui/console/api/readFailures";
import { ApiRequestError } from "@operator-ui/shared/api/apiClient";

const render = (error: unknown) => renderToStaticMarkup(<LoudFailure error={error} />);

describe("LoudFailure", () => {
  it("names the route, the status and a next check when a live read fails", () => {
    // Re-aimed after the Updates slice landed ReadFailure: a live failure is
    // now that component's words, so the guard holds its vocabulary. The
    // subject is unchanged -- the reader must be told which route failed and
    // what came back.
    const markup = render(new ApiRequestError({
      route: "/viz/api/console/producers", status: 503, responseText: "no",
    }));
    expect(markup).toContain("could not be read");
    expect(markup).toContain("/viz/api/console/producers");
    expect(markup).toContain("503");
    expect(markup).toContain("Check that the generation published");
  });

  it("carries the server's own reason and next check when it gave them", () => {
    const markup = render(new ApiRequestError({
      route: "/viz/api/console/producers",
      status: 503,
      responseText: JSON.stringify({
        reason: "producers_composer_failed",
        error: "The producers read is unavailable.",
        next_check: "check the console producers backend log and retry the read",
      }),
    }));
    expect(markup).toContain("producers_composer_failed");
    expect(markup).toContain("The producers read is unavailable.");
    expect(markup).toContain("check the console producers backend log");
  });

  it("does not tell a live operator to edit the fixture generator", () => {
    // The recovery line used to be the fixture advice unconditionally, so a
    // live console that could not reach a route pointed its operator at a
    // file no deployment has.
    for (const error of [
      new ApiRequestError({ route: "/viz/api/console/producers", status: 503 }),
      new Error("No producer named no-such-producer is among the 4 the reads list."),
    ]) {
      const markup = render(error);
      expect(markup).not.toContain("deployments.mjs");
      expect(markup).not.toContain("regenerate the fixtures");
    }
  });

  it("keeps the fixture advice for a read the fixture really is missing", () => {
    const markup = render(new FixtureReadMissing("archiv3-small", "/viz/api/x", []));
    expect(markup).toContain("not in the fixture");
    expect(markup).toContain("deployments.mjs");
    expect(markup).toContain("regenerate the fixtures");
  });

  it("says a read never reached the server when it carries no status", () => {
    const markup = render(new ApiRequestError({ route: "/viz/api/console/producers" }));
    expect(markup).toContain("did not reach the server");
  });
});
