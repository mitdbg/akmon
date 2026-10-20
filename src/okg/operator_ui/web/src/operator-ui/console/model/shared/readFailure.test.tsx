// Which 404 the console is looking at, and what it may say about it.
//
// A 404 reaches this console from three places, and only two of them say
// anything about the read API:
//
//   1. a route the API SERVES, answered with that route's own body, carrying
//      a `reason` -- a fact about the deployment's history;
//   2. the app's own catch-all, `{"detail": "unknown viz API route"}` raised
//      at src/okg/viz/app.py -- a fact about the API build;
//   3. anything else: a proxy's HTML error page, a load balancer, a static
//      host, a stale CDN entry -- a fact about neither.
//
// The classifier tested only for the ABSENCE of a reason, which (2) and (3)
// both pass, so every foreign 404 was reported to the operator as "this
// console build is newer than this deployment's read API, which does not
// serve <route>". That is a claim about the API's routing table drawn from a
// response that never mentioned it.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  ReadFailure, notFoundKind, routeNotServedSentence, unrecognisedNotFoundSentence,
} from "@operator-ui/console/components/ReadFailure";
import { namedReadFailure } from "@operator-ui/console/model/shared/readFailure";
import { ApiRequestError } from "@operator-ui/shared/api/apiClient";

const ROUTE = "/viz/api/console/attempts/detail";

function failure(status: number, responseText: string | undefined) {
  return namedReadFailure(
    ROUTE, new ApiRequestError({ route: ROUTE, status, responseText }),
  );
}

/** The app's own catch-all, verbatim: FastAPI renders an HTTPException's
 *  string detail as exactly this. */
const APP_CATCH_ALL = JSON.stringify({ detail: "unknown viz API route" });

/** A served route's own 404, which carries the reason. */
const SERVED = JSON.stringify({
  detail: { reason: "attempt_not_found", error: "No such attempt." },
});

/** What sits in front of a read API when one is misconfigured. */
const PROXY_HTML = "<html><head><title>404 Not Found</title></head>"
  + "<body><center><h1>404 Not Found</h1></center><hr><center>nginx</center>"
  + "</body></html>";

/** JSON, but not this app's: no `detail`, no `reason`. */
const FOREIGN_JSON = JSON.stringify({ error: "not_found", service: "edge-cache" });

describe("telling the three 404s apart", () => {
  it("names a served route's 404 by the reason the server sent", () => {
    const named = failure(404, SERVED);
    expect(named.reason).toBe("attempt_not_found");
    expect(notFoundKind(named)).toBe("named");
  });

  it("calls the app's own catch-all a route this API does not serve", () => {
    const caught = failure(404, APP_CATCH_ALL);
    expect(caught.reason).toBeNull();
    // The distinguishing fact: JSON with a STRING detail. The absence of a
    // reason alone is what used to decide it, and it decides nothing.
    expect(caught.appDetailBody).toBe(true);
    expect(notFoundKind(caught)).toBe("route-not-served");
  });

  it("refuses to classify a 404 whose body is not this app's", () => {
    for (const [what, body] of Object.entries({
      "a proxy's HTML": PROXY_HTML,
      "someone else's JSON": FOREIGN_JSON,
      "an empty body": undefined,
      "a bare string": '"gone"',
    })) {
      const other = failure(404, body);
      expect(other.reason, what).toBeNull();
      expect(other.appDetailBody, what).toBe(false);
      expect(notFoundKind(other), what).toBe("unrecognised");
    }
  });

  it("says nothing about 404s to a status that is not one", () => {
    expect(notFoundKind(failure(503, "{}"))).toBe("not-a-404");
    expect(notFoundKind(namedReadFailure(ROUTE, new Error("offline"))))
      .toBe("not-a-404");
  });
});

describe("what the operator is told, for each of the three", () => {
  it("claims the API does not serve the route ONLY for the app's catch-all", () => {
    const markup = renderToStaticMarkup(
      <ReadFailure
        route={ROUTE}
        what="What this attempt recorded"
        error={new ApiRequestError({ route: ROUTE, status: 404, responseText: APP_CATCH_ALL })}
      />,
    );
    expect(markup).toContain("not served by this deployment&#x27;s read API");
  });

  it("makes no such claim for a proxy's 404, and names the route instead", () => {
    const markup = renderToStaticMarkup(
      <ReadFailure
        route={ROUTE}
        what="What this attempt recorded"
        error={new ApiRequestError({ route: ROUTE, status: 404, responseText: PROXY_HTML })}
      />,
    );
    expect(markup).not.toContain("not served by this deployment");
    expect(markup).toContain("did not come from the read API&#x27;s own handlers");
    expect(markup).toContain(ROUTE);
  });

  it("invents no reason code for a 404 that carried none", () => {
    for (const body of [APP_CATCH_ALL, PROXY_HTML, FOREIGN_JSON]) {
      const markup = renderToStaticMarkup(
        <ReadFailure
          route={ROUTE}
          what="What this attempt recorded"
          error={new ApiRequestError({ route: ROUTE, status: 404, responseText: body })}
        />,
      );
      expect(markup).not.toContain("attempt_not_found");
    }
  });

  it("gives each of the two sentences the route and nothing invented", () => {
    expect(routeNotServedSentence(ROUTE)).toContain(ROUTE);
    expect(unrecognisedNotFoundSentence(ROUTE)).toContain(ROUTE);
    expect(unrecognisedNotFoundSentence(ROUTE))
      .toContain("answered 404 with a body this console does not recognise");
    // It says nothing about routes, versions or builds.
    expect(unrecognisedNotFoundSentence(ROUTE)).not.toContain("does not serve");
    expect(unrecognisedNotFoundSentence(ROUTE)).not.toContain("newer than");
  });
});
