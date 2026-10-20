// @vitest-environment jsdom
// The failure shapes, the counting line, and the wording the 2026-09-23 audit
// asked for. PACT console-read-failure-recovery.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ReadingLine } from "@operator-ui/console/components/readRecovery";
import { CopyTextButton } from "@operator-ui/console/components/CopyControls";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";
import { SEARCHABLE_LINE, buildListing } from "@operator-ui/console/model/explore/landing";
import { MAX_QUERY_CHARS, overlongSentence, parseExploreParams } from "@operator-ui/console/model/explore/params";
import { QUERY_ECHO_CHARS, echoQuery } from "@operator-ui/console/model/explore/search";
import { namedReadFailure, validationReasons } from "@operator-ui/console/model/shared/readFailure";
import { consoleLink } from "@operator-ui/routes/console/ConsoleRoute";
import { ApiRequestError, createApiClient, withReadTimeout } from "@operator-ui/shared/api/apiClient";

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const HEALTH = "/viz/api/console/health";

function answering(body: string, contentType: string, status = 200) {
  return createApiClient({
    mountBase: "/",
    fetchImpl: vi.fn(async () => new Response(body, { status, headers: { "content-type": contentType } })),
  });
}

async function failureOf(promise: Promise<unknown>): Promise<ApiRequestError> {
  try {
    await promise;
  } catch (error) {
    if (error instanceof ApiRequestError) return error;
    throw error;
  }
  throw new Error("the read answered");
}

describe("the client names the failures it can recognise", () => {
  it("a body that says JSON and is not: 'The reply was not data', naming the route", async () => {
    const error = await failureOf(answering('{"payload": "unterminated', "application/json").get(HEALTH));
    expect(error.shape).toBe("not_data");
    const named = namedReadFailure("/viz/api/console/*", error);
    expect(named.route).toBe(HEALTH);
    expect(named.detail).toBe("The reply was not data.");
  });

  it("an HTML page with a 200: a web page came back instead of data", async () => {
    const error = await failureOf(answering("<!doctype html><title>Sign in</title>", "text/html").get(HEALTH));
    const named = namedReadFailure(HEALTH, error);
    expect(named.detail).toBe("A web page came back instead of data (a sign-in page or a proxy?).");
    expect(named.status).toBeNull();
  });

  it("a FastAPI 422 prints its validation reasons", async () => {
    const body = JSON.stringify({ detail: [{ loc: ["query", "q"], msg: "String should have at most 512 characters", type: "string_too_long" }] });
    expect(validationReasons(body)).toBe("q: String should have at most 512 characters");
    const error = await failureOf(answering(body, "application/json", 422).get(HEALTH));
    const named = namedReadFailure(HEALTH, error);
    expect(named.detail).toBe("The read API refused the request: q: String should have at most 512 characters.");
  });

  it("a read past its bound fails as 'timed out after N s' and aborts the request", async () => {
    vi.useFakeTimers();
    let aborted = false;
    const fetchImpl = vi.fn((_url: RequestInfo | URL, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => { aborted = true; reject(init.signal?.reason); });
    }));
    const api = withReadTimeout(createApiClient({ mountBase: "/", fetchImpl }), 20_000);
    const pending = failureOf(api.get(HEALTH));
    await vi.advanceTimersByTimeAsync(20_000);
    const error = await pending;
    vi.useRealTimers();
    expect(aborted).toBe(true);
    expect(error.shape).toBe("timeout");
    expect(namedReadFailure(HEALTH, error).detail).toBe("The read timed out after 20 s.");
  });

  it("the headline is the failed read's own subject, not the page's", () => {
    const error = new ApiRequestError({ route: "/viz/api/console/attempts", status: 500, responseText: "" });
    const html = renderToStaticMarkup(
      <ReadFailure route="/viz/api/console/producers" error={error} what="This deployment's producers" />,
    );
    expect(html).toContain("The update attempts by day could not be read.");
    expect(html).not.toContain("producers could not be read");
  });
});

let host: HTMLDivElement;
let root: Root;
beforeEach(() => {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
});
afterEach(() => {
  act(() => root.unmount());
  host.remove();
  vi.useRealTimers();
});

describe("a slow read counts, in neutral words", () => {
  it("reads 'Reading', then after 3 s 'Still reading ... (N s)'", async () => {
    vi.useFakeTimers();
    await act(async () => { root.render(<ReadingLine what="this deployment" />); });
    expect(host.textContent).toBe("Reading this deployment…");
    await act(async () => { vi.advanceTimersByTime(3_000); });
    expect(host.textContent).toBe("Still reading this deployment (3 s)");
    await act(async () => { vi.advanceTimersByTime(4_000); });
    expect(host.textContent).toBe("Still reading this deployment (7 s)");
  });
});

describe("'copied' is announced", () => {
  it("through a polite live region", async () => {
    vi.stubGlobal("navigator", { clipboard: { writeText: () => Promise.resolve() } });
    await act(async () => { root.render(<CopyTextButton text="x" what="Home as text" />); });
    const region = host.querySelector('[data-testid="copy-announcement"]');
    expect(region?.getAttribute("aria-live")).toBe("polite");
    expect(region?.textContent).toBe("");
    await act(async () => { host.querySelector("button")?.click(); });
    await act(async () => { await Promise.resolve(); });
    expect(region?.textContent).toBe("Copied Home as text");
    vi.unstubAllGlobals();
  });
});

describe("Explore's long queries and wording", () => {
  it("echoes at most 200 characters of a query, with an ellipsis", () => {
    const long = "x".repeat(10_000);
    expect(echoQuery(long)).toHaveLength(QUERY_ECHO_CHARS + 1);
    expect(echoQuery(long).endsWith("…")).toBe(true);
    expect(echoQuery("muon")).toBe("muon");
  });

  it("does not send a query over 512 characters, and says why", () => {
    const params = parseExploreParams({ q: "y".repeat(MAX_QUERY_CHARS + 1) });
    expect(params.q).toBe("");
    expect(params.overlong).toBe(MAX_QUERY_CHARS + 1);
    expect(overlongSentence(10_000)).toBe(
      "This search is 10,000 characters long; searches take at most 512. Shorten it and press Enter.",
    );
    expect(parseExploreParams({ q: "y".repeat(MAX_QUERY_CHARS) }).overlong).toBeNull();
  });

  it("tells the reader to press Enter", () => {
    expect(SEARCHABLE_LINE.startsWith("Type and press Enter to search")).toBe(true);
  });

  it("names a type the version does not hold, instead of 'no record is visible'", () => {
    const listing = buildListing(
      { type: "no_such_type", records: [], nextOffset: null, shortPage: false },
      [{ type: "document", count: 233 } as never],
      parseExploreParams({ type: "no_such_type" }),
      consoleLink,
    );
    expect(listing.emptySentence).toBe(
      "There is no record type called no_such_type in this version; the types it holds are listed above.",
    );
    const real = buildListing(
      { type: "document", records: [], nextOffset: null, shortPage: false },
      [{ type: "document", count: 233 } as never],
      parseExploreParams({ type: "document" }),
      consoleLink,
    );
    expect(real.emptySentence).toBe("No document record is visible in this version.");
  });
});

describe("a narrower read of the same route keeps its own subject", () => {
  it("names the producer for a producer's detail read, and 'the next page' for a cursor", () => {
    const detail = new ApiRequestError({ route: "/viz/api/console/producers", params: { name: "jira", run_limit: 20 }, status: 500, responseText: "" });
    expect(renderToStaticMarkup(<ReadFailure route="/viz/api/console/producers" error={detail} what="x" />))
      .toContain("The read for jira could not be read.");
    const next = new ApiRequestError({ route: "/viz/api/console/generations", params: { cursor: "c1" }, status: 500, responseText: "" });
    expect(renderToStaticMarkup(<ReadFailure route="/viz/api/console/generations" error={next} what="x" />))
      .toContain("The next page of update attempts could not be read.");
  });
});
