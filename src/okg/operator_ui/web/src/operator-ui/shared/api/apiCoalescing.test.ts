// One load, one request.
//
// The Sources page fetched `/console/producers` twice on every load
// (requests 107 and 113 in the okg-workspace capture). The cause is not two
// callers: it is React StrictMode, which double-invokes an effect in
// development. The effect's `cancelled` flag suppressed the second STATE
// UPDATE while the second REQUEST still went out -- so on okg-workspace the
// page paid a second six-second read for a payload already in flight.
//
// The fix is in the client, not in the one component: two identical GETs
// issued before the first answers collapse into one request. These tests are
// the guard, and they count REQUESTS, because counting renders would not have
// caught the defect in the first place.
import { describe, expect, it } from "vitest";

import { createApiClient } from "@operator-ui/shared/api/apiClient";
import { loadConsoleSources } from "@operator-ui/console/api/sources";

const ORIGIN = "http://127.0.0.1:8799";

/** A fetch that records every URL it is asked for and answers after a tick. */
function countingFetch(body: unknown = { payload: {} }) {
  const urls: string[] = [];
  let release: (() => void) | null = null;
  const held = new Promise<void>((resolve) => { release = resolve; });
  const impl = async (input: RequestInfo | URL): Promise<Response> => {
    urls.push(String(input));
    await held;
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };
  return { impl, urls, release: () => release?.() };
}

describe("the console client sends one request per question in flight", () => {
  it("collapses two identical reads issued before the first answers", async () => {
    const fetcher = countingFetch();
    const api = createApiClient({ baseUrl: ORIGIN, fetchImpl: fetcher.impl });

    const first = api.get("/viz/api/console/producers");
    const second = api.get("/viz/api/console/producers");
    fetcher.release();
    await Promise.all([first, second]);

    expect(fetcher.urls).toHaveLength(1);
  });

  it("is coalescing, not caching: a read after the first settles is sent", async () => {
    const fetcher = countingFetch();
    const api = createApiClient({ baseUrl: ORIGIN, fetchImpl: fetcher.impl });

    fetcher.release();
    await api.get("/viz/api/console/producers");
    await api.get("/viz/api/console/producers");

    // Two requests, deliberately. Holding the answer would serve a stale
    // payload to a page that asked again, which is a different defect from
    // the one being fixed.
    expect(fetcher.urls).toHaveLength(2);
  });

  it("keeps two different questions apart", async () => {
    const fetcher = countingFetch();
    const api = createApiClient({ baseUrl: ORIGIN, fetchImpl: fetcher.impl });

    const list = api.get("/viz/api/console/producers");
    const detail = api.get("/viz/api/console/producers", { name: "git" });
    fetcher.release();
    await Promise.all([list, detail]);

    expect(fetcher.urls).toHaveLength(2);
  });

  it("keeps a refresh apart from a plain read", async () => {
    const fetcher = countingFetch();
    const api = createApiClient({ baseUrl: ORIGIN, fetchImpl: fetcher.impl });

    const plain = api.get("/viz/api/console/producers");
    const refreshed = api.get("/viz/api/console/producers", {}, { refresh: true });
    fetcher.release();
    await Promise.all([plain, refreshed]);

    // A refresh bypasses the server's cache and re-resolves the latest
    // generation. Answering it from a plain read already in flight would
    // serve the operator exactly the payload they asked to go behind.
    expect(fetcher.urls).toHaveLength(2);
  });

  it("never coalesces a request that carries an abort signal", async () => {
    const fetcher = countingFetch();
    const api = createApiClient({ baseUrl: ORIGIN, fetchImpl: fetcher.impl });
    const controller = new AbortController();

    const bare = api.get("/viz/api/console/producers");
    const signalled = api.get(
      "/viz/api/console/producers", {}, { signal: controller.signal },
    );
    fetcher.release();
    await Promise.all([bare, signalled]);

    // Sharing one promise between them would let either caller's abort reject
    // the other's read: a duplicate request traded for a wrong answer.
    expect(fetcher.urls).toHaveLength(2);
  });

  it("loads the Sources page's four reads once, not twice", async () => {
    const fetcher = countingFetch({ payload: { producers: [], now: "2026-09-22T00:00:00Z" } });
    const api = createApiClient({ baseUrl: ORIGIN, fetchImpl: fetcher.impl });

    // What StrictMode does to the page's load effect, exactly.
    const first = loadConsoleSources(api);
    const second = loadConsoleSources(api);
    fetcher.release();
    await Promise.all([first, second]);

    const producers = fetcher.urls.filter(
      (url) => url.includes("/console/producers"),
    );
    expect(producers).toHaveLength(1);
    // And the same for the other three reads the page issues.
    expect(fetcher.urls).toHaveLength(4);
  });
});
