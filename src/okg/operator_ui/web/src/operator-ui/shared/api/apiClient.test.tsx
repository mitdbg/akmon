import { describe, expect, it, vi } from "vitest";
import {
  ApiRequestError,
  buildApiUrl,
  createApiClient,
  withAbortSignalClient,
  withRefreshClient,
} from "@operator-ui/shared/api/apiClient";

describe("operator API client", () => {
  it("builds /viz/api URLs and encodes params", () => {
    expect(
      buildApiUrl("/viz/api/console/health", { focus: "cms ops", limit: 3 }),
    ).toBe("/viz/api/console/health?focus=cms+ops&limit=3");
  });

  it("rejects non API routes", () => {
    expect(() => buildApiUrl("/api/health" as "/viz/api/health")).toThrow(
      ApiRequestError,
    );
  });

  it("only issues GET requests", async () => {
    const fetchImpl = vi.fn(async () =>
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const api = createApiClient({ fetchImpl });

    await expect(api.get("/viz/api/health")).resolves.toEqual({ ok: true });
    expect(fetchImpl).toHaveBeenCalledWith("/viz/api/health", {
      method: "GET",
      headers: { Accept: "application/json" },
    });
  });

  it("refresh client adds refresh=1 + Cache-Control and bypasses the browser cache", async () => {
    const fetchImpl = vi.fn(async () =>
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const api = withRefreshClient(createApiClient({ fetchImpl }));

    await expect(api.get("/viz/api/console/health")).resolves.toEqual({ ok: true });
    expect(fetchImpl).toHaveBeenCalledWith("/viz/api/console/health?refresh=1", {
      method: "GET",
      cache: "reload",
      headers: { Accept: "application/json", "Cache-Control": "no-cache" },
    });
  });

  it("non-refresh GET keeps the minimal init (no cache override)", async () => {
    const fetchImpl = vi.fn(async () =>
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const api = createApiClient({ fetchImpl });
    await api.get("/viz/api/console/health");
    expect(fetchImpl).toHaveBeenCalledWith("/viz/api/console/health", {
      method: "GET",
      headers: { Accept: "application/json" },
    });
  });

  it("propagates abort signals through normal and refresh clients", async () => {
    const fetchImpl = vi.fn((_url: RequestInfo | URL, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason));
      }));
    const controller = new AbortController();
    const api = withAbortSignalClient(
      withRefreshClient(createApiClient({ fetchImpl })),
      controller.signal,
    );

    const request = api.get("/viz/api/console/health");
    controller.abort(new DOMException("superseded", "AbortError"));

    await expect(request).rejects.toMatchObject({
      route: "/viz/api/console/health",
      params: { refresh: 1 },
      responseText: "superseded",
    });
    expect(fetchImpl.mock.calls[0]?.[1]?.signal).toBe(controller.signal);
  });

  it("reports failed GETs with route and params", async () => {
    const fetchImpl = vi.fn(async () => new Response("missing", { status: 503 }));
    const api = createApiClient({ fetchImpl });

    await expect(api.get("/viz/api/console/health", { okg: "cms" })).rejects.toMatchObject({
      route: "/viz/api/console/health",
      params: { okg: "cms" },
      status: 503,
      responseText: "missing",
    });
  });

  it("reports refresh failures with the actual refresh request params", async () => {
    const fetchImpl = vi.fn(async () => new Response("missing", { status: 503 }));
    const api = withRefreshClient(createApiClient({ fetchImpl }));

    await expect(
      api.get("/viz/api/console/records/list", { type: "cmssw_release" }),
    ).rejects.toMatchObject({
      route: "/viz/api/console/records/list",
      params: { type: "cmssw_release", refresh: 1 },
      status: 503,
      responseText: "missing",
    });
  });

  it("rejects HTML fallback responses as API failures", async () => {
    const fetchImpl = vi.fn(async () =>
      new Response("<!doctype html><title>OKG Operator UI</title>", {
        status: 200,
        headers: { "content-type": "text/html" },
      }),
    );
    const api = createApiClient({ fetchImpl });

    await expect(api.get("/viz/api/console/health")).rejects.toMatchObject({
      route: "/viz/api/console/health",
      status: 200,
      responseText: expect.stringContaining("<!doctype html>"),
    });
  });

  it("rejects JSON failure envelopes even when the HTTP response is 200", async () => {
    const fetchImpl = vi.fn(async () =>
      new Response(
        JSON.stringify({
          ok: false,
          status: 502,
          error: "operator UI dev server has no OKG API target",
        }),
        {
          status: 200,
          headers: { "content-type": "application/json" },
        },
      ),
    );
    const api = createApiClient({ fetchImpl });

    await expect(api.get("/viz/api/console/health")).rejects.toMatchObject({
      route: "/viz/api/console/health",
      status: 502,
      responseText: expect.stringContaining("no OKG API target"),
    });
  });
});
