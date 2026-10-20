import {
  joinMountPath,
  MountBaseError,
  normalizeMountBase,
  OPERATOR_UI_BASE_ENV,
} from "@operator-ui/shared/api/mountBase";

export interface ApiGetOptions {
  /**
   * Force a fresh read: bypass the browser HTTP cache AND tell the backend to
   * bypass its response cache + re-resolve the latest published generation
   * (the `refresh=1` query param). Used by the operator Refresh control.
   */
  refresh?: boolean;
  /** Abort superseded route/panel reads so only the latest request may finish. */
  signal?: AbortSignal;
  /**
   * Give up on the read after this many milliseconds and say so. Absent, the
   * read waits as long as the browser does. The console sets it
   * (`withReadTimeout`); the older Home and Health surfaces do not, because
   * their readiness read can run past any bound this client would pick.
   */
  timeoutMs?: number;
}

/**
 * Which of the failures the client itself recognised, where it recognised
 * one. Each is a different thing to tell a reader:
 *
 *  - `timeout`: nothing came back inside the bound the caller set.
 *  - `not_data`: the reply said it was JSON and did not parse as JSON.
 *  - `web_page`: an HTML page came back where data was asked for -- a
 *    sign-in page, a proxy's error page, or the app's own index.
 */
export type ApiFailureShape = "timeout" | "not_data" | "web_page";

export interface ApiClient {
  get<T>(
    route: `/viz/api/${string}`,
    params?: Record<string, string | number | boolean | null | undefined>,
    opts?: ApiGetOptions,
  ): Promise<T>;
}

export interface ApiClientOptions {
  baseUrl?: string;
  fetchImpl?: typeof fetch;
  /**
   * The console's mount, e.g. `"/console/"`. Defaults to the mount the bundle
   * was BUILT with (`import.meta.env.BASE_URL`). Pass it explicitly only from
   * tests that need to exercise a mount other than the one under test.
   */
  mountBase?: string;
}

export class ApiRequestError extends Error {
  readonly route: string;
  readonly params: Record<string, string | number | boolean | null | undefined>;
  readonly status?: number;
  readonly responseText?: string;
  readonly shape?: ApiFailureShape;
  /** The bound a `timeout` failure gave up at. */
  readonly timeoutMs?: number;

  constructor({
    route,
    params = {},
    status,
    responseText,
    shape,
    timeoutMs,
  }: {
    route: string;
    params?: Record<string, string | number | boolean | null | undefined>;
    status?: number;
    responseText?: string;
    shape?: ApiFailureShape;
    timeoutMs?: number;
  }) {
    super(
      status
        ? `Read failed: ${route} (${status})`
        : `Read failed: ${route}`,
    );
    this.name = "ApiRequestError";
    this.route = route;
    this.params = params;
    this.status = status;
    this.responseText = responseText;
    this.shape = shape;
    this.timeoutMs = timeoutMs;
  }
}

/** How long a console read may take before it is named as timed out. */
export const READ_TIMEOUT_MS = 20_000;

function isFailureEnvelope(payload: unknown): payload is {
  ok: false;
  status?: number;
  error?: string;
  next_check?: string;
} {
  return Boolean(
    payload
      && typeof payload === "object"
      && "ok" in payload
      && (payload as { ok?: unknown }).ok === false,
  );
}

/**
 * The mount this bundle was BUILT for, injected by Vite from `base`.
 *
 * Read lazily (never at module load) so a broken build input surfaces as a
 * failed read with a legible message rather than a bare import-time crash.
 * There is no `?? "/"` here on purpose: defaulting a missing mount to the
 * origin root is the silent failure this module exists to remove — a prefixed
 * console would send every read to whatever owns `/`, which under the intended
 * one-hostname topology is the chat site, and the reads would SUCCEED against
 * the wrong application.
 */
function builtMountBase(): string {
  const injected = (import.meta as { env?: { BASE_URL?: unknown } }).env?.BASE_URL;
  if (typeof injected !== "string") {
    throw new MountBaseError(
      "the operator console does not know where it is mounted: "
        + "import.meta.env.BASE_URL was not injected by the build. Refusing to "
        + `assume the origin root. Build the console through `
        + `vite.operator-ui.config.ts (set ${OPERATOR_UI_BASE_ENV} for a prefixed mount).`,
      injected,
    );
  }
  return normalizeMountBase(injected);
}

export function buildApiUrl(
  route: `/viz/api/${string}`,
  params: Record<string, string | number | boolean | null | undefined> = {},
  baseUrl?: string,
  mountBase?: string,
) {
  // Route-shape guard: the console reads only its own API surface. Checked
  // BEFORE the mount is applied, so a mounted console cannot be talked into
  // fetching an arbitrary path.
  if (!route.startsWith("/viz/api/")) {
    throw new ApiRequestError({ route, params });
  }

  // Address the API relative to the console's own mount. At the root mount
  // this is the identity, so today's request paths do not move.
  const mountedRoute = joinMountPath(
    mountBase === undefined ? builtMountBase() : mountBase,
    route,
  );

  const url = baseUrl
    ? new URL(mountedRoute, baseUrl)
    : new URL(mountedRoute, globalThis.location?.origin ?? "http://127.0.0.1");

  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  }

  return baseUrl ? url.href : `${url.pathname}${url.search}`;
}

export function createApiClient({
  baseUrl,
  fetchImpl = fetch,
  mountBase,
}: ApiClientOptions = {}): ApiClient {
  // Requests this client has sent and not yet settled, by the URL they ask
  // for. Two identical GETs issued before the first answers collapse into ONE
  // request; the entry is dropped the moment it settles, so nothing is held
  // and nothing is served stale. This is coalescing, not caching.
  //
  // It exists because the Sources page fetched `/console/producers` twice on
  // every load. React StrictMode double-invokes an effect in development, and
  // the effect's `cancelled` flag suppressed the second STATE UPDATE while the
  // second REQUEST still went out -- on okg-workspace that was a second
  // six-second read for a payload already in flight. A `useRef` mount guard in
  // the one component would have hidden this instance of it; coalescing fixes
  // the shape, for any two callers that ask the same question at once.
  //
  // A request carrying an AbortSignal is never coalesced. One caller aborting
  // would otherwise reject a promise another caller is waiting on, which
  // trades a duplicate request for a wrong answer.
  const inFlight = new Map<string, Promise<unknown>>();

  return {
    // `async`, so that the route guard inside `buildApiUrl` -- which this
    // method now reaches BEFORE sending, to build the coalescing key -- still
    // refuses by rejecting rather than by throwing synchronously at the call
    // site. A caller that awaits a read must not have to also try/catch it.
    async get<T>(
      route: `/viz/api/${string}`,
      params: Record<string, string | number | boolean | null | undefined> = {},
      opts: ApiGetOptions = {},
    ): Promise<T> {
      const coalesceKey = opts.signal
        ? null
        : `${opts.refresh ? "refresh" : "read"} ${buildApiUrl(
            route,
            opts.refresh ? { ...params, refresh: 1 } : params,
            baseUrl,
            mountBase,
          )}`;
      if (coalesceKey !== null) {
        const pending = inFlight.get(coalesceKey);
        if (pending) return pending as Promise<T>;
      }
      const started = send<T>(route, params, opts);
      if (coalesceKey !== null) {
        inFlight.set(coalesceKey, started);
        void started.catch(() => undefined).finally(() => {
          inFlight.delete(coalesceKey);
        });
      }
      return started;
    },
  };

  async function send<T>(
    route: `/viz/api/${string}`,
    params: Record<string, string | number | boolean | null | undefined>,
    opts: ApiGetOptions,
  ): Promise<T> {
    // A refresh adds the `refresh` flag (server bypass + relatest) and forces
    // the browser to skip its own HTTP cache, so the operator always gets a
    // freshly-read, latest-generation payload.
    const effectiveParams = opts.refresh ? { ...params, refresh: 1 } : params;
    const url = buildApiUrl(route, effectiveParams, baseUrl, mountBase);
    // Normal reads keep the minimal init (browser HTTP cache + ETag handle
    // revalidation). A refresh additionally forces the browser to skip its
    // cache and signals the backend bypass.
    // A bounded read gets a signal of its own, aborted by the bound or by the
    // caller's signal, whichever comes first. Without a bound the caller's
    // signal is passed through as it is.
    const bound = boundSignal(opts.signal, opts.timeoutMs);
    const signal = bound ? bound.signal : opts.signal;
    const init: RequestInit = opts.refresh
      ? {
          method: "GET",
          cache: "reload",
          headers: { Accept: "application/json", "Cache-Control": "no-cache" },
          ...(signal ? { signal } : {}),
        }
      : {
          method: "GET",
          headers: { Accept: "application/json" },
          ...(signal ? { signal } : {}),
        };
    try {
      return await receive<T>(route, effectiveParams, url, init, () => bound?.timedOut() ?? false, opts.timeoutMs);
    } finally {
      bound?.release();
    }
  }

  async function receive<T>(
    route: `/viz/api/${string}`,
    effectiveParams: Record<string, string | number | boolean | null | undefined>,
    url: string,
    init: RequestInit,
    timedOut: () => boolean,
    timeoutMs: number | undefined,
  ): Promise<T> {
    const timeoutError = () => new ApiRequestError({
      route,
      params: effectiveParams,
      responseText: `timed out after ${Math.round((timeoutMs ?? 0) / 1000)} s`,
      shape: "timeout",
      timeoutMs,
    });
    let response: Response;
    try {
      response = await fetchImpl(url, init);
    } catch (error) {
      if (timedOut()) throw timeoutError();
      throw new ApiRequestError({
        route,
        params: effectiveParams,
        responseText: error instanceof Error ? error.message : String(error),
      });
    }

    if (!response.ok) {
      let responseText = "";
      try {
        responseText = await response.text();
      } catch {
        responseText = "";
      }
      throw new ApiRequestError({
        route,
        params: effectiveParams,
        status: response.status,
        responseText,
      });
    }

    const contentType = (response.headers.get("content-type") ?? "").toLowerCase();
    if (!contentType.includes("application/json")) {
      let responseText = "";
      try {
        responseText = await response.text();
      } catch {
        responseText = "";
      }
      throw new ApiRequestError({
        route,
        params: effectiveParams,
        status: response.status,
        responseText,
        shape: contentType.includes("text/html") || /^\s*<(!doctype|html)/i.test(responseText)
          ? "web_page"
          : "not_data",
      });
    }

    // Read as text and parsed here, so a body that says it is JSON and is
    // not is a failure naming THIS route in words, not a bare SyntaxError
    // the page has no route for ("Unterminated string in JSON at position
    // 21" under `/viz/api/console/*`, audit 2026-09-23).
    let text: string;
    try {
      text = await response.text();
    } catch (error) {
      if (timedOut()) throw timeoutError();
      throw new ApiRequestError({
        route,
        params: effectiveParams,
        responseText: error instanceof Error ? error.message : String(error),
      });
    }
    let payload: unknown;
    try {
      payload = JSON.parse(text);
    } catch {
      throw new ApiRequestError({
        route,
        params: effectiveParams,
        status: response.status,
        responseText: text.slice(0, 200),
        shape: "not_data",
      });
    }
    if (isFailureEnvelope(payload)) {
      throw new ApiRequestError({
        route,
        params: effectiveParams,
        status: typeof payload.status === "number" ? payload.status : response.status,
        responseText: JSON.stringify(payload),
      });
    }

    return payload as T;
  }
}

/** A signal aborted by `outer` or by a `timeoutMs` bound, and a way to tell
 *  which. Null when there is no bound: the caller's signal is then used as
 *  it stands. */
function boundSignal(
  outer: AbortSignal | undefined,
  timeoutMs: number | undefined,
): { signal: AbortSignal; timedOut: () => boolean; release: () => void } | null {
  if (timeoutMs === undefined || timeoutMs <= 0) return null;
  const controller = new AbortController();
  let expired = false;
  const timer = setTimeout(() => {
    expired = true;
    controller.abort(new DOMException("timed out", "TimeoutError"));
  }, timeoutMs);
  const follow = () => controller.abort(outer?.reason);
  if (outer) {
    if (outer.aborted) follow();
    else outer.addEventListener("abort", follow, { once: true });
  }
  return {
    signal: controller.signal,
    timedOut: () => expired,
    release: () => {
      clearTimeout(timer);
      outer?.removeEventListener("abort", follow);
    },
  };
}

/** Bound every read from a client. The console wraps its client in this; a
 *  read that takes longer fails as "timed out after N s". */
export function withReadTimeout(api: ApiClient, timeoutMs: number = READ_TIMEOUT_MS): ApiClient {
  return {
    get: (route, params, opts) => api.get(route, params, { timeoutMs, ...opts }),
  };
}

/**
 * Wrap an ApiClient so every `get` forces a refresh. A route swaps to this
 * client for a single load when the operator clicks Refresh, so all of that
 * view's reads bypass the cache + re-resolve the latest generation without
 * threading a flag through every loader.
 */
export function withRefreshClient(api: ApiClient): ApiClient {
  return {
    get: (route, params, opts) => api.get(route, params, { ...opts, refresh: true }),
  };
}

/** Bind every read from a route load to one abort signal. Panel-specific
 * signals still win, allowing nested latest-request-wins effects. */
export function withAbortSignalClient(api: ApiClient, signal: AbortSignal): ApiClient {
  return {
    get: (route, params, opts) => api.get(route, params, {
      ...opts,
      signal: opts?.signal ?? signal,
    }),
  };
}
