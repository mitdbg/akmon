// What the page can say about ONE live read that failed, from the error alone.
//
// Pure, and in the model rather than beside the component, because two things
// need it: the `ReadFailure` element that prints a failed panel's place, and
// Home's Needs attention, which lists a failed producer read as a row. A model
// importing a component to name a failure is the wrong way round.
import { SearchResponseError } from "@operator-ui/console/api/searchData";
import { ApiRequestError } from "@operator-ui/shared/api/apiClient";

/**
 * What kind of failure this was, because the four need four different next
 * steps and only one of them is about the server being down.
 *
 *  - `answered`: the server responded and said no. Its status is the fact.
 *  - `no_response`: nothing came back at all -- `fetch` itself threw, which
 *    `createApiClient` records as an ApiRequestError carrying no status.
 *  - `refused_by_client`: the server answered and a strict parser HERE
 *    refused that answer. The read reached the server; telling an operator
 *    it did not would send them to check a process that is up and serving.
 *  - `unclassified`: an error of none of those kinds. Its message is printed
 *    and no recovery step is invented for it.
 */
export type ReadFailureKind = "answered" | "no_response" | "refused_by_client" | "unclassified";

export interface NamedReadFailure {
  /** Which of the four this is. See ReadFailureKind. */
  kind: ReadFailureKind;
  /** The route, as the client asked for it. */
  route: string;
  /** The HTTP status, when the failure reached the server at all. */
  status: number | null;
  /** The server's own machine reason, e.g. `ontology_rollup_unavailable` or
   *  `policy_denied`. Null when the response carried none. */
  reason: string | null;
  /** The server's sentence, or the error's message. */
  detail: string;
  /** What to do next. The server's `next_check` when it gave one. */
  nextCheck: string | null;
  /**
   * True when the body parsed as JSON carrying a STRING `detail`.
   *
   * That, with no `reason`, is the shape of this read API's own catch-all --
   * `{"detail": "unknown viz API route"}`, raised at src/okg/viz/app.py by the
   * archived-frontend boundary. A 404 from anything in front of the API (a
   * proxy's HTML error page, a load balancer, a static host) parses to nothing
   * and carries neither, and says NOTHING about which routes the read API
   * serves. Without this the two were indistinguishable and every foreign 404
   * was reported as a route this deployment does not serve.
   */
  appDetailBody: boolean;
}

function parseBody(text: string | undefined): Record<string, unknown> | null {
  if (!text) return null;
  try {
    const parsed = JSON.parse(text) as Record<string, unknown>;
    const detail = parsed.detail;
    // A FastAPI validation refusal carries a LIST under `detail`; that is
    // read by `validationReasons`, not unwrapped as if it were the body.
    if (detail && typeof detail === "object" && !Array.isArray(detail)) {
      return detail as Record<string, unknown>;
    }
    return parsed;
  } catch {
    return null;
  }
}

/**
 * The reasons a 422 gave, in words: "q: String should have at most 512
 * characters". FastAPI answers a request it will not validate with
 * `{"detail": [{"loc": [...], "msg": "..."}]}`; the page used to print the
 * route and "422" and nothing a reader could act on. Null for any other body.
 */
export function validationReasons(text: string | undefined): string | null {
  if (!text) return null;
  let parsed: unknown;
  try { parsed = JSON.parse(text); } catch { return null; }
  const detail = (parsed as { detail?: unknown } | null)?.detail;
  if (!Array.isArray(detail) || detail.length === 0) return null;
  const reasons = detail.map((item) => {
    const entry = item as { loc?: unknown; msg?: unknown };
    const msg = typeof entry.msg === "string" ? entry.msg : null;
    if (!msg) return null;
    const loc = Array.isArray(entry.loc)
      ? entry.loc.filter((part) => part !== "query" && part !== "body").join(".")
      : "";
    return loc ? `${loc}: ${msg}` : msg;
  }).filter((reason): reason is string => reason !== null);
  return reasons.length ? reasons.join("; ") : null;
}

/**
 * What the page cannot show when THIS read failed, in the reader's words.
 *
 * The headline is built from the read that failed, never from the page it
 * failed on: Sources printed "producers could not be read" over a failure of
 * `/console/attempts`, Updates "update attempts" over `/console/health`
 * (audit 2026-09-23). A route not listed here falls back to what the caller
 * says the page lost.
 */
export const READ_WHAT: Readonly<Record<string, string>> = {
  "/viz/api/deployments": "This deployment's name",
  "/viz/api/console/health": "This deployment's health",
  "/viz/api/console/generations": "The update attempts",
  "/viz/api/console/attempts": "The update attempts by day",
  "/viz/api/console/attempts/detail": "This attempt's record",
  "/viz/api/console/versions": "The published versions",
  "/viz/api/console/producers": "The configured producers",
  "/viz/api/console/source-diagnostics": "The configured sources",
  "/viz/api/console/ontology-rollup": "The record and relationship counts",
  "/viz/api/console/records/types": "The record types",
  "/viz/api/console/records/search": "These results",
  "/viz/api/console/records/list": "These records",
  "/viz/api/console/records/detail": "This record",
  "/viz/api/console/records/artifact": "This record's original file",
  "/viz/api/console/records/raw": "This record's raw layers",
  "/viz/api/console/graph/types": "The record types and their relationships",
  "/viz/api/console/graph/source-links": "Which sources link to which",
  "/viz/api/console/graph/record": "This record's neighbourhood",
  "/viz/api/console/graph/ontology": "The declared record types",
};

/** The headline's subject for a failure: the failed read's own `what`, or
 *  the caller's when the route is not one this console names. */
export function failedReadWhat(
  failure: NamedReadFailure, fallback: string, error?: unknown,
): string {
  // Two reads are the same route asked something narrower, and say so.
  const params = error instanceof ApiRequestError ? error.params : {};
  if (failure.route === "/viz/api/console/producers" && typeof params.name === "string") {
    return `The read for ${params.name}`;
  }
  if (failure.route === "/viz/api/console/generations" && typeof params.cursor === "string") {
    return "The next page of update attempts";
  }
  return READ_WHAT[failure.route] ?? fallback;
}

/** The sentence for a failure the client recognised by its shape, or null. */
function shapeSentence(error: ApiRequestError): string | null {
  if (error.shape === "timeout") {
    return `The read timed out after ${Math.round((error.timeoutMs ?? 0) / 1000)} s.`;
  }
  if (error.shape === "web_page") {
    return "A web page came back instead of data (a sign-in page or a proxy?).";
  }
  if (error.shape === "not_data") return "The reply was not data.";
  return null;
}

/** What the page can say about one failed read, from the error alone. */
export function namedReadFailure(route: string, error: unknown): NamedReadFailure {
  if (!(error instanceof ApiRequestError)) {
    return {
      kind: error instanceof SearchResponseError ? "refused_by_client" : "unclassified",
      route,
      status: null,
      reason: null,
      detail: error instanceof Error ? error.message : String(error),
      nextCheck: null,
      appDetailBody: false,
    };
  }
  const shaped = shapeSentence(error);
  if (shaped !== null) {
    return {
      kind: error.shape === "timeout" ? "no_response" : "answered",
      route: error.route,
      // A web page or a non-JSON body that came back 200 is not a status
      // worth printing: the 200 is the thing that was wrong with it.
      status: error.status === undefined || error.status === 200 ? null : error.status,
      reason: null,
      detail: shaped,
      nextCheck: error.shape === "timeout"
        ? "The read API may be busy or stuck. Check its log for this route."
        : "Check what is in front of the read API for this route.",
      appDetailBody: false,
    };
  }
  const body = parseBody(error.responseText);
  const reason = typeof body?.reason === "string" ? body.reason : null;
  const serverError = typeof body?.error === "string" ? body.error : null;
  const nextCheck = typeof body?.next_check === "string" ? body.next_check : null;
  const validation = error.status === 422 ? validationReasons(error.responseText) : null;
  return {
    // No status is the client recording that `fetch` threw: no response.
    kind: error.status === undefined ? "no_response" : "answered",
    route: error.route,
    status: error.status ?? null,
    reason,
    detail: validation
      ? `The read API refused the request: ${validation}.`
      : serverError ?? error.message,
    nextCheck: nextCheck ?? (validation
      ? "Check the address this page was opened with."
      : null),
    appDetailBody: typeof body?.detail === "string",
  };
}

/**
 * True when the detail is only the client's own default message.
 *
 * `ApiRequestError` words itself "Read failed: <route> (<status>)". Printed
 * after the route and the status it repeated both, so a 502 from the proxy in
 * front of the read API read "/viz/api/console/health \u00b7 502 \u00b7 Read
 * failed: /viz/api/console/health (502)" -- the route twice and the status
 * twice in one line (Archiv3 review, 2026-09-22). The detail is dropped when
 * it says nothing the route and the status do not.
 */
export function detailIsClientDefault(failure: NamedReadFailure): boolean {
  const status = failure.status === null ? "" : ` (${failure.status})`;
  return failure.detail === `Read failed: ${failure.route}${status}`;
}

/**
 * True when nothing behind the proxy answered at all.
 *
 * A 502 or a 504 with no body the read API wrote is the gateway saying the
 * read API did not answer it.
 */
export function serverNotAnswering(failure: NamedReadFailure): boolean {
  if (failure.kind !== "answered" || failure.status === null) return false;
  if (failure.reason !== null || !detailIsClientDefault(failure)) return false;
  // 503 is left out: the read API answers 503 itself when a read's
  // authority is unavailable, and that has its own next step.
  return failure.status === 502 || failure.status === 504;
}

/** "The data server is not answering (502)." -- one sentence for a gateway
 *  failure, in place of the route, the status and the client's own message. */
export function notAnsweringSentence(failure: NamedReadFailure): string {
  return `The data server is not answering (${failure.status}).`;
}

/** The failure as ONE line: what happened, then the route once. For a place
 *  that has room for a row and not for a panel. Nothing is derived and no
 *  next step is invented. */
export function readFailureLine(failure: NamedReadFailure): string {
  if (serverNotAnswering(failure)) {
    return `${notAnsweringSentence(failure)} ${failure.route}`;
  }
  return [
    failure.route,
    failure.status === null ? null : String(failure.status),
    failure.reason,
    detailIsClientDefault(failure) ? null : failure.detail,
  ].filter((part): part is string => Boolean(part)).join(" \u00b7 ");
}
