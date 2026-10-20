// What the Graph view says when one of its reads fails, beyond what the
// console's shared read-failure surface says for any read.
//
// Four failures are not faults, and the generic words for them were wrong:
//
//   - Nothing published (404 `version_not_published` with no `?version=`):
//     the deployment is empty. The generic 404 step said "Check the id this
//     page was opened with", and the type map takes no id. It is said as Home
//     says it, in one sentence, with no Read again.
//   - A version the URL names that this deployment never published: said by
//     name, with the picker as the way out.
//   - A record id the version does not hold (404 `record_not_found`): a plain
//     named line. Reading again cannot change the answer.
//   - A policy refusal (403): named as one, with no Read again and no
//     automatic re-read; a refusal does not go away on its own.
//
// And the one failure the Grid expects by design: the source-links grid is
// computed on request under a statement timeout, and the server answers a
// timeout with a code and two numbers but no sentence (`{ reason:
// "source_links_unavailable", cause: "timeout", elapsed_ms, timeout_ms }`).
import { ApiRequestError } from "@operator-ui/shared/api/apiClient";
import { shortVersion } from "@operator-ui/console/model/shared/ids";

/** The failure body's `detail` object, or null. */
export function failureDetail(error: unknown): Record<string, unknown> | null {
  if (!(error instanceof ApiRequestError) || !error.responseText) return null;
  let body: unknown;
  try { body = JSON.parse(error.responseText); } catch { return null; }
  const detail = (body as { detail?: unknown } | null)?.detail;
  const d = detail && typeof detail === "object" && !Array.isArray(detail) ? detail : body;
  return d && typeof d === "object" && !Array.isArray(d) ? d as Record<string, unknown> : null;
}

export type SettledFailure =
  | { kind: "nothing_published" }
  | { kind: "version_not_published"; version: string | null }
  | { kind: "record_not_found"; id: string }
  | { kind: "refused"; reason: string | null; sentence: string | null };

/**
 * The failures reading again cannot change, classified; null for any other,
 * which the shared surface names with its Read again.
 *
 * `askedVersion` is the version the page asked for (null for the latest):
 * `version_not_published` means "nothing is published" only when no version
 * was asked for.
 */
export function settledFailure(error: unknown, askedVersion: string | null, recordId: string | null = null): SettledFailure | null {
  if (!(error instanceof ApiRequestError)) return null;
  const d = failureDetail(error);
  const reason = typeof d?.reason === "string" ? d.reason : null;
  if (error.status === 404 && reason === "version_not_published") {
    return askedVersion ? { kind: "version_not_published", version: askedVersion } : { kind: "nothing_published" };
  }
  if (error.status === 404 && reason === "record_not_found" && recordId !== null) {
    return { kind: "record_not_found", id: recordId };
  }
  if (error.status === 403) {
    const sentence = typeof d?.error === "string" ? d.error : null;
    return { kind: "refused", reason, sentence };
  }
  return null;
}

/** True for a failure the page must not re-read on its own schedule. */
export function isSettled(error: unknown): boolean {
  return settledFailure(error, null, "") !== null;
}

/** The sentence a settled failure prints. `deployment` names the deployment
 *  when the page knows it. */
export function settledSentence(f: SettledFailure, deployment: string | null, what: string): string {
  switch (f.kind) {
    case "nothing_published":
      return deployment
        ? `No version has been published for ${deployment}; there is nothing to draw yet.`
        : "No version has been published for this deployment; there is nothing to draw yet.";
    case "version_not_published":
      return `Version ${shortVersion(f.version) ?? "named on this link"} is not published in this deployment. `
        + "Choose a published version from the version picker.";
    case "record_not_found":
      return `This version holds no record with the id ${f.id} that you may open.`;
    case "refused":
      return `${what} was refused by the read policy${f.reason ? ` (${f.reason})` : ""}. `
        + `${f.sentence ? `${f.sentence} ` : ""}A refusal is not a fault; reading again gives the same answer.`;
    default:
      return "";
  }
}

export interface SourceLinksTimeout { elapsedMs: number | null; timeoutMs: number | null }

export function sourceLinksTimeout(error: unknown): SourceLinksTimeout | null {
  if (!(error instanceof ApiRequestError) || error.status !== 503) return null;
  const d = failureDetail(error);
  if (!d || d.reason !== "source_links_unavailable" || d.cause !== "timeout") return null;
  const num = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? v : null);
  return { elapsedMs: num(d.elapsed_ms), timeoutMs: num(d.timeout_ms) };
}

const seconds = (ms: number) => `${(ms / 1000).toFixed(1).replace(/\.0$/, "")} s`;

export function sourceLinksTimeoutSentence(t: SourceLinksTimeout): string {
  const limit = t.timeoutMs !== null ? ` at its ${seconds(t.timeoutMs)} limit` : "";
  // "at its 18 s limit, after 18 s" said one number twice: the elapsed time
  // is printed only when it differs from the limit as printed.
  const after = t.elapsedMs !== null && (t.timeoutMs === null || seconds(t.elapsedMs) !== seconds(t.timeoutMs))
    ? `, after ${seconds(t.elapsedMs)}`
    : "";
  return `This grid is computed when it is asked for, and the computation stopped${limit}${after}. Nothing is missing from the graph.`;
}

export const SOURCE_LINKS_TIMEOUT_NEXT = "Read again: a quieter database may finish in time.";

/** The next step for any other failed source-links read. The shared 503 step
 *  -- "check that its maintained tables exist" -- does not apply: this grid
 *  has no maintained table; it is computed on request. */
export const SOURCE_LINKS_FAILED_NEXT = "This grid is computed on request and the computation failed. Read again; if it fails again, check the read API's log for this route.";

/** "computed in 11.6 s when first asked for; kept for this version": what
 *  the source-links answer says about itself. */
export function sourceLinksBasisClause(computedMs: number | null, cached: boolean): string | null {
  if (computedMs === null) return cached ? "Kept from an earlier read of this version." : null;
  return cached
    ? `Computed in ${seconds(computedMs)} when first asked for, and kept for this version.`
    : `Computed in ${seconds(computedMs)} for this read.`;
}
