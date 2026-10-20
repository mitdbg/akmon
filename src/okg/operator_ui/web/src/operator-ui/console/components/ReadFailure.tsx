// A live read that failed, said in place, by name.
//
// LoudFailure beside this file is the harness's version: its next step is
// "add the read to the fixture and regenerate". On a live console that advice
// is wrong, and a reader following it would edit a file that has nothing to
// do with what broke. This component says what a live operator needs: WHICH
// read, WHAT the server said, and WHAT to check next -- the server's own
// `next_check` when it gave one, and otherwise a step derived from the status
// code rather than invented.
//
// Which read failed is not the same question as WHAT failed. A response the
// server sent and a strict parser here refused is not a server that could not
// be reached, and the recovery step differs: one is "check that the read API
// is running", the other is "the console and the read API disagree about this
// payload". `namedReadFailure` classifies them (see ReadFailureKind) and no
// step is offered for a failure nothing here can classify. Partially
// addresses #2153.
//
// Nothing is substituted for the failed read. A panel that failed renders
// this and no figure; a zero here would be indistinguishable from a real
// zero, which is the failure mode rule 13 exists to prevent.
import { useContext, type ReactElement } from "react";

import { ConsoleReadsContext } from "@operator-ui/console/components/readRecovery";
import {
  detailIsClientDefault, failedReadWhat, namedReadFailure, notAnsweringSentence, serverNotAnswering,
  type NamedReadFailure, type ReadFailureKind,
} from "@operator-ui/console/model/shared/readFailure";

// Re-exported: `namedReadFailure` used to live here, and the element that
// prints a failure and the model that names one are the same fact.
export { namedReadFailure, type NamedReadFailure, type ReadFailureKind };

/**
 * Which 404 this is.
 *
 * A console read that reaches a route this API SERVES and finds nothing is
 * answered with that route's own constant body, which carries a `reason`
 * (`attempt_not_found`).
 *
 * A read that names a route this API does not route at all is answered by the
 * app's own not-found handler: a JSON body with a string `detail` and no
 * `reason` -- the literal `{"detail": "unknown viz API route"}` raised at
 * src/okg/viz/app.py.
 *
 * A 404 that is NEITHER is a third thing, and the one this used to get wrong.
 * The test was the ABSENCE of a reason alone, which a proxy's HTML error page,
 * a load balancer and a stale CDN entry all pass; every one of them was
 * reported as a route this deployment's read API does not serve, which is a
 * claim about the API build that nothing in the response supports. The
 * absence of a reason is necessary and not sufficient: the body must also be
 * the app's own shape. What is left says only what is true -- that a 404 came
 * back with a body this console cannot read -- and names the route.
 *
 * No branch here reads the body's PROSE, so a future handler that rewrites its
 * sentence still classifies the same way; what it may not do is stop being
 * JSON with a string `detail`.
 */
export type NotFoundKind =
  | "not-a-404" | "named" | "route-not-served" | "unrecognised";

export function notFoundKind(failure: NamedReadFailure): NotFoundKind {
  if (failure.status !== 404) return "not-a-404";
  if (failure.reason !== null) return "named";
  return failure.appDetailBody ? "route-not-served" : "unrecognised";
}

/** The one sentence a page prints for a route this read API does not serve.
 *  It names the read, and it invents no reason code. */
export function routeNotServedSentence(route: string): string {
  return "This console build is newer than this deployment's read API, "
    + `which does not serve ${route}.`;
}

/** The one sentence for a 404 that is neither of the two the console knows.
 *  It claims nothing about the read API's routes, because the body gave it
 *  nothing to claim them from. */
export function unrecognisedNotFoundSentence(route: string): string {
  return "The read API answered 404 with a body this console does not "
    + `recognise, for ${route}.`;
}

/** The step to offer when the server named none. Derived from the kind and
 *  the status, not guessed: each of these is a fact about what that failure
 *  means here. Null when nothing recorded supports a step, in which case the
 *  surface prints none rather than one the response contradicts. */
export function fallbackNextCheck(failure: NamedReadFailure): string | null {
  if (failure.kind === "refused_by_client") {
    return "This read reached the server and this console refused its answer. "
      + "Check that the read API is the version this console expects.";
  }
  if (failure.kind === "unclassified") return null;
  if (serverNotAnswering(failure)) return "Check that the read API is running.";
  if (failure.status === 403) {
    return "This read was refused by policy, not by a fault. "
      + "Check the active policy bundle for this deployment.";
  }
  if (failure.status === 503) {
    return "The read's authority was unavailable for the pinned generation. "
      + "Check that the generation published and that its maintained tables exist.";
  }
  if (failure.status === 404) {
    // Not one step but three, because a 404 here has three causes and each is
    // checked somewhere else.
    const kind = notFoundKind(failure);
    if (kind === "route-not-served") {
      return "This route is not served by this deployment's read API. "
        + "Check that the API is the version this console expects.";
    }
    if (kind === "unrecognised") {
      return "This 404 did not come from the read API's own handlers. "
        + "Check what is in front of the read API for this route.";
    }
    return "The read reached this route and it holds no such record. "
      + "Check the id this page was opened with.";
  }
  if (failure.kind === "no_response") {
    return "The read did not reach the server. Check that the read API is running.";
  }
  return "Check the read API's log for this route.";
}

export function ReadFailure({
  route, error, what, onReadAgain, retrying = false, nextCheck = null,
}: {
  route: string;
  error: unknown;
  /** What the page cannot show, in the reader's words, when the failed
   *  read is not one `READ_WHAT` names. The headline is the FAILED READ's
   *  own subject wherever it is known, never a fixed per-page string. */
  what: string;
  /** Re-issue this read. Absent, "Read again" reads the whole view again;
   *  outside the live console there is no button. */
  onReadAgain?: (() => void) | null;
  /** A re-read is in flight: the button says so and waits. */
  retrying?: boolean;
  /** The step to offer when the server named none and the caller knows a
   *  better one than the status code gives. The server's own always wins. */
  nextCheck?: string | null;
}): ReactElement {
  const { readAgainView } = useContext(ConsoleReadsContext);
  const failure = namedReadFailure(route, error);
  const next = failure.nextCheck ?? nextCheck ?? fallbackNextCheck(failure);
  const again = onReadAgain ?? readAgainView;
  return (
    <div className="okg-console__read-failed" role="alert" data-testid={`read-failed-${failure.route}`}>
      <p className="okg-console__read-failed-headline">
        {failedReadWhat(failure, what, error)} could not be read.
      </p>
      {/* A gateway failure is one sentence and the route once. The route,
          the status and the client's own "Read failed: <route> (<status>)"
          used to print the route twice and the status twice under a
          headline that already said the read failed. */}
      {serverNotAnswering(failure) ? (
        <p className="okg-console__read-failed-detail">
          {notAnsweringSentence(failure)} <code>{failure.route}</code>
        </p>
      ) : (
        <>
          <p className="okg-console__read-failed-detail">
            <code>{failure.route}</code>
            {failure.status === null ? null : <> · {failure.status}</>}
            {failure.reason === null ? null : <> · <code>{failure.reason}</code></>}
          </p>
          {detailIsClientDefault(failure) ? null : (
            <p className="okg-console__read-failed-detail">{failure.detail}</p>
          )}
        </>
      )}
      {next === null ? null : (
        <p className="okg-console__read-failed-next">{next}</p>
      )}
      {again ? (
        <p className="okg-console__read-failed-again">
          <button
            type="button"
            className="okg-console__read-again"
            data-testid="read-again"
            disabled={retrying}
            onClick={again}
          >
            {retrying ? "Reading again\u2026" : "Read again"}
          </button>
        </p>
      ) : null}
    </div>
  );
}
