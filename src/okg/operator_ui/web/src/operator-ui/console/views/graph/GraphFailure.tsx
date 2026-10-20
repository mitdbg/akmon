// A Graph read that failed, said the way that failure deserves (see
// model/graph/failures.ts). Shared by the whole-graph pages and the record
// page, so a failure reads the same wherever it happens.
import type { ReactElement } from "react";
import { LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { settledFailure, settledSentence } from "@operator-ui/console/model/graph/failures";

/**
 * A read that failed, said the way that failure deserves. The failures that
 * reading again cannot change -- nothing published, a version or record this
 * deployment does not hold, a policy refusal -- are one plain line with no
 * Read again; every other goes to the console's shared surface.
 */
export function GraphFailure({
  error, route, what, readAgain, retrying, askedVersion, recordId = null, deployment = null,
}: {
  error: unknown;
  route: string;
  what: string;
  readAgain: () => void;
  retrying: boolean;
  askedVersion: string | null;
  recordId?: string | null;
  deployment?: string | null;
}): ReactElement {
  const settled = settledFailure(error, askedVersion, recordId);
  if (settled) {
    return (
      <p className={`okg-console__fact graph-settled graph-settled--${settled.kind}`} role="status" data-testid={`graph-settled-${settled.kind}`}>
        {settledSentence(settled, deployment, what)}
      </p>
    );
  }
  return <LoudFailure error={error} route={route} what={what} onReadAgain={readAgain} retrying={retrying} />;
}
