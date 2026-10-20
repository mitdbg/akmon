// What one attempt changed, in words. One rule, two callers: Home's attempts
// list and the Updates table both print this phrase, and they used to compute
// it separately and identically wrongly.
//
// The defect this file exists to fix: both read only `nodes_added` and
// `edges_added`. The live deployment holds a published attempt whose
// nodes_retracted is 988 and edges_retracted 1,818 with both added counts
// zero. Read through the added pair alone, that attempt is "no change" -- a
// sentence the console printed about a publish that removed 2,806 things.
import { signed } from "@operator-ui/console/model/shared/format";
import type { RawAttempt } from "@operator-ui/console/model/shared/reads";

function count(value: number | null | undefined): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

/**
 * The phrase for one attempt's change, or "" when the outcome already said it.
 *
 * `separator` is the only difference between the two callers: Home sets its
 * parts with a comma, the Updates row with a middot.
 */
export function changePhrase(attempt: RawAttempt, separator = " · "): string {
  if (attempt.status !== "published") return "";

  // A read that could not compute the delta says so, with its own reason. It
  // is not "no change", and it is not a zero.
  if (attempt.delta_available === false) {
    const reason = attempt.delta_unavailable_reason;
    return reason ? `change not recorded: ${reason}` : "change not recorded";
  }
  const added = [attempt.nodes_added, attempt.edges_added];
  if (added.some((value) => value === null || value === undefined)) {
    return "change not recorded";
  }

  // Two conventions reach this function and both must read correctly.
  //
  //   * The live composer splits the delta: nodes_added is max(delta, 0) and
  //     the other side is in nodes_retracted.
  //   * A read that carries no retraction field expresses a net loss as a
  //     NEGATIVE nodes_added, and a version that retracted more than it added
  //     must keep its minus sign.
  //
  // So a negative added count IS a removal of that size, and any retraction
  // field adds to it. Reading only one of the two is how "no change" ended up
  // printed for an attempt that removed 2,806 things.
  // Per FIELD, not all-or-nothing. A recorded retraction is printed even when
  // the other side was never served, and the missing side is named. The
  // all-or-nothing rule dropped a retraction it HAD read because its sibling
  // was absent, and could print a negative "added" beside "removals not
  // recorded", which says the opposite of itself.
  const nodesRead = attempt.nodes_retracted !== null
    && attempt.nodes_retracted !== undefined;
  const edgesRead = attempt.edges_retracted !== null
    && attempt.edges_retracted !== undefined;

  const addedNodes = Math.max(count(attempt.nodes_added), 0);
  const addedEdges = Math.max(count(attempt.edges_added), 0);
  // A negative added count IS a recorded removal, whatever the retraction
  // field did, so it counts on both sides of that question.
  const negativeNodes = Math.max(-count(attempt.nodes_added), 0);
  const negativeEdges = Math.max(-count(attempt.edges_added), 0);
  const removedNodes = (nodesRead ? count(attempt.nodes_retracted) : 0) + negativeNodes;
  const removedEdges = (edgesRead ? count(attempt.edges_retracted) : 0) + negativeEdges;

  const parts: string[] = [];
  if (addedNodes > 0) parts.push(`${signed(addedNodes)} records`);
  if (addedEdges > 0) parts.push(`${signed(addedEdges)} relationships`);
  if (removedNodes > 0) {
    parts.push(`-${removedNodes.toLocaleString("en-US")} records`);
  }
  if (removedEdges > 0) {
    parts.push(`-${removedEdges.toLocaleString("en-US")} relationships`);
  }
  // Only a side with nothing recorded AND nothing derived from a negative
  // added count is named as unrecorded: saying "record removals not recorded"
  // beside "-988 records" would contradict the clause next to it.
  const unread: string[] = [];
  if (!nodesRead && negativeNodes === 0) unread.push("record removals not recorded");
  if (!edgesRead && negativeEdges === 0) {
    unread.push("relationship removals not recorded");
  }

  if (parts.length === 0 && unread.length === 0) return "no change";
  return [...parts, ...unread].join(separator);
}
