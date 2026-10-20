// The Related tab: relationship chips, the direction control and the capped
// rows, with what the cap leaves out stated.
import { type ExploreParams, exploreQuery } from "@operator-ui/console/model/explore/params";
import type { ExploreRecord } from "@operator-ui/console/model/explore/record";
import { recordTitle, titlesInList } from "@operator-ui/console/model/explore/titles";
import { exact } from "@operator-ui/console/model/shared/format";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { RelatedRecord } from "@operator-ui/console/api/searchData";

/** Related rows per page. */
export const RELATED_PAGE = 50;

// --- Related --------------------------------------------------------------------

export type RelatedDirectionFilter = "both" | "outgoing" | "incoming";

export interface RelatedChip {
  relationship: string;
  /** Rows of this type before the cap when the read says; else the rows
   *  shown, marked as a floor. */
  count: number;
  countIsFloor: boolean;
  selected: boolean;
  href: string;
}

/** One edge as the read served it: a neighbour, a relationship and which way
 *  round the graph recorded it. */
export interface RelatedEdge {
  record: RelatedRecord;
  relationship: string;
  direction: "outgoing" | "incoming";
}

export interface RelatedRow {
  record: RelatedRecord;
  /** EVERY way this one record is recorded against the record being read.
   *  A chunk and its document are usually recorded twice -- `contains` one
   *  way, `member_of` the other -- and that is one neighbour, not two. */
  edges: RelatedEdge[];
  /** The Relationship cell: every relationship this neighbour is recorded
   *  under, "contains · member_of". */
  relationship: string;
  /** The Direction cell: every direction, "outgoing · incoming". */
  direction: string;
  href: string;
  /** What the row is CALLED: the same rule as a results row and the record
   *  page it links to -- the document, then the part of it -- so the name
   *  does not change under the reader on the way there. The id is not in
   *  it; it stays in the cell's title and its copy control. */
  title: string;
}

/**
 * One row per RECORD, carrying every edge that reached it.
 *
 * The read serves one entry per edge, and the tab listed them as they came:
 * the live document's two chunks drew four rows, each chunk twice with the
 * same title and the same id, under a heading counting four and a sentence
 * counting two. A reader of that page has two neighbours.
 */
export function rowsByRecord(
  edges: readonly RelatedEdge[], open: (id: string) => string,
): RelatedRow[] {
  const rows = new Map<string, RelatedRow>();
  for (const edge of edges) {
    const found = rows.get(edge.record.nodeId);
    if (found) { found.edges.push(edge); continue; }
    rows.set(edge.record.nodeId, {
      record: edge.record,
      edges: [edge],
      relationship: "",
      direction: "",
      href: open(edge.record.nodeId),
      title: relatedTitle(edge.record),
    });
  }
  // The cells, in the order the read served the edges, each name once: a
  // neighbour recorded `contains` twice reads "contains", not
  // "contains · contains".
  for (const row of rows.values()) {
    row.relationship = [...new Set(row.edges.map((edge) => edge.relationship))].join(" · ");
    row.direction = [...new Set(row.edges.map((edge) => edge.direction))].join(" · ");
  }
  return inPartOrder([...rows.values()]);
}

/**
 * The parts of one document in the order their titles number them.
 *
 * A document's chunks came back in the order the edges were stored, so the
 * Related tab listed "Part 7, Part 2, Part 31" (review 2026-09-22). Rows that
 * are parts of the same document are sorted by their position among the
 * slots those rows already hold; every other row keeps its place.
 */
function inPartOrder(rows: RelatedRow[]): RelatedRow[] {
  const slots = new Map<string, number[]>();
  rows.forEach((row, index) => {
    const origin = row.record.origin;
    if (origin.chunkIndex === null || (origin.path === null && origin.title === null)) return;
    const key = origin.title ?? origin.path ?? "";
    slots.set(key, [...(slots.get(key) ?? []), index]);
  });
  const out = [...rows];
  for (const indices of slots.values()) {
    const sorted = indices
      .map((index) => rows[index])
      .sort((a, b) => (a.record.origin.chunkIndex as number) - (b.record.origin.chunkIndex as number));
    indices.forEach((slot, position) => { out[slot] = sorted[position]; });
  }
  return out;
}

export interface RelatedModel {
  chips: RelatedChip[];
  direction: RelatedDirectionFilter;
  directions: { value: RelatedDirectionFilter; label: string; href: string }[];
  rows: RelatedRow[];
  /** "1–50 of 200 rows · 500 related: 400 outgoing, 100 incoming". */
  countLine: string;
  /** The whole relationship set in one sentence -- "Contained in
   *  HcalIsoTrackValidationSim." -- when every row is one kind of
   *  containment and the page holds all of them. Null otherwise, and then
   *  the count line and the controls say it instead. */
  summary: string | null;
  /** True when the chips and the direction control are worth drawing: more
   *  than one row to filter, and no sentence that already says the lot. */
  showControls: boolean;
  /** True when the table prints a Direction column. It is dropped under a
   *  summary sentence, which states the direction in words. */
  showDirection: boolean;
  capSentences: string[];
  firstHref: string | null;
  prevHref: string | null;
  nextHref: string | null;
  emptySentence: string | null;
}

/** "3 of 41 outgoing shown", when the read served a total larger than the
 *  rows it sent. A capped list and a total past the tally ceiling are NOT
 *  said here: `relatedCaps` on the record read states both from the flags
 *  the server sets, and this would say the same thing a second time in
 *  weaker words. */
export function capSentence(
  direction: "outgoing" | "incoming", shown: number, total: number | null,
): string | null {
  if (total !== null && total > shown) return `${exact(shown)} of ${exact(total)} ${direction} shown`;
  return null;
}

/** What one related row is called. The record page's own rule, applied to
 *  the neighbour: its document and its part, or the served label when the
 *  neighbour's source recorded no identity. */
export function relatedTitle(record: RelatedRecord): string {
  return recordTitle({ label: record.label, origin: record.origin });
}

/**
 * The two shapes a chunk and its document are recorded in, from the record
 * being READ: `member_of` out of it and `contains` into it both say it is
 * inside something; `contains` out of it and `member_of` into it both say it
 * holds something.
 *
 * The composer reports which shape reached a document's chunks as
 * `member_of`, `contains` or `both`, and `both` is a fact the graph really
 * records -- the two readers on the live graphs write one edge each and a
 * deployment that ran both has the pair twice. It is kept and said in words.
 */
const INSIDE: ReadonlySet<string> = new Set(["member_of:outgoing", "contains:incoming"]);
const HOLDS: ReadonlySet<string> = new Set(["contains:outgoing", "member_of:incoming"]);

/**
 * Every related row as one sentence, or null when one sentence would leave
 * something out.
 *
 * It is written only when the page is showing the WHOLE relationship set --
 * no chip, no direction filter, no cap, no next page -- and every row is one
 * kind of containment. Anything else (a `references` edge beside a
 * `contains`, a capped list, a second page) keeps the count line, because a
 * sentence over part of a set reads as a sentence over the set.
 */
export function relatedSentence(rows: readonly RelatedRow[]): string | null {
  if (!rows.length) return null;
  const edges = rows.flatMap((row) => row.edges);
  const pairs = new Set(edges.map((edge) => `${edge.relationship}:${edge.direction}`));
  const inside = [...pairs].every((pair) => INSIDE.has(pair));
  const holds = [...pairs].every((pair) => HOLDS.has(pair));
  if (!inside && !holds) return null;
  const neighbours = new Map(rows.map((row) => [row.record.nodeId, row.record]));
  // One edge per neighbour, or two -- the pair recorded both ways. A count in
  // between means some neighbours are recorded one way and some the other,
  // which no single sentence covers.
  const perNeighbour = edges.length / neighbours.size;
  if (perNeighbour !== 1 && perNeighbour !== 2) return null;
  if (perNeighbour === 2 && pairs.size !== 2) return null;
  const bothWays = perNeighbour === 2 ? ", recorded both ways" : "";
  const lead = inside ? "Contained in" : "Contains";
  const only = neighbours.size === 1 ? [...neighbours.values()][0] : null;
  // The TITLE, not the label: "Contained in Document Chunk - e734d84f" is
  // the hash again, and the sentence has to name the same thing the row and
  // the page behind it name.
  if (only) return `${lead} ${relatedTitle(only)}${bothWays}.`;
  const types = new Set([...neighbours.values()].map((record) => record.type));
  const type = types.size === 1 ? `${[...types][0]} ` : "";
  return `${lead} ${exact(neighbours.size)} ${type}records${bothWays}.`;
}

export function buildRelated(
  detail: ExploreRecord, params: ExploreParams, link: LinkFn, open: (id: string) => string,
): RelatedModel {
  const base = detail.base;
  // One entry per EDGE, which is what the read serves and what the
  // direction and relationship filters select on. The rows the reader sees
  // are built from these, one per record, below.
  const all: RelatedEdge[] = [
    ...base.outgoing.map((record) => ({
      record, relationship: record.relationship, direction: "outgoing" as const,
    })),
    ...base.incoming.map((record) => ({
      record, relationship: record.relationship, direction: "incoming" as const,
    })),
  ];
  const allRows = rowsByRecord(all, open);
  const byDirection = all.filter((edge) => params.dir === "both" || edge.direction === params.dir);
  const tallyKnown = detail.byRelationship !== null;
  const tally = new Map<string, number>();
  if (detail.byRelationship) {
    for (const entry of detail.byRelationship) {
      if (params.dir !== "both" && entry.direction !== params.dir) continue;
      tally.set(entry.relationship, (tally.get(entry.relationship) ?? 0) + entry.count);
    }
  } else {
    for (const edge of byDirection) tally.set(edge.relationship, (tally.get(edge.relationship) ?? 0) + 1);
  }
  if (params.rel && !tally.has(params.rel)) tally.set(params.rel, 0);
  const hrefFor = (next: Partial<ExploreParams>) => link("explore", exploreQuery({
    ...params, record: base.nodeId, tab: "related", roffset: 0, ...next,
  }));
  const chips: RelatedChip[] = [...tally.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([relationship, count]) => ({
      relationship,
      count,
      countIsFloor: !tallyKnown,
      selected: relationship === params.rel,
      href: hrefFor({ rel: relationship === params.rel ? null : relationship }),
    }));
  const selected = params.rel
    ? byDirection.filter((edge) => edge.relationship === params.rel)
    : byDirection;
  // The filters run over edges; the PAGE is over records. Deduplicating
  // after the filters keeps a direction or relationship filter honest: under
  // `dir=outgoing` a pair recorded both ways is one edge, so the row names
  // that one direction.
  const filtered = rowsByRecord(selected, open);
  // Titled against the rows the reader actually sees. A collision is a
  // property of one page of one list, so it is settled after the filter and
  // the slice, not over rows that are not on screen.
  const shown = filtered.slice(params.roffset, params.roffset + RELATED_PAGE);
  const titles = titlesInList(shown, (row) => ({ title: row.title, id: row.record.nodeId }));
  const page = shown.map((row, index) => ({ ...row, title: titles[index] }));
  const first = params.roffset + 1;
  const last = params.roffset + page.length;
  const totalsKnown = detail.outgoingTotal !== null && detail.incomingTotal !== null;
  const totalLine = totalsKnown
    ? `${exact((detail.outgoingTotal as number) + (detail.incomingTotal as number))} related: `
      + `${exact(detail.outgoingTotal as number)} outgoing, ${exact(detail.incomingTotal as number)} incoming`
    : `${exact(all.length)} related edges read: ${exact(base.outgoing.length)} outgoing, ${exact(base.incoming.length)} incoming`;
  const scope = [
    params.dir !== "both" ? params.dir : "",
    params.rel ? `\`${params.rel}\`` : "",
  ].filter(Boolean).join(" ");
  const countLine = page.length
    ? `${exact(first)}–${exact(last)} of ${exact(filtered.length)} ${scope ? `${scope} ` : ""}rows · ${totalLine}`
    : `0 ${scope ? `${scope} ` : ""}rows · ${totalLine}`;
  const capSentences = [
    capSentence("outgoing", base.outgoing.length, detail.outgoingTotal),
    capSentence("incoming", base.incoming.length, detail.incomingTotal),
  ].filter((sentence): sentence is string => sentence !== null)
    // Then every cap and ceiling the read itself reported, in its words.
    .concat(detail.relatedCaps.sentences);
  const directions: RelatedDirectionFilter[] = ["both", "outgoing", "incoming"];
  // "both" is the console's own word for an unfiltered direction, and it was
  // printed as the token. Every option reads as what it selects.
  const DIRECTION_LABELS: Record<RelatedDirectionFilter, string> = {
    both: "Both ways", outgoing: "Outgoing", incoming: "Incoming",
  };
  // One sentence stands for the count line only when this page is the WHOLE
  // set: no chip, no direction filter, no second page, and no cap or ceiling
  // sentence saying something was left out.
  const whole = params.rel === null && params.dir === "both"
    && page.length === allRows.length && capSentences.length === 0;
  const summary = whole ? relatedSentence(page) : null;
  return {
    chips,
    direction: params.dir,
    directions: directions.map((value) => ({ value, label: DIRECTION_LABELS[value], href: hrefFor({ dir: value }) })),
    rows: page,
    countLine,
    summary,
    // Nothing to filter with one neighbour, and nothing left to filter when
    // the sentence above the table already names the whole set -- except
    // under a filter the reader has set, where hiding the controls would
    // strand them on it.
    showControls: summary === null
      && (allRows.length > 1 || params.rel !== null || params.dir !== "both"),
    showDirection: summary === null,
    capSentences,
    firstHref: params.roffset > 0 ? hrefFor({}) : null,
    prevHref: params.roffset > 0 ? hrefFor({ roffset: Math.max(0, params.roffset - RELATED_PAGE) }) : null,
    nextHref: params.roffset + RELATED_PAGE < filtered.length ? hrefFor({ roffset: params.roffset + RELATED_PAGE }) : null,
    emptySentence: page.length === 0
      ? (allRows.length === 0
        ? "No relationship is visible on this record in this version."
        : `No ${scope.replace(/`/g, "")} relationship is visible on this record in this version.`)
      : null,
  };
}
