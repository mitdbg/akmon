// Explore's landing: the header line, the type chips and the listing of one
// type's records.
import { type ExploreParams, exploreQuery } from "@operator-ui/console/model/explore/params";
import { exact } from "@operator-ui/console/model/shared/format";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { RecordSummary, RecordTypeCount, SearchPage } from "@operator-ui/console/api/searchData";

// --- the header --------------------------------------------------------------

/** "2,605 records · 6 types · version eea4e0d4e88d". The record count is the
 *  sum of the type counts the read carries, before access rules. */
export function contentsLine(types: RecordTypeCount[], version: string): string {
  const records = types.reduce((sum, row) => sum + row.count, 0);
  return `${exact(records)} ${records === 1 ? "record" : "records"} · `
    + `${exact(types.length)} ${types.length === 1 ? "type" : "types"} · `
    + `version ${shortVersion(version)}`;
}

/** No read says which types carry searchable text (a backend gap the ledger
 *  records), so the line states what is true of every type. */
// Plain words. It used to read "Text search covers records with searchable
// text; every type is reachable by its exact record id or from its chip",
// which says "text" three times and describes the reader's options in the
// passive.
export const SEARCHABLE_LINE = "Type and press Enter to search the records that store text. "
  + "Any record also opens by its exact id, and every type by its chip below.";

export function nothingToSearchSentence(deployment: string): string {
  return `No version has been published for ${deployment}; there is nothing to search yet.`;
}

// --- the type chips ----------------------------------------------------------

/** How many chips the landing shows before "and N more types". 214 types on
 *  scale-200 must bound themselves; two rows of chips at 960 px is about this. */
export const CHIP_CAP = 24;

export interface TypeChip {
  type: string;
  /** The count printed on the chip, or null when no read carries one for
   *  this query. */
  count: number | null;
  selected: boolean;
  href: string;
}

export interface ChipStrip {
  chips: TypeChip[];
  /** Types past the cap, and the records they hold. */
  moreTypes: number;
  moreRecords: number;
  /** True when the counts are per-query tallies rather than the version's. */
  perQuery: boolean;
  /** Printed when the query's counts could not be tallied exactly. */
  countNote: string | null;
}

/**
 * The chips over the landing or the results.
 *
 * With no query the counts are the version's, ordered by count. With a query
 * they move with it: tallied from the results when the read returned every
 * match (total equals the rows, nothing truncated or rank-capped), and left
 * off when it did not -- a count off one page of twenty is a floor, and a
 * floor printed as a count is the misstatement #1931 was corrected for. No
 * read carries facet counts (a backend gap the ledger records).
 */
export function buildChips(
  types: RecordTypeCount[],
  params: ExploreParams,
  link: LinkFn,
  results: SearchPage | null,
  showAll = false,
): ChipStrip {
  const href = (type: string | null) => link("explore", exploreQuery({ q: params.q, type }));
  const ordered = [...types].sort((a, b) => b.count - a.count || a.type.localeCompare(b.type));
  if (!params.q || !results) {
    const shown = showAll ? ordered : ordered.slice(0, CHIP_CAP);
    const rest = ordered.slice(shown.length);
    // The selected type stays visible past the cap.
    if (params.type && !shown.some((row) => row.type === params.type)) {
      const selected = ordered.find((row) => row.type === params.type);
      if (selected) shown.push(selected);
    }
    return {
      chips: shown.map((row) => ({
        type: row.type,
        count: row.count,
        selected: row.type === params.type,
        href: href(row.type === params.type ? null : row.type),
      })),
      moreTypes: rest.length,
      moreRecords: rest.reduce((sum, row) => sum + row.count, 0),
      perQuery: false,
      countNote: null,
    };
  }
  const complete = results.totalCount !== null
    && results.totalCount === results.results.length
    && !results.truncated
    && !results.rankCapped
    && results.type === null;
  const tally = new Map<string, number>();
  for (const hit of results.results) tally.set(hit.type, (tally.get(hit.type) ?? 0) + 1);
  if (params.type && !tally.has(params.type)) tally.set(params.type, 0);
  const chips = [...tally.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([type, count]) => ({
      type,
      count: complete ? count : null,
      selected: type === params.type,
      href: href(type === params.type ? null : type),
    }));
  return {
    chips,
    moreTypes: 0,
    moreRecords: 0,
    perQuery: true,
    countNote: complete
      ? null
      : "Counts per type for this query are not in the search read; the chips name the types on this page.",
  };
}

// --- the listing ------------------------------------------------------------

export interface ListingModel {
  type: string;
  /** "Records of type document · 96 in this version, in id order". */
  countLine: string;
  rows: { summary: RecordSummary; href: string }[];
  emptySentence: string | null;
  shortPageNote: string | null;
  firstHref: string | null;
  nextHref: string | null;
}

export function buildListing(
  page: { type: string; records: RecordSummary[]; nextOffset: number | null; shortPage: boolean },
  types: RecordTypeCount[],
  params: ExploreParams,
  link: LinkFn,
): ListingModel {
  const total = types.find((row) => row.type === page.type)?.count ?? null;
  const first = params.offset + 1;
  const last = params.offset + page.records.length;
  const range = page.records.length ? `${exact(first)}–${exact(last)}` : "0";
  return {
    type: page.type,
    countLine: total === null
      ? `Records of type ${page.type} · ${range}, in id order`
      : `Records of type ${page.type} · ${range} of ${exact(total)}, in id order`,
    rows: page.records.map((summary) => ({
      summary,
      href: link("explore", exploreQuery({ type: params.type, record: summary.nodeId })),
    })),
    emptySentence: page.records.length === 0
      ? (params.offset > 0
        ? `No more ${page.type} records are visible in this version.`
        // A type the version's own type list does not hold is not a type
        // with no visible records: it is no type at all (a typo, or a link
        // from another deployment). The list is the version's, so it can
        // say so; an empty listing of a real type keeps the old sentence.
        : types.length > 0 && !types.some((row) => row.type === page.type)
          ? `There is no record type called ${page.type} in this version; the types it holds are listed above.`
          : `No ${page.type} record is visible in this version.`)
      : null,
    shortPageNote: page.shortPage && page.nextOffset !== null
      ? "The access policy hides some records on this page; the next page may hold more."
      : null,
    firstHref: params.offset > 0 ? link("explore", exploreQuery({ type: params.type })) : null,
    nextHref: page.nextOffset !== null
      ? link("explore", exploreQuery({ type: params.type, offset: page.nextOffset }))
      : null,
  };
}
