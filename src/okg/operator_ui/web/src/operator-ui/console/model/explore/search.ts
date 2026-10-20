// The search read and the results page: rail, rows, paging, the CLI
// equivalent and keyboard focus.
//
// What this parser reads is exactly what `compose_record_search` sends,
// measured against a real published deployment. Four per-row fields the
// earlier pass reached for -- updated, related_count, citation_url,
// has_artifact -- are not served by any published generation, and neither are
// `applied` or `facets`. They are cut rather than parsed as optional, because
// an optional parse of a field the server never sends is a control that is
// permanently blank with nothing saying why.
import { asCount, asObject, asOptionalString, asString } from "@operator-ui/console/model/explore/json";
import { type ExploreParams, exploreQuery } from "@operator-ui/console/model/explore/params";
import { type EffectiveFilters, effectiveFilters, parseQuery } from "@operator-ui/console/model/explore/query";
import { exact } from "@operator-ui/console/model/shared/format";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import { type TitledGroup, groupByDocument, recordHeading, rowTitle, titlesInList } from "@operator-ui/console/model/explore/titles";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import { type Pinned, type RecordSummary, type SearchHit, type SearchPage, SearchResponseError, parseSearchPage } from "@operator-ui/console/api/searchData";

// --- the search read ---------------------------------------------------------------

/** A result row carries nothing beyond what `parseSearchPage` reads. The
 *  alias is kept so the page's types read as Explore's own. */
export type ExploreHit = SearchHit;

/** Why a type-filtered read matched nothing, in the server's own vocabulary
 *  (`_diagnose_subtype_zero_hits`). This is the only honest answer to "I
 *  filtered by a type and got nothing"; without it the page can only say the
 *  words did not match, which for `not_indexed` is false. */
export const ZERO_HIT_REASONS = ["no_rows", "not_indexed", "not_tokenized", "no_match"] as const;
export type ZeroHitReason = (typeof ZERO_HIT_REASONS)[number];

export const ZERO_HIT_SENTENCES: Record<ZeroHitReason, string> = {
  no_rows: "no record of this type is readable in this version under this policy",
  not_indexed: "records of this type are stored without searchable text, so no words can ever match them",
  not_tokenized: "a record of this type does contain these characters, but the text index did not split them into the same words",
  no_match: "records of this type are searchable and none carries these words",
};

export interface ExploreSearchPage extends Omit<SearchPage, "results"> {
  results: ExploreHit[];
  /** The filters the read echoes back, under its own key `filters`. Never
   *  null: the composer sends it on every answer, including the zero-hit
   *  one, so a missing echo is a broken read and fails here. */
  applied: { type: string | null; source: string | null };
  /** Why a type-filtered read matched nothing, when the read diagnosed it. */
  zeroHitReason: ZeroHitReason | null;
  /** How many records of each type match the query across the whole
   *  version, where the read serves `type_counts` (sibling change
   *  console-archiv3-read-layer-fixes). Null on a read that predates it,
   *  and then the rail falls back, SAID, to the names on this page. */
  typeCounts: Facet[] | null;
  /** The deployment search profile the read ran under, the types it searched
   *  and the profile types this version does not hold (issue #2545). Null on
   *  a read that predates `skipped_types`. */
  profileScope: ProfileScope | null;
}

export interface ProfileScope {
  profile: string | null;
  /** The types the ranked query was restricted to; null when unscoped. */
  searched: string[] | null;
  /** Profile types this version's catalog does not define, left out. */
  skipped: string[];
}

function asNames(value: unknown, what: string): string[] {
  if (!Array.isArray(value)) throw new SearchResponseError(what);
  return value.map((name) => asString(name, what));
}

/** `search_profile`, `searched_types` and `skipped_types`, strictly. Absent
 *  `skipped_types` is a read that predates them; anything malformed fails. A
 *  skipped list with no searched list is a contradiction and fails too: the
 *  page could not say how much it did search. */
export function parseProfileScope(payload: Record<string, unknown>): ProfileScope | null {
  if (payload.skipped_types === undefined) return null;
  const skipped = asNames(payload.skipped_types, "skipped types");
  const searched = payload.searched_types === null || payload.searched_types === undefined
    ? null : asNames(payload.searched_types, "searched types");
  if (skipped.length && searched === null) throw new SearchResponseError("skipped types without searched types");
  return {
    profile: asOptionalString(payload.search_profile, "search profile"),
    searched,
    skipped,
  };
}

/** "Searched 99 types (the analyst profile's types and their subtypes); 25
 *  types the profile names are not in this version." The first count is the
 *  searched set, which includes each present profile type's subtypes, so the
 *  sentence says so rather than calling it the profile's own count. Null when
 *  nothing was left out. */
export function skippedTypesLine(scope: ProfileScope | null): string | null {
  if (!scope || !scope.skipped.length || scope.searched === null) return null;
  const whose = scope.profile ? `the ${scope.profile} profile's` : "the search profile's";
  const held = scope.searched.length;
  const missing = scope.skipped.length;
  const types = (n: number) => (n === 1 ? "type" : "types");
  return `Searched ${exact(held)} ${types(held)} (${whose} types and their subtypes); `
    + `${exact(missing)} ${types(missing)} the profile names ${missing === 1 ? "is" : "are"} not in this version.`;
}

/**
 * `type_counts`, as either shape a read may carry it in: a list of
 * `{type, count}` rows, or an object keyed by type. Anything else is a broken
 * read and fails here, rather than a rail that silently loses its counts.
 */
export function parseTypeCounts(value: unknown): Facet[] | null {
  if (value === undefined || value === null) return null;
  const entries: [string, number][] = Array.isArray(value)
    ? value.map((raw) => {
      const row = asObject(raw, "type count");
      return [asString(row.type, "type count type"), asCount(row.count, "type count")];
    })
    : Object.entries(asObject(value, "type counts")).map(
      ([name, count]) => [name, asCount(count, "type count")],
    );
  return entries
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([name, count]) => ({ name, count }));
}

/** #1931's parser, then the filters echo and the zero-hit diagnosis off the
 *  same payload, both strict. */
export function parseExploreSearch(envelope: unknown): Pinned<ExploreSearchPage> {
  const parsed = parseSearchPage(envelope);
  const payload = asObject(asObject(envelope, "search result").payload, "search result");
  const echo = asObject(payload.filters, "filters echo");
  const applied = {
    type: asOptionalString(echo.type, "echoed type filter"),
    source: asOptionalString(echo.source, "echoed source filter"),
  };
  const reason = payload.zero_hit_reason;
  if (reason !== undefined && reason !== null
    && !(typeof reason === "string" && (ZERO_HIT_REASONS as readonly string[]).includes(reason))) {
    // Named, not swallowed: a reason this build does not know is a server
    // saying something about the result the page would otherwise drop.
    throw new SearchResponseError(`search zero-hit reason ${JSON.stringify(reason)}`);
  }
  return {
    version: parsed.version,
    payload: {
      ...parsed.payload,
      applied,
      zeroHitReason: (reason ?? null) as ZeroHitReason | null,
      typeCounts: parseTypeCounts(payload.type_counts),
      profileScope: parseProfileScope(payload),
    },
  };
}

// --- the results page -----------------------------------------------------------------

export interface ResultRow {
  hit: ExploreHit;
  href: string;
  /** Position in the flat list the arrow keys walk. */
  index: number;
  /** What the row is CALLED: its position in its document when it is a chunk
   *  ("chunk 3"), else what the source calls the record. Never the record's
   *  hash, which said nothing about what was found. */
  title: string;
  /** "issue · source-07-git". The date and the related link are cut: no
   *  published generation carries `updated` or `related_count` on a result. */
  meta: string[];
}

export interface RailOption {
  label: string;
  value: string;
  count: number | null;
  selected: boolean;
  href: string;
}

/** The rail holds the two filters the read honours, and nothing else.
 *
 *  Cut with their controls: "sort" and "time", which the read refuses with
 *  `filter_has_no_stored_basis`, and "artifact", whose per-record flag no
 *  published generation carries. */
export interface RailSection {
  key: "type" | "source";
  title: string;
  kind: "check";
  options: RailOption[];
  /** "and 3 more" when the list is cut at eight. */
  more: number;
  /** A 12 px line under the section when its counts are not from the read. */
  note: string | null;
}

export interface PageLink {
  /** 0 is a gap ("…") between two runs of pages. */
  number: number;
  href: string | null;
  current: boolean;
}

export type Paging =
  | { kind: "pages"; pages: PageLink[]; prevHref: string | null; nextHref: string | null; note: string }
  | { kind: "more"; moreHref: string | null; note: string }
  | { kind: "none" };

export interface ResultsModel {
  filters: EffectiveFilters;
  /** The flat lists, in the read's order: what the arrow keys walk and what
   *  the copy text prints. */
  idMatches: ResultRow[];
  /** Records whose label is the query, exactly. The server lists them after
   *  the id matches and before the ranked text matches. */
  labelMatches: ResultRow[];
  textMatches: ResultRow[];
  /** The same rows as the page draws them -- consecutive chunks of one
   *  document under the document's own heading. Derived from the lists
   *  above, never a second reading of the payload. */
  idGroups: TitledGroup<ResultRow>[];
  labelGroups: TitledGroup<ResultRow>[];
  textGroups: TitledGroup<ResultRow>[];
  /** "43 results (2 ms)", or "More than 500 results (2 ms)". */
  aboutLine: string;
  rail: RailSection[];
  /** Filters the read did not honour, by name (rule 13). */
  unappliedNote: string | null;
  /** Profile types the read left out because this version lacks them: the
   *  one-line count, and the names for the expanded view (issue #2545). */
  skippedTypes: { line: string; names: string[] } | null;
  paging: Paging;
  /** The sentence an empty page shows: that nothing matches, or, on a page
   *  past the last result, that the results end before it. */
  noMatchSentence: string | null;
  /** The page is empty because it lies past the last result, not because
   *  nothing matches (issue #2789). */
  pastEnd: boolean;
  broaden: { label: string; href: string }[];
  cli: string;
}

/** How many rail options a section shows before "more". */
export const RAIL_CAP = 8;

/** A name and how many of THIS PAGE's rows carry it. Never a count over every
 *  match: the read serves no facets, so the rail counts what it can see and
 *  says so. */
export interface Facet {
  name: string;
  count: number;
}

/** `Showing 1–20 of 67`; `Showing 1–3 of 3`. When the total is unknown the
 *  read said "more than" and the line says that. */
export function showingLine(offset: number, shown: number, total: number | null, capped: boolean): string {
  if (shown === 0) return "Showing 0";
  const first = offset + 1;
  const last = offset + shown;
  if (total === null) return `Showing ${exact(first)}–${exact(last)}`;
  return `Showing ${exact(first)}–${exact(last)} of ${capped ? "more than " : ""}${exact(total)}`;
}

/** "43 results": exact when the read gives the total, "More than" when it
 *  capped the rank, "Results" when it gave none.
 *
 *  No time. "(212 ms)" was on the line an operator reads to learn how many
 *  records matched, it is the client's own round trip and not a property of
 *  the result, and nothing on the page acts on it. */
export function aboutLine(total: number | null, capped: boolean): string {
  if (total === null) return "Results";
  if (capped) return `More than ${exact(total)} results`;
  return `${exact(total)} ${total === 1 ? "result" : "results"}`;
}

/** The URL for the same query with one filter changed. A filter set by an
 *  operator in the query is changed in the query, not beside it. */
export function filterHref(
  link: LinkFn, params: ExploreParams, next: Partial<Pick<ExploreParams, "type" | "source" | "offset">>,
): string {
  const parsed = parseQuery(params.q);
  let q = params.q;
  const operator = (name: "type" | "source", value: string | null | undefined) => {
    if (value === undefined) return;
    if (parsed[name] !== null) {
      q = q.replace(new RegExp(`\\s*\\b${name}:\\S+`), "").trim();
      if (value) q = `${q} ${name}:${value}`.trim();
    }
  };
  operator("type", next.type);
  operator("source", next.source);
  const fromUrl = (name: "type" | "source") => (parsed[name] !== null ? null : (name in next ? next[name] ?? null : params[name]));
  return link("explore", exploreQuery({
    q,
    type: fromUrl("type"),
    source: fromUrl("source"),
    offset: next.offset ?? 0,
  }));
}

/** One rail section from this page's rows.
 *
 *  `complete` is the only case where a tally off the page is a tally over the
 *  whole result: the read returned every match, uncapped and unranked-capped.
 *  Otherwise the names are shown with no count, under a line saying so --
 *  never with the page's own tally passed off as the total. */
function facetSection(
  key: "type" | "source",
  title: string,
  fromResults: Facet[],
  complete: boolean,
  selected: string | null,
  link: LinkFn,
  params: ExploreParams,
): RailSection {
  const counted = complete ? fromResults : null;
  const names = fromResults;
  let options = names.map((facet) => ({
    label: facet.name,
    value: facet.name,
    count: counted ? facet.count : null,
    selected: facet.name === selected,
    href: filterHref(link, params, { [key]: facet.name === selected ? null : facet.name }),
  }));
  if (selected && !options.some((option) => option.selected)) {
    options.unshift({ label: selected, value: selected, count: counted ? 0 : null, selected: true, href: filterHref(link, params, { [key]: null }) });
  }
  const shown = options.filter((option) => option.selected);
  const rest = options.filter((option) => !option.selected);
  options = [...shown, ...rest.slice(0, Math.max(0, RAIL_CAP - shown.length))];
  return {
    key,
    title,
    kind: "check",
    options,
    more: Math.max(0, names.length + (selected && !names.some((f) => f.name === selected) ? 1 : 0) - options.length),
    // The note that used to say "counts are not in this read; the names are
    // the ones on this page" is cut: it appeared under BOTH sections of
    // nearly every read, and what it describes -- an absent count -- the
    // reader can see, because there is no number beside the name. The fact
    // is recorded in ui/cuts.md.
    note: null,
  };
}

/** Why this read matched nothing.
 *
 *  When the server diagnosed a type-filtered miss it says which of four
 *  conditions fired, and that is added rather than left to the reader to
 *  guess. `not_indexed` in particular means no wording could ever have
 *  matched, which "no record matches these words" states as a falsehood. */
/** The most of a query the page echoes back. The rest is cut with an
 *  ellipsis; the box still holds all of it. */
export const QUERY_ECHO_CHARS = 200;

export function echoQuery(query: string): string {
  return query.length > QUERY_ECHO_CHARS ? `${query.slice(0, QUERY_ECHO_CHARS)}\u2026` : query;
}

export function noMatchSentence(page: ExploreSearchPage, scope: string, version: string): string {
  const head = `No record matches “${echoQuery(page.query)}”${scope ? ` ${scope}` : ""} in version ${shortVersion(version)}.`;
  if (!page.zeroHitReason) return head;
  return `${head} The read says why: ${ZERO_HIT_SENTENCES[page.zeroHitReason]}.`;
}

/** What an empty page past the first says: the results end before it. */
export function pastEndSentence(page: ExploreSearchPage, scope: string, version: string): string {
  return `The results for “${echoQuery(page.query)}”${scope ? ` ${scope}` : ""} in version ${shortVersion(version)} end before this page.`;
}

export function buildResults(
  page: ExploreSearchPage,
  params: ExploreParams,
  link: LinkFn,
  deployment: string,
  version: string,
  pageSize: number,
): ResultsModel {
  const filters = effectiveFilters(params);
  const open = (hit: RecordSummary) => link("explore", exploreQuery({ q: params.q, type: params.type, record: hit.nodeId }));
  // A row's title is settled against the whole result list, because that is
  // what the reader scans. Rows under a document heading are scoped by it: a
  // "chunk 1" in one file and a "chunk 1" in another do not read the same.
  const titles = titlesInList(page.results, (hit) => ({
    title: rowTitle(hit), id: hit.nodeId, scope: recordHeading(hit),
  }));
  const ranked: ResultRow[] = page.results.map((hit, index) => ({
    hit,
    href: open(hit),
    index,
    title: titles[index],
    // Two facts, both on the row. No date: no published generation records a
    // per-record date, which is the same absence that refuses `sort`.
    meta: [
      hit.type,
      hit.source ?? "no source recorded",
    ],
  }));
  // Grouped by document ACROSS the page, then numbered in the order they are
  // drawn: the arrow keys walk `index`, and a list walked in rank order while
  // it is drawn in group order jumps around the screen.
  // The three runs the server orders: the record id or `<type>:<query>` is
  // the query, then the label is the query, then the ranked text matches.
  // Every kind the parser admits lands in exactly one run.
  const idGroups = groupByDocument(ranked.filter((row) => row.hit.match === "record_id" || row.hit.match === "typed_record_id"));
  const labelGroups = groupByDocument(ranked.filter((row) => row.hit.match === "label"));
  const textGroups = groupByDocument(ranked.filter((row) => row.hit.match === "text"));
  let drawn = 0;
  const renumber = (groups: TitledGroup<ResultRow>[]) => groups.map((group) => ({
    ...group,
    rows: group.rows.map((row) => ({ ...row, index: drawn++ })),
  }));
  const idDrawn = renumber(idGroups);
  const labelDrawn = renumber(labelGroups);
  const textDrawn = renumber(textGroups);
  const idMatches = idDrawn.flatMap((group) => group.rows);
  const labelMatches = labelDrawn.flatMap((group) => group.rows);
  const textMatches = textDrawn.flatMap((group) => group.rows);
  const rows = [...idMatches, ...labelMatches, ...textMatches];
  const complete = page.totalCount !== null
    && page.totalCount === page.results.length
    && !page.truncated
    && !page.rankCapped;
  const tally = (pick: (hit: ExploreHit) => string | null): Facet[] => {
    const counts = new Map<string, number>();
    for (const hit of page.results) { const key = pick(hit); if (key) counts.set(key, (counts.get(key) ?? 0) + 1); }
    return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).map(([name, count]) => ({ name, count }));
  };
  // Two sections. Sort, Updated and Has artifact are cut with their reads:
  // the first two are refused by the server, the third has no per-record flag.
  // A section with ONE name is not a filter: every row on the page already
  // carries it, so clicking it changes nothing and un-clicking it is the
  // page you are on. It is dropped unless that one name is the filter in
  // force, where it is the way back out.
  //
  // The TYPE section is the exception, and is never dropped (review
  // 2026-09-22): a page of twenty document chunks from a deployment that
  // also matches jira issues showed no type rail at all, so the reader had
  // no way to learn the other types existed. It is drawn from `type_counts`
  // -- the whole version's matches, counted -- where the read serves it, and
  // from the page's own rows otherwise, where one name is still the fact
  // that the page holds one type.
  const typeSection = page.typeCounts
    ? facetSection("type", "Type", page.typeCounts, true, filters.type, link, params)
    : facetSection("type", "Type", tally((hit) => hit.type), complete, filters.type, link, params);
  const sourceSection = facetSection("source", "Source", tally((hit) => hit.source), complete, filters.source, link, params);
  const rail: RailSection[] = [
    ...(typeSection.options.length ? [typeSection] : []),
    ...(sourceSection.options.length > 1 || sourceSection.options.some((option) => option.selected)
      ? [sourceSection] : []),
  ];

  // Rule 13: a filter the read did not apply is named, never assumed. The
  // read echoes back what it honoured under `filters`, so this compares what
  // was asked against what came back rather than trusting the request.
  const unapplied: string[] = [];
  if (filters.type && page.applied.type !== filters.type) unapplied.push(`type ${filters.type}`);
  if (filters.source && page.applied.source !== filters.source) unapplied.push(`source ${filters.source}`);

  const skippedLine = skippedTypesLine(page.profileScope);
  const total = page.totalCount;
  // Pages are counted from the rows later pages hold, never from `total`:
  // the total also counts the exact rows, which only the first page lists,
  // so 21 matches with one label match fill ONE page, and a count taken
  // from the total offered a second one that came back empty (issue #2789).
  // A read that does not serve the paged count gets the next link its own
  // `next_offset` vouches for, and no numbered pages.
  const paged = page.pagedCount;
  let paging: Paging = { kind: "none" };
  if (paged !== null && !page.rankCapped && paged > pageSize) {
    const count = Math.ceil(paged / pageSize);
    const current = Math.floor(params.offset / pageSize) + 1;
    const wanted = new Set([1, count, current - 1, current, current + 1, current + 2].filter((n) => n >= 1 && n <= count));
    const pages: PageLink[] = [];
    let previous = 0;
    for (const number of [...wanted].sort((a, b) => a - b)) {
      if (number - previous > 1) pages.push({ number: 0, href: null, current: false });
      pages.push({ number, href: filterHref(link, params, { offset: (number - 1) * pageSize }), current: number === current });
      previous = number;
    }
    paging = {
      kind: "pages",
      pages,
      prevHref: current > 1 ? filterHref(link, params, { offset: (current - 2) * pageSize }) : null,
      nextHref: current < count ? filterHref(link, params, { offset: current * pageSize }) : null,
      note: "",
    };
  } else if (page.nextOffset !== null) {
    paging = {
      kind: "more",
      moreHref: filterHref(link, params, { offset: page.nextOffset }),
      note: "",
    };
  } else if (params.offset > 0) {
    paging = { kind: "more", moreHref: null, note: "the last of the results" };
  }

  const scope = [
    filters.type ? `in ${filters.type}` : "",
    filters.source ? `from ${filters.source}` : "",
  ].filter(Boolean).join(", ");
  // An empty page past the first, on a read that did not say nothing
  // matches, is past the last result: a stale or hand-typed offset. "No
  // record matches" is false there (issue #2789), so it says where the
  // results end and links back to their first page. A read that diagnosed
  // a zero-hit keeps its diagnosis: that sentence says why nothing matched.
  const pastEnd = rows.length === 0 && params.offset > 0 && total !== 0 && page.zeroHitReason === null;
  const broaden: { label: string; href: string }[] = [];
  if (pastEnd) {
    broaden.push({ label: "the first page", href: filterHref(link, params, { offset: 0 }) });
  } else if (rows.length === 0) {
    if (filters.source) broaden.push({ label: "every source", href: filterHref(link, params, { source: null }) });
    if (filters.type) broaden.push({ label: "all types", href: filterHref(link, params, { type: null }) });
    if (filters.parsed.phrases.length) {
      broaden.push({ label: "the words without the quotes", href: link("explore", exploreQuery({ ...params, q: params.q.replace(/"/g, ""), offset: 0 })) });
    }
    if (filters.parsed.id) {
      broaden.push({ label: `text search for ${filters.parsed.id}`, href: link("explore", exploreQuery({ ...params, q: filters.parsed.id, offset: 0 })) });
    }
    if (filters.parsed.words.length > 1) {
      const fewer = [...filters.parsed.phrases.map((phrase) => `"${phrase}"`), ...filters.parsed.words.slice(0, -1)].join(" ");
      broaden.push({ label: `fewer words: ${fewer}`, href: link("explore", exploreQuery({ q: fewer, offset: 0 })) });
    }
    // Nothing left to drop: the types themselves are the way in.
    broaden.push({ label: "browsing the types", href: link("explore") });
  }

  return {
    filters,
    idMatches,
    labelMatches,
    textMatches,
    idGroups: idDrawn,
    labelGroups: labelDrawn,
    textGroups: textDrawn,
    aboutLine: aboutLine(total, page.rankCapped),
    rail,
    unappliedNote: unapplied.length
      ? `This read did not apply ${unapplied.join(", ")}: the results are as the read returned them.`
      : null,
    skippedTypes: skippedLine && page.profileScope
      ? { line: skippedLine, names: page.profileScope.skipped }
      : null,
    paging,
    noMatchSentence: pastEnd
      ? pastEndSentence(page, scope, version)
      : rows.length === 0 ? noMatchSentence(page, scope, version) : null,
    pastEnd,
    broaden: broaden.slice(0, 2),
    cli: cliSearch(deployment, page.query, filters.type, version, pageSize, params.offset),
  };
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

/** The same page of text matches from the CLI. #1931's command, unchanged:
 *  the CLI has no exact id lookup and reads under its own policy identity. */
export function cliSearch(
  deployment: string, query: string, type: string | null, version: string, limit: number, offset: number,
): string {
  return [
    "okg search",
    `--deployment ${shellQuote(deployment)}`,
    `--query ${shellQuote(query)}`,
    ...(type ? [`--subtype ${shellQuote(type)}`] : []),
    `--generation-id ${shellQuote(version)}`,
    `--limit ${limit}`,
    ...(offset > 0 ? [`--offset ${offset}`] : []),
    "--json",
  ].join(" ");
}

/** The DOM id of the result row at `index`, in drawn order. */
export function resultRowId(index: number): string {
  return `explore-result-${index}`;
}

/** The keyboard: ArrowDown and ArrowUp walk the rows, never past either end;
 *  Home and End jump; Escape clears the focus (the box's text is cleared by
 *  the page). Null is "no row focused", which is where the page opens. */
export function moveFocus(current: number | null, count: number, key: string): number | null {
  if (count === 0) return null;
  if (key === "ArrowDown") return current === null ? 0 : Math.min(count - 1, current + 1);
  if (key === "ArrowUp") return current === null || current === 0 ? null : current - 1;
  if (key === "Home") return 0;
  if (key === "End") return count - 1;
  if (key === "Escape") return null;
  return current;
}
