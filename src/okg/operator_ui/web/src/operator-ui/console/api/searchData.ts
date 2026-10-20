/**
 * Record read data: response shapes, strict parsers, and display helpers
 * (PACT operator-ui-search-view; moved here for the console's Explore view).
 *
 * Parsers throw on a malformed response instead of filling in defaults, so a
 * broken read shows as a failure rather than as an empty result.
 */

export interface PublishedVersion {
  generationId: string;
  publishedAt: string | null;
}

export interface VersionList {
  deployment: string;
  versions: PublishedVersion[];
  latest: string | null;
  truncated: boolean;
}

export interface RecordTypeCount {
  type: string;
  count: number;
}

/**
 * What the SOURCE calls this record.
 *
 * `source_record_id` is the source's own identity for the record, and its
 * shape is the source's: the twiki-eos reader sends `{path, chunk_index}`,
 * the git reader `{path, repo}`. Four keys are read, each optional, because a
 * source that records none of them is not a broken read -- it is a source
 * whose records have no path. A record with no path keeps the label the
 * server served, which is what every row used to show.
 */
export interface RecordOrigin {
  /** The file the record came out of: "HcalIsoTrackValidationSim.txt". */
  path: string | null;
  /** A title or heading the source recorded, when it records one. */
  title: string | null;
  /** This chunk's position among its document's chunks, counted from 0. */
  chunkIndex: number | null;
  /** How many chunks the parent has, when the read says. Usually absent on a
   *  search row, which is why a position is printed as "chunk 3" and not as
   *  "chunk 3 of 9" unless the count is really there. */
  chunkCount: number | null;
}

export const NO_ORIGIN: RecordOrigin = { path: null, title: null, chunkIndex: null, chunkCount: null };

export interface RecordSummary {
  nodeId: string;
  type: string;
  label: string;
  source: string | null;
  origin: RecordOrigin;
}

/** Why a row is on the page, exactly the set `compose_record_search` sends
 *  (src/okg/substrate/console_views/records.py). The first three are the exact
 *  tiers, listed before the ranked text matches: the record id is the query,
 *  the id is `<type>:<query>`, or the label is the query. A kind outside this
 *  set is a read this build cannot place, and fails the page. */
export const SEARCH_MATCH_KINDS = ["record_id", "typed_record_id", "label", "text"] as const;
export type SearchMatch = (typeof SEARCH_MATCH_KINDS)[number];

function isSearchMatch(value: string): value is SearchMatch {
  return (SEARCH_MATCH_KINDS as readonly string[]).includes(value);
}

export interface SearchHit extends RecordSummary {
  match: SearchMatch;
  excerpt: string | null;
}

export interface SearchPage {
  query: string;
  type: string | null;
  results: SearchHit[];
  exactMatch: boolean;
  /** The exact match is also one of this page's text matches, listed once. */
  exactMatchInText: boolean;
  totalCount: number | null;
  /** How many rows the pages walk: the ranked text matches less the exact
   *  rows, which the first page alone lists. Page k holds rows
   *  (k-1)*limit to k*limit of them. `totalCount` counts the exact rows as
   *  well, so a page count taken from it can offer an empty last page
   *  (issue #2789). Null on a read that does not serve it. */
  pagedCount: number | null;
  truncated: boolean;
  nextOffset: number | null;
  rankCapped: boolean;
}

export interface ListPage {
  type: string;
  records: RecordSummary[];
  /** Position of the next page among the type's records, counted before access rules. */
  nextOffset: number | null;
  shortPage: boolean;
}

export interface RecordField {
  key: string;
  value: string | null;
  hidden: boolean;
  /** The value is the record's own title, written again under this key. */
  echoesTitle: boolean;
}

export interface RecordSection {
  key: string;
  title: string;
  /** The text, or null when the source policy hid it. */
  value: string | null;
  hidden: boolean;
}

export interface RelatedRecord {
  relationship: string;
  nodeId: string;
  type: string;
  label: string;
  /** What the SOURCE calls the neighbour, on the rows that record it. A
   *  chunk's own label is its hash, so a document's Related tab named none
   *  of its chunks until the read started serving this. */
  origin: RecordOrigin;
}

export interface FoundRecord {
  found: true;
  nodeId: string;
  type: string;
  label: string;
  source: string | null;
  sourceRecordId: unknown;
  sourceRevision: unknown;
  policyRedacted: boolean;
  fields: RecordField[];
  sections: RecordSection[];
  outgoing: RelatedRecord[];
  incoming: RelatedRecord[];
  relatedCap: number;
  outgoingCapped: boolean;
  incomingCapped: boolean;
}

export interface MissingRecord {
  found: false;
  nodeId: string;
}

export type RecordDetail = FoundRecord | MissingRecord;

export interface Pinned<T> {
  version: string;
  payload: T;
}

type Json = Record<string, unknown>;

export class SearchResponseError extends Error {
  constructor(what: string) {
    super(`The server returned an unreadable ${what}.`);
    this.name = "SearchResponseError";
  }
}

function asObject(value: unknown, what: string): Json {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new SearchResponseError(what);
  }
  return value as Json;
}

function asArray(value: unknown, what: string): unknown[] {
  if (!Array.isArray(value)) throw new SearchResponseError(what);
  return value;
}

function asString(value: unknown, what: string): string {
  if (typeof value !== "string") throw new SearchResponseError(what);
  return value;
}

function asOptionalString(value: unknown, what: string): string | null {
  if (value === null || value === undefined) return null;
  return asString(value, what);
}

function asBoolean(value: unknown, what: string): boolean {
  if (typeof value !== "boolean") throw new SearchResponseError(what);
  return value;
}

function asOptionalNumber(value: unknown, what: string): number | null {
  if (value === null || value === undefined) return null;
  return asNumber(value, what);
}

function asNumber(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) throw new SearchResponseError(what);
  return value;
}

/** A page position: absent, or a whole number that is not negative. */
function asOptionalPosition(value: unknown, what: string): number | null {
  const position = asOptionalNumber(value, what);
  if (position !== null && (!Number.isInteger(position) || position < 0)) throw new SearchResponseError(what);
  return position;
}

function pinned<T>(envelope: unknown, what: string, parse: (payload: Json) => T): Pinned<T> {
  const body = asObject(envelope, what);
  return {
    version: asString(body.generation_id, `${what} version`),
    payload: parse(asObject(body.payload, what)),
  };
}

export function parseVersionList(envelope: unknown): VersionList {
  const payload = asObject(asObject(envelope, "version list").payload, "version list");
  return {
    deployment: asString(payload.deployment, "version list deployment"),
    versions: asArray(payload.versions, "version list").map((raw) => {
      const row = asObject(raw, "version");
      return {
        generationId: asString(row.generation_id, "version id"),
        publishedAt: asOptionalString(row.published_at, "version time"),
      };
    }),
    latest: asOptionalString(payload.latest, "latest version"),
    truncated: asBoolean(payload.truncated, "version list cap"),
  };
}

export function parseRecordTypes(envelope: unknown): Pinned<RecordTypeCount[]> {
  return pinned(envelope, "record type list", (payload) =>
    asArray(payload.types, "record type list").map((raw) => {
      const row = asObject(raw, "record type");
      return { type: asString(row.type, "record type"), count: asNumber(row.count, "record type count") };
    }));
}

/** The source identity off a row, as far as it goes.
 *
 * A key whose value is the wrong type is dropped rather than refused: this is
 * the source's own free-form identity, and a reader that sends a number where
 * this build expects a string must not take down a page of results. What is
 * absent is absent -- nothing is substituted, and the row falls back to the
 * label the server served. */
export function parseOrigin(raw: unknown): RecordOrigin {
  if (raw === null || raw === undefined || typeof raw !== "object" || Array.isArray(raw)) return NO_ORIGIN;
  const row = raw as Record<string, unknown>;
  const text = (value: unknown) => (typeof value === "string" && value.trim() ? value : null);
  const position = (value: unknown) => (typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : null);
  return {
    path: text(row.path),
    title: text(row.title) ?? text(row.heading),
    chunkIndex: position(row.chunk_index),
    chunkCount: position(row.chunk_count),
  };
}

function parseSummary(raw: unknown, what: string): RecordSummary {
  const row = asObject(raw, what);
  return {
    nodeId: asString(row.node_id, `${what} ID`),
    type: asString(row.type, `${what} type`),
    label: asString(row.label, `${what} label`),
    source: asOptionalString(row.source, `${what} source`),
    origin: parseOrigin(row.source_record_id),
  };
}

export function parseSearchPage(envelope: unknown): Pinned<SearchPage> {
  return pinned(envelope, "search result", (payload) => ({
    query: asString(payload.query, "search query"),
    type: asOptionalString(payload.type, "search type"),
    results: asArray(payload.results, "search results").map((raw) => {
      const row = asObject(raw, "search result");
      const match = asString(row.match, "search match");
      if (!isSearchMatch(match)) throw new SearchResponseError("search match");
      return {
        ...parseSummary(row, "search result"),
        match,
        excerpt: asOptionalString(row.excerpt, "search excerpt"),
      };
    }),
    exactMatch: asBoolean(payload.exact_match, "exact match flag"),
    exactMatchInText: asBoolean(payload.exact_match_in_text, "exact match text flag"),
    totalCount: asOptionalNumber(payload.total_count, "search total"),
    pagedCount: asOptionalPosition(payload.paged_count, "search paged total"),
    truncated: asBoolean(payload.truncated, "search cap"),
    nextOffset: asOptionalPosition(payload.next_offset, "search next page"),
    rankCapped: asBoolean(payload.rank_capped, "search rank cap"),
  }));
}

export function parseListPage(envelope: unknown): Pinned<ListPage> {
  return pinned(envelope, "record list", (payload) => ({
    type: asString(payload.type, "record list type"),
    records: asArray(payload.records, "record list").map((raw) => parseSummary(raw, "record")),
    nextOffset: asOptionalPosition(payload.next_offset, "record list next page"),
    shortPage: asBoolean(payload.short_page, "record list short page"),
  }));
}

function parseRelated(raw: unknown): RelatedRecord {
  const row = asObject(raw, "related record");
  return {
    relationship: asString(row.relationship, "relationship"),
    nodeId: asString(row.node_id, "related record ID"),
    type: asString(row.type, "related record type"),
    label: asString(row.label, "related record label"),
    // Absent on a neighbour whose source recorded no identity, which is a
    // row with no path rather than a broken read: `parseOrigin` answers
    // NO_ORIGIN and the row keeps the label the server served.
    origin: parseOrigin(row.source_record_id),
  };
}

export function parseRecordDetail(envelope: unknown): Pinned<RecordDetail> {
  return pinned(envelope, "record", (payload) => {
    const found = asBoolean(payload.found, "record found flag");
    const nodeId = asString(payload.node_id, "record ID");
    if (!found) return { found: false, nodeId };
    const related = asObject(payload.related, "related records");
    return {
      found: true,
      nodeId,
      type: asString(payload.type, "record type"),
      label: asString(payload.label, "record label"),
      source: asOptionalString(payload.source, "record source"),
      sourceRecordId: payload.source_record_id ?? null,
      sourceRevision: payload.source_revision ?? null,
      policyRedacted: asBoolean(payload.policy_redacted, "record redaction flag"),
      fields: asArray(payload.fields, "record fields").map((raw) => {
        const row = asObject(raw, "record field");
        const hidden = asBoolean(row.hidden, "field hidden flag");
        return {
          key: asString(row.key, "field name"),
          value: hidden ? null : asString(row.value, "field value"),
          hidden,
          echoesTitle: asBoolean(row.echoes_title, "field title-echo flag"),
        };
      }),
      sections: asArray(payload.sections, "record content").map((raw) => {
        const row = asObject(raw, "record content");
        const hidden = asBoolean(row.hidden, "content hidden flag");
        return {
          key: asString(row.key, "content key"),
          title: asString(row.title, "content title"),
          value: hidden ? null : asString(row.value, "content text"),
          hidden,
        };
      }),
      outgoing: asArray(related.outgoing, "related records").map(parseRelated),
      incoming: asArray(related.incoming, "related records").map(parseRelated),
      relatedCap: asNumber(related.cap, "related record cap"),
      outgoingCapped: asBoolean(related.outgoing_capped, "related record cap flag"),
      incomingCapped: asBoolean(related.incoming_capped, "related record cap flag"),
    };
  });
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

/** The short form of a version ID used everywhere in the UI: its final 12 characters. */
export function shortVersion(generationId: string): string {
  const tail = generationId.split(":").pop() ?? generationId;
  return tail.length === 12 ? tail : generationId.slice(-12);
}

export function typeLabel(type: string): string {
  return type.replace(/_/g, " ");
}

/** A source ID without the deployment prefix the server adds to producer names. */
export function sourceLabel(source: string | null, deployment: string | null): string {
  if (!source) return "No source recorded";
  if (deployment && source.startsWith(`${deployment}.`)) return source.slice(deployment.length + 1);
  return source;
}

export function formatUtc(iso: string | null): string {
  if (!iso) return "time not recorded";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "time not recorded";
  return `${date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC",
  })} UTC`;
}

export type IdentityDisplay =
  | { kind: "none" }
  | { kind: "pseudonymized"; value: string }
  | { kind: "value"; value: string };

/** How a stored source record ID or revision reads to an operator. */
export function describeIdentity(value: unknown): IdentityDisplay {
  if (value === null || value === undefined) return { kind: "none" };
  if (typeof value === "string" || typeof value === "number") return { kind: "value", value: String(value) };
  if (typeof value === "object" && !Array.isArray(value)) {
    const entries = Object.entries(value as Json);
    if (entries.length === 0) return { kind: "none" };
    const pseudonym = (value as Json).nomos_pseudonym;
    if (entries.length === 1 && typeof pseudonym === "string") {
      return { kind: "pseudonymized", value: pseudonym };
    }
    if (entries.length === 1) {
      const [, only] = entries[0];
      if (typeof only === "string" || typeof only === "number") return { kind: "value", value: String(only) };
    }
  }
  return { kind: "value", value: JSON.stringify(value) };
}

/** The token the source policy writes in place of a hidden value. */
export const POLICY_MASK_TOKEN = "[nomos:masked]";

export type TextPart = { kind: "text"; value: string } | { kind: "hidden" };

/** Split text around policy-hidden values so each can be shown as hidden. */
export function splitMaskedText(value: string): TextPart[] {
  const parts: TextPart[] = [];
  const chunks = value.split(POLICY_MASK_TOKEN);
  chunks.forEach((chunk, index) => {
    if (chunk) parts.push({ kind: "text", value: chunk });
    if (index < chunks.length - 1) parts.push({ kind: "hidden" });
  });
  return parts;
}

// A cut-off piece of a hidden-value token is recognised only from this many
// characters, so ordinary text such as "[a" is not mistaken for one.
const PARTIAL_TOKEN_MIN = 4;

/**
 * A search excerpt that starts at a word boundary rather than mid-word, and
 * whose cut-off ends do not show part of a hidden-value token as text.
 */
export function tidyExcerpt(excerpt: string): string {
  let text = excerpt.trim();
  if (text.startsWith("...")) {
    const rest = text.slice(3);
    const tokenEnd = rest.indexOf("]") + 1;
    const head = rest.slice(0, tokenEnd);
    if (tokenEnd >= PARTIAL_TOKEN_MIN && tokenEnd < POLICY_MASK_TOKEN.length && POLICY_MASK_TOKEN.endsWith(head)) {
      text = `…${POLICY_MASK_TOKEN}${rest.slice(tokenEnd)}`;
    } else {
      // The excerpt is cut at a character count, so its first word is usually partial.
      const firstSpace = rest.search(/\s/);
      text = firstSpace === -1 ? `…${rest}` : `…${rest.slice(firstSpace + 1)}`;
    }
  }
  if (text.endsWith("...")) {
    text = `${closePartialToken(text.slice(0, -3))}…`;
  }
  return text;
}

/** Text ending partway through a hidden-value token, with that token completed. */
function closePartialToken(text: string): string {
  const cut = text.lastIndexOf("[");
  const tail = cut === -1 ? "" : text.slice(cut);
  const partial = tail.length >= PARTIAL_TOKEN_MIN && tail.length < POLICY_MASK_TOKEN.length && POLICY_MASK_TOKEN.startsWith(tail);
  return partial ? `${text.slice(0, cut)}${POLICY_MASK_TOKEN}` : text;
}

/** The first `limit` characters of text, never ending inside a hidden-value token. */
export function truncateText(text: string, limit: number): string {
  if (text.length <= limit) return text;
  const start = text.lastIndexOf(POLICY_MASK_TOKEN, limit - 1);
  const insideToken = start !== -1 && start + POLICY_MASK_TOKEN.length > limit;
  return text.slice(0, insideToken ? start + POLICY_MASK_TOKEN.length : limit);
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

/**
 * The CLI search for the same page of text matches from the same version.
 * The CLI has no exact record ID lookup, so an exact ID match shown by this
 * view is not part of its output, and it reads under the CLI's own policy
 * identity.
 */
export function cliSearchCommand(request: {
  deployment: string;
  query: string;
  type: string | null;
  version: string;
  limit: number;
  offset: number;
}): string {
  return [
    "okg search",
    `--deployment ${shellQuote(request.deployment)}`,
    `--query ${shellQuote(request.query)}`,
    ...(request.type ? [`--subtype ${shellQuote(request.type)}`] : []),
    `--generation-id ${shellQuote(request.version)}`,
    `--limit ${request.limit}`,
    ...(request.offset > 0 ? [`--offset ${request.offset}`] : []),
    "--json",
  ].join(" ");
}

export function groupRelated(records: RelatedRecord[]): Array<{ relationship: string; records: RelatedRecord[] }> {
  const groups = new Map<string, RelatedRecord[]>();
  for (const record of records) {
    const group = groups.get(record.relationship);
    if (group) group.push(record);
    else groups.set(record.relationship, [record]);
  }
  return [...groups.entries()].map(([relationship, grouped]) => ({ relationship, records: grouped }));
}
