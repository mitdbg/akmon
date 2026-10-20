// Explore's URL: the parameters the search and the record page share, and how
// they round-trip through the hash.
import { looksLikeVersionId } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";

// --- the URL --------------------------------------------------------------

// Content first, and it is the default: an operator who opens a record wants
// what the record SAYS. Fields is the plumbing under it -- the source keys and
// their values -- so it sits after Related, which is the other thing a reader
// follows out of a record.
export const RECORD_TABS = ["content", "related", "fields", "provenance", "raw"] as const;
export type RecordTab = (typeof RECORD_TABS)[number];

// --- the parameters this read cannot honour ---------------------------------

/**
 * `sort` and `since` were Explore's rail controls until the live read was
 * measured. The server refuses both with HTTP 422 and
 * `filter_has_no_stored_basis`: the published graph stores no per-record date,
 * so there is nothing to order or cut by.
 *
 * They are still parsed, because old bookmarks and chat deep links carry them.
 * Neither is sent to the server -- a read carrying one fails outright -- and
 * neither is dropped in silence: `ignoredParams` names each one it found, and
 * the page prints that line. Dropping them quietly would answer a reader's
 * "newest first" bookmark with relevance order and no word about it.
 */
export const IGNORED_PARAMS = ["sort", "since"] as const;
export type IgnoredParamName = (typeof IGNORED_PARAMS)[number] | "version";

export const NO_STORED_DATE = "the published graph stores no per-record date";
export const NOT_A_VERSION = "it is not a published version id";

export interface IgnoredParam {
  name: IgnoredParamName;
  /** As the URL spelled it, so the line quotes the reader's own bookmark. */
  value: string;
  why: string;
  /** False where printing the value would put it somewhere the reader never
   *  had it. `?version=` is whatever a reader or an earlier link put on the
   *  URL, and on 2026-09-22 that was a PostgreSQL DSN, password included. */
  showValue: boolean;
}

/**
 * The refused parameters this URL carries, in the order they are declared.
 *
 * `version` joined `sort` and `since` here because Explore is the view whose
 * reads TAKE a version, and an unrecognised one was being dropped in silence:
 * the page read the latest and said nothing, so a reader following a
 * truncated or stale link had no way to learn that the version they asked for
 * was not the version they were looking at.
 */
export function ignoredParams(params: Record<string, string>): IgnoredParam[] {
  const dated: IgnoredParam[] = IGNORED_PARAMS
    .filter((name) => (params[name] ?? "").trim().length > 0)
    .map((name) => ({ name, value: params[name].trim(), why: NO_STORED_DATE, showValue: true }));
  const asked = (params.version ?? "").trim();
  if (!asked || looksLikeVersionId(asked)) return dated;
  return [...dated, { name: "version", value: asked, why: NOT_A_VERSION, showValue: false }];
}

/**
 * One line naming every parameter this read left out, and why. Null when the
 * URL carries none.
 *
 * `resultsShown` is what the second sentence rests on. It says the results
 * below are ranked by relevance instead of by the order the link asked for,
 * which is only a fact when there are results below it: `#console/explore?sort=newest`
 * with no query is the landing page, and the sentence there points at rows
 * that are not on the screen.
 */
export function ignoredSentence(ignored: IgnoredParam[], resultsShown: boolean): string | null {
  if (!ignored.length) return null;
  // One sentence per REASON. `sort` and `since` share theirs and are listed
  // together; `version` has its own, so saying the reason once per group
  // keeps the line from repeating either of them.
  const byWhy = new Map<string, IgnoredParam[]>();
  for (const entry of ignored) {
    const found = byWhy.get(entry.why);
    if (found) found.push(entry); else byWhy.set(entry.why, [entry]);
  }
  const sentences = [...byWhy.entries()].map(([why, entries]) => {
    const named = entries.map((entry) => (entry.showValue ? `${entry.name}=${entry.value}` : entry.name));
    const list = named.length === 1 ? named[0] : `${named.slice(0, -1).join(", ")} and ${named[named.length - 1]}`;
    const sentence = `This link asked for ${list}, which this read cannot honour: ${why}.`;
    // The ranking sentence belongs to the DATE parameters: it answers "you
    // asked for newest first and did not get it". Under a refused version it
    // would be answering a question nobody asked.
    return why === NO_STORED_DATE && resultsShown
      ? `${sentence} The results below are ranked by relevance over every date.`
      : sentence;
  });
  return sentences.join(" ");
}

export interface ExploreParams {
  q: string;
  type: string | null;
  record: string | null;
  /**
   * The version named on the URL: a chat deep link opens a record at the
   * version the chat read. Null means whatever the console is reading.
   *
   * Null also when `?version=` carried something that is not a generation id.
   * Explore is the only view whose reads take a version, and `?version=` is
   * whatever a reader or an earlier link put on the URL; the rest of the
   * console already refuses to PRINT such a value (`namedVersion`) or to put
   * it on a link (`consoleLink`), and this is the third place it can do harm.
   * Sent to `console/records/*` verbatim it is answered 422 and the view
   * becomes a read failure, where the identical URL on Home, Updates or
   * Sources is dropped and the latest is read. One value must not mean two
   * things, and the behaviour that breaks must not be the one on the view a
   * stale or truncated link is likeliest to name. A value that IS shaped like
   * a generation id is passed through untouched: a generation this deployment
   * never published is then refused by the server, by name, which is a fact
   * about that generation rather than about the string on the URL.
   */
  version: string | null;
  offset: number;
  tab: RecordTab;
  /** Related's filters: one relationship type, a direction, a page. */
  rel: string | null;
  dir: "both" | "outgoing" | "incoming";
  roffset: number;
  /** The results rail: one source. */
  source: string | null;
  /** The refused parameters this URL carried, named for the page to print.
   *  Never sent to the server. */
  ignored: IgnoredParam[];
  /** The query-syntax help, open. */
  help: boolean;
  /** The length of a `q` too long to send, when the URL carried one. The
   *  query is then not searched and the page says why (`overlongSentence`). */
  overlong?: number | null;
  /** True when the URL itself named a tab; the default Content is then a
   *  request the page can refuse by name rather than a default. */
  tabAsked?: boolean;
}

/** The longest query the console sends. A 10,000-character query widened
 *  the page to 76,018 px and was sent whole (audit 2026-09-23). */
export const MAX_QUERY_CHARS = 512;

/** What the page says instead of searching an overlong query. */
export function overlongSentence(length: number): string {
  return `This search is ${length.toLocaleString("en-US")} characters long; `
    + `searches take at most ${MAX_QUERY_CHARS}. Shorten it and press Enter.`;
}

function position(raw: string | undefined): number {
  const value = Number.parseInt(raw ?? "0", 10);
  return Number.isInteger(value) && value > 0 ? value : 0;
}

/** `#explore?q=&type=&record=&version=&offset=&tab=&rel=&dir=&roffset=`. */
export function parseExploreParams(params: Record<string, string>): ExploreParams {
  const tab = RECORD_TABS.find((candidate) => candidate === params.tab) ?? "content";
  const dir = params.dir === "outgoing" || params.dir === "incoming" ? params.dir : "both";
  const q = (params.q ?? "").trim();
  const overlong = q.length > MAX_QUERY_CHARS ? q.length : null;
  return {
    // An overlong query is not searched: it is dropped here, and `overlong`
    // makes the page say so rather than show the landing as if nothing had
    // been asked.
    q: overlong === null ? q : "",
    overlong,
    tabAsked: typeof params.tab === "string" && params.tab !== "",
    type: params.type || null,
    record: params.record || null,
    version: looksLikeVersionId(params.version ?? "") ? params.version : null,
    offset: position(params.offset),
    tab,
    rel: params.rel || null,
    dir,
    roffset: position(params.roffset),
    source: params.source || null,
    ignored: ignoredParams(params),
    help: params.help === "1",
  };
}

/** The parameters an Explore href carries: only the ones that are set. */
export function exploreQuery(params: Partial<ExploreParams>): Record<string, string> {
  const out: Record<string, string> = {};
  if (params.q) out.q = params.q;
  if (params.type) out.type = params.type;
  if (params.record) out.record = params.record;
  if (params.version) out.version = params.version;
  if (params.offset) out.offset = String(params.offset);
  // The default tab is left out of the URL. A record that serves no content
  // has no Content tab and opens on the first tab it does have, which the
  // page then writes into the href explicitly.
  if (params.tab && params.tab !== "content") out.tab = params.tab;
  if (params.rel) out.rel = params.rel;
  if (params.dir && params.dir !== "both") out.dir = params.dir;
  if (params.roffset) out.roffset = String(params.roffset);
  if (params.source) out.source = params.source;
  // sort and since are deliberately not written back: an href this view builds
  // never re-adds a parameter the read refuses, so following any link on the
  // page leaves the dead parameter behind.
  if (params.help) out.help = "1";
  return out;
}

/** Which version Explore reads: the one the URL names (a chat deep link
 *  opens a record at the version the chat read), else the one the console is
 *  reading, else the latest published one. Null when nothing is published. */
export function versionToRead(
  params: ExploreParams, readingVersion: string | null, latest: string | null,
): string | null {
  return params.version ?? readingVersion ?? latest;
}

/** The link chat answers emit: the record, at the version the chat read. */
export function exploreRecordHash(link: LinkFn, record: string, version: string | null): string {
  return link("explore", exploreQuery({ record, version }));
}
