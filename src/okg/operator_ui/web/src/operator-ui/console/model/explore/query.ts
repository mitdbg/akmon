// The query syntax (type:, source:, id:), match highlighting, and the filters
// the URL and the query agree on.
import type { ExploreParams } from "@operator-ui/console/model/explore/params";

// --- the results ------------------------------------------------------------

export type TextPart = { text: string; match: boolean };

/**
 * The text with the query's occurrences marked. The whole phrase first; when
 * it occurs nowhere, each word of two or more characters. Case-insensitive,
 * and the original text is what is returned, split, never a lower-cased copy.
 */
export function highlight(text: string, query: string): TextPart[] {
  const needles = [query.trim()].filter(Boolean);
  const lower = text.toLowerCase();
  if (!needles.length || !lower.includes(needles[0].toLowerCase())) {
    needles.splice(0, needles.length, ...query.split(/\s+/).filter((word) => word.length >= 2));
  }
  const found = needles.map((n) => n.toLowerCase()).filter((n) => n && lower.includes(n));
  if (!found.length) return [{ text, match: false }];
  const parts: TextPart[] = [];
  let index = 0;
  while (index < text.length) {
    let next = -1;
    let length = 0;
    for (const needle of found) {
      const at = lower.indexOf(needle, index);
      if (at !== -1 && (next === -1 || at < next)) { next = at; length = needle.length; }
    }
    if (next === -1) { parts.push({ text: text.slice(index), match: false }); break; }
    if (next > index) parts.push({ text: text.slice(index, next), match: false });
    parts.push({ text: text.slice(next, next + length), match: true });
    index = next + length;
  }
  return parts;
}

// --- the query syntax ------------------------------------------------------------

/** What the box's text says: bare words and quoted phrases for the text
 *  read, and the three operators. `id:` wins: it is an exact lookup. */
export interface ParsedQuery {
  /** The words and phrases, for the read and the highlighter. */
  text: string;
  id: string | null;
  type: string | null;
  source: string | null;
  phrases: string[];
  words: string[];
}

/** `bare words`, `"a phrase"`, `id:<record id>`, `type:<name>`,
 *  `source:<name>`. An operator given twice keeps the last. A phrase is kept
 *  whole for the read, which matches substrings. */
export function parseQuery(raw: string): ParsedQuery {
  const out: ParsedQuery = { text: "", id: null, type: null, source: null, phrases: [], words: [] };
  const tokens = raw.trim().matchAll(/"([^"]*)"|(\S+)/g);
  for (const token of tokens) {
    if (token[1] !== undefined) { if (token[1].trim()) out.phrases.push(token[1].trim()); continue; }
    const word = token[2];
    const operator = /^(id|type|source):(.+)$/.exec(word);
    if (operator) {
      const value = operator[2].replace(/^"|"$/g, "");
      if (operator[1] === "id") out.id = value;
      else if (operator[1] === "type") out.type = value;
      else out.source = value;
      continue;
    }
    out.words.push(word);
  }
  out.text = [...out.phrases, ...out.words].join(" ");
  return out;
}

/** The query the read gets: the exact id when one was asked for, else the
 *  words and phrases. */
export function readQuery(parsed: ParsedQuery): string {
  return parsed.id ?? parsed.text;
}

export const QUERY_SYNTAX: { form: string; means: string }[] = [
  { form: "ingest stalled", means: "records whose text carries every word" },
  { form: "\"blocked ingest\"", means: "records carrying the phrase" },
  { form: "id:issue:ingest-001", means: "the record with exactly that id, first" },
  { form: "type:issue", means: "only records of that type; the same as checking it in the rail" },
  { form: "source:source-07-git", means: "only records from that producer" },
];

// --- the filters the URL and the query agree on -------------------------------------

/** The filters in force: the URL's, with the query's operators on top.
 *
 *  `sort` and `since` are not here. The read refuses both with
 *  `filter_has_no_stored_basis`, so there is no such thing as a sort order in
 *  force; a URL that names one is reported by `params.ignored`, not folded in
 *  here as if it had been applied. */
export interface EffectiveFilters {
  parsed: ParsedQuery;
  type: string | null;
  source: string | null;
}

export function effectiveFilters(params: ExploreParams): EffectiveFilters {
  const parsed = parseQuery(params.q);
  return {
    parsed,
    type: parsed.type ?? params.type,
    source: parsed.source ?? params.source,
  };
}

/** The parameters the search read is asked with. Null filters are dropped
 *  by the client, so a read with none is the plain `q` read. Nothing the
 *  server refuses can appear here: this is the whole request surface. */
export function searchRequest(
  version: string, filters: EffectiveFilters, offset: number, limit: number,
): Record<string, string | number | null> {
  return {
    version,
    q: readQuery(filters.parsed),
    type: filters.type,
    source: filters.source,
    offset,
    limit,
  };
}
