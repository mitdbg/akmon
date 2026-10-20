// Number and text formatting shared by the console models.

/** Exact integers with thousands separators. Pinned to en-US, like the rest of
 *  the console: a host under another locale otherwise renders "4 182 044" in
 *  English copy. */
export function exact(value: number): string {
  return value.toLocaleString("en-US");
}

/** `+2,605`, `-185`, `0`. A delta always carries its sign, and a negative one
 *  carries its own: `+${exact(-185)}` printed "+-185" on every version that
 *  retracted more than it added. */
export function signed(value: number): string {
  return value > 0 ? `+${exact(value)}` : exact(value);
}

/**
 * Drop a leading machine code from a recorded failure line.
 *
 * The composer writes `ingest_source_incomplete: cmssw-releases returned 412
 * rows of an expected 2,414;` -- a code, then the sentence a person reads.
 * Home prints the sentence. The code is not lost: it stays in the read, and
 * Updates and Sources show it beside the verbatim error text one level down.
 * Rule 3 puts the raw failure one level down, and a summary page that leads
 * with `clone_failed:` is asking the reader to learn the vocabulary first.
 */
export function plainCause(detail: string | null): string | null {
  if (!detail) return null;
  const match = /^[a-z][a-z0-9_]*:\s+(.*)$/s.exec(detail.trim());
  const text = (match ? match[1] : detail.trim()).trim();
  // A cause is a fragment, not a sentence: the composer's trailing ";" and "."
  // are punctuation for a line that no longer exists.
  return text.replace(/[;.,]+$/, "") || null;
}

/** A recorded measurement with the composer's trailing punctuation removed. A
 *  cause is a fragment, not a sentence. */
export function recordedText(actual: string | null | undefined): string {
  return (actual ?? "").trim().replace(/[;.,]+$/, "");
}

/**
 * What the runtime calls a thing it counts, in the words a person reads.
 *
 * The health read records a measurement as a bare string: `dlq_nonzero`
 * writes `actual: "12281 in dead_letter"`, and the console printed exactly
 * that on Home -- an unseparated five-digit number and a queue named by the
 * column it lives in. Neither is wrong; both are machine vocabulary on the
 * page an operator opens to see whether anything needs doing.
 *
 * A map rather than a rule, for the same reason `CONVERGENCE_CAUSES` is one:
 * a code this console has never seen is printed as its own words and nothing
 * more, because inventing an English name for it would assert a meaning
 * nobody gave it.
 */
const RECORDED_OBJECT_WORDS: Readonly<Record<string, string>> = {
  dead_letter: "the dead-letter queue",
};

/** A recorded measurement, as a person reads it.
 *
 *  Two changes and no others: a count of four digits or more gets its
 *  thousands separators, and a recorded code becomes words. A token that
 *  names a database object is left exactly as it was -- `namesDatabaseObject`
 *  decides what the row does with the whole string, and rewriting the name
 *  first would hide it from that test. */
export function measurementWords(actual: string | null | undefined): string {
  const recorded = recordedText(actual);
  if (!recorded) return "";
  return recorded
    .split(/(\s+)/)
    .map((token) => {
      if (/^\d{4,}$/.test(token)) return exact(Number(token));
      const bare = bareToken(token);
      if (isDatabaseObject(bare)) return token;
      const words = RECORDED_OBJECT_WORDS[bare];
      if (words) return token.replace(bare, words);
      return /^[a-z][a-z0-9]*(_[a-z0-9]+)+$/.test(bare)
        ? token.replace(bare, bare.replace(/_/g, " "))
        : token;
    })
    .join("");
}

/**
 * Does this recorded string name a thing inside the database?
 *
 * The live Archiv3 deployment printed `dbos.workflow_status missing` on Home:
 * the name of a table, on the page an operator opens to find out whether the
 * graph is current. What the console may print about a failure is the sentence
 * the check already wrote; the object the check measured stays one level down.
 *
 * The requirement is that narrow, and so is this. An earlier version read it
 * as "any lowercase token with an internal dot or underscore", which withheld
 * a producer's own name (`cmssw_releases returned 412 rows`), a version
 * (`v1.2.0`), a host (`api.github.com`), a file (`config.yaml`) and even
 * `e.g.`, while missing `relation "public.workflowStatus"`,
 * `okg.contributor_edge's` and anything a writer capitalised. Three things
 * name a database object and nothing else does:
 *
 *   1. a token qualified by one of this deployment's schemas, in any case;
 *   2. a token ending in one of the suffixes the graph's own tables and views
 *      carry, with or without a schema in front of it;
 *   3. a QUOTED name after the word `relation`, which is how PostgreSQL's own
 *      errors write one whatever it is called.
 *
 * A producer NAME on its own is printed: the producer is named on the page
 * anyway, in the row the cause belongs to, so withholding the sentence hides
 * the fact and keeps nothing.
 */
/** A schema, WITH its dot. Bare, three of the four are ordinary words: a fix
 *  hint reading "inspect DBOS history volume and the runtime queue authority"
 *  names no table, and withholding it withheld the only sentence the row had.
 *  `information_schema` is nobody's English and is matched bare as well. */
const SCHEMA_PREFIXES: readonly string[] = ["okg.", "dbos.", "public.", "information_schema."];

/** The suffixes this graph's tables and views end in. */
const OBJECT_SUFFIXES: readonly string[] = ["_status", "_facts", "_edge", "_node", "_refs"];

/** PostgreSQL's own way of naming one: `relation "run_ledger" does not exist`. */
const QUOTED_RELATION = /\brelation\s+["'`][^"'`\s]+["'`]/i;

/** A word with the punctuation a sentence wrapped it in removed, including a
 *  possessive: `okg.contributor_edge's` is the same object as
 *  `okg.contributor_edge`. Lowercased, because the case a writer used says
 *  nothing about what the thing is. */
function bareToken(token: string): string {
  return token
    .replace(/^[("'`]+/, "")
    .replace(/[)"'`:;.,]+$/, "")
    .replace(/'s$/i, "")
    .toLowerCase();
}

function isDatabaseObject(token: string): boolean {
  if (!token) return false;
  if (token === "information_schema") return true;
  if (SCHEMA_PREFIXES.some((prefix) => token.startsWith(prefix))) return true;
  const name = token.includes(".") ? token.slice(token.lastIndexOf(".") + 1) : token;
  return OBJECT_SUFFIXES.some((suffix) => name.endsWith(suffix));
}

export function namesDatabaseObject(text: string | null | undefined): boolean {
  const recorded = recordedText(text);
  if (!recorded) return false;
  if (QUOTED_RELATION.test(recorded)) return true;
  return recorded.split(/\s+/).some((token) => isDatabaseObject(bareToken(token)));
}

/** The function words that tell a sentence from a command line. */
const FUNCTION_WORDS = new Set([
  "a", "an", "and", "at", "but", "by", "for", "from", "in", "into", "its", "of",
  "on", "or", "that", "the", "this", "to", "with",
]);

/**
 * Does this read as a sentence rather than as something to type?
 *
 * A check's `fix_hint` is either words ("inspect DBOS history volume and the
 * runtime queue authority") or a command line ("okg queue dlq list"). Home
 * prints the words. A command belongs one level down with the rest of the
 * machine vocabulary, and a summary page that answers "what is wrong" with
 * four bare tokens is asking the reader to learn the CLI first.
 */
export function readsAsProse(text: string | null | undefined): boolean {
  if (!text) return false;
  return text.toLowerCase().split(/[^a-z']+/).some((word) => FUNCTION_WORDS.has(word));
}

/**
 * A share of a total, as a person reads it.
 *
 * Whole percents, because a census is not a measurement -- with one exception,
 * and it is about not printing a zero for something that is there. A count
 * that is not zero never prints "0%"; it prints "<1%", which is the true
 * statement the rounding threw away. okg-workspace holds 2,050 `blocks`
 * against 1,037,093 relationships, and the Contents panel said that type was
 * 0% of the version.
 *
 * A count that IS zero prints "0%", because that is exactly what it is.
 */
export function shareLabel(count: number, total: number): string {
  if (total <= 0 || count <= 0) return "0%";
  const percent = (count / total) * 100;
  return percent < 0.5 ? "<1%" : `${Math.round(percent)}%`;
}

/** `1 source`, not `1 sources`. */
export function noun(value: number, singular: string): string {
  return value === 1 ? singular : `${singular}s`;
}
