// One configured producer is one row.
//
// This file used to fold the sources read onto the registry in the browser,
// because that read returned a producer twice: once under the name its
// registry document gives it and once under the key its runtime attributes
// work to. The mocks ledger called that fold "liberty 9" and recorded it as a
// stand-in for a backend change.
//
// The backend change landed. `console/producers` resolves a producer by the
// `ownership_id` its registry declares and returns it once, so there is
// nothing left to fold and no name to guess at. The fold could not have been
// right in any case: it split a name on ":" , which repairs `docsite:worker`
// but not `bib-references` against `okg-workspace.bib_references`, and would
// have turned `okg-ci`'s `agent:okg-ci-triage` into a producer called
// `agent`.
//
// What remains here is an adapter: the read's row, given the shape the table
// and the producer page already expect.
import type { RawCountDisagreement, RawOmittedData, RawProducerRecord, RawProducersRead, RawReadAuthority } from "@operator-ui/console/model/sources/reads";
import { exact } from "@operator-ui/console/model/shared/format";
import { stateOf, stateRank } from "@operator-ui/console/model/shared/producerStates";
import type { ProducerKind } from "@operator-ui/console/model/shared/reads";
import type { Glyph } from "@operator-ui/console/model/shared/status";

// --- producers --------------------------------------------------------------

export interface ProducerRecord {
  name: string;
  /** The registry's declared runtime key. */
  ownershipId: string;
  /** The key the table renders a row by. The read serves it as `kind` plus
   *  `ownership_id`, because the two are not interchangeable: an enricher's
   *  own output is counted in the rollup's `source` dimension under
   *  `_enricher:<name>`, so an enricher and a source row could arrive holding
   *  the SAME ownership id. The read now folds that pair into one row; this
   *  key is the second guard, and it makes the collision impossible to render
   *  rather than merely absent today. */
  rowKey: string;
  kind: ProducerKind;
  /** False for a producer the reads observed running that no configuration
   *  names -- a derived producer, or one removed from the registry. */
  configured: boolean;
  health: RawProducerRecord["health"];
  workflow: RawProducerRecord["workflow"];
  contribution: RawProducerRecord["contribution"];
  freshness: RawProducerRecord["freshness"];
  latestRun: RawProducerRecord["latest_run"];
  latestRunUnavailableReason: string | null;
  runCount: number | null;
  runCountUnavailableReason: string | null;
  /** One sentence, when two reads counted this producer's records and did
   *  not agree. Null in the ordinary case, and null too when the read is too
   *  old to carry the key. */
  countDisagreement: string | null;
}

/** The read's rows, as the view's records. No collapsing: the read already
 *  returns each producer once. */
export function producerRecordsFrom(read: RawProducersRead): ProducerRecord[] {
  const disagreement = new Map(
    (read.payload.count_disagreements ?? [])
      .filter((entry) => entry.ownership_id)
      .map((entry) => [String(entry.ownership_id), countDisagreementSentence(entry)]),
  );
  return (read.payload.producers ?? []).map((row) => ({
    name: row.name,
    // A row the read somehow returned without one is keyed by its name rather
    // than by an empty string, which two such rows would share.
    ownershipId: row.ownership_id ?? row.name,
    rowKey: row.row_key ?? `${row.kind}:${row.ownership_id ?? row.name}`,
    kind: row.kind,
    configured: row.configured,
    health: row.health,
    workflow: row.workflow,
    contribution: row.contribution,
    freshness: row.freshness,
    latestRun: row.latest_run,
    latestRunUnavailableReason: row.latest_run_unavailable_reason ?? null,
    runCount: row.run_count,
    runCountUnavailableReason: row.run_count_unavailable_reason ?? null,
    countDisagreement: disagreement.get(row.ownership_id ?? row.name) ?? null,
  }));
}

/**
 * Two recorded counts that contradict each other, in one sentence.
 *
 * NOT a partial read, and deliberately not built from `omitted_data`: that
 * list means a read came back short, every entry of it is rendered as that
 * sentence, and filing a disagreement there made the page say a read had
 * failed when none had. Both figures were read, in full, and they do not
 * agree.
 *
 * So the sentence states BOTH, and says they are not reconciled. It does not
 * pick one, because no read this console has is authority over the other,
 * and it does not round the difference into a caveat on a single number.
 */
export function countDisagreementSentence(entry: RawCountDisagreement): string {
  const name = entry.name ? `\`${entry.name}\`` : "this producer";
  const source = entry.source_dimension_records;
  const own = entry.enricher_dimension_records;
  if (typeof source !== "number" || typeof own !== "number") {
    return `Two reads counted ${name}'s records and did not agree; neither `
      + "figure was carried, so this page states none.";
  }
  return `The rollup counts ${exact(source)} records under this enricher's `
    + `source name and ${exact(own)} under its enricher dimension; the two `
    + "are not reconciled, and neither is shown in place of the other.";
}

// --- a read that came back short ---------------------------------------------

/** The `read_authority` key that answers for an `omitted_data` authority. */
const AUTHORITY_KEY: Record<string, string> = {
  enricher_diagnostics: "enrichers",
  view_diagnostics: "views",
};

/** What a producer of that kind is called, in the plural. */
const KIND_PLURAL: Record<string, string> = {
  source: "sources",
  enricher: "enrichers",
  view: "views",
};

/** What a failed sub-read of the sources composer costs the rows it left.
 *
 *  Each of these reads fills a FIELD on every source row. When one fails the
 *  rows are all still there, and the field falls back to words the page also
 *  prints for a deployment that genuinely records nothing -- so the sentence
 *  has to name the words, or a reader takes the failure for the data. */
const FIELD_OMISSION: Record<string, string> = {
  runtime_readiness:
    'No row\'s state or freshness was read: where a row reads "State not '
    + 'recorded" or "not evaluated", that is this failed read and not the '
    + "deployment.",
  source_run_diagnostics:
    'No row\'s last run was read: where a row reads "never run", that is '
    + "this failed read and not the producer's history.",
};

/**
 * One sentence per read that came back short.
 *
 * TWO SHAPES, because two different things can go missing.
 *
 * A producer KIND can. The producers read composes three diagnostics reads;
 * when the enricher or the view composer raises, it records the failure in
 * `read_authority`, names the kind in `omitted_data`, and returns the rows it
 * does have. The table then looks complete and is not: an operator reads "no
 * enricher is configured" where the truth is "the enricher read failed".
 *
 * A FIELD can. A sub-read inside the sources composer -- runtime readiness,
 * the run-diagnostics query -- fills one field on every source row. When it
 * fails every producer is still listed, and the field falls back to
 * `not_evaluated`, a null state, or no run: the same words a deployment that
 * records none of those gets. Such an entry carries no `kind`, because no
 * producer went missing, and its sentence names the words to distrust
 * instead of claiming the table is short.
 *
 * The reason is the composer's own, in words. The exception's class and its
 * text are in the payload and are deliberately not read: the console never
 * serves exception text.
 */
export function partialReadSentences(read: RawProducersRead): string[] {
  const authority = (read.payload.read_authority ?? {}) as Record<string, unknown>;
  const sources = (authority.sources ?? {}) as Record<string, unknown>;
  const words = (value: string) => value.replace(/_/g, " ");
  return (read.payload.omitted_data ?? []).map((entry: RawOmittedData) => {
    const name = entry.authority ?? "";
    if (!entry.kind) {
      const detail = sources[name];
      const reason = detail && typeof detail === "object"
        ? (detail as RawReadAuthority).reason ?? null
        : null;
      const because = words(reason ?? entry.reason ?? `${name || "a"} read failed`);
      const cost = FIELD_OMISSION[name]
        ?? "The fields it fills were not read; what the rows show in their "
          + "place is this failed read, not the deployment.";
      return `The ${words(name) || "read"} behind this table came back short: `
        + `${because}. Every producer is still listed. ${cost}`;
    }
    const key = AUTHORITY_KEY[name] ?? name;
    const detail = key ? authority[key] : null;
    const reason = detail && typeof detail === "object"
      ? (detail as RawReadAuthority).reason ?? null
      : null;
    const plural = KIND_PLURAL[entry.kind] ?? `${entry.kind}s`;
    const because = reason
      ? words(reason)
      : `the ${key || "read"} behind them is unavailable`;
    return `No ${plural} are listed: ${because}. Nothing stands in for them, `
      + "and every count on this page is short by that many producers.";
  });
}

// --- states -------------------------------------------------------------------
//
// The vocabulary itself -- the states three reads record, the word and glyph
// each is printed as, and where each sorts -- was written here and now lives
// in `console/model/shared/producerStates.ts`, because Home reads the same
// three diagnostics routes and needs the same words. This file keeps only
// what is about a producer RECORD rather than about a state.

/**
 * Default rank: failed, then what is held off, then never run, then work under
 * way, then the rest. Within a rank, by last run, newest first. Not configured
 * last of all.
 *
 * The ranks are `stateRank`'s. What is decided here is the one thing that is
 * not a property of a state: a producer no configuration names sorts after
 * every recorded state, whatever state the reads observed it in.
 */
export function defaultRank(record: ProducerRecord): number {
  if (!record.configured) return 90;
  return stateRank(stateOf(record));
}

const RUN_GLYPHS: Record<string, Glyph> = {
  ok: { icon: "check", word: "Completed", severity: "ok" },
  failed: { icon: "close", word: "Failed", severity: "failed" },
  running: { icon: "sync", word: "Running", severity: "running" },
  withheld: { icon: "stop", word: "Withheld", severity: "never-run" },
};

export function runGlyph(outcome: string): Glyph {
  return RUN_GLYPHS[outcome] ?? { icon: "minus", word: outcome, severity: "never-run" };
}
