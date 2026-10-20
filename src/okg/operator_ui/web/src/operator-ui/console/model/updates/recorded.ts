// What ONE attempt recorded, as the attempt page reads it.
//
// The read behind this file is GET /viz/api/console/attempts/detail?id=<full
// generation id>, composed by src/okg/substrate/console_views/attempt_detail.py.
// The operator's verdict on 2026-09-21 was that the Updates list "looks good but the
// detailed update view shows nothing": the page had a headline, an id, one
// change line and two sentences about what is not recorded, because until
// that read existed there was nothing else to draw from.
//
// One rule decides every section: a section whose data the deployment did not
// record is CUT, and an absent section says nothing. A section whose data was
// REFUSED is different -- the read names its reason, and the page prints that
// reason inline. The two are never merged, because "this deployment never
// recorded a consistency check" and "the rollup read failed" ask the reader
// to do different things.
import { exact, noun, signed } from "@operator-ui/console/model/shared/format";
import type { RawAttempt, RawViewRun } from "@operator-ui/console/model/shared/reads";
import { type Glyph } from "@operator-ui/console/model/shared/status";
import type { LinkFn } from "@operator-ui/console/model/shared/links";

// --- the raw payload ------------------------------------------------------

/** An unavailable part, named. Never a blank and never a zero. */
export interface RawMarker {
  reason: string;
  [key: string]: unknown;
}

export interface RawMoved {
  added: number | null;
  updated: number | null;
  retracted: number | null;
}

export interface RawHolds {
  node_count: number | null;
  edge_count: number | null;
  nodes: RawMoved | null;
  edges: RawMoved | null;
  unavailable: RawMarker[];
  /** Set when this checkpoint never published. It is NOT an entry in
   *  `unavailable` above: the totals were recorded, and printing it under
   *  "part of this version's totals was not recorded" would be false. */
  unpublished_version: RawMarker | null;
}

export interface RawCountRow {
  name: string;
  /** On a by-source row: the name Sources prints, where the read serves it
   *  (the rollup keys a source by its ownership id). */
  source_name?: string | null;
  count: number;
  /** Null, never zero, when the parent side could not be read: zero is a
   *  measured fact about the parent version, null is the absence of one. */
  parent_count: number | null;
  parent_available: boolean;
}

export interface RawCountSection {
  rows: RawCountRow[];
  shown: number;
  total: number;
  capped: boolean;
  limit: number;
  unavailable: RawMarker | null;
  parent_unavailable?: RawMarker | null;
  /** The composer's own marker for a section read off a checkpoint that
   *  never published. Distinct from `unavailable`: the counts are real and
   *  still render; what the marker says is which version they describe. */
  unpublished_version: RawMarker | null;
}

export interface RawWithheld {
  key: string;
  kind: string;
  size: number;
  withheld_reason: string;
}

export interface RawGate {
  gate_name?: string | null;
  check_id?: string | null;
  result?: string | null;
  reason?: string | null;
  blocks_publish?: boolean | null;
  elapsed_ms?: number | null;
  /** False when the check ran over part of the graph; `coverage_basis`
   *  names what it covered and which rule families it skipped. */
  coverage_complete?: boolean | null;
  coverage_basis?: string | null;
  run_id?: string | null;
  withheld?: RawWithheld[];
  [key: string]: unknown;
}

export interface RawChecks {
  gates: RawGate[];
  total: number | null;
  passed: number | null;
  blocked: number | null;
  unavailable: RawMarker | null;
}

export interface RawRuns {
  runs: RawViewRun[];
  total: number;
  capped: boolean;
  /** Set when the read could not join this attempt to the runs table at all
   *  -- which is not the same fact as an attempt with no recorded run. */
  unavailable: RawMarker | null;
}

export interface RawNeighbours {
  newer_attempt_id: string | null;
  older_attempt_id: string | null;
  /** Null for an attempt the read fetched BY ID because it is older than the
   *  window it scanned: its place in the list is not known, and a number
   *  guessed from a window that does not hold it would be wrong. */
  position: number | null;
  total: number;
  /** `checkpoint_id_prefix` when the read counted the history, `scan_window`
   *  when the backend could not and the number is only what the window saw. */
  total_basis?: string;
  /** Why `older_attempt_id` is null, when the reason is the window rather
   *  than the end of the history. Without it the page drew "this is the
   *  oldest attempt" over a history the read never reached. */
  older_attempt_unavailable_reason?: string | null;
  /** Why `position` is null. */
  unavailable_reason?: string | null;
  parent_generation_id: string | null;
  parent_source: string | null;
}

export interface RawNameList {
  recorded: boolean;
  names: string[];
  withheld_count?: number;
  unavailable?: RawMarker | null;
}

export interface RawConfigured {
  deployment_attestations: RawNameList;
  nomos_policy_bundle_ids: RawNameList;
  excluded_sources: RawNameList;
  enricher_governance_record_count: number | null;
  catalog_version_id: number | null;
}

/** The runs recorded between the previous version and this one, per source. */
export interface RawSourceWrites {
  rows: {
    source_name: string;
    /** The runtime key `by_source` keys the same source by. */
    ownership_id?: string | null;
    runs: number;
    records_written: number | null;
    relationships_written: number | null;
  }[];
  window: { after?: string | null; until?: string | null };
  basis: string;
  unavailable: RawMarker | null;
}

export interface RawAttemptDetail {
  payload: {
    deployment: string;
    branch: string;
    attempt_id: string;
    version_published: boolean;
    attempt: RawAttempt;
    holds: RawHolds;
    checks: RawChecks;
    runs: RawRuns;
    neighbours: RawNeighbours;
    configured: RawConfigured;
    by_record_type: RawCountSection;
    by_relationship_type: RawCountSection;
    by_source: RawCountSection;
    /** Absent on a read that predates it; the page then names no source. */
    source_writes?: RawSourceWrites;
  };
}

// --- the page's view of it ------------------------------------------------

export interface CountRow {
  name: string;
  /** The page this row opens: Explore for a record type, Sources for a
   *  source Sources names. Null where there is no such page. */
  href: string | null;
  /** "1,490". */
  count: string;
  /** "+903", "-988", or null when the parent side could not be read. */
  delta: string | null;
  /** Said once per row where the parent could not be read, instead of a
   *  delta: a missing parent is not a delta of zero. */
  parentNote: string | null;
}

export interface CountTable {
  key: string;
  title: string;
  testid: string;
  rows: CountRow[];
  /** "Showing 200 of 431 record types." Null when nothing was capped. */
  capLine: string | null;
  /** The read's own reason, printed instead of rows when it refused. */
  refusedNote: string | null;
  /** Said once for the whole table when the parent version could not be
   *  read, so the rows do not each repeat it. */
  parentNote: string | null;
}

export interface GateRow {
  key: string;
  /** The gate in words: "Graph consistency check". */
  name: string;
  /** The gate's recorded name, printed once in a muted line of its own. */
  code: string | null;
  mark: Glyph;
  /** "no violations found · took 173 ms", the recorded outcome and its span. */
  detail: string;
  /** On a gate that did not pass: its details are withheld (they can hold
   *  error text), so the note names its recorded failure code instead. Null
   *  on a passed gate, where "fields are not shown" was noise that read as
   *  "errors are being hidden". */
  withheldNote: string | null;
}

export interface RunRow {
  key: string;
  name: string;
  kind: string;
  mark: Glyph;
  timing: string;
  errorText: string | null;
}

export interface RecordedDetail {
  /** Printed at the top of the counts when version_published is false,
   *  driven by the read's own counts_describe_an_unpublished_version marker
   *  and by nothing else. */
  unpublishedNote: string | null;
  /** "This version holds 1,723 records and 3,006 relationships." */
  holdsLine: string | null;
  /** What the header line does NOT already say: the retraction, and the
   *  update that no totals-subtraction delta can see. Null for a pure
   *  addition, because the header above this section has just printed it and
   *  a page should state one fact once. */
  movedLine: string | null;
  /** Reasons the read named for a part of `holds` it could not serve. */
  holdsNotes: string[];
  tables: CountTable[];
  /** Null cuts the section; a sentence prints in place of the rows. */
  checksNote: string | null;
  gates: GateRow[];
  checksSummary: string | null;
  runs: RunRow[];
  /** False when every recorded run is a check already listed above, so the
   *  section would only repeat it. */
  showRuns: boolean;
  runsNote: string | null;
  runsCapLine: string | null;
  /** "Ran 2nd of 6 attempts", plus the parent it built on. */
  positionLine: string | null;
  parentLine: string | null;
  newerHref: string | null;
  olderHref: string | null;
  newerId: string | null;
  olderId: string | null;
  /** Only what the attempt actually recorded here. */
  configuredLines: string[];
  /** The absences, collapsed. Four sentences of absence in a row is a
   *  section that says nothing; one line naming them is the same
   *  information. Never recorded and recorded-as-empty stay apart. */
  configuredAbsences: string[];
}

/**
 * Where an attempt sits, counted from the newest end, which is where the
 * list starts. "Attempt 1 of 3" read as the first attempt ever made; it was
 * the newest (review 2026-09-22).
 */
export function fromNewest(position: number, total: number): string {
  if (position === 1) return "Newest";
  if (position === total) return "Oldest";
  return `${ordinal(position)} newest`;
}

/** 2nd, 3rd, 11th, 22nd. */
function ordinal(value: number): string {
  const teen = value % 100 >= 11 && value % 100 <= 13;
  const suffix = teen ? "th" : ({ 1: "st", 2: "nd", 3: "rd" } as Record<number, string>)[value % 10] ?? "th";
  return `${exact(value)}${suffix}`;
}

/** How many producer runs the page prints before it states a total instead. */
export const RUNS_SHOWN = 20;

const GATE_MARKS: Record<string, Glyph> = {
  evaluated: { icon: "check", word: "Passed", severity: "ok" },
  passed: { icon: "check", word: "Passed", severity: "ok" },
  blocked: { icon: "stop", word: "Blocked", severity: "failed" },
  failed: { icon: "close", word: "Failed", severity: "failed" },
  skipped: { icon: "minus", word: "Skipped", severity: "never-run" },
};

const RUN_MARKS: Record<string, Glyph> = {
  completed: { icon: "check", word: "Completed", severity: "ok" },
  carried_forward: { icon: "check", word: "Carried forward", severity: "ok" },
  failed: { icon: "close", word: "Failed", severity: "failed" },
  blocked: { icon: "stop", word: "Blocked", severity: "failed" },
  running: { icon: "sync", word: "Running", severity: "running" },
};

/** The marker the read sets on a section whose checkpoint never published.
 *  Shared with the design-harness generator and checked against the Python
 *  composer's own constant in attemptLive.test.tsx, so the two cannot drift
 *  into agreeing on a key the product never emits. */
export const UNPUBLISHED = "counts_describe_an_unpublished_version";

function markOf(table: Record<string, Glyph>, status: string | null | undefined): Glyph {
  const key = (status ?? "").trim();
  return table[key] ?? { icon: "minus", word: key || "not recorded", severity: "never-run" };
}

/**
 * A marker, as a sentence, with its own code in it.
 *
 * The code is kept rather than translated away: it is what an operator greps
 * the read API's log for, and a sentence that paraphrased it would leave them
 * nothing to search. Rule 3 keeps a raw code off a SUMMARY page; this is the
 * detail, which is where the code belongs.
 */
function markerSentence(marker: RawMarker, lead: string): string {
  const extra = Object.entries(marker)
    .filter(([key, value]) => key !== "reason" && typeof value !== "object")
    .map(([key, value]) => `${key} ${String(value)}`);
  return `${lead}: ${marker.reason}${extra.length ? ` (${extra.join(", ")})` : ""}.`;
}

/** `12 records`, `1,818 relationships`. */
function counted(value: number, unit: string): string {
  return `${exact(value)} ${noun(value, unit)}`;
}

/**
 * What the header line does not already say.
 *
 * The header prints the attempt's change -- "+988 records \u00b7 +1,818
 * relationships" -- and the list row prints the same phrase, so a pure
 * addition restated here is the same fact twice on one page. What the header
 * CANNOT say is the update count: its four numbers are derived from
 * consecutive TOTALS, so a version that rewrote every row without changing
 * its size is indistinguishable from one that did nothing. A retraction is
 * stated too, because "what did this publish remove" is the question a
 * retraction page exists to answer.
 */
function movedSentence(
  nodes: RawMoved | null,
  edges: RawMoved | null,
): string | null {
  const sides: { moved: RawMoved; unit: string }[] = [
    ...(nodes ? [{ moved: nodes, unit: "record" }] : []),
    ...(edges ? [{ moved: edges, unit: "relationship" }] : []),
  ];
  const clause = (
    pick: (moved: RawMoved) => number | null,
    verb: string,
  ): string | null => {
    const parts: string[] = [];
    for (const side of sides) {
      const value = pick(side.moved);
      if (typeof value === "number" && value > 0) parts.push(counted(value, side.unit));
    }
    return parts.length ? `${verb} ${parts.join(" and ")}` : null;
  };
  // A version that rewrote rows and added and retracted none moved no count,
  // so the header above says "no change" -- which is true of the counts and
  // false of the graph. Said as one sentence that holds both facts: Archiv3's
  // second and third versions each rewrote 571 records and 692 relationships
  // (review 2026-09-22), under a lowercase "updated ..." that read as the
  // tail of a sentence the page never printed.
  const none = (pick: (moved: RawMoved) => number | null) => sides.every(
    (side) => pick(side.moved) === 0,
  );
  if (none((moved) => moved.added) && none((moved) => moved.retracted)) {
    const rewrote = clause((moved) => moved.updated, "Rewrote");
    return rewrote ? `${rewrote} without changing the counts.` : null;
  }
  const said = [
    clause((moved) => moved.retracted, "retracted"),
    clause((moved) => moved.updated, "updated"),
  ].filter((part): part is string => part !== null);
  if (!said.length) return null;
  const sentence = said.join(", and ");
  return `${sentence.charAt(0).toUpperCase()}${sentence.slice(1)}.`;
}

/** Fields of the projection delta the checkpoint did not record. */
function missingMoves(moved: RawMoved | null, label: string): string[] {
  if (!moved) return [];
  const missing = (["added", "updated", "retracted"] as const)
    .filter((field) => moved[field] === null);
  return missing.length
    ? [`${label}: ${missing.join(" and ")} not recorded.`]
    : [];
}

const TABLE_TITLES: [keyof RawAttemptDetail["payload"], string, string][] = [
  ["by_record_type", "By record type", "record types"],
  ["by_relationship_type", "By relationship type", "relationship types"],
  ["by_source", "By source", "sources"],
];

/** The count table's change column. "Against the parent" was the runtime's
 *  word for the version before this one. */
export const DELTA_HEADER = "Change from previous";

/** The name Sources prints for a by-source row: the one the read served
 *  beside it, else the one the source's own run rows record for the same
 *  runtime key. Null where neither says. */
type SourceNames = ReadonlyMap<string, string>;

function sourceNameOf(row: RawCountRow, names: SourceNames): string | null {
  return row.source_name ?? names.get(row.name) ?? null;
}

/** Where a count row opens, by table. Relationship types have no page. */
function rowHref(key: string, row: RawCountRow, link: LinkFn, names: SourceNames): string | null {
  if (key === "by_record_type") return link("explore", { type: row.name });
  const name = key === "by_source" ? sourceNameOf(row, names) : null;
  return name ? link(`sources/${encodeURIComponent(name)}`) : null;
}

function buildTable(
  key: string,
  title: string,
  unit: string,
  raw: RawCountSection,
  link: LinkFn,
  names: SourceNames,
): CountTable | null {
  // Every `unavailable` marker IS a refusal now. The unpublished marker has
  // its own field and never appears here, so filtering it out of this one
  // would only swallow a refusal that happened to carry the same code.
  const refused = raw.unavailable;
  // Cut, not blanked. A section with no row and no refusal is a section the
  // deployment recorded nothing for, and an absent section says nothing --
  // which is the honest answer, unlike an empty table with a header.
  if (!raw.rows.length && !refused) return null;
  const parentMarker = raw.parent_unavailable ?? null;
  // NO parent is not a failed read of one. The first version of a deployment
  // has nothing before it, and the read says so with `no_predecessor_version`;
  // the page printed "The parent version's counts could not be read:
  // no_predecessor_version." three times and a truncated "the parent version
  // was not read" in nineteen cells, under a header saying +8,941 (review
  // 2026-09-22). Each row's change is its whole count, and says why.
  const firstVersion = parentMarker?.reason === NO_PREDECESSOR;
  return {
    key,
    title,
    testid: `updates-detail-counts-${key}`,
    rows: raw.rows.map((row) => {
      const read = row.parent_available && row.parent_count !== null;
      return {
        // One name for one source everywhere: the name Sources prints where
        // the read serves it, not the rollup's ownership id.
        name: (key === "by_source" ? sourceNameOf(row, names) : null) ?? row.name,
        href: rowHref(key, row, link, names),
        count: exact(row.count),
        delta: read
          ? signed(row.count - (row.parent_count as number))
          : firstVersion ? `${signed(row.count)} (first version)` : null,
        parentNote: read || firstVersion ? null : "the parent version was not read",
      };
    }),
    capLine: raw.capped
      ? `Showing ${exact(raw.shown)} of ${exact(raw.total)} ${unit}.`
      : null,
    refusedNote: refused
      ? markerSentence(refused, "This count could not be read")
      : null,
    parentNote: parentMarker && !firstVersion
      ? markerSentence(parentMarker, "The parent version's counts could not be read")
      : null,
  };
}

/** The marker the read sets when the version has no predecessor at all. */
export const NO_PREDECESSOR = "no_predecessor_version";

/** Gates and their recorded results, in words. A name or result this map
 *  does not know prints as its own words, underscores for spaces. */
const GATE_NAMES: Readonly<Record<string, string>> = {
  transform_consistency_gate: "Consistency check",
};

function codeWords(code: string, table: Readonly<Record<string, string>>): string {
  return table[code] ?? code.replace(/_/g, " ");
}

/** "a", "a and b", "a, b or c". */
function listed(items: readonly string[], last = "and"): string {
  if (items.length <= 1) return items.join("");
  return `${items.slice(0, -1).join(", ")} ${last} ${items[items.length - 1]}`;
}

/**
 * What a gate found, over what it covered, and whether it holds the publish.
 *
 * Built from the recorded fields, never a fixed phrase: Archiv3's one check
 * read "1 check ran, 1 passed ... no violations found" while it had covered
 * only the records the update touched, skipped the cardinality rules and
 * could not have blocked the publish (review 2026-09-23). The duration is
 * gone too: the same run printed 336 ms here and 13 ms under Producer runs.
 */
function gateDetail(gate: RawGate): string {
  const basis = gate.coverage_basis ?? "";
  const touched = /dirty/.test(basis);
  // Where the check looked, only as far as the read says: the records the
  // update touched, the whole graph, or -- with no basis recorded -- nowhere
  // in particular.
  const clean = touched
    ? "no problems in the records this update touched"
    : gate.coverage_complete === true ? "no problems in the graph" : "no problems found";
  const found = gate.reason === "no_violations"
    ? clean
    : gate.reason ? gate.reason.replace(/_/g, " ") : null;
  const skipped = [...basis.matchAll(/skipped_families:([^+]+)/g)]
    .flatMap((match) => match[1].split(","))
    .map((family) => family.replace(/_/g, " ").trim())
    .filter(Boolean);
  const blocks = gate.blocks_publish === true
    ? "blocks publishing"
    : gate.blocks_publish === false ? "does not block publishing" : null;
  return [
    found,
    skipped.length ? `${listed(skipped)} not checked` : null,
    blocks,
  ].filter(Boolean).join("; ");
}

function buildGates(checks: RawChecks): GateRow[] {
  return checks.gates.map((gate, index) => {
    const withheld = gate.withheld ?? [];
    const recordedName = gate.gate_name ?? gate.check_id ?? null;
    const mark = markOf(GATE_MARKS, gate.result);
    return {
      key: String(gate.check_id ?? gate.gate_name ?? index),
      name: recordedName ? codeWords(String(recordedName), GATE_NAMES) : "A gate with no recorded name",
      code: recordedName ? String(recordedName) : null,
      mark,
      detail: gateDetail(gate),
      // Only where the gate did not pass. Its details are withheld because
      // they can hold error text; the failure code is what the reader can
      // search for, so that is what the line gives them.
      withheldNote: withheld.length && mark.severity !== "ok"
        ? "Error details are kept out of this page; the failure code is "
          + `${gate.reason ?? "not recorded"}.`
        : null,
    };
  });
}

function checksSentence(checks: RawChecks): string | null {
  if (checks.total === null) return null;
  const parts = [`${exact(checks.total)} ${noun(checks.total, "check")} ran`];
  if (checks.passed !== null && checks.passed > 0) parts.push(`${exact(checks.passed)} passed`);
  if (checks.blocked !== null && checks.blocked > 0) {
    parts.push(`${exact(checks.blocked)} blocked the publish`);
  }
  return `${parts.join(", ")}.`;
}

function runTiming(run: RawViewRun): string {
  if (!run.started_at || !run.completed_at) return "";
  const span = Date.parse(run.completed_at) - Date.parse(run.started_at);
  if (!Number.isFinite(span) || span < 0) return "";
  return `took ${exact(span)} ms`;
}

/** One configured entry: what it says, or which kind of absence it is. */
interface ConfiguredEntry {
  line: string | null;
  /** "never" -- the attempt recorded no such key at all. "empty" -- it
   *  recorded the key and it held nothing. The read works to keep these
   *  apart and the page must not merge them back into one word. */
  absence: "never" | "empty" | null;
}

function nameEntry(list: RawNameList, subject: string): ConfiguredEntry {
  if (!list.recorded) return { line: null, absence: "never" };
  const withheld = list.withheld_count
    ? ` ${exact(list.withheld_count)} ${noun(list.withheld_count, "entry")} withheld.`
    : "";
  if (!list.names.length) {
    return { line: withheld ? withheld.trim() : null, absence: "empty" };
  }
  return { line: `${subject}: ${list.names.join(", ")}.${withheld}`, absence: null };
}

/**
 * What was configured when the attempt ran, and what it did not record.
 *
 * On the live deployment this section was four sentences of absence in a row
 * -- no attestation, no policy bundle, no excluded source, no enricher
 * governance record -- followed by one fact. Four ways of saying nothing is a
 * section the reader learns to skip. Only what IS recorded gets a line; the
 * absences collapse into one, and the section is cut whole when the attempt
 * recorded nothing in it at all.
 */
function configured(raw: RawConfigured): {
  configuredLines: string[];
  configuredAbsences: string[];
} {
  const governance = raw.enricher_governance_record_count;
  const entries: [string, ConfiguredEntry][] = [
    ["attestation", nameEntry(raw.deployment_attestations, "Attestations")],
    ["policy bundle", nameEntry(raw.nomos_policy_bundle_ids, "Nomos policy bundles")],
    ["source exclusions", nameEntry(raw.excluded_sources, "Sources excluded by the publication gate")],
    ["enricher governance", governance === null
      ? { line: null, absence: "never" }
      : governance === 0
        ? { line: null, absence: "empty" }
        : {
          line: `${exact(governance)} enricher governance `
            + `${noun(governance, "record")}.`,
          absence: null,
        }],
    ["catalog version", raw.catalog_version_id === null
      ? { line: null, absence: "never" }
      : { line: `Schema version ${exact(raw.catalog_version_id)}.`, absence: null }],
  ];
  const lines = entries
    .map(([, entry]) => entry.line)
    .filter((line): line is string => line !== null);
  // One plain line for what was set and held nothing. What the attempt never
  // recorded at all is not listed: "Not recorded for this attempt:
  // attestation" told the reader nothing they could act on.
  const empty = entries
    .filter(([, entry]) => entry.absence === "empty")
    .map(([name]) => EMPTY_WORDS[name] ?? name);
  const absences = empty.length
    ? [`No ${listed(empty, "or")} was set.`]
    : [];
  // Nothing recorded at all: the section is cut, and an absent section says
  // nothing, which is the honest answer.
  if (!lines.length) return { configuredLines: [], configuredAbsences: [] };
  return { configuredLines: lines, configuredAbsences: absences };
}

/** The configured entries, singular, as the one "was set" line names them. */
const EMPTY_WORDS: Readonly<Record<string, string>> = {
  attestation: "attestation",
  "policy bundle": "policy bundle",
  "source exclusions": "excluded source",
  "enricher governance": "enricher governance record",
};

/** " Sources whose runs wrote since the previous version: jira." -- the
 *  sources whose runs recorded a write between
 *  the previous version and this one. Empty where the read served no window
 *  or no source wrote. */
function writtenBy(raw: RawSourceWrites | undefined): string {
  if (!raw || raw.unavailable) return "";
  const writers = raw.rows
    .filter((row) => (row.records_written ?? 0) + (row.relationships_written ?? 0) > 0)
    .map((row) => row.source_name);
  return writers.length ? ` Sources whose runs wrote since the previous version: ${listed(writers)}.` : "";
}

/**
 * The read's own unpublished-version marker, from wherever it set one.
 *
 * The composer sets it on `holds` and on all three count sections at once.
 * Reading all four rather than one means the sentence survives a blocked
 * attempt whose rollup came back empty -- no count table renders, but the
 * holds numbers do, and they are exactly what the sentence qualifies.
 */
function unpublishedMarker(page: RawAttemptDetail["payload"]): RawMarker | null {
  return [
    page.holds.unpublished_version,
    page.by_record_type.unpublished_version,
    page.by_relationship_type.unpublished_version,
    page.by_source.unpublished_version,
  ].find((marker): marker is RawMarker => !!marker) ?? null;
}

export function buildRecordedDetail(
  raw: RawAttemptDetail,
  { link }: { link: LinkFn },
): RecordedDetail {
  const page = raw.payload;
  const holds = page.holds;
  const names: SourceNames = new Map(
    (page.source_writes?.rows ?? [])
      .filter((row) => row.ownership_id)
      .map((row) => [row.ownership_id as string, row.source_name]),
  );
  const tables = TABLE_TITLES
    .map(([key, title, unit]) => buildTable(
      key as string, title, unit, page[key] as RawCountSection, link, names,
    ))
    .filter((table): table is CountTable => table !== null);

  const holdsParts = [
    holds.node_count === null ? null : `${exact(holds.node_count)} ${noun(holds.node_count, "record")}`,
    holds.edge_count === null ? null : `${exact(holds.edge_count)} ${noun(holds.edge_count, "relationship")}`,
  ].filter(Boolean);

  const checks = page.checks;
  const checksMarker = checks.unavailable;
  // A consistency check that ran for this attempt is listed ONCE, under
  // Publication checks, which carries its result. Its run row is the same run
  // (the same run id) and printed a second duration beside the first.
  const gateRuns = new Set(
    checks.gates.map((gate) => gate.run_id).filter((id): id is string => Boolean(id)),
  );
  const producerRuns = page.runs.runs.filter((run) => !gateRuns.has(run.run_id));
  const listedElsewhere = page.runs.runs.length - producerRuns.length;
  const runsShown = producerRuns.slice(0, RUNS_SHOWN);
  const older = page.neighbours.older_attempt_id;
  const newer = page.neighbours.newer_attempt_id;
  const parent = page.neighbours.parent_generation_id;

  return {
    // Driven by the read's own marker and by nothing else. The page does not
    // infer it from `version_published` plus a row count: that inference
    // printed the sentence on a payload whose sections carried no marker,
    // which is a claim the read did not make -- and it stayed silent on a
    // payload that DID carry one whenever the flag disagreed.
    unpublishedNote: unpublishedMarker(page)
      ? "These counts describe a version that was not published. They are what "
        + "the refused cycle staged, not what the graph holds."
      : null,
    holdsLine: holdsParts.length
      ? `This version holds ${holdsParts.join(" and ")}.`
      : null,
    movedLine: (() => {
      const moved = movedSentence(holds.nodes, holds.edges);
      return moved ? `${moved}${writtenBy(page.source_writes)}` : null;
    })(),
    holdsNotes: [
      ...holds.unavailable.map(
        (marker) => markerSentence(marker, "Part of this version's totals was not recorded"),
      ),
      ...missingMoves(holds.nodes, "Records"),
      ...missingMoves(holds.edges, "Relationships"),
    ],
    tables,
    checksNote: checksMarker
      ? checksMarker.reason === "consistency_gates_not_recorded"
        // Never "0 checks ran, all passed": this deployment recorded no
        // consistency check at all, which is not the same fact as a check
        // that ran and found nothing.
        ? "No consistency check is recorded for this attempt. This deployment "
          + "configured none when it ran."
        : markerSentence(checksMarker, "The publication checks could not be read")
      : null,
    gates: buildGates(checks),
    checksSummary: checksMarker ? null : checksSentence(checks),
    showRuns: !(listedElsewhere > 0 && producerRuns.length === 0
      && page.runs.total === listedElsewhere && !page.runs.unavailable),
    runs: runsShown.map((run) => ({
      key: run.run_id,
      name: run.definition_id,
      // `consistency_check` is a code; the tag reads it as words.
      kind: (run.artifact_kind ?? "").replace(/_/g, " "),
      mark: markOf(RUN_MARKS, run.status),
      timing: runTiming(run),
      // The producer's own error, as data. The read already withheld its
      // message and traceback; whatever survives is set as escaped text.
      errorText: run.error === null || run.error === undefined
        ? null
        : typeof run.error === "string" ? run.error : JSON.stringify(run.error, null, 2),
    })),
    // Three different answers, never merged. The read could not look; it
    // looked and found none; it found some.
    runsNote: page.runs.unavailable
      ? markerSentence(
        page.runs.unavailable,
        "The producer runs for this attempt could not be looked up",
      )
      : page.runs.total === 0
        ? "No per-producer result is recorded for this attempt."
        : null,
    runsCapLine: page.runs.total - listedElsewhere > runsShown.length
      ? `Showing ${exact(runsShown.length)} of ${exact(page.runs.total - listedElsewhere)} recorded `
        + `producer runs for this attempt.`
      : null,
    // The position is a claim about where this attempt sits in the whole
    // history, and the read can only make it for an attempt inside the window
    // it scanned. For an older one it says what it DOES know -- how many
    // attempts the deployment recorded -- rather than printing a place in a
    // list this read never built.
    positionLine: page.neighbours.position === null
      ? `One of ${exact(page.neighbours.total)} attempts in this deployment's `
        + "recorded history; this attempt is older than the window this read "
        + "scanned, so its place in the list was not read."
      : page.neighbours.total === 1
        ? "The only attempt in this deployment's history."
        : `${fromNewest(page.neighbours.position, page.neighbours.total)} of `
          + `${exact(page.neighbours.total)} attempts in this deployment's history.`,
    // "the predecessor this checkpoint names" was the runtime's wording for a
    // plain fact: the version this one was built on (review 2026-09-22).
    parentLine: parent
      ? `Built on ${parent.slice(-12)}, `
        + (page.neighbours.parent_source === "recorded_predecessor"
          ? "the version before it."
          : "the attempt listed before it; this attempt did not record what it built on.")
      : page.neighbours.older_attempt_unavailable_reason
        ? "This attempt did not record what it built on, and the attempt before "
          + "it is older than the window this read scanned."
        : "This is the first version: no earlier attempt is recorded.",
    newerHref: newer ? link(`updates/${newer}`) : null,
    olderHref: older ? link(`updates/${older}`) : null,
    newerId: newer ? newer.slice(-12) : null,
    olderId: older ? older.slice(-12) : null,
    ...configured(page.configured),
  };
}
