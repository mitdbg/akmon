// One producer's page: what the reads record about it, and its runs.
//
// Four things the mock drew are gone, because no read supplies them and the
// page must not invent them. Each is in the change's mismatch ledger:
//
//   1. The registry entry as YAML, with its adapter, settings, scope and
//      cadence. Serving the registry document means serving
//      credentials-adjacent configuration, which needs server-side redaction
//      designed as its own change rather than a client-side scrub.
//   2. A run's error text, and the cause sentence above it.
//      `okg.v_source_run_diagnostics` records no message column, so a failed
//      run has an outcome and a phase and no text.
//   3. "Open in Updates". The link needed an attempt whose `stopped_producer`
//      is this producer; `console/generations` sets that key to null on every
//      attempt because no checkpoint column records it (issue #2120), so the
//      link could never be drawn.
//   4. "Runtime keys folded into this row". The read returns each producer
//      once, so there is nothing folded.
//
// A producer's records by type IS recorded -- the count rollup's
// `source_subtype` dimension -- and is drawn. So is an enricher's latest
// issue, in all three of the states that record one. A SOURCE's withheld or
// excluded reason is not recorded anywhere and is a fifth cut: runtime
// readiness `convergence`, which is where a source's workflow comes from,
// carries a state and active work and no policy reason at all.
import { causeWords, STATES_WITH_A_PRINTED_CAUSE } from "@operator-ui/console/model/sources/causes";
import { noun } from "@operator-ui/console/model/sources/list";
import { type ProducerRecord, runGlyph } from "@operator-ui/console/model/sources/producers";
import { ATTENTION_STATES, freshnessWords, producerGlyph, stateOf } from "@operator-ui/console/model/shared/producerStates";
import type { ProducerDetailRead, RawProducerRun, RawRecovery } from "@operator-ui/console/model/sources/reads";
import { RUNS_PAGE, hasRun, inWords } from "@operator-ui/console/model/sources/rows";
import { exact, plainCause, shareLabel } from "@operator-ui/console/model/shared/format";
import type { ProducerKind } from "@operator-ui/console/model/shared/reads";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { Glyph } from "@operator-ui/console/model/shared/status";
import { duration, relativeAge, utcStamp, utcStampSeconds } from "@operator-ui/console/model/shared/time";

// --- one producer ---------------------------------------------------------------

export interface RunRow {
  key: string;
  /** The recorded UTC time, to the second. */
  when: string;
  /** The relative age, printed beside it in muted text. */
  whenAge: string;
  whenExact: string;
  mark: Glyph;
  /** What the run read. No read records it: every served run carries
   *  `items_in: null`, so this says so rather than printing the pushed-event
   *  count (0 for every pulled run) as "0 read". */
  read: string;
  /** What the run wrote: "571 records, 692 relationships" where the read
   *  splits it, else the combined count it served. */
  wrote: string;
  /** The same, short enough for the table's cell: "571 + 692". */
  wroteShort: string;
  duration: string;
  /** The SLOWEST phase the run recorded, in words where this page knows the
   *  phase, else the recorded code. Not the last phase: no read records the
   *  order phases ran in (`last_phase` is served null with that reason), so
   *  the column is headed "Slowest step". */
  phase: string;
  /** The recorded code behind `phase`, when `phase` is words; null when
   *  `phase` already is the code. Printed once, in the cell's title. */
  phaseCode: string | null;
}

/**
 * Run phases, in words.
 *
 * A run records the deepest phase it reached as a dotted code; Archiv3's
 * three jira runs each read `source_runner.flush.chronos_apply` in the
 * table (review 2026-09-22). A code this map does not know prints as itself.
 */
export const PHASE_WORDS: Readonly<Record<string, string>> = {
  "source_runner.flush.chronos_apply": "writing its batch to the graph",
};

// The credential patterns live with the YAML composer, which is the one
// place a setting is printed; re-exported so the tests that hold them keep
// their import.

export interface ProducerDetail {
  name: string;
  kind: ProducerKind;
  configured: boolean;
  mark: Glyph;
  /** The 13 px line under the name, each entry a whole phrase, in words. */
  meta: string[];
  /** What the operator does next, and where, for a producer that needs
   *  something: this console starts nothing. `command` is the one the read
   *  recorded, printed once, on this page only. Null where nothing is due. */
  nextStep: { sentence: string; command: string | null } | null;
  /** The state code the read served, printed once in a muted line of its
   *  own beneath the words. Null when no state was recorded. */
  stateCode: string | null;
  /** Records and relationships as figures. A view has rows, not records. */
  figures: { value: number; label: string }[];
  /** Under the figures. */
  figuresNote: string;
  /** False for a producer with no configuration: no detail read is asked
   *  for it, so its page is the list row alone. The sections a detail read
   *  fills then say they were not read. */
  detailRead: boolean;
  runs: RunRow[];
  /** Under the run table where no detail read was made, so an empty section
   *  is not read as "this producer never ran". Null otherwise. */
  runsNote: string | null;
  /** How many runs exist, or null when the read's query was capped. */
  runTotal: number | null;
  /** Why runTotal is null, as a plain sentence, or null. */
  runTotalNote: string | null;
  /** "2 runs · 1 failed". */
  runsLine: string;
  /** Under the run table where a run's write is split: what "571 + 692"
   *  counts. Null where no run carries the split. */
  wroteNote: string | null;
  /** True when older runs exist that this read did not fetch. */
  moreRuns: boolean;
  /** The issue the producer's read recorded, verbatim, where one did. Only
   *  the enricher read records one; a source's and a view's are null with the
   *  reason beside them, and this is null in turn. */
  /** Why the producer is in the state it is, in words, from the code the
   *  convergence read recorded. Null where the state is one whose cause would
   *  only restate it, and where no code was recorded. */
  causeSentence: string | null;
  /** How many incidents the source read counted for this producer, when it
   *  counted any. Null at zero: "0 incidents" is a line that says nothing. */
  incidentCount: number | null;
  /** Two recorded counts for this producer that contradict each other, in
   *  one sentence. Null in the ordinary case. */
  countDisagreement: string | null;
  reasonTitle: "Withheld" | "Latest issue" | null;
  reasonSentence: string | null;
  reasonText: string | null;
  /** This producer's records grouped by record type, from the maintained
   *  count rollup's `source_subtype` dimension. */
  recordTypeRows: {
    type: string; records: number; share: number; shareLabel: string;
    /** Explore, filtered to this type; null where no link was supplied. */
    href: string | null;
  }[];
  /** How many types the read returned beyond those listed. */
  recordTypesOmitted: number;
  /** Set when the rollup could not be read for this version; the page prints
   *  it instead of the table. */
  recordTypesUnavailable: string | null;
  /** Whether the records-by-type section earns its space: a read that
   *  failed (news), or a split of two or more types. One row only restates
   *  the Contribution figure above it (review L13). */
  showRecordTypes: boolean;
}

export function buildProducerDetail(
  record: ProducerRecord,
  read: ProducerDetailRead | null,
  { nowMs, link }: { nowMs: number; link?: LinkFn },
): ProducerDetail {
  const row = read?.row ?? null;
  const runs: RunRow[] = (read?.runs ?? []).map((run, index) => ({
    // A run id when the read carries one; otherwise the time plus the row's
    // place, because two runs of one producer can share a recorded time.
    key: run.run_id ?? `${run.iso ?? "no-time"}#${index}`,
    // The recorded time is what tells two runs apart; the age is the reader's
    // orientation and sits beside it, muted. Three runs 33 seconds apart all
    // read "13 h ago", so the age alone distinguished nothing.
    when: run.iso ? utcStampSeconds(run.iso) : "time not recorded",
    whenAge: run.iso ? relativeAge(run.iso, nowMs) : "",
    whenExact: run.iso ? utcStamp(run.iso) : "",
    mark: runGlyph(run.outcome ?? ""),
    read: typeof run.items_in === "number" ? exact(run.items_in) : "not recorded",
    wrote: wroteWords(run),
    wroteShort: typeof run.records_written === "number" && typeof run.relationships_written === "number"
      ? `${exact(run.records_written)} + ${exact(run.relationships_written)}`
      : wroteWords(run),
    duration: run.duration_ms === null || run.duration_ms === undefined
      ? "" : duration(run.duration_ms),
    phase: slowest(run) ? PHASE_WORDS[slowest(run) as string] ?? (slowest(run) as string) : "",
    phaseCode: slowest(run) && PHASE_WORDS[slowest(run) as string] ? slowest(run) : null,
  }));

  // The run count, and why it may be absent. A read whose query was capped
  // has a floor, not a count: printing the rows in hand would say "ran twice"
  // of a producer that ran three times.
  const runTotal = read?.runTotal ?? null;
  const runTotalNote = read?.runTotalUnavailableReason
    ? `${inWords(read.runTotalUnavailableReason)}.`
    : null;
  const failedRuns = runs.filter((run) => run.mark.severity === "failed").length;
  const counted = runTotal ?? runs.length;
  // A producer with no configuration gets no detail read, so its runs and its
  // record types were never asked for. Saying "No run is recorded" and "holds
  // no record" there states as facts two things no read looked for.
  const detailRead = read !== null;
  const notRead = "No detail read was made for this producer: no configuration "
    + "names it, so nothing was asked for its runs or its record types.";
  // An enricher's runs are read from a WINDOW of published versions, not from
  // its whole history, so the window is stated wherever the count is. Without
  // it an enricher whose last run predates the window reads as one that never
  // ran. A source's runs are queried per source and carry no such window, so
  // it says nothing there rather than inventing a bound.
  const runsWindow = record.kind === "enricher" ? read?.enricherRunsWindow ?? null : null;
  const windowPhrase = runsWindow === null
    ? ""
    : ` · newest ${exact(runsWindow)} published ${noun(runsWindow, "version")}`;
  const runsLine = !detailRead
    ? "Run history not read"
    : runs.length === 0
      ? `No run is recorded${windowPhrase}.`
      : `${exact(counted)} ${noun(counted, "run")}`
        + (runTotal === null ? " so far" : "")
        + (failedRuns ? ` · ${exact(failedRuns)} failed` : "")
        + windowPhrase;

  const workflow = row?.workflow ?? record.workflow;
  const contribution = row?.contribution ?? record.contribution;
  const freshness = row?.freshness ?? record.freshness;
  // One recorded issue, whatever state recorded it. "Withheld" names the
  // state the operator acts on differently; every other state that records an
  // issue gets the neutral title, because the issue is a recorded class or a
  // fixed phrase and the state beside it already says which it is.
  const reason = workflow.latest_issue ?? null;
  // The convergence read's own verdict, in words, where the state is one the
  // reader opened this page to understand. A fresh producer's reason is not
  // printed: "fresh, because the watermark is within the window" is the state
  // said twice.
  //
  // Nothing here explains the gap between a run's ITEMS OUT and the records
  // the version holds. No read carries anything that would: the run row's
  // `output_count` and the rollup's per-producer count are two different
  // measurements over two different horizons, and no third read reconciles
  // them. So this prints nothing about it rather than a sentence the reads do
  // not support.
  const recovery = workflow.recovery ?? null;
  const causeSentence = STATES_WITH_A_PRINTED_CAUSE.includes(stateOf(record))
    ? constraintWords(workflow.reason ?? null, recovery) ?? causeWords(workflow.reason ?? null)
    : null;
  const needsAction = ATTENTION_STATES.includes(stateOf(record))
    || freshness.status === "stale";
  const nextStep = needsAction ? nextStepFor(record.name, recovery) : null;
  const incidents = (row?.health ?? record.health).incident_count;
  const incidentCount = typeof incidents === "number" && incidents > 0
    ? incidents
    : null;
  const reasonTitle = reason
    ? (stateOf(record) === "withheld" ? "Withheld" : "Latest issue")
    : null;

  const ran = hasRun(record);
  const lastRunIso = (row?.latest_run ?? record.latestRun)?.iso ?? null;
  // Whole phrases, each one a fact some read recorded. There is no scope, no
  // adapter and no cadence: they live in the registry entry, which no read
  // returns.
  // The state's own served code leads, because this page is where a reader
  // who needs the code the read returned comes to find it: the table above
  // prints the word. Freshness follows the same rule as the table's cell, so
  // the two pages cannot word one recorded state two ways.
  //
  // With no state recorded there is no code to lead with, and the entry read
  // "recorded state not recorded" one line under a chip already reading
  // "State not recorded" -- the same words twice. The chip is the statement;
  // this entry exists to carry the served CODE beside it. Every source on
  // lane-d-home-render is in that case, because that deployment has no
  // runtime to observe.
  // The state's WORD is the chip beside the name; its served code is printed
  // once, in a muted line of its own (`stateCode`), where a reader matching
  // the page to a log finds it. It used to lead this line as "recorded state
  // manual_required" -- a raw code in the sentence (review 2026-09-22).
  const meta = [
    // "freshness no schedule" read as a broken sentence; the phrase stands
    // alone where nothing declares a schedule.
    freshness.status === "not_configured"
      ? freshnessWords(freshness.status)
      : `freshness ${freshnessWords(freshness.status)}`,
    lastRunIso ? `last run ${relativeAge(lastRunIso, nowMs)}` : "never run",
  ];

  // Records by type, with each type's share of this producer's total so the
  // reader can see the shape without doing the division.
  const byType = read?.recordsByType ?? null;
  const typeTotal = byType?.total ?? 0;
  const typeRows = (byType?.types ?? []).map((entry) => ({
    type: entry.type,
    records: entry.records,
    share: typeTotal > 0 ? (entry.records / typeTotal) * 100 : 0,
    // The same rule Home's Contents panel prints by, from the same function:
    // a type that holds rows never reads "0%", whatever it rounds to.
    shareLabel: shareLabel(entry.records, typeTotal),
    href: link ? link("explore", { type: entry.type }) : null,
  }));
  const typesUnavailable = !detailRead
    ? notRead
    : byType && byType.status !== "available"
      ? `Records by type could not be read for this version${
        byType.reason ? ` (${inWords(byType.reason)})` : ""
      }. Nothing was substituted for it.`
      : null;
  // Review L13: the section is worth its space only when it SPLITS the total.
  // One row restates the Contribution figure directly above it in a table
  // whose share column can only read 100%. A read that failed still shows,
  // because "could not be read" is news and an absent section is not.
  //
  // One row is shown too (review 2026-09-22): without it a single-type
  // source never named its type anywhere on the page.
  const showRecordTypes = Boolean(typesUnavailable) || typeRows.length >= 1;

  // A view builds rows and emits no relationships, so it shows one figure.
  // A figure the read did not carry is dropped, never printed as a zero: the
  // relationships figure used to read `?? 0`, so a producer whose count the
  // read withheld showed "0 relationships" -- which is a claim, and
  // indistinguishable from a producer that really emitted none.
  const figures = (record.kind === "view"
    ? [{ value: contribution.records_in_gen, label: "rows built" }]
    : [
      { value: contribution.records_in_gen, label: "records" },
      { value: contribution.edges_in_gen, label: "relationships" },
    ]
  ).filter(
    (figure): figure is { value: number; label: string } =>
      typeof figure.value === "number",
  );

  return {
    name: record.name,
    kind: record.kind,
    configured: record.configured,
    mark: producerGlyph(stateOf(record)),
    meta,
    nextStep,
    stateCode: record.workflow.state ?? null,
    detailRead,
    figures: ran ? figures : [],
    figuresNote: ran
      ? (record.kind === "view" ? "in the last build" : "in the selected version")
      : detailRead
        ? "This producer has not run, so no version holds its work."
        : "No run is recorded for this producer in the producers list.",
    runs: runs.slice(0, RUNS_PAGE),
    // A producer the read answered for that has no run: what is true, and
    // where to look next, instead of a bare "No run is recorded" beside
    // figures that say it contributed (audit 2026-09-23, empty-zero-runs).
    runsNote: !detailRead
      ? notRead
      : runs.length === 0
        ? "The run history holds no run of this producer. Check that it is scheduled "
          + "and that the worker that runs it is up."
        : null,
    runTotal,
    runTotalNote,
    runsLine,
    wroteNote: runs.some((run) => run.wroteShort !== run.wrote)
      ? "Wrote is records + relationships each run wrote; what a run read is not recorded."
      : null,
    moreRuns: Boolean(read?.moreRuns),
    causeSentence,
    incidentCount,
    countDisagreement: record.countDisagreement,
    reasonTitle,
    reasonSentence: reason ? plainCause(reason) : null,
    reasonText: reason,
    recordTypeRows: typeRows,
    recordTypesOmitted: read?.recordsByType?.omitted ?? 0,
    recordTypesUnavailable: typesUnavailable,
    showRecordTypes,
  };
}

/** The phase a run row names: the served `slowest_phase`, or `phase`, which
 *  carries the same value on a read that predates the split. */
function slowest(run: RawProducerRun): string | null {
  return run.slowest_phase ?? run.phase ?? null;
}

/** "571 records, 692 relationships" where the read split the write; the
 *  combined count where it did not; blank where it recorded neither. */
function wroteWords(run: RawProducerRun): string {
  const records = run.records_written;
  const relationships = run.relationships_written;
  if (typeof records === "number" && typeof relationships === "number") {
    return `${exact(records)} ${noun(records, "record")}, `
      + `${exact(relationships)} ${noun(relationships, "relationship")}`;
  }
  return typeof run.items_out === "number" ? exact(run.items_out) : "";
}

/**
 * Which of the two halves of `manual_or_credential_constraint` applies.
 *
 * The code says "a manual run OR a credential"; the recovery read says
 * which, and the page used to print both and leave the reader to guess.
 */
function constraintWords(reason: string | null, recovery: RawRecovery | null): string | null {
  if (reason !== "manual_or_credential_constraint" || !recovery) return null;
  if (recovery.needs_credential && recovery.needs_manual_run) {
    return "needs a credential and a manual run to refresh";
  }
  if (recovery.needs_credential) return "needs a credential to refresh";
  if (recovery.needs_manual_run) return "needs a manual run to refresh";
  return null;
}

/**
 * The next step, in one line, where the operator has one.
 *
 * This console is read-only and says so at the point it matters. Where the
 * read recorded the command that brings the source back it is printed --
 * once, and only here: summary pages never print a command.
 */
export function nextStepFor(
  name: string, recovery: RawRecovery | null,
): { sentence: string; command: string | null } {
  const lead = "This console cannot start a run.";
  if (recovery?.command) {
    return { sentence: `${lead} Refresh it from the command line:`, command: recovery.command };
  }
  if (recovery?.needs_credential) {
    return {
      sentence: `${lead} Refresh it from the command line with the ${name} credential.`,
      command: null,
    };
  }
  if (recovery?.needs_manual_run) {
    return { sentence: `${lead} Someone runs it by hand from the command line.`, command: null };
  }
  return { sentence: `${lead} Refresh it from the command line.`, command: null };
}
