// The Home view's model: raw console read payloads in, one fixed-size page of
// facts out.
//
// Everything here is pure. The view renders this model and adds no facts of its
// own, so the numbers on the page can be asserted in a unit test without a
// browser, and the "nothing grows with the deployment" rule is visible as a
// cap in code rather than as a hope about CSS.
//
// Two rules are enforced HERE rather than in the component:
//
//   1. Every list is bounded. `NEEDS_ATTENTION_CAP` and `CONTENTS_CAP` cut the
//      lists and the remainder is stated as a fact, never dropped.
//   2. A missing number is never rendered as a plausible zero. A read that did
//      not answer raises out of the loader (fixtureClient refuses undefined
//      reads); a field the backend does not record yet is carried as `null` and
//      the copy says what is not known.
import {
  attentionProducers, buildAttention, chipTargetId, type AttentionModel,
} from "@operator-ui/console/model/attention";
import { collapseAttempts, type AttemptRow } from "@operator-ui/console/model/attempts";
import {
  countByState, producerCounts, stateSegments,
  type CountLink, type ProducersModel,
} from "@operator-ui/console/model/counts";
import { runningPace } from "@operator-ui/console/model/pace";
import { noun, shareLabel } from "@operator-ui/console/model/shared/format";
import { producerGlyph } from "@operator-ui/console/model/shared/producerStates";
import {
  producerRecordsFrom, type ProducerRecord,
} from "@operator-ui/console/model/sources/producers";
import type { RawProducersRead } from "@operator-ui/console/model/sources/reads";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type {
  RawAttemptsByDay, RawGenerations, RawHealth, RawRollup, RawVersions,
} from "@operator-ui/console/model/shared/reads";
import {
  producersAnswered as producersRead, producersState, type ProducersRead,
} from "@operator-ui/console/model/shared/readState";
import type { Severity } from "@operator-ui/console/model/shared/status";
import { ageAlone, relativeAge, utcStamp } from "@operator-ui/console/model/shared/time";

/** At most five rows in each Contents table, then a tail row that states what
 *  the five leave out. */
export const CONTENTS_CAP = 5;

/** At most five producers named in Home's Sources block, then "and N more". */
export const SOURCES_BLOCK_CAP = 5;

// --- the model ------------------------------------------------------------

/** The one chip beside the deployment name. */
export interface StatusChip {
  label: string;
  severity: Severity;
  /** The DOM id of the Needs attention row this chip is about, where it is
   *  about one: "Failing check" rests on one runtime check, and the chip
   *  jumps to it. */
  targetId?: string | null;
}

/** The chip a health verdict states, for the two verdicts that are not
 *  "healthy". Amber for degraded, red for failing: the same two colours the
 *  rows below the header use for a warning and a failure. */
export const VERDICT_CHIPS: Record<string, StatusChip> = {
  failing: { label: "Failing check", severity: "failed" },
  degraded: { label: "Degraded", severity: "never-run" },
};

/**
 * The header must not contradict the page under it.
 *
 * Two different facts drive the two: the chip said what the newest attempt
 * did, and Needs attention lists what the health read found. On live Archiv3
 * the newest attempt published cleanly and a runtime check was failing, so the
 * header said "Up to date" three lines above a red "Failed" row and the count
 * "Needs attention · 1".
 *
 * The publication state only wins where the read agrees nothing is wrong. Once
 * the verdict is `degraded` or `failing` the chip names the verdict instead:
 * it is the worse of the two states, and it is the one the rest of the page is
 * about. The publication state is not lost -- the top bar's version picker
 * says which version reads use and how old it is, which is what "Up to date"
 * was standing in for.
 *
 * A chip that already carries a bad state (blocked, updating, never published)
 * contradicts nothing and is left alone: it is more specific than the verdict,
 * and it names the attempt the operator has to look at.
 */
export function agreeWithVerdict(chip: StatusChip, verdict: string): StatusChip {
  if (chip.severity !== "ok") return chip;
  return VERDICT_CHIPS[verdict] ?? chip;
}

/** The chip while the producer reads are outstanding. `producers = []` is not
 *  "nothing is wrong": it is "nothing has answered yet", and the chip said
 *  "Up to date" over it. */
export const PRODUCERS_PENDING_CHIP: StatusChip = {
  label: "Reading the producers\u2026", severity: "unrecorded",
};

/** The chip when the producer reads FAILED. The page cannot say the sources
 *  are fine, and it cannot say they are not; it says it did not read them.
 *  Amber, because an operator has something to do about it. */
export const PRODUCERS_UNREAD_CHIP: StatusChip = {
  label: "Producers not read", severity: "never-run",
};

/**
 * The header must not be calmer than the Sources block either.
 *
 * `agreeWithVerdict` covers the health read. It does not cover the producers:
 * on Archiv3 the health verdict came back `healthy` while two of the six
 * sources recorded `blocked` and one `manual_required`, because the health
 * composer's checks are about the runtime and the graph, not about whether
 * each source converged. The chip said "Up to date" over three sources that
 * were not going to update.
 *
 * "Needs attention" rather than a state word, because there may be several
 * and they may not agree; the block below names them. Amber, not red: a
 * blocked source is something to act on, not a failure of the deployment.
 * A chip that already carries a bad state is more specific and is left alone.
 */
export function agreeWithProducers(
  chip: StatusChip, producers: ProducerRecord[], state: ProducersRead,
): StatusChip {
  if (chip.severity !== "ok") return chip;
  if (state === "pending") return PRODUCERS_PENDING_CHIP;
  if (state === "failed") return PRODUCERS_UNREAD_CHIP;
  if (attentionProducers(producers).length === 0) return chip;
  return { label: "Needs attention", severity: "never-run" };
}

export interface IdentityModel {
  /** The chip: what state the deployment is in, in two or three words. */
  chip: StatusChip;
  /** Present only on a deployment that has never published: the one fact
   *  the chip and the top bar's version picker cannot give. */
  neverPublishedSentence: string | null;
  deployment: string;
  version: string | null;
  fullVersion: string | null;
  publishedAt: string | null;
  publishedAge: string | null;
  /** `empty` only: the configured producers that have never run. */
  neverRunSentence: string | null;
}

/** The four numbers the page opens with. */
export interface HeroFigure {
  value: number;
  label: string;
  /** What the newest published version added, when a published attempt records
   *  one worth printing. Null on a figure no attempt reports a delta for, and
   *  null on a delta of zero. */
  delta: number | null;
}

/**
 * A delta of zero is not printed.
 *
 * Live Archiv3 published two versions in a row that added nothing, and the
 * figures carried a small grey "0" beside "8,941 records" and "7,855
 * relationships". A zero in a count strip occupies the place a fact goes and
 * says only that the fact is absent. "8,941 records" already says the version
 * holds what the last one held; a signed delta is printed where it changes
 * that reading, in either direction.
 */
export function printableDelta(value: number | null | undefined): number | null {
  return value === null || value === undefined || value === 0 ? null : value;
}

/** What the metric chart draws from: the attempts-by-day read for the chosen
 *  span and the (capped) generations read. The chart's own model is in
 *  metricsModel.ts; the page hands it the reads and the metric select does
 *  the rest. */
export interface ChartInputs {
  attempts: RawAttemptsByDay;
  generations: RawGenerations;
}

/** One row of the Contents panel: a type, how many of it, and what share of the
 *  version it is. */
export interface ContentsRow {
  name: string;
  count: number;
  /** Rounded to a whole percent, for the bar's width. Can be 0 for a type
   *  that holds rows; `shareLabel` is what the page prints. */
  share: number;
  /** The share as the panel prints it. Never "0%" for a non-zero count:
   *  `blocks` holds 2,050 of okg-workspace's 1,037,093 relationships, which
   *  rounds to zero, and the panel said a type with two thousand rows in it
   *  was 0% of the version. */
  shareLabel: string;
}

/** What the metric chart draws from: the attempts-by-day read for the chosen
 *  span and the (capped) generations read. The chart's own model is in
 *  metricsModel.ts; the page hands it the reads and the metric select does
 *  the rest. */
export interface ChartInputs {
  attempts: RawAttemptsByDay;
  generations: RawGenerations;
}

export interface ContentsModel {
  records: number;
  relationships: number;
  recordTypes: number;
  relationshipTypes: number;
  /** The five largest record types. Five, not all 214: the full table belongs
   *  on Explore's landing. */
  topRecordTypes: ContentsRow[];
  /** How many record types the five leave out, and how many records they hold. */
  moreTypes: number;
  moreTypeRecords: number;
  /** The five largest relationship types, in the same shape and the same table
   *  as the record types. The operator read the one-line form -- "supersedes
   *  52%, mentions 35%, authored_by 9%" -- and rejected it: the same fact in
   *  two different forms on one panel makes the reader translate between them
   *  before comparing them. */
  topRelationshipTypes: ContentsRow[];
  moreRelationshipTypes: number;
  moreRelationshipCount: number;
  /** Printed INSTEAD of a table, when there is no type to put in one. Two
   *  separate sentences, because a version can hold records and no
   *  relationships. */
  recordsEmptyLine: string | null;
  relationshipsEmptyLine: string | null;
}

/** How many of a type list the top `limit` leave out, and how many rows those
 *  omitted types hold. Stated, never dropped. */
export function tailOf(
  rows: { type: string; count: number }[],
  limit: number,
): { types: number; count: number } {
  const rest = [...rows]
    .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type))
    .slice(limit);
  return { types: rest.length, count: rest.reduce((sum, row) => sum + row.count, 0) };
}

/** The top `limit` types with their share of the total. */
export function topTypes(
  rows: { type: string; count: number }[],
  limit: number,
): ContentsRow[] {
  const total = rows.reduce((sum, row) => sum + row.count, 0);
  if (total === 0) return [];
  return [...rows]
    .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type))
    .slice(0, limit)
    .map((row) => ({
      name: row.type,
      count: row.count,
      share: Math.round((row.count / total) * 100),
      shareLabel: shareLabel(row.count, total),
    }));
}

export interface HomeModel {
  identity: IdentityModel;
  hero: HeroFigure[];
  /** Null until the producers read lands, or when it failed. The Sources
   *  block prints which of the two in its own place. */
  producers: ProducersModel | null;
  /** True for the first of those two: the read is still outstanding. The
   *  Sources block and Needs attention each say so, because a short list that
   *  says nothing reads as a complete one. */
  producersPending: boolean;
  /** True while the attempts read for the CHOSEN span is outstanding. The
   *  chart keeps its frame and its span control and says which span it is
   *  loading; `chart === null` with this false is the read having failed. */
  attemptsPending: boolean;
  /** The same, for the ontology-rollup read behind Contents. */
  rollupPending: boolean;
  chart: ChartInputs | null;
  recentAttempts: AttemptRow[];
  noAttemptsSentence: string | null;
  attention: AttentionModel;
  contents: ContentsModel | null;
  footer: string;
  nowMs: number;
}

export interface HomeInputs {
  health: RawHealth;
  generations: RawGenerations;
  /** Null when the versions read failed. It contributes the publish time
   *  and whether a second published version exists; with it gone both are
   *  CUT and the page says which read failed. Nothing is defaulted. */
  versions: RawVersions | null;
  /** Null when the attempts-by-day read FAILED. It feeds the chart, the
   *  attempt counts and the page's read time; with none, the chart says what
   *  broke and the rest of Home still renders from the other reads. */
  attempts: RawAttemptsByDay | null;
  /**
   * Null until the producers read lands, and null when it failed.
   * `producersPending` tells the two apart; the page prints a different
   * sentence for each and never a zero for either.
   *
   * ONE READ, and the SAME ONE the Sources page draws. Home used to read the
   * three `*-diagnostics` routes and count their rows, which are inventories
   * of recorded keys rather than of producers: on okg-workspace the source
   * inventory holds `_enricher:code_reference` and
   * `_enricher:declarative:agent_message_mentions_pact_change_dir`, which are
   * enricher outputs under the key the count rollup writes, and Home's hero
   * figure therefore read "37 sources configured" over a Sources page that
   * said 35 of the same deployment. `console/producers` resolves that
   * identity server-side, so one population and one basis feed both pages.
   */
  producers: RawProducersRead | null;
  /** True while a read is still outstanding. The page renders every block
   *  whose own reads have landed, so these say which of the missing blocks
   *  are coming and which are not. */
  producersPending?: boolean;
  attemptsPending?: boolean;
  rollupPending?: boolean;
  /** What the producers read failed with, when it failed. The Sources block
   *  prints it, once (audit 2026-09-23: Needs attention printed it a second
   *  time, as "· undefined" when the route did not pass it). The model
   *  tells a failed read from an outstanding one by `producersPending`. */
  producersFailure?: unknown;
  /** Null when the ontology-rollup read FAILED. The Contents panel is the
   *  only thing that read feeds, so the rest of the page still renders and
   *  that panel says what broke, by name, in its place. Nothing is
   *  substituted: a zero record-type count would be indistinguishable from a
   *  version that really holds none. */
  rollup: RawRollup | null;
}

/**
 * The producers this page counts and lists: the read's own rows, once each.
 *
 * `producerRecordsFrom` is the Sources view's adapter, used here rather than
 * copied, so the two pages cannot drift into two populations. There is no
 * folding left to do -- the read returns each producer once, keyed by the
 * `ownership_id` its registry declares.
 */
export function collectProducers(inputs: HomeInputs): ProducerRecord[] {
  if (!inputs.producers) return [];
  return producerRecordsFrom(inputs.producers);
}

function buildIdentity(
  inputs: HomeInputs,
  producers: ProducerRecord[],
  nowMs: number,
  link: LinkFn,
): IdentityModel {
  const health = inputs.health.payload;
  const latest = inputs.generations.payload.generations[0] ?? null;
  const publishedAt = inputs.versions?.payload.versions[0]?.published_at ?? null;
  const fullVersion = health.generation_id;

  // No version at all. The page must say so rather than render a blank where a
  // version id belongs, and must say what IS configured.
  if (!fullVersion) {
    return {
      deployment: health.deployment,
      chip: { label: "Never published", severity: "never-run" },
      neverPublishedSentence: `No version has been published for ${health.deployment}.`,
      version: null,
      fullVersion: null,
      publishedAt: null,
      publishedAge: null,
      neverRunSentence: null,
    };
  }

  // No second sentence at all. The chip says what state the deployment is
  // in, and the top bar's version picker says which version reads use and
  // how old it is; a sentence repeating either is the slop this pass cuts.

  // What reads are on, as an adjective, for a chip that has to say the latest
  // attempt did not land. "Blocked 2 h ago" leaves the reader to look up at
  // the top bar and work out that reads are therefore five days old; this is
  // the one fact the chip was missing (story S1).
  const readsOn = `reads a ${ageAlone(publishedAt, nowMs)} old version`;

  let chip: StatusChip;
  if (latest && latest.status === "running") {
    const pace = runningPace(latest.duration_ms, inputs.generations);
    chip = {
      label: `Updating \u00b7 ${pace.label} \u00b7 ${readsOn}`,
      // Amber, not the calm blue, once an attempt has run past three times
      // what this deployment's last ten published attempts took. The page's
      // first element said "fine" while its fourth said no worker had
      // reported for six hours.
      severity: pace.late ? "never-run" : "running",
    };
  } else if (latest && latest.status === "blocked" && latest.full_generation_id !== fullVersion) {
    chip = {
      label: `Blocked \u00b7 ${latest.stopped_producer ?? latest.stopped_stage ?? "unrecorded"}`
        + ` \u00b7 ${relativeAge(latest.created_at, nowMs)} \u00b7 ${readsOn}`,
      severity: "blocked",
    };
  } else {
    // Nothing about the age of the read version on a deployment that is up to
    // date: the top bar's picker says it, and here it would be the reassurance
    // this page does not print.
    chip = { label: "Up to date", severity: "ok" };
  }

  return {
    deployment: health.deployment,
    chip: agreeWithProducers(
      agreeWithVerdict(chip, health.verdict), producers, producersState(inputs),
    ),
    neverPublishedSentence: null,
    version: shortVersion(fullVersion),
    fullVersion,
    publishedAt,
    publishedAge: relativeAge(publishedAt, nowMs),
    neverRunSentence: null,
  };
}

/** One sentence. The queue state is a Needs attention row when it is wrong and
 *  nobody's business when it is not; repeating it in the footer of every page
 *  was two facts where the reader needed one. */
/**
 * The footer: what the numbers count, and when they were read.
 *
 * Every age on this page is relative to a moment the page did not state. An
 * operator who left the tab open at lunch read "2 h ago" at four o'clock and
 * it was wrong by three hours. A read-only console with no refresh cannot do
 * without the read time.
 *
 * A deployment with no published version counts nothing, so the first clause
 * goes and the read time stays.
 */
function buildFooter(inputs: HomeInputs, nowMs: number): string {
  // The read time comes from the attempts read, which reports the moment the
  // server answered. With that read gone the page can only say when the
  // BROWSER drew it, and it says which of the two it is rather than passing
  // one off as the other.
  const readAt = inputs.attempts
    ? utcStamp(inputs.attempts.payload.now)
    : `${utcStamp(new Date(nowMs).toISOString())} by this browser's clock`;
  const fullVersion = inputs.health.payload.generation_id;
  if (!fullVersion) return `As of ${readAt}.`;
  // With no rollup there is no count basis to state, and stating one anyway
  // would attribute a basis to counts this page did not get.
  if (!inputs.rollup) return `As of ${readAt}.`;
  // "this version" made the reader work out which one from the bar, and on a
  // page whose bar was showing the version the URL ASKED for it was wrong.
  // The footer names the version the counts are of.
  const basis = countBasisWords(
    inputs.rollup.payload.count_basis, shortVersion(fullVersion) ?? fullVersion,
  );
  return `${basis} \u00b7 as of ${readAt}.`;
}

/** The words for a count basis the read returned.
 *
 *  The field is an ENUM shared with every other console read, not free text,
 *  so an unrecognised value is a version mismatch between this page and the
 *  server -- not something to print raw and hope the reader works out. It
 *  fails loud, naming the value, because a footer that states a basis the
 *  page does not understand is a claim about numbers nobody checked.
 */
export const COUNT_BASIS_WORDS: Record<string, string> = {
  published_total_before_access_rules:
    "Counts are totals for version {version}, before access rules",
};

export function countBasisWords(basis: string, version: string): string {
  const words = COUNT_BASIS_WORDS[basis];
  if (words === undefined) {
    throw new Error(
      `The ontology-rollup read returned a count basis this console does not `
      + `know: "${basis}". Known values: ${Object.keys(COUNT_BASIS_WORDS).join(", ")}.`,
    );
  }
  return words.replace("{version}", version);
}

/**
 * Home's Sources block.
 *
 * It used to be a state bar, a count strip, and a link reading "View all
 * sources (6)" -- which on Archiv3 printed the number 6 three times, once in
 * the figures above as well, and named none of the three sources that were
 * blocked or waiting on a person. The block now spends its space on WHICH
 * producers need something, capped at five, and says the rest as a number.
 *
 * Where one good state covers every producer there is nothing to name, so the
 * whole block is one line: "6 sources, all fresh".
 */
export function buildSourcesBlock(
  producers: ProducerRecord[], nowMs: number, link: LinkFn,
): ProducersModel {
  const total = producers.length;
  const states = countByState(producers);
  const onlyState = states.length === 1 ? states[0].state : null;
  const allGood = onlyState !== null && producerGlyph(onlyState).severity === "ok";
  // The kind counts are the hero figures' number again unless there is more
  // than one kind of producer to tell apart.
  const kinds = new Set(producers.map((row) => row.kind));
  const counts: CountLink[] = allGood
    ? []
    : producerCounts(producers, link).filter(
      (count) => kinds.size > 1 || !KIND_LABELS.has(count.label),
    );
  return {
    segments: stateSegments(producers),
    counts,
    total,
    href: link("sources"),
    allGoodLine: allGood && onlyState
      ? `${exactCount(total)} ${noun(total, kindWord(producers))}, `
        + `all ${producerGlyph(onlyState).word.toLowerCase()}`
      : null,
    linkLabel: `All ${exactCount(total)} ${noun(total, kindWord(producers))}`,
  };
}

/** The kind labels `producerCounts` emits, singular and plural, so the Sources
 *  block can drop them without matching on a number. */
const KIND_LABELS = new Set([
  "source", "sources", "enricher", "enrichers", "view", "views",
]);

/** "sources" where every producer is one, "producers" where they are mixed. */
function kindWord(producers: ProducerRecord[]): string {
  const kinds = new Set(producers.map((row) => row.kind));
  return kinds.size === 1 ? [...kinds][0] : "producer";
}

/** The count, grouped, without importing the view's formatter into the noun
 *  helper's argument list twice. */
function exactCount(value: number): string {
  return value.toLocaleString("en-US");
}

export function buildHomeModel(inputs: HomeInputs, link: LinkFn): HomeModel {
  const nowMs = inputs.attempts ? Date.parse(inputs.attempts.payload.now) : Date.now();
  const producers = collectProducers(inputs);
  const rollup = inputs.rollup?.payload ?? null;
  const attemptRows = collapseAttempts(inputs.generations, 4, nowMs);

  const sourceCount = producers.filter((row) => row.kind === "source").length;

  // A delta needs something to be a delta FROM. With one published version the
  // "+2,605" under "2,605 records" is the same number twice.
  // Unread is not "one version": with the list gone there is nothing to
  // compare against, so the delta is withheld rather than shown as a first
  // publish's full count. The sentence above the figures names the read.
  const hasPrevious = (inputs.versions?.payload.versions.length ?? 0) >= 2;
  const newestPublished = hasPrevious
    ? inputs.generations.payload.generations.find((a) => a.status === "published") ?? null
    : null;

  const attention = buildAttention(inputs, producers, nowMs, link);
  const identity = buildIdentity(inputs, producers, nowMs, link);
  const verdictChip = Object.values(VERDICT_CHIPS).some((chip) => chip.label === identity.chip.label);
  if (verdictChip) {
    identity.chip = { ...identity.chip, targetId: chipTargetId(attention.rows) };
  }

  return {
    identity,
    hero: [
      {
        value: inputs.health.payload.vitals.nodes_live,
        label: "records",
        delta: printableDelta(newestPublished?.nodes_added),
      },
      {
        value: inputs.health.payload.vitals.edges_live,
        label: "relationships",
        delta: printableDelta(newestPublished?.edges_added),
      },
      // The record-type figure comes from the rollup; with no rollup the
      // figure is absent rather than zero.
      ...(rollup ? [{
        value: rollup.node_type_total,
        label: noun(rollup.node_type_total, "record type"),
        delta: null,
      }] : []),
      // The same rule as the chip and the all-clear sentence: a count of the
      // producers is a claim about a read that has to have ANSWERED. While
      // the three diagnostics reads are outstanding this figure printed
      // "0 sources configured" on a deployment with six, beside three figures
      // that were real. The figure is absent until the reads answer -- it is
      // not a zero, and it is not a spinner in the place a number goes.
      ...(producersRead(inputs) ? [{
        value: sourceCount,
        // "sources configured", not "sources". The number counts every source
        // in the registry, including ones that have produced nothing into
        // this version: on lane-d-home-render two of the four (bootstrap-
        // fixture and docs) hold no record at all. Beside three figures that
        // are all counts of what this version HOLDS, the bare word read as
        // "four sources contributed", which is false. Showing two instead
        // would hide the configured sources that produced nothing, and that
        // is exactly what an operator needs to see.
        label: `${noun(sourceCount, "source")} configured`,
        delta: null,
      }] : []),
    ],
    producers: producersRead(inputs) ? buildSourcesBlock(producers, nowMs, link) : null,
    chart: inputs.attempts
      ? { attempts: inputs.attempts, generations: inputs.generations }
      : null,
    recentAttempts: attemptRows,
    noAttemptsSentence: attemptRows.length === 0
      ? "No update attempt has been recorded for this deployment."
      : null,
    attention,
    producersPending: producersState(inputs) === "pending",
    attemptsPending: inputs.attempts === null && Boolean(inputs.attemptsPending),
    rollupPending: inputs.rollup === null && Boolean(inputs.rollupPending),
    contents: rollup === null ? null : {
      records: inputs.health.payload.vitals.nodes_live,
      relationships: inputs.health.payload.vitals.edges_live,
      recordTypes: rollup.node_type_total,
      relationshipTypes: rollup.relationship_type_total,
      topRecordTypes: topTypes(rollup.node_types, CONTENTS_CAP),
      moreTypes: Math.max(rollup.node_type_total - CONTENTS_CAP, 0),
      moreTypeRecords: tailOf(rollup.node_types, CONTENTS_CAP).count,
      topRelationshipTypes: topTypes(rollup.relationship_types, CONTENTS_CAP),
      moreRelationshipTypes: Math.max(rollup.relationship_type_total - CONTENTS_CAP, 0),
      moreRelationshipCount: tailOf(rollup.relationship_types, CONTENTS_CAP).count,
      recordsEmptyLine: rollup.node_types.length
        ? null
        : "This version holds no records.",
      relationshipsEmptyLine: rollup.relationship_types.length
        ? null
        : "This version holds no relationships.",
    },
    footer: buildFooter(inputs, nowMs),
    nowMs,
  };
}
