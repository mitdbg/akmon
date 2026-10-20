// Needs attention: the ranked, bounded list of what is wrong, collapsed so
// eight rows that say the same thing are one fact.
import type { HomeInputs } from "@operator-ui/console/model/home";
import { producersState } from "@operator-ui/console/model/shared/readState";
import {
  exact, measurementWords, namesDatabaseObject, plainCause, readsAsProse,
  recordedText,
} from "@operator-ui/console/model/shared/format";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import {
  ATTENTION_STATES, OVERDUE_REASONS, freshnessWords, producerGlyph, stateOf, stateRank,
} from "@operator-ui/console/model/shared/producerStates";
import { causeWords } from "@operator-ui/console/model/sources/causes";
import {
  partialReadSentences, type ProducerRecord,
} from "@operator-ui/console/model/sources/producers";
import type { RawCheck } from "@operator-ui/console/model/shared/reads";
import { OUTCOME_GLYPHS, type Glyph, type Severity } from "@operator-ui/console/model/shared/status";
import { duration, relativeAge, utcStamp } from "@operator-ui/console/model/shared/time";

/** At most eight rows of Needs attention, then "and N more". */
export const NEEDS_ATTENTION_CAP = 8;

/** The two kinds of row that are not about one producer, in words.
 *
 *  `runtime` is the SCOPE the health payload records for the check
 *  (`console_views/health.py`), and the chip printed it raw: one lowercase
 *  code in a row whose every other word was English. These say what the row
 *  is about instead. */
export const KIND_RUNTIME_CHECK = "Runtime check";
export const KIND_READ = "Console read";

export interface AttentionRow {
  key: string;
  subject: string;
  cause: string;
  time: string;
  outcome: Glyph;
  /** How many rows this one stands for. 1 unless identical rows collapsed. */
  count: number;
  /** The subjects behind a collapsed row, for the "show N" toggle. */
  members: string[];
  /** What KIND of thing this row is about, where it is not a producer, so a
   *  runtime problem and a producer problem read as two kinds rather than one
   *  list. Written as the words a person reads -- "Runtime check", "Console
   *  read" -- and not as the scope code the health payload carries: the chip
   *  read `runtime` beside a row whose every other word was English. */
  kindLabel: string | null;
  /** The producer's own page. Null on a row that is not one producer: a
   *  runtime check has no page, and a collapsed group stands for several, so
   *  neither gets a link that would have to pick one. */
  href: string | null;
  /** The producer's name and kind, on a row about one producer. The subject
   *  used to carry the kind as a "(source)" tag, and the collapse and the
   *  merge below parsed it back out of the sentence; the tag is gone from
   *  the words (review 2026-09-22) and these carry it instead. */
  producer?: { name: string; kind: string };
}

export interface AttentionModel {
  rows: AttentionRow[];
  /** Rows past the cap, counted as ROWS. */
  omitted: number;
  /** What the heading counts: the rows listed plus the rows past the cap.
   *  A collapsed row is one row. The heading read "Needs attention · 4"
   *  over three rows on Archiv3, because a group of two counted twice. */
  openCount: number;
  omittedHref: string;
  /** The worst state in the list, which is what colours the title's count.
   *  Null when there is nothing to count. A page whose only open item is a
   *  producer that has not run yet has no red on it. */
  severity: Severity | null;
  /** The one sentence shown when nothing is wrong. Never a reassurance with no
   *  fact in it. */
  allClearSentence: string | null;
}

/**
 * A blocked attempt and the producer that blocked it are one fact.
 *
 * archiv3-small printed "Update attempt bd2648936849 was blocked / Stopped on
 * cmssw_releases" and then "cmssw_releases (source) failed its last run" --
 * two rows, one event. They merge when the attempt names a producer that also
 * has a failed row; the merged row keeps the producer's recorded cause,
 * because that is the one that says why. An attempt blocked on a publication
 * check names no producer and stays on its own.
 */
export function mergeBlockedWithProducer(rows: AttentionRow[]): AttentionRow[] {
  const attempt = rows.find((row) => row.key.startsWith("attempt:"));
  if (!attempt) return rows;
  const producer = /Stopped on (\S+) in /.exec(attempt.cause)?.[1];
  if (!producer) return rows;
  const failed = rows.find((row) => row.producer?.name === producer);
  if (!failed) return rows;
  return rows
    .filter((row) => row !== attempt)
    .map((row) => (row === failed
      ? {
        ...row,
        subject: `${producer} failed its last run and blocked the update`,
        time: newerAge(attempt.time, failed.time),
      }
      : row));
}

/** Of two relative ages, the one that reads as more recent. Both come from
 *  `relativeAge`, so the unit and the number decide it. */
function newerAge(left: string, right: string): string {
  const rank = (age: string) => {
    // "last run 2 h ago", "blocked 2 h ago" and "2 h ago" rank alike.
    const [value, unit] = age.replace(/^(last run|blocked) /, "").split(" ");
    const scale = unit === "min" ? 1 : unit === "h" ? 60 : unit === "d" ? 1440 : 0;
    return Number(value) * scale;
  };
  return rank(left) <= rank(right) ? left : right;
}

/**
 * Eight rows that say the same thing are one fact.
 *
 * scale-200 has eight sources that all failed with "repository not found
 * (404)". Printed one per row that is eight lines of identical text where one
 * line and a count would do. Rows collapse when they are the same KIND of
 * subject and carry the same cause; the collapsed row keeps the newest age and
 * the subjects behind it, so the view can expand it in place.
 */
export function collapseAttention(rows: AttentionRow[]): AttentionRow[] {
  const groups = new Map<string, AttentionRow[]>();
  const order: string[] = [];
  for (const row of rows) {
    const kind = row.key.split(":")[0];
    const subjectKind = row.producer?.kind ?? kind;
    // The STATE is part of the key. Without it, Archiv3's two blocked
    // sources and its one waiting on authorization collapsed into a single
    // row reading "3 sources are blocked", because all three record the same
    // cause -- a stale watermark. Rows collapse when they say the same thing;
    // these say three things about two states.
    const groupKey = `${kind}|${subjectKind}|${row.outcome.word}|${row.cause}`;
    if (!groups.has(groupKey)) {
      groups.set(groupKey, []);
      order.push(groupKey);
    }
    groups.get(groupKey)?.push(row);
  }
  return order.map((groupKey) => {
    const members = groups.get(groupKey) ?? [];
    const first = members[0];
    if (members.length === 1) return first;
    const [, subjectKind] = groupKey.split("|");
    return {
      ...first,
      key: `group:${groupKey}`,
      subject: `${members.length} ${subjectKind}s ${groupVerb(first.outcome.word)}`,
      count: members.length,
      members: members.map((row) => row.producer?.name ?? row.subject),
      // The row now stands for several producers, so it can no longer open
      // one of their pages. The member list is what names them.
      href: null,
    };
  });
}

/**
 * A detail line, and the fix hint that may follow it.
 *
 * One place for the rule about a hint, because two kinds of row use it now: a
 * hint is printed only where it reads as a sentence. A hint that is a command
 * line ("okg queue dlq list") is machine vocabulary and stays one level down
 * with the rest of it. The console's producer diagnostics record no hint at
 * all today, so a producer row passes none and gets none; nothing is invented
 * to fill the space.
 */
function causeLine(lead: string[], fixHint?: string | null): string {
  const parts = lead.filter(Boolean);
  if (readsAsProse(fixHint)) parts.push(recordedText(fixHint));
  return parts.join(" · ");
}

/**
 * What a runtime check's detail line says.
 *
 * The measurement the check recorded, unless it names a thing inside the
 * database. Live Archiv3's failing check recorded `dbos.workflow_status
 * missing`, and the console printed the table name on the page an operator
 * opens to see whether the graph is current. The measurement is not lost --
 * the health read still carries it, and Sources and Updates show a recorded
 * failure verbatim one level down -- but on this page its place says that
 * something was measured and that the detail is not shown.
 *
 * Where the detail is withheld the check's own fix hint takes its place, so
 * the row still says what to do. Only where that hint is a sentence: a hint
 * that is a command line ("okg queue dlq list") is machine vocabulary and
 * stays one level down with the rest of it.
 */
/**
 * Runtime checks whose measurement this page can say in plain words.
 *
 * Archiv3's one failing check read "Runtime queue evidence is unavailable ·
 * detail withheld · inspect DBOS history volume and the runtime queue
 * authority" -- three phrases, none of which told an operator what was wrong.
 * What its measurement (`dbos.workflow_status missing`, or
 * `runtime_schema_absent` once the health read says so by code) means is one
 * sentence, and that sentence is the row.
 */
const RUNTIME_QUEUE_CODES = new Set(["runtime_queue_unavailable", "runtime_queue_not_observed"]);
const WORKER_RECORDS_MISSING = /^(dbos\.workflow_status missing|runtime_schema_absent)$/;

export function runtimeCheckRow(check: RawCheck): { subject: string; cause: string } {
  if (RUNTIME_QUEUE_CODES.has(check.code) && WORKER_RECORDS_MISSING.test((check.actual ?? "").trim())) {
    return {
      subject: "The background worker's job records are missing, so queued work cannot be checked",
      cause: "",
    };
  }
  return { subject: check.summary.replace(/\.$/, ""), cause: checkCause(check) };
}

/** The DOM id of a runtime check's row, so the header chip can point at it. */
export const CHECK_ROW_ID = (code: string): string => `console-attention-check-${code}`;

/** The row the header's verdict chip points at: the first runtime check
 *  listed. Null where no check is listed, and the chip stays plain text. */
export function chipTargetId(rows: readonly { key: string }[]): string | null {
  const row = rows.find((candidate) => candidate.key.startsWith("check:"));
  return row ? CHECK_ROW_ID(row.key.slice("check:".length)) : null;
}

/** A producer row's age, labelled: "3 d ago" alone did not say whether it
 *  was the last run or when the source went stale. */
export function producerAge(producer: ProducerRecord, nowMs: number): string {
  if (producer.latestRun?.iso) return `last run ${relativeAge(producer.latestRun.iso, nowMs)}`;
  return producer.freshness.age ? `last run ${producer.freshness.age}` : "";
}

export function checkCause(check: RawCheck): string {
  if (!check.actual) return causeLine(["no measurement recorded"], check.fix_hint);
  if (namesDatabaseObject(check.actual)) return causeLine(["detail withheld"], check.fix_hint);
  // The measurement, in words and with its separators. The check wrote
  // `12281 in dead_letter`; the page prints `12,281 in the dead-letter
  // queue`. Nothing is added and nothing is dropped.
  return measurementWords(check.actual);
}

/**
 * What a failed producer's detail line says.
 *
 * The same rule, one row down and one writer over. A check's measurement is
 * composed for a reader; a producer's `health.detail` is the exception text
 * its own run raised, and workspace-mixed's view build raised `relation
 * "okg.contributor_edge" does not exist` -- a relation name on the summary
 * page, out of a string nobody wrote for this page at all.
 *
 * Withheld by the same shape test, so no raw error string from any writer
 * reaches Home. What stands in its place is what the read itself recorded
 * about the producer: its health status word, then that the detail is not
 * shown. The verbatim line is not lost -- Sources prints it one level down,
 * which is where rule 3 puts a raw failure.
 *
 * Every other recorded error still prints its sentence. "repository not found
 * (404)" and "the remote refused the connection after 3 retries" say nothing
 * about the database and are what the operator needs on the page.
 */
export function producerCause(producer: ProducerRecord): string {
  // ONE field, because ONE read answers for all three kinds:
  // `workflow.latest_issue`, which `console/producers` fills from the
  // enricher read's composed sentence ("0 edges emitted since wiring") and
  // leaves null, with a reason beside it, where no read records one.
  const plain = plainCause(producer.workflow.latest_issue);
  if (!plain) return "The run recorded no error text.";
  if (!namesDatabaseObject(plain)) return plain;
  return causeLine([(producer.health.status ?? "").trim(), "detail withheld"]);
}

/**
 * Needs attention, ranked and bounded.
 *
 * Rank order is the requirement's: a blocked attempt, then failed producers,
 * then failed runtime checks, then never-run producers.
 *
 * The blocked attempt appears only when it is NEWER than the version being
 * read — an older blocked attempt that a later attempt already superseded is
 * history, and it is counted in the strip above, not repeated as a live
 * problem.
 *
 * Only `scope: "runtime"` checks are listed. The other scopes the composer
 * emits (`sources`, `views`, `publisher`) restate a failed producer row or the
 * identity line, and Home is not the place to say one fact twice.
 */
export function buildAttention(
  inputs: HomeInputs,
  producers: ProducerRecord[],
  nowMs: number,
  link: LinkFn,
): AttentionModel {
  const health = inputs.health.payload;
  const rows: AttentionRow[] = [];

  // "Blocked SINCE the last publish" needs the last publish's time, and only
  // the versions read carries it. Three states, and they are not the same:
  // a read that listed versions gives the time; a read that listed NONE means
  // nothing has published, so every blocked attempt is since the last publish
  // (-Infinity); and a read that FAILED gives no time at all, so the question
  // cannot be decided and the row is withheld. Treating the failure as the
  // empty case would raise every blocked attempt in the history as new. The
  // page names the failed read in its own line.
  const readVersionAt = inputs.versions?.payload.versions[0]?.published_at ?? null;
  const readVersionMs = readVersionAt
    ? Date.parse(readVersionAt)
    : (inputs.versions ? -Infinity : null);
  const blocked = readVersionMs === null ? undefined : inputs.generations.payload.generations.find(
    (attempt) => attempt.status === "blocked" && Date.parse(attempt.created_at) > readVersionMs,
  );
  if (blocked) {
    rows.push({
      count: 1,
      members: [],
      kindLabel: null,
      href: null,
      key: `attempt:${blocked.full_generation_id}`,
      subject: `Update attempt ${shortVersion(blocked.full_generation_id)} was blocked`,
      cause: blocked.stopped_producer
        ? `Stopped on ${blocked.stopped_producer} in ${blocked.stopped_stage ?? "an unrecorded stage"}`
        : `Stopped in ${blocked.stopped_stage ?? "an unrecorded stage"}; the attempt records no producer`,
      // Labelled, like every age in this list (review 2026-09-23).
      time: `blocked ${relativeAge(blocked.created_at, nowMs)}`,
      outcome: OUTCOME_GLYPHS.blocked,
    });
  }

  for (const producer of attentionProducers(producers)) {
    const state = stateOf(producer);
    // A producer listed for its watermark alone keeps its own state's word
    // nowhere near this list: "cmssw_releases (source) is fresh" under Needs
    // attention is a contradiction. The row says what put it here.
    const stale = !ATTENTION_STATES.includes(state);
    rows.push({
      count: 1,
      members: [],
      // The kind is a tag only where it is not a source. "jira (source)
      // needs ..." carried a word the reader did not need on a deployment
      // whose every producer is a source.
      kindLabel: producer.kind === "source" ? null : KIND_WORDS[producer.kind] ?? producer.kind,
      producer: { name: producer.name, kind: producer.kind },
      key: `producer:${producer.kind}:${producer.name}`,
      subject: `${producer.name} ${stale ? statePhrase("overdue") : statePhrase(state)}`,
      cause: producerAttentionCause(producer, state, nowMs),
      // A producer with no recorded last run has no age. `never run` had this
      // rule already; the live states inherit it rather than printing the
      // page's read time as if it were the producer's.
      // A source's read records a timestamp, which becomes a relative age.
      // The enricher and view reads record an age ALREADY IN WORDS and no
      // timestamp, so it is printed as it stands rather than parsed. A
      // producer with neither has no age and says nothing.
      // A source's read records a timestamp, which becomes a relative age.
      // The enricher and view reads record an age ALREADY IN WORDS ("8 Sep
      // 2026") and no timestamp, which `console/producers` carries in
      // `freshness.age`, so it is printed as it stands rather than parsed.
      time: producerAge(producer, nowMs),
      outcome: stale ? STALE_GLYPH : producerGlyph(state),
      href: link(`sources/${encodeURIComponent(producer.name)}`),
    });
  }

  // The producers read FAILED. It used to add a row here naming the failure,
  // and the Sources block below named it again -- twice on one page, and the
  // row printed "· undefined" whenever the route forgot to pass the error
  // (audit 2026-09-23). The failure is printed ONCE now, in the Sources
  // block, with its "Read again"; this list says in one line that it holds
  // the runtime checks alone. The all-clear below still waits for an
  // ANSWERED read, so nothing here can claim every producer is fine.

  // The producers read ANSWERED and came back short. Its composer records
  // three diagnostics reads; when the enricher or the view one raises it
  // names the kind in `omitted_data` and returns the rows it has, so every
  // count on this page is short by that many producers with nothing on the
  // page saying so. The Sources view already prints these sentences; Home
  // printed a smaller number and called it the deployment.
  if (inputs.producers) {
    for (const sentence of partialReadSentences(inputs.producers)) {
      rows.push({
        count: 1,
        members: [],
        kindLabel: KIND_READ,
        href: null,
        key: `read:short:${sentence.slice(0, 48)}`,
        subject: "A read behind the producer counts came back short",
        cause: sentence,
        time: "",
        outcome: { icon: "warning", word: "Warning", severity: "never-run" },
      });
    }
  }

  for (const check of health.checks.filter((row) => row.scope === "runtime")) {
    const worded = runtimeCheckRow(check);
    rows.push({
      count: 1,
      members: [],
      kindLabel: KIND_RUNTIME_CHECK,
      href: null,
      key: `check:${check.code}`,
      // The check's own summary, not its code. It is already a plain sentence
      // ("No worker has reported since 03:11 UTC."), it carries a fact the code
      // does not, and it works for a check code nobody has written yet -- which
      // a hand-written map from code to English would not.
      subject: worded.subject,
      cause: worded.cause,
      // The footer says when the page was read; "now" on every runtime row
      // said the same of each of them.
      time: "",
      outcome: check.severity === "fail"
        ? { icon: "close", word: "Failed", severity: "failed" }
        : { icon: "stop", word: "Warning", severity: "blocked" },
    });
  }

  const collapsed = namesInCause(
    collapseAttention(mergeBlockedWithProducer(rows)),
  );
  const shown = collapsed.slice(0, NEEDS_ATTENTION_CAP);
  const omitted = collapsed.length - shown.length;

  // The all-clear names a number of producers, so it is a claim about a read
  // that has to have ANSWERED. While the three reads are outstanding the list
  // is the runtime checks alone and the block says so; when they failed the
  // row above says that instead.
  let allClearSentence: string | null = null;
  if (rows.length === 0 && producersState(inputs) === "answered") {
    // "Updated" is one of THREE words a read may record for a producer that
    // is doing what it should: the fixtures say `updated`, a source says
    // `fresh`, an enricher or a view says `healthy`. Counting one of them
    // told a deployment with six fresh sources that none of its producers
    // had updated.
    const updated = producers.filter(
      (row) => producerGlyph(stateOf(row)).severity === "ok",
    ).length;
    const latestPublished = inputs.generations.payload.generations.find(
      (attempt) => attempt.status === "published",
    );
    const when = latestPublished ? utcStamp(latestPublished.created_at) : "an unrecorded time";
    const queue = health.vitals.queue_total === 0 && health.vitals.dlq_count === 0
      ? "the queue and the dead-letter queue are empty"
      : `the queue holds ${exact(health.vitals.queue_total)} jobs and the dead-letter queue `
        + `${exact(health.vitals.dlq_count)}`;
    allClearSentence = `All ${updated} configured ${updated === 1 ? "producer" : "producers"} `
      + `updated in the version published ${when}; ${queue}; no check is failing.`;
  }

  const worst: Severity | null = shown.length === 0
    ? null
    : shown.some((row) => row.outcome.severity === "failed"
      || row.outcome.severity === "blocked")
      ? "failed"
      : shown[0].outcome.severity;

  return {
    rows: shown,
    omitted,
    openCount: shown.length + omitted,
    omittedHref: link("sources", { state: "failed" }),
    severity: worst,
    allClearSentence,
  };
}

/**
 * A small collapsed group names its members instead of hiding them.
 *
 * On a new deployment the only thing to say is which sources are configured
 * and have not run. Hiding the three names behind "show 3" made the one page
 * that exists to answer "what do I do next" answer it with a click.
 *
 * The rule used to apply to one case -- a never-run group that was the whole
 * list. It applies to any group of two or three now, because live Archiv3
 * produced another: two sources blocked for the same recorded reason
 * collapsed into "2 sources are blocked", and the two names were the only
 * thing on the row an operator could act on. Above three the names are
 * longer than the row and the expander earns its place.
 */
const NAME_MEMBERS_UP_TO = 3;

function namesInCause(rows: AttentionRow[]): AttentionRow[] {
  return rows.map((row) => {
    if (row.count < 2 || row.count > NAME_MEMBERS_UP_TO || row.members.length === 0) return row;
    const names = row.members.map((name) => name.split(" (")[0]).join(", ");
    const detail = row.cause.replace(/^Configured; /, "");
    return {
      ...row,
      cause: detail ? `${names} \u00b7 ${detail}` : names,
      members: [],
    };
  });
}

// --- producers that need attention ------------------------------------------

/**
 * What a producer in each state IS, as a clause after its name.
 *
 * Written out rather than composed from the glyph word, because the glyph
 * word is an adjective for a table cell and this is the middle of a sentence:
 * "jira (source) Needs authorization" is not English, and "jira (source) is
 * needs authorization" is worse.
 */
const STATE_PHRASE: Record<string, string> = {
  failed: "failed its last run",
  failing: "is failing",
  blocked: "is blocked",
  stuck: "is stuck",
  unsupported: "is not supported by this deployment's runtime",
  // The recorded constraint is `manual_or_credential_constraint`. Archiv3's
  // jira ran three times; "needs authorization before it can run" said it
  // never had (review 2026-09-22).
  manual_required: "needs a manual run or a credential to refresh",
  overdue: "is overdue",
  "wired-but-silent": "is wired and has produced nothing",
  "never run": "has never run",
};

export function statePhrase(state: string): string {
  return STATE_PHRASE[state] ?? `is ${producerGlyph(state).word.toLowerCase()}`;
}

/** The same clause for a collapsed group of them. */
const GROUP_VERB: Record<string, string> = {
  "Never run": "have never run",
  Failed: "failed their last run",
  Failing: "are failing",
  Blocked: "are blocked",
  Stuck: "are stuck",
  "Needs a manual run": "need a manual run or a credential to refresh",
  Overdue: "are overdue",
  "Not supported": "are not supported",
  "Wired, silent": "are wired and have produced nothing",
};

function groupVerb(glyphWord: string): string {
  return GROUP_VERB[glyphWord] ?? `are ${glyphWord.toLowerCase()}`;
}

/** The state a stale freshness verdict is listed under, for a producer whose
 *  own state is otherwise unremarkable. It is the same fact the Overdue state
 *  names -- no run in the freshness window -- so it is drawn the same way. */
export const STALE_GLYPH: Glyph = producerGlyph("overdue");

/** A producer kind, as the tag beside a row that is not a source. */
const KIND_WORDS: Record<string, string> = { enricher: "Enricher", view: "View" };

/**
 * The producers Home lists, worst first.
 *
 * Two different reads put a producer here. `workflow.state` is the
 * source-sync convergence state, and the states in `ATTENTION_STATES` are the
 * ones that will not resolve themselves. `freshness.status` is a separate
 * measurement, and a source whose state is otherwise unremarkable can still
 * be past its declared cadence -- so a `stale` watermark lists the producer
 * too, once, under the state it already has when it has one.
 */
export function attentionProducers(producers: ProducerRecord[]): ProducerRecord[] {
  const listed = producers.filter(
    (row) => ATTENTION_STATES.includes(stateOf(row))
      || row.freshness.status === "stale",
  );
  return [...listed].sort(
    (left, right) => attentionRank(left) - attentionRank(right)
      || left.name.localeCompare(right.name),
  );
}

/** A producer listed for its watermark alone ranks after every recorded bad
 *  state: it is a producer that is late, not one that has stopped. */
function attentionRank(producer: ProducerRecord): number {
  const state = stateOf(producer);
  return ATTENTION_STATES.includes(state) ? stateRank(state) : 25;
}

/**
 * Why a producer is where it is, in the words the read recorded.
 *
 * NOTHING IS INVENTED. Three things can be recorded and each is printed as
 * itself: the run's own error text (withheld by `producerCause` where it
 * names a database object), the freshness verdict where it is a measurement
 * rather than a statement that nothing was measured, and -- for a producer
 * that has never run -- what the registry declares about its next run. A
 * producer for which none of the three was recorded gets an empty cause, and
 * the view prints nothing in its place.
 */
export function producerAttentionCause(
  producer: ProducerRecord, state: string, _nowMs: number,
): string {
  const parts: string[] = [];
  if (plainCause(producer.workflow.latest_issue)) parts.push(producerCause(producer));
  // What the CONVERGENCE read recorded BESIDE the state, in words: the
  // read's own sentence where it serves one (`workflow.state_reason`), else
  // the reason code it records, through `causeWords`. Only
  // `console/producers` carries either.
  const served = (producer.workflow.state_reason ?? "").trim();
  const reason = ATTENTION_STATES.includes(state)
    ? (served ? causeWords(served) : causeWords(producer.workflow.reason ?? null))
    : null;
  // Not where it only repeats the subject. `manual_or_credential_constraint`
  // is what the manual_required phrase already says, and an overdue reason is
  // what "is overdue" already says; the producer's page explains both.
  const restatesOverdue = !served && state === "overdue"
    && OVERDUE_REASONS.has(producer.workflow.reason ?? "");
  if (reason && reason !== statePhrase(state) && !restatesOverdue) {
    parts.push(reason);
  }
  // The freshness read's separate verdict, where it measured something --
  // `not_evaluated`, `not_configured` and `last_run_age_only` each say
  // nothing was measured, and none is a cause -- and where the reason did
  // not already say it: an overdue reason and a stale verdict are one
  // measurement stated twice.
  const freshness = ATTENTION_STATES.includes(state)
    ? producer.freshness.status
    : null;
  const reasonSaidIt = Boolean(served) || OVERDUE_REASONS.has(producer.workflow.reason ?? "");
  if (freshness && freshness !== "fresh" && !UNMEASURED_FRESHNESS.has(freshness) && !reasonSaidIt) {
    const cause = freshnessCause(freshness, producer.freshness);
    if (cause) parts.push(cause);
  }
  if (state === "never run") {
    // No cadence is printed. The console has ONE producers read and it
    // carries no next-due field, on any kind of producer: only the design
    // harness's generated fixtures ever had one, and a sentence built from an
    // absent field would be this page inventing a schedule.
    parts.push("Configured; no schedule is recorded");
  }
  return parts.join(" \u00b7 ");
}

/** A freshness verdict as a cause, without the runtime's word "watermark". */
const FRESHNESS_CAUSE: Record<string, string> = {
  missing: "no completed run recorded",
};

/**
 * "no run in the last 2 d" where the read serves the window, and nothing
 * where it does not. "no run in the freshness window" named a window no
 * page could state: every Archiv3 source serves no window at all, because
 * none declares a schedule.
 */
function freshnessCause(status: string, freshness: ProducerRecord["freshness"]): string | null {
  if (status === "stale") {
    const window = (freshness as { max_age_seconds?: unknown }).max_age_seconds;
    return typeof window === "number" && window > 0
      ? `no run in the last ${duration(window * 1000)}`
      : null;
  }
  return FRESHNESS_CAUSE[status] ?? `freshness ${freshnessWords(status)}`;
}

/** Freshness verdicts that say nothing was measured, rather than measuring
 *  something. `last_run_age_only` is the producers read's own word for an
 *  enricher or a view, neither of which records a watermark at all. */
const UNMEASURED_FRESHNESS = new Set([
  "not_evaluated", "not_configured", "last_run_age_only",
]);
