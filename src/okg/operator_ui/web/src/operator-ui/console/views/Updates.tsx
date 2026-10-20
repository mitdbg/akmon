// Updates: did the last attempt publish, and if it did not, where did it stop?
//
// Reference: ui/references/updates.md. GitHub's Actions runs list is the table
// (one flat table, newest first, a filter strip above it, a fixed page size);
// GitHub's failed-run summary is the error block (one sentence of cause, then
// the recorded text verbatim in monospace); Dagster's run detail is the body
// (the stages in order, with the one it stopped at marked).
//
// It is drawn inside the shell the operator approved for Home on 2026-09-18,
// and it uses Home's own parts -- Section, Status, StateIcon, ChartBars -- so
// the two pages are one product rather than two drawings of the same tokens.
//
// Two things this page will not do:
//
//   1. It never prints a fact no read recorded. Measured against a published
//      deployment, the attempts read returns duration_ms and stopped_stage
//      null on every attempt of every deployment, because no column holds
//      them. So the Duration column is CUT rather than filled with blanks,
//      the running-pace phrase and its "late" marker are cut with it (they
//      are the median of those same durations), and the five-stage list is
//      cut too: five rows reading "Not recorded" state nothing, and deriving
//      a stage from the blocker's code would print a position no run
//      measured. The expander that lived in the Duration cell moves into
//      Outcome.
//   2. It never pages by asking for a cursor and appending whatever comes
//      back. `loadMoreAttempts` refuses a page that repeats a row already in
//      the table, and the refusal is rendered by name (rule 13).
import { ConsoleLoading } from "@operator-ui/console/components/ConsoleLoading";
import { ConsoleAttemptDetail, InlineAttempt } from "@operator-ui/console/views/AttemptDetail";
import { StateIcon, Status } from "@operator-ui/console/components/StatusWord";
import { VersionNotice } from "@operator-ui/console/components/VersionNote";
import { DEFAULT_RANGE, RANGES, type Range, pageContextFor } from "@operator-ui/console/api/client";
import {
  type ConsoleUpdatesData, UPDATES_ATTEMPTS_ROUTE, UPDATES_PRODUCERS_ROUTE, UPDATES_VERSIONS_ROUTE,
  loadAttemptDetail, loadConsoleUpdates, loadMoreAttempts, updatesIncomplete,
} from "@operator-ui/console/api/generations";
import { clientKey, useRecoveringRead } from "@operator-ui/console/components/readRecovery";
import { CopyTextButton, CopyableId } from "@operator-ui/console/components/CopyControls";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";
import { MetricChart } from "@operator-ui/console/components/MetricChart";
import { type PageContext, updatesText } from "@operator-ui/console/model/copyText";
import type { ChartInputs } from "@operator-ui/console/model/home";
import { exact } from "@operator-ui/console/model/shared/format";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { RawAttempt } from "@operator-ui/console/model/shared/reads";
import { type AttemptDetail, buildAttemptDetail, newestBlockedAttempt, resolveAttempt } from "@operator-ui/console/model/updates/detail";
import { type RawAttemptDetail, buildRecordedDetail } from "@operator-ui/console/model/updates/recorded";
import type { RecordedState } from "@operator-ui/console/views/AttemptRecorded";
import { UPDATES_PAGE, ignoredSortParam, ignoredVersionParam, parseUpdatesFilter } from "@operator-ui/console/model/updates/filters";
import { type OutcomeChip, type UpdatesModel, buildUpdatesModel } from "@operator-ui/console/model/updates/list";
import type { UpdateRow } from "@operator-ui/console/model/updates/rows";
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import { Chip } from "@operator-ui/console/components/Chip";
import { type ReactElement, useEffect, useState } from "react";

/** One toggle in the count strip. Rule 1: the counts above the table are the
 *  filters on it, and a chip that is on says so with a word, not a tint. */
function OutcomeToggle({ chip }: { chip: OutcomeChip }) {
  return (
    <Chip
      className="okg-console__chipfilter"
      severity={chip.mark.severity}
      checked={chip.selected}
      onToggle={() => { window.location.hash = chip.href; }}
    >
      <StateIcon icon={chip.mark.icon} />
      <span className="okg-console__count-value">{exact(chip.value)}</span>
      <span className="okg-console__count-label">{chip.label.toLowerCase()}</span>
      {chip.note ? (
        <span className="okg-console__count-note">{`\u00b7 ${chip.note}`}</span>
      ) : null}
    </Chip>
  );
}

/** One attempt. The subject is the outcome and what it stopped on, because on
 *  this page that is what distinguishes one row from the next. */
function AttemptTableRow({ row }: { row: UpdateRow }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <tr
        className="okg-console__urow okg-console__copyrow"
        data-severity={row.outcome.severity}
        data-testid="updates-row"
        onClick={(event) => {
          // The whole row opens the attempt. The only way in used to be the
          // 6 by 18 px chevron (audit 2026-09-23). A click on a link, the
          // expander or a copy button keeps its own meaning.
          if ((event.target as HTMLElement).closest("a, button")) return;
          window.location.hash = row.href;
        }}
      >
        <td className="okg-console__urow-when" data-column="when" title={row.whenExact}>{row.when}</td>
        <td className="okg-console__urow-outcome" data-column="outcome">
          <Status mark={row.outcome} />
          {row.stoppedOn ? (
            <span className="okg-console__urow-stopped"> {row.stoppedOn}</span>
          ) : null}
          {/* The expander lives here now. It used to sit in the Duration
              cell, and that column is cut: no column records a publish's
              wall clock, so every cell in it was empty. */}
          {/* The Change phrase again, shown only at narrow width, where the
              Change column has nowhere to go. Without it every row at 390
              read identically: same age, same outcome word, nothing else. */}
          {row.narrowDetail ? (
            <span className="okg-console__urow-change-narrow">{row.narrowDetail}</span>
          ) : null}
          {row.repeat ? (
            <>
              {" "}
              <button
                type="button"
                className="okg-console__link-button"
                aria-expanded={open}
                aria-label={`${open ? "Hide" : "Show"} the ${exact(row.count)} attempts`}
                onClick={() => setOpen((was) => !was)}
              >
                {row.repeat}
              </button>
            </>
          ) : null}
        </td>
        <td className="okg-console__urow-version" data-column="version">
          {row.version && row.versionFullId ? (
            <CopyableId text={row.versionFullId} what="version id" reveal="row">
              <code className="okg-console__id">{row.version}</code>
            </CopyableId>
          ) : null}
          {row.reading ? <span className="okg-console__tag">reading</span> : null}
        </td>
        <td className="okg-console__urow-change" data-column="change">{row.change}</td>
        <td className="okg-console__urow-open" data-column="open">
          <a href={row.href} aria-label={`Open attempt ${row.attemptId}`}>›</a>
        </td>
      </tr>
      {open
        ? row.members.map((member) => (
          <tr
            className="okg-console__urow okg-console__urow--member okg-console__copyrow"
            key={member.attemptId}
          >
            <td className="okg-console__urow-when" data-column="when" title={member.whenExact}>{member.when}</td>
            <td className="okg-console__urow-outcome" data-column="outcome" colSpan={2}>
              <CopyableId text={member.fullId} what="attempt id" reveal="row">
                <code className="okg-console__id">{member.attemptId}</code>
              </CopyableId>
            </td>
            <td className="okg-console__urow-change" data-column="change" />
            <td className="okg-console__urow-open" data-column="open" />
          </tr>
        ))
        : null}
    </>
  );
}

function AttemptTable({ rows }: { rows: UpdateRow[] }) {
  return (
    <table className="okg-console__utable" data-testid="updates-table">
      <thead>
        <tr>
          {/* data-column, not nth-child: the Duration column was cut and
              every positional selector after it silently shifted onto the
              wrong cell. A column can be added or removed now without
              re-numbering a stylesheet. */}
          <th scope="col" data-column="when">When</th>
          <th scope="col" data-column="outcome">Outcome</th>
          <th scope="col" data-column="version">Version</th>
          <th scope="col" data-column="change">Change</th>
          <th scope="col" data-column="open" aria-label="Open the attempt" />
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => <AttemptTableRow key={row.key} row={row} />)}
      </tbody>
    </table>
  );
}

export function ConsoleUpdates({
  model, chart, range, onRange, onMore, moreError, loadingMore, inline, context,
  askedVersion = null, ignoredParam = null, ignoredSort = null, published = [],
  chartFailure = null, versionsFailure = null, producersFailure = null, onReadAgain = null,
  retrying = false,
}: {
  /** This span's attempts read failed: the chart's place names it. */
  chartFailure?: unknown;
  /** The versions read failed: named under the title. */
  versionsFailure?: unknown;
  /** The producers read behind the configured-source count failed; named
   *  only where its one sentence would have been, on a deployment with no
   *  attempt. That sentence then says the count is unknown. */
  producersFailure?: unknown;
  /** Read the page again, for any of the three above. */
  onReadAgain?: (() => void) | null;
  retrying?: boolean;
  /** The generation ids the versions read returned, so the note can name a
   *  real one rather than withholding it for failing a shape test. */
  published?: readonly string[];
  /** What to say about a `version` parameter this table does not filter by. */
  ignoredParam?: string | null;
  /** What to say about a `sort` parameter this list does not order by. Its
   *  own line, beside the version one: they are different questions and a
   *  URL can ask both. */
  ignoredSort?: string | null;
  /** What the top bar's version picker put on the URL, when it put anything.
   *  The reads behind this page take no version; the note says so and names
   *  both ids. It is drawn here, at the page's top, and NOT inside the
   *  inline blocked block below, so the page states it once. */
  askedVersion?: string | null;
  model: UpdatesModel;
  /** Null when this span's attempts read failed. */
  chart: ChartInputs | null;
  range: Range;
  onRange: (next: Range) => void;
  onMore: (() => void) | null;
  moreError: unknown;
  loadingMore: boolean;
  /** What "Copy as text" states in its header. */
  context: PageContext;
  /** The newest finished attempt, when it is blocked: its detail is rendered
   *  under the table, so the operator who arrived from an alert reads the
   *  stage it stopped in and the recorded error without a second hop. Null
   *  when the newest finished attempt published. */
  inline: { detail: AttemptDetail; href: string } | null;
}): ReactElement {
  return (
    <main className="okg-console" data-testid="console-updates-view">
      <header className="okg-console__header" data-testid="updates-header">
        <div className="okg-console__header-row">
          <h1 className="okg-console__title">Updates</h1>
          {/* Nothing beside the title. It used to read "N attempts recorded ·
              newest X ago": N is the sum of the outcome chips directly below,
              which are also the filters, and the rows themselves are the
              third statement of it; X is the first row's own When cell. The
              header carried no fact the page below it did not already make
              twice. On a deployment with no attempt it printed nothing
              anyway, so cutting it costs that page nothing either. */}
          <CopyTextButton
            className="okg-console__copy--page"
            what="Updates as text"
            label="Copy as text"
            text={() => updatesText(model, inline?.detail ?? null, context)}
          />
        </div>
      </header>
      {/* Under the title, never above it (review 2026-09-22): the page's
          name is the first thing on it, and a line about the URL is not. */}
      <VersionNotice
        reading={context.version}
        asked={askedVersion}
        ignoredParam={ignoredParam}
        published={published}
      />
      {ignoredSort ? (
        <p className="okg-console__status-note" data-testid="updates-ignored-sort">
          {ignoredSort}
        </p>
      ) : null}
      {versionsFailure ? (
        <ReadFailure
          route={UPDATES_VERSIONS_ROUTE}
          what="The published versions"
          error={versionsFailure}
          onReadAgain={onReadAgain}
          retrying={retrying}
        />
      ) : null}

      {model.noAttemptsSentence ? (
        <>
          <p className="okg-console__fact okg-console__none">{model.noAttemptsSentence}</p>
          {producersFailure ? (
            <ReadFailure
              route={UPDATES_PRODUCERS_ROUTE}
              what="The configured sources"
              error={producersFailure}
              onReadAgain={onReadAgain}
              retrying={retrying}
            />
          ) : null}
          {model.configuredSentence ? (
            <p className="okg-console__fact okg-console__none">
              {model.configuredSentence}{" "}
              <a href={model.configuredHref ?? "#sources"}>{model.configuredLink}</a>
            </p>
          ) : null}
        </>
      ) : (
        <>
          <div className="okg-console__filters" data-testid="updates-filters">
            <p className="okg-console__count-line">
              {model.chips.map((chip) => <OutcomeToggle key={chip.outcome} chip={chip} />)}
              {model.filter.producer ? (
                <a className="okg-console__chipclear" href={model.clearHref}>
                  on {model.filter.producer} &times;
                </a>
              ) : null}
              {model.filter.attempt ? (
                <a
                  className="okg-console__chipclear"
                  href={model.clearHref}
                  data-testid="updates-attempt-chip"
                >
                  attempt <code className="okg-console__id">{model.filter.attempt}</code> &times;
                </a>
              ) : null}
            </p>
          </div>

          {chart ? (
            <MetricChart
              attempts={chart.attempts}
              generations={chart.generations}
              defaultMetric="attempts"
              range={range}
              onRange={onRange}
              testid="updates-chart"
            />
          ) : (
            <div data-testid="updates-chart-failed">
              <ReadFailure
                route={UPDATES_ATTEMPTS_ROUTE}
                what="The update attempts by day"
                error={chartFailure}
                onReadAgain={onReadAgain}
                retrying={retrying}
              />
            </div>
          )}

          {model.shownSentence
            ? <p className="okg-console__tablecount">{model.shownSentence}</p>
            : null}
          {model.noMatchSentence
            ? <p className="okg-console__fact">{model.noMatchSentence}</p>
            : <AttemptTable rows={model.rows} />}

          {/* ReadFailure, not LoudFailure, for the same reason the read at the
              top of this route uses it: LoudFailure's next step is "add this
              read to the fixture generator and regenerate", which is advice
              for the design harness. An operator whose next page of attempts
              was refused needs the route, the server's reason and a check.
              The read is `console/generations` again, asked for a further
              page. */}
          {moreError ? (
            <ReadFailure
              route="/viz/api/console/generations"
              what="The next page of update attempts"
              error={moreError}
            />
          ) : null}
          {onMore ? (
            <p className="okg-console__more">
              <button
                type="button"
                className="okg-console__link-button"
                disabled={loadingMore}
                onClick={onMore}
              >
                {loadingMore ? "Loading…" : `Load ${UPDATES_PAGE} more`}
              </button>
            </p>
          ) : null}

          {inline ? <InlineAttempt detail={inline.detail} href={inline.href} /> : null}
        </>
      )}
    </main>
  );
}


function shortId(attempt: RawAttempt): string {
  return attempt.full_generation_id.slice(-12);
}

/** The route: load once, then render the list or one attempt. */
export function ConsoleUpdatesRoute({
  api, link, params, attemptId, readingVersion, onRead, askedVersion = null,
  onReadVersion,
}: {
  api: ApiClient;
  link: LinkFn;
  /** The route's query parameters, minus the harness's own. */
  params: Record<string, string>;
  /** The 12-hex id in `#updates/<id>`, or null for the list. */
  attemptId: string | null;
  /** The version every view is reading. Null falls back to the health read. */
  readingVersion?: string | null;
  /** Called by "Read this version". Null makes the button absent, which is
   *  what a console with nowhere to put the choice should show. */
  onRead?: ((fullVersionId: string) => void) | null;
  /** What the top bar's version picker put on the URL. These reads take no
   *  version, so both pages say which one they are showing and which one was
   *  asked for rather than appearing to honour the choice. */
  askedVersion?: string | null;
  /** Called with the full generation id these reads answered for, so the top
   *  bar can show that one rather than the one the URL asked for. Home reports
   *  the same way; the bar shows whichever view is mounted. */
  onReadVersion?: (fullVersion: string | null) => void;
}): ReactElement {
  // One page read, recovering: "Read again", the bar's schedule, and a click
  // on Updates in the bar all re-issue it. A minor read failing leaves the
  // page drawn from the rest and re-read on the schedule until it answers.
  const list = useRecoveringRead<ConsoleUpdatesData>(
    clientKey(api),
    (signal) => loadConsoleUpdates(api, signal),
    { incomplete: updatesIncomplete },
  );
  const data = list.value;
  const error = list.error;
  const [extra, setExtra] = useState<RawAttempt[]>([]);
  const [moreError, setMoreError] = useState<unknown>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const rangeParam = RANGES.find((candidate) => candidate === params.range);
  const [range, setRange] = useState<Range>(rangeParam ?? DEFAULT_RANGE);

  // A new first page starts the table again: pages appended to the old one
  // would follow a cursor the new read did not hand out.
  useEffect(() => {
    setExtra([]);
    setMoreError(null);
  }, [data?.generations]);

  // The attempt's own recorded detail, read only once the id in the hash has
  // resolved to a FULL generation id. The route takes the full id and nothing
  // shorter: a suffix is either refused as malformed or, worse, matches an
  // attempt the reader did not ask for. The list page issues this read not at
  // all -- it is the detail page's cost, not everybody's.
  const all = data
    ? [...data.generations.payload.generations, ...extra]
    : [];
  const match = attemptId && data ? resolveAttempt(all, attemptId) : null;
  const fullId = match && match.kind === "one" ? match.attempt.full_generation_id : null;

  const recordedRead = useRecoveringRead<RawAttemptDetail>(
    fullId ? `${clientKey(api)}|${fullId}` : null,
    (signal) => loadAttemptDetail(api, fullId as string, signal),
  );
  const recorded = fullId ? recordedRead.value : null;
  const recordedError = fullId ? recordedRead.error : null;

  // The version these reads answered for, reported up for the top bar. It is
  // the health read's pinned generation unless a version was actually read.
  // Null while the read is in flight: the bar must not name a version this
  // page has not yet seen.
  const readVersion = data
    ? (readingVersion ?? data.health.payload.generation_id ?? null)
    : null;
  useEffect(() => { onReadVersion?.(readVersion); }, [onReadVersion, readVersion]);

  return (
    <ConsoleUpdatesState
      recordedRead={fullId
        ? { data: recorded, error: recordedError }
        : null}
      data={data}
      error={error}
      extra={extra}
      attemptId={attemptId}
      link={link}
      params={params}
      readingVersion={readingVersion ?? null}
      onRead={onRead ?? null}
      askedVersion={askedVersion}
      onReadAgain={list.readAgain}
      retrying={list.retrying}
      range={range}
      onRange={setRange}
      moreError={moreError}
      loadingMore={loadingMore}
      onLoadMore={(cursor, have) => {
        setLoadingMore(true);
        setMoreError(null);
        loadMoreAttempts(api, cursor, have).then(
          (rows) => { setExtra((was) => [...was, ...rows]); setLoadingMore(false); },
          (reason) => { setMoreError(reason); setLoadingMore(false); },
        );
      }}
    />
  );
}

/**
 * Every state this route can be in, as a PURE component.
 *
 * It holds no hooks and issues no reads, so a test can render each state for
 * real. The route above owns the reads and the state; this owns the
 * branching. Before this split the state tests called the route through
 * `renderToStaticMarkup`, which never runs an effect, so "the read failed"
 * and "that attempt id is not here" were both asserted against the loading
 * markup and would have passed whatever those branches rendered.
 */
export function ConsoleUpdatesState({
  data, error, extra, attemptId, link, params, readingVersion, onRead,
  range, onRange, moreError, loadingMore, onLoadMore, recordedRead = null,
  askedVersion = null, onReadAgain = null, retrying = false,
}: {
  /** Re-issue the page's reads. */
  onReadAgain?: (() => void) | null;
  retrying?: boolean;
  /** The attempt-detail read, once an id has resolved. Null on the list, and
   *  on a hash that names no attempt. `data` and `error` both null means the
   *  read is still in flight -- the three states the page must tell apart. */
  recordedRead?: { data: RawAttemptDetail | null; error: unknown } | null;
  data: ConsoleUpdatesData | null;
  error: unknown;
  extra: RawAttempt[];
  attemptId: string | null;
  link: LinkFn;
  params: Record<string, string>;
  readingVersion: string | null;
  onRead: ((fullVersionId: string) => void) | null;
  /** What `?version=` carries, or null. */
  askedVersion?: string | null;
  range: Range;
  onRange: (next: Range) => void;
  moreError: unknown;
  loadingMore: boolean;
  onLoadMore: (cursor: string, have: RawAttempt[]) => void;
}): ReactElement {
  const filter = parseUpdatesFilter(params);
  // An old link's `?version=<short>` is named, not obeyed: obeying it is the
  // collision this rename undoes, and dropping it silently would make a whole
  // table look like a filter that matched everything.
  const ignoredParam = ignoredVersionParam(params);
  // And a `?sort=` this list does not order by: the read returns the
  // attempts in the order it holds them and takes no ordering, so a URL
  // asking for one used to change nothing and say nothing.
  const ignoredSort = ignoredSortParam(params);
  const inputs = data
    ? {
      health: data.health,
      versions: data.versions,
      attempts: data.attemptsByRange[range] ?? null,
      generations: {
        payload: {
          ...data.generations.payload,
          generations: [...data.generations.payload.generations, ...extra],
        },
      },
    }
    : null;

  // ReadFailure, not LoudFailure: this is a live console, and LoudFailure
  // names only the message and then tells the reader to add a read to the
  // FIXTURE GENERATOR -- advice for the design harness, not for an operator
  // whose deployment just refused a read. ReadFailure names the route, the
  // server's own reason code, its detail and the next check.
  if (error) {
    return (
      <main className="okg-console" data-testid="console-updates-read-failed">
        <ReadFailure
          route="/viz/api/console/generations"
          what="This deployment's update attempts"
          error={error}
          onReadAgain={onReadAgain}
          retrying={retrying}
        />
      </main>
    );
  }
  if (!data || !inputs) return <ConsoleLoading what="this deployment" />;

  const reading = readingVersion ?? data.health.payload.generation_id;
  const context = pageContextFor(
    data.health.payload.deployment, reading, data.versions,
    inputs.attempts ? Date.parse(inputs.attempts.payload.now) : Date.now(),
  );
  // The ids the versions read returned. The version note names a `?version=`
  // among them outright; anything else it must judge by shape or withhold.
  const published = data.versions?.payload.versions.map((row) => row.generation_id) ?? [];
  const model = buildUpdatesModel(
    {
      ...inputs,
      producers: data.producers,
      health: { payload: { ...data.health.payload, generation_id: reading } },
    },
    filter,
    link,
  );

  if (attemptId) {
    const all = inputs.generations.payload.generations;
    // A SUFFIX match, so more than one attempt can answer to it. Taking the
    // first would open somebody else's attempt under the id the reader typed
    // and say nothing about it. Two matches is a question, not a page.
    const resolved = resolveAttempt(all, attemptId);
    const matches = resolved.kind === "many" ? resolved.matches : [];
    if (resolved.kind === "many") {
      return (
        <main className="okg-console" data-testid="console-updates-ambiguous">
          <p className="okg-console__back"><a href={link("updates")}>&larr; Updates</a></p>
          <p className="okg-console__fact">
            {exact(matches.length)} update attempts end{" "}
            <code className="okg-console__id">{attemptId}</code>. Open one by its
            full id:{" "}
            {matches.map((attempt, position) => (
              <span key={attempt.full_generation_id}>
                {position > 0 ? ", " : null}
                <a href={link(`updates/${attempt.full_generation_id}`)}>
                  <code className="okg-console__id">{attempt.full_generation_id}</code>
                </a>
              </span>
            ))}
          </p>
        </main>
      );
    }
    const index = resolved.kind === "one" ? resolved.index : -1;
    if (index === -1) {
      // A plain named line, not a failure block and not Home. Nothing failed:
      // the read worked and this id is not among what it returned. The line
      // says the id, how many attempts were searched, and where to go back to.
      return (
        <main className="okg-console" data-testid="console-updates-not-found">
          <p className="okg-console__back"><a href={link("updates")}>&larr; Updates</a></p>
          <p className="okg-console__fact">
            No update attempt ending{" "}
            <code className="okg-console__id">{attemptId}</code> is among the{" "}
            {exact(all.length)}{" "}
            {all.length === 1 ? "attempt" : "attempts"} this read returned for{" "}
            {data.health.payload.deployment}.
          </p>
        </main>
      );
    }
    // The attempt's own producers come from the attempt itself now: the
    // generations read carries the view runs recorded against THIS
    // generation, so nothing has to be matched up from another read.
    const detail = buildAttemptDetail(all[index], {
      previous: all[index + 1] ?? null,
      nowMs: model.nowMs,
      readingVersion: reading,
    });
    // Three states, never merged: in flight, refused by name, or answered.
    const recordedState: RecordedState | null = recordedRead === null
      ? null
      : recordedRead.error
        ? { kind: "failed", error: recordedRead.error }
        : recordedRead.data
          ? {
            kind: "recorded",
            recorded: buildRecordedDetail(recordedRead.data, { link }),
          }
          : { kind: "loading" };
    return (
      <ConsoleAttemptDetail
        detail={detail}
        recorded={recordedState}
        backHref={link("updates")}
        onRead={detail.publishedVersion && onRead && !detail.reading
          ? () => onRead(detail.publishedVersion as string)
          : null}
        context={context}
        askedVersion={askedVersion}
        ignoredParam={ignoredParam}
        published={published}
      />
    );
  }

  const newestBlocked = newestBlockedAttempt(inputs.generations.payload.generations);
  const inline = newestBlocked
    ? {
      detail: buildAttemptDetail(newestBlocked.attempt, {
        previous: newestBlocked.previous,
        nowMs: model.nowMs,
        readingVersion: reading,
      }),
      href: link(`updates/${shortId(newestBlocked.attempt)}`),
    }
    : null;

  const cursor = model.moreCursor;
  const onMore = cursor
    ? () => onLoadMore(cursor, inputs.generations.payload.generations)
    : null;

  const spanAttempts = data.attemptsByRange[range];
  return (
    <ConsoleUpdates
      model={model}
      chart={spanAttempts ? { attempts: spanAttempts, generations: inputs.generations } : null}
      chartFailure={spanAttempts ? null : data.failures?.attempts?.[range] ?? null}
      versionsFailure={data.failures?.versions ?? null}
      producersFailure={data.failures?.producers ?? null}
      onReadAgain={onReadAgain}
      retrying={retrying}
      range={range}
      onRange={onRange}
      onMore={onMore}
      moreError={moreError}
      loadingMore={loadingMore}
      inline={inline}
      context={context}
      askedVersion={askedVersion}
      ignoredParam={ignoredParam}
      ignoredSort={ignoredSort}
      published={published}
    />
  );
}
