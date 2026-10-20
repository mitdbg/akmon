// Home: is the graph current, what is in it, and is anything wrong?
//
// References: ui/references/home.md, and the operator's own least-rejected
// earlier mock ("home v10") for the palette and the panel treatment -- a navy
// rail-coloured top bar over a meander band, a light canvas, white panels with
// a one-pixel border, and a small chart living INSIDE a panel rather than
// spanning the page. Prefect's dashboard and Airbyte's workspace home are the
// research for a chart inside a panel.
//
// Two operator reviews shaped this file and both are recorded in the ledger:
// the contents of the graph open the page as four stat tiles (pass 3), and the
// page is a set of surfaces rather than one flat text document (pass 4).
//
// Reading order: header, figures, Needs attention, Updates (the metric chart
// and the newest four attempts), Sources, Contents, footer. One column at both
// widths.
//
// BOUNDED SIZE, not fixed: every list is capped in consoleModel.ts, so the page
// cannot grow with the deployment; no block reserves space it is not using, so
// a page with less to say is shorter. design/audit-rules.mjs measures both.
import { ConsoleLoading } from "@operator-ui/console/components/ConsoleLoading";
import { Contents } from "@operator-ui/console/views/HomeContents";
import { DEFAULT_RANGE, type Range } from "@operator-ui/console/api/client";
import {
  loadAttemptsRange, loadHomeHead, loadHomeProducers, loadHomeRollup,
  type HomeHeadReads, type HomeProducerReads,
} from "@operator-ui/console/api/health";
import { clientKey, useRecoveringRead } from "@operator-ui/console/components/readRecovery";
import { CopyTextButton, CopyableId } from "@operator-ui/console/components/CopyControls";
import { ConsolePageFailure } from "@operator-ui/console/components/LoudFailure";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";
import { ATTEMPTS_ROUTE, PRODUCERS_ROUTE } from "@operator-ui/console/api/health";
import { MetricChart, MetricChartPending } from "@operator-ui/console/components/MetricChart";
import type { MetricId } from "@operator-ui/console/model/metrics/series";
import { Section } from "@operator-ui/console/components/Section";
import { StateIcon, Status } from "@operator-ui/console/components/StatusWord";
import type { AttemptRow } from "@operator-ui/console/model/attempts";
import { type AttentionModel, type AttentionRow, CHECK_ROW_ID } from "@operator-ui/console/model/attention";
import { homeText } from "@operator-ui/console/model/copyText";
import type { CountLink, ProducersModel } from "@operator-ui/console/model/counts";
import { type ChartInputs, type HeroFigure, type HomeModel, type IdentityModel, buildHomeModel } from "@operator-ui/console/model/home";
import { exact, signed } from "@operator-ui/console/model/shared/format";
import { versionNotReadHere } from "@operator-ui/console/components/VersionNote";
import { VersionIds } from "@operator-ui/console/components/VersionIds";
import { producerGlyph, stateParam } from "@operator-ui/console/model/shared/producerStates";
import { SERVES_EVERY_ROUTE, type LinkFn, type ServesRouteFn } from "@operator-ui/console/model/shared/links";
import type { RawAttemptsByDay, RawRollup } from "@operator-ui/console/model/shared/reads";
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import { type ReactElement, useEffect, useMemo, useRef, useState } from "react";

function Header({ identity, copyText }: { identity: IdentityModel; copyText: () => string }) {
  return (
    <header className="okg-console__header" data-testid="console-header">
      <div className="okg-console__header-row">
        <h1 className="okg-console__title">{identity.deployment}</h1>
        {/* The chip that rests on one check jumps to that check's row. The
            router owns the hash, so the jump is a scroll, not a fragment. */}
        {identity.chip.targetId ? (
          <a
            className="okg-console__chip"
            data-severity={identity.chip.severity}
            href={`#${identity.chip.targetId}`}
            onClick={(event) => {
              event.preventDefault();
              const row = document.getElementById(identity.chip.targetId ?? "");
              row?.scrollIntoView({ block: "center" });
              row?.focus();
            }}
          >
            {identity.chip.label}
          </a>
        ) : (
          <span className="okg-console__chip" data-severity={identity.chip.severity}>
            {identity.chip.label}
          </span>
        )}
        {/* Rule 15: the page's facts as Markdown, at the far right of the h1 row. */}
        <CopyTextButton
          className="okg-console__copy--page"
          what="Home as text"
          label="Copy as text"
          text={copyText}
        />
      </div>
      {identity.neverPublishedSentence ? (
        <p className="okg-console__status-note">{identity.neverPublishedSentence}</p>
      ) : null}
    </header>
  );
}

function Figures({ hero }: { hero: HeroFigure[] }) {
  return (
    <p className="okg-console__figures" data-testid="console-figures">
      {hero.map((figure) => (
        <span className="okg-console__figure" key={figure.label}>
          <span className="okg-console__figure-top">
            <span className="okg-console__figure-value">{exact(figure.value)}</span>
            {/* A delta of zero never reaches here: the model drops it, so the
                place beside the figure is empty rather than holding a "0". */}
            {figure.delta === null ? null : (
              <span className="okg-console__figure-delta">{signed(figure.delta)}</span>
            )}
          </span>
          <span className="okg-console__figure-label">{figure.label}</span>
        </span>
      ))}
    </p>
  );
}

/** One badge in a count strip: icon where the state has one, then the number in
 *  weight 600, then the word in muted. No bold words. */
function CountItem({ count, served }: { count: CountLink; served: boolean }) {
  const body = (
    <>
      {count.icon ? <StateIcon icon={count.icon} /> : null}
      <span className="okg-console__count-value">{exact(count.value)}</span>
      <span className="okg-console__count-label">{count.label}</span>
    </>
  );
  // The count is the fact and it is printed either way; the link is a promise
  // that the view behind it exists, and this build only makes that promise
  // for the views it serves.
  if (!served) {
    return (
      <span className="okg-console__count" data-severity={count.severity} data-served="false">
        {body}
      </span>
    );
  }
  return (
    <a className="okg-console__count" href={count.href} data-severity={count.severity}>
      {body}
    </a>
  );
}

/**
 * The Sources block: the state bar, the count strip, and the link.
 *
 * It named the producers that need something for one pass, and Needs
 * attention -- three blocks above it -- named the same three, with the same
 * state word and the same cause. Two printings of one list, and the reader
 * had to compare them to find out they were the same list. The names live
 * under Needs attention, which is the block the page is ranked around;
 * this block is the census. `ui/cuts.md` records it.
 *
 * Where one good state covers every producer there is nothing to count, so
 * the whole block is one line: "6 sources, all fresh".
 */
function Sources({
  producers, serves, pending, failure, onReadAgain = null, retrying = false,
}: {
  producers: ProducersModel | null;
  serves: ServesRouteFn;
  /** True while the three producer reads are still outstanding. */
  pending: boolean;
  /** The producer reads' failure, when they failed. This block is the ONE
   *  place Home prints it; Needs attention says only that its list is short. */
  failure?: unknown;
  onReadAgain?: (() => void) | null;
  retrying?: boolean;
}) {
  const servesSources = serves("sources");
  if (producers === null) {
    return (
      <Section title="Sources" testid="console-sources">
        {pending
          ? <p className="okg-console__fact" data-testid="console-sources-pending">Reading the producers…</p>
          : (
            <ReadFailure
              route={PRODUCERS_ROUTE}
              error={failure}
              what="The configured producers"
              onReadAgain={onReadAgain}
              retrying={retrying}
            />
          )}
      </Section>
    );
  }
  return (
    <Section title="Sources" testid="console-sources">
      {producers.segments.length ? (
        <span
          className="okg-console__statebar"
          role="img"
          aria-label={`Producers by state: ${producers.segments
            .map((segment) => `${segment.count} ${producerGlyph(segment.state).word.toLowerCase()}`)
            .join(", ")}.`}
        >
          {producers.segments.map((segment) => (
            <span
              key={segment.state}
              className="okg-console__statebar-part"
              data-state={stateParam(segment.state)}
              data-severity={producerGlyph(segment.state).severity}
              style={{ width: `${segment.share}%` }}
              role="img"
              aria-label={`${segment.count} ${producerGlyph(segment.state).word.toLowerCase()}`}
            />
          ))}
        </span>
      ) : null}
      {producers.counts.length ? (
        <p className="okg-console__count-line">
          {producers.counts.map((count) => (
            <CountItem key={count.label} count={count} served={servesSources} />
          ))}
        </p>
      ) : null}
      <p className="okg-console__more">
        {servesSources
          ? <a href={producers.href}>{producers.allGoodLine ?? producers.linkLabel}</a>
          : (producers.allGoodLine ?? producers.linkLabel)}
      </p>
    </Section>
  );
}

/**
 * The Updates section: the metric chart, then the newest four attempts.
 *
 * The chart is the shared MetricChart; Home opens it on records added, because
 * what the graph gained is a fact about the graph, and the reader can switch
 * it to relationships, attempts by outcome or duration. A deployment with no
 * attempt renders no chart and no controls for one: an empty axis with a
 * sentence under it is a block that says nothing, and the sentence below
 * already says it.
 */
function Updates({
  chart, attempts, total, noAttemptsSentence, range, onRange, link, offeredMetrics, serves,
  chartFailure, chartPending, onReadAgain = null, retrying = false,
}: {
  /** Re-issue this span's attempts read alone. */
  onReadAgain?: (() => void) | null;
  retrying?: boolean;
  serves: ServesRouteFn;
  /** Null while the chosen span is being READ and null when the attempts read
   *  FAILED. `chartPending` tells the two apart. The page said the second of
   *  them for both: switching the span rendered a read-failure panel built
   *  from a null error, which printed the word "null" and an invented "The
   *  read did not reach the server", and took the span control away with it. */
  chart: ChartInputs | null;
  /** True while the chosen span's read is outstanding. */
  chartPending: boolean;
  chartFailure?: unknown;
  /** Passed straight to the chart: which metrics this build can draw. */
  offeredMetrics?: readonly MetricId[];
  attempts: AttemptRow[];
  total: number;
  noAttemptsSentence: string | null;
  range: Range;
  onRange: (next: Range) => void;
  link: LinkFn;
}) {
  const servesUpdates = serves("updates");
  return (
    <Section title="Updates" testid="console-updates">
      {chart === null ? (
        chartPending ? (
          <MetricChartPending
            range={range}
            onRange={onRange}
            offeredMetrics={offeredMetrics}
            testid="console-metric-chart"
          />
        ) : (
          <ReadFailure
            route={ATTEMPTS_ROUTE}
            error={chartFailure}
            what="How often this deployment publishes"
            onReadAgain={onReadAgain}
            retrying={retrying}
          />
        )
      ) : noAttemptsSentence ? null : (
        <MetricChart
          attempts={chart.attempts}
          generations={chart.generations}
          defaultMetric={offeredMetrics && !offeredMetrics.includes("records")
            ? offeredMetrics[0]
            : "records"}
          offeredMetrics={offeredMetrics}
          range={range}
          onRange={onRange}
          testid="console-metric-chart"
        />
      )}

      {noAttemptsSentence
        ? <p className="okg-console__fact">{noAttemptsSentence}</p>
        : (
          <ul className="okg-console__rows">
            {attempts.map((row) => (
              <li className="okg-console__row okg-console__copyrow" key={row.key}>
                <span className="okg-console__row-main">
                  <Status mark={row.outcome} />
                  {row.headline ? (
                    <span className="okg-console__row-headline">{row.headline}</span>
                  ) : null}
                  {row.count > 1 ? (
                    <span className="okg-console__dim">{`${exact(row.count)}\u00d7`}</span>
                  ) : null}
                </span>
                <span className="okg-console__row-when">
                  {/* The row opens its attempt. */}
                  {servesUpdates
                    ? (
                      <a
                        href={link(`updates/${row.fullVersion}`)}
                        aria-label={`Open attempt ${row.version}, ${row.when}`}
                      >
                        {row.when}
                      </a>
                    )
                    : row.when}
                  {row.superseded ? <span className="okg-console__tag">superseded</span> : null}
                </span>
                <span className="okg-console__row-sub">
                  <CopyableId
                    text={row.fullVersion}
                    what={row.outcome.word === "Published" ? "version id" : "attempt id"}
                    reveal="row"
                  >
                    <code className="okg-console__id">{row.version}</code>
                  </CopyableId>
                  {" "}
                  {row.detail}
                </span>
              </li>
            ))}
          </ul>
        )}
      {/* No link to a page that lists nothing, and none to a page this build
          does not serve: the number of attempts is the fact, and "View all"
          on a hash that renders Home again reads as a page that broke. */}
      {total > 0 ? (
        <p className="okg-console__more">
          {servesUpdates
            ? <a href={link("updates")}>View all update attempts ({exact(total)})</a>
            : `${exact(total)} update attempts`}
        </p>
      ) : null}
    </Section>
  );
}

/** One attention row. A collapsed row names how many it stands for and can
 *  expand in place. */
function AttentionRowView({ row, serves }: { row: AttentionRow; serves: ServesRouteFn }) {
  const [expanded, setExpanded] = useState(false);
  // A producer row opens that producer's page; a runtime check and a
  // collapsed group carry no href, and a build that does not serve Sources
  // prints the subject rather than promising a page it answers with Home.
  const linked = row.href !== null && serves("sources");
  return (
    <li
      className="okg-console__row"
      data-severity={row.outcome.severity}
      id={row.key.startsWith("check:") ? CHECK_ROW_ID(row.key.slice("check:".length)) : undefined}
      tabIndex={row.key.startsWith("check:") ? -1 : undefined}
    >
      <span className="okg-console__row-main">
        {linked
          ? <a className="okg-console__row-headline" href={row.href ?? ""}>{row.subject}</a>
          : <span className="okg-console__row-headline">{row.subject}</span>}
      </span>
      <span className="okg-console__row-state">
        <Status mark={row.outcome} />
        {row.kindLabel ? (
          <span className="okg-console__tag">{row.kindLabel}</span>
        ) : null}
      </span>
      {/* The age is labelled now ("last run 3 d ago"), which is too long for
          the age column, so it rides on the line under the subject. */}
      <span className="okg-console__row-age" />
      {row.cause || row.time ? (
        <span className="okg-console__row-sub">
          {[row.cause, row.time].filter(Boolean).join(" \u00b7 ")}
        </span>
      ) : null}
      {/* Nothing to expand where the cause already names the members. */}
      {row.count > 1 && row.members.length > 0 ? (
        <span className="okg-console__row-expand">
          <button
            type="button"
            className="okg-console__link-button"
            aria-expanded={expanded}
            onClick={() => setExpanded((open) => !open)}
          >
            {expanded ? "hide" : `show ${exact(row.count)}`}
          </button>
          {expanded ? (
            <span className="okg-console__row-members">{row.members.join(", ")}</span>
          ) : null}
        </span>
      ) : null}
    </li>
  );
}

function Attention({
  model, serves, producersPending, producersFailed = false,
}: {
  /** The producers read failed. Its failure is printed ONCE, in the Sources
   *  block; here the list says only that it holds the runtime checks alone,
   *  as it does while the read is outstanding. It printed the failure twice
   *  before, the second time as "· undefined" (audit 2026-09-23). */
  producersFailed?: boolean;
  model: AttentionModel;
  serves: ServesRouteFn;
  /** True while the producer reads are outstanding: the list is then the
   *  runtime checks alone, and the block says so rather than letting a reader
   *  take a short list for a complete one. */
  producersPending: boolean;
}) {
  // The count is of ROWS. It counted items, so a group of two sources read
  // as two and the heading said "Needs attention · 4" over three rows
  // (review 2026-09-22); the group's own subject says how many it holds.
  const open = model.openCount;
  return (
    <Section
      title="Needs attention"
      // A NUMBER here is a claim that this is everything, and while the
      // producers read is outstanding the list is the runtime checks alone.
      // The heading read "Needs attention · 2" over a body that said it was
      // still reading the producers, and the 2 became 14 a moment later. It
      // says it is reading instead, in the same place, so the heading and the
      // block below it cannot say two different things.
      count={producersPending ? "reading\u2026" : (open > 0 ? exact(open) : null)}
      countSeverity={producersPending ? "unrecorded" : model.severity}
      testid="console-attention"
    >
      {model.allClearSentence && !producersPending ? (
        <p className="okg-console__fact">{model.allClearSentence}</p>
      ) : (
        <ul className="okg-console__rows">
          {model.rows.map((row) => (
            <AttentionRowView key={row.key} row={row} serves={serves} />
          ))}
        </ul>
      )}
      {producersPending ? (
        <p className="okg-console__fact" data-testid="console-attention-pending">
          Still reading the producers; only the runtime checks are listed yet.
        </p>
      ) : null}
      {producersFailed ? (
        <p className="okg-console__fact" data-testid="console-attention-producers-failed">
          The producers were not read, so only the runtime checks are listed;
          Sources below says why.
        </p>
      ) : null}
      {model.omitted > 0 ? (
        <p className="okg-console__more">
          <a href={model.omittedHref}>and {exact(model.omitted)} more</a>
        </p>
      ) : null}
    </Section>
  );
}


/** The Markdown "Copy as text" hands over: the same model, one function. */
export function homeCopyText(model: HomeModel, total: number): string {
  return homeText(model, total, {
    deployment: model.identity.deployment,
    version: model.identity.fullVersion,
    publishedAt: model.identity.publishedAt,
    nowMs: model.nowMs,
  });
}

/** "Read again" for each of Home's blocks, each re-issuing its own read. */
export interface HomeReadAgain {
  producers?: (() => void) | null;
  attempts?: (() => void) | null;
  rollup?: (() => void) | null;
  /** Which of the three has a re-read in flight. */
  retrying?: { producers?: boolean; attempts?: boolean; rollup?: boolean };
}

export function ConsoleHome({
  model, total, range, onRange, link, offeredMetrics, rollupFailure = null,
  attemptsFailure = null, producersFailure = null, serves = SERVES_EVERY_ROUTE,
  versionNote = null, readAgain = {},
}: {
  readAgain?: HomeReadAgain;
  /** The attempts read's failure, when it failed. */
  attemptsFailure?: unknown;
  /** The producer reads' failure, when they failed. */
  producersFailure?: unknown;
  /** One sentence about which version these reads answered for, when the top
   *  bar's picker is showing something this page cannot honour. */
  versionNote?: string | null;
  /** Which routes this build serves. The harness serves them all. */
  serves?: ServesRouteFn;
  model: HomeModel;
  total: number;
  range: Range;
  onRange: (next: Range) => void;
  link: LinkFn;
  offeredMetrics?: readonly MetricId[];
  /** The ontology-rollup read's failure, when it failed. */
  rollupFailure?: unknown;
}): ReactElement {
  return (
    <main className="okg-console" data-testid="console-home">
      <Header identity={model.identity} copyText={() => homeCopyText(model, total)} />
      {versionNote ? (
        <p className="okg-console__status-note" data-testid="console-version-note">
          {versionNote}
        </p>
      ) : null}
      <Figures hero={model.hero} />
      <Attention
        model={model.attention}
        serves={serves}
        producersPending={model.producersPending}
        producersFailed={model.producers === null && !model.producersPending}
      />
      <Updates
        chart={model.chart}
        chartPending={model.attemptsPending}
        chartFailure={attemptsFailure}
        attempts={model.recentAttempts}
        total={total}
        noAttemptsSentence={model.noAttemptsSentence}
        range={range}
        onRange={onRange}
        link={link}
        offeredMetrics={offeredMetrics}
        serves={serves}
        onReadAgain={readAgain.attempts ?? null}
        retrying={readAgain.retrying?.attempts ?? false}
      />
      <Sources
        producers={model.producers}
        serves={serves}
        pending={model.producersPending}
        failure={producersFailure}
        onReadAgain={readAgain.producers ?? null}
        retrying={readAgain.retrying?.producers ?? false}
      />
      <Contents
        contents={model.contents}
        pending={model.rollupPending}
        link={link}
        failure={rollupFailure}
        serves={serves}
        onReadAgain={readAgain.rollup ?? null}
        retrying={readAgain.retrying?.rollup ?? false}
      />
      <p className="okg-console__footer" data-testid="console-footer"><VersionIds text={model.footer} /></p>
    </main>
  );
}

/**
 * The route: draw each block as its own reads land.
 *
 * It used to await nine reads -- health, generations, versions, three
 * diagnostics, the rollup and the attempts route at all four spans -- and
 * render "Reading this deployment…" until the slowest answered. On Archiv3
 * the slowest is the source diagnostics read, which is the one the header and
 * the figures do not need at all.
 *
 * Now four groups are in flight at once and each block renders when its own
 * group resolves: the header, the figures and the attempts list when health,
 * generations and versions land; the chart when the attempts read lands;
 * Sources when the three diagnostics reads land; Contents when the rollup
 * lands. A group that fails prints its failure in its own block's place --
 * except the head group, which the whole page is built from, so its failure
 * is still the page's.
 *
 * ONE attempts span is fetched. The other three are fetched when the span
 * control asks for them, and the control shows the span it is loading rather
 * than the old one's bars under a new label.
 */
export function ConsoleHomeRoute({
  api, link, offeredMetrics, serves = SERVES_EVERY_ROUTE, askedVersion = null,
  onReadVersion,
}: {
  api: ApiClient;
  link: LinkFn;
  offeredMetrics?: readonly MetricId[];
  serves?: ServesRouteFn;
  /** The version the URL asked for, when it asked for one. Home's reads take
   *  no version, so the page names it in a sentence beside the version it
   *  DID read rather than appearing to honour it. */
  askedVersion?: string | null;
  /** Called with the full generation id Home's reads answered for, so the top
   *  bar can show that one rather than the one the URL asked for. */
  onReadVersion?: (fullVersion: string | null) => void;
}): ReactElement {
  // Four groups, each its own recovering read: "Read again" on a block
  // re-issues that block's read alone, the bar's schedule re-reads whatever
  // failed, and a click on Home in the bar re-reads them all.
  const client = clientKey(api);
  const headRead = useRecoveringRead<HomeHeadReads>(client, (signal) => loadHomeHead(api, signal));
  const producersRead = useRecoveringRead<HomeProducerReads>(
    client, (signal) => loadHomeProducers(api, signal),
  );
  const rollupRead = useRecoveringRead<RawRollup>(client, (signal) => loadHomeRollup(api, signal));
  const [range, setRange] = useState<Range>(DEFAULT_RANGE);
  // ONE span is read at a time: the one on screen. A span read once is kept
  // for this client, so switching back costs nothing; a span whose read
  // failed is not kept, so its "Read again" and the schedule read it afresh.
  const spans = useRef<{ client: string; byRange: Partial<Record<Range, RawAttemptsByDay>> }>({
    client, byRange: {},
  });
  if (spans.current.client !== client) spans.current = { client, byRange: {} };
  const attemptsRead = useRecoveringRead<RawAttemptsByDay>(`${client}|${range}`, (signal) => {
    const have = spans.current.byRange[range];
    if (have) return Promise.resolve(have);
    return loadAttemptsRange(api, range, signal).then((value) => {
      spans.current.byRange[range] = value;
      return value;
    });
  });

  const head = headRead.value;
  const headError = headRead.error;
  const producers = producersRead.value;
  const producersFailure = producersRead.error;
  const rollup = rollupRead.value;
  const rollupFailure = rollupRead.error;
  const attempts = attemptsRead.value;
  const attemptsFailure = attemptsRead.error;

  const model = useMemo(() => {
    if (!head) return null;
    return buildHomeModel({
      health: head.health,
      generations: head.generations,
      versions: head.versions,
      attempts,
      producers,
      rollup,
      producersPending: producers === null && producersFailure === null,
      // Passed, so Needs attention never formats an absent failure: it
      // printed "· undefined" when this was left out (audit 2026-09-23).
      producersFailure,
      attemptsPending: attempts === null && attemptsFailure === null,
      rollupPending: rollup === null && rollupFailure === null,
    }, link);
  }, [head, producers, producersFailure, rollup, rollupFailure,
    attempts, attemptsFailure, link]);

  // What the bar prints has to be what this page READ. Reporting it up is the
  // only way the bar can know: the pinned generation is in the health read,
  // which the bar does not make.
  const readVersion = head?.health.payload.generation_id ?? null;
  useEffect(() => { onReadVersion?.(readVersion); }, [onReadVersion, readVersion]);

  // Inside the page frame, like every other page of this console. Returned
  // bare -- which this did -- the block starts at the window's left edge with
  // no column, which reads as a page that broke rather than as a page saying
  // what broke.
  //
  // `route` is the fallback only. Every read in the head group goes through
  // `api.get`, which rejects with an ApiRequestError carrying the route it
  // asked for, and the surface prints THAT in preference to this.
  if (headError) {
    // The route printed is the read that failed: every read in the head group
    // rejects with an ApiRequestError naming its own route, a body that was
    // not data included. `console/health` is named only for an error that
    // carries no route, and is the read the whole page is built from.
    return (
      <ConsolePageFailure
        error={headError}
        route="/viz/api/console/health"
        what="This deployment"
        testId="console-home-read-failed"
        onReadAgain={headRead.readAgain}
        retrying={headRead.retrying}
      />
    );
  }
  if (!head || !model) {
    return <ConsoleLoading what="this deployment" />;
  }
  return (
    <ConsoleHome
      model={model}
      total={head.generations.payload.total}
      range={range}
      onRange={setRange}
      link={link}
      offeredMetrics={offeredMetrics}
      rollupFailure={rollupFailure}
      attemptsFailure={attemptsFailure}
      producersFailure={producersFailure}
      readAgain={{
        producers: producersRead.readAgain,
        attempts: attemptsRead.readAgain,
        rollup: rollupRead.readAgain,
        retrying: {
          producers: producersRead.retrying,
          attempts: attemptsRead.retrying,
          rollup: rollupRead.retrying,
        },
      }}
      versionNote={askedVersion && model.identity.version
        // The RAW parameter, and the ids the versions read returned. It used
        // to be pre-shortened by the caller, which is what made the sentence
        // echo whatever arrived on the URL; naming it is the sentence's own
        // decision to make, and it needs the unshortened value to make it.
        ? versionNotReadHere(
          model.identity.version,
          askedVersion,
          head.versions.payload.versions.map((row) => row.generation_id),
        )
        : null}
      serves={serves}
    />
  );
}
