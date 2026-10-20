// One attempt, inline under the table and as its own page: the stages it
// ran and the recorded error verbatim.
import { AttemptRecorded, type RecordedState } from "@operator-ui/console/views/AttemptRecorded";
import { Section } from "@operator-ui/console/components/Section";
import { StateIcon, Status } from "@operator-ui/console/components/StatusWord";
import { VersionNotice } from "@operator-ui/console/components/VersionNote";
import { CopyTextButton, CopyableId } from "@operator-ui/console/components/CopyControls";
import { type PageContext, attemptText } from "@operator-ui/console/model/copyText";
import { exact } from "@operator-ui/console/model/shared/format";
import type { AttemptDetail, MultilineField } from "@operator-ui/console/model/updates/detail";
import type { ReactElement } from "react";

/**
 * The newest finished attempt, when it is blocked, one level down on the same
 * page.
 *
 * Rule 3 puts the failure text one level down; on the page an operator opens
 * from an alert, that level is here rather than behind a chevron. It renders
 * only while the newest finished attempt is a failure, so a deployment whose
 * last finished attempt published has nothing here.
 */
export function InlineAttempt({ detail, href }: { detail: AttemptDetail; href: string }) {
  return (
    <section className="okg-console__inline" data-testid="updates-inline-detail">
      <div className="okg-console__section-head okg-console__inline-head">
        <h2 className="okg-console__section-title">Newest blocked attempt</h2>
        <a className="okg-console__section-aside" href={href}>Open attempt</a>
      </div>
      <p className="okg-console__inline-headline">
        <span className="okg-console__status" data-severity={detail.outcome.severity}>
          <StateIcon icon={detail.outcome.icon} />
        </span>
        {detail.headline}
      </p>
      <p className="okg-console__meta">{detail.meta.join(" · ")}</p>
      <AttemptFacts detail={detail} />
      <ProducerList detail={detail} />
      {detail.blockedText || detail.blockedOversizeLine ? <BlockedBlock detail={detail} /> : null}
    </section>
  );
}

/**
 * A multi-line string inside a recorded value, shown with real line breaks.
 *
 * The JSON block beside it is the record and is what "Copy" hands over, with
 * every newline still escaped, so a copied blocker round-trips. This is the
 * same value displayed: every line renders, which is what the pinned
 * reference asks for. It is set as TEXT, so a value that looks like markup is
 * shown rather than mounted.
 */
function MultilineBlocks({ fields }: { fields: MultilineField[] }) {
  if (!fields.length) return null;
  return (
    <>
      {fields.map((field) => (
        <div className="okg-console__verbatim-field" key={field.path}>
          <p className="okg-console__meta">
            <code className="okg-console__id">{field.path}</code>
            {" \u00b7 "}
            {exact(field.lineCount)}
            {field.lineCount === 1 ? " line" : " lines"}
          </p>
          <pre className="okg-console__verbatim">{field.text}</pre>
        </div>
      ))}
    </>
  );
}

function ProducerList({ detail }: { detail: AttemptDetail }) {
  return (
    // "View producers" read as a control, and nothing under it was viewable.
    // What the section holds is the attempt's producer runs, under the name
    // the recorded section on the same page already uses. The two are never
    // drawn together: ConsoleAttemptDetail draws this one only while the
    // attempts/detail read has not answered.
    <Section title="Producer runs" testid="updates-detail-producers">
      {detail.runningNote ? <p className="okg-console__fact">{detail.runningNote}</p> : null}
      {detail.viewRunsNote ? (
        <p
          className="okg-console__fact"
          data-testid={detail.viewRunsUnavailable
            ? "updates-detail-runs-unavailable"
            : "updates-detail-runs-none"}
        >
          {detail.viewRunsNote}
        </p>
      ) : null}
      {detail.viewRunsCapLine ? (
        <p className="okg-console__fact" data-testid="updates-detail-runs-capped">
          {detail.viewRunsCapLine}
        </p>
      ) : null}
      {detail.viewRuns.length ? (
        <ul className="okg-console__stage-rows">
          {detail.viewRuns.map((run) => (
            <li
              className="okg-console__stage-row okg-console__copyrow"
              key={run.key}
              data-severity={run.mark.severity}
            >
              <CopyableId text={run.name} what="producer name" reveal="row" />
              {run.kind ? (
                <span className="okg-console__tag">{run.kind}</span>
              ) : null}
              <Status mark={run.mark} />
              <span className="okg-console__stage-detail">{run.timing}</span>
              {/* The producer's own error, as data. A run records it as JSON;
                  it is set as escaped text and never interpreted. */}
              {run.errorText ? (
                <pre className="okg-console__verbatim">{run.errorText}</pre>
              ) : null}
              <MultilineBlocks fields={run.errorMultiline} />
            </li>
          ))}
        </ul>
      ) : null}
      {/* Said plainly rather than left as a gap the reader has to notice. */}
      <p className="okg-console__fact okg-console__none">{detail.sourcesNote}</p>
    </Section>
  );
}

/** What the attempt changed, and what the gate excluded. Both are recorded on
 *  every attempt, whatever its outcome. */
function AttemptFacts({ detail }: { detail: AttemptDetail }) {
  if (!detail.change && !detail.excludedLine) return null;
  return (
    <>
      {detail.change ? (
        <p className="okg-console__fact" data-testid="updates-detail-change">
          {detail.change}
        </p>
      ) : null}
      {detail.excludedLine ? (
        <p className="okg-console__fact" data-testid="updates-detail-excluded">
          {detail.excludedLine}
        </p>
      ) : null}
    </>
  );
}

/**
 * The recorded blocker, in full.
 *
 * Every line is rendered, unclamped and untruncated, and as TEXT: the value is
 * the publisher's own jsonb and it is set inside a `<pre>` as a string, so
 * React escapes it. Nothing here interprets it as markup, and no key of it is
 * treated as an instruction. Where the record was too large to serve, the
 * server withheld it whole and said how large it was; the page prints that
 * line rather than an empty block.
 */
function BlockedBlock({ detail }: { detail: AttemptDetail }) {
  return (
    <Section
      title="Recorded blocker"
      testid="updates-detail-error"
      aside={(
        <span className="okg-console__section-aside">
          {detail.code ? <code className="okg-console__id">{detail.code}</code> : null}
          {detail.code && detail.blockedText ? " \u00b7 " : null}
          {detail.blockedText ? (
            <>
              {exact(detail.blockedLineCount)}
              {detail.blockedLineCount === 1 ? " line" : " lines"}
              {/* Rule 15: the block verbatim, exactly as recorded. */}
              <CopyTextButton what="recorded blocker" text={detail.blockedText} />
            </>
          ) : null}
        </span>
      )}
    >
      {detail.blockedOversizeLine ? (
        <p className="okg-console__fact" data-testid="updates-detail-oversize">
          {detail.blockedOversizeLine}
        </p>
      ) : null}
      {detail.blockedText ? (
        <pre className="okg-console__verbatim">{detail.blockedText}</pre>
      ) : null}
      <MultilineBlocks fields={detail.blockedMultiline} />
    </Section>
  );
}

export function ConsoleAttemptDetail({
  detail, backHref, onRead, context, recorded = null, askedVersion = null,
  ignoredParam = null, published = [],
}: {
  /** What to say about a `version` parameter this page does not read. */
  ignoredParam?: string | null;
  /** The generation ids the versions read returned, so the note can name a
   *  real one rather than withholding it for failing a shape test. */
  published?: readonly string[];
  /** What the top bar's version picker put on the URL, when it put anything.
   *  Null from the Updates list, which draws its own note above the table:
   *  this component is rendered inside that page too, and the fact belongs to
   *  the page once, not to every block that could carry it. */
  askedVersion?: string | null;
  detail: AttemptDetail;
  backHref: string;
  onRead: (() => void) | null;
  context: PageContext;
  /** What the attempts/detail read says about this attempt, or its failure.
   *  Null on the Updates list's inline block, which has no such read: the
   *  page then shows only what the generations read carried, as before. */
  recorded?: RecordedState | null;
}): ReactElement {
  // The producer list is drawn ONCE. When the detail read answered, its own
  // runs section carries the same rows with the attempt's real total and cap,
  // so drawing both would put the same producers on the page twice under two
  // different totals.
  const recordedRuns = recorded !== null && recorded.kind === "recorded";
  return (
    <main className="okg-console" data-testid="console-updates-detail">
      <p className="okg-console__back"><a href={backHref}>&larr; Updates</a></p>
      <header className="okg-console__header" data-testid="updates-detail-header">
        <div className="okg-console__header-row">
          <h1 className="okg-console__title okg-console__detail-title">
            <span className="okg-console__status" data-severity={detail.outcome.severity}>
              <StateIcon icon={detail.outcome.icon} />
            </span>
            {detail.headline}
            {detail.reading ? <span className="okg-console__tag">reading</span> : null}
          </h1>
          <CopyTextButton
            className="okg-console__copy--page"
            what="attempt as text"
            label="Copy as text"
            text={() => attemptText(
              detail, context, recordedRuns ? recorded.recorded : null,
            )}
          />
        </div>
        <p className="okg-console__meta">{detail.meta.join(" · ")}</p>
        <p className="okg-console__meta">
          <CopyableId text={detail.fullId} what="attempt id" />
        </p>
        <AttemptFacts detail={detail} />
        {onRead ? (
          <p className="okg-console__more">
            <button type="button" className="okg-console__readbutton" onClick={onRead}>
              Read this version
            </button>
          </p>
        ) : null}
      </header>
      <VersionNotice
        reading={context.version}
        asked={askedVersion}
        ignoredParam={ignoredParam}
        published={published}
      />
      {recordedRuns ? null : <ProducerList detail={detail} />}
      {detail.blockedText || detail.blockedOversizeLine ? <BlockedBlock detail={detail} /> : null}
      {recorded ? <AttemptRecorded state={recorded} /> : null}
    </main>
  );
}
