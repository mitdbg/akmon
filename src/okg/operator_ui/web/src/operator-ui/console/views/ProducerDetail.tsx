// One producer's page: what the reads record about it, its records by type,
// and its runs. No recorded error and no configuration YAML: no read carries
// either, and both are cuts in ui/cuts.md.
import { Section } from "@operator-ui/console/components/Section";
import { Status } from "@operator-ui/console/components/StatusWord";
import { CopyTextButton, CopyableId } from "@operator-ui/console/components/CopyControls";
import { LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { type PageContext, producerText } from "@operator-ui/console/model/copyText";
import { exact } from "@operator-ui/console/model/shared/format";
import type { ProducerDetail } from "@operator-ui/console/model/sources/detail";
import { ERROR_FOLD } from "@operator-ui/console/model/sources/rows";
import { VersionNotice } from "@operator-ui/console/components/VersionNote";
import { type ReactElement, useState } from "react";

// --- one producer ---------------------------------------------------------------

function RunTable({ detail }: { detail: ProducerDetail }) {
  return (
    <table className="okg-console__rtable" data-testid="sources-detail-runs-table">
      <thead>
        <tr>
          <th scope="col" data-col="when">When</th>
          <th scope="col" data-col="outcome">Outcome</th>
          <th scope="col" data-col="itemsin">Read</th>
          <th scope="col" data-col="itemsout">Wrote</th>
          <th scope="col" data-col="duration">Duration</th>
          {/* The SLOWEST phase: no read records the order phases ran in, so
              "phase reached" was a claim the data cannot make. */}
          <th scope="col" data-col="phase">Slowest step</th>
        </tr>
      </thead>
      <tbody>
        {detail.runs.map((run) => (
          <tr key={run.key} data-severity={run.mark.severity}>
            <td data-col="when" title={run.whenExact}>
              <span className="okg-console__run-when">{run.when}</span>
              {run.whenAge
                ? <span className="okg-console__run-age">{run.whenAge}</span>
                : null}
            </td>
            <td data-col="outcome"><Status mark={run.mark} /></td>
            <td data-col="itemsin" className="okg-console__rtable-num">{run.read}</td>
            <td data-col="itemsout" className="okg-console__rtable-num" title={run.wrote}>{run.wroteShort}</td>
            <td data-col="duration" className="okg-console__rtable-num">{run.duration}</td>
            <td data-col="phase" title={run.phaseCode ?? undefined}>
              {run.phaseCode
                ? run.phase
                : <code className="okg-console__id">{run.phase}</code>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** One sentence, then the recorded text, folded past ERROR_FOLD lines. The
 *  Copy control hands over the whole text, folded or not (rule 15). */
function Verbatim({
  title, testid, sentence, code, text, lineCount, copyWhat,
}: {
  title: string;
  testid: string;
  sentence: string | null;
  code: string | null;
  text: string;
  lineCount: number;
  copyWhat: string;
}) {
  const [open, setOpen] = useState(false);
  const folded = lineCount > ERROR_FOLD && !open;
  const shownText = folded ? text.split("\n").slice(0, ERROR_FOLD).join("\n") : text;
  return (
    <Section
      title={title}
      testid={testid}
      aside={(
        <span className="okg-console__section-aside">
          {code ? <code className="okg-console__id">{code}</code> : null}
          {code ? " · " : null}
          {exact(lineCount)}
          {lineCount === 1 ? " line" : " lines"}
          <CopyTextButton what={copyWhat} text={text} />
        </span>
      )}
    >
      {sentence ? <p className="okg-console__cause">{sentence}</p> : null}
      <pre className="okg-console__verbatim">{shownText}</pre>
      {lineCount > ERROR_FOLD ? (
        <p className="okg-console__more">
          <button
            type="button"
            className="okg-console__link-button"
            aria-expanded={open}
            onClick={() => setOpen((was) => !was)}
          >
            {open ? `Show the first ${ERROR_FOLD} lines` : `Show all ${exact(lineCount)} lines`}
          </button>
        </p>
      ) : null}
    </Section>
  );
}

export function ConsoleProducerDetail({
  detail, backHref, onMoreRuns, moreError, context, readingVersion = null,
  askedVersion = null, published = [],
}: {
  detail: ProducerDetail;
  backHref: string;
  onMoreRuns: (() => void) | null;
  moreError: unknown;
  context: PageContext;
  /** The version this page's reads answered for, for the version sentence. */
  readingVersion?: string | null;
  /** What `?version=` carries, or null. */
  askedVersion?: string | null;
  /** The generation ids the versions read returned. */
  published?: readonly string[];
}): ReactElement {
  return (
    <main className="okg-console" data-testid="console-sources-detail">
      <p className="okg-console__back"><a href={backHref}>&larr; Sources</a></p>
      <header className="okg-console__header" data-testid="sources-detail-header">
        <div className="okg-console__header-row">
          <h1 className="okg-console__title okg-console__detail-name">
            <CopyableId
              text={detail.name}
              what="producer name"
              codeClassName="okg-console__detail-id"
            />
          </h1>
          <span className="okg-console__detail-kind">{detail.kind}</span>
          {/* The state's served code rides in the chip's title, where a
              reader matching the page to a log finds it; it is no longer a
              line of its own ("recorded as manual_required"). */}
          {detail.configured
            ? <span title={detail.stateCode ?? undefined}><Status mark={detail.mark} /></span>
            : <span className="okg-console__tag">not configured</span>}
          <CopyTextButton
            className="okg-console__copy--page"
            what="producer as text"
            label="Copy as text"
            text={() => producerText(detail, context)}
          />
        </div>
        {/* The answer first: why the producer is in this state, then what
            to do about it, then the facts under them. */}
        {detail.causeSentence || detail.incidentCount !== null ? (
          <p className="okg-console__cause" data-testid="sources-detail-cause">
            {detail.causeSentence ? `Recorded cause: ${detail.causeSentence}.` : null}
            {detail.causeSentence && detail.incidentCount !== null ? " " : null}
            {detail.incidentCount !== null
              ? `${exact(detail.incidentCount)} `
                + `${detail.incidentCount === 1 ? "incident" : "incidents"} counted.`
              : null}
          </p>
        ) : null}
        {detail.nextStep ? (
          <p className="okg-console__fact" data-testid="sources-detail-next-step">
            {detail.nextStep.sentence}
            {detail.nextStep.command ? (
              <>
                {" "}
                <code className="okg-console__id">{detail.nextStep.command}</code>
              </>
            ) : null}
          </p>
        ) : null}
        <p className="okg-console__meta">
          {detail.meta.join(" · ")}
        </p>
        {/* No "Open in Updates" link. It needed an attempt whose
            `stopped_producer` is this producer, and console/generations sets
            that key to null on every attempt because no checkpoint column
            records it (issue #2120). */}
      </header>
      {/* Once, under the title: this page's reads take no version either,
          so the sentence is the same fact the list states. */}
      <VersionNotice
        reading={readingVersion}
        asked={askedVersion}
        published={published}
      />

      <Section title="Contribution" testid="sources-detail-contribution">
        {detail.figures.length ? (
          <p className="okg-console__figures okg-console__figures--detail">
            {detail.figures.map((figure) => (
              <span className="okg-console__figure" key={figure.label}>
                <span className="okg-console__figure-top">
                  <span className="okg-console__figure-value">{exact(figure.value)}</span>
                </span>
                <span className="okg-console__figure-label">
                  {figure.label} {detail.figuresNote}
                </span>
              </span>
            ))}
          </p>
        ) : <p className="okg-console__fact">{detail.figuresNote}</p>}
      </Section>

      {/* What else the version records for this producer. The rollup's
          `source_subtype` dimension is keyed by the same producer expression
          as its `source` total, so this is the same join that produces the
          figure above -- not a second guess at identity. */}
      {detail.showRecordTypes ? (
      <Section
        title="Records by type"
        testid="sources-detail-record-types"
        aside={detail.recordTypesOmitted ? (
          <span className="okg-console__section-aside">
            {exact(detail.recordTypesOmitted)} more not shown
          </span>
        ) : null}
      >
        {detail.recordTypesUnavailable ? (
          <p className="okg-console__fact" data-testid="record-types-unavailable">
            {detail.recordTypesUnavailable}
          </p>
        ) : detail.recordTypeRows.length ? (
          <table className="okg-console__typecount" data-testid="record-types-table">
            <tbody>
              {detail.recordTypeRows.map((row) => (
                <tr key={row.type}>
                  <td data-col="type">
                    {row.href
                      ? <a href={row.href}><code className="okg-console__id">{row.type}</code></a>
                      : <code className="okg-console__id">{row.type}</code>}
                  </td>
                  <td data-col="count">{exact(row.records)}</td>
                  <td data-col="share">
                    <span className="okg-console__share-pct">
                      {row.shareLabel}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="okg-console__fact">
            This producer holds no record in the selected version.
          </p>
        )}
      </Section>
      ) : null}

      {/* Two reads counted this producer's records and did not agree. Said
          here, beside the Contribution figure it is about, and NOT as a
          partial-read sentence: nothing came back short, and the page has
          both figures. */}
      {detail.countDisagreement ? (
        <p className="okg-console__fact" data-testid="sources-detail-count-disagreement">
          {detail.countDisagreement}
        </p>
      ) : null}

      <Section
        title="Run history"
        testid="sources-detail-runs"
        aside={<span className="okg-console__section-aside">{detail.runsLine}</span>}
      >
        {detail.runs.length ? <RunTable detail={detail} /> : null}
        {detail.wroteNote && detail.runs.length ? (
          <p className="okg-console__meta" data-testid="sources-detail-wrote-note">{detail.wroteNote}</p>
        ) : null}
        {detail.runsNote ? (
          <p className="okg-console__fact" data-testid="runs-not-read">
            {detail.runsNote}
          </p>
        ) : null}
        {moreError ? <LoudFailure error={moreError} /> : null}
        {onMoreRuns ? (
          <p className="okg-console__more">
            <button type="button" className="okg-console__link-button" onClick={onMoreRuns}>
              Load more
            </button>
          </p>
        ) : null}
      </Section>

      {detail.reasonTitle && detail.reasonText ? (
        <Verbatim
          title={detail.reasonTitle}
          testid="sources-detail-reason"
          sentence={null}
          code={null}
          text={detail.reasonText}
          lineCount={detail.reasonText.split("\n").length}
          copyWhat={detail.reasonTitle === "Withheld" ? "withheld reason" : "latest issue"}
        />
      ) : null}

      {/* No Configuration section. The reference showed the registry entry
          as YAML with credentials redacted in place, composed in the browser
          from whatever the reads happened to carry. No read returns the
          recorded entry, and serving one means serving credentials-adjacent
          configuration, which needs server-side redaction designed as its own
          change rather than a client-side scrub. A composed near-copy would
          read as the entry itself, so it is cut.

          No "Runtime keys folded into this row" either: console/producers
          resolves a producer by the ownership_id its registry declares and
          returns it once, so nothing is folded. */}
    </main>
  );
}
