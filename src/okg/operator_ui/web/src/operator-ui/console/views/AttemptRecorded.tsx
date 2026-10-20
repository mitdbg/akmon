// What ONE attempt recorded, drawn from the attempts/detail read.
//
// The operator's verdict on 2026-09-21: the Updates list "looks good but the
// detailed update view shows nothing". It showed a headline, an id, one
// change line and two sentences about what is not recorded, because that was
// everything the generations read carried per attempt. These sections are the
// rest of what the deployment actually wrote down.
//
// Drawn in the shell approved for Home: one column, no cards, no tiles, no
// charts, hairline rows, a word beside every glyph. Read-only -- nothing here
// is a control, and the only buttons on the page belong to its parent's copy
// header.
//
// The order is the order an operator asks the questions in: what was this
// attempt, what does the version hold, what moved and where, what checked it,
// which producers ran, and what was configured when it ran. A section whose
// data the deployment did not record is CUT; an absent section says nothing.
// A section the read REFUSED prints the read's own reason instead.
import { Section } from "@operator-ui/console/components/Section";
import { Status } from "@operator-ui/console/components/StatusWord";
import {
  ReadFailure,
  namedReadFailure,
  notFoundKind,
  routeNotServedSentence,
  unrecognisedNotFoundSentence,
} from "@operator-ui/console/components/ReadFailure";
import { type CountTable, DELTA_HEADER, type RecordedDetail } from "@operator-ui/console/model/updates/recorded";
import type { ReactElement } from "react";

export const ATTEMPT_DETAIL_ROUTE = "/viz/api/console/attempts/detail";

/** Every state the recorded sections can be in. The route owns the read; this
 *  owns the branching, so each branch can be rendered for real by a test. */
export type RecordedState =
  | { kind: "loading" }
  | { kind: "failed"; error: unknown }
  | { kind: "recorded"; recorded: RecordedDetail };

/** One dimension's counts at this version against its parent's. */
function CountSection({ table }: { table: CountTable }) {
  return (
    <Section title={table.title} testid={table.testid}>
      {table.refusedNote ? (
        <p className="okg-console__fact" data-testid={`${table.testid}-refused`}>
          {table.refusedNote}
        </p>
      ) : null}
      {table.parentNote ? (
        <p className="okg-console__fact">{table.parentNote}</p>
      ) : null}
      {table.rows.length ? (
        <table className="okg-console__ctable">
          <thead>
            <tr>
              <th data-column="name" scope="col">Name</th>
              <th data-column="count" scope="col">At this version</th>
              <th data-column="delta" scope="col">{DELTA_HEADER}</th>
            </tr>
          </thead>
          <tbody>
            {table.rows.map((row) => (
              <tr className="okg-console__crow" key={row.name}>
                <td data-column="name">
                  {row.href
                    ? <a href={row.href}><code className="okg-console__id">{row.name}</code></a>
                    : <code className="okg-console__id">{row.name}</code>}
                </td>
                <td data-column="count">
                  {row.count}
                  {/* The delta again, shown only at the width that cuts its
                      column. At 390 two fixed 168 px columns left the name
                      22 px and every row read `d...`, `c...`, `l...`. The
                      column goes and the fact it carried moves here, the
                      same way the attempts list moves Change under the
                      outcome. Same text, one source. */}
                  <span className="okg-console__crow-delta-narrow">
                    {row.delta ?? row.parentNote}
                  </span>
                </td>
                {/* A parent that could not be read is named here, never
                    rendered as a delta of zero: zero is a measured fact
                    about the parent, and this is the absence of one. */}
                <td data-column="delta">{row.delta ?? row.parentNote}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
      {table.capLine ? <p className="okg-console__fact">{table.capLine}</p> : null}
    </Section>
  );
}

function Recorded({ recorded }: { recorded: RecordedDetail }): ReactElement {
  return (
    <>
      {recorded.holdsLine || recorded.movedLine || recorded.holdsNotes.length ? (
        <Section title="What this version holds" testid="updates-detail-holds">
          {recorded.holdsLine ? (
            <p className="okg-console__fact">{recorded.holdsLine}</p>
          ) : null}
          {/* Only what the header line above has not already said: the
              retraction, and the update no totals delta can see. */}
          {recorded.movedLine ? (
            <p className="okg-console__fact" data-testid="updates-detail-moved">
              {recorded.movedLine}
            </p>
          ) : null}
          {recorded.holdsNotes.map((line) => (
            <p className="okg-console__fact okg-console__none" key={line}>{line}</p>
          ))}
        </Section>
      ) : null}

      {/* Not gated on there being a count table. A refused cycle's rollup
          can come back empty, and then the holds numbers above are the only
          thing on the page -- and they are exactly what this qualifies. */}
      {recorded.unpublishedNote ? (
        <p className="okg-console__fact okg-console__none" data-testid="updates-detail-unpublished">
          {recorded.unpublishedNote}
        </p>
      ) : null}
      {recorded.tables.map((table) => <CountSection key={table.key} table={table} />)}

      {recorded.checksNote || recorded.gates.length ? (
        <Section title="Publication checks" testid="updates-detail-checks">
          {recorded.checksNote ? (
            <p className="okg-console__fact" data-testid="updates-detail-checks-none">
              {recorded.checksNote}
            </p>
          ) : null}
          {recorded.checksSummary ? (
            <p className="okg-console__fact">{recorded.checksSummary}</p>
          ) : null}
          {recorded.gates.length ? (
            <ul className="okg-console__rrows">
              {recorded.gates.map((gate) => (
                <li
                  className="okg-console__rrow"
                  key={gate.key}
                  data-severity={gate.mark.severity}
                >
                  <span className="okg-console__rrow-name">{gate.name}</span>
                  <Status mark={gate.mark} />
                  {gate.detail ? (
                    <span className="okg-console__rrow-detail">{gate.detail}</span>
                  ) : null}
                  {gate.withheldNote ? (
                    <span className="okg-console__rrow-detail">{gate.withheldNote}</span>
                  ) : null}
                  {/* The recorded name, once, muted: the words above are
                      what the gate is; this is what to grep for. */}
                  {gate.code && gate.code !== gate.name ? (
                    <span className="okg-console__rrow-detail okg-console__state-code">
                      <code className="okg-console__id">{gate.code}</code>
                    </span>
                  ) : null}
                </li>
              ))}
            </ul>
          ) : null}
        </Section>
      ) : null}

      {recorded.showRuns ? (
      <Section title="Producer runs" testid="updates-detail-runs">
        {recorded.runsNote ? (
          <p className="okg-console__fact" data-testid="updates-detail-runs-none">
            {recorded.runsNote}
          </p>
        ) : null}
        {recorded.runsCapLine ? (
          <p className="okg-console__fact" data-testid="updates-detail-runs-capped">
            {recorded.runsCapLine}
          </p>
        ) : null}
        {recorded.runs.length ? (
          <ul className="okg-console__rrows">
            {recorded.runs.map((run) => (
              <li
                className="okg-console__rrow"
                key={run.key}
                data-severity={run.mark.severity}
              >
                {/* A definition id runs to 78 characters on the live
                    deployment. It takes the whole first column and wraps
                    inside it rather than sliding under the status word. */}
                <span className="okg-console__rrow-name">
                  <code className="okg-console__id">{run.name}</code>
                  {/* A space, so the id and the tag never read as one word
                      ("...liveness.v4consistency check") in copied text or
                      where the tag's own margin is not drawn. */}
                  {run.kind ? <>{" "}<span className="okg-console__tag">{run.kind}</span></> : null}
                </span>
                <Status mark={run.mark} />
                {run.timing ? (
                  <span className="okg-console__rrow-detail">{run.timing}</span>
                ) : null}
                {/* The producer's own error, as data: set as escaped text
                    inside a <pre>, never interpreted as markup. */}
                {run.errorText ? (
                  <pre className="okg-console__verbatim">{run.errorText}</pre>
                ) : null}
              </li>
            ))}
          </ul>
        ) : null}
      </Section>
      ) : null}

      <Section title="Where this attempt sits" testid="updates-detail-neighbours">
        {recorded.positionLine ? (
          <p className="okg-console__fact">{recorded.positionLine}</p>
        ) : null}
        {recorded.parentLine ? (
          <p className="okg-console__fact">{recorded.parentLine}</p>
        ) : null}
        <p className="okg-console__meta">
          {recorded.newerHref && recorded.newerId ? (
            <a href={recorded.newerHref}>
              &larr; newer attempt <code className="okg-console__id">{recorded.newerId}</code>
            </a>
          ) : null}
          {recorded.newerHref && recorded.olderHref ? " · " : null}
          {recorded.olderHref && recorded.olderId ? (
            <a href={recorded.olderHref}>
              older attempt <code className="okg-console__id">{recorded.olderId}</code> &rarr;
            </a>
          ) : null}
        </p>
      </Section>

      {recorded.configuredLines.length ? (
        <Section title="Configured when it ran" testid="updates-detail-configured">
          {recorded.configuredLines.map((line) => (
            <p className="okg-console__fact" key={line}>{line}</p>
          ))}
          {/* The absences, collapsed. Four sentences of absence in a row is
              a section the reader learns to skip. */}
          {recorded.configuredAbsences.length ? (
            <div className="okg-console__none" data-testid="updates-detail-configured-absent">
              {recorded.configuredAbsences.map((line) => (
                <p className="okg-console__fact" key={line}>{line}</p>
              ))}
            </div>
          ) : null}
        </Section>
      ) : null}
    </>
  );
}

export function AttemptRecorded({ state }: { state: RecordedState }): ReactElement {
  if (state.kind === "loading") {
    return (
      <p className="okg-console__fact" data-testid="updates-detail-recorded-loading">
        Reading what this attempt recorded…
      </p>
    );
  }
  if (state.kind === "failed") {
    const failure = namedReadFailure(ATTEMPT_DETAIL_ROUTE, state.error);
    const notFound = notFoundKind(failure);
    // Nothing failed on a SERVED route's 404: the read worked and this
    // deployment's history holds no such attempt. The body is a CONSTANT,
    // carrying no id and no count, so an id from another deployment reads the
    // same as one that never existed -- and the page says exactly that much
    // and no more. The reason code printed is the server's OWN; this branch
    // is reached only where the server sent one.
    if (notFound === "named") {
      return (
        // The reason code is the SERVER's own and it is kept -- in the title,
        // where the repo's convention puts a machine code, and in the copy
        // text. It is out of the sentence: a reader who came here from an
        // alert needs to know their deployment has no such attempt, and
        // "(attempt_not_found)" in the middle of that sentence is the same
        // fact written a second time in a language they do not read.
        <p
          className="okg-console__fact"
          data-testid="updates-detail-recorded-not-found"
          title={failure.reason ?? undefined}
        >
          This deployment records no attempt under that id.
        </p>
      );
    }
    // The same status from the opposite cause: this read API does not route
    // this path at all, so it says nothing about the attempt. One sentence,
    // inside the attempt page frame, naming the read and no reason code.
    if (notFound === "route-not-served") {
      return (
        <p
          className="okg-console__fact okg-console__none"
          data-testid="updates-detail-recorded-route-unserved"
        >
          {routeNotServedSentence(ATTEMPT_DETAIL_ROUTE)}
        </p>
      );
    }
    // A 404 from neither of those: a body this console cannot read. It says
    // that much and nothing about which routes the read API serves.
    if (notFound === "unrecognised") {
      return (
        <p
          className="okg-console__fact okg-console__none"
          data-testid="updates-detail-recorded-404-unrecognised"
        >
          {unrecognisedNotFoundSentence(ATTEMPT_DETAIL_ROUTE)}
        </p>
      );
    }
    return (
      <div data-testid="updates-detail-recorded-failed">
        <ReadFailure
          route={ATTEMPT_DETAIL_ROUTE}
          what="What this attempt recorded"
          error={state.error}
        />
      </div>
    );
  }
  return <Recorded recorded={state.recorded} />;
}
