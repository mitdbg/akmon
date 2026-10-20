// Home's Contents section: the record types and the relationship types as
// the same table twice, side by side.
import { Section } from "@operator-ui/console/components/Section";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";
import { ROLLUP_ROUTE } from "@operator-ui/console/api/health";
import type { ContentsModel, ContentsRow } from "@operator-ui/console/model/home";
import { exact } from "@operator-ui/console/model/shared/format";
import { SERVES_EVERY_ROUTE, type LinkFn, type ServesRouteFn } from "@operator-ui/console/model/shared/links";

/**
 * One of the two Contents tables.
 *
 * Record types and relationship types are the same KIND of fact -- a name, a
 * count, a share of the version -- so they are the same table twice, side by
 * side. The operator read the earlier form, where record types were a table
 * and relationship types were the sentence "supersedes 52%, mentions 35%,
 * authored_by 9%", and rejected it: two forms for one fact makes the reader
 * translate before comparing.
 */
function TypeTable({
  title, testid, total, unit, rows, moreTypes, moreCount, emptyLine, typeHref = null,
}: {
  /** Where a type row opens: Explore filtered to it, for record types in a
   *  build that serves Explore. Null draws the name as text. */
  typeHref?: ((type: string) => string) | null;
  title: string;
  testid: string;
  total: number;
  /** "records" or "relationships" -- the column head and the tail row. */
  unit: string;
  rows: ContentsRow[];
  moreTypes: number;
  moreCount: number;
  emptyLine: string | null;
}) {
  return (
    <div className="okg-console__typeblock" data-testid={testid}>
      <p className="okg-console__typeblock-title">
        {title}
        {emptyLine ? null : <> {"\u00b7"} {exact(total)}</>}
      </p>
      {emptyLine ? (
        <p className="okg-console__fact">{emptyLine}</p>
      ) : (
        <table className="okg-console__types">
          <thead>
            <tr>
              <th scope="col">Type</th>
              <th scope="col">{unit}</th>
              <th scope="col">Share</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.name}>
                <td>
                  {typeHref
                    ? <a href={typeHref(row.name)}><code className="okg-console__id">{row.name}</code></a>
                    : <code className="okg-console__id">{row.name}</code>}
                </td>
                <td className="okg-console__types-count">{exact(row.count)}</td>
                <td>
                  <span className="okg-console__types-share">
                    <span className="okg-console__share-track">
                      <span
                        className="okg-console__share-bar"
                        style={{ width: `${Math.max(row.share, 1)}%` }}
                        role="img"
                        aria-label={`${row.shareLabel} of ${unit.toLowerCase()}`}
                      />
                    </span>
                    <span className="okg-console__share-pct">{row.shareLabel}</span>
                  </span>
                </td>
              </tr>
            ))}
            {moreTypes > 0 ? (
              <tr className="okg-console__types-tail">
                <td colSpan={3}>
                  {exact(moreTypes)}
                  {moreTypes === 1 ? " more type \u00b7 " : " more types \u00b7 "}
                  {exact(moreCount)} {unit.toLowerCase()}
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      )}
    </div>
  );
}

export function Contents({
  contents, link, failure, pending = false, serves = SERVES_EVERY_ROUTE, onReadAgain = null,
  retrying = false,
}: {
  /** Which routes this build serves. */
  serves?: ServesRouteFn;
  /** Null while the rollup read is outstanding, and null when it failed.
   *  `pending` tells the two apart. */
  contents: ContentsModel | null;
  /** True while the rollup read is still outstanding -- on first load, which
   *  is every load. The panel said "What this version holds could not be
   *  read" with no reason under it for as long as the read took. */
  pending?: boolean;
  link: LinkFn;
  /** The reason that read gave, printed in place of the tables. */
  failure?: unknown;
  /** Re-issue the rollup read alone. */
  onReadAgain?: (() => void) | null;
  retrying?: boolean;
}) {
  // The read is still in flight: the panel says so and draws no table. A
  // failure panel here would name a failure that has not happened.
  if (contents === null && pending) {
    return (
      <Section title="Contents" testid="console-contents">
        <p className="okg-console__fact" data-testid="console-contents-pending">
          Reading what this version holds…
        </p>
      </Section>
    );
  }
  // The panel's read failed: say which read, what the server said and what to
  // check, and draw no table. An empty table here would read as a version
  // that holds nothing.
  if (contents === null) {
    return (
      <Section title="Contents" testid="console-contents">
        <ReadFailure
          route={ROLLUP_ROUTE}
          error={failure}
          what="What this version holds"
          onReadAgain={onReadAgain}
          retrying={retrying}
        />
      </Section>
    );
  }
  // Two tables of the same shape, side by side at 1440 and stacked at 390.
  // Each carries its own count, so rule 1 holds for each table rather than for
  // the pair.
  return (
    <Section title="Contents" testid="console-contents">
      <div className="okg-console__typegrid">
        <TypeTable
          title="Record types"
          testid="console-contents-records"
          total={contents.recordTypes}
          unit="Records"
          rows={contents.topRecordTypes}
          typeHref={serves("explore") ? (type) => link("explore", { type }) : null}
          moreTypes={contents.moreTypes}
          moreCount={contents.moreTypeRecords}
          emptyLine={contents.recordsEmptyLine}
        />
        <TypeTable
          title="Relationship types"
          testid="console-contents-relationships"
          total={contents.relationshipTypes}
          unit="Relationships"
          rows={contents.topRelationshipTypes}
          moreTypes={contents.moreRelationshipTypes}
          moreCount={contents.moreRelationshipCount}
          emptyLine={contents.relationshipsEmptyLine}
        />
      </div>
      {/* Nothing to explore on a version that holds nothing -- and nothing to
          offer at all in a build that does not serve Explore. The tables above
          already say what the version holds; a dead "Explore records" would
          add no fact and one broken promise. */}
      {serves("explore") && !(contents.recordsEmptyLine && contents.relationshipsEmptyLine) ? (
        <p className="okg-console__more"><a href={link("explore")}>Explore records</a></p>
      ) : null}
    </Section>
  );
}
