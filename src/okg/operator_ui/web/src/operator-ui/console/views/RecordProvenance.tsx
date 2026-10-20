// The record page's Provenance tab: which source the record came from, what
// the source called this record, and what the access policy withheld.
//
// The fact log, the "last written by" attempt link and the enricher tally
// are NOT here. The record detail payload carries no `provenance` key at
// all, so nothing on the wire sources any of the three; they are cut, and
// ui/cuts.md records what read would have had to supply them.
import { CopyableId } from "@operator-ui/console/components/CopyControls";
import type { IdentityField } from "@operator-ui/console/model/explore/identity";
import type { RecordPageModel } from "@operator-ui/console/model/explore/record";
import type { ReactElement } from "react";

// --- Provenance -----------------------------------------------------------------

/** One identity, as its own small list of labelled rows.
 *
 *  This was `JSON.stringify` of whatever the source recorded, printed as one
 *  line: `Source record id {"path":"HcalIsoTrackValidationSim.txt",
 *  "chunk_index":0}`. Every fact in there is worth a line of its own. */
function Identity({ rows, what, model }: { rows: IdentityField[]; what: string; model: RecordPageModel }): ReactElement {
  if (!rows.length) return <span className="okg-console__dim">not recorded by the source</span>;
  if (rows.length === 1 && !rows[0].label) {
    return <CopyableId text={rows[0].full} what={what} />;
  }
  return (
    <ul className="okg-console__identity" data-testid={`explore-identity-${what.replace(/\s+/g, "-")}`}>
      {rows.map((row) => (
        <li key={row.key} className="okg-console__identity-row">
          <span className="okg-console__identity-label">{row.label}</span>
          {row.kind === "hash" || row.key.endsWith("_id") ? (
            <CopyableId text={row.full} what={`${what} ${row.label.toLowerCase()}`}>
              <code className="okg-console__id">{row.value}</code>
            </CopyableId>
          ) : (
            <span
              className={row.kind === "count" || row.kind === "position"
                ? "okg-console__identity-value okg-console__num"
                : "okg-console__identity-value"}
              // What the source actually recorded, when the row shows
              // something else: a chunk's position is its stored index plus
              // one, and the stored index is what a query would use.
              title={row.asRecorded ?? undefined}
            >
              {row.value}
            </span>
          )}
          {row.shortened ? (
            <span
              className="okg-console__shortened"
              title={model.maskedPaths?.rule ?? undefined}
              data-testid="explore-identity-shortened"
            >
              shortened
            </span>
          ) : null}
        </li>
      ))}
    </ul>
  );
}

export function ProvenanceTab({ model }: { model: RecordPageModel }): ReactElement {
  const provenance = model.provenance;
  return (
    <div data-testid="explore-record-provenance">
      <dl className="okg-console__facts">
        <dt>Source</dt>
        <dd>
          {provenance.source ? (
            <>
              {provenance.sourceHref ? (
                <a href={provenance.sourceHref}>{provenance.source}</a>
              ) : (
                <CopyableId text={provenance.source} what="source name" />
              )}
              {/* The ownership id the record carries, when the producer it
                  belongs to is called something else. Both are facts a
                  reader may need to quote. */}
              {provenance.sourceOwnershipId ? (
                <> · <code className="okg-console__id okg-console__dim">{provenance.sourceOwnershipId}</code></>
              ) : null}
            </>
          ) : <span className="okg-console__dim">no source recorded</span>}
        </dd>
        <dt>Source record id</dt>
        <dd><Identity rows={provenance.sourceRecordId} what="source record id" model={model} /></dd>
        <dt>Source revision</dt>
        <dd><Identity rows={provenance.sourceRevision} what="source revision" model={model} /></dd>
        {/* Absent when the policy neither hid nor shortened anything: a line
            saying nothing was done is a line about nothing. */}
        {provenance.policyLine ? (
          <>
            <dt>Policy</dt>
            <dd>{provenance.policyLine}</dd>
          </>
        ) : null}
      </dl>
    </div>
  );
}
