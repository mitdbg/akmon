// The record page's Raw tab: the stored row, the edges and the retained
// payload or the refusal, verbatim.
import { CopyTextButton, CopyableId } from "@operator-ui/console/components/CopyControls";
import { JsonTree } from "@operator-ui/console/components/JsonTree";
import { LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { RAW_ROUTE } from "@operator-ui/console/api/artifacts";
import { bytesLabel } from "@operator-ui/console/model/explore/content";
import { INLINE_PAYLOAD_LIMIT, RAW_HEAD_LINES, type RawRead, edgeCountLine, payloadText, prettyJson, refusalDetail, refusalSentence } from "@operator-ui/console/model/explore/raw";
import type { RecordPageModel } from "@operator-ui/console/model/explore/record";
import { exact } from "@operator-ui/console/model/shared/format";
import { Fragment, type ReactElement, useState } from "react";

// --- Raw ------------------------------------------------------------------------

function EdgeRows({ raw }: { raw: RawRead }): ReactElement {
  const [open, setOpen] = useState<number | null>(null);
  return (
    <div className="okg-console__block" data-testid="explore-raw-edges">
      <div className="okg-console__block-head">
        <span className="okg-console__block-title">Edge rows</span>
        <span className="okg-console__section-aside">
          {edgeCountLine(raw)}
          {raw.edges.length ? <CopyTextButton what="edge rows" text={prettyJson(raw.edges.map((edge) => edge.row))} /> : null}
        </span>
      </div>
      {raw.edges.length ? (
        <table className="okg-console__ltable okg-console__edgetable">
          <thead>
            <tr>
              <th scope="col" data-col="rel">Relationship</th>
              <th scope="col" data-col="src">From</th>
              <th scope="col" data-col="dst">To</th>
              <th scope="col" data-col="open" aria-label="Show the row as stored" />
            </tr>
          </thead>
          <tbody>
            {raw.edges.map((edge, index) => (
              <Fragment key={index}>
                <tr className="okg-console__prow" data-open={open === index ? "true" : "false"} onClick={() => setOpen(open === index ? null : index)}>
                  <td data-col="rel"><code className="okg-console__id">{edge.relationship}</code></td>
                  <td data-col="src" title={edge.src}><code className="okg-console__id">{edge.src}</code></td>
                  <td data-col="dst" title={edge.dst}><code className="okg-console__id">{edge.dst}</code></td>
                  <td data-col="open">
                    <button
                      type="button"
                      className="okg-console__link-button"
                      aria-expanded={open === index}
                      onClick={(event) => { event.stopPropagation(); setOpen(open === index ? null : index); }}
                    >
                      {open === index ? "close" : "json"}
                    </button>
                  </td>
                </tr>
                {open === index ? (
                  <tr className="okg-console__edge-json">
                    <td colSpan={4}><pre className="okg-console__verbatim">{prettyJson(edge.row)}</pre></td>
                  </tr>
                ) : null}
              </Fragment>
            ))}
          </tbody>
        </table>
      ) : <p className="okg-console__fact">No edge row is stored for this record.</p>}
    </div>
  );
}

function PayloadBlock({ raw }: { raw: RawRead }): ReactElement {
  const payload = raw.payload;
  if (!payload) {
    return (
      <div className="okg-console__block" data-testid="explore-raw-payload">
        <div className="okg-console__block-head"><span className="okg-console__block-title">Retained payload</span></div>
        <p className="okg-console__fact" data-testid="explore-raw-refused">
          {raw.refusal ? refusalSentence(raw.refusal) : "No retained payload: the read gave no reason."}
        </p>
        {/* The server's own words, once and muted, where the sentence above
            put them in the reader's. */}
        {raw.refusal && refusalDetail(raw.refusal) ? (
          <p className="okg-console__meta" data-testid="explore-raw-refused-detail">
            The read says: {refusalDetail(raw.refusal)}.
          </p>
        ) : null}
      </div>
    );
  }
  let parsed: unknown = undefined;
  if (payload.inline !== null) {
    try { parsed = JSON.parse(payload.inline); } catch { parsed = undefined; }
  }
  const facts = (
    <>
      <span title={`${exact(payload.byteCount)} bytes`}>{bytesLabel(payload.byteCount)}</span>
      {" · "}
      <CopyableId text={payload.sha256} what="SHA-256">
        <code className="okg-console__id">sha256 {payload.sha256.slice(0, 12)}…</code>
      </CopyableId>
    </>
  );
  // What the pin rests on, named, with what it does not promise. The
  // caveat is the read's own: a stronger claim than `bound_before_publish`
  // would be one nothing records.
  const pinning = (
    <p className="okg-console__meta" data-testid="explore-raw-pinning">
      Pinning basis <code className="okg-console__id">{payload.pinningBasis}</code>.
      {" "}{payload.pinningCaveat}
    </p>
  );
  if (payload.inline === null) {
    return (
      <div className="okg-console__block" data-testid="explore-raw-payload" data-payload="head">
        <div className="okg-console__block-head">
          <span className="okg-console__block-title">Retained payload</span>
          <span className="okg-console__section-aside">
            {facts}
            {" · "}
            <a href={payload.url} target="_blank" rel="noopener noreferrer">Open raw</a>
            {" · "}
            <a href={payload.url} download>Download</a>
            <CopyTextButton what="retained payload head" text={payloadText(payload)} />
          </span>
        </div>
        <p className="okg-console__meta">
          {exact(payload.byteCount)} bytes is past the {exact(INLINE_PAYLOAD_LIMIT)}-byte inline limit: the first {exact(RAW_HEAD_LINES)}
          {payload.lineCount !== null ? ` of ${exact(payload.lineCount)}` : ""} lines follow; Open raw has the whole file.
        </p>
        {pinning}
        <pre className="okg-console__verbatim okg-console__verbatim--capped">{payload.head}</pre>
      </div>
    );
  }
  return (
    <div className="okg-console__block" data-testid="explore-raw-payload" data-payload="inline">
      <div className="okg-console__block-head">
        <span className="okg-console__block-title">Retained payload</span>
        <span className="okg-console__section-aside">
          {facts}
          {" · "}
          <a href={payload.url} target="_blank" rel="noopener noreferrer">Open raw</a>
          {" · "}
          <a href={payload.url} download>Download</a>
          <CopyTextButton what="retained payload" text={payloadText(payload)} />
        </span>
      </div>
      {pinning}
      {parsed !== undefined
        ? <JsonTree value={parsed} openDepth={2} testid="explore-raw-payload-tree" />
        : <pre className="okg-console__verbatim">{payload.inline}</pre>}
    </div>
  );
}

export function RawTab({ model, failure }: { model: RecordPageModel; failure: unknown }): ReactElement {
  const raw = model.raw;
  if (failure) {
    return (
      <div data-testid="explore-record-raw">
        <LoudFailure error={failure} route={RAW_ROUTE} what="This record's raw layers" />
      </div>
    );
  }
  if (!raw) {
    return (
      <p className="okg-console__fact" data-testid="explore-record-raw">
        This read returned no raw layers: the record detail read does not serve the stored row, the edge rows or the retained payload yet.
      </p>
    );
  }
  return (
    <div data-testid="explore-record-raw">
      {/* The longer copy of the masking paragraph is cut here too: see
          Record.tsx. The Raw tab shows the row as stored, and a value the
          read shortened is shortened in that row. */}
      <div className="okg-console__block" data-testid="explore-raw-row">
        <div className="okg-console__block-head">
          <span className="okg-console__block-title">Stored row</span>
          <span className="okg-console__section-aside">
            {raw.row ? `${exact(Object.keys(raw.row).length)} columns` : null}
            {raw.row ? <CopyTextButton what="stored row" text={prettyJson(raw.row)} /> : null}
          </span>
        </div>
        {raw.row ? <JsonTree value={raw.row} openDepth={3} testid="explore-raw-row-tree" /> : <p className="okg-console__fact">No stored row was returned for this record.</p>}
      </div>
      <EdgeRows raw={raw} />
      <PayloadBlock raw={raw} />
    </div>
  );
}
