// The record page: header, then the Fields, Content and Related tabs. The
// Provenance and Raw tabs are beside it in their own files.
import { ProvenanceTab } from "@operator-ui/console/views/RecordProvenance";
import { RawTab } from "@operator-ui/console/views/RecordRaw";
import { ExportOutlined } from "@operator-ui/console/components/icons";
import { CopyTextButton, CopyableId } from "@operator-ui/console/components/CopyControls";
import { LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { ARTIFACT_ROUTE } from "@operator-ui/console/api/artifacts";
import { Masked } from "@operator-ui/console/components/TextMarks";
import { type PageContext, recordText } from "@operator-ui/console/model/copyText";
import type { ContentModel } from "@operator-ui/console/model/explore/content";
import type { RecordTab } from "@operator-ui/console/model/explore/params";
import { type ChunkRow, type ExploreField, type RecordPageModel, foldValue } from "@operator-ui/console/model/explore/record";
import { RELATED_PAGE } from "@operator-ui/console/model/explore/related";
import { exact } from "@operator-ui/console/model/shared/format";
import { type DocumentViewerKind, noDocumentViewerSentence, useDocumentViewers } from "@operator-ui/console/components/documentViewers";
import type { ViewerState } from "@operator-ui/console/viewers/viewerState";
import { Chip } from "@operator-ui/console/components/Chip";
import { Switcher } from "@operator-ui/console/components/Switcher";
import { type ReactElement, type ReactNode, useCallback, useEffect, useState } from "react";

// --- the record page ------------------------------------------------------------

function FieldValue({ field }: { field: ExploreField }): ReactElement {
  const [open, setOpen] = useState(false);
  if (field.hidden) {
    return (
      // The read carries no policy name on a field row, so none is printed.
      // "by a policy the read did not name" was a sentence about a gap the
      // reader cannot act on; see ui/cuts.md.
      <span className="okg-console__hidden-value">
        <span className="okg-console__hidden">hidden</span>
        {" "}
        <span className="okg-console__dim">by policy</span>
      </span>
    );
  }
  if (field.value === null) {
    // The read listed the key and carried no value for it. An empty cell
    // here reads as a value that is empty, which is a different fact; the
    // copy text says the same words for the same shape (copyText/explore.ts).
    return <span className="okg-console__dim">not recorded by the source</span>;
  }
  const value = field.value;
  const { shown, folded } = foldValue(value);
  return (
    <span className="okg-console__field-value">
      <Masked value={open || !folded ? value : `${shown}…`} />
      {folded ? (
        <>
          {" "}
          <button type="button" className="okg-console__link-button" aria-expanded={open} onClick={() => setOpen((was) => !was)}>
            {open ? "show less" : `show all ${exact(value.length)} characters`}
          </button>
        </>
      ) : null}
    </span>
  );
}

function FieldsTab({ model }: { model: RecordPageModel }): ReactElement {
  return (
    <div data-testid="explore-record-fields">
      <p className="okg-console__tablecount">
        {exact(model.fieldCount)} {model.fieldCount === 1 ? "field" : "fields"}
        {model.hiddenCount ? ` · ${exact(model.hiddenCount)} hidden by policy` : ""}
      </p>
      {model.fieldCount ? (
        <table className="okg-console__ftable" data-testid="explore-fields-table">
          <thead>
            <tr>
              <th scope="col" data-col="name">Field</th>
              <th scope="col" data-col="value">Value</th>
            </tr>
          </thead>
          <tbody>
            {model.fields.map((field) => (
              <tr key={field.key} data-hidden={field.hidden ? "true" : "false"}>
                <td data-col="name"><code className="okg-console__id">{field.key}</code></td>
                <td data-col="value"><FieldValue field={field} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : <p className="okg-console__fact">The read returned no fields for this record.</p>}
    </div>
  );
}

// --- Content --------------------------------------------------------------------

/** The artifact as a person reads it. Images render here; nothing else
 *  does. The pane reports its state on `data-viewer-state` so the capture
 *  script can wait for "ready" and refuse "failed".
 *
 *  A PDF, HTML, Markdown or code artifact is named, not drawn: no route
 *  serves those bytes (#2121), and the four viewers are not in this
 *  bundle. The design harness fills `DocumentViewersContext` and draws
 *  them; the live console leaves it null. */
function RenderedPane({ content }: { content: ContentModel }): ReactElement {
  const [state, setState] = useState<ViewerState>({ kind: "pending" });
  const documentViewers = useDocumentViewers();
  const toolbar = content.toolbar;
  const viewer = content.viewer;
  const onState = useCallback((next: ViewerState) => setState(next), []);
  useEffect(() => {
    // An image reports through its own load events; everything else is a
    // sentence, which is on screen as soon as it is rendered.
    if (viewer && viewer.kind === "image") setState({ kind: "pending" });
    if (viewer && viewer.kind !== "image") setState({ kind: "ready", note: "no viewer" });
  }, [viewer]);
  if (!toolbar || !viewer) return <></>;
  const inBrowser = typeof window !== "undefined";
  const mediaType = (content.artifact as { mediaType: string }).mediaType;
  let body: ReactNode;
  if (viewer.kind === "image") {
    body = !inBrowser ? <p className="okg-console__fact">The image renders in the browser.</p> : (
      <img
        className="okg-console__image"
        data-testid="explore-content-image"
        src={toolbar.url}
        alt={toolbar.fileName}
        onLoad={() => setState({ kind: "ready" })}
        onError={() => setState({ kind: "failed", reason: `the image could not load ${toolbar.url}` })}
      />
    );
  } else if (viewer.kind === "none") {
    body = (
      <p className="okg-console__fact" data-testid="explore-content-none">
        No viewer for <code className="okg-console__id">{mediaType}</code>.
        {" "}Open the original or download it from the toolbar.
      </p>
    );
  } else if (documentViewers) {
    const Viewers = documentViewers;
    body = (
      <Viewers kind={viewer.kind as DocumentViewerKind} url={toolbar.url} language={viewer.language} fileName={toolbar.fileName} onState={onState} />
    );
  } else {
    body = (
      <p className="okg-console__fact" data-testid="explore-content-no-viewer">
        {noDocumentViewerSentence(viewer.kind as DocumentViewerKind, mediaType)}
      </p>
    );
  }
  return (
    <div
      className="okg-console__pane okg-console__pane--rendered"
      data-testid="explore-content-rendered"
      data-viewer={viewer.kind}
      data-viewer-state={state.kind}
    >
      <p className="okg-console__pane-head">
        Rendered
        {state.kind === "ready" && state.note ? <span className="okg-console__section-aside">{state.note}</span> : null}
        {state.kind === "pending" && inBrowser ? <span className="okg-console__section-aside">rendering…</span> : null}
      </p>
      {state.kind === "failed" ? (
        <p className="okg-console__fact okg-console__viewer-failed" data-testid="explore-content-failed">
          The viewer failed: {state.reason}. Open the original or download it from the toolbar.
        </p>
      ) : null}
      {body}
    </div>
  );
}

function TextPane({ text, isCode }: { text: string; isCode: boolean }): ReactElement {
  return (
    <div className="okg-console__pane okg-console__pane--text" data-testid="explore-content-text">
      <p className="okg-console__pane-head">
        Extracted text
        <span className="okg-console__section-aside">
          <CopyTextButton what="extracted text" text={text} />
        </span>
      </p>
      {/* Prose unless the record IS code. Still preformatted, so every line
          and paragraph break the source recorded survives; what changes is
          the face, the size and the measure. */}
      <pre className="okg-console__extracted" data-face={isCode ? "code" : "prose"}>{text}</pre>
    </div>
  );
}

/** The record's content, read end to end: the chunks the read listed, in the
 *  order it served them, at the console's prose measure.
 *
 *  It is PREFORMATTED, not rendered. A chunk's text is often Markdown or
 *  YAML, and a Markdown renderer did not fit under the console's bundle
 *  ceiling; preformatted wrapped text keeps every line and paragraph break
 *  the source recorded and invents no structure the read did not send.
 *
 *  Each block is its own block and they are NOT glued into one string on the
 *  page, because the offsets the read sends can overlap -- and where they do,
 *  the sentence above the text says so rather than trimming what it cannot
 *  know is duplicate. */
function ReadableText({ content }: { content: ContentModel }): ReactElement | null {
  const readable = content.readable;
  if (!readable) return null;
  return (
    <div className="okg-console__chunks" data-testid="explore-content-readable">
      <p className="okg-console__pane-head">
        Content
        <span className="okg-console__section-aside">
          <CopyTextButton what="content" text={readable.text} label="Copy as text" />
        </span>
      </p>
      <p className="okg-console__meta" data-testid="explore-content-order">{readable.orderSentence}</p>
      {readable.overlapSentence ? (
        <p className="okg-console__meta" data-testid="explore-content-overlap">{readable.overlapSentence}</p>
      ) : null}
      {readable.outline.length ? (
        <ol className="okg-console__outline" data-testid="explore-content-outline">
          {readable.outline.map((heading) => <li key={heading}>{heading}</li>)}
        </ol>
      ) : null}
      <div className="okg-console__prose">
        {readable.blocks.map((block) => (
          <section key={block.nodeId} className="okg-console__chunk">
            <div className="okg-console__chunk-head">
              {block.heading ? <span className="okg-console__chunk-heading">{block.heading}</span> : null}
              <span className="okg-console__dim">{block.placement}</span>
              {block.page !== null ? <span className="okg-console__dim">page {exact(block.page)}</span> : null}
              <a className="okg-console__chunk-link" href={content.open(block.nodeId)}>open</a>
            </div>
            <pre className="okg-console__chunk-text" data-face={content.isCode ? "code" : "prose"}>{block.text}</pre>
          </section>
        ))}
      </div>
    </div>
  );
}

type PaneChoice = "both" | "rendered" | "text";

function ContentTab({ model, failure }: { model: RecordPageModel; failure: unknown }): ReactElement {
  const content = model.content;
  const [choice, setChoice] = useState<PaneChoice>("both");
  const toolbar = content.toolbar;
  const rendered = Boolean(toolbar);
  const text = content.extractedText;
  const both = rendered && text !== null;
  const showRendered = rendered && (choice !== "text" || !both);
  const showText = text !== null && (choice !== "rendered" || !both);
  return (
    <div data-testid="explore-record-content">
      {content.partOf ? (
        <p className="okg-console__fact okg-console__partof" data-testid="explore-content-partof">
          {content.partOf.position ? `${content.partOf.position} of ` : "part of "}
          <a href={content.partOf.href}>{content.partOf.label}</a>
          {" "}<CopyableId text={content.partOf.id} what="record id" />
        </p>
      ) : null}
      {failure ? (
        <LoudFailure error={failure} route={ARTIFACT_ROUTE} what="This record's original file" />
      ) : null}
      {toolbar ? (
        <div className="okg-console__content-toolbar" data-testid="explore-content-toolbar">
          <span className="okg-console__content-fact"><code className="okg-console__id">{toolbar.format}</code></span>
          <span className="okg-console__content-fact" title={`${exact(toolbar.byteCount)} bytes`}>{toolbar.bytesLabel}</span>
          <span className="okg-console__content-fact" title={`SHA-256 ${toolbar.sha256}`}>
            <CopyableId text={toolbar.sha256} what="SHA-256">
              <code className="okg-console__id">sha256 {toolbar.sha256.slice(0, 12)}…</code>
            </CopyableId>
          </span>
          <span className="okg-console__content-actions">
            <a href={toolbar.url} target="_blank" rel="noopener noreferrer">Open original</a>
            <a href={toolbar.url} download={toolbar.fileName}>Download</a>
            {text !== null ? <CopyTextButton what="extracted text" label="Copy text" text={text} /> : null}
            {both ? (
              <Switcher<PaneChoice>
                className="okg-console__panes-toggle"
                label="Panes"
                value={choice}
                onChange={setChoice}
                options={[
                  { value: "both", label: "Both" },
                  { value: "rendered", label: "Rendered" },
                  { value: "text", label: "Text" },
                ]}
              />
            ) : null}
          </span>
        </div>
      ) : null}
      {content.absenceSentence ? (
        <p
          className={`okg-console__fact okg-console__absence ${content.artifact && !content.artifact.exists && content.artifact.reason === "not_retained" ? "okg-console__meta" : "okg-console__status-note"}`}
          data-testid="explore-content-absent"
        >
          {content.absenceSentence}
          {content.artifact && !content.artifact.exists && content.artifact.reason !== "not_retained" && text !== null
            ? " The extracted text follows."
            : ""}
        </p>
      ) : null}
      {content.capSentences.map((sentence) => (
        <p key={sentence} className="okg-console__meta" data-testid="explore-content-caps">{sentence}</p>
      ))}
      {showRendered || showText ? (
        <div className="okg-console__panes" data-panes={showRendered && showText ? "two" : "one"}>
          {showRendered ? <RenderedPane content={content} /> : null}
          {showText ? <TextPane text={text as string} isCode={content.isCode} /> : null}
        </div>
      ) : null}
      {!rendered && text === null && content.chunks.length === 0
        && content.capSentences.length === 0 && !content.absenceSentence ? (
        <p className="okg-console__fact">This record has no content to show.</p>
      ) : null}
      <ReadableText content={content} />
    </div>
  );
}

// --- Related --------------------------------------------------------------------

function RelatedTab({ model }: { model: RecordPageModel }): ReactElement {
  const related = model.related;
  return (
    <div data-testid="explore-record-related">
      {/* A chip strip, a three-way switch and "1–1 of 1 rows · 1 related: 0
          outgoing, 1 incoming" over ONE row said, in four controls and
          eleven numbers, what "Contained in HcalIsoTrackValidationSim."
          says. The controls are drawn when there is something to filter and
          no sentence has already named the whole set. */}
      {related.showControls ? (
        <div className="okg-console__tablebar okg-console__related-bar">
          <p className="okg-console__count-line okg-console__chipgroup" data-group="relationship">
            {related.chips.map((chip) => (
              <Chip
                key={chip.relationship}
                className="okg-console__chipfilter okg-console__typechip"
                checked={chip.selected}
                onToggle={() => { window.location.hash = chip.href; }}
              >
                <code className="okg-console__count-label okg-console__typechip-name">{chip.relationship}</code>
                <span className="okg-console__count-value">{exact(chip.count)}{chip.countIsFloor ? "+" : ""}</span>
              </Chip>
            ))}
          </p>
          <Switcher
            className="okg-console__ranges"
            label="Direction"
            value={related.direction}
            onChange={(next) => {
              const target = related.directions.find((option) => option.value === next);
              if (target) window.location.hash = target.href;
            }}
            options={related.directions.map((option) => ({ value: option.value, label: option.label }))}
          />
        </div>
      ) : null}
      {related.summary
        ? <p className="okg-console__fact okg-console__reltable-summary" data-testid="explore-related-summary">{related.summary}</p>
        : <p className="okg-console__tablecount" data-testid="explore-related-count">{related.countLine}</p>}
      {related.capSentences.map((sentence) => (
        <p key={sentence} className="okg-console__meta">{sentence}</p>
      ))}
      {related.emptySentence ? <p className="okg-console__fact">{related.emptySentence}</p> : (
        <table className="okg-console__ltable okg-console__reltable" data-testid="explore-related-table">
          <thead>
            <tr>
              <th scope="col" data-col="rel">Relationship</th>
              {related.showDirection ? <th scope="col" data-col="dir">Direction</th> : null}
              <th scope="col" data-col="label">Record</th>
              <th scope="col" data-col="type">Type</th>
            </tr>
          </thead>
          <tbody>
            {related.rows.map(({ record, relationship, direction, href, title }) => (
              <tr
                key={record.nodeId}
                className="okg-console__prow okg-console__copyrow"
                onClick={(event) => {
                  if ((event.target as HTMLElement).closest("a, button")) return;
                  window.location.hash = href;
                }}
              >
                {/* Every relationship this ONE neighbour is recorded under:
                    "contains · member_of" for a pair the graph records both
                    ways. The tab used to draw that neighbour twice instead,
                    once per direction, with the same title and the same id
                    in both rows. */}
                <td data-col="rel" title={relationship}><code className="okg-console__id">{relationship}</code></td>
                {related.showDirection ? <td data-col="dir" className="okg-console__dim">{direction}</td> : null}
                {/* The name and the id are one cell. A 260 px Record id
                    column was the widest thing on the tab and the narrow
                    rules dropped it outright, which took the id off the
                    page; here it copies from the name and reads in the
                    cell's title at every width.

                    The NAME is the record page's own title rule, so a chunk
                    reads "HcalIsoTrackValidationSim.txt · Part 2" and not
                    its own hash. The hash is still here, in the cell title
                    and on the copy control, where an id belongs. */}
                <td data-col="label" title={`${title} · ${record.nodeId}`}>
                  <CopyableId text={record.nodeId} what="record id" reveal="row">
                    <a className="okg-console__prow-name" href={href}>{title}</a>
                  </CopyableId>
                </td>
                <td data-col="type" title={record.type}><code className="okg-console__id">{record.type}</code></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {related.firstHref || related.prevHref || related.nextHref ? (
        <p className="okg-console__more">
          {related.firstHref ? <a href={related.firstHref}>First</a> : null}
          {related.firstHref && related.prevHref ? " · " : null}
          {related.prevHref ? <a href={related.prevHref}>Previous {RELATED_PAGE}</a> : null}
          {(related.firstHref || related.prevHref) && related.nextHref ? " · " : null}
          {related.nextHref ? <a href={related.nextHref}>Next {RELATED_PAGE}</a> : null}
        </p>
      ) : null}
    </div>
  );
}


// --- the page ---------------------------------------------------------------------

const TAB_LABELS: Record<RecordTab, (model: RecordPageModel) => string> = {
  fields: (model) => `Fields (${exact(model.fieldCount)})`,
  content: (model) => (model.content.chunks.length
    ? `Content (${exact(model.content.chunks.length)} ${model.content.chunks.length === 1 ? "chunk" : "chunks"})`
    : "Content"),
  related: (model) => `Related (${exact(model.relatedCount)}${model.relatedCountIsFloor ? "+" : ""})`,
  provenance: () => "Provenance",
  raw: () => "Raw",
};

export interface RecordReadFailures {
  artifact: unknown;
  raw: unknown;
}

const NO_FAILURES: RecordReadFailures = { artifact: null, raw: null };

export function ConsoleRecordPage({
  model, context, failures = NO_FAILURES,
}: { model: RecordPageModel; context: PageContext; failures?: RecordReadFailures }): ReactElement {
  const bodies: Record<RecordTab, () => ReactElement> = {
    fields: () => <FieldsTab model={model} />,
    content: () => <ContentTab model={model} failure={failures.artifact} />,
    related: () => <RelatedTab model={model} />,
    provenance: () => <ProvenanceTab model={model} />,
    raw: () => <RawTab model={model} failure={failures.raw} />,
  };
  // Every tab's body is rendered up front so Copy as text and the tests
  // see the whole page, except Content: its image fetches and draws the
  // artifact, and that should happen when the reader opens the tab, not on
  // every record. The panel that is NOT current is hidden, not unmounted.
  //
  // A strip of LINKS, not antd's Tabs. Each tab is a URL this page can be
  // opened at, so a link is what it is: it copies, it middle-clicks, and
  // it needs no JavaScript to follow. rc-tabs cost 17,502 gzip bytes in
  // the console bundle and bought an ink bar.
  return (
    <main className="okg-console" data-testid="console-explore-record">
      <p className="okg-console__back"><a href={model.backHref}>&larr; Explore</a></p>
      <header className="okg-console__header" data-testid="explore-record-header">
        <div className="okg-console__header-row">
          <h1 className="okg-console__title">{model.title}</h1>
          {model.openInSource ? (
            <a
              className="okg-console__open-source"
              data-testid="explore-open-in-source"
              href={model.openInSource}
              target="_blank"
              rel="noopener noreferrer"
              title={model.openInSource}
            >
              <ExportOutlined aria-hidden="true" /> Open source
            </a>
          ) : null}
          <a className="okg-console__open-source" data-testid="explore-open-in-graph" href={model.graphHref}>
            Open in graph
          </a>
          {/* "Copy RECORD as text", because that is what it copies: every
              tab, not the one on screen. Labelled "Copy as text" it sat
              inside a tab strip and read as an offer to copy that tab. */}
          <CopyTextButton
            className="okg-console__copy--page"
            what="record as text"
            label="Copy record as text"
            text={() => recordText(model, context)}
          />
        </div>
        <p className="okg-console__meta okg-console__record-meta">
          <code className="okg-console__id">{model.type}</code>
          {" · "}
          {model.source ?? "no source recorded"}
          {" · "}
          <CopyableId text={model.id} what="record id" />
          {" · "}
          read from <CopyableId text={model.version} what="version id">
            <code className="okg-console__id">{model.version.slice(-12)}</code>
          </CopyableId>, published {model.readLine.replace(/^.*published /, "")}
        </p>
        {model.newerVersionSentence ? (
          <p className="okg-console__status-note" data-testid="explore-newer-version">{model.newerVersionSentence}</p>
        ) : null}
        {/* A reader searching for release notes lands here; the page says
            the graph does not hold them rather than leaving them to look. */}
        {model.noTextSentence ? (
          <p className="okg-console__fact" data-testid="explore-no-text">{model.noTextSentence}</p>
        ) : null}
        {/* The 70-word masking paragraph that used to sit here -- the
            sentence, the list of keys and the whole rule -- is cut. It was on
            every record page of every deployment that shortens a path, it
            said the same thing on each, and it was three lines away from the
            value it was about. The value now says "shortened" beside itself,
            with the rule in its title, on the row where it matters. */}
      </header>
      {model.tabNote ? (
        <p className="okg-console__status-note" data-testid="explore-tab-note">{model.tabNote}</p>
      ) : null}
      <div data-testid="explore-record-tabs" className="okg-console__tabs">
        <nav className="okg-console__tabstrip" aria-label="Record sections">
          {model.tabs.map((tab) => (
            <a
              key={tab}
              className="okg-console__tab"
              href={model.tabHref(tab)}
              data-tab={tab}
              data-current={tab === model.tab ? "true" : "false"}
              aria-current={tab === model.tab ? "page" : undefined}
            >
              {TAB_LABELS[tab](model)}
            </a>
          ))}
        </nav>
        {model.tabs.map((tab) => (
          // Content is mounted only when it is the tab being read.
          tab === "content" && tab !== model.tab ? null : (
            <div key={tab} className="okg-console__tabpanel" hidden={tab !== model.tab}>
              {bodies[tab]()}
            </div>
          )
        ))}
      </div>
      {/* No CLI transcript. It went from the results page in round 2 on the
          grounds that it restated the query the operator had just typed; on
          the record page it restates the id that is already in the meta line
          with its own copy control, under a command that cannot even fetch
          the record -- `okg search` has no exact id lookup, which the block
          itself had to say. `ui/cuts.md` records it. */}
    </main>
  );
}
