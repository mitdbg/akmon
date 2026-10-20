// Graph: what the deployment holds, as a map of record types, a Table and a
// Grid, and one record's neighbourhood.
//
// Ported from the prototype the operator approved (console-graph-views
// v2.html); PACT console-graph-views. Reads (GET only), each failing in its
// own place: the type map read (console/graph/types) is the page for the map,
// the Table and the Grid; the ontology read fills the Table's rows and the
// side panel's declared count; the source-links read is the Grid's second
// grid; the record read is the neighbourhood. Two console reads name what the
// graph reads hold: the producers read (what Sources draws) names each
// producer, and the versions read gives the version's publish minute.
//
// URL: see model/graph/params.ts. `graph` is the type map, `graph/record?id=`
// a record's neighbourhood, and `view=map|table|grid` says what is drawn. A
// page whose URL names no view opens on the Table under 700 px and the map
// above, decided once when the page opens and written into the URL.
//
// Every view is handed the same three things: `params` (the URL's
// parameters), `link` (the console's href builder, which carries the version)
// and `setParams` (change this page's parameters, replacing the history entry
// unless `push` is asked for).
//
// Loaded lazily by the console route, so d3 is paid only here.
import "@operator-ui/console/styles/graph.css";
import "@operator-ui/console/styles/graph-tables.css";
import { type ReactElement, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import {
  GRAPH_ONTOLOGY_ROUTE, GRAPH_RECORD_ROUTE, GRAPH_TYPES_ROUTE,
  readGraphTypes, readOntology, readProducerNames, readPublishedVersions, readRecordNeighbourhood, readSourceLinks,
} from "@operator-ui/console/api/graph";
import { PRODUCERS_ROUTE } from "@operator-ui/console/api/sources";
import { CopyTextButton } from "@operator-ui/console/components/CopyControls";
import { ReadingLine, clientKey, useRecoveringRead } from "@operator-ui/console/components/readRecovery";
import { graphGridText, graphRecordText, graphTableText, graphTypesText } from "@operator-ui/console/model/copyText/graph";
import { buildGraphModel, buildSourceLinks, producerNamer } from "@operator-ui/console/model/graph/derive";
import { isSettled } from "@operator-ui/console/model/graph/failures";
import {
  GRAPH_MODES, type GraphMode, type ParamCheck, type SetParams, askedMode, defaultMode, goTo, graphIgnored, graphIgnoredSentence,
  graphRecordHref, mapTypeHref, modeHref, parseCell, patchedHref, recordChecks,
} from "@operator-ui/console/model/graph/params";
import { producerCoverage } from "@operator-ui/console/model/graph/table";
import { mapChecks, readMapState } from "@operator-ui/console/model/graph/typeMapModel";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import { utcStamp } from "@operator-ui/console/model/shared/time";
import { GridView } from "@operator-ui/console/views/graph/GridView";
import { GraphFailure } from "@operator-ui/console/views/graph/GraphFailure";
import { RecordPage } from "@operator-ui/console/views/graph/RecordView";
import { TableView } from "@operator-ui/console/views/graph/TableView";
import { TypeMapView } from "@operator-ui/console/views/graph/TypeMapView";
import { type ApiClient, withAbortSignalClient } from "@operator-ui/shared/api/apiClient";

export { GRAPH_MODES, type GraphMode, type SetParams, graphRecordHref };

function useWindowWidth(): number {
  const [width, setWidth] = useState(() => (typeof window === "undefined" ? 1440 : window.innerWidth));
  useEffect(() => {
    const on = () => setWidth(window.innerWidth);
    window.addEventListener("resize", on);
    return () => window.removeEventListener("resize", on);
  }, []);
  return width;
}

/** The page fills the window below the bar; the bar's height moves with its
 *  failure line, so it is measured rather than assumed. */
function useFillHeight(ref: { current: HTMLElement | null }, width: number): void {
  // A layout effect, so the height is set before the drawings below measure
  // their stage (their effects run after every layout effect).
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return undefined;
    const apply = () => el.style.setProperty("--okg-graph-height", `${Math.max(420, window.innerHeight - el.getBoundingClientRect().top - window.scrollY)}px`);
    apply();
    window.addEventListener("resize", apply);
    return () => window.removeEventListener("resize", apply);
  }, [ref, width]);
}

/** The Map / Table / Grid switch. Links, not buttons: each is a URL, so it
 *  opens in a new tab and Back walks it like any other link. */
function ModeLinks({ mode, link, onRecord }: { mode: GraphMode; link: LinkFn; onRecord: boolean }): ReactElement {
  return (
    <nav className="okg-console__switcher" aria-label="Drawing">
      {GRAPH_MODES.map((m) => {
        // On a record page the map entry is the record itself: it is
        // current, and links back to the whole-graph map from nowhere else.
        const current = m.value === mode;
        return (
          <a
            key={m.value}
            className="okg-console__switch"
            href={onRecord && m.value === "map" ? undefined : modeHref(link, m.value)}
            aria-current={current ? "page" : undefined}
            data-checked={current ? "true" : "false"}
            data-testid={`graph-mode-${m.value}`}
          >
            {m.label}
          </a>
        );
      })}
    </nav>
  );
}

export function ConsoleGraphRoute({
  api, link, params, detail, readingVersion, nowMs = Date.now(), onShownVersion,
}: {
  api: ApiClient;
  link: LinkFn;
  params: Record<string, string>;
  /** The segment after `graph`: "record" for a neighbourhood, else null. */
  detail: string | null;
  /** The version on the URL, when it is a published id; null reads the latest. */
  readingVersion: string | null;
  nowMs?: number;
  /** Told the version the page shows (null when it shows none), so the bar's
   *  record picker searches that version. */
  onShownVersion?: (version: string | null) => void;
}): ReactElement {
  const width = useWindowWidth();
  const mainRef = useRef<HTMLElement>(null);
  useFillHeight(mainRef, width);
  const onRecordPage = detail === "record";
  const recordId = onRecordPage ? (params.id ?? "") : null;
  // The default is decided once, when the page opens, and written into the
  // URL: it used to flip between the map and the Table as the window was
  // resized, with nothing on the URL saying which was meant.
  const [opened] = useState(() => defaultMode(onRecordPage, width));
  const asked = askedMode(params);
  // Table and Grid are whole-graph views: a record page shows its record.
  const mode: GraphMode = onRecordPage ? "map" : (asked ?? opened);
  const route = onRecordPage ? "graph/record" : "graph";
  useEffect(() => {
    if (onRecordPage || params.view) return;
    goTo(patchedHref(link, route, params, { view: opened }), false);
  }, [onRecordPage, params, link, route, opened]);

  const setParams: SetParams = useCallback(
    (patch, opts = {}) => goTo(patchedHref(link, route, params, patch), Boolean(opts.push)),
    [link, route, params],
  );

  const onRecord = onRecordPage;
  const v = readingVersion;
  const ck = clientKey(api);
  const settled = { settled: isSettled };
  const types = useRecoveringRead(
    onRecord ? null : `${ck}|types|${v ?? ""}`,
    (signal) => readGraphTypes(withAbortSignalClient(api, signal), v),
    settled,
  );
  const ontology = useRecoveringRead(
    onRecord || mode === "grid" ? null : `${ck}|ontology|${v ?? ""}`,
    (signal) => readOntology(withAbortSignalClient(api, signal), v),
    settled,
  );
  const links = useRecoveringRead(
    !onRecord && mode === "grid" ? `${ck}|links|${v ?? ""}` : null,
    (signal) => readSourceLinks(withAbortSignalClient(api, signal), v),
    settled,
  );
  const record = useRecoveringRead(
    onRecord && recordId ? `${ck}|record|${v ?? ""}|${recordId}` : null,
    (signal) => readRecordNeighbourhood(withAbortSignalClient(api, signal), recordId as string, v),
    settled,
  );
  // The console's own reads, for names and the publish minute. Neither
  // takes a version: producer names and publish times do not change with it.
  const producers = useRecoveringRead(`${ck}|producers`, (signal) => readProducerNames(withAbortSignalClient(api, signal)), settled);
  const versions = useRecoveringRead(`${ck}|versions`, (signal) => readPublishedVersions(withAbortSignalClient(api, signal)), settled);

  const names = useMemo(() => producerNamer(producers.value, link), [producers.value, link]);
  const model = useMemo(() => (types.value ? buildGraphModel(types.value, names) : null), [types.value, names]);
  const coverage = useMemo(() => (model && producers.value ? producerCoverage(model, producers.value) : null), [model, producers.value]);
  const linkModel = useMemo(() => (links.value && model ? buildSourceLinks(links.value, model) : null), [links.value, model]);
  const openRecordHref = useCallback((id: string) => graphRecordHref(link, id), [link]);
  const onOpen = useCallback((id: string) => goTo(graphRecordHref(link, id), true), [link]);

  // Only the record this URL names: a record read left over from the last
  // id must not name the page or fill its copy text.
  const recordRead = record.value && recordId && record.value.record.id === recordId ? record.value : null;
  const deployment = recordRead?.deployment ?? model?.deployment ?? "";
  const shownVersion = recordRead?.generationId ?? model?.generationId ?? null;
  const publishedAt = shownVersion
    ? versions.value?.find((row) => row.generationId === shownVersion)?.publishedAt ?? null
    : null;
  const context = { deployment, version: shownVersion, publishedAt, nowMs };
  useEffect(() => { onShownVersion?.(shownVersion); }, [onShownVersion, shownVersion]);
  useEffect(() => () => { onShownVersion?.(null); }, [onShownVersion]);
  const recordLabel = recordRead ? (recordRead.record.label || recordRead.record.id) : recordId;

  // What the URL asked for that this page cannot honour, in one line.
  const checks: Record<string, ParamCheck> = {};
  if (model && ontology.value && mode === "table") {
    const known = new Set([...model.types, ...ontology.value.types.map((t) => t.type)]);
    checks.open = (value) => (known.has(value) ? null : "no record type by that name is declared or holds records");
    checks.empty = (value) => (value === "1" ? null : "it takes 1");
  }
  if (model && mode === "grid") {
    checks.cell = (value) => {
      const cell = parseCell(value);
      if (!cell) return "it names no cell of the grids";
      const inGrid = cell.grid === "tt"
        ? model.types.includes(cell.row) && model.types.includes(cell.col)
        : linkModel === null || (linkModel.sources.includes(cell.row) && linkModel.sources.includes(cell.col));
      return inGrid ? null : "that row or column is not in this grid";
    };
  }
  if (model && mode === "map" && !onRecord) Object.assign(checks, mapChecks(model));
  if (recordRead) Object.assign(checks, recordChecks(recordRead));
  const ignoredLine = graphIgnoredSentence(graphIgnored(params, onRecord ? "record" : mode, checks, detail));

  const mapState = model && mode === "map" && !onRecord ? readMapState(params, model) : null;
  const selectedType = mapState?.type ?? null;
  const crumb = onRecord
    ? (
      <>
        <a href={link("graph", { view: "map" })}>Record types</a>
        <span className="sepr" aria-hidden="true">›</span>
        {recordRead ? (
          <>
            {/* The record's type, selected on the map. */}
            <a href={mapTypeHref(link, recordRead.record.type)}>{recordRead.record.type}</a>
            <span className="sepr" aria-hidden="true">›</span>
          </>
        ) : null}
        <b
          aria-current="location" className={recordLabel && /\s/.test(recordLabel) ? "prose" : undefined}
          title={recordLabel || undefined}
        >
          {recordLabel || "No record named"}
        </b>
      </>
    )
    : mode !== "map"
      ? null
      : selectedType
        ? (
          <>
            <a href={link("graph", { view: "map" })}>Record types</a>
            <span className="sepr" aria-hidden="true">›</span>
            <b aria-current="location">{selectedType}</b>
          </>
        )
        : <span aria-current="location">Record types</span>;

  const copyText = onRecord
    ? (recordRead ? () => graphRecordText(recordRead, (p) => names(p).name, context, params) : null)
    : !model
      ? null
      : mode === "table"
        ? () => graphTableText(model, context, {
          ontology: ontology.value, coverage, open: params.open ?? null, showEmpty: params.empty === "1",
        })
        : mode === "grid"
          ? () => graphGridText(model, context, { links: linkModel, cell: parseCell(params.cell), linksFailed: links.error !== null })
          : () => graphTypesText(model, context, mapState);

  const single = (child: ReactElement) => <div className="work single"><div className="tablewrap">{child}</div></div>;
  let body: ReactElement;
  if (onRecord) {
    body = (
      <RecordPage
        recordId={recordId ?? ""} record={record} read={recordRead} askedVersion={v}
        params={params} link={link} setParams={setParams} onOpen={onOpen} producers={names} width={width}
      />
    );
  } else if (types.error !== null) {
    body = single(
      <GraphFailure
        error={types.error} route={GRAPH_TYPES_ROUTE} what="The record types and their relationships" readAgain={types.readAgain}
        retrying={types.retrying} askedVersion={v}
      />,
    );
  } else if (!model) {
    body = single(<ReadingLine what="the record types" />);
  } else if (model.types.length === 0) {
    body = single(<p className="okg-console__fact" data-testid="graph-no-records">This version holds no records.</p>);
  } else if (mode === "table") {
    body = (
      <TableView
        model={model} params={params} link={link} setParams={setParams}
        ontology={ontology.value}
        ontologyFailure={ontology.error === null ? null : (
          <GraphFailure
            error={ontology.error} route={GRAPH_ONTOLOGY_ROUTE} what="The declared record types" readAgain={ontology.readAgain}
            retrying={ontology.retrying} askedVersion={v}
          />
        )}
        coverage={coverage}
        producersFailure={producers.error === null ? null : (
          <GraphFailure
            error={producers.error} route={PRODUCERS_ROUTE} what="The producers' names" readAgain={producers.readAgain}
            retrying={producers.retrying} askedVersion={null}
          />
        )}
      />
    );
  } else if (mode === "grid") {
    body = (
      <GridView
        model={model} params={params} link={link} setParams={setParams}
        links={linkModel} linksRead={links.value}
        linksFailure={links.error === null ? null : { error: links.error, readAgain: links.readAgain, retrying: links.retrying }}
      />
    );
  } else {
    body = (
      <TypeMapView
        model={model} ontology={ontology.value} ontologyFailure={ontology.error} params={params} link={link}
        setParams={setParams} producersPending={producers.value === null && producers.error === null}
        producersFailure={producers.error} coverage={coverage}
      />
    );
  }
  return (
    <main className="okg-console okg-graph" ref={mainRef} data-testid="console-graph" data-mode={onRecord ? "record" : mode}>
      <div className="head">
        <h1>Graph</h1>
        <ModeLinks mode={mode} link={link} onRecord={onRecord} />
        {crumb ? <nav className="crumb" aria-label="Where you are">{crumb}</nav> : null}
        {shownVersion ? (
          <p className="graph-version" data-testid="graph-version">
            Version <span className="okg-console__version-id">{shortVersion(shownVersion)}</span>
            {publishedAt ? <>, published {utcStamp(publishedAt)}</> : null}
          </p>
        ) : null}
        {copyText ? (
          <CopyTextButton className="okg-console__copy--page" what="Graph as text" label="Copy as text" text={copyText} />
        ) : null}
      </div>
      {ignoredLine ? (
        <p className="okg-console__fact graph-ignored" data-testid="graph-ignored-params">{ignoredLine}</p>
      ) : null}
      {body}
    </main>
  );
}

export default ConsoleGraphRoute;
