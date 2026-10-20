// Sources: every producer of facts -- sources, enrichers, views -- one row
// each, and one page per producer.
//
// Reference: ui/references/sources.md. Fivetran's connection list is the
// scale design (counts above a filtered, sortable table that holds 12,000
// rows without the page getting longer); Airbyte's connection row is the row
// (state, last run age, next run as a fact, what it moved); Fivetran's broken
// connector is the failed row (the error's first line on the row, the full
// text one level down).
//
// Drawn inside the shell the operator approved for Home: one 960 px column,
// hairlines, Ant Design icons for status, tabular numbers, identifiers in
// monospace, the type scale Home and Updates use. It reuses Home's Section,
// Status and StateIcon and Updates' table and chip styles.
//
// What this page will not do:
//
//   1. It never prints a number no read recorded. A producer that has not run
//      has empty contribution cells, not zeros; a column no row fills is not
//      drawn.
//   2. It never pages by trusting a read. The producers reads carry every row
//      today; "Load 100 more" reveals the next hundred from what was read and
//      fails by name when the read declared more rows than it returned.
import { ConsoleLoading } from "@operator-ui/console/components/ConsoleLoading";
import { StateIcon, Status } from "@operator-ui/console/components/StatusWord";
import { VersionNotice } from "@operator-ui/console/components/VersionNote";
import { ConsoleProducerDetail } from "@operator-ui/console/views/ProducerDetail";
import { pageContextFor } from "@operator-ui/console/api/client";
import {
  type SourcesReadFailures, SOURCES_ATTEMPTS_ROUTE, SOURCES_HEALTH_ROUTE, SOURCES_VERSIONS_ROUTE,
  loadConsoleSources, loadProducerDetail, sourcesIncomplete,
} from "@operator-ui/console/api/sources";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";
import { clientKey, useRecoveringRead } from "@operator-ui/console/components/readRecovery";
import { CopyTextButton, CopyableId } from "@operator-ui/console/components/CopyControls";
import { LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { type PageContext, sourcesText } from "@operator-ui/console/model/copyText";
import { exact } from "@operator-ui/console/model/shared/format";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import { buildProducerDetail } from "@operator-ui/console/model/sources/detail";
import { type SourcesChip, type SourcesModel, buildSourcesModel, pageOf, shownSentence } from "@operator-ui/console/model/sources/list";
import { producerRecordsFrom } from "@operator-ui/console/model/sources/producers";
import { producerGlyph, stateParam } from "@operator-ui/console/model/shared/producerStates";
import type { ProducerDetailRead } from "@operator-ui/console/model/sources/reads";
import { SOURCES_PAGE, type Sort, type SortKey, type SourceRow, ignoredSortParam, nextSort, parseSourcesFilter, parseSourcesSort, sourcesFilterParams, sourcesSortParams } from "@operator-ui/console/model/sources/rows";
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import { Chip } from "@operator-ui/console/components/Chip";
import { Breakable, VersionIds } from "@operator-ui/console/components/VersionIds";
import { type ReactElement, useEffect, useMemo, useState } from "react";

/** One toggle in the count strip. The same control Updates uses, so a
 *  reader who learned it there knows it here. */
function FilterChip({ chip }: { chip: SourcesChip }) {
  return (
    <Chip
      className="okg-console__chipfilter"
      severity={chip.mark?.severity ?? "ok"}
      checked={chip.selected}
      onToggle={() => { window.location.hash = chip.href; }}
    >
      {chip.mark ? <StateIcon icon={chip.mark.icon} /> : null}
      <span className="okg-console__count-value">{exact(chip.value)}</span>
      <span className="okg-console__count-label">{chip.label}</span>
    </Chip>
  );
}

// A screen reader hears the word the screen shows: "sources" on a deployment
// of sources, "producers" only where enrichers or views share the table.
function StateBar({ segments, unit }: { segments: SourcesModel["segments"]; unit: string }) {
  if (!segments.length) return null;
  return (
    <span
      className="okg-console__statebar"
      data-testid="sources-statebar"
      role="img"
      aria-label={`${unit.charAt(0).toUpperCase()}${unit.slice(1)} by state: ${segments
        .map((segment) => `${segment.count} ${producerGlyph(segment.state).word.toLowerCase()}`)
        .join(", ")}.`}
    >
      {segments.map((segment) => (
        <span
          key={segment.state}
          className="okg-console__statebar-part"
          data-state={stateParam(segment.state)}
          style={{ width: `${segment.share}%` }}
          role="img"
          aria-label={`${segment.count} ${producerGlyph(segment.state).word.toLowerCase()}`}
        />
      ))}
    </span>
  );
}

const HEADERS: { key: SortKey | null; label: string; col: string }[] = [
  { key: "name", label: "Name", col: "name" },
  { key: "state", label: "State", col: "state" },
  { key: "lastRun", label: "Last run", col: "lastrun" },
  // No "Next due": no read records when a producer is next due. No "Last
  // error": okg.v_source_run_diagnostics has no message column, so a failed
  // run has an outcome and a phase and no text. Freshness takes a column of
  // its own, because the runtime's state is a fact worth printing and is not
  // the same fact as the last run's age.
  { key: null, label: "Freshness", col: "freshness" },
  { key: "records", label: "Records", col: "records" },
  { key: "relationships", label: "Relationships", col: "relationships" },
];

function SortHeader({
  header, sort, onSort,
}: {
  header: (typeof HEADERS)[number];
  sort: Sort;
  onSort: (key: SortKey) => void;
}) {
  const active = header.key !== null && sort.key === header.key;
  // A sortable header that is NOT the sorted one reads `none`, not nothing.
  // Omitting the attribute tells a screen reader the column does not sort at
  // all, which is a different fact from "it sorts, and not by this one now".
  const ariaSort = header.key === null
    ? undefined
    : active
      ? (sort.direction === "asc" ? "ascending" : "descending")
      : "none";
  return (
    <th scope="col" data-col={header.col} aria-sort={ariaSort}>
      {header.key ? (
        <button
          type="button"
          className="okg-console__sortbutton"
          data-active={active ? "true" : "false"}
          onClick={() => onSort(header.key as SortKey)}
        >
          {header.label}
          {active ? (
            <span className="okg-console__sortmark" aria-hidden="true">
              {sort.direction === "asc" ? "↑" : "↓"}
            </span>
          ) : null}
        </button>
      ) : header.label}
    </th>
  );
}

function ProducerRow({ row, columns }: { row: SourceRow; columns: SourcesModel["columns"] }) {
  return (
    <tr
      className="okg-console__prow okg-console__copyrow"
      data-severity={row.mark.severity}
      data-state={stateParam(row.state)}
      data-testid="sources-row"
      onClick={(event) => {
        // A click on the row is a click on the chevron, unless it landed on
        // the link itself, which the browser already handles.
        if ((event.target as HTMLElement).closest("a, button")) return;
        window.location.hash = row.href;
      }}
    >
      <td data-col="name" title={row.displayName === row.name ? undefined : row.name}>
        <CopyableId text={row.name} what="producer name" reveal="row">
          <a className="okg-console__id okg-console__prow-name" href={row.href}><Breakable text={row.displayName} /></a>
        </CopyableId>
        {row.kindWord ? <span className="okg-console__prow-kind">{row.kindWord}</span> : null}
        {row.configured ? null : <span className="okg-console__tag">not configured</span>}
      </td>
      <td data-col="state" title={row.configured ? `recorded as ${row.stateCode}` : undefined}>
        {row.configured ? <Status mark={row.mark} /> : null}
      </td>
      <td data-col="lastrun" title={row.lastRunExact || undefined}>{row.lastRun}</td>
      {columns.freshness ? (
        <td data-col="freshness">{row.freshness}</td>
      ) : null}
      {columns.contribution ? (
        <>
          <td data-col="records">{row.records === null ? "" : exact(row.records)}</td>
          <td data-col="relationships" title={row.relationshipsNote || undefined}>
            {row.relationships === null
              ? (row.relationshipsNote || "")
              : exact(row.relationships)}
          </td>
        </>
      ) : null}
      <td data-col="open">
        <a href={row.href} aria-label={`Open ${row.name}`}>›</a>
      </td>
    </tr>
  );
}

function ProducerTable({
  rows, columns, sort, onSort, unit,
}: {
  unit: string;
  rows: SourceRow[];
  columns: SourcesModel["columns"];
  sort: Sort;
  onSort: (key: SortKey) => void;
}) {
  const headers = HEADERS.filter((header) => {
    if (header.col === "freshness") return columns.freshness;
    if (header.col === "records" || header.col === "relationships") return columns.contribution;
    return true;
  });
  return (
    <table className="okg-console__ptable" data-testid="sources-table">
      <thead>
        <tr>
          {headers.map((header) => (
            <SortHeader key={header.col} header={header} sort={sort} onSort={onSort} />
          ))}
          <th scope="col" data-col="open" aria-label={`Open the ${unit.replace(/s$/, "")}`} />
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => <ProducerRow key={row.key} row={row} columns={columns} />)}
      </tbody>
    </table>
  );
}

/** The minor reads this page was drawn without, each named in its own
 *  place with what the page lost by it. */
function MinorReadFailures({
  failures, onReadAgain, retrying,
}: {
  failures: SourcesReadFailures;
  onReadAgain: (() => void) | null;
  retrying: boolean;
}): ReactElement | null {
  const parts: { route: string; error: unknown; what: string }[] = [];
  if (failures.health !== undefined) {
    parts.push({ route: SOURCES_HEALTH_ROUTE, error: failures.health, what: "This deployment's health" });
  }
  if (failures.attempts !== undefined) {
    parts.push({ route: SOURCES_ATTEMPTS_ROUTE, error: failures.attempts, what: "The update attempts by day" });
  }
  if (failures.versions !== undefined) {
    parts.push({ route: SOURCES_VERSIONS_ROUTE, error: failures.versions, what: "The published versions" });
  }
  if (!parts.length) return null;
  return (
    <div data-testid="sources-minor-failures">
      {parts.map((part) => (
        <ReadFailure
          key={part.route}
          route={part.route}
          error={part.error}
          what={part.what}
          onReadAgain={onReadAgain}
          retrying={retrying}
        />
      ))}
    </div>
  );
}

export function ConsoleSources({
  model, sort, onSort, nameQuery, onNameQuery, pages, onMore, moreError, context,
  askedVersion = null, ignoredSort = null, published = [], failures = {}, onReadAgain = null,
  retrying = false,
}: {
  /** The minor reads that failed; each is named above the table. */
  failures?: SourcesReadFailures;
  /** Read the page again, for the failures above. */
  onReadAgain?: (() => void) | null;
  retrying?: boolean;
  model: SourcesModel;
  sort: Sort;
  onSort: (key: SortKey) => void;
  nameQuery: string;
  onNameQuery: (next: string) => void;
  pages: number;
  onMore: () => void;
  moreError: unknown;
  /** What "Copy as text" states in its header. */
  context: PageContext;
  /** What `?version=` carries, or null. The version this page DID read is
   *  `model.readingVersion`. */
  askedVersion?: string | null;
  /** The generation ids the versions read returned, so the version sentence
   *  names a real one rather than withholding it for failing a shape test. */
  published?: readonly string[];
  /** The sentence from `ignoredSortParam`, when the URL asked this table to
   *  sort by something it does not sort by. */
  ignoredSort?: string | null;
}): ReactElement {
  const shown = pageOf(model.rows, pages);
  const partial = shownSentence(shown.length, model.rows.length, "producers");
  const unit = model.mixedKinds ? "producers" : "sources";
  return (
    <main className="okg-console" data-testid="console-sources-view">
      <header className="okg-console__header" data-testid="sources-header">
        <div className="okg-console__header-row">
          <h1 className="okg-console__title">Sources</h1>
          <span className="okg-console__header-note">
            {model.headerCount}
          </span>
          <CopyTextButton
            className="okg-console__copy--page"
            what="Sources as text"
            label="Copy as text"
            text={() => sourcesText(model, shown, context)}
          />
        </div>
      </header>

      {/* Once per page, above everything the version could be read into.
          These reads take no `version`: they answer for the generation the
          read API's session is pinned to, which is what the sentence says. */}
      <VersionNotice
        reading={model.readingVersion}
        asked={askedVersion}
        published={published}
      />

      <MinorReadFailures failures={failures} onReadAgain={onReadAgain} retrying={retrying} />

      {/* A `sort` or `dir` this table did not use. The substitution is
          deliberate -- a hand-edited URL must not break the page -- and
          saying nothing about it was not: the rows were ranked by something
          other than what the link asked for, with nothing on the page to say
          so. Above the table, beside the other parameter lines. */}
      {ignoredSort ? (
        <p className="okg-console__status-note" data-testid="sources-ignored-sort">
          {ignoredSort}
        </p>
      ) : null}

      {/* A read that came back short says so, above the table and above the
          empty state. The producers read composes three diagnostics reads;
          when one of them fails it records the failure and returns the rows
          it has. Without this line a table missing every enricher reads as a
          deployment that configures none, and a page whose two derived reads
          both failed reads as a deployment with no producers at all. */}
      {model.partialReads.length ? (
        <div data-testid="sources-partial-read">
          {model.partialReads.map((sentence) => (
            <p className="okg-console__fact okg-console__partialread" key={sentence}>
              {sentence}
            </p>
          ))}
        </div>
      ) : null}

      {model.total === 0 ? (
        <p className="okg-console__fact okg-console__none">
          No producer is configured for {model.deployment}.
        </p>
      ) : (
        <>
          <div className="okg-console__sources-bar">
            <StateBar segments={model.segments} unit={unit} />
          </div>
          <div className="okg-console__filters" data-testid="sources-filters">
            <p className="okg-console__count-line">
              {model.kindChips.length ? (
                <span className="okg-console__chipgroup" data-group="kind">
                  {model.kindChips.map((chip) => <FilterChip key={chip.key} chip={chip} />)}
                </span>
              ) : null}
              <span className="okg-console__chipgroup" data-group="state">
                {model.stateChips.map((chip) => <FilterChip key={chip.key} chip={chip} />)}
              </span>
              {model.filter.kind && !model.kindChips.length ? (
                <a className="okg-console__chipclear" href={model.clearHref}>
                  {model.filter.kind}s &times;
                </a>
              ) : null}
            </p>
          </div>

          {model.sharedCause ? (
            <p className="okg-console__tablecount okg-console__sharedcause" data-testid="sources-shared-cause">
              {model.sharedCause}
            </p>
          ) : null}

          {model.nameFilter || partial ? (
            <div className="okg-console__tablebar">
              {model.nameFilter ? (
                <input
                  className="okg-console__namefilter"
                  type="search"
                  placeholder={`Filter ${unit} by name`}
                  aria-label={`Filter ${unit} by name`}
                  value={nameQuery}
                  onChange={(event) => onNameQuery(event.target.value)}
                />
              ) : <span />}
              {partial ? <span className="okg-console__tablecount">{partial}</span> : null}
            </div>
          ) : null}

          {model.noMatchSentence
            ? <p className="okg-console__fact">{model.noMatchSentence}</p>
            : <ProducerTable rows={shown} columns={model.columns} sort={sort} onSort={onSort} unit={unit} />}

          {moreError ? <LoudFailure error={moreError} /> : null}
          {shown.length < model.rows.length ? (
            <p className="okg-console__more">
              <button type="button" className="okg-console__link-button" onClick={onMore}>
                Load {SOURCES_PAGE} more
              </button>
            </p>
          ) : null}
        </>
      )}
      {/* What Home's health verdict counts a source by, while that basis
          differs from this table's state. Null once the health read says the
          two share one basis, and then nothing is printed. */}
      {model.total === 0 || !model.stateBasisNote ? null : (
        <p className="okg-console__footer" data-testid="sources-state-basis">
          {model.stateBasisNote}
        </p>
      )}
      {/* This page's readiness read is not the release gate. It runs the
          checks whose results reach this table and declines the rest, so a
          reader who took the table for a full readiness verdict would be
          reading a bounded check as a complete one. */}
      {model.readinessScopeNote ? (
        <p className="okg-console__footer" data-testid="sources-readiness-scope">
          {model.readinessScopeNote}
        </p>
      ) : null}
      {/* Separate spans, so the clause explaining the contribution COLUMNS
          can go with them at 390 instead of outliving them (review N4).
          The as-of fact is written TWICE, joined and standing alone, because
          hiding the clause left the sentence beginning mid-word: "as of
          2026-09-22 05:36 UTC." was what a phone actually showed. Exactly
          one of the two is displayed at any width, by the rules in
          `sources.css` and `narrow.css`. */}
      <p className="okg-console__footer" data-testid="sources-footer">
        {model.footerContribution ? (
          <>
            <span data-part="contribution"><VersionIds text={model.footerContribution} /></span>
            <span data-part="sep"> · </span>
            <span data-part="asof-joined">{model.footerAsOfJoined}</span>
          </>
        ) : null}
        <span data-part="asof"><VersionIds text={model.footerAsOf} /></span>
      </p>
    </main>
  );
}


/** The route: load the lists, then render the table or one producer. */
/**
 * A producer name the read does not list.
 *
 * Nothing failed: `console/producers` answered, and no row it returned
 * carries this name. So this is a page with a line in it -- the name asked
 * for, the deployment, how many producers the read did list, and the way back
 * -- and not a failure block. A failure block here printed the route as `this
 * view's read`, drew outside the page column, and ended with "The read did
 * not reach the server", which the answered read contradicts.
 */
export function ConsoleProducerNotFound({
  producerName, deployment, listed, backHref,
}: {
  producerName: string;
  deployment: string;
  /** How many producers the read did return. */
  listed: number;
  backHref: string;
}): ReactElement {
  return (
    <main className="okg-console" data-testid="console-sources-not-found">
      <p className="okg-console__back"><a href={backHref}>&larr; Sources</a></p>
      <p className="okg-console__fact okg-console__fact--page">
        No producer named{" "}
        <code className="okg-console__id">{producerName}</code> is recorded in{" "}
        {deployment}; {exact(listed)}{" "}
        {listed === 1 ? "producer is" : "producers are"} listed.
      </p>
    </main>
  );
}

export function ConsoleSourcesRoute({
  api, link, params, producerName, askedVersion = null, onReadVersion,
}: {
  api: ApiClient;
  link: LinkFn;
  /** The route's query parameters, minus the harness's own. */
  params: Record<string, string>;
  /** The name in `#sources/<name>`, or null for the list. */
  producerName: string | null;
  /** What `?version=` carries. None of these reads takes it, so the page
   *  says which version it IS showing instead. */
  askedVersion?: string | null;
  /** What the top bar prints. The bar makes no health read of its own, so the
   *  version the page read has to be reported up to it -- the same contract
   *  Home and the Updates list answer. */
  onReadVersion?: (fullVersion: string | null) => void;
}): ReactElement {
  // One page read, recovering: "Read again", the bar's schedule, and a click
  // on Sources in the bar all re-issue it. A minor read failing leaves the
  // page drawn from the rest and re-read on the schedule until it answers.
  const list = useRecoveringRead(
    clientKey(api),
    (signal) => loadConsoleSources(api, signal),
    { incomplete: sourcesIncomplete },
  );
  const data = list.value;
  const error = list.error;
  const [pages, setPages] = useState(1);
  const [moreError, setMoreError] = useState<unknown>(null);
  const urlFilter = useMemo(() => parseSourcesFilter(params), [params]);
  // Off the PRIMITIVES, not off `params`: the route builds a fresh object
  // every render, so a memo or an effect keyed on it would recompute forever.
  const sortParam = params.sort ?? "";
  const dirParam = params.dir ?? "";
  const urlSort = useMemo(
    () => parseSourcesSort({ sort: sortParam, dir: dirParam }),
    [sortParam, dirParam],
  );
  const [sort, setSort] = useState<Sort>(urlSort);
  const [nameQuery, setNameQuery] = useState(urlFilter.name);

  useEffect(() => { setNameQuery(urlFilter.name); }, [urlFilter.name]);
  // The URL is the sort's home. Back, forward and a pasted link all arrive
  // here as a hash change, and the table follows it.
  useEffect(() => { setSort(urlSort); }, [urlSort]);

  const records = useMemo(
    () => (data ? producerRecordsFrom(data.producers) : []),
    [data],
  );

  // What the bar prints has to be what this page READ. The producers read
  // answers for the generation the read API's session is pinned to, which the
  // health read is the record of; reporting it up is the only way the bar can
  // know. Before the reads land there is nothing read, and the bar says so
  // rather than showing the last page's version.
  const readVersion = data
    ? (data.health?.payload.generation_id ?? data.producers.payload.generation_id ?? null)
    : null;
  useEffect(() => { onReadVersion?.(readVersion); }, [onReadVersion, readVersion]);
  const record = producerName
    ? records.find((candidate) => candidate.name === producerName) ?? null
    : null;

  // A producer with no configuration has no detail read to ask for: its
  // page is built from the list row alone.
  const detailName = record && record.configured ? record.name : null;
  const detail = useRecoveringRead<ProducerDetailRead>(
    detailName === null ? null : `${clientKey(api)}|${detailName}`,
    (signal) => loadProducerDetail(api, detailName as string, signal),
  );
  const detailRead = detail.value;
  const detailError = detail.error;

  // Inside the page frame, like every other page of this console. Rendered
  // bare, a failure block starts at the window's left edge with no column and
  // no way back, which reads as a broken page rather than a named failure.
  if (error) {
    return (
      <main className="okg-console" data-testid="console-sources-read-failed">
        <LoudFailure
          error={error}
          route="/viz/api/console/producers"
          what="This deployment's producers"
          onReadAgain={list.readAgain}
          retrying={list.retrying}
        />
      </main>
    );
  }
  if (!data) return <ConsoleLoading what="this deployment" />;

  const filter = { ...urlFilter, name: nameQuery };
  const model = buildSourcesModel(data, filter, link, sort);
  const context = pageContextFor(
    model.deployment, model.readingVersion, data.versions, model.nowMs,
  );
  // The ids the versions read returned. A `?version=` among them is named
  // outright; anything else falls back to the id SHAPE, and an unrecognised
  // value is withheld rather than echoed into the page's own sentence.
  const published = data.versions?.payload.versions.map((row) => row.generation_id) ?? [];

  if (producerName) {
    if (!record) {
      // Nothing failed. The read answered, and this name is not among the
      // producers it returned -- so this is a plain named line inside the
      // page, not a failure block. It used to build an Error and hand it to
      // LoudFailure, which printed the route as `this view's read`, drew
      // itself outside the page column, and finished with "The read did not
      // reach the server", a next step the answered read contradicts.
      return (
        <ConsoleProducerNotFound
          producerName={producerName}
          deployment={model.deployment}
          listed={records.length}
          backHref={link("sources")}
        />
      );
    }
    if (detailError) {
      return (
        <main className="okg-console" data-testid="console-sources-detail-read-failed">
          <p className="okg-console__back"><a href={link("sources")}>&larr; Sources</a></p>
          <LoudFailure
            error={detailError}
            route="/viz/api/console/producers"
            what={`The read for ${record.name}`}
            onReadAgain={detail.readAgain}
            retrying={detail.retrying}
          />
        </main>
      );
    }
    if (record.configured && !detailRead) {
      return <ConsoleLoading what={record.name} />;
    }
    const producer = buildProducerDetail(record, detailRead, { nowMs: model.nowMs, link });
    const moreRuns = producer.runTotal !== null && producer.runTotal > producer.runs.length;
    return (
      <ConsoleProducerDetail
        detail={producer}
        backHref={link("sources")}
        onMoreRuns={moreRuns
          ? () => setMoreError(new Error(
            `The run history read returned ${exact(producer.runs.length)} of `
            + `${exact(producer.runTotal as number)} runs and defines no second page. `
            + "No rows were added.",
          ))
          : null}
        moreError={moreError}
        context={context}
        readingVersion={model.readingVersion}
        askedVersion={askedVersion}
        published={published}
      />
    );
  }

  return (
    <ConsoleSources
      model={model}
      sort={sort}
      onSort={(key) => {
        // Written to the URL, not held in this component: a reader who sorted
        // by records to find the empty producers can reload the page, or hand
        // someone the link, and get back what they were looking at. The
        // hashchange above is what actually moves the table.
        const next = nextSort(sort, key);
        window.location.hash = link("sources", {
          ...sourcesFilterParams(urlFilter),
          ...sourcesSortParams(next),
        });
      }}
      nameQuery={nameQuery}
      onNameQuery={(next) => { setNameQuery(next); setPages(1); }}
      pages={pages}
      onMore={() => {
        if (pages * SOURCES_PAGE >= model.rows.length) {
          setMoreError(new Error(
            `The producers reads returned ${exact(model.rows.length)} rows and define no `
            + "second page. No rows were added.",
          ));
          return;
        }
        setPages((was) => was + 1);
      }}
      moreError={moreError}
      context={context}
      askedVersion={askedVersion}
      published={published}
      ignoredSort={ignoredSortParam({ sort: sortParam, dir: dirParam })}
      failures={data.failures ?? {}}
      onReadAgain={list.readAgain}
      retrying={list.retrying}
    />
  );
}
