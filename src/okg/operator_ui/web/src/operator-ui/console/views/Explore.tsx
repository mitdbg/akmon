// Explore: find a record and inspect it, in the console's shell.
//
// Reference: ui/references/explore.md. Base: #1931's Search view, whose reads,
// parsers and policy handling this reuses (now `console/api/searchApi.ts` and
// `console/api/searchData.ts`); its shell, layout and styling are superseded
// by the console's -- the top bar's search box IS this page's search input, one 960
// px column, hairlines, the type scale Home, Updates and Sources use.
//
// Three page states: the landing (type chips, then one type's records as a
// table), the results (id matches first, then text matches with the match
// marked), and the record page (a fixed header, then Fields, Content, Related,
// Provenance and Raw tabs whose labels carry counts). A deployment with no published
// version gets one sentence. Every read failure renders by name (rule 13).
import { ConsoleLoading } from "@operator-ui/console/components/ConsoleLoading";
import { ConsoleRecordPage, type RecordReadFailures } from "@operator-ui/console/views/Record";
import { ChipStrip, Listing, Results } from "@operator-ui/console/views/SearchResults";
import { SearchOutlined } from "@operator-ui/console/components/icons";
import { readArtifact, readRaw } from "@operator-ui/console/api/artifacts";
import { PRODUCERS_ROUTE } from "@operator-ui/console/api/sources";
import type { RawProducersRead } from "@operator-ui/console/model/sources/reads";
import { producerRecordsFrom } from "@operator-ui/console/model/sources/producers";
import type { ProducerName } from "@operator-ui/console/model/explore/record";
import { DETAIL_ROUTE, SEARCH_ROUTE, readRecordDetail, searchRecords } from "@operator-ui/console/api/records";
import { CopyTextButton } from "@operator-ui/console/components/CopyControls";
import { ConsolePageFailure, LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { VersionIds } from "@operator-ui/console/components/VersionIds";
import { type PageContext, exploreText } from "@operator-ui/console/model/copyText";
import { type ChipStrip as ChipStripModel, type ListingModel, SEARCHABLE_LINE, buildChips, buildListing, contentsLine, nothingToSearchSentence } from "@operator-ui/console/model/explore/landing";
import { type ExploreParams, exploreQuery, ignoredSentence, overlongSentence, parseExploreParams, versionToRead } from "@operator-ui/console/model/explore/params";
import { QUERY_SYNTAX, effectiveFilters, searchRequest } from "@operator-ui/console/model/explore/query";
import type { ArtifactRead, RawRead } from "@operator-ui/console/model/explore/raw";
import { type ExploreDetail, buildRecordPage } from "@operator-ui/console/model/explore/record";
import { type ExploreSearchPage, type ResultsModel, buildResults, moveFocus, resultRowId } from "@operator-ui/console/model/explore/search";
import { exact } from "@operator-ui/console/model/shared/format";
import { type LinkFn, searchHash } from "@operator-ui/console/model/shared/links";
import { RECORD_LIST_ROUTE, RECORD_TYPES_ROUTE, SEARCH_PAGE_SIZE, VERSIONS_ROUTE, listRecords, loadRecordTypes, loadVersions } from "@operator-ui/console/api/searchApi";
import type { ListPage, Pinned, RecordTypeCount, VersionList } from "@operator-ui/console/api/searchData";
import { type ApiClient, withAbortSignalClient } from "@operator-ui/shared/api/apiClient";
import { ReadingLine, clientKey, useRecoveringRead } from "@operator-ui/console/components/readRecovery";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import { Fragment, type ReactElement, type ReactNode, useEffect, useMemo, useRef, useState } from "react";

export const SEARCH_INPUT_SELECTOR = '[data-testid="console-search"] input';

// --- the landing and results page -----------------------------------------------

/** The query syntax, in a small panel beside the box. `?help=1` opens it
 *  from the URL so a capture can show it; the button toggles it in place. */
function QueryHelp({ open, onToggle }: { open: boolean; onToggle: () => void }): ReactElement {
  return (
    <span className="okg-console__help">
      <button
        type="button"
        className="okg-console__help-button"
        aria-label="Query syntax"
        aria-expanded={open}
        onClick={onToggle}
      >
        ?
      </button>
      {open ? (
        <div className="okg-console__help-panel" data-testid="explore-query-help" role="dialog" aria-label="Query syntax">
          <p className="okg-console__help-title">Query syntax</p>
          <dl className="okg-console__help-list">
            {QUERY_SYNTAX.map((entry) => (
              <Fragment key={entry.form}>
                <dt><code className="okg-console__id">{entry.form}</code></dt>
                <dd>{entry.means}</dd>
              </Fragment>
            ))}
          </dl>
          <p className="okg-console__help-note">Words match anywhere in a record's text; the search is case-insensitive.</p>
        </div>
      ) : null}
    </span>
  );
}

/** The search box as the page: 640 by 44, centred, the icon, the `/` hint.
 *  Controlled by the shell's draft when one is passed (it mirrors the top
 *  bar's box); otherwise it keeps its own text. Either way it is a real
 *  `<form role="search">`, so the browser's own Enter-in-a-single-field
 *  submission and a screen reader's submit both reach `onSubmit` -- this box
 *  used to act on Enter ONLY when the shell did NOT own the draft, and once
 *  the shell owned one draft for the whole console that was never. */
function SearchBox({
  search, link, help, onHelp, autoFocus,
}: {
  search: { value: string; onChange: (value: string) => void } | null;
  link: LinkFn;
  help: boolean;
  onHelp: () => void;
  autoFocus: boolean;
}): ReactElement {
  const [own, setOwn] = useState("");
  const value = search ? search.value : own;
  const setValue = search ? search.onChange : setOwn;
  const inputRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (autoFocus) inputRef.current?.focus();
  }, [autoFocus]);
  const submit = () => { window.location.hash = searchHash(value, link); };
  return (
    <form
      className="okg-console__searchbox"
      data-testid="explore-searchbox"
      role="search"
      onSubmit={(event) => { event.preventDefault(); submit(); }}
    >
      <label className="okg-console__searchbox-field">
        <SearchOutlined className="okg-console__searchbox-icon" aria-hidden="true" />
        <input
          ref={inputRef}
          className="okg-console__searchbox-input"
          type="search"
          value={value}
          placeholder="Search records by text or id"
          aria-label="Search records by text or id"
          data-testid="explore-search-input"
          onChange={(event) => setValue(event.target.value)}
          onKeyDown={(event) => {
            // The form submits on Enter by itself; this is the same thing for
            // a build where implicit submission is not available (and for
            // jsdom, which does not implement it). The page's own document
            // handler takes Enter BEFORE this whenever a result row is
            // focused, and only then.
            if (event.key === "Enter") { event.preventDefault(); submit(); }
            if (event.key === "Escape") setValue("");
          }}
        />
        <kbd className="okg-console__searchbox-kbd">/</kbd>
      </label>
      <QueryHelp open={help} onToggle={onHelp} />
    </form>
  );
}

export interface ExplorePageProps {
  deployment: string;
  version: string | null;
  types: RecordTypeCount[];
  params: ExploreParams;
  chips: ChipStripModel | null;
  /** The chips a page that matched NOTHING offers: the version's types, with
   *  the query dropped from their links. Null on any other page. */
  emptyChips?: ChipStripModel | null;
  onMoreChips: (() => void) | null;
  results: ResultsModel | null;
  listing: ListingModel | null;
  /** Rendered in place of the results or listing while its read is pending,
   *  or the read's failure. */
  pending: ReactNode;
  focus: number | null;
  context: PageContext;
  link: LinkFn;
  /** The harness's search draft, mirrored by the top bar's box; null when
   *  the landing keeps its own text. */
  search: { value: string; onChange: (value: string) => void } | null;
  help: boolean;
  onHelp: () => void;
}

/** "46,498 records in 10 types · version 31caba7be14a". */
export function landingLine(types: RecordTypeCount[], version: string): string {
  const records = types.reduce((sum, row) => sum + row.count, 0);
  return `${exact(records)} ${records === 1 ? "record" : "records"} in `
    + `${exact(types.length)} ${types.length === 1 ? "type" : "types"} · version ${version.slice(-12)}`;
}

export function ConsoleExplore(props: ExplorePageProps): ReactElement {
  const { deployment, version, types, params, chips, results, listing, context } = props;
  const landing = version !== null && !params.q && !listing && !props.pending;
  const ignoredLine = ignoredSentence(params.ignored, results !== null);
  return (
    <main className="okg-console" data-testid="console-explore" data-page={landing ? "landing" : (results ? "results" : "listing")}>
      {landing ? (
        <header className="okg-console__header okg-console__header--search" data-testid="explore-header">
          <div className="okg-console__header-row">
            <h1 className="okg-console__title">Explore</h1>
            <CopyTextButton
              className="okg-console__copy--page"
              what="Explore as text"
              label="Copy as text"
              text={() => exploreText(props)}
            />
          </div>
          <SearchBox search={props.search} link={props.link} help={params.help} onHelp={props.onHelp} autoFocus />
          {params.overlong ? (
            <p className="okg-console__status-note" data-testid="explore-overlong">
              {overlongSentence(params.overlong)}
            </p>
          ) : null}
          <p className="okg-console__meta okg-console__landing-line" data-testid="explore-searchable">
            <VersionIds text={landingLine(types, version as string)} />
            <br />
            {SEARCHABLE_LINE}
          </p>
        </header>
      ) : (
        <header className="okg-console__header" data-testid="explore-header">
          <div className="okg-console__header-row">
            <h1 className="okg-console__title">Explore</h1>
            {version ? (
              <span className="okg-console__header-note">{contentsLine(types, version)}</span>
            ) : null}
            {version && params.q ? <QueryHelp open={params.help} onToggle={props.onHelp} /> : null}
            <CopyTextButton
              className="okg-console__copy--page"
              what="Explore as text"
              label="Copy as text"
              text={() => exploreText(props)}
            />
          </div>
          {params.overlong ? (
            <p className="okg-console__status-note" data-testid="explore-overlong">
              {overlongSentence(params.overlong)}
            </p>
          ) : null}
          {version && !params.q ? (
            <p className="okg-console__meta" data-testid="explore-searchable">{SEARCHABLE_LINE}</p>
          ) : null}
        </header>
      )}

      {!version ? (
        <p className="okg-console__fact okg-console__none" data-testid="explore-empty">
          {nothingToSearchSentence(deployment)}
        </p>
      ) : (
        <>
          {/* An old bookmark or chat link carrying sort= or since= is answered
              by relevance order over every date. Saying so is the whole point:
              the read refuses both parameters, so honouring the link silently
              would hand back a different ranking than the one asked for. */}
          {ignoredLine ? (
            <p className="okg-console__fact okg-console__ignored" data-testid="explore-ignored-params">
              {ignoredLine}
            </p>
          ) : null}
          {chips && !results ? <ChipStrip strip={chips} onMore={props.onMoreChips} /> : null}
          {props.pending}
          {/* The zero-hit page offers the types: the way out of a search that
              matched nothing is to see what there IS, and the chips are the
              same control the landing uses. */}
          {results ? (
            <Results
              model={results}
              query={params.q}
              focus={props.focus}
              chips={results.noMatchSentence && !results.pastEnd ? (props.emptyChips ?? null) : null}
              onMoreChips={props.onMoreChips}
            />
          ) : null}
          {listing ? <Listing model={listing} /> : null}
          {/* The CLI transcript is cut from the results page. It was under
              every search, it restates the query the operator just typed, and
              a console whose whole job is to answer the question does not
              need to print the command that would answer it elsewhere. It
              stays on the record page, where the id it carries is not on the
              screen anywhere else. */}
        </>
      )}
      {version ? (
        <p className="okg-console__footer" data-testid="explore-footer">
          Counts are totals in version{" "}
          <span className="okg-console__version-id">{version.slice(-12)}</span> before access rules
          {" · "}
          as of {new Date(context.nowMs).toISOString().slice(0, 16).replace("T", " ")} UTC
        </p>
      ) : null}
    </main>
  );
}


// --- the route ------------------------------------------------------------------

interface RecordReads {
  artifact: ArtifactRead | null;
  raw: RawRead | null;
  /** Ownership id to producer name, for the source link. Null when the
   *  producers read failed; the source is then printed and not linked. */
  producers: ProducerName[] | null;
  failures: RecordReadFailures;
}

interface Base {
  versions: VersionList;
  version: string | null;
  types: Pinned<RecordTypeCount[]> | null;
  /** A `?version=` this deployment did not publish, when the versions read
   *  listed every published version and it was not among them. */
  unpublished: string | null;
}

/** A failure of one of the two reads that frame the page, carrying which. */
class BaseReadFailure {
  constructor(readonly route: string, readonly reason: unknown) {}
}

interface RecordAnswer { detail: Pinned<ExploreDetail>; reads: RecordReads }

/** The route: the versions and types, then whichever of search, listing or
 *  record the URL asks for. Nothing is caught into a default. */
export function ConsoleExploreRoute({
  api, link, params: rawParams, readingVersion, nowMs, search = null,
}: {
  api: ApiClient;
  link: LinkFn;
  params: Record<string, string>;
  /** The version the console reads when the URL names none. */
  readingVersion: string | null;
  nowMs: number;
  /** The shell's search draft, when the shell owns the box (the harness
   *  does): the landing's big box and the top bar's box show the same text. */
  search?: { value: string; onChange: (value: string) => void } | null;
}): ReactElement {
  const params = useMemo(() => parseExploreParams(rawParams), [rawParams]);
  const [help, setHelp] = useState<boolean | null>(null);
  const [focus, setFocus] = useState<number | null>(null);
  const [allChips, setAllChips] = useState(false);

  const wanted = versionToRead(params, readingVersion, null);
  const client = clientKey(api);

  // Every read here recovers: "Read again" on its failure, the bar's bounded
  // schedule, and a click on Explore in the bar all re-issue it. Each is
  // bound to its own abort signal, so a read superseded by a new query is
  // cancelled rather than left running on the server.
  const baseRead = useRecoveringRead<Base>(`${client}|${wanted ?? ""}`, async (signal) => {
    const bound = withAbortSignalClient(api, signal);
    let versions: VersionList;
    try {
      versions = await loadVersions(bound);
    } catch (reason) {
      throw new BaseReadFailure(VERSIONS_ROUTE, reason);
    }
    const version = versionToRead(params, readingVersion, versions.latest);
    if (!version) return { versions, version: null, types: null, unpublished: null };
    // A version the deployment never published is not a failure of this
    // deployment: say so, and link to the latest. Only when the list is
    // whole -- a truncated list cannot say what it does not hold.
    if (!versions.truncated && !versions.versions.some((row) => row.generationId === version)) {
      return { versions, version: null, types: null, unpublished: version };
    }
    try {
      return { versions, version, types: await loadRecordTypes(bound, version), unpublished: null };
    } catch (reason) {
      throw new BaseReadFailure(RECORD_TYPES_ROUTE, reason);
    }
  });
  const base = baseRead.value;
  const baseError = baseRead.error instanceof BaseReadFailure
    ? baseRead.error
    : baseRead.error === null ? null : new BaseReadFailure(VERSIONS_ROUTE, baseRead.error);

  const version = base?.version ?? null;
  const request = version && params.q ? searchRequest(version, effectiveFilters(params), params.offset, SEARCH_PAGE_SIZE) : null;
  const searchKey = request ? JSON.stringify(request) : null;
  const searchRead = useRecoveringRead<{ page: Pinned<ExploreSearchPage>; elapsedMs: number }>(
    searchKey === null ? null : `${client}|${searchKey}`,
    (signal) => {
      const started = performance.now();
      return searchRecords(withAbortSignalClient(api, signal), request as NonNullable<typeof request>)
        .then((page) => ({ page, elapsedMs: performance.now() - started }));
    },
  );
  const searched = searchRead.value && searchKey ? { key: searchKey, ...searchRead.value } : null;
  const searchError = searchRead.error;
  useEffect(() => { setFocus(null); }, [searchKey]);

  const listKey = version && !params.q && params.type && !params.record ? `${version}|${params.type}|${params.offset}` : null;
  const listingRead = useRecoveringRead<Pinned<ListPage>>(
    listKey === null ? null : `${client}|${listKey}`,
    (signal) => listRecords(withAbortSignalClient(api, signal), {
      version: version as string, type: params.type as string, offset: params.offset,
    }),
  );
  const listing = listingRead.value && listKey ? { key: listKey, page: listingRead.value } : null;
  const listingError = listingRead.error;

  const recordKey = version && params.record ? `${version}|${params.record}` : null;
  const recordRead = useRecoveringRead<RecordAnswer>(
    recordKey === null ? null : `${client}|${recordKey}`,
    (signal) => {
      const bound = withAbortSignalClient(api, signal);
      const recordId = params.record as string;
      const at = version as string;
      // Three reads at once: the record, its original file and its raw
      // layers. The record's failure is the page's; the other two fail in
      // place on their own tabs (rule 13), never silently as an absent block.
      const settled = <T,>(promise: Promise<T>): Promise<{ value: T; error: null } | { value: null; error: unknown }> =>
        promise.then((value) => ({ value, error: null }), (error: unknown) => ({ value: null, error }));
      return Promise.all([
        readRecordDetail(bound, at, recordId),
        settled(readArtifact(bound, at, recordId)),
        settled(readRaw(bound, at, recordId)),
        // The producers, for ONE thing: the record's `source` is an ownership
        // id and Sources is addressed by producer name, so without this read
        // "open in Sources" pointed at a page that is not there. It is settled
        // and unused if it fails -- the source is then printed and not linked.
        settled(bound.get<RawProducersRead>(PRODUCERS_ROUTE)),
      ]).then(([detail, artifact, raw, producers]) => ({
        detail,
        reads: {
          artifact: artifact.value ? artifact.value.payload : null,
          raw: raw.value ? raw.value.payload : null,
          producers: producers.value
            ? producerRecordsFrom(producers.value).map((row) => ({ name: row.name, ownershipId: row.ownershipId }))
            : null,
          failures: { artifact: artifact.error, raw: raw.error },
        },
      }));
    },
  );
  const record = recordRead.value && recordKey ? { key: recordKey, ...recordRead.value } : null;
  const recordError = recordRead.error;

  // The keyboard, on whichever search box is on screen: arrows walk the rows,
  // Enter opens the FOCUSED one, and "/" focuses the box from anywhere.
  //
  // Registered in the CAPTURE phase. React 19 attaches its own handlers at
  // the root container, which is inside `document`, so a bubble-phase
  // listener here runs AFTER the box's `onKeyDown` -- and the box's Enter now
  // submits the search. Capturing puts this first, and it stops the event
  // only for the keys it actually takes; everything else, Enter included,
  // travels on to the box.
  const resultsModel = useMemo(() => {
    if (!base?.version || !searchKey || !searched || searched.key !== searchKey) return null;
    return buildResults(searched.page.payload, params, link, base.versions.deployment, base.version, SEARCH_PAGE_SIZE);
  }, [base, searched, searchKey, params, link]);
  const rowCount = resultsModel ? resultsModel.idMatches.length + resultsModel.labelMatches.length + resultsModel.textMatches.length : 0;
  useEffect(() => {
    if (typeof document === "undefined") return undefined;
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      const inSearch = Boolean(target && (
        target.closest(SEARCH_INPUT_SELECTOR.replace(" input", "")) || target.closest('[data-testid="explore-searchbox"]')
      ));
      if (!inSearch) return;
      // Escape clears: the focus row first, then the box's text.
      if (event.key === "Escape") {
        if (focus !== null) { event.preventDefault(); event.stopPropagation(); setFocus(null); return; }
        return;
      }
      if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
        if (!rowCount) return;
        event.preventDefault();
        event.stopPropagation();
        setFocus((current) => moveFocus(current, rowCount, event.key));
        return;
      }
      // Enter opens the focused row -- and ONLY when the reader has actually
      // arrowed to one. It used to open `rows[0]` whenever results were on
      // screen, which took Enter away from the box: a reader who amended the
      // query and pressed Enter got the first hit of the OLD query instead of
      // a search for what they had just typed. With no row focused, Enter
      // belongs to the form, which submits the draft.
      if (event.key === "Enter" && resultsModel && rowCount && focus !== null) {
        event.preventDefault();
        event.stopPropagation();
        const rows = [...resultsModel.idMatches, ...resultsModel.labelMatches, ...resultsModel.textMatches];
        const row = rows.find((candidate) => candidate.index === focus) ?? rows[0];
        window.location.hash = row.href;
      }
    };
    document.addEventListener("keydown", onKey, true);
    return () => document.removeEventListener("keydown", onKey, true);
  }, [resultsModel, rowCount, focus]);

  // `aria-activedescendant` on whichever search box is on screen, naming the
  // row the arrow keys have. The focus never leaves the box -- that is what
  // lets the reader keep typing -- so without it a screen reader announced
  // nothing as the highlighted row moved (review 2026-09-22). The boxes
  // belong to the top bar and to the landing, which do not know the focus;
  // the page does, so the page sets it, and clears it when there is none.
  useEffect(() => {
    if (typeof document === "undefined") return undefined;
    const boxes = [...document.querySelectorAll<HTMLInputElement>(
      `${SEARCH_INPUT_SELECTOR}, [data-testid="explore-search-input"]`,
    )];
    for (const box of boxes) {
      if (focus === null || !rowCount) box.removeAttribute("aria-activedescendant");
      else box.setAttribute("aria-activedescendant", resultRowId(focus));
    }
    return () => { for (const box of boxes) box.removeAttribute("aria-activedescendant"); };
  }, [focus, rowCount]);

  // Inside the page frame, with the route that failed named. Explore cannot
  // render partially from these two: the version comes from the versions read
  // and every other read on this page is scoped to a version, so with it gone
  // there is nothing to show a version of. That is a real full-page failure,
  // and it is said in the page rather than against the window's left edge.
  if (baseError) {
    return (
      <ConsolePageFailure
        error={baseError.reason}
        route={baseError.route}
        what="This deployment's published versions and record types"
        testId="console-explore-read-failed"
        onReadAgain={baseRead.readAgain}
        retrying={baseRead.retrying}
      />
    );
  }
  if (!base) return <ConsoleLoading what="this deployment" />;

  if (base.unpublished) {
    // Not a failure. The read answered and listed every published version,
    // and this is not one of them: a stale or hand-edited link. It used to
    // render as a red deployment failure from the record-types read.
    const latest = base.versions.latest;
    return (
      <main className="okg-console" data-testid="console-explore-unpublished">
        <p className="okg-console__fact okg-console__fact--page">
          Version <code className="okg-console__id">{shortVersion(base.unpublished)}</code> is
          {" "}not published here.
          {latest ? (
            <>
              {" "}
              <a href={link("explore", { ...exploreQuery({ ...params, version: null }), version: latest })}>
                Open the latest version ({shortVersion(latest)})
              </a>.
            </>
          ) : null}
        </p>
      </main>
    );
  }

  const deployment = base.versions.deployment;
  const context: PageContext = {
    deployment,
    version,
    publishedAt: version ? base.versions.versions.find((row) => row.generationId === version)?.publishedAt ?? null : null,
    nowMs,
  };
  const types = base.types?.payload ?? [];

  if (params.record && version) {
    if (recordError) {
      return (
        <LoudFailure
          error={recordError}
          route={DETAIL_ROUTE}
          what="This record"
          onReadAgain={recordRead.readAgain}
          retrying={recordRead.retrying}
        />
      );
    }
    if (!record || record.key !== recordKey) return <ConsoleLoading what={params.record} />;
    const detail = record.detail.payload;
    if (!detail.found) {
      // A plain named line, not a failure block. Nothing failed: the read
      // worked and answered that this version does not hold the record.
      // The failure surface would say "the read did not reach the server",
      // which is the opposite of what happened.
      return (
        <main className="okg-console" data-testid="console-explore-not-found">
          <p className="okg-console__back">
            <a href={link("explore", exploreQuery({ q: params.q, type: params.type }))}>&larr; Explore</a>
          </p>
          <p className="okg-console__fact">
            Version <code className="okg-console__id">{record.detail.version.slice(-12)}</code> of {deployment}
            {" "}has no record <code className="okg-console__id">{detail.nodeId}</code>, or the access policy
            {" "}keeps it from you.
          </p>
        </main>
      );
    }
    const model = buildRecordPage(
      detail.record,
      { artifact: record.reads.artifact, raw: record.reads.raw, producers: record.reads.producers },
      record.detail.version, base.versions.versions, params, link, deployment, nowMs, SEARCH_PAGE_SIZE,
    );
    return (
      <ConsoleRecordPage
        model={model}
        context={{ ...context, version: record.detail.version }}
        failures={record.reads.failures}
      />
    );
  }

  const page = searched && searched.key === searchKey ? searched.page.payload : null;
  const chips = version ? buildChips(types, params, link, page, allChips) : null;
  // The chips a zero-hit page offers are the VERSION's types, not a tally of
  // the nothing this query returned, and their links drop the query: the way
  // out of a search that matched nothing is to see what there is.
  const emptyChips = version && page && page.results.length === 0
    ? buildChips(types, { ...params, q: "", type: null }, link, null, allChips)
    : null;
  const helpOpen = help ?? params.help;
  let pending: ReactNode = null;
  if (searchKey && searchError) {
    pending = (
      <LoudFailure
        error={searchError}
        route={SEARCH_ROUTE}
        what="These results"
        onReadAgain={searchRead.readAgain}
        retrying={searchRead.retrying}
      />
    );
  } else if (searchKey && !resultsModel) pending = <ReadingLine what="the records that match" />;
  else if (listKey && listingError) {
    pending = (
      <LoudFailure
        error={listingError}
        route={RECORD_LIST_ROUTE}
        what={`Records of type ${params.type}`}
        onReadAgain={listingRead.readAgain}
        retrying={listingRead.retrying}
      />
    );
  } else if (listKey && (!listing || listing.key !== listKey)) pending = <ReadingLine what={`${params.type} records`} />;
  const listingModel = listKey && listing && listing.key === listKey && !listingError
    ? buildListing(listing.page.payload, types, params, link)
    : null;

  return (
    <ConsoleExplore
      deployment={deployment}
      version={version}
      types={types}
      params={params}
      chips={chips}
      emptyChips={emptyChips}
      onMoreChips={chips?.moreTypes ? () => setAllChips(true) : null}
      results={resultsModel}
      listing={listingModel}
      pending={pending}
      focus={focus}
      context={context}
      link={link}
      search={search}
      help={helpOpen}
      onHelp={() => setHelp(!helpOpen)}
    />
  );
}
