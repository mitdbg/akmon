// The console, on live data: the shell, Home, Updates and Sources, reading
// this deployment.
//
// PACT operator-console-live-home, slice 1 of issue #1924. The console was
// drawn against fixtures in the design harness and the operator approved
// Home's static mock on 2026-09-19; nothing an operator could open rendered
// it. This module is the whole difference: the same components the harness
// mounts, handed `createApiClient()` instead of `createFixtureApiClient()`.
// The console consumes one interface (ApiClient.get) and both clients satisfy
// it, so there is no second code path and no fixture import anywhere below.
//
// This file is loaded LAZILY from HomeApp, which opens it for every URL: it is
// the whole operator UI since PACT console-default-remove-old-views deleted
// the old Home and Health pages. The split keeps the entry bundle to the app's
// boot code and this chunk under its own ceiling in audit/bundle-budgets.json.
//
// All five views ship live: Home, Updates, Sources, Explore and Graph (PACT
// operator-console-live-home, then -updates, -sources, -explore and
// console-graph-views). Each landed only once its reads were measured against
// a published deployment and the elements no read could source were cut.
//
// Explore mounts WITHOUT the four document viewers. No route serves PDF,
// HTML, Markdown or code bytes (#2121), so `DocumentViewersContext` is
// left null here and the Content tab names the condition instead of
// drawing an empty pane. The design harness fills it.
import { Suspense, lazy, useCallback, useEffect, useMemo, useState, type ReactElement } from "react";
import { ConsoleReadsContext, type ConsoleReads } from "@operator-ui/console/components/readRecovery";
import { useScrollMemory } from "@operator-ui/console/components/scrollMemory";
import "@operator-ui/console/styles/index.css";
import { ConsoleHomeRoute } from "@operator-ui/console/views/Home";
import { ConsoleTopBar } from "@operator-ui/console/views/TopBar";
import { ConsoleSourcesRoute } from "@operator-ui/console/views/Sources";
import { ConsoleUpdatesRoute } from "@operator-ui/console/views/Updates";
import { ConsoleExploreRoute } from "@operator-ui/console/views/Explore";
import { ConsoleLoading } from "@operator-ui/console/components/ConsoleLoading";
import { versionNotReadHere } from "@operator-ui/console/components/VersionNote";
import { looksLikeVersionId } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { MetricId } from "@operator-ui/console/model/metrics/series";
import type { ServesRouteFn } from "@operator-ui/console/model/shared/links";
import { type ApiClient, withReadTimeout } from "@operator-ui/shared/api/apiClient";

/** Graph is loaded on its own: d3's force layout and zoom are paid only by a
 *  reader who opens it, never by the other four views. */
const ConsoleGraphRoute = lazy(() => import("@operator-ui/console/views/graph/Graph"));

/** The hash the shipped navigation points at. */
export const CONSOLE_HASH = "#console";

/**
 * Every in-app link the console builds, under the `#console` prefix.
 *
 * The mirror of the harness's `designLink`: same grammar, with the fixture
 * replaced by the console's own hash segment. A console view asks for
 * `link("updates", {source: "papers"})`; here that becomes
 * `#console/updates?source=papers`. The views that those links open do not
 * ship yet, which is exactly why the shell marks them as not live: a link
 * whose destination is not served must not be presented as one that is.
 */
export function consoleLink(route: string, params: Record<string, string> = {}): string {
  // `version` is dropped unless it is shaped like a generation id.
  //
  // The picker's choice rides on every link so moving between views does not
  // silently lose it, and while the value comes from the picker it is always
  // a published id. But `?version=` is whatever a reader or an earlier link
  // put on the URL: on 2026-09-22 a PostgreSQL DSN -- password included --
  // arrived there. Nothing PRINTS an unrecognised value any more
  // (`namedVersion`), and this is the other half: a link an operator copies
  // out of the page and pastes into an issue, a chat or a ticket carries it
  // somewhere the reader never had it, which the address bar it came from
  // does not. A recognised id still rides; anything else is left behind, and
  // the reader's own URL is untouched.
  const entries = Object.entries(params).filter(
    ([key, value]) => key !== "version" || looksLikeVersionId(value),
  );
  const query = new URLSearchParams(entries).toString();
  return query ? `#console/${route}?${query}` : `#console/${route}`;
}

/** The query a console hash carries: `#console/home?version=okg:...` -> the
 *  parameters, so a control can change one and keep the rest. */
export function consoleParamsFromHash(rawHash: string | null | undefined): Record<string, string> {
  const hash = (rawHash ?? "").replace(/^#/, "");
  const queryIndex = hash.indexOf("?");
  if (queryIndex === -1) return {};
  return Object.fromEntries(new URLSearchParams(hash.slice(queryIndex + 1)));
}

/** `#console`, `#console/home`, `#console/updates?x=y` -> the view segment. */
export function consoleViewFromHash(rawHash: string | null | undefined): string {
  const hash = (rawHash ?? "").replace(/^#/, "");
  const queryIndex = hash.indexOf("?");
  const path = queryIndex === -1 ? hash : hash.slice(0, queryIndex);
  const segments = path.toLowerCase().split("/").filter(Boolean);
  return segments[1] ?? "home";
}

/** Every view name the console HAS: the five in the bar. */
export const CONSOLE_VIEW_NAMES: readonly string[] = ["Home", "Updates", "Sources", "Explore", "Graph"];

/** The views this build serves live. All five read this deployment, so
 *  every entry in the bar is a link and none is dimmed. */
export const LIVE_VIEWS: readonly string[] = ["Home", "Updates", "Sources", "Explore", "Graph"];

/** The only metric the live chart offers. See the comment at its use. */
export const OFFERED_METRICS: readonly MetricId[] = ["attempts"];

/** The routes a production build of this slice actually serves. Everything
 *  the console can link to is checked against this, so a link can never
 *  promise a view the router answers with Home. */
export const SERVED_ROUTES: readonly string[] = ["home", "updates", "sources", "explore", "graph"];

export const servesRoute: ServesRouteFn = (route) => SERVED_ROUTES.includes(
  route.split("/")[0].split("?")[0].toLowerCase(),
);

/**
 * The third segment of a console hash, as the read must receive it.
 *
 * A producer is named by its registry key, which can hold characters a URL
 * escapes: `okg-ci` configures `ci-triage-verdicts` whose runtime key is
 * `agent:okg-ci-triage`, and a colon in a path segment arrives percent
 * encoded. So the segment is decoded, and NOT lowercased -- a read handed a
 * lowercased name answers "no such producer" for one that exists.
 *
 * `decodeURIComponent` throws on a malformed escape such as `%zz`, which a
 * person can type into the address bar. The raw segment is handed on as it
 * stands instead: the view then refuses it by name, which is a page saying
 * what is wrong, where a thrown error is a blank screen.
 */
export function decodeHashSegment(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}

/**
 * The views whose reads accept a `version`, and so can honour `?version=`.
 *
 * Explore and Graph. `console/health`, `console/generations`,
 * `console/versions`, the three diagnostics reads, `console/attempts` and
 * `console/ontology-rollup` all answer for the generation the read API's own
 * session is pinned to, and none declares a `version` query parameter. The
 * record reads behind Explore DO: `console/records/*` takes `version` and
 * answers for it, which is why searching or opening a record under an older
 * generation gives that generation's answer. So do Graph's four reads
 * (`console/graph/*`). So the picker is a real control
 * -- it changes the URL and every link carries it -- and the views that
 * cannot honour it show the version they DID read and say so.
 *
 * The bar used to print the asked-for version over figures that were the
 * pinned one's, which is the one reading of this page that is simply false.
 */
export const VERSION_READING_VIEWS: readonly string[] = ["Explore", "Graph"];

/** True when this view's own reads take `?version=`. The hash gives the view
 *  folded to lower case and `VERSION_READING_VIEWS` holds the names as the
 *  bar prints them, so the comparison is made here rather than at each use,
 *  where one of the two spellings would sooner or later be compared raw. */
export function readsVersion(view: string): boolean {
  return VERSION_READING_VIEWS.some((name) => name.toLowerCase() === view.toLowerCase());
}

/** The hash choosing `generationId` in the version picker produces: the view
 *  the reader is on, its existing parameters, and `version=` replaced. */
export function versionHash(
  view: string, params: Record<string, string>, generationId: string,
): string {
  return consoleLink(view, { ...params, version: generationId });
}

/**
 * The sentence a page prints when `?version=` names a version its reads do
 * not take -- the text half of `VERSION_READING_VIEWS`, which is the list.
 *
 * It is a FUNCTION of the two versions, not a fixed string: the line used to
 * say that a version was not honoured without naming either the one on the
 * page or the one in the URL, so a reader looking at the figures could not
 * tell which version they belonged to. Home, the Updates list and the attempt
 * page all print this one sentence, each exactly once. The text lives with
 * the component that draws it, under `console/components`, because a view
 * importing this route would close an import cycle; the name is re-exported
 * here, where the picker that causes the line lives.
 */
export const VERSION_NOT_READ_HERE = versionNotReadHere;

/** `#console/updates/5c5404d36118?outcome=blocked` -> the attempt id and the
 *  query parameters. The view and the parameters are read by the shell's own
 *  helpers, so there is one parser for each; the attempt id is the third
 *  segment, which only the Updates route asks for. */
export function consoleRouteParts(rawHash: string | null | undefined): {
  view: string;
  attemptId: string | null;
  params: Record<string, string>;
} {
  const hash = (rawHash ?? "").replace(/^#/, "");
  const queryIndex = hash.indexOf("?");
  const path = queryIndex === -1 ? hash : hash.slice(0, queryIndex);
  const segments = path.split("/").filter(Boolean);
  return {
    view: consoleViewFromHash(rawHash),
    // NOT lowercased: a generation id is hex and a producer name is not, and
    // the route must hand back what the URL held rather than a version of it
    // the read cannot find. Decoded, because a producer key can carry a colon.
    attemptId: segments[2] === undefined ? null : decodeHashSegment(segments[2]),
    params: consoleParamsFromHash(rawHash),
  };
}

/** Where the search box's unsubmitted text is kept across Back and Forward:
 *  the session's own storage, one entry per view and URL query. */
export const SEARCH_DRAFT_KEY = "okg-console-search-draft";

function readDraft(key: string): string | null {
  try {
    const raw = window.sessionStorage.getItem(SEARCH_DRAFT_KEY);
    const drafts = raw ? JSON.parse(raw) as Record<string, unknown> : {};
    const value = drafts[key];
    return typeof value === "string" ? value : null;
  } catch {
    return null;
  }
}

function writeDraft(key: string, value: string): void {
  try {
    const raw = window.sessionStorage.getItem(SEARCH_DRAFT_KEY);
    const drafts = raw ? JSON.parse(raw) as Record<string, unknown> : {};
    drafts[key] = value;
    window.sessionStorage.setItem(SEARCH_DRAFT_KEY, JSON.stringify(drafts));
  } catch {
    // Storage refused (a private window, a quota): the draft lives only as
    // long as this page, which is what it did before it was kept at all.
  }
}

export function ConsoleRoute({
  api: shellApi, hash,
}: {
  api: ApiClient;
  /** The location hash, so the route can answer `#console/<view>`. Read from
   *  the window when the caller passes none. */
  hash?: string | null;
}): ReactElement {
  const [readVersion, setReadVersion] = useState<string | null>(null);
  // The version the Graph page shows, for the bar's record picker.
  const [graphVersion, setGraphVersion] = useState<string | null>(null);
  // Every console read is bounded: one that takes longer than
  // READ_TIMEOUT_MS fails as "timed out after 20 s" instead of leaving the
  // page reading forever.
  const api = useMemo(() => withReadTimeout(shellApi), [shellApi]);
  // How the page's reads recover. `reloadKey` re-reads the whole view (a
  // click on its own link in the bar); `recoveryKey` re-reads only what
  // failed, whenever any read answers after failing.
  const [reloadKey, setReloadKey] = useState(0);
  const [recoveryKey, setRecoveryKey] = useState(0);
  const reportRecovered = useCallback(() => setRecoveryKey((key) => key + 1), []);
  const readAgainView = useCallback(() => setReloadKey((key) => key + 1), []);
  const reads: ConsoleReads = useMemo(
    () => ({ reloadKey, recoveryKey, reportRecovered, readAgainView }),
    [reloadKey, recoveryKey, reportRecovered, readAgainView],
  );
  const rawHash = hash ?? (typeof window === "undefined" ? "" : window.location.hash);
  const { view, attemptId, params } = consoleRouteParts(rawHash);
  useScrollMemory(rawHash);
  const asked = params.version || null;
  /**
   * The asked-for version, IF it is one.
   *
   * `?version=` is whatever a reader or an earlier link put on the URL, and
   * this console already has one rule for such a value: `consoleLink` leaves
   * it off every generated href and `namedVersion` prints it nowhere. This
   * applies that same rule one step earlier, before the value reaches a read.
   *
   * It matters now because Explore is the first view whose reads TAKE a
   * version. Handed `?version=x`, `console/records/types` answers 422 and the
   * whole view becomes a read failure -- while the identical URL on Home,
   * Updates or Sources is quietly dropped and the latest is read. One value,
   * two behaviours, and the one that breaks is the view a stale or truncated
   * link is most likely to name. A value this console does not recognise as a
   * generation id is not a version anywhere, so Explore reads the latest, as
   * it does when no version was asked for at all.
   *
   * A version it DOES recognise is passed through untouched. If the
   * deployment never published it the read refuses by name, which is a fact
   * about that generation and is reported rather than swallowed.
   */
  const version = asked && looksLikeVersionId(asked) ? asked : null;
  // Every link this console builds carries the chosen version, so moving
  // between views does not silently drop it.
  const link: LinkFn = useMemo(
    () => (route, extra = {}) => consoleLink(
      route, version ? { version, ...extra } : extra,
    ),
    [version],
  );
  // ONE search draft, owned by the shell (rule 9). On Explore the top bar's
  // box IS the page's box: it drives the search in place instead of opening
  // a second copy of the view. The landing has its own 640 px box, and there
  // the bar hides its own rather than showing the same text twice.
  //
  // The draft STARTS as the query the URL carries and follows it on a hash
  // change, so a shared or bookmarked results link shows the query that
  // produced what is on screen, and typing amends it rather than replacing
  // it. The effect is keyed on the URL's query alone: a re-render at the same
  // query does not run it, so text the reader has typed since is never
  // overwritten -- only an actual change of query re-seeds the box.
  //
  // Text typed and NOT submitted used to be lost on Back: the draft was
  // re-seeded from the URL, which never held it. It is kept per view in the
  // session's storage and restored when the view's URL carries no query of
  // its own (audit 2026-09-23, L5).
  //
  // Kept by view AND by the query the URL carries, so Back to a results page
  // brings back what was typed over THAT query, and a new query starts from
  // its own text.
  const urlQuery = params.q ?? "";
  const draftKey = `${view}?${urlQuery}`;
  const [draft, setDraftState] = useState(() => readDraft(draftKey) ?? urlQuery);
  useEffect(() => {
    setDraftState(readDraft(draftKey) ?? urlQuery);
  }, [draftKey, urlQuery]);
  const setDraft = useCallback((next: string) => {
    setDraftState(next);
    writeDraft(draftKey, next);
  }, [draftKey]);
  const onExplore = view === "explore";
  const exploreLanding = onExplore && !params.q && !params.record;
  const search = useMemo(
    () => ({ value: draft, onChange: setDraft }),
    [draft, setDraft],
  );
  // Exported and tested as a function: the picker's own onChange cannot be
  // driven in jsdom without a testing library, so what the control does is
  // proved here and that it is WIRED is proved on the rendered element.
  const onVersion = (generationId: string) => {
    // The attempt id rides along: picking a version on an attempt page must
    // not drop the reader back to the list.
    window.location.hash = versionHash(
      attemptId ? `${view}/${attemptId}` : view, params, generationId,
    );
  };
  // The picker shows the version the READER CHOSE, on every view.
  //
  // It used to show the version this view's reads answered for, so on Home,
  // Updates and Sources -- whose reads take no version -- the control snapped
  // back to the pinned one the instant the choice was made. A select that
  // undoes the reader's selection reads as a control that did not work, and
  // the reader had nothing to compare their choice against.
  //
  // What the page SHOWS is said in words instead, twice and precisely: the
  // once-only note under the bar names both versions -- "This page shows
  // version A; it cannot read version B" -- and the footer names the one the
  // figures belong to. An unrecognised `?version=` is not a choice and does
  // not reach the picker; the note says a version was asked for and that it
  // is not a published version id.
  const barVersion = version;
  return (
    <ConsoleReadsContext.Provider value={reads}>
      <div className="okg-design okg-console-live" data-testid="console-live">
        <ConsoleTopBar
          api={api}
          view={view === "updates" ? "Updates" : view === "sources" ? "Sources" : view === "explore" ? "Explore" : view === "graph" ? "Graph" : "Home"}
          link={link}
          version={barVersion}
          readVersion={readVersion}
          onVersion={onVersion}
          // Home, Updates and Sources always read the latest version, so the
          // picker there says "latest"; Explore's and Graph's reads take the
          // choice.
          versionReading={readsVersion(view)}
          // A page read that answers is the bar's cue to read again whatever
          // it could not read before.
          pageReadKey={readVersion}
          onReloadView={readAgainView}
          search={onExplore && !exploreLanding ? search : null}
          hideSearch={exploreLanding}
          // On Graph a picked search result opens that record's
          // neighbourhood; everywhere else the box opens Explore.
          pickRecord={view === "graph" ? (id: string) => {
            window.location.hash = link("graph/record", { id });
          } : null}
          pickVersion={view === "graph" ? graphVersion : null}
          liveViews={LIVE_VIEWS}
        />
        {/* Attempts by outcome only. The other three metrics are derived from
            a per-attempt duration no checkpoint records and from a
            records-added figure the capped generations read runs out of
            before the span does; a picker offering them would promise data
            this deployment cannot give. */}
        {view === "home" ? (
          <ConsoleHomeRoute
            api={api}
            link={link}
            offeredMetrics={OFFERED_METRICS}
            serves={servesRoute}
            // The RAW parameter, not the recognised one: the sentence decides
            // for itself whether there is a version here worth naming, and it
            // says "a version was asked for and it is not a published version
            // id" for one there is not. Handed the filtered value it would
            // print nothing at all and the reader would never learn that what
            // they put on the URL was not read.
            askedVersion={asked}
            onReadVersion={setReadVersion}
          />
        ) : view === "updates" ? (
          <ConsoleUpdatesRoute
            api={api}
            link={link}
            params={params}
            attemptId={attemptId}
            askedVersion={asked}
            onReadVersion={setReadVersion}
          />
        ) : view === "sources" ? (
          <ConsoleSourcesRoute
            api={api}
            link={link}
            params={params}
            producerName={attemptId}
            askedVersion={asked}
            onReadVersion={setReadVersion}
          />
        ) : view === "explore" ? (
          // The shell's draft, shared with the top bar: whichever box is on
          // screen is the same field. No `DocumentViewersContext.Provider`:
          // see the note at the top of this file.
          //
          // `readingVersion` is the version on the URL, not the one another
          // view read: Explore's record reads DO take a version, which is
          // what puts "Explore" in `VERSION_READING_VIEWS`.
          <ConsoleExploreRoute
            api={api}
            link={link}
            params={params}
            readingVersion={version}
            nowMs={Date.now()}
            search={search}
          />
        ) : view === "graph" ? (
          // The version on the URL, as Explore: the graph reads take one.
          <Suspense fallback={<ConsoleLoading what="the graph view" />}>
            <ConsoleGraphRoute
              api={api}
              link={link}
              params={params}
              detail={attemptId}
              readingVersion={version}
              onShownVersion={setGraphVersion}
            />
          </Suspense>
        ) : (
          // A hash naming NO console view at all, which is the ONLY thing
          // left for this branch to answer: every name in
          // `CONSOLE_VIEW_NAMES` is routed above, so the "<view> is not live
          // in this build" page that used to sit here is unreachable and is
          // gone with it. A hash that named nothing used to get that same
          // line, which said a view by that name exists and is coming. None
          // does. The five that do are named here, so the reader does not
          // have to look back at the bar to find out what to ask for.
          <main className="okg-console" data-testid="console-no-such-view">
            <p className="okg-console__fact">
              No console view is named &quot;{view}&quot;. The console has
              five: {CONSOLE_VIEW_NAMES.join(", ")}.
            </p>
          </main>
        )}
      </div>
    </ConsoleReadsContext.Provider>
  );
}

export default ConsoleRoute;
