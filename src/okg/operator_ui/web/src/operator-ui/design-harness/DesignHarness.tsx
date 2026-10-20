// The design harness: the four views of the redesign, rendered from one mock
// deployment, with no network.
//
// All four views are drawn (PACT tasks req.operator-ui-redesign-mocks.home,
// .updates, .sources and .explore, against the pinned references under
// ui/references/). Explore is built on #1931's data layer inside the console
// shell; the shipping Search view is no longer rendered here.
//
// Every read goes through the fixture client, which refuses a read the fixture
// does not define. That refusal is rendered here as a named, visible failure --
// rule 13 -- so a missing read can never look like a healthy empty page.
import { Suspense, lazy, useEffect, useMemo, useRef, useState, type ReactElement } from "react";
import "@operator-ui/design-harness/design.css";
import "@operator-ui/console/styles/index.css";
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import { OperatorAntdProvider } from "@operator-ui/shared/ui/antdTheme";
import {
  createFixtureApiClient,
  type DesignFixture,
} from "@operator-ui/design-harness/fixtureClient";
import {
  DESIGN_FIXTURES,
  DESIGN_VIEWS,
  fixtureByName,
  type DesignView,
} from "@operator-ui/design-harness/fixtures";
import { LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { ConsoleHomeRoute } from "@operator-ui/console/views/Home";
import { ConsoleUpdatesRoute } from "@operator-ui/console/views/Updates";
import { ConsoleSourcesRoute } from "@operator-ui/console/views/Sources";
import { ConsoleExploreRoute } from "@operator-ui/console/views/Explore";
import { DocumentViewersContext } from "@operator-ui/console/components/documentViewers";
import { ConsoleTopBar } from "@operator-ui/console/views/TopBar";
import type { LinkFn } from "@operator-ui/console/model/shared/links";

/** The four document viewers, mounted HERE and nowhere else.
 *
 *  The live console does not carry them: no route serves PDF, HTML,
 *  Markdown or code bytes (#2121), so pdf.js, react-markdown and shiki
 *  would be paid for a viewer that can never fire. The harness draws them
 *  against fixture artifacts, and the `explore_js` and `pdf_worker`
 *  budgets measure THIS build. See console/components/documentViewers.ts.
 */
const DocumentViewers = lazy(() => import("@operator-ui/console/viewers/DocumentViewers"));

/** Graph, lazily, as the live console loads it. */
const ConsoleGraphRoute = lazy(() => import("@operator-ui/console/views/graph/Graph"));

/** How long after the last keystroke Explore's URL -- and so its results --
 *  move. */
export const SEARCH_DEBOUNCE_MS = 150;

export interface DesignLocation {
  fixture: string | null;
  view: DesignView;
  /** The id after the view, as in `#updates/bd2648936849`. Null on a list. */
  detail?: string | null;
  /** The route's own parameters -- the filters a view carries -- with the
   *  harness's `fixture` removed. */
  params?: Record<string, string>;
}

/** Read `#<view>[/<detail>]?fixture=<name>&<filters>` without disturbing the
 *  shipping hash grammar. Pure, and exported so a test can drive it without a
 *  browser. */
export function parseDesignLocation(rawHash: string | null | undefined): DesignLocation {
  const hash = (rawHash ?? "").replace(/^#/, "");
  const queryIndex = hash.indexOf("?");
  const path = (queryIndex === -1 ? hash : hash.slice(0, queryIndex)).toLowerCase();
  const [head, ...rest] = path.split("/");
  const query = new URLSearchParams(queryIndex === -1 ? "" : hash.slice(queryIndex + 1));
  const view = DESIGN_VIEWS.find((candidate) => candidate.toLowerCase() === head) ?? "Home";
  // Every parameter except the harness's own fixture: they are the view's
  // filters, and Home's counts are only real links if they survive the hop.
  const params: Record<string, string> = {};
  for (const [name, value] of query.entries()) {
    if (name !== "fixture") params[name] = value;
  }
  return {
    fixture: query.get("fixture"),
    view,
    detail: rest.length ? rest.join("/") : null,
    params,
  };
}

export function designHash(view: DesignView, fixture: string): string {
  return `#${view.toLowerCase()}?fixture=${encodeURIComponent(fixture)}`;
}

/**
 * Every in-app link the console builds, with the harness's fixture carried
 * along.
 *
 * Exported and pure: the counts on Home are only real filters if the href they
 * carry survives, and this is where that is decided for all of them.
 */
export function designLink(fixture: string): LinkFn {
  return (route, params = {}) => {
    const query = new URLSearchParams({ fixture, ...params });
    return `#${route}?${query.toString()}`;
  };
}

/** The switcher: design-time only, and dressed as scaffolding rather than as
 *  one of the console's own controls. */
function FixtureSwitcher({ fixture, view }: { fixture: DesignFixture; view: DesignView }) {
  return (
    <label className="okg-design-topbar__switcher">
      <span>mock</span>
      <select
        data-testid="design-fixture-switcher"
        aria-label="Mock deployment"
        value={fixture.name}
        onChange={(event) => {
          window.location.hash = designHash(view, event.target.value);
        }}
      >
        {DESIGN_FIXTURES.map((candidate) => (
          <option key={candidate.name} value={candidate.name}>{candidate.name}</option>
        ))}
      </select>
    </label>
  );
}

/**
 * The harness, or a named failure when the fixture does not exist.
 *
 * Returning null is what keeps the shipping console unchanged: `HomeApp`
 * renders its normal routes whenever the hash names no fixture, and a
 * production build never reaches this module at all.
 */
export function DesignHarness({ location }: { location: DesignLocation }): ReactElement {
  const fixture = fixtureByName(location.fixture);
  if (!fixture) {
    return (
      <LoudFailure
        error={new Error(
          `No design fixture is named "${location.fixture}". The six are: `
          + `${DESIGN_FIXTURES.map((candidate) => candidate.name).join(", ")}.`,
        )}
      />
    );
  }

  // HomeApp resolves the fixture and the view from the hash and hands them
  // over; the detail id and the view's filters are read here, from the same
  // hash, rather than duplicating this grammar in the shipping app. HomeApp
  // re-renders on every hash change, so this is re-read with it.
  const fromHash = parseDesignLocation(
    typeof window === "undefined" ? "" : window.location.hash,
  );
  const resolved: DesignLocation = {
    ...location,
    detail: location.detail ?? fromHash.detail ?? null,
    params: location.params ?? fromHash.params ?? {},
  };

  return <DesignSession key={fixture.name} fixture={fixture} location={resolved} />;
}

/**
 * One fixture, with the version every view reads.
 *
 * The version is state rather than a constant because Updates can change it:
 * choosing "Read this version" on a published attempt is what the requirement
 * means by "selecting a published attempt switches every view to that version"
 * (section 7). Keyed by fixture name, so switching mock deployment starts from
 * that deployment's own latest rather than carrying a version it never had.
 */
function DesignSession({
  fixture, location,
}: { fixture: DesignFixture; location: DesignLocation }): ReactElement {
  const api: ApiClient = useMemo(() => createFixtureApiClient(fixture), [fixture]);
  const link = designLink(fixture.name);
  const [version, setVersion] = useState<string | null>(null);
  const params = location.params ?? {};
  const onExplore = location.view === "Explore";

  // The search box's text. On Explore it is the page's `q`: the box shows
  // what the URL says, and typing rewrites the URL after SEARCH_DEBOUNCE_MS
  // (replacing the entry, so Back leaves the page rather than the keystroke)
  // and Explore re-reads. Off Explore the bar keeps its own text.
  const [draft, setDraft] = useState(params.q ?? "");
  const typed = useRef(false);
  const paramsKey = JSON.stringify(params);
  useEffect(() => {
    // The URL is the source of truth until the reader types; then the box
    // is, until its text has reached the URL.
    if (onExplore && !typed.current) setDraft(params.q ?? "");
  }, [onExplore, params.q]);
  useEffect(() => {
    if (!onExplore || !typed.current) return undefined;
    const timer = setTimeout(() => {
      const next: Record<string, string> = { ...(JSON.parse(paramsKey) as Record<string, string>) };
      const trimmed = draft.trim();
      if (trimmed) next.q = trimmed; else delete next.q;
      // A new query starts at the first page and closes any open record.
      delete next.offset;
      delete next.record;
      delete next.tab;
      typed.current = false;
      const hash = link("explore", next);
      if (window.location.hash === hash) return;
      window.history.replaceState(null, "", hash);
      window.dispatchEvent(new HashChangeEvent("hashchange"));
    }, SEARCH_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [draft, onExplore, paramsKey, link]);

  return (
    <OperatorAntdProvider>
      <div className="okg-design" data-testid="design-harness" data-fixture={fixture.name}>
        <ConsoleTopBar
          api={api}
          view={location.view}
          link={link}
          nowMs={Date.parse(fixture.clock)}
          version={version}
          onVersion={setVersion}
          search={onExplore ? {
            value: draft,
            onChange: (value) => { typed.current = true; setDraft(value); },
            // On the landing the big box takes focus; on results the bar's.
            autoFocus: !params.record && Boolean(params.q),
          } : null}
          designControl={<FixtureSwitcher fixture={fixture} view={location.view} />}
          pickRecord={location.view === "Graph" ? (id: string) => {
            window.location.hash = link("graph/record", { id });
          } : null}
        />
        {location.view === "Home" ? <ConsoleHomeRoute api={api} link={link} /> : null}
        {location.view === "Updates" ? (
          <ConsoleUpdatesRoute
            api={api}
            link={link}
            params={location.params ?? {}}
            attemptId={location.detail ?? null}
            readingVersion={version}
            onRead={setVersion}
          />
        ) : null}
        {location.view === "Explore" ? (
          <DocumentViewersContext.Provider value={(props) => <DocumentViewers {...props} />}>
            <ConsoleExploreRoute
              api={api}
              link={link}
              params={params}
              readingVersion={version}
              nowMs={Date.parse(fixture.clock)}
              search={{ value: draft, onChange: (value) => { typed.current = true; setDraft(value); } }}
            />
          </DocumentViewersContext.Provider>
        ) : null}
        {location.view === "Graph" ? (
          <Suspense fallback={null}>
            <ConsoleGraphRoute
              api={api}
              link={link}
              params={params}
              detail={location.detail ?? null}
              readingVersion={version}
              nowMs={Date.parse(fixture.clock)}
            />
          </Suspense>
        ) : null}
        {location.view === "Sources" ? (
          <ConsoleSourcesRoute
            api={api}
            link={link}
            params={location.params ?? {}}
            producerName={location.detail ? decodeURIComponent(location.detail) : null}
          />
        ) : null}
      </div>
    </OperatorAntdProvider>
  );
}
