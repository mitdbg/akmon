import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { createApiClient, type ApiClient } from "@operator-ui/shared/api/apiClient";
import {
  chatSiteStateForDeployments,
  deploymentThemeStateForDeployments,
  loadDeployments,
  type DeploymentEntry,
} from "@operator-ui/app/deployments";
import {
  ChatSiteContext,
  type ChatSiteState,
} from "@operator-ui/shared/hooks/chatSiteContext";
import {
  applyDeploymentTheme,
  resolveDeploymentTheme,
  type DeploymentThemeState,
} from "@operator-ui/styles/deploymentTheme";
import { readStoredTheme, setFiring, useTheme } from "@operator-ui/styles/theme";

// The operator UI is the console. PACT console-default-remove-old-views made
// it the page every URL opens and deleted the old Home and Health pages, the
// sidebar they shared and its "Console (preview)" entry.

// The design harness (PACT operator-ui-redesign-mocks), reachable only at
// `#<view>?fixture=<name>` in a DEVELOPMENT build.
//
// What keeps the harness out of the shipped bundle is the RESOLVER in
// vite.operator-ui.config.ts: a production build resolves
// @operator-ui/design-harness/DesignHarness to a stub module, so the dynamic import
// below reaches neither the harness nor the 1.4 MB of fixture JSON it pulls
// in. `import.meta.env.DEV` is read through a cast here, so Rollup does NOT
// fold this expression away; the flag only decides whether the route is
// offered at runtime, and the stub chunk (a hundred bytes) is still emitted.
// scripts/operator-ui-design/check-bundle-clean.mjs proves the outcome rather than trusting
// either mechanism. The flag is read HERE, not imported from
// @operator-ui/design-harness, because importing it would statically pull the
// fixtures into this module and defeat the resolver.
const DESIGN_MOCKS_ENABLED = Boolean(
  (import.meta as { env?: { DEV?: unknown } }).env?.DEV,
);

const DesignHarnessRoute = DESIGN_MOCKS_ENABLED
  ? lazy(() => import("@operator-ui/design-harness/DesignHarness").then((module) => ({
    default: module.DesignHarness,
  })))
  : null;

// The console, loaded lazily. Every URL opens it, so the split no longer
// spares a reader anything; it stays because it keeps the entry bundle
// (`home_js` in audit/bundle-budgets.json) to this file's boot code and the
// console's code in its own separately budgeted chunk (`console_js`). The
// Graph view's d3 code is a further lazy chunk inside it.
const ConsoleRoute = lazy(() => import("@operator-ui/routes/console/ConsoleRoute").then(
  (module) => ({ default: module.ConsoleRoute }),
));

/** Where a hash that names no console view lands: the console's Home. */
export const CONSOLE_HOME_HASH = "#console";

/** The fixture named in the hash, or null. Deliberately a bare read of the
 *  query string rather than a call into the design tree, for the reason above. */
export function designFixtureFromHash(rawHash: string | null | undefined): string | null {
  const hash = (rawHash ?? "").replace(/^#/, "");
  const queryIndex = hash.indexOf("?");
  if (queryIndex === -1) return null;
  return new URLSearchParams(hash.slice(queryIndex + 1)).get("fixture");
}

/** The views of the redesign proposal, for the harness's hash grammar. */
const DESIGN_VIEW_PATHS = ["home", "updates", "sources", "explore", "graph"] as const;

function designViewFromHash(rawHash: string | null | undefined) {
  const hash = (rawHash ?? "").replace(/^#/, "");
  const queryIndex = hash.indexOf("?");
  // The first segment only: the redesign's attempt detail is `#updates/<id>`,
  // and a hash that carried an id used to resolve to Home.
  const path = (queryIndex === -1 ? hash : hash.slice(0, queryIndex))
    .toLowerCase()
    .split("/")[0];
  const match = DESIGN_VIEW_PATHS.find((candidate) => candidate === path) ?? "home";
  return (match.charAt(0).toUpperCase() + match.slice(1)) as
    "Home" | "Updates" | "Sources" | "Explore" | "Graph";
}

/**
 * The console hash a location hash opens. Pure.
 *
 * A console hash -- `#console`, or `#console/<view>[/<id>][?<query>]` -- is
 * the console's own grammar and is handed on untouched. EVERY other hash
 * opens the console's Home: none at all, `#home` and `#health` from the two
 * pages this app used to open on, `#graph`, `#trace`, `#performance` and
 * `#deployment` from views deleted before them, and anything a person types.
 * A bookmark or an old chat answer then lands on the page that says whether
 * this deployment is healthy, never on a blank or an error page. The old
 * hash's query is not carried over: the old pages' parameters (`?producer=`
 * and the like) mean nothing to the console.
 */
export function consoleHashFor(rawHash: string | null | undefined): string {
  const hash = (rawHash ?? "").replace(/^#/, "");
  const queryIndex = hash.indexOf("?");
  const path = (queryIndex === -1 ? hash : hash.slice(0, queryIndex)).toLowerCase();
  if (path === "console" || path.startsWith("console/")) return `#${hash}`;
  return CONSOLE_HOME_HASH;
}

export function HomeApp({ api }: { api?: ApiClient }) {
  const apiClient = useMemo(() => api ?? createApiClient(), [api]);
  // The raw hash. The console reads its view, id and parameters from it, and
  // the design harness switches fixture and view through its QUERY
  // (`#home?fixture=empty`), so both follow every change of it.
  const [rawHash, setRawHash] = useState(() => globalThis.location?.hash ?? "");
  const [deployments, setDeployments] = useState<DeploymentEntry[]>([]);
  const [deploymentsLoaded, setDeploymentsLoaded] = useState(false);
  // In a design session the harness owns every read, and they all come from the
  // fixture. The app's own deployments read below must NOT run: it is outside
  // any route and would otherwise fetch /viz/api/deployments from whatever is
  // serving the page. capture.mjs fails the capture on any such request, which
  // is how this was found.
  const designActive = Boolean(DesignHarnessRoute && designFixtureFromHash(rawHash));

  useEffect(() => {
    function updateHash() {
      setRawHash(globalThis.location?.hash ?? "");
    }

    updateHash();
    globalThis.addEventListener?.("hashchange", updateHash);
    return () => globalThis.removeEventListener?.("hashchange", updateHash);
  }, []);

  useEffect(() => {
    if (designActive) return undefined;
    let cancelled = false;
    loadDeployments(apiClient).then(
      (entries) => {
        if (!cancelled) {
          setDeployments(entries);
          // The read ANSWERED. Recorded separately from the entries so an
          // empty list after a failure is distinguishable from a list that
          // has not arrived yet — the chat affordance says different things
          // about those two, and must say neither before the read lands.
          setDeploymentsLoaded(true);
        }
      },
      () => {
        if (!cancelled) {
          setDeployments([]);
          setDeploymentsLoaded(true);
        }
      },
    );
    return () => {
      cancelled = true;
    };
  }, [apiClient, designActive]);

  // The console's ONLY source for the chat site is the deployments read; the
  // console's top bar reads it from context. A deployment that declares
  // nothing yields href=null, which the bar states rather than repairs.
  const chatSite = useMemo<ChatSiteState>(
    () => chatSiteStateForDeployments(deployments, deploymentsLoaded),
    [deployments, deploymentsLoaded],
  );

  // The same read also carries the deployment's declared theme tokens. They are
  // applied as an EFFECT on the document root rather than passed down, because
  // the properties they override are the ones the console's CSS already reads —
  // no view learns that a value came from a manifest instead of a palette.
  const deploymentTheme = useMemo<DeploymentThemeState>(
    () => deploymentThemeStateForDeployments(deployments, deploymentsLoaded),
    [deployments, deploymentsLoaded],
  );
  // The active firing is a TRIGGER here, not an input. The decision's real
  // inputs are the declared polarity and the operator's STORED choice, and the
  // picker writes both localStorage and `data-theme`, so observing the
  // attribute is how this effect learns the stored choice changed. Re-running
  // on every flip also keeps the overrides' survival structural rather than a
  // cascade coincidence a future `applyTheme` could break. Switching
  // DEPLOYMENT is a full page navigation, so the first run after boot is the
  // one that matters there.
  //
  // `setFiring` can change `data-theme` and so re-enter this effect. It
  // converges after one extra pass: the second run reads the same stored
  // choice, resolves the same plan, and `setFiring` no-ops on an attribute
  // already equal to it.
  const firing = useTheme();
  useEffect(() => {
    const plan = resolveDeploymentTheme(deploymentTheme, readStoredTheme());
    // Never `applyTheme`: a deployment-supplied base must not be written into
    // the operator's stored preference. See `setFiring`.
    setFiring(plan.firing);
    applyDeploymentTheme(plan);
  }, [deploymentTheme, firing]);

  return (
    <ChatSiteContext.Provider value={chatSite}>{renderRoute()}</ChatSiteContext.Provider>
  );

  function renderRoute() {
    // A design session takes the whole app: the harness owns its own top bar
    // and reads only from the fixture. In a production build DesignHarnessRoute
    // is null and this branch is unreachable.
    const designFixture = designFixtureFromHash(rawHash);
    if (DesignHarnessRoute && designFixture) {
      return (
        <Suspense fallback={<div className="okg-loading">Loading the design harness…</div>}>
          <DesignHarnessRoute
            location={{ fixture: designFixture, view: designViewFromHash(rawHash) }}
          />
        </Suspense>
      );
    }

    return (
      <Suspense fallback={<div className="okg-loading">Loading the console…</div>}>
        <ConsoleRoute api={apiClient} hash={consoleHashFor(rawHash)} />
      </Suspense>
    );
  }
}
