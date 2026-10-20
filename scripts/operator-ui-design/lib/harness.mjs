// Shared plumbing for the two scripts that drive the design harness in a
// browser: capture.mjs (screenshots) and audit-rules.mjs (measurements).
//
// Both need the same three things: a dev server on a free port, a page that
// has actually rendered the harness, and a guarantee that nothing was read over
// the network. The network guarantee is the important one -- a mock that
// quietly fell through to a real API would be approved as a mock.
import { spawn } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
export const DESIGN_DIR = join(HERE, "..");
export const REPO_ROOT = join(HERE, "..", "..", "..");
export const PACT_UI_DIR = join(
  REPO_ROOT, "pact", "changes", "operator-ui-redesign-mocks", "ui",
);

/**
 * The captures and the audit are evidence of one PACT change record, and they
 * are written into it. A checkout without that record (the public copy of
 * this repository) must not have a record directory invented for it: refuse
 * and say which directory is missing.
 */
export function requirePactUiDir() {
  if (!existsSync(PACT_UI_DIR)) {
    throw new Error(
      `the design harness writes its evidence into ${PACT_UI_DIR}, the ` +
        "operator-ui-redesign-mocks PACT change record, which this checkout " +
        "does not have",
    );
  }
  return PACT_UI_DIR;
}

export const VIEWS = ["Home", "Updates", "Sources", "Explore"];
export const WIDTHS = [
  { width: 1440, height: 1000 },
  { width: 390, height: 844 },
];

export function fixtureNames() {
  return readdirSync(join(DESIGN_DIR, "fixtures"))
    .filter((name) => name.endsWith(".json"))
    .map((name) => name.replace(/\.json$/, ""))
    // Switcher order, not alphabetical: the real shape first, then the stresses.
    .sort((a, b) => ORDER.indexOf(a) - ORDER.indexOf(b));
}

const ORDER = [
  "archiv3-small", "workspace-mixed", "scale-200", "empty", "broken-runtime", "all-clear",
];

/**
 * The attempt whose detail page is captured for this fixture.
 *
 * The newest BLOCKED attempt where there is one -- that is the page the view
 * exists for -- otherwise the newest published one, and nothing at all for a
 * deployment with no attempts. Read from the fixture rather than pinned in a
 * list here, so a regenerated fixture cannot leave the captures pointing at an
 * attempt that no longer exists.
 */
export function detailAttemptFor(name) {
  const fixture = JSON.parse(
    readFileSync(join(DESIGN_DIR, "fixtures", `${name}.json`), "utf8"),
  );
  const rows = fixture.reads["/viz/api/console/generations"]?.payload?.generations ?? [];
  const pick = rows.find((row) => row.status === "blocked")
    ?? rows.find((row) => row.status === "published")
    ?? null;
  return pick ? pick.full_generation_id.slice(-12) : null;
}

/**
 * The attempt whose detail shows the "Read this version" control.
 *
 * The control appears only on a PUBLISHED attempt that is not the version the
 * console is already reading, and none of the blocked-attempt captures can
 * show it. `workspace-mixed` is the fixture with eighteen published versions,
 * which is the case the control exists for (story S16); one capture there is
 * enough for the operator to judge it, and doing this on every fixture would
 * add four more pages saying the same thing.
 */
export function readableDetailAttemptFor(name) {
  if (name !== "workspace-mixed") return null;
  const fixture = JSON.parse(
    readFileSync(join(DESIGN_DIR, "fixtures", `${name}.json`), "utf8"),
  );
  const rows = fixture.reads["/viz/api/console/generations"]?.payload?.generations ?? [];
  const pick = rows.find((row) => row.status === "published" && !row.is_current) ?? null;
  return pick ? pick.full_generation_id.slice(-12) : null;
}

/**
 * The attempt whose detail shows one row per enricher and per view.
 *
 * Only the attempt the enricher and view diagnostics reads name: on that one
 * they describe THIS run, and story S19's operator can see which enricher was
 * withheld and which view failed. `workspace-mixed` is the fixture with three
 * enrichers and four views.
 */
export function currentDetailAttemptFor(name) {
  const fixture = JSON.parse(
    readFileSync(join(DESIGN_DIR, "fixtures", `${name}.json`), "utf8"),
  );
  const enrichers = fixture.reads["/viz/api/console/enricher-diagnostics"]?.payload ?? {};
  const rows = fixture.reads["/viz/api/console/generations"]?.payload?.generations ?? [];
  const id = enrichers.generation_id;
  if (!id || (enrichers.enrichers ?? []).length === 0) return null;
  const match = rows.find((row) => row.full_generation_id === id);
  return match ? match.full_generation_id.slice(-12) : null;
}

/**
 * The producers whose Sources detail page is captured for this fixture.
 *
 * The failed one where one exists -- that is the page the view exists for
 * (story S2) -- otherwise the healthy one on all-clear and a never-run one on
 * empty. workspace-mixed gets two: its failed view and its withheld enricher
 * (story S19), under `sources-detail` and `sources-detail-enricher`. Read
 * from the fixture rather than pinned here, and only producers the fixture
 * defines a detail read for.
 */
export function sourcesDetailTargetsFor(name) {
  const fixture = JSON.parse(
    readFileSync(join(DESIGN_DIR, "fixtures", `${name}.json`), "utf8"),
  );
  const reads = fixture.reads;
  const list = (kind, key) => reads[`/viz/api/console/${kind}-diagnostics`]?.payload?.[key] ?? [];
  const hasDetail = (kind, producer) => {
    // A source's page reads the producers route, as the console does; the
    // fixtures serve no `console/source-detail` answer (issue #2118).
    if (kind === "source") return Boolean(reads[`/viz/api/console/producers?name=${encodeURIComponent(producer)}&run_limit=20`]);
    if (kind === "enricher") return Boolean(reads[`/viz/api/console/enricher-diagnostics?mode=detail&name=${encodeURIComponent(producer)}&run_limit=20`]);
    return Boolean(reads[`/viz/api/console/view-diagnostics?name=${encodeURIComponent(producer)}&run_limit=20`]);
  };
  const pick = (kind, key, state) => list(kind, key)
    .find((row) => row.workflow.state === state && hasDetail(kind, row.name))?.name ?? null;
  const targets = [];
  const failed = pick("view", "views", "failed") ?? pick("source", "sources", "failed");
  if (failed) targets.push(["sources-detail", failed]);
  const withheld = pick("enricher", "enrichers", "withheld");
  if (withheld) targets.push(["sources-detail-enricher", withheld]);
  if (!failed) {
    const other = pick("source", "sources", "updated") ?? pick("source", "sources", "never run");
    if (other) targets.push(["sources-detail", other]);
  }
  return targets;
}

/**
 * The Explore pages captured for this fixture beyond the landing, at 1440.
 *
 * Read from the fixture, never pinned here: the results page for the one
 * non-empty query the fixture defines a search read for; the featured
 * record's page, opened on Related where that record's related rows are
 * capped (story S13) and on Fields otherwise (S15); and, for every other
 * record whose retained payload the policy refused while its stored row is
 * present, that record's page on Raw (S14). `empty` defines no search read
 * and no record, so it gets none of these.
 */
export function exploreTargetsFor(name) {
  const fixture = JSON.parse(
    readFileSync(join(DESIGN_DIR, "fixtures", `${name}.json`), "utf8"),
  );
  const reads = fixture.reads;
  const targets = [];
  // No published version: the landing already says there is nothing to
  // search, and a query changes nothing (story S20).
  if (!reads["/viz/api/console/versions"]?.payload?.latest) return targets;
  const version = reads["/viz/api/console/versions"].payload.latest;
  // The results page: the query with the most matches among the plain
  // reads (no filter, first page); then, where the fixture defines them, its
  // second page, one type checked, an empty query, and the syntax help open.
  const plain = Object.keys(reads)
    .filter((key) => key.startsWith("/viz/api/console/records/search?"))
    .map((key) => ({ key, params: new URLSearchParams(key.slice(key.indexOf("?") + 1)), payload: reads[key].payload }))
    .filter(({ params }) => params.get("q") && params.get("version") && params.get("offset") === "0"
      && !params.has("type") && !params.has("source") && !params.has("sort") && !params.has("since"));
  const richest = [...plain].sort((a, b) => (b.payload.total_count ?? 0) - (a.payload.total_count ?? 0))[0];
  if (richest) {
    const q = richest.payload.query;
    targets.push(["explore-results", { q }]);
    const has = (extra) => Boolean(reads[readKey("/viz/api/console/records/search", { version, q, offset: 0, limit: 20, ...extra })]);
    if (has({ offset: 20 })) targets.push(["explore-results-page2", { q, offset: "20" }]);
    const typed = richest.payload.facets?.types?.[0]?.name;
    if (typed && has({ type: typed })) targets.push(["explore-results-filtered", { q, type: typed }]);
    const empty = plain.find((entry) => entry.payload.total_count === 0);
    if (empty) targets.push(["explore-results-empty", { q: empty.payload.query }]);
    // The syntax panel once, on the fixture whose results page has pages.
    if (has({ offset: 20 })) targets.push(["explore-query-help", { q, help: "1" }]);
  }
  const featured = fixture.expect.featured_record;
  const detailFor = (id) => Object.keys(reads)
    .filter((key) => key.startsWith("/viz/api/console/records/detail?"))
    .map((key) => reads[key].payload)
    .find((payload) => payload.found && payload.node_id === id);
  if (featured) {
    const payload = detailFor(featured);
    const capped = Boolean(payload && (payload.related.outgoing_capped || payload.related.incoming_capped));
    targets.push(["explore-record", capped ? { record: featured, tab: "related" } : { record: featured }]);
  }
  // The record page's other tabs, one capture per state the fixture carries,
  // each derived from the artifact and raw reads rather than pinned here:
  //   explore-record-content-<pdf|md|code|html|image>: a rendered artifact,
  //     named by its format, on Content with both panes;
  //   explore-record-content-refused / -missing: an artifact policy refuses,
  //     or one the disk no longer holds;
  //   explore-record-chunk: a document_chunk's page, "part of" first;
  //   explore-record-provenance: the featured record's Provenance tab;
  //   explore-record-raw: a refused retained payload on Raw (S14);
  //   explore-record-raw-large: a payload past the inline limit on Raw.
  const FORMAT_NAMES = { pdf: "pdf", markdown: "md", code: "code", html: "html", image: "image" };
  const seenContent = new Set();
  const seen = new Set();
  const ids = Object.keys(reads)
    .filter((key) => key.startsWith("/viz/api/console/records/detail?") && key.includes("version="))
    .map((key) => reads[key].payload)
    .filter((payload) => payload.found)
    .map((payload) => payload.node_id);
  if (featured) targets.push(["explore-record-provenance", { record: featured, tab: "provenance" }]);
  for (const id of ids) {
    if (seen.has(id)) continue;
    seen.add(id);
    const params = { id, version };
    const detail = reads[readKey("/viz/api/console/records/detail", params)].payload;
    const artifact = reads[readKey("/viz/api/console/records/artifact", params)]?.payload;
    const raw = reads[readKey("/viz/api/console/records/raw", params)]?.payload;
    if (detail.type === "document_chunk" && !seenContent.has("chunk")) {
      seenContent.add("chunk");
      targets.push(["explore-record-chunk", { record: id, tab: "content" }]);
    }
    if (artifact && artifact.exists) {
      const name = FORMAT_NAMES[artifact.format] ?? null;
      if (name && !seenContent.has(name)) {
        seenContent.add(name);
        targets.push([`explore-record-content-${name}`, { record: id, tab: "content" }]);
      }
    } else if (artifact && artifact.reason === "policy_refused" && !seenContent.has("refused")) {
      seenContent.add("refused");
      targets.push(["explore-record-content-refused", { record: id, tab: "content" }]);
    } else if (artifact && artifact.reason === "file_missing" && !seenContent.has("missing")) {
      seenContent.add("missing");
      targets.push(["explore-record-content-missing", { record: id, tab: "content" }]);
    }
    if (raw && raw.refusal && raw.refusal.reason === "policy_refused" && raw.row && !seenContent.has("raw")) {
      seenContent.add("raw");
      targets.push(["explore-record-raw", { record: id, tab: "raw" }]);
    }
    if (raw && raw.payload && raw.payload.inline === null && !seenContent.has("raw-large")) {
      seenContent.add("raw-large");
      targets.push(["explore-record-raw-large", { record: id, tab: "raw" }]);
    }
  }
  return targets;
}

/** The lookup key a read becomes; the same function as generate-fixtures.mjs
 *  and fixtureClient.ts hold. */
export function readKey(route, params = {}) {
  const pairs = Object.entries(params)
    .filter(([name, value]) => value !== null && value !== undefined && name !== "refresh")
    .map(([name, value]) => [name, String(value)])
    .sort((a, b) => a[0].localeCompare(b[0]));
  if (pairs.length === 0) return route;
  return `${route}?${pairs.map(([n, v]) => `${n}=${encodeURIComponent(v)}`).join("&")}`;
}

export function designExploreUrl(base, fixture, params) {
  const query = new URLSearchParams({ fixture, ...params });
  return `${base}#explore?${query.toString()}`;
}

/** Open one Explore page (results or a record) and wait for it. Same network
 *  guarantee as `openDetail`. */
export async function openExplore(page, base, fixture, params) {
  const apiRequests = [];
  const listener = (request) => {
    if (request.url().includes("/viz/api/")) apiRequests.push(request.url());
  };
  page.on("request", listener);
  await page.goto(designExploreUrl(base, fixture, params), { waitUntil: "load" });
  await page.reload({ waitUntil: "load" });
  await page.waitForSelector(`[data-testid="design-harness"][data-fixture="${fixture}"]`, {
    timeout: 30000,
  });
  await page.waitForSelector(
    '[data-testid="explore-results"], [data-testid="explore-list"], '
    + '[data-testid="console-explore-record"], [data-testid="design-read-failed"]',
    { timeout: 30000 },
  );
  // A Content tab's viewer draws after its lazy chunk and the artifact have
  // loaded: wait until no rendered pane is still pending, so a capture never
  // shows "rendering…" in place of the artifact. A failed viewer is left for
  // the caller to refuse (`viewerStates`).
  await page.waitForFunction(
    () => ![...document.querySelectorAll('[data-testid="explore-content-rendered"]')]
      .some((pane) => pane.getAttribute("data-viewer-state") === "pending"),
    null,
    { timeout: 30000 },
  ).catch(() => {});
  await page.waitForLoadState("networkidle").catch(() => {});
  page.off("request", listener);
  return apiRequests;
}

/** Every rendered pane's viewer and state on the page: `[{viewer, state}]`.
 *  Empty when the page has no Content tab open. */
export async function viewerStates(page) {
  return page.evaluate(() => [...document.querySelectorAll('[data-testid="explore-content-rendered"]')]
    .map((pane) => ({
      viewer: pane.getAttribute("data-viewer"),
      state: pane.getAttribute("data-viewer-state"),
      reason: pane.querySelector('[data-testid="explore-content-failed"]')?.textContent ?? null,
    })));
}

/** Requests the page made to any origin but the dev server's own. The
 *  Content tab's frame carries a remote script on purpose; with scripts off
 *  it must never be fetched. */
export function foreignRequestListener(page, base) {
  const origin = new URL(base).origin;
  const foreign = [];
  const listener = (request) => {
    if (!request.url().startsWith(origin) && !request.url().startsWith("data:") && !request.url().startsWith("blob:")) {
      foreign.push(request.url());
    }
  };
  page.on("request", listener);
  return { foreign, stop: () => page.off("request", listener) };
}

/** Open one producer's page and wait for it. Same network guarantee as
 *  `openDetail`. */
export async function openSourcesDetail(page, base, fixture, name) {
  const apiRequests = [];
  const listener = (request) => {
    if (request.url().includes("/viz/api/")) apiRequests.push(request.url());
  };
  page.on("request", listener);
  await page.goto(designDetailUrl(base, "Sources", fixture, name), { waitUntil: "load" });
  await page.reload({ waitUntil: "load" });
  await page.waitForSelector(`[data-testid="design-harness"][data-fixture="${fixture}"]`, {
    timeout: 30000,
  });
  await page.waitForSelector(
    '[data-testid="console-sources-detail"], [data-testid="design-read-failed"]',
    { timeout: 30000 },
  );
  await page.waitForLoadState("networkidle").catch(() => {});
  page.off("request", listener);
  return apiRequests;
}

export function designDetailUrl(base, view, fixture, detail) {
  return `${base}#${view.toLowerCase()}/${encodeURIComponent(detail)}`
    + `?fixture=${encodeURIComponent(fixture)}`;
}

/**
 * Open one attempt's detail page and wait for it.
 *
 * Same network guarantee as `openView`: the harness reads from the fixture and
 * a request here would mean the page fell through to the network.
 */
export async function openDetail(page, base, view, fixture, detail) {
  const apiRequests = [];
  const listener = (request) => {
    if (request.url().includes("/viz/api/")) apiRequests.push(request.url());
  };
  page.on("request", listener);
  await page.goto(designDetailUrl(base, view, fixture, detail), { waitUntil: "load" });
  await page.reload({ waitUntil: "load" });
  await page.waitForSelector(`[data-testid="design-harness"][data-fixture="${fixture}"]`, {
    timeout: 30000,
  });
  await page.waitForSelector('[data-testid="console-updates-detail"]', { timeout: 30000 });
  page.off("request", listener);
  return apiRequests;
}

export function fixtureExpect(name) {
  return JSON.parse(readFileSync(join(DESIGN_DIR, "fixtures", `${name}.json`), "utf8")).expect;
}

async function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

/** Start the Vite dev server and resolve once it prints its URL. Rejects
 *  rather than falling back to a guessed URL: a script that screenshotted a
 *  connection-refused page would produce 48 identical white PNGs and call it
 *  evidence. */
export async function startDevServer() {
  const port = await freePort();
  const child = spawn(
    "npx",
    [
      "vite", "--config", "vite.operator-ui.config.ts",
      "--host", "127.0.0.1", "--port", String(port), "--strictPort",
    ],
    { cwd: REPO_ROOT, stdio: ["ignore", "pipe", "pipe"] },
  );
  const log = [];
  child.stdout.on("data", (chunk) => log.push(String(chunk)));
  child.stderr.on("data", (chunk) => log.push(String(chunk)));

  const url = `http://127.0.0.1:${port}/`;
  const deadline = Date.now() + 60000;
  for (;;) {
    if (child.exitCode !== null) {
      throw new Error(`the dev server exited ${child.exitCode}:\n${log.join("")}`);
    }
    try {
      const response = await fetch(url);
      if (response.ok) break;
    } catch {
      // not listening yet
    }
    if (Date.now() > deadline) {
      child.kill("SIGTERM");
      throw new Error(`the dev server did not answer on ${url} within 60s:\n${log.join("")}`);
    }
    await new Promise((resolve) => { setTimeout(resolve, 250); });
  }
  return { url, stop: () => child.kill("SIGTERM"), log };
}

export function designUrl(base, view, fixture) {
  return `${base}#${view.toLowerCase()}?fixture=${encodeURIComponent(fixture)}`;
}

/**
 * Open one view of one fixture and wait for it to render.
 *
 * Returns the network requests the page made to the console's API surface.
 * They must be empty: the harness reads from the fixture, and a request here
 * would mean a view fell through to the network.
 */
export async function openView(page, base, view, fixture) {
  const apiRequests = [];
  const listener = (request) => {
    if (request.url().includes("/viz/api/")) apiRequests.push(request.url());
  };
  page.on("request", listener);
  await page.goto(designUrl(base, view, fixture), { waitUntil: "load" });
  // The hash alone does not reload the app between views, so force the route
  // to re-resolve and then wait for the harness root of THIS fixture.
  await page.reload({ waitUntil: "load" });
  await page.waitForSelector(`[data-testid="design-harness"][data-fixture="${fixture}"]`, {
    timeout: 30000,
  });
  // A view is rendered once its main element or a named failure is in the
  // DOM; the "Reading this deployment…" line is not a view.
  await page.waitForSelector(
    '[data-testid^="console-"], [data-testid="design-read-failed"]',
    { timeout: 30000 },
  );
  await page.waitForLoadState("networkidle").catch(() => {});
  page.off("request", listener);
  return apiRequests;
}
