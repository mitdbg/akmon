#!/usr/bin/env node
/**
 * Hold the operator UI's first-load bundle to a declared ceiling.
 *
 * The entry bundle is what every view pays before it renders anything, so a
 * library added to a shared module — rather than to one lazy route — shows up
 * here. PACT operator-ui-search-view relies on that: Ant Design loads with the
 * Search route, and this check is what keeps it out of the entry.
 *
 * The ceilings live in `bundle-budgets.json` beside this file, measured, with
 * the date and the change that set them. This replaces the bundle arm of the
 * performance-readiness verifier, which PACT operator-ui-remove-four-views
 * removed with the views it checked.
 *
 * A second kind of budget covers what a route loads LATER: `lazy_gzip_bytes`
 * names a set of lazily imported modules and a ceiling for everything they
 * pull in that the first load did not already pay for. Explore's Content tab
 * viewers (pdf.js, react-markdown, shiki) are held this way, from the design
 * build (`npm run operator-ui:build:design`), because the console's views
 * reach the production bundle only when the harness is built in. The
 * closure is computed from Vite's manifest, so a library that moves from a
 * viewer into a shared module shows up here as a drop and in `home_js` as
 * the rise it is.
 *
 * Usage: `npm run operator-ui:build && npm run operator-ui:build:design &&
 * node <this file>`; it reads both builds and exits non-zero on a breach or
 * on a missing build.
 */
import { gzipSync } from "node:zlib";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "../../../../..");
const budgets = JSON.parse(readFileSync(join(here, "bundle-budgets.json"), "utf8"));
const distDir = resolve(repoRoot, budgets.dist);
const assetsDir = join(distDir, "assets");

function fail(message) {
  console.error(`operator-ui bundle budget: ${message}`);
  process.exit(1);
}

let indexHtml;
try {
  indexHtml = readFileSync(join(distDir, "index.html"), "utf8");
} catch {
  fail(`no build at ${budgets.dist} — run \`npm run operator-ui:build\` first`);
}

let assets;
try {
  assets = readdirSync(assetsDir);
} catch {
  fail(`no assets directory at ${budgets.dist}/assets`);
}

// The entry files are the ones index.html itself loads. Naming them by hash
// would rot on every build; naming them by pattern alone could pick up a lazy
// chunk that happens to match.
function entryFile(extension) {
  const named = assets.filter(
    (name) => name.endsWith(extension) && indexHtml.includes(name),
  );
  if (named.length !== 1) {
    fail(
      `expected exactly one entry ${extension} in index.html, found ${named.length}`
      + (named.length ? `: ${named.join(", ")}` : ""),
    );
  }
  return named[0];
}

const rows = [];
let breached = false;
for (const [key, budget] of Object.entries(budgets.entry_gzip_bytes)) {
  const file = entryFile(budget.extension);
  const gzipBytes = gzipSync(readFileSync(join(assetsDir, file)), { level: 9 }).length;
  const over = gzipBytes > budget.ceiling;
  breached = breached || over;
  rows.push({ key, file, gzipBytes, ceiling: budget.ceiling, verdict: over ? "OVER" : "ok" });
}

for (const row of rows) {
  console.log(
    `${row.verdict === "ok" ? "ok  " : "OVER"} ${row.key}: ${row.gzipBytes} gzip bytes `
    + `(ceiling ${row.ceiling}) — ${row.file}`,
  );
}

// --- lazy closures --------------------------------------------------------

function gzipOf(path) {
  return gzipSync(readFileSync(path), { level: 9 }).length;
}

/** Every chunk file reachable from `keys` through static imports, per the
 *  manifest. Dynamic imports are not followed: they are paid later, on
 *  their own. */
function staticClosure(manifest, keys) {
  const seen = new Set();
  const files = new Set();
  const stack = [...keys];
  while (stack.length) {
    const key = stack.pop();
    if (seen.has(key)) continue;
    seen.add(key);
    const entry = manifest[key];
    if (!entry) fail(`the manifest names no module ${key}`);
    files.add(entry.file);
    for (const imported of entry.imports ?? []) stack.push(imported);
  }
  return files;
}

for (const [key, budget] of Object.entries(budgets.lazy_gzip_bytes ?? {})) {
  const lazyDist = resolve(repoRoot, budget.dist);
  let manifest;
  try {
    manifest = JSON.parse(readFileSync(join(lazyDist, ".vite", "manifest.json"), "utf8"));
  } catch {
    fail(`no manifest at ${budget.dist}/.vite/manifest.json — run \`${budget.build}\` first`);
  }
  if (budget.asset) {
    // One emitted asset, matched by its name prefix: the PDF worker.
    const names = Object.values(manifest).map((entry) => entry.file)
      .filter((file) => file.startsWith(`assets/${budget.asset}`));
    if (names.length !== 1) {
      fail(`expected exactly one asset named ${budget.asset}* in ${budget.dist}, found ${names.length}`);
    }
    const gzipBytes = gzipOf(join(lazyDist, names[0]));
    const over = gzipBytes > budget.ceiling;
    breached = breached || over;
    console.log(
      `${over ? "OVER" : "ok  "} ${key}: ${gzipBytes} gzip bytes (ceiling ${budget.ceiling}) — ${names[0]}`,
    );
    continue;
  }
  const paid = staticClosure(manifest, budget.already_paid_by);
  const closure = staticClosure(manifest, budget.entries);
  const own = [...closure].filter((file) => !paid.has(file));
  let gzipBytes = 0;
  for (const file of own) gzipBytes += gzipOf(join(lazyDist, file));
  const over = gzipBytes > budget.ceiling;
  breached = breached || over;
  console.log(
    `${over ? "OVER" : "ok  "} ${key}: ${gzipBytes} gzip bytes (ceiling ${budget.ceiling}) — `
    + `${own.length} chunk(s): ${own.map((file) => file.replace(/^assets\//, "")).join(", ")}`,
  );
}

if (breached) {
  fail(
    "a bundle grew past its ceiling. Load the new code with the route that "
    + "needs it, or re-measure the ceiling in bundle-budgets.json with the reason.",
  );
}
