#!/usr/bin/env node
// Prove the design harness is absent from the production bundle.
//
//   npm run operator-ui:build
//   node scripts/operator-ui-design/check-bundle-clean.mjs
//
// The harness is gated on `import.meta.env.DEV`, which Vite replaces with the
// literal `false` for production, after which Rollup should drop the dynamic
// import and everything it reaches -- including 1.4 MB of fixture JSON. Should.
// A future edit that reads the flag at runtime, or imports a fixture at module
// scope, would ship all of it and nothing would say so. This script says so.
//
// Fails closed: a missing build directory, an empty one, or a bundle with no
// recognisable application code is a failure, never a pass.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(HERE, "..", "..");
const DIST = join(REPO_ROOT, ".tmp", "operator-ui-dist");

// Strings that exist only in the design tree. Each is checked separately so a
// failure names which one leaked.
const FORBIDDEN = [
  "archiv3-small",
  "workspace-mixed",
  "scale-200",
  "broken-runtime",
  "all-clear",
  "Mock deployment",
  "mock pending:",
  "design-fixture-switcher",
  "okg-design-topbar",
];

// "FixtureReadMissing" is not on the list either: the class lives in
// console/api/readFailures.ts so that LoudFailure, which ships, can
// recognise it without importing the design tree.
//
// "empty" is deliberately NOT on that list: it is an ordinary English word that
// appears in shipped copy, so searching for it would fail on unrelated text.
// The fixture is reached through DESIGN_FIXTURES, and five of its six siblings
// are checked above; none of them can survive alone.

function walk(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) out.push(...walk(path));
    else out.push(path);
  }
  return out;
}

function main() {
  if (process.env.OKG_OPERATOR_UI_DESIGN_MOCKS === "1") {
    console.error(
      "REFUSED: OKG_OPERATOR_UI_DESIGN_MOCKS=1 builds the harness IN on purpose, "
      + "so this build cannot be reported clean. Unset it and rebuild.",
    );
    return 1;
  }
  let files;
  try {
    files = walk(DIST);
  } catch (error) {
    console.error(
      `REFUSED: ${DIST} could not be read (${error.message}). `
      + "Run `npm run operator-ui:build` first; a missing build is not a pass.",
    );
    return 1;
  }

  // Source maps carry the original module text, so a map would report a leak
  // that the served bundle does not have. The served files are what ships.
  const served = files.filter((path) => !path.endsWith(".map"));
  if (served.length === 0) {
    console.error(`REFUSED: ${DIST} contains no served files`);
    return 1;
  }

  const code = served.filter((path) => /\.(js|css|html)$/.test(path));
  if (code.length === 0) {
    console.error(`REFUSED: ${DIST} contains no JavaScript, CSS or HTML`);
    return 1;
  }
  // Sanity: the bundle must contain the shipping app, or we are searching an
  // empty haystack and would report clean for the wrong reason. The anchor is
  // the console's root class, which ConsoleRoute renders on every page. It was
  // `okg-home`, the old Home page's class, until PACT
  // console-default-remove-old-views deleted that page; the class survived
  // only in dead CSS selectors, so it no longer proved the app was built.
  const ANCHOR = "okg-console-live";
  const anchor = code
    .filter((path) => path.endsWith(".js"))
    .some((path) => readFileSync(path, "utf8").includes(ANCHOR));
  if (!anchor) {
    console.error(
      `REFUSED: no built JavaScript mentions \`${ANCHOR}\`, so this does not look `
      + "like the operator console's bundle. Refusing to report it clean.",
    );
    return 1;
  }

  // The harness's own chunk would be named after its module. Only the
  // resolver stub (a hundred bytes, "-stub" in its name) may exist.
  const harnessChunks = served
    .map((path) => path.slice(REPO_ROOT.length + 1))
    .filter((path) => /DesignHarness/.test(path));
  if (harnessChunks.length) {
    for (const path of harnessChunks) console.error(`LEAK: ${path} is a design-harness chunk`);
    console.error("the design harness was built into the production bundle");
    return 1;
  }

  const leaks = [];
  for (const path of code) {
    const text = readFileSync(path, "utf8");
    for (const needle of FORBIDDEN) {
      if (text.includes(needle)) {
        leaks.push({ path: path.slice(REPO_ROOT.length + 1), needle });
      }
    }
  }

  if (leaks.length) {
    for (const leak of leaks) {
      console.error(`LEAK: ${leak.path} contains the design-only string "${leak.needle}"`);
    }
    console.error(
      `${leaks.length} design-only string(s) reached the production bundle. The harness `
      + "must stay behind `import.meta.env.DEV`, reached only through a dynamic import.",
    );
    return 1;
  }

  console.log(
    `ok: ${code.length} built file(s) checked against ${FORBIDDEN.length} design-only `
    + "strings; the harness and its fixtures are absent",
  );
  return 0;
}

process.exit(main());
