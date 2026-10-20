// Operator-UI source layout check (`npm run operator-ui:layout-check`).
//
// Rules, each matched to the tree as it is:
// - The operator-ui root holds index.ts, vendor.d.ts and six directories:
//   app/, routes/, shared/, styles/, console/ (the operator console) and
//   design-harness/ (the design-mock harness behind operator-ui:build:design).
// - console/ is split by role into views/, model/, api/, components/, viewers/
//   and styles/, plus __fixtures__/ for test data. Its own top level holds
//   test files only; shipping code lives in one of those directories.
// - A route may not import another route.
// - shared/ may not import app/ or routes/.
// Dot-files are skipped, and a missing console/, routes/ or shared/ is a named
// failure.
//
// Not checked: file length. Issue #2177 proposed a 500-line cap for console
// files; the tree does not follow it (views/TopBar.tsx and
// model/updates/recorded.ts, among others, are past 700 lines) and no other
// check enforces it, so encoding it here would only turn this check red.
//
// The operator-UI unit suite runs this script (app/layoutCheck.test.ts), so
// the hosted operator-ui-unit-contract job fails when the layout does.
//
// Usage: node layout-check.mjs [operator-ui root]. The root defaults to the
// operator-ui source tree next to this script, so the check does not depend
// on the working directory.
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = process.argv[2]
  ? resolve(process.argv[2])
  : resolve(dirname(fileURLToPath(import.meta.url)), "../src/operator-ui");
const allowedRootFiles = new Set(["index.ts", "vendor.d.ts"]);
const allowedRootDirs = new Set([
  "app",
  "console",
  "design-harness",
  "routes",
  "shared",
  "styles",
]);
const consoleDirs = new Set([
  "__fixtures__",
  "api",
  "components",
  "model",
  "styles",
  "viewers",
  "views",
]);
const testFile = /\.test\.(ts|tsx)$/;
const failures = [];

// Dot-files (a macOS .DS_Store, for one) are ignored by git (.gitignore) and
// never ship, so they are not layout.
const listed = (dir) => readdirSync(dir).filter((entry) => !entry.startsWith("."));

// The directories the rules below read. A missing one is a named failure, not
// a stack trace, and its rule is skipped.
const present = (name) => {
  if (existsSync(join(root, name)) && statSync(join(root, name)).isDirectory()) return true;
  failures.push(`missing directory: ${name}`);
  return false;
};
const hasConsole = present("console");
const hasRoutes = present("routes");
const hasShared = present("shared");

for (const entry of listed(root)) {
  const path = join(root, entry);
  const stat = statSync(path);
  if (stat.isDirectory()) {
    if (!allowedRootDirs.has(entry)) {
      failures.push(`unexpected root directory: ${entry}`);
    }
  } else if (!allowedRootFiles.has(entry)) {
    failures.push(`unexpected root file: ${entry}`);
  }
}

const consoleRoot = join(root, "console");
for (const entry of hasConsole ? listed(consoleRoot) : []) {
  const stat = statSync(join(consoleRoot, entry));
  if (stat.isDirectory()) {
    if (!consoleDirs.has(entry)) {
      failures.push(
        `unexpected console directory: console/${entry}; use views/, model/, api/, components/, viewers/ or styles/ (__fixtures__/ for test data)`,
      );
    }
  } else if (!testFile.test(entry)) {
    failures.push(
      `unexpected console file: console/${entry}; only tests sit at the console top level`,
    );
  }
}

function walk(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) out.push(...walk(path));
    else out.push(path);
  }
  return out;
}

const routesRoot = join(root, "routes");
const routeFiles = (hasRoutes ? walk(routesRoot) : [])
  .filter((path) => /\.(ts|tsx)$/.test(path))
  .filter((path) => !/\.test\.tsx$/.test(path));

function routeFromSpecifier(file, specifier) {
  if (specifier.startsWith("@operator-ui/routes/")) {
    return specifier.split("/")[2] ?? null;
  }
  if (!specifier.startsWith(".")) return null;

  const relativeTarget = relative(resolve(routesRoot), resolve(dirname(file), specifier));
  if (
    relativeTarget === ""
    || relativeTarget.startsWith("..")
    || relativeTarget.startsWith(`${sep}..`)
    || relativeTarget.startsWith(sep)
  ) {
    return null;
  }
  return relativeTarget.split(sep)[0] ?? null;
}

function importSpecifiers(source) {
  return [
    ...source.matchAll(/\bfrom\s+["']([^"']+)["']/g),
    ...source.matchAll(/\bimport\s+["']([^"']+)["']/g),
  ].map((match) => match[1]);
}

for (const file of routeFiles) {
  const parts = relative(routesRoot, file).split(sep);
  const currentRoute = parts[0];
  const source = readFileSync(file, "utf8");
  const specifiers = importSpecifiers(source);
  for (const specifier of specifiers) {
    const targetRoute = routeFromSpecifier(file, specifier);
    if (targetRoute && targetRoute !== currentRoute) {
      failures.push(
        `${relative(root, file)} imports route ${targetRoute}; move shared code to shared/ or app/`,
      );
    }
  }
}

function sharedBoundaryTarget(file, specifier) {
  if (specifier.startsWith("@operator-ui/app/")) return "app";
  if (specifier.startsWith("@operator-ui/routes/")) return "routes";
  if (!specifier.startsWith(".")) return null;

  const targetFromRoot = relative(resolve(root), resolve(dirname(file), specifier));
  if (
    targetFromRoot === ""
    || targetFromRoot.startsWith("..")
    || targetFromRoot.startsWith(`${sep}..`)
    || targetFromRoot.startsWith(sep)
  ) {
    return null;
  }

  const topLevel = targetFromRoot.split(sep)[0] ?? null;
  return topLevel === "app" || topLevel === "routes" ? topLevel : null;
}

const sharedFiles = (hasShared ? walk(join(root, "shared")) : [])
  .filter((path) => /\.(ts|tsx)$/.test(path))
  .filter((path) => !/\.test\.tsx$/.test(path));

for (const file of sharedFiles) {
  const source = readFileSync(file, "utf8");
  for (const specifier of importSpecifiers(source)) {
    const target = sharedBoundaryTarget(file, specifier);
    if (!target) continue;
    failures.push(
      `${relative(root, file)} imports ${specifier} (${target}/); shared modules must not depend on app/ or routes/`,
    );
  }
}

if (failures.length > 0) {
  console.error("operator-ui layout check failed:");
  for (const failure of failures) console.error(`- ${failure}`);
  process.exit(1);
}

console.log(`operator-ui layout check passed (${routeFiles.length} route files)`);
