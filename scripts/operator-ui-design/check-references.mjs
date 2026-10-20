#!/usr/bin/env node
// Check the pinned visual references.
//
//   node scripts/operator-ui-design/check-references.mjs
//
// A reference is the first gate in docs/runbooks/agents/ui.md: no visual
// implementation starts without one. This checks the two things a script can
// check: that each of the five reference files exists, and that each names its
// reviewer. The captures a reference describes are kept outside the
// repository (each file's first line says where), so nothing here resolves a
// screenshot path.
//
// It does NOT check whether the reference is a good one. That is the operator's
// job and no script replaces it.
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(HERE, "..", "..");
const REF_DIR = join(
  REPO_ROOT, "pact", "changes", "operator-ui-redesign-mocks", "ui", "references",
);

const REQUIRED = ["home", "updates", "sources", "explore", "top-bar"];
const REVIEWER_RE = /\|\s*Reviewer\s*\|\s*\S[^|]*\|/;

function main() {
  if (!existsSync(REF_DIR)) {
    // The references are kept in a PACT change record; a checkout without it
    // (the public copy) has none to check, which is not "not written yet".
    console.error(
      `REFUSED: the reference directory ${REF_DIR} (the operator-ui-redesign-mocks ` +
        "PACT change record) is not in this checkout",
    );
    return 2;
  }
  const failures = [];
  const report = [];

  for (const name of REQUIRED) {
    const path = join(REF_DIR, `${name}.md`);
    if (!existsSync(path)) {
      failures.push(`MISSING: ${name}.md has not been written`);
      continue;
    }
    const text = readFileSync(path, "utf8");
    const reviewer = text.match(REVIEWER_RE);
    if (!reviewer) {
      failures.push(`${name}.md does not name a reviewer`);
      continue;
    }
    report.push(`${name}.md: present, reviewer row ${reviewer[0].trim()}`);
  }

  for (const line of report) console.log(`  ${line}`);
  if (failures.length) {
    for (const failure of failures) console.error(`REFUSED: ${failure}`);
    console.error(`${failures.length} problem(s) in the pinned references`);
    return 1;
  }
  console.log(`ok: ${REQUIRED.length} references present, each names its reviewer`);
  return 0;
}

process.exit(main());
