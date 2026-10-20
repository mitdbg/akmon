/**
 * The operator-UI layout check, run as part of the unit suite.
 *
 * `audit/layout-check.mjs` holds the source layout rules (the allowed top-level
 * directories, the console module's sub-directories, no route importing
 * another route, shared/ not importing app/ or routes/). Running it from here
 * puts it in the hosted operator-ui-unit-contract job, which runs this suite;
 * before issue #2177 nothing hosted ran it and it sat red on dev unnoticed.
 */
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const script = fileURLToPath(new URL("../../../audit/layout-check.mjs", import.meta.url));

function runCheck(root?: string) {
  const args = root === undefined ? [script] : [script, root];
  const result = spawnSync(process.execPath, args, { encoding: "utf8" });
  return { status: result.status, output: `${result.stdout}${result.stderr}` };
}

const scratch: string[] = [];

afterEach(() => {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
});

/** A minimal tree that satisfies every rule; each test then breaks one. */
function conformingTree(): string {
  const root = mkdtempSync(join(tmpdir(), "operator-ui-layout-"));
  scratch.push(root);
  writeFileSync(join(root, "index.ts"), "export {};\n");
  for (const dir of ["app", "design-harness", "shared", "styles", "routes/home"]) {
    mkdirSync(join(root, dir), { recursive: true });
  }
  writeFileSync(join(root, "routes/home/HomeView.tsx"), "export {};\n");
  for (const dir of ["views", "model", "api", "components", "viewers", "styles", "__fixtures__"]) {
    mkdirSync(join(root, "console", dir), { recursive: true });
  }
  writeFileSync(join(root, "console/wording.test.ts"), "export {};\n");
  return root;
}

describe("operator-UI layout check", { timeout: 30_000 }, () => {
  it("passes on the source tree as it is", () => {
    const { status, output } = runCheck();
    expect(output).toContain("operator-ui layout check passed");
    expect(status).toBe(0);
  });

  it("passes on a conforming tree", () => {
    const { status, output } = runCheck(conformingTree());
    expect(output).toContain("operator-ui layout check passed (1 route files)");
    expect(status).toBe(0);
  });

  it("refuses a stray top-level directory", () => {
    const root = conformingTree();
    mkdirSync(join(root, "widgets"));
    const { status, output } = runCheck(root);
    expect(output).toContain("unexpected root directory: widgets");
    expect(status).toBe(1);
  });

  it("refuses a console directory outside the module layout", () => {
    const root = conformingTree();
    mkdirSync(join(root, "console/helpers"));
    const { status, output } = runCheck(root);
    expect(output).toContain("unexpected console directory: console/helpers");
    expect(status).toBe(1);
  });

  it("refuses shipping code at the console top level", () => {
    const root = conformingTree();
    writeFileSync(join(root, "console/Panel.tsx"), "export {};\n");
    const { status, output } = runCheck(root);
    expect(output).toContain("unexpected console file: console/Panel.tsx");
    expect(status).toBe(1);
  });

  it("ignores dot-files such as a macOS .DS_Store", () => {
    const root = conformingTree();
    writeFileSync(join(root, ".DS_Store"), "");
    writeFileSync(join(root, "console/.DS_Store"), "");
    const { status, output } = runCheck(root);
    expect(output).toContain("operator-ui layout check passed");
    expect(status).toBe(0);
  });

  it("names a missing console directory instead of crashing", () => {
    const root = conformingTree();
    rmSync(join(root, "console"), { recursive: true });
    const { status, output } = runCheck(root);
    expect(output).toContain("missing directory: console");
    expect(output).not.toContain("ENOENT");
    expect(status).toBe(1);
  });

  it("refuses a route that imports another route", () => {
    const root = conformingTree();
    mkdirSync(join(root, "routes/health"));
    writeFileSync(
      join(root, "routes/health/HealthView.tsx"),
      'import { x } from "../home/HomeView";\nexport { x };\n',
    );
    const { status, output } = runCheck(root);
    expect(output).toContain("imports route home");
    expect(status).toBe(1);
  });
});
