import { spawnSync } from "node:child_process";
import { readdirSync, readFileSync, rmSync } from "node:fs";
import { fileURLToPath, URL } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
import { OPERATOR_UI_BASE_ENV } from "@operator-ui/shared/api/mountBase";

// Build-SHAPE arms: they prove the emitted bundle addresses itself under the
// declared mount. They do NOT prove the console boots there — that is the live
// browser proof (task.operator-ui-base-aware-build.live-prefix-proof), and this
// file must not be read as closing req.operator-ui-base-aware-build.prefix-aware-assets
// on its own. The measured failure returned HTTP 200 for a blank page, so no
// amount of emitted-URL inspection substitutes for loading it.

// repo root: .../src/okg/operator_ui/web/src/operator-ui/shared/api -> up 8
const repoUrl = new URL("../../../../../../../../", import.meta.url);
const REPO_ROOT = fileURLToPath(repoUrl);
const VITE_BIN = fileURLToPath(new URL("node_modules/vite/bin/vite.js", repoUrl));
const OUT_ROOT = fileURLToPath(new URL(".tmp/operator-ui-mount-build-test", repoUrl));

const MOUNT = "/console/";

function build(outDir: string, mount: string | null) {
  // Start from a definitely-unset mount so an ambient OKG_OPERATOR_UI_BASE in
  // the developer's shell cannot turn the root arm into a second prefixed one.
  const env = { ...process.env };
  delete env[OPERATOR_UI_BASE_ENV];
  if (mount !== null) {
    env[OPERATOR_UI_BASE_ENV] = mount;
  }
  const result = spawnSync(
    process.execPath,
    [VITE_BIN, "build", "--config", "vite.operator-ui.config.ts", "--outDir", outDir],
    {
      cwd: REPO_ROOT,
      encoding: "utf8",
      env,
      timeout: 300_000,
    },
  );
  if (result.status !== 0) {
    throw new Error(
      `vite build failed (status ${result.status})\n${result.stdout ?? ""}\n${result.stderr ?? ""}`,
    );
  }
  return readFileSync(`${outDir}/index.html`, "utf8");
}

/** Every URL the index asks the browser to fetch, minus inline data: URIs. */
function assetUrls(html: string): string[] {
  return [...html.matchAll(/(?:src|href)="([^"]+)"/g)]
    .map((match) => match[1] as string)
    .filter((url) => !url.startsWith("data:"));
}

/** Every javascript chunk one arm emitted, not just the entry. */
function armScripts(arm: string): { name: string; text: string }[] {
  const dir = `${OUT_ROOT}/${arm}/assets`;
  return readdirSync(dir)
    .filter((name) => name.endsWith(".js"))
    .map((name) => ({ name, text: readFileSync(`${dir}/${name}`, "utf8") }));
}

/**
 * The mount as a bundle actually EMITS it: a call carrying the baked-in
 * constant, which the minifier writes as ``h(`/console/`)``.
 *
 * Deliberately NOT a bare search for the mount string. mountBase.ts names
 * `"/console/"` as the example mount inside its rejection message, so that exact
 * double-quoted literal ships in EVERY bundle, root included — a bare search
 * matches prose and is satisfied by a bundle with no mount baked in at all. The
 * live proof's mutation helper documents the same trap and depends on the count
 * below being exactly one, since it rewrites that constant to build the
 * unmounted-reads negative control
 * (evidence/browser-proof/prefix-proof.spec.ts:54-82).
 */
function bakedMountCalls(arm: string, mount: string): string[] {
  const literal = mount.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const callForm = new RegExp(`[A-Za-z_$][\\w$]*\\((["'\`])${literal}\\1\\)`, "g");
  return armScripts(arm).flatMap(({ name, text }) =>
    [...text.matchAll(callForm)].map((match) => `${name}: ${match[0]}`));
}

let rootIndex = "";
let prefixedIndex = "";

describe("operator console build shape under a declared mount", () => {
  beforeAll(() => {
    rmSync(OUT_ROOT, { recursive: true, force: true });
    rootIndex = build(`${OUT_ROOT}/root`, null);
    prefixedIndex = build(`${OUT_ROOT}/prefixed`, MOUNT);
  }, 600_000);

  it("prefixed build emits prefixed asset urls", () => {
    const urls = assetUrls(prefixedIndex);
    expect(urls.length).toBeGreaterThan(0);
    for (const url of urls) {
      expect(url, `${url} is requested outside the console mount`).toMatch(/^\/console\//);
    }
    // The exact failure measured in the read-handshake spike: the index asking
    // the ORIGIN ROOT for /assets/*.
    expect(urls.some((url) => url.startsWith("/assets/"))).toBe(false);
  });

  it("root build emits unchanged asset urls", () => {
    const urls = assetUrls(rootIndex);
    expect(urls.length).toBeGreaterThan(0);
    for (const url of urls) {
      expect(url, `${url} moved off the origin root`).toMatch(/^\/assets\//);
    }
  });

  it("the mount is baked into the bundle so the runtime can address its own API", () => {
    // Vite substitutes import.meta.env.BASE_URL at build time; that constant is
    // what buildApiUrl joins its /viz/api routes onto. Without it the assets
    // would load under the prefix while every read still left the mount.
    //
    // Asserted DIFFERENTIALLY against the root build, which is the control: it
    // comes from the same source and declares no mount, so anything present in
    // BOTH builds cannot be evidence that a mount was baked in.
    //
    // Why that control is not decorative: the root build carries the mount
    // string too, in mountBase.ts's rejection message. An assertion that merely
    // looked for `"/console/"` would pass HERE — on a build that declares no
    // mount — which is what makes the call-form search the real check.
    const rootSource = armScripts("root").map(({ text }) => text).join("\n");
    expect(rootSource, "the prose that makes this arm differential is gone; re-derive the check")
      .toContain(`"${MOUNT}"`);
    expect(
      bakedMountCalls("root", MOUNT),
      "the root build baked in a mount it never declared",
    ).toEqual([]);

    // Exactly one, across every emitted chunk: the live proof rewrites this one
    // constant to build its unmounted-reads negative control and refuses to run
    // on any other count, so the two checks fail together rather than leaving
    // the fast lane green while the browser proof breaks.
    expect(
      bakedMountCalls("prefixed", MOUNT),
      "the prefixed build did not bake in exactly one runtime mount",
    ).toHaveLength(1);
  });
});
