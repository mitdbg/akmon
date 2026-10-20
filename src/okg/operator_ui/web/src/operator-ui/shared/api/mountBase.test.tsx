import { readFileSync } from "node:fs";
import { fileURLToPath, URL } from "node:url";
import { describe, expect, it } from "vitest";
import {
  joinMountPath,
  MountBaseError,
  normalizeMountBase,
  OPERATOR_UI_BASE_ENV,
  resolveOperatorUiBase,
  ROOT_MOUNT_BASE,
} from "@operator-ui/shared/api/mountBase";

// repo root: .../src/okg/operator_ui/web/src/operator-ui/shared/api -> up 8
const VITE_CONFIG = fileURLToPath(
  new URL("../../../../../../../../vite.operator-ui.config.ts", import.meta.url),
);

describe("operator console mount base", () => {
  it("prefix input is read from configuration not hardcoded", () => {
    // The prefix comes from the environment, so ONE source produces both the
    // root build and the prefixed build.
    expect(resolveOperatorUiBase({ [OPERATOR_UI_BASE_ENV]: "/console/" })).toBe("/console/");
    expect(resolveOperatorUiBase({ [OPERATOR_UI_BASE_ENV]: "/ops/akmon/" })).toBe("/ops/akmon/");
    expect(resolveOperatorUiBase({})).toBe(ROOT_MOUNT_BASE);

    // And the build config binds Vite's `base` to that resolver rather than to
    // a literal, so no mount is baked into the repo. Comments are stripped
    // first: prose naming an example mount is documentation, not a hardcode.
    const config = readFileSync(VITE_CONFIG, "utf8");
    const code = config
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .replace(/^\s*\/\/.*$/gm, "");
    expect(code).toContain("resolveOperatorUiBase(process.env)");
    expect(code).toContain("base: mountBase,");
    expect(code).not.toMatch(/base:\s*["'`]\//);
    expect(code).not.toMatch(/["'`]\/console/);
  });

  it("accepts a prefix with or without its trailing slash", () => {
    expect(normalizeMountBase("/console")).toBe("/console/");
    expect(normalizeMountBase("/console/")).toBe("/console/");
    expect(normalizeMountBase("/")).toBe("/");
  });

  it("a malformed prefix fails loudly instead of reverting to the origin root", () => {
    // Every one of these used to be a candidate for a silent `?? "/"`, which is
    // the defect: a console that quietly believes it owns / sends its reads to
    // whatever really does.
    for (const bad of [
      "",
      "   ",
      "console/",
      "//chat.example.com/",
      "https://cdn.example.com/console/",
      "/console/?x=1",
      "/console/#/health",
      "/console//nested/",
      "/console/../etc/",
      "/con sole/",
      " /console/",
      undefined,
      null,
      42,
    ]) {
      expect(() => normalizeMountBase(bad), `expected ${JSON.stringify(bad)} to throw`)
        .toThrow(MountBaseError);
    }
  });

  it("an unset prefix is the declared root build, a set-but-empty one is a misconfiguration", () => {
    expect(resolveOperatorUiBase({})).toBe("/");
    expect(() => resolveOperatorUiBase({ [OPERATOR_UI_BASE_ENV]: "" })).toThrow(MountBaseError);
  });

  it("joins the console's own paths under its mount and leaves the root mount alone", () => {
    expect(joinMountPath("/console/", "/viz/api/console/health"))
      .toBe("/console/viz/api/console/health");
    expect(joinMountPath("/console", "/viz/api/console/health"))
      .toBe("/console/viz/api/console/health");
    expect(joinMountPath("/", "/viz/api/console/health"))
      .toBe("/viz/api/console/health");
  });
});
