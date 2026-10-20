/**
 * The browser half of PACT `operator-ui-deployment-theme`.
 *
 * What is under test is the honesty of the states and the BASE-FIRING RULE,
 * not the happy path. Applying a declared accent is one assertion; the ones
 * that matter are that the absences apply NOTHING (so the built-in palette
 * arrives through the cascade rather than as a value this module chose), that
 * a partial declaration leaves the operator's other properties alone, that a
 * declared theme selects a base of its own polarity instead of landing on a
 * mismatched one, that a cross-polarity choice by the operator drops the
 * overrides WHOLE and says so, and that every write goes through the CSSOM
 * setter instead of a concatenated string.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  applyDeploymentTheme,
  DEPLOYMENT_THEME_ATTRIBUTE,
  DEPLOYMENT_THEME_PROPERTIES,
  deploymentThemeFromPayload,
  resolveDeploymentTheme,
  VIZ_THEME_TOKENS,
  type DeploymentThemePlan,
  type DeploymentThemeState,
} from "@operator-ui/styles/deploymentTheme";
import {
  DEFAULT_FIRING_FOR_POLARITY,
  DEFAULT_THEME,
  THEME_IS_DARK,
  THEMES,
  type Theme,
} from "@operator-ui/styles/theme";
import { deploymentThemeStateForDeployments } from "@operator-ui/app/deployments";

/** `okg-workspace`'s shipped declaration, verbatim — including the surface
    family the first 2026-08-18 operator ruling added and that manifest now
    completes. Kept identical to `deployments/okg-workspace/deployment.yaml`
    so this suite exercises the real declaration, not a convenient one. */
const DECLARED = {
  background: "#121417",
  accent: "#10b981",
  fg: "#eef1f4",
  fg_faint: "#8d98a5",
  border: "#2a3138",
  surface: "#1a1e23",
  rail: "#0b0d10",
};

/** The seven properties `DECLARED` paints, keyed the way the DOM holds them. */
const DECLARED_PROPERTIES = {
  "--okg-color-bg": "#121417",
  "--okg-color-accent": "#10b981",
  "--okg-color-text": "#eef1f4",
  "--okg-color-muted": "#8d98a5",
  "--okg-color-line": "#2a3138",
  "--okg-color-surface": "#1a1e23",
  "--okg-color-rail": "#0b0d10",
};

const DARK_FIRINGS = THEMES.filter((theme) => THEME_IS_DARK[theme]);
const LIGHT_FIRINGS = THEMES.filter((theme) => !THEME_IS_DARK[theme]);

/** A document root stub: the operator-ui suite runs in vitest's `node`
    environment, so `document` is ours to provide. Records the CSSOM calls in
    order, which is how the "never concatenated into CSS text" claim is
    checked — a `cssText` write would show up as an absent setProperty. */
function makeRoot() {
  const properties = new Map<string, string>();
  const attributes: Record<string, string> = {};
  const calls: Array<{ op: "set" | "remove"; name: string; value?: string }> = [];
  return {
    calls,
    attributes,
    properties,
    element: {
      style: {
        setProperty(name: string, value: string) {
          properties.set(name, value);
          calls.push({ op: "set", name, value });
        },
        removeProperty(name: string) {
          properties.delete(name);
          calls.push({ op: "remove", name });
        },
        get cssText(): string {
          throw new Error("the applier must not read or write cssText");
        },
        set cssText(_value: string) {
          throw new Error("the applier must not read or write cssText");
        },
      },
      setAttribute(name: string, value: string) {
        attributes[name] = value;
      },
    },
  };
}

let root: ReturnType<typeof makeRoot>;

beforeEach(() => {
  root = makeRoot();
  vi.stubGlobal("document", { documentElement: root.element });
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function state(
  overrides: Partial<DeploymentThemeState> = {},
): DeploymentThemeState {
  return {
    status: "declared",
    tokens: DECLARED,
    polarity: "dark",
    deploymentName: "themed",
    ...overrides,
  };
}

/** The plan the console would apply for a state and a stored choice. Every
    applier test goes through the resolver rather than hand-building a plan, so
    no test can assert an application the rule would never produce. */
function plan(
  overrides: Partial<DeploymentThemeState> = {},
  storedFiring: Theme | null = null,
): DeploymentThemePlan {
  return resolveDeploymentTheme(state(overrides), storedFiring);
}

describe("the token contract", () => {
  it("is the seven tokens the server serves, in its order", () => {
    // Pinned against `VIZ_THEME_TOKENS` in
    // okg.substrate.deployments.chat_block. A token added on one side without
    // the other fails here and in test_deployment_theme.py.
    expect([...VIZ_THEME_TOKENS]).toEqual([
      "background",
      "accent",
      "fg",
      "fg_faint",
      "border",
      "surface",
      "rail",
    ]);
  });

  it("maps each token onto exactly one distinct existing custom property", () => {
    const properties = VIZ_THEME_TOKENS.map(
      (token) => DEPLOYMENT_THEME_PROPERTIES[token],
    );
    expect(properties).toEqual([
      "--okg-color-bg",
      "--okg-color-accent",
      "--okg-color-text",
      "--okg-color-muted",
      "--okg-color-line",
      "--okg-color-surface",
      "--okg-color-rail",
    ]);
    // One-to-one: two tokens sharing a property would make the later
    // declaration silently win over the earlier one.
    expect(new Set(properties).size).toBe(properties.length);
  });

  it("carries the surface family onto the properties that paint cards and the rail", () => {
    // Asserted by name rather than by position: these two property names are
    // the ones the audit measured as stranded at their light built-in values
    // under a dark declaration. Both exist in styles/tokens.css in all four
    // firings.
    expect(DEPLOYMENT_THEME_PROPERTIES.surface).toBe("--okg-color-surface");
    expect(DEPLOYMENT_THEME_PROPERTIES.rail).toBe("--okg-color-rail");
  });
});

describe("the base firing each polarity selects", () => {
  it("is the console default on the light side and Hephaestus on the dark", () => {
    // The dark half is the choice the second 2026-08-18 ruling required and
    // `styles/theme.ts` documents: Cyclades names Lyceaum as its warm
    // counterpart and Hephaestus names Melanomorpha as its, so the firings
    // pair by temperature and the default light firing's dark partner is
    // Hephaestus.
    expect(DEFAULT_FIRING_FOR_POLARITY.light).toBe(DEFAULT_THEME);
    expect(DEFAULT_FIRING_FOR_POLARITY.dark).toBe("hephaestus");
  });

  it("actually has the polarity it is chosen for", () => {
    // Guards the pair against a later edit to THEME_IS_DARK or to the map:
    // a light firing sitting under `dark` would reintroduce the exact
    // mismatch this whole rule removes.
    expect(THEME_IS_DARK[DEFAULT_FIRING_FOR_POLARITY.dark]).toBe(true);
    expect(THEME_IS_DARK[DEFAULT_FIRING_FOR_POLARITY.light]).toBe(false);
  });

  it("has both polarities represented among the built-in firings", () => {
    expect(DARK_FIRINGS.length).toBeGreaterThan(0);
    expect(LIGHT_FIRINGS.length).toBeGreaterThan(0);
  });
});

describe("resolving a declared theme against the operator's choice", () => {
  it("boots an unchosen browser onto the polarity-matched base, tokens on top", () => {
    // The arm that fixes the audit. Before this rule a dark okg-workspace
    // landed on the LIGHT default and its undeclared ink siblings —
    // --okg-color-strong, --okg-color-link, --okg-color-surface-muted,
    // --okg-color-subtle — stayed light-firing, which is where 98% of the 694
    // residual contrast violations came from.
    const resolved = plan({}, null);

    expect(resolved.firing).toBe("hephaestus");
    expect(resolved.firingFromDeployment).toBe(true);
    expect(resolved.application).toBe("declared");
    expect(resolved.tokens).toEqual(DECLARED);
  });

  it("boots a light declaration onto the light default", () => {
    const resolved = plan(
      { tokens: { background: "#ffffff" }, polarity: "light" },
      null,
    );

    expect(resolved.firing).toBe(DEFAULT_THEME);
    expect(resolved.firingFromDeployment).toBe(true);
    expect(resolved.application).toBe("declared");
  });

  it.each(DARK_FIRINGS)(
    "keeps the operator on %s — a same-polarity choice — and layers the tokens",
    (firing) => {
      // Rule 3. The base only ever had to agree on POLARITY: Melanomorpha is
      // warm brown and Hephaestus cold iron, and a dark declaration is
      // coherent on either, because every ink sibling comes from a palette
      // built for dark ground.
      const resolved = plan({}, firing);

      expect(resolved.firing).toBe(firing);
      expect(resolved.firingFromDeployment).toBe(false);
      expect(resolved.application).toBe("declared");
      expect(resolved.tokens).toEqual(DECLARED);
    },
  );

  it.each(LIGHT_FIRINGS)(
    "suppresses the overrides whole when the operator chose %s against a dark theme",
    (firing) => {
      // Rule 4, and the reason the light-ink-on-dark-ground class is gone
      // rather than reduced: the overrides do not land partially, they do not
      // land at all, and the operator's choice is not overruled either.
      const resolved = plan({}, firing);

      expect(resolved.firing).toBe(firing);
      expect(resolved.application).toBe("suppressed_polarity_mismatch");
      expect(resolved.tokens).toEqual({});
    },
  );

  it.each(DARK_FIRINGS)(
    "suppresses them the other way too — %s against a light theme",
    (firing) => {
      const resolved = plan(
        { tokens: { background: "#ffffff" }, polarity: "light" },
        firing,
      );

      expect(resolved.application).toBe("suppressed_polarity_mismatch");
      expect(resolved.tokens).toEqual({});
    },
  );

  it("never moves the firing for a declaration that names no ground", () => {
    // An accent-only theme has no polarity, so it can be neither matched nor
    // mismatched. Picking a base here would repaint an operator's console
    // over one colour they asked to change.
    for (const firing of [...THEMES, null]) {
      const resolved = plan(
        { tokens: { accent: "#ff8800" }, polarity: null },
        firing,
      );

      expect(resolved.firing).toBe(firing ?? DEFAULT_THEME);
      expect(resolved.firingFromDeployment).toBe(false);
      expect(resolved.application).toBe("declared");
      expect(resolved.tokens).toEqual({ accent: "#ff8800" });
    }
  });

  it.each(["undeclared", "unresolved", "pending"] as const)(
    "leaves the %s state exactly as it was before any of this",
    (status) => {
      // Rule 5: unchanged behaviour. The firing is the operator's choice, or
      // the console default when they have none, and no token lands.
      expect(resolveDeploymentTheme(state({ status, tokens: {}, polarity: null }), null))
        .toMatchObject({
          firing: DEFAULT_THEME,
          firingFromDeployment: false,
          application: status,
          tokens: {},
        });
      expect(
        resolveDeploymentTheme(
          state({ status, tokens: {}, polarity: null }),
          "melanomorpha",
        ),
      ).toMatchObject({ firing: "melanomorpha", application: status, tokens: {} });
    },
  );

  it("ignores a polarity carried by anything but a declared state", () => {
    // Defense in depth against a caller that builds a state by hand: an
    // absence must never select a base firing, whatever fields it carries.
    const resolved = resolveDeploymentTheme(
      state({ status: "unresolved", tokens: DECLARED, polarity: "dark" }),
      null,
    );

    expect(resolved.firing).toBe(DEFAULT_THEME);
    expect(resolved.application).toBe("unresolved");
    expect(resolved.tokens).toEqual({});
  });
});

describe("applying a declared theme", () => {
  it("sets every declared token on the document root", () => {
    applyDeploymentTheme(plan());

    expect(Object.fromEntries(root.properties)).toEqual(DECLARED_PROPERTIES);
    expect(root.attributes[DEPLOYMENT_THEME_ATTRIBUTE]).toBe("declared");
  });

  it("does not touch data-theme — the firing module owns that attribute", () => {
    // The applier writes the seven properties and the marker. Setting
    // `data-theme` here as well would give the console two writers for one
    // attribute, and the picker would be racing this effect.
    applyDeploymentTheme(plan());

    expect(root.attributes["data-theme"]).toBeUndefined();
  });

  it("no longer strands the card and rail grounds under a dark declaration", () => {
    // The regression test for the defect the first ruling closed. Before the
    // widening these two properties kept the LIGHT default firing's values
    // while the declared ink went light, which is where the 1099
    // color-contrast violations came from. Asserted as "not the Cyclades
    // value" as well as "the declared value", so an implementation that wrote
    // the token somewhere else would fail rather than merely look different.
    // `#fbfdfe` is Cyclades' own `--okg-color-surface` in styles/tokens.css.
    applyDeploymentTheme(plan());

    expect(root.properties.get("--okg-color-surface")).toBe("#1a1e23");
    expect(root.properties.get("--okg-color-rail")).toBe("#0b0d10");
    expect(root.properties.get("--okg-color-surface")).not.toBe("#fbfdfe");
  });

  it("writes only through the CSSOM setter, never as CSS text", () => {
    // The manifest-load grammar already refuses a value carrying `;` or
    // `url(`, but that grammar is the safety boundary and this is the defense
    // behind it. The stub throws on any cssText access, so a concatenating
    // implementation fails rather than merely looking different.
    applyDeploymentTheme(plan());

    expect(root.calls.every((call) => call.op === "set")).toBe(true);
    expect(root.calls).toHaveLength(VIZ_THEME_TOKENS.length);
  });

  it("layers a PARTIAL declaration per token, leaving the base firing's rest alone", () => {
    // The scenario arm that matters: a deployment declaring only an accent
    // must not repaint the operator's chosen background, ink or rules. Absent
    // tokens are REMOVED so the cascade supplies the firing's own values.
    applyDeploymentTheme(
      plan({ tokens: { accent: "#ff8800" }, polarity: null, deploymentName: "partial" }),
    );

    expect(Object.fromEntries(root.properties)).toEqual({
      "--okg-color-accent": "#ff8800",
    });
    expect(
      root.calls.filter((call) => call.op === "remove").map((call) => call.name),
    ).toEqual([
      "--okg-color-bg",
      "--okg-color-text",
      "--okg-color-muted",
      "--okg-color-line",
      "--okg-color-surface",
      "--okg-color-rail",
    ]);
  });

  it("drops a previous deployment's overrides rather than leaving them standing", () => {
    applyDeploymentTheme(plan());
    applyDeploymentTheme(
      plan({ tokens: { accent: "#ff8800" }, polarity: null, deploymentName: "partial" }),
    );

    expect([...root.properties.keys()]).toEqual(["--okg-color-accent"]);
  });

  it("is idempotent", () => {
    applyDeploymentTheme(plan());
    const first = Object.fromEntries(root.properties);
    applyDeploymentTheme(plan());

    expect(Object.fromEntries(root.properties)).toEqual(first);
  });
});

describe("a cross-polarity choice suppresses the overrides", () => {
  it("strips a set already standing, rather than leaving it on a mismatched base", () => {
    // The live sequence: boot dark-on-dark with the overrides applied, then
    // the operator picks a light firing. Leaving even one override behind
    // would put declared dark paper or light ink under a light palette, which
    // is the failure being removed.
    applyDeploymentTheme(plan({}, "hephaestus"));
    expect(Object.fromEntries(root.properties)).toEqual(DECLARED_PROPERTIES);

    applyDeploymentTheme(plan({}, "cyclades"));

    expect([...root.properties.keys()]).toEqual([]);
  });

  it("stamps the suppression explicitly, never silently", () => {
    applyDeploymentTheme(plan({}, "cyclades"));

    expect(root.attributes[DEPLOYMENT_THEME_ATTRIBUTE])
      .toBe("suppressed_polarity_mismatch");
  });

  it("says something different from 'this deployment declares no theme'", () => {
    // Both render the pure built-in firing, and they are not the same fact: a
    // reader of the DOM must be able to tell "a declaration was refused a base
    // it agrees with" from "there was no declaration".
    applyDeploymentTheme(plan({}, "cyclades"));
    const suppressed = root.attributes[DEPLOYMENT_THEME_ATTRIBUTE];

    applyDeploymentTheme(plan({ status: "undeclared", tokens: {}, polarity: null }));

    expect(root.attributes[DEPLOYMENT_THEME_ATTRIBUTE]).toBe("undeclared");
    expect(suppressed).not.toBe("undeclared");
  });

  it("sets no console default in place of the dropped overrides", () => {
    applyDeploymentTheme(plan({}, "lyceaum"));

    expect(root.calls.some((call) => call.op === "set")).toBe(false);
  });
});

describe("the absences apply nothing", () => {
  it.each(["undeclared", "unresolved", "pending"] as const)(
    "removes every override in the %s state",
    (status) => {
      applyDeploymentTheme(plan());
      applyDeploymentTheme(
        plan({ status, tokens: {}, polarity: null, deploymentName: null }),
      );

      expect([...root.properties.keys()]).toEqual([]);
      expect(root.attributes[DEPLOYMENT_THEME_ATTRIBUTE]).toBe(status);
    },
  );

  it("never sets a console default in place of a declaration", () => {
    // Asserted as the absence of any set call, because an implementation that
    // filled the built-in palette's values in here would pass every other test
    // in this file while telling the DOM the deployment asked for them.
    applyDeploymentTheme(
      plan({
        status: "undeclared",
        tokens: {},
        polarity: null,
        deploymentName: "bare",
      }),
    );

    expect(root.calls.some((call) => call.op === "set")).toBe(false);
  });

  it("carries tokens ONLY in the declared state, even if a caller supplies them", () => {
    applyDeploymentTheme(
      plan({ status: "unresolved", tokens: DECLARED, deploymentName: "broken" }),
    );

    expect([...root.properties.keys()]).toEqual([]);
  });
});

describe("a value the browser cannot paint", () => {
  it("is applied verbatim and warned about, never repaired", () => {
    // The load grammar admits SHAPE, not membership: `notacolor` is a
    // well-formed CSS identifier. Substituting a default here would be the
    // silent failure this change exists to remove, so the console applies what
    // was served and says the value will not render.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.stubGlobal("CSS", { supports: (_p: string, v: string) => v !== "notacolor" });

    applyDeploymentTheme(
      plan({ tokens: { accent: "notacolor" }, polarity: null, deploymentName: "shapely" }),
    );

    expect(root.properties.get("--okg-color-accent")).toBe("notacolor");
    expect(warn).toHaveBeenCalledTimes(1);
    const message = String(warn.mock.calls[0][0]);
    expect(message).toContain("shapely");
    expect(message).toContain("viz.theme.accent");
    expect(message).toContain("notacolor");
  });

  it("stays quiet for the color forms the grammar admits", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.stubGlobal("CSS", { supports: (_p: string, v: string) => v !== "notacolor" });

    applyDeploymentTheme(plan());

    expect(warn).not.toHaveBeenCalled();
  });

  it("says nothing at all when the overrides were suppressed", () => {
    // No property was written, so there is nothing to warn about. A warning
    // here would name a manifest line that had no effect on this render.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.stubGlobal("CSS", { supports: (_p: string, v: string) => v !== "notacolor" });

    applyDeploymentTheme(
      plan({ tokens: { ...DECLARED, accent: "notacolor" } }, "cyclades"),
    );

    expect(warn).not.toHaveBeenCalled();
  });
});

describe("reading a served payload", () => {
  it("keeps the declared tokens verbatim and the served polarity", () => {
    expect(
      deploymentThemeFromPayload({
        status: "declared",
        tokens: DECLARED,
        polarity: "dark",
      }),
    ).toEqual({ status: "declared", tokens: DECLARED, polarity: "dark" });
  });

  it("reads a light polarity as light", () => {
    expect(
      deploymentThemeFromPayload({
        status: "declared",
        tokens: { background: "#ffffff" },
        polarity: "light",
      }).polarity,
    ).toBe("light");
  });

  it("reads an ABSENT polarity as null, so the firing does not move", () => {
    // A backend predating the base-firing ruling serves `{status, tokens}`.
    // Guessing a polarity from the served token values would be a second
    // implementation of the luminance rule, in a second language, drifting
    // from the one that does the refusing.
    expect(
      deploymentThemeFromPayload({ status: "declared", tokens: DECLARED }).polarity,
    ).toBeNull();
  });

  it("reads an unrecognized polarity as null rather than guessing one", () => {
    for (const served of ["Dark", "auto", "", 0, true, null, {}]) {
      expect(
        deploymentThemeFromPayload({
          status: "declared",
          tokens: DECLARED,
          polarity: served as unknown as string,
        }).polarity,
      ).toBeNull();
    }
  });

  it("distinguishes undeclared from unresolved", () => {
    expect(deploymentThemeFromPayload({ status: "undeclared", tokens: {} }))
      .toEqual({ status: "undeclared", tokens: {}, polarity: null });
    expect(deploymentThemeFromPayload({ status: "unresolved", tokens: {} }))
      .toEqual({ status: "unresolved", tokens: {}, polarity: null });
  });

  it("carries no polarity in either absence, even if one is served", () => {
    expect(
      deploymentThemeFromPayload({
        status: "undeclared", tokens: {}, polarity: "dark",
      }).polarity,
    ).toBeNull();
  });

  it("reads an ABSENT field as unresolved, not undeclared", () => {
    // A backend predating this field has said nothing about the manifest.
    // Reading silence as "declares no theme" is the defect the server's
    // three-way split exists to prevent; the browser must not reintroduce it.
    expect(deploymentThemeFromPayload(undefined).status).toBe("unresolved");
    expect(deploymentThemeFromPayload(null).status).toBe("unresolved");
    expect(deploymentThemeFromPayload("declared").status).toBe("unresolved");
    expect(deploymentThemeFromPayload({ tokens: DECLARED }).status)
      .toBe("unresolved");
  });

  it("reads an unrecognized status as unresolved", () => {
    expect(deploymentThemeFromPayload({ status: "maybe", tokens: DECLARED }))
      .toEqual({ status: "unresolved", tokens: {}, polarity: null });
  });

  it("drops keys outside the token set and values that are not usable strings", () => {
    expect(
      deploymentThemeFromPayload({
        status: "declared",
        tokens: {
          accent: "#ff8800",
          acccent: "#000000",
          fg: "   ",
          border: 16711680 as unknown as string,
        },
      }),
    ).toEqual({
      status: "declared",
      tokens: { accent: "#ff8800" },
      polarity: null,
    });
  });
});

describe("resolving the state from a deployments read", () => {
  it("is pending until the read answers", () => {
    expect(deploymentThemeStateForDeployments([], false)).toEqual({
      status: "pending",
      tokens: {},
      polarity: null,
      deploymentName: null,
    });
  });

  it("is unresolved when no entry came back stamped current", () => {
    // `/viz/api/deployments` derives `is_current` from a context lookup whose
    // failure is caught and dropped, so this is reachable in normal operation.
    expect(
      deploymentThemeStateForDeployments(
        [{ name: "themed", viz_theme: { status: "declared", tokens: DECLARED } }],
        true,
      ),
    ).toEqual({
      status: "unresolved", tokens: {}, polarity: null, deploymentName: null,
    });
  });

  it("takes the tokens and the polarity from the CURRENT entry and no other", () => {
    expect(
      deploymentThemeStateForDeployments(
        [
          {
            name: "other",
            viz_theme: { status: "declared", tokens: DECLARED, polarity: "dark" },
          },
          {
            name: "mine",
            is_current: true,
            viz_theme: {
              status: "declared",
              tokens: { background: "#ffffff" },
              polarity: "light",
            },
          },
        ],
        true,
      ),
    ).toEqual({
      status: "declared",
      tokens: { background: "#ffffff" },
      polarity: "light",
      deploymentName: "mine",
    });
  });

  it("reports the current deployment's undeclared state, naming it", () => {
    expect(
      deploymentThemeStateForDeployments(
        [{ name: "bare", is_current: true, viz_theme: { status: "undeclared", tokens: {} } }],
        true,
      ),
    ).toEqual({
      status: "undeclared", tokens: {}, polarity: null, deploymentName: "bare",
    });
  });

  it("takes the CHECKED accent from viz_theme, never from the flat field", () => {
    // The one input on which the two reads disagree, pinned on the browser
    // side: a refused sibling manifest serves `unresolved` while the untyped
    // `theme_accent` still carries the string the typed read rejected.
    expect(
      deploymentThemeStateForDeployments(
        [
          {
            name: "broken",
            is_current: true,
            theme_accent: "not a color!",
            viz_theme: { status: "unresolved", tokens: {} },
          },
        ],
        true,
      ),
    ).toEqual({
      status: "unresolved", tokens: {}, polarity: null, deploymentName: "broken",
    });
  });

  it("carries a served declaration all the way to a base-firing decision", () => {
    // End to end across the two modules, on okg-workspace's real payload: a
    // wire response becomes a state becomes a plan. Asserted here because the
    // unit tests either side of the seam could both pass while the seam drops
    // the polarity.
    const resolved = resolveDeploymentTheme(
      deploymentThemeStateForDeployments(
        [
          {
            name: "okg-workspace",
            is_current: true,
            viz_theme: { status: "declared", tokens: DECLARED, polarity: "dark" },
          },
        ],
        true,
      ),
      null,
    );

    expect(resolved).toEqual({
      firing: "hephaestus",
      firingFromDeployment: true,
      application: "declared",
      tokens: DECLARED,
      deploymentName: "okg-workspace",
    });
  });
});

describe("no document", () => {
  it("is a safe no-op", () => {
    vi.stubGlobal("document", undefined);
    expect(() => applyDeploymentTheme(plan())).not.toThrow();
  });
});
