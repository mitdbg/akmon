/**
 * The deployment's declared theme tokens, applied as CSS custom properties.
 *
 * A deployment manifest may declare seven console tokens under `viz.theme`
 * (background, accent, fg, fg_faint, border, surface, rail).
 * `okg.viz.registry` serves them per entry over `/viz/api/deployments` with an
 * explicit state (PACT `operator-ui-deployment-theme`); this module is the
 * browser half. It layers the declared tokens over whichever of the four
 * built-in firings the operator chose — see `styles/theme.ts` and
 * `styles/tokens.css`.
 *
 * `surface` and `rail` are the SURFACE FAMILY, added by the operator ruling of
 * 2026-08-18. Without them the set could name the page ground but not the
 * cards and rail standing on it, so a dark declaration over the light default
 * firing painted a dark ground under near-white cards — measured at 1099
 * color-contrast violations. The server now refuses that declaration at
 * manifest load rather than serving it; this module's job is unchanged, it
 * just has two more properties to write.
 *
 * BASE FIRING + OVERRIDES — the SECOND operator ruling of 2026-08-18, and the
 * rule this module now implements. The surface family cut the regression from
 * 1102 to 694 and stopped there, because seven tokens cannot name forty
 * properties: 98% of the residual was ink from `--okg-color-strong`,
 * `--okg-color-link`, `--okg-color-surface-muted` and `--okg-color-subtle`,
 * every one of them an UNDECLARED SIBLING of a declared token. The same
 * deployment rendered perfectly under a dark built-in firing, which pins the
 * cause: not a missing token, but a declaration layered onto a palette of the
 * opposite polarity. So a declared theme now first SELECTS a base — the
 * built-in firing matching its served `polarity` — and layers its tokens over
 * that. Every sibling then comes from a palette that agrees with the
 * declaration. `resolveDeploymentTheme` below is the whole rule; the state it
 * returns is stamped on the document root so the choice is never silent.
 *
 * KNOWN, AND STRUCTURAL: the base firing cannot be chosen before boot. The
 * polarity arrives on the deployments read, so `main.tsx` paints the stored (or
 * default) firing first and this module swaps it when the read answers — a
 * brief light-to-dark flash for a dark deployment in an unchosen browser. The
 * only way to remove it is to cache a polarity in localStorage and paint from
 * that, which is a stored value that can be wrong about the deployment being
 * opened now; a wrong palette asserted confidently is worse than a flash. The
 * tokens themselves already had this flash before the base-firing rule; it is
 * one attribute wider, not new.
 *
 * LAYERING IS PER TOKEN, and it is inline style on the document root. A firing
 * sets its values in a `:root[data-theme="…"]` rule; an inline declaration on
 * that same element outranks every one of them. So a deployment declaring only
 * `accent` overrides only `--okg-color-accent`, the other six properties keep
 * the base firing's values, and flipping the firing swaps those six while the
 * accent override stays. Nothing here edits the four built-in palettes.
 *
 * FOUR states, three of them absences — the same split as
 * `shared/hooks/chatSiteContext.ts`, for the same reason:
 *
 * - `declared`   — a manifest was read and it declares `viz.theme`. The ONLY
 *                  state in which anything is overridden, and the only state
 *                  in which the console may say a deployment declares a theme.
 * - `undeclared` — a manifest was read and declares no `viz.theme`. A fact
 *                  about a file that was opened.
 * - `unresolved` — no manifest was read, or the one read was refused, or the
 *                  backend served no `viz_theme` field at all. Saying
 *                  "declares no theme" here would describe a file nobody
 *                  successfully opened.
 * - `pending`    — the deployments read has not answered yet. The browser's
 *                  own fourth state; the same lie as `unresolved`, one moment
 *                  earlier.
 *
 * In all three absences the seven properties are REMOVED, not set to console
 * defaults: the built-in palette is the fallback and it must arrive through
 * the cascade, never as a value this module chose and stamped as if the
 * deployment had asked for it.
 *
 * A FIFTH marker joins them, and it is the base-firing ruling's visible half:
 *
 * - `suppressed_polarity_mismatch` — a theme WAS declared, and the operator is
 *   sitting on a firing of the opposite polarity. Their choice wins and the
 *   overrides do not land at all. Painting them anyway is precisely the
 *   light-ink-on-dark-ground failure this ruling removes, and dropping the
 *   operator's choice instead would be the console overruling a person. So
 *   neither: the firing is rendered pure, and the marker says so out loud.
 */

import {
  DEFAULT_FIRING_FOR_POLARITY,
  DEFAULT_THEME,
  THEME_IS_DARK,
  type Theme,
} from "@operator-ui/styles/theme";

/** The seven tokens, in the server's order (`VIZ_THEME_TOKENS` in
    `okg.substrate.deployments.chat_block`). A token added on one side without
    the other is caught by `test_the_token_set_is_the_seven_the_console_reads`
    and by this module's own suite. The surface family is APPENDED, so the
    original five keep their positions on both sides. */
export const VIZ_THEME_TOKENS = [
  "background",
  "accent",
  "fg",
  "fg_faint",
  "border",
  "surface",
  "rail",
] as const;

export type VizThemeToken = (typeof VIZ_THEME_TOKENS)[number];

/**
 * Token → the ONE existing custom property it overrides.
 *
 * Still deliberately one-to-one, and that survived the widening: `background`
 * drives the page ground and not also `--okg-color-surface` /
 * `--okg-color-canvas`; `fg_faint` drives the muted ink and not also
 * `--okg-color-subtle`. A deployment declaring one token gets one property
 * changed, which is the only mapping an operator can predict from their
 * manifest. What the 2026-08-18 ruling changed is not this shape but the
 * VOCABULARY: the operator can now NAME the surface and the rail, instead of a
 * `background` declaration reaching across and restyling surfaces they never
 * wrote down. The server refuses a dark `background` declared without a
 * `surface`, so the incoherent pairing this map used to permit is now a
 * load-time error rather than an unreadable render.
 *
 * `--okg-color-surface` is the cards, panels and menus; `--okg-color-rail` is
 * the navigation rail's ground. The rail's own ink (`--okg-color-rail-text`
 * and friends) is NOT in the token set and is NOT overridden, which is why a
 * declared `rail` has to stay dark — every built-in firing, light ones
 * included, pairs that light rail ink with a dark rail.
 *
 * `--okg-color-muted` is also what `shared/ui/GraphCanvas.tsx` reads for
 * instance labels (`cssValue("--okg-color-muted", …)`), so a declared
 * `fg_faint` reaches the canvas with no change on that side — the canvas
 * already resolves live CSS vars for exactly this bridge.
 */
export const DEPLOYMENT_THEME_PROPERTIES: Record<VizThemeToken, string> = {
  background: "--okg-color-bg",
  accent: "--okg-color-accent",
  fg: "--okg-color-text",
  fg_faint: "--okg-color-muted",
  border: "--okg-color-line",
  surface: "--okg-color-surface",
  rail: "--okg-color-rail",
};

export type DeploymentThemeStatus =
  | "declared"
  | "undeclared"
  | "unresolved"
  | "pending";

/** The declared ground's light/dark side, computed SERVER-side from its
    luminance (`theme_polarity` in `okg.substrate.deployments.chat_block`) and
    served alongside the tokens. `null` means the declaration names no ground
    this console can place — an `accent`-only theme is the ordinary case — and
    a `null` polarity never moves the operator's firing. */
export type DeploymentThemePolarity = "dark" | "light";

/** The wire shape served on each `/viz/api/deployments` entry. `status` is the
    server's three-way split; the browser adds `pending` on top of it.
    `polarity` may be absent, from a backend predating the base-firing ruling —
    which reads as `null`, i.e. "do not move the firing", the behaviour that
    backend's console had. */
export interface DeploymentVizTheme {
  status: string;
  tokens: Record<string, string>;
  polarity?: string | null;
}

export interface DeploymentThemeState {
  status: DeploymentThemeStatus;
  /** Non-empty only when `status` is `declared`. Partial by design: each token
      is independently optional in the manifest. */
  tokens: Partial<Record<VizThemeToken, string>>;
  /** Non-null only when `status` is `declared` AND the declaration names a
      ground with a computable luminance. */
  polarity: DeploymentThemePolarity | null;
  deploymentName: string | null;
}

/** What the console actually did with a declaration — the five markers of the
    module docstring. `declared` means the tokens are ON the root; the other
    four all mean the built-in firing is rendering pure, for four different and
    separately reported reasons. */
export type DeploymentThemeApplication =
  | DeploymentThemeStatus
  | "suppressed_polarity_mismatch";

/** The resolved decision: which firing the root should carry, whether the
    deployment's tokens land on it, and what to stamp. Pure — produced by
    `resolveDeploymentTheme`, written by `applyDeploymentTheme`. */
export interface DeploymentThemePlan {
  /** The firing the document root should carry. Equal to the operator's stored
      choice whenever they made one; the polarity-matched default otherwise.
      The caller renders it with `setFiring`, which never persists — so a
      deployment-supplied base cannot become a preference the operator did not
      express, and re-rendering their own choice is a no-op. */
  firing: Theme;
  /** Whether `firing` came from the deployment rather than from the operator.
      Reported, not branched on: the caller renders `firing` the same way
      either way. It exists so this rule's two "tokens applied" arms are
      distinguishable — deployment-chosen base vs operator's matching choice —
      by a test, and by anyone reading the plan. */
  firingFromDeployment: boolean;
  application: DeploymentThemeApplication;
  /** Empty unless `application` is `declared`. */
  tokens: Partial<Record<VizThemeToken, string>>;
  deploymentName: string | null;
}

/** The state marker stamped on the document root. Zero pixels: it exists so
    the applied state is observable — by the live browser proof, by the audit,
    and by anyone reading the DOM — without any surface CLAIMING a declaration
    that was not made. */
export const DEPLOYMENT_THEME_ATTRIBUTE = "data-deployment-theme";

function isThemeToken(value: string): value is VizThemeToken {
  return (VIZ_THEME_TOKENS as readonly string[]).includes(value);
}

/** A served polarity, or `null` for anything else — including a field the
    backend did not send. An unrecognized value must not be guessed into one of
    the two: it would pick a base firing on a string nobody defined. */
function readPolarity(value: unknown): DeploymentThemePolarity | null {
  return value === "dark" || value === "light" ? value : null;
}

/**
 * Normalize one served `viz_theme` payload into a browser state.
 *
 * An ABSENT or malformed payload is `unresolved`, never `undeclared`: a
 * backend that predates this field has told the console nothing about the
 * manifest, and reading silence as "declares no theme" is the same defect the
 * server's three-way split exists to prevent.
 */
export function deploymentThemeFromPayload(
  payload: unknown,
): Omit<DeploymentThemeState, "deploymentName"> {
  if (typeof payload !== "object" || payload === null) {
    return { status: "unresolved", tokens: {}, polarity: null };
  }
  const { status, tokens, polarity } = payload as Partial<DeploymentVizTheme>;
  if (status === "undeclared") {
    return { status: "undeclared", tokens: {}, polarity: null };
  }
  if (status !== "declared") {
    return { status: "unresolved", tokens: {}, polarity: null };
  }

  const declared: Partial<Record<VizThemeToken, string>> = {};
  if (typeof tokens === "object" && tokens !== null) {
    for (const [name, value] of Object.entries(tokens)) {
      // Unknown keys are dropped rather than passed through. They could not
      // reach a property name anyway — the applier iterates the fixed token
      // list and looks up the fixed property map, so no served string is ever
      // used AS a property name — but a state object carrying keys the console
      // does not apply would misreport what was applied.
      if (isThemeToken(name) && typeof value === "string" && value.trim()) {
        declared[name] = value;
      }
    }
  }
  return {
    status: "declared",
    tokens: declared,
    polarity: readPolarity(polarity),
  };
}

/**
 * THE BASE-FIRING RULE (operator ruling 2, 2026-08-18). Pure; no DOM.
 *
 * Five arms, and the reason each exists:
 *
 * 1. Not `declared` — nothing was declared, so nothing moves. The operator's
 *    choice, or the console default when they have none, rendered pure. This
 *    is the behaviour that existed before any of this, and it is unchanged.
 * 2. `declared` with `polarity === null` — a declaration that names no ground
 *    (an `accent`-only theme, the ordinary case). It has no polarity to match,
 *    so it cannot be mismatched either: the tokens layer over whatever the
 *    operator is on. Choosing a base here would repaint their console over a
 *    single accent.
 * 3. `declared`, polarity known, operator has NO stored choice — the default
 *    becomes the polarity-matched firing and the tokens layer over it. This is
 *    the arm that fixes the audit: `okg-workspace` boots dark-on-dark instead
 *    of dark-on-light. `firingFromDeployment` is true so the caller renders it
 *    without recording it as a preference.
 * 4. `declared`, polarity known, operator's choice MATCHES it — their firing
 *    stands and the tokens layer over it. A dark deployment under Melanómorpha
 *    is as coherent as under Hephaestus; the base only ever had to agree on
 *    polarity, not on hue.
 * 5. `declared`, polarity known, operator's choice is CROSS-polarity — the
 *    firing renders pure and the overrides are dropped whole. Not partially:
 *    a subset of seven tokens on a mismatched base is the same defect in
 *    miniature. This is the arm that kills the light-ink-on-dark-ground class
 *    outright, and `suppressed_polarity_mismatch` is how the console admits to
 *    it rather than quietly ignoring a manifest.
 */
export function resolveDeploymentTheme(
  state: DeploymentThemeState,
  storedFiring: Theme | null,
): DeploymentThemePlan {
  const base = {
    firing: storedFiring ?? DEFAULT_THEME,
    firingFromDeployment: false,
    tokens: {} as Partial<Record<VizThemeToken, string>>,
    deploymentName: state.deploymentName,
  };

  if (state.status !== "declared") {
    return { ...base, application: state.status };
  }
  if (state.polarity === null) {
    return { ...base, application: "declared", tokens: state.tokens };
  }
  if (storedFiring === null) {
    return {
      ...base,
      firing: DEFAULT_FIRING_FOR_POLARITY[state.polarity],
      firingFromDeployment: true,
      application: "declared",
      tokens: state.tokens,
    };
  }
  const chosenIsDark = THEME_IS_DARK[storedFiring];
  if (chosenIsDark === (state.polarity === "dark")) {
    return { ...base, application: "declared", tokens: state.tokens };
  }
  return { ...base, application: "suppressed_polarity_mismatch" };
}

/**
 * Warn — loudly, without repairing — when a served token cannot paint.
 *
 * The manifest-load grammar (`_is_admissible_color`) admits SHAPE, not
 * membership: `accent: notacolor` has the shape of a CSS identifier, loads
 * clean, and arrives here. Such a value is still applied verbatim, because the
 * alternative is silently substituting a console default for what the operator
 * wrote — the exact failure this whole change removes. What the console owes
 * them instead is a message naming the token, the value, and the deployment,
 * so an unpainted color points at the manifest line that caused it.
 */
function warnIfUnpaintable(
  token: VizThemeToken,
  value: string,
  deploymentName: string | null,
): void {
  const css = (globalThis as { CSS?: { supports?: (p: string, v: string) => boolean } }).CSS;
  if (typeof css?.supports !== "function") return;
  let paints: boolean;
  try {
    paints = css.supports("color", value);
  } catch {
    return;
  }
  if (paints) return;
  console.warn(
    `deployment theme: ${deploymentName ?? "this deployment"} declares `
      + `viz.theme.${token} = ${JSON.stringify(value)}, which this browser `
      + `cannot paint. It is applied to ${DEPLOYMENT_THEME_PROPERTIES[token]} `
      + `verbatim and will not render; fix the value in the manifest.`,
  );
}

/**
 * Apply a resolved plan to the document root. Idempotent; safe with no
 * document. Writes the SEVEN properties and the marker — never `data-theme`,
 * which is the firing module's to own (`setFiring` / `applyTheme`).
 *
 * Every write goes through `style.setProperty` / `style.removeProperty`. The
 * served values are never concatenated into CSS text: the manifest-load
 * grammar is anchored so no token can carry a `;`, a `}` or a `url(` past it,
 * but that grammar is a safety boundary and this is the defense behind it —
 * the CSSOM setter cannot be escaped out of no matter what string arrives.
 *
 * Absent tokens are REMOVED on every call, not just skipped, so switching from
 * a deployment declaring seven to one declaring one leaves exactly one
 * override standing instead of six stale ones — and so a switch INTO
 * `suppressed_polarity_mismatch` strips a previously-applied set rather than
 * leaving it on a base that no longer matches it.
 */
export function applyDeploymentTheme(plan: DeploymentThemePlan): void {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  if (!root?.style) return;

  const declared = plan.application === "declared" ? plan.tokens : {};
  for (const token of VIZ_THEME_TOKENS) {
    const property = DEPLOYMENT_THEME_PROPERTIES[token];
    const value = declared[token];
    if (typeof value === "string" && value.trim()) {
      root.style.setProperty(property, value);
      warnIfUnpaintable(token, value, plan.deploymentName);
    } else {
      root.style.removeProperty(property);
    }
  }
  root.setAttribute?.(DEPLOYMENT_THEME_ATTRIBUTE, plan.application);
}
