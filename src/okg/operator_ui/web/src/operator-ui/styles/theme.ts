/**
 * Theme selection for the operator UI.
 *
 * Four firings of one token contract (see tokens.css): "cyclades" (light, the
 * default), "lyceaum" (light), "melanomorpha" (dark), and "hephaestus" (dark).
 * The active theme is a `data-theme` attribute on the document root; components
 * read only the semantic tokens.
 *
 * The choice persists per browser in localStorage. Read-only: switching the
 * theme never touches OKG state.
 *
 * A deployment declaring `viz.theme` can move the DEFAULT — not the operator's
 * choice — to the firing matching its declared polarity, and its tokens then
 * layer over that base. `DEFAULT_FIRING_FOR_POLARITY` below is that mapping;
 * `styles/deploymentTheme.ts` owns the rule.
 */

import { useSyncExternalStore } from "react";

export type Theme = "lyceaum" | "melanomorpha" | "cyclades" | "hephaestus";

export const THEMES: readonly Theme[] = [
  "lyceaum",
  "cyclades",
  "melanomorpha",
  "hephaestus",
];

// Each name says which polarity it is: four Greek names gave no hint which
// were light. "Lyceum" is the spelling of the place; the id stays `lyceaum`,
// because it is the stored choice in every browser that picked it and the
// selector the token sheet keys on.
export const THEME_LABELS: Record<Theme, string> = {
  lyceaum: "Lyceum (light)",
  melanomorpha: "Melanómorpha (dark)",
  cyclades: "Cyclades (light)",
  hephaestus: "Hephaestus (dark)",
};

/** Whether a firing is dark — used to pick the toggle glyph and ordering. */
export const THEME_IS_DARK: Record<Theme, boolean> = {
  lyceaum: false,
  cyclades: false,
  melanomorpha: true,
  hephaestus: true,
};

const STORAGE_KEY = "okg-operator-theme";

/** Operator decision: Cyclades (cool Aegean light) is the default; the rest are opt-in. */
export const DEFAULT_THEME: Theme = "cyclades";

/**
 * The default-most firing of each polarity — the BASE a declared deployment
 * theme is layered onto (PACT `operator-ui-deployment-theme`, operator ruling
 * of 2026-08-18).
 *
 * `light` is `DEFAULT_THEME` by definition; there is nothing to choose. `dark`
 * had to be chosen between Melanómorpha and Hephaestus, and the firing
 * definitions in `tokens.css` decide it rather than taste:
 *
 * - Each firing's own comment names its counterpart across the light/dark
 *   line, and the axis is colour TEMPERATURE, not brightness. Cyclades: "the
 *   warm counterpart is Lyceaum". Hephaestus: "the warm-clay counterpart is
 *   Melanomorpha". So the firings come in two pairs — cool {Cyclades,
 *   Hephaestus} and warm {Lyceaum, Melanómorpha} — and the default light
 *   firing is the cool one. Its dark partner is Hephaestus.
 * - Lyceaum is marked "(opt-in)" in `tokens.css`; Melanómorpha is its pair.
 *   Taking Melanómorpha as the dark default would make the console's default
 *   pair straddle both temperatures, which no firing comment supports.
 * - Measured, not only argued: the round that produced this ruling recorded
 *   `okg-workspace` rendering coherently under Hephaestus specifically, and
 *   that deployment's declared greys (`#121417` / `#1a1e23` / `#0b0d10`) sit
 *   within a step of Hephaestus's own cool iron (`#101317` / `#191e24` /
 *   `#090b0e`) while Melanómorpha's ground is warm brown (`#140e08`). The
 *   overrides land on Hephaestus with almost no seam.
 *
 * This is a BASE, not a lock: an operator who picks Melanómorpha keeps it, and
 * the tokens still layer over it (same polarity). See
 * `styles/deploymentTheme.ts`.
 */
export const DEFAULT_FIRING_FOR_POLARITY: Record<"light" | "dark", Theme> = {
  light: DEFAULT_THEME,
  dark: "hephaestus",
};

function isTheme(value: unknown): value is Theme {
  return (
    value === "lyceaum" ||
    value === "melanomorpha" ||
    value === "cyclades" ||
    value === "hephaestus"
  );
}

/**
 * The operator's persisted choice, or `null` when they have not made one.
 *
 * Deliberately NOT `getStoredTheme`. That function answers "what should the
 * console render", so it substitutes the default and can never say "unset".
 * This one answers "did the operator choose", which is a different question
 * with a different consumer: `styles/deploymentTheme.ts` may replace the
 * DEFAULT with a deployment's polarity-matched base firing, and must never
 * replace a choice the operator actually made. Collapsing the two would make
 * an unset browser indistinguishable from one where Cyclades was picked on
 * purpose, and the deployment base would then never apply.
 */
export function readStoredTheme(): Theme | null {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (isTheme(stored)) return stored;
  } catch {
    /* localStorage can throw in private mode / sandboxed frames */
  }
  return null;
}

/** The persisted theme, or the default when unset/unavailable. */
export function getStoredTheme(): Theme {
  return readStoredTheme() ?? DEFAULT_THEME;
}

/**
 * Render a firing WITHOUT persisting it — the attribute only.
 *
 * The deployment base-firing selection uses this and never `applyTheme`: a
 * deployment supplying the DEFAULT must not be recorded as a choice the
 * operator made. If it were, opening a themed deployment once would silently
 * repaint every unthemed deployment the same browser visits afterwards, and
 * the console would have written a preference on the operator's behalf.
 *
 * Writes only on a real change, so the `data-theme` MutationObserver behind
 * `useTheme` is not re-fired by a no-op re-assertion.
 */
export function setFiring(theme: Theme): void {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  if (root.getAttribute("data-theme") === theme) return;
  root.setAttribute("data-theme", theme);
}

/** Apply a theme to the document root and persist the choice. */
export function applyTheme(theme: Theme): void {
  if (typeof document !== "undefined") {
    document.documentElement.setAttribute("data-theme", theme);
  }
  try {
    window.localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    /* ignore persistence failures; the attribute is still applied */
  }
}

/**
 * Set the initial theme before React mounts, so there is no light-to-dark
 * flash. Safe to call more than once. Call from the entry module.
 */
export function initTheme(): Theme {
  const theme = getStoredTheme();
  if (typeof document !== "undefined") {
    document.documentElement.setAttribute("data-theme", theme);
  }
  return theme;
}

/**
 * The active firing, read from the document root (the single source of truth).
 * Used as the getSnapshot for a useSyncExternalStore-based theme hook so React
 * views (legend, Outline, the toggle) re-render when the firing flips.
 */
export function readTheme(): Theme {
  if (typeof document === "undefined") return DEFAULT_THEME;
  const attr = document.documentElement.getAttribute("data-theme");
  return isTheme(attr) ? attr : DEFAULT_THEME;
}

/**
 * Subscribe to firing changes (the data-theme attribute on <html>). Returns an
 * unsubscribe. SSR/test-safe: a no-op when there is no document/observer.
 */
export function subscribeTheme(onChange: () => void): () => void {
  if (typeof document === "undefined" || typeof MutationObserver === "undefined") {
    return () => {};
  }
  const observer = new MutationObserver(onChange);
  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["data-theme"],
  });
  return () => observer.disconnect();
}

/**
 * React hook: the active firing, re-rendering the caller whenever it flips.
 * Backed by the data-theme attribute so the toggle, the graph legend, and the
 * Outline swatches never diverge from the canvas.
 */
export function useTheme(): Theme {
  return useSyncExternalStore(subscribeTheme, readTheme, readTheme);
}
