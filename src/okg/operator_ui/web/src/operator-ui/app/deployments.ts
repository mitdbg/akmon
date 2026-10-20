import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import type { ChatSiteState } from "@operator-ui/shared/hooks/chatSiteContext";
import {
  deploymentThemeFromPayload,
  type DeploymentThemeState,
  type DeploymentVizTheme,
} from "@operator-ui/styles/deploymentTheme";

export interface DeploymentEntry {
  name: string;
  description?: string;
  port?: number;
  url?: string | null;
  /** The manifest's `viz.theme.accent`, flattened and UNCHECKED.
   *
   *  It used to be the one manifest value this console read. It is not any
   *  more: `viz_theme` below is the typed seven-token read, and it is the one a
   *  consumer wanting a CHECKED accent must use. The two disagree on exactly
   *  one input and the divergence is deliberate — a sibling deployment whose
   *  `viz.theme` the loader refuses serves `viz_theme.status = "unresolved"`
   *  while this field still carries the rejected string verbatim (pinned in
   *  `tests/operator_ui/test_deployment_theme.py`, the malformed-sibling case).
   *  Nothing in this console reads it today. */
  theme_accent?: string | null;
  /** The deployment's declared console theme tokens and, explicitly, HOW they
   *  came to be absent — `{status, tokens, polarity}` from
   *  `src/okg/viz/registry.py`. `declared` carries the tokens verbatim plus the
   *  declared ground's light/dark polarity; `undeclared` and `unresolved` are
   *  two different absences (a manifest that was read and declares none, vs no
   *  manifest read at all). Applied by `styles/deploymentTheme.ts`; see there
   *  for the token→property mapping, the base-firing rule and the layering. */
  viz_theme?: DeploymentVizTheme | null;
  /** The deployment's public chat site, from its manifest's
      `chat.deep_links.chat_site_base`. Surfaced by
      `src/okg/viz/registry.py` over `/viz/api/deployments` — the console
      has no other source for it. Absent = this deployment declares no
      chat site, which renders as a stated absence, never a guessed host. */
  chat_site_base?: string | null;
  /** The deployment-owned static operator app URL. The registry's `url`
      points at the read API and must never be guessed into a browser route.
      Served, and read by nothing in this app since the old sidebar's
      deployment switcher went with the Home and Health pages. */
  operator_ui_base?: string | null;
  launch_command?: string;
  is_current?: boolean;
}

export interface DeploymentRegistry {
  entries: DeploymentEntry[];
}

export async function loadDeployments(api: ApiClient): Promise<DeploymentEntry[]> {
  const registry = await api.get<DeploymentRegistry>("/viz/api/deployments");
  return Array.isArray(registry.entries) ? registry.entries : [];
}

/**
 * The entry the backend STAMPED as current, or null.
 *
 * It selects an ENTRY whose fields become real behaviour — its
 * `chat_site_base` becomes an anchor's href — so there is no display-string
 * fallback here: one would let a deployment that merely happens to carry the
 * fallback's name supply the chat link for a different deployment entirely.
 * Entry selection uses `is_current` and nothing else; unresolved is a state,
 * not a thing to repair.
 */
export function currentDeploymentEntry(
  entries: DeploymentEntry[],
): DeploymentEntry | null {
  return entries.find((entry) => entry.is_current) ?? null;
}

/**
 * The chat site this deployment declares, or null.
 *
 * Null means "the deployment declares no chat site" — the console then
 * says so rather than substituting a host. There is no default and no
 * same-origin guess: an operator following a fabricated chat URL is worse
 * off than one told the value is missing.
 */
export function chatSiteUrlForDeployment(
  entry: DeploymentEntry | null | undefined,
): string | null {
  const declared = entry?.chat_site_base;
  if (typeof declared !== "string") {
    return null;
  }
  return declared.trim() || null;
}

/**
 * Resolve the shell's chat-site state from a deployments read.
 *
 * Four outcomes, deliberately not three. "The current deployment declares no
 * chat site" is a fact about a manifest that was READ. "The current
 * deployment could not be identified" is the absence of that reading —
 * /viz/api/deployments stamps `is_current` from a context lookup that is
 * caught and dropped (`src/okg/viz/app.py`), so an unresolvable context makes
 * every entry non-current. Collapsing the two would have the console assert
 * something about a manifest it never looked at.
 *
 * `pending` is the third absence: the deployments read has not answered yet.
 * Reporting "unresolved" during boot would be the same lie one moment earlier.
 */
export function chatSiteStateForDeployments(
  entries: DeploymentEntry[],
  loaded: boolean,
): ChatSiteState {
  if (!loaded) {
    return { status: "pending", href: null, deploymentName: null };
  }

  const entry = currentDeploymentEntry(entries);
  if (entry === null) {
    return { status: "unresolved", href: null, deploymentName: null };
  }

  const href = chatSiteUrlForDeployment(entry);
  return {
    status: href === null ? "undeclared" : "declared",
    href,
    deploymentName: entry.name,
  };
}

/**
 * Resolve the console-wide theme state from a deployments read.
 *
 * Same four outcomes and the same entry-selection rule as
 * `chatSiteStateForDeployments` above, deliberately: both answer a question
 * about the CURRENT deployment's manifest, so both must refuse to answer it
 * from an entry the backend did not stamp `is_current`. A display-string
 * fallback here would let a deployment that merely happens to carry the
 * fallback's name repaint the console for a different deployment entirely.
 *
 * Only `declared` carries tokens and a polarity. The other three are the
 * console's own four built-in palettes, unmodified.
 */
export function deploymentThemeStateForDeployments(
  entries: DeploymentEntry[],
  loaded: boolean,
): DeploymentThemeState {
  if (!loaded) {
    return { status: "pending", tokens: {}, polarity: null, deploymentName: null };
  }

  const entry = currentDeploymentEntry(entries);
  if (entry === null) {
    return { status: "unresolved", tokens: {}, polarity: null, deploymentName: null };
  }

  return {
    ...deploymentThemeFromPayload(entry.viz_theme),
    deploymentName: entry.name,
  };
}
