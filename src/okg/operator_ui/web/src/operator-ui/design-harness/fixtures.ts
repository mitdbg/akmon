// The six mock deployments, and the rule that keeps them out of production.
//
// The JSON is imported statically so vitest and the dev server both get it
// without a fetch. `DESIGN_MOCKS_ENABLED` is `import.meta.env.DEV`, which Vite
// replaces with the literal `false` in a production build; Rollup then drops
// every branch guarded by it, and with it the only references to these modules.
// scripts/operator-ui-design/check-bundle-clean.mjs proves that actually happened rather than
// trusting it.
import archiv3Small from "@operator-ui-design-fixtures/archiv3-small.json";
import workspaceMixed from "@operator-ui-design-fixtures/workspace-mixed.json";
import scale200 from "@operator-ui-design-fixtures/scale-200.json";
import emptyDeployment from "@operator-ui-design-fixtures/empty.json";
import brokenRuntime from "@operator-ui-design-fixtures/broken-runtime.json";
import allClear from "@operator-ui-design-fixtures/all-clear.json";
import type { DesignFixture } from "@operator-ui/design-harness/fixtureClient";
import { GRAPH_SCALE_FIXTURES } from "@operator-ui/design-harness/graphScaleFixtures";

/** True only in a development build. See the note above. */
export const DESIGN_MOCKS_ENABLED: boolean = Boolean(
  (import.meta as { env?: { DEV?: unknown } }).env?.DEV,
);

/** Ordered as the switcher lists them: the real shape first, then the stresses. */
export const DESIGN_FIXTURES: DesignFixture[] = [
  archiv3Small as unknown as DesignFixture,
  workspaceMixed as unknown as DesignFixture,
  scale200 as unknown as DesignFixture,
  emptyDeployment as unknown as DesignFixture,
  brokenRuntime as unknown as DesignFixture,
  allClear as unknown as DesignFixture,
];

export const DESIGN_FIXTURE_NAMES: string[] = DESIGN_FIXTURES.map((fixture) => fixture.name);

/** The fixture with this name, or null. Null means "not a design session",
 *  which is the normal case; the caller then uses the real API client. */
export function fixtureByName(name: string | null | undefined): DesignFixture | null {
  if (!name) return null;
  // The synthetic Graph scale deployments answer by name only; they are not
  // in the switcher (graphScaleFixtures.ts).
  return [...DESIGN_FIXTURES, ...GRAPH_SCALE_FIXTURES].find((fixture) => fixture.name === name) ?? null;
}

/** The four views of the redesign, in top-bar order. Separate from
 *  shared/data/navigation.ts on purpose: that list is what the shipping console
 *  serves today (Home, Health, Search), and this one is the proposal. They
 *  converge when the views land, not before. */
export const DESIGN_VIEWS = ["Home", "Updates", "Sources", "Explore", "Graph"] as const;

export type DesignView = (typeof DESIGN_VIEWS)[number];
