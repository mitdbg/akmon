// @vitest-environment jsdom
//
// The Graph view, mounted and driven to completion against the harness's
// fixtures: the Table, the Grid, the type map's side panel, and every read
// failing in its own place. The record page: RecordView.test.tsx.
//
// The two SVG drawings are d3 over getBBox and getComputedTextLength, which
// jsdom does not implement; they are replaced here by recorders, so these
// tests hold what the view hands them. Their layout is held by
// model/graph/graph.test.ts, and their pixels by the rendered captures.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const drawn = vi.hoisted(() => ({ maps: [] as unknown[], trees: [] as unknown[] }));
vi.mock("@operator-ui/console/views/graph/typeMapDraw", () => ({
  createTypeMap: (input: unknown) => {
    drawn.maps.push(input);
    return {
      setSelection: () => {}, setHidden: () => {}, setText: () => {}, zoomBy: () => {}, fit: () => {},
      focusNode: () => {}, transform: () => ({ x: 0, y: 0, k: 1 }), destroy: () => {},
    };
  },
}));
vi.mock("@operator-ui/console/views/graph/recordTreeDraw", () => ({
  createRecordTree: (input: { onStatus: (s: string) => void }) => {
    drawn.trees.push(input);
    input.onStatus("drawn");
    return { setSelection: () => {}, zoomBy: () => {}, fit: () => {}, destroy: () => {} };
  },
}));

import { createFixtureApiClient } from "@operator-ui/design-harness/fixtureClient";
import { fixtureByName } from "@operator-ui/design-harness/fixtures";
import { designLink } from "@operator-ui/design-harness/DesignHarness";
import { ConsoleGraphRoute } from "@operator-ui/console/views/graph/Graph";

let host: HTMLDivElement;
let root: Root;

beforeEach(() => {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  drawn.maps.length = 0;
  drawn.trees.length = 0;
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 1440 });
});

afterEach(() => {
  act(() => root.unmount());
  host.remove();
});

async function mount(fixture: string, params: Record<string, string>, detail: string | null = null): Promise<void> {
  const f = fixtureByName(fixture);
  if (!f) throw new Error(`no fixture ${fixture}`);
  await act(async () => {
    root.render(
      <ConsoleGraphRoute api={createFixtureApiClient(f)} link={designLink(fixture)} params={params} detail={detail} readingVersion={null} nowMs={0} />,
    );
  });
  // Every read answers on a microtask; flush them.
  for (let i = 0; i < 5; i += 1) await act(async () => { await Promise.resolve(); });
}

const q = (selector: string) => host.querySelector(selector);
const qa = (selector: string) => [...host.querySelectorAll(selector)];

describe("the type map", () => {
  it("hands the drawing the model and says at rest what the deployment holds", async () => {
    await mount("workspace-mixed", { view: "map" });
    expect(drawn.maps).toHaveLength(1);
    const input = drawn.maps[0] as { model: { types: string[] }; hidden: Set<string> };
    expect(input.model.types).toHaveLength(25);
    expect(input.hidden.size).toBe(0);
    const rail = q('[data-testid="graph-rail"]')?.textContent ?? "";
    expect(rail).toContain("822,142");
    expect(rail).toContain("25 of 140 declared");
    // one entry per producer that colours a type: git-files colours none
    expect(qa(".legend .lentry")).toHaveLength(8);
  });

  it("hides a producer's types by URL when its legend entry is pressed", async () => {
    await mount("workspace-mixed", { view: "map" });
    await act(async () => { (qa(".legend .lentry")[2] as HTMLElement).click(); });
    expect(decodeURIComponent(window.location.hash)).toContain("hide=okg-workspace.pact");
    await mount("workspace-mixed", { view: "map", hide: "okg-workspace.pact" });
    expect([...(drawn.maps[drawn.maps.length - 1] as { hidden: Set<string> }).hidden]).toEqual(["okg-workspace.pact"]);
  });

  it("fails by name when the type map read is unavailable", async () => {
    await mount("broken-runtime", { view: "map" });
    const failed = q('[data-testid="read-failed-/viz/api/console/graph/types"]')?.textContent ?? "";
    expect(failed).toContain("The record types and their relationships could not be read.");
    expect(failed).toContain("count_rollup_unavailable");
    expect(drawn.maps).toHaveLength(0);
  });

  it("selects the type the URL names, in the crumb, and names on one line what the map cannot honour", async () => {
    await mount("workspace-mixed", { view: "map", type: "source_file" });
    expect(q(".crumb b")?.textContent).toBe("source_file");
    expect((drawn.maps[drawn.maps.length - 1] as { selected: string | null }).selected).toBe("source_file");
    expect(q('[data-testid="graph-ignored-params"]')).toBeNull();
    await mount("workspace-mixed", { view: "map", type: "no_such_type", hide: "nobody" });
    const line = q('[data-testid="graph-ignored-params"]')?.textContent ?? "";
    expect(line).toContain("type=no_such_type");
    expect(line).toContain("hide=nobody");
    expect(q('[data-testid="graph-map-ignored"]')).toBeNull();
  });

  it("says nothing is published, as Home does, with no Read again", async () => {
    await mount("empty", { view: "map" });
    expect(q('[data-testid="graph-settled-nothing_published"]')?.textContent).toContain("No version has been published");
    expect(q('[data-testid="read-again"]')).toBeNull();
  });
});

// A record's neighbourhood: RecordView.test.tsx.
