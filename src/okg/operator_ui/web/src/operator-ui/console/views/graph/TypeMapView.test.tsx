// @vitest-environment jsdom
//
// The type map's page around the drawing: the side panel at rest and with a
// type selected, the legend, the URL it writes, the keyboard, and the states
// (a URL value it cannot honour, every producer hidden, the selected type
// hidden, the producers read pending or failed).
//
// The drawing itself is d3 over SVG geometry jsdom does not implement; it is
// replaced by a recorder, so these tests hold what the view asks of it. Its
// layout and labels are held by model/graph/typeMap.test.ts, its pixels by
// the rendered captures.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const drawn = vi.hoisted(() => ({ maps: [] as { hidden: ReadonlySet<string>; selected: string | null; expand: ReadonlySet<string> | null }[], calls: [] as string[] }));
vi.mock("@operator-ui/console/views/graph/typeMapDraw", () => ({
  createTypeMap: (input: { hidden: ReadonlySet<string>; selected: string | null; expand: ReadonlySet<string> | null }) => {
    drawn.maps.push(input);
    return {
      setSelection: (t: string | null) => drawn.calls.push(`select ${t}`),
      setHidden: (h: ReadonlySet<string>) => drawn.calls.push(`hide ${[...h].join(",")}`),
      setText: () => {},
      zoomBy: (f: number) => drawn.calls.push(`zoom ${f > 1 ? "in" : "out"}`),
      fit: () => drawn.calls.push("fit"),
      focusNode: (t: string) => drawn.calls.push(`focus ${t}`),
      transform: () => ({ x: 0, y: 0, k: 1 }),
      destroy: () => {},
    };
  },
}));

import graphData from "@operator-ui-design-fixtures/graph/okg-workspace.json";
import { buildGraphModel, type GraphModel, type ProducerRef, producerNamer } from "@operator-ui/console/model/graph/derive";
import { patchedHref } from "@operator-ui/console/model/graph/params";
import { parseGraphTypes, parseOntology } from "@operator-ui/console/model/graph/reads";
import { producerCoverage } from "@operator-ui/console/model/graph/table";
import { TypeMapView } from "@operator-ui/console/views/graph/TypeMapView";
import { syntheticGraph } from "@operator-ui/design-harness/graphScaleFixtures";

const data = graphData as unknown as { types: unknown; ontology: unknown };
const read = parseGraphTypes(data.types);
const big = buildGraphModel(parseGraphTypes(syntheticGraph("graph-scale-140", 140, 16, 400).types));
const ontology = parseOntology(data.ontology);
const link = (route: string, params: Record<string, string> = {}) => {
  const q = new URLSearchParams(params).toString();
  return `#${route}${q ? `?${q}` : ""}`;
};
/** The producers read: three named sources, and 35 sources and 25
 *  enrichers in all, as Sources would count them. */
const producerRows: ProducerRef[] = [
  { name: "git-history", ownershipId: "okg-workspace.git-history", kind: "source" },
  { name: "git-files", ownershipId: "okg-workspace.git-files", kind: "source" },
  { name: "pact", ownershipId: "okg-workspace.pact", kind: "source" },
  ...Array.from({ length: 32 }, (_, i) => ({ name: `s${i}`, ownershipId: `x.s${i}`, kind: "source" as const })),
  ...Array.from({ length: 25 }, (_, i) => ({ name: `e${i}`, ownershipId: `x.e${i}`, kind: "enricher" as const })),
];
const model = buildGraphModel(read, producerNamer(producerRows, link));
const unnamed = buildGraphModel(read);

let host: HTMLDivElement;
let root: Root;
let went: string[];

beforeEach(() => {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  drawn.maps.length = 0;
  drawn.calls.length = 0;
  went = [];
});

afterEach(() => {
  act(() => root.unmount());
  host.remove();
  vi.unstubAllGlobals();
});

async function render(params: Record<string, string>, opts: { m?: GraphModel; producers?: null; producersFailure?: unknown } = {}) {
  // `producers: null` is the producers read pending (or failed): names are ids.
  const m = opts.m ?? (opts.producers === null ? unnamed : model);
  await act(async () => {
    root.render(
      <TypeMapView
        model={m} ontology={ontology} ontologyFailure={null} params={params} link={link}
        setParams={(patch) => { went.push(patchedHref(link, "graph", params, patch)); }}
        producersPending={opts.producers === null && !opts.producersFailure} producersFailure={opts.producersFailure ?? null}
        coverage={opts.producers === null ? null : producerCoverage(m, producerRows)}
      />,
    );
  });
}

const q = (s: string) => host.querySelector(s);
const qa = (s: string) => [...host.querySelectorAll(s)];
const rail = () => q('[data-testid="graph-rail"]')?.textContent ?? "";
const key = (el: Element, k: string) => act(() => { el.dispatchEvent(new KeyboardEvent("keydown", { key: k, bubbles: true })); });

describe("the side panel at rest", () => {
  it("states the totals, their basis, and producers against Sources' total", async () => {
    await render({});
    expect(rail()).toContain("822,142");
    expect(rail()).toContain("Counted when the version was published, before access rules.");
    expect(rail()).toContain("25 of 140 declared");
    // Against the producers read's own totals, in Sources' words.
    expect(q('[data-testid="graph-coverage"]')?.textContent).toContain("of 35 sources made records in this version");
    expect(q('[data-testid="graph-coverage"]')?.textContent).toContain("of 25 enrichers added relationships");
    expect(q('.rail a[href="#sources"]')).not.toBeNull();
    expect(rail()).toContain("7 of 38");
    expect(rail()).not.toContain("Records to open");
  });

  it("says it is reading the producers' names, and names that read when it fails", async () => {
    await render({}, { producers: null });
    expect(rail()).toContain("Reading the producers' names");
    await render({}, { producers: null, producersFailure: new Error("boom") });
    expect(q('[data-testid="read-failed-/viz/api/console/producers"]')).not.toBeNull();
  });

  it("lists every type as one tab stop that arrows walk and Enter picks", async () => {
    await render({});
    const rows = qa(".tr-types button") as HTMLButtonElement[];
    expect(rows).toHaveLength(25);
    expect(rows.filter((b) => b.tabIndex === 0)).toHaveLength(1);
    rows[0].focus();
    await key(rows[0], "ArrowDown");
    expect(document.activeElement).toBe(rows[1]);
    await key(rows[1], "s");
    expect(document.activeElement?.textContent).toContain("source_file");
    await act(async () => { (document.activeElement as HTMLButtonElement).click(); });
    expect(went.at(-1)).toBe("#graph?type=source_file");
  });
});

describe("a selected type", () => {
  it("answers for the type: copyable name, Explore link, producers linked to Sources", async () => {
    await render({ type: "source_file" });
    expect(q(".rail h2 [data-copy]")).not.toBeNull();
    const open = q('[data-testid="graph-open-in-explore"]') as HTMLAnchorElement;
    expect(open.textContent).toBe("Open the 31,411 source_file records in Explore");
    expect(open.getAttribute("href")).toBe("#explore?type=source_file");
    expect(q('.rail a[href="#sources/git-history"]')?.textContent).toBe("git-history");
    expect(q('.rail a[href="#sources/git-files"]')?.textContent).toBe("git-files");
    expect(drawn.maps[0].selected).toBe("source_file");
  });

  it("says when its producer is switched off, and shows it again on request", async () => {
    await render({ type: "pact_task", hide: "okg-workspace.pact" });
    expect(q('[data-testid="graph-selected-hidden"]')?.textContent).toContain("pact is switched off in the legend");
    await act(async () => { (q('[data-testid="graph-selected-hidden"] button') as HTMLButtonElement).click(); });
    expect(went.at(-1)).toBe("#graph?type=pact_task");
  });

  it("clears on Escape anywhere on the page", async () => {
    await render({ type: "source_file", view: "graph" });
    await key(q(".rail h2") as Element, "Escape");
    expect(went.at(-1)).toBe("#graph?view=graph");
  });

  it("names the relationships to itself and who made them", async () => {
    await render({ type: "code_symbol" });
    expect(rail()).toContain("Links to itself 2");
    expect(rail()).toContain("calls");
  });
});

describe("the legend", () => {
  it("switches a producer off through the URL, and the drawing fades it without a new layout", async () => {
    await render({});
    await act(async () => { (qa(".legend .lentry")[2] as HTMLButtonElement).click(); });
    expect(went.at(-1)).toBe("#graph?hide=okg-workspace.pact");
    await render({ hide: "okg-workspace.pact" });
    expect(drawn.maps).toHaveLength(1);
    expect(drawn.calls).toContain("hide okg-workspace.pact");
  });

  it("says so on the stage when every producer is off, and shows them all again", async () => {
    const all = [...new Set(model.types.map((t) => model.typeSource(t)))].join(",");
    await render({ hide: all });
    expect(q('[data-testid="graph-map-all-hidden"]')?.textContent).toContain("Every producer is switched off");
    await act(async () => { (q('[data-testid="graph-map-all-hidden"] button') as HTMLButtonElement).click(); });
    expect(went.at(-1)).toBe("#graph");
  });

  it("groups the producers past the eighth colour, and one press hides them all", async () => {
    await render({}, { m: big, producers: null });
    const group = qa(".legend .lentry").find((b) => b.textContent?.includes("smaller producers")) as HTMLButtonElement;
    expect(group).toBeDefined();
    await act(async () => { group.click(); });
    const hide = new URLSearchParams(went.at(-1)?.split("?")[1]).get("hide")?.split(",") ?? [];
    expect(hide.length).toBeGreaterThan(1);
  });

  it("is a disclosure button on a phone, its entries out of the tab order while closed", async () => {
    vi.stubGlobal("matchMedia", (query: string) => ({ matches: query.includes("max-width"), addEventListener: () => {}, removeEventListener: () => {} }));
    await render({});
    const head = q(".legend button.lhead") as HTMLButtonElement;
    expect(head.getAttribute("aria-expanded")).toBe("false");
    expect((q("#graph-legend-body") as HTMLElement).hidden).toBe(true);
    await act(async () => { head.click(); });
    expect(head.getAttribute("aria-expanded")).toBe("true");
    expect((q("#graph-legend-body") as HTMLElement).hidden).toBe(false);
  });
});

describe("the URL and the keyboard on the drawing", () => {
  it("selects nothing for a type the version does not hold (the page's ignored line names it: Graph.test.tsx)", async () => {
    await render({ type: "no_such_type" });
    expect(drawn.maps[0].selected).toBeNull();
  });

  it("zooms with + and −, fits with 0, and jumps to a type as it is typed", async () => {
    await render({});
    const svg = q("svg") as Element;
    await key(svg, "+");
    await key(svg, "-");
    await key(svg, "0");
    expect(drawn.calls).toEqual(expect.arrayContaining(["zoom in", "zoom out", "fit"]));
    await key(svg, "g");
    await key(svg, "i");
    await key(svg, "t");
    await key(svg, "_");
    await key(svg, "c");
    expect(went.at(-1)).toBe("#graph?type=git_commit");
    await key(svg, "q");
    expect(q('[data-testid="graph-status"]')?.textContent).toContain("No shown type's name contains");
  });

  it("opens one circle per producer above 40 types, and a selected type's producer is open", async () => {
    await render({}, { m: big, producers: null });
    expect(drawn.maps[0].expand?.size).toBe(0);
    const t = big.types[3];
    await render({ type: t }, { m: big, producers: null });
    expect(drawn.maps.at(-1)?.expand?.has(big.typeSource(t))).toBe(true);
    // picking a type in a folded producer opens that producer on the URL
    const other = big.types.find((x) => big.typeSource(x) !== big.typeSource(t)) as string;
    await render({}, { m: big, producers: null });
    const row = qa(".tr-types button").find((b) => b.querySelector(".tname")?.textContent === other) as HTMLButtonElement;
    await act(async () => { row.click(); });
    const sp = new URLSearchParams(went.at(-1)?.split("?")[1]);
    expect(sp.get("type")).toBe(other);
    expect(sp.get("expand")?.split(",")).toContain(big.typeSource(other));
  });
});
