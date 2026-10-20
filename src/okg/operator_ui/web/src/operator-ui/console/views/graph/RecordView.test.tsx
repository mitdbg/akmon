// @vitest-environment jsdom
//
// The record page, mounted inside the Graph route against the harness's
// fixtures: the side panel at rest, with a group open and with a record
// selected; the URL as its state (replace within the page, push to another
// record, ignored values named); Escape; the lists on a narrow window; the
// empty, absent-neighbour, many-groups, not-found and no-id states; the
// breadcrumb and Copy as text; and leaving for the whole graph's Table.
//
// The tree is d3 over getBBox, which jsdom lacks; it is replaced by a
// recorder, so these tests hold what the view hands it and call its callbacks
// as a click would. Its layout is held by model/graph/recordTree.test.ts and
// its pixels by the rendered captures.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const drawn = vi.hoisted(() => ({ trees: [] as unknown[], selections: [] as unknown[] }));
vi.mock("@operator-ui/console/views/graph/recordTreeDraw", () => ({
  createRecordTree: (input: { onStatus: (s: string) => void }) => {
    drawn.trees.push(input);
    input.onStatus("drawn");
    return { setSelection: (s: unknown) => { drawn.selections.push(s); }, zoomBy: () => {}, fit: () => {}, destroy: () => {} };
  },
}));
vi.mock("@operator-ui/console/views/graph/typeMapDraw", () => ({
  createTypeMap: () => ({ setSelection: () => {}, zoomBy: () => {}, fit: () => {}, destroy: () => {} }),
}));

import { createFixtureApiClient } from "@operator-ui/design-harness/fixtureClient";
import { fixtureByName } from "@operator-ui/design-harness/fixtures";
import { designLink } from "@operator-ui/design-harness/DesignHarness";
import {
  ABSENT_RECORD_ID, EMPTY_RECORD_ID, HUB_RECORD_ID, MANY_GROUPS_RECORD_ID, MISSING_RECORD_ID,
} from "@operator-ui/design-harness/graphFixtures";
import { ConsoleGraphRoute } from "@operator-ui/console/views/graph/Graph";
import { producerNamer } from "@operator-ui/console/model/graph/derive";
import type { RecordTreeInput } from "@operator-ui/console/views/graph/recordTreeDraw";

const NODE_DETAIL = "file:okg:src/okg/substrate/console_views/node_detail.py";

let host: HTMLDivElement;
let root: Root;

beforeEach(() => {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  drawn.trees.length = 0;
  drawn.selections.length = 0;
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 1440 });
  window.history.replaceState(null, "", "#graph/record");
});

afterEach(() => {
  act(() => root.unmount());
  host.remove();
});

async function mount(fixture: string, params: Record<string, string>, detail: string | null = "record"): Promise<void> {
  const f = fixtureByName(fixture);
  if (!f) throw new Error(`no fixture ${fixture}`);
  await act(async () => {
    root.render(
      <ConsoleGraphRoute api={createFixtureApiClient(f)} link={designLink(fixture)} params={params} detail={detail} readingVersion={null} nowMs={0} />,
    );
  });
  for (let i = 0; i < 6; i += 1) await act(async () => { await Promise.resolve(); });
}

const q = (selector: string) => host.querySelector(selector);
const text = (selector: string) => q(selector)?.textContent ?? "";
const lastTree = () => drawn.trees[drawn.trees.length - 1] as RecordTreeInput;
const hashParams = () => Object.fromEntries(new URLSearchParams(window.location.hash.slice(window.location.hash.indexOf("?") + 1)));

describe("the side panel", () => {
  it("shows the record's identity at rest: name, type linked to Explore, copyable id, producer, Open in Explore", async () => {
    await mount("workspace-mixed", { id: NODE_DETAIL });
    expect(drawn.trees).toHaveLength(1);
    const rail = text('[data-testid="graph-rail"]');
    expect(rail).toContain("src/okg/substrate/console_views/node_detail.py");
    expect(q('[data-testid="graph-rail"] a.rt-type')?.getAttribute("href")).toBe("#explore?fixture=workspace-mixed&type=source_file");
    expect(q('[data-testid="graph-rail"] button[data-copy="record id"]')).not.toBeNull();
    expect(q('[data-testid="graph-open-in-explore"]')?.getAttribute("href")).toBe(`#explore?fixture=workspace-mixed&record=${encodeURIComponent(NODE_DETAIL)}`);
    // The canvas draws the groups; the panel does not repeat them or their totals.
    expect(rail).not.toContain("Links to this record");
    expect(rail).not.toMatch(/related records/);
    expect(text('[data-testid="graph-status"]')).toBe("drawn");
  });

  it("opens a group from the URL: its facts, its listed records folded by name with their ids, and Explore filtered to it", async () => {
    await mount("workspace-mixed", { id: HUB_RECORD_ID });
    const tree = lastTree();
    const group = tree.read.groups.find((g) => g.count > g.items.length) as RecordTreeInput["read"]["groups"][number];
    const param = `${group.direction}:${group.relationship}:${group.type}`;
    await mount("workspace-mixed", { id: HUB_RECORD_ID, group: param });
    expect(lastTree().expanded).toBe(`${group.direction}|${group.relationship}|${group.type}`);
    const rail = text('[data-testid="graph-rail"]');
    expect(rail).toContain(`First ${group.items.length} of ${group.count.toLocaleString("en-US")}`);
    expect(q('[data-testid="graph-rail-items"]')?.querySelectorAll('button[data-copy="record id"]').length).toBe(group.items.length);
    const explore = q('[data-testid="graph-rail-explore-group"]')?.getAttribute("href") ?? "";
    expect(explore).toContain(`record=${encodeURIComponent(HUB_RECORD_ID)}`);
    expect(explore).toContain("tab=related");
    expect(explore).toContain(`rel=${group.relationship}`);
    expect(explore).toContain(group.direction === "in" ? "dir=incoming" : "dir=outgoing");
  });

  it("shows a selected record: where it sits, who made its link, Open in graph and Open in Explore", async () => {
    await mount("workspace-mixed", { id: NODE_DETAIL });
    const g = lastTree().read.groups[0];
    await mount("workspace-mixed", { id: NODE_DETAIL, sel: g.items[0].id });
    const rail = text('[data-testid="graph-rail"]');
    expect(rail).toContain("Made by");
    expect(rail).toContain(g.relationship);
    expect(q('[data-testid="graph-rail-open-in-graph"]')?.getAttribute("href")).toBe(`#graph/record?fixture=workspace-mixed&id=${encodeURIComponent(g.items[0].id)}`);
    expect(drawn.selections[drawn.selections.length - 1]).toEqual({ key: `${g.direction}|${g.relationship}|${g.type}`, id: g.items[0].id });
  });

  it("names a producer by the producers read and links it to Sources; an unknown one stays an id", () => {
    const names = producerNamer([{ name: "git-files", ownershipId: "okg-workspace.git-files", kind: "source" }], designLink("workspace-mixed"));
    expect(names("okg-workspace.git-files").name).toBe("git-files");
    expect(names("okg-workspace.git-files").href).toBe("#sources/git-files?fixture=workspace-mixed");
    expect(names("okg-workspace.other").name).toBe("okg-workspace.other");
    expect(names("okg-workspace.other").href).toBeNull();
  });
});

describe("the URL is the state", () => {
  it("replaces the history entry when a group opens or a record is selected, and pushes one to centre on another record", async () => {
    await mount("workspace-mixed", { id: NODE_DETAIL });
    const tree = lastTree();
    const g = tree.read.groups[0];
    const key = `${g.direction}|${g.relationship}|${g.type}`;
    const before = window.history.length;
    act(() => tree.onToggle(key));
    expect(hashParams().group).toBe(`${g.direction}:${g.relationship}:${g.type}`);
    act(() => tree.onSelectLeaf(key, g.items[0].id));
    expect(hashParams().sel).toBe(g.items[0].id);
    expect(window.history.length).toBe(before);
    act(() => tree.onOpen(g.items[0].id));
    expect(hashParams()).toEqual({ fixture: "workspace-mixed", id: g.items[0].id });
    expect(window.history.length).toBe(before + 1);
  });

  it("names a group or a selection this record does not have, and a view a record page does not draw", async () => {
    await mount("workspace-mixed", { id: NODE_DETAIL, group: "out:nothing:nowhere", sel: "no:such", view: "table" });
    const line = text('[data-testid="graph-ignored-params"]');
    expect(line).toContain("group=out:nothing:nowhere");
    expect(line).toContain("sel=no:such");
    expect(line).toContain("view=table");
    expect(drawn.trees).toHaveLength(1);
  });

  it("clears the selection with Escape, then closes the group", async () => {
    await mount("workspace-mixed", { id: NODE_DETAIL });
    const g = lastTree().read.groups[0];
    const group = `${g.direction}:${g.relationship}:${g.type}`;
    await mount("workspace-mixed", { id: NODE_DETAIL, group, sel: g.items[0].id });
    act(() => { window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" })); });
    expect(hashParams()).toEqual({ fixture: "workspace-mixed", id: NODE_DETAIL, group });
    await mount("workspace-mixed", { id: NODE_DETAIL, group });
    act(() => { window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" })); });
    expect(hashParams()).toEqual({ fixture: "workspace-mixed", id: NODE_DETAIL });
  });

  it("goes to the whole graph's Table, without the record, from a record page", async () => {
    await mount("workspace-mixed", { id: NODE_DETAIL });
    // The switch is a set of links: the Table's names no record.
    const table = [...host.querySelectorAll(".okg-console__switch")].find((b) => b.textContent === "Table") as HTMLElement;
    expect(table.getAttribute("href")).toBe("#graph?fixture=workspace-mixed&view=table");
  });
});

describe("the page's other states", () => {
  it("draws lists on a phone even when the URL names no view", async () => {
    Object.defineProperty(window, "innerWidth", { configurable: true, value: 390 });
    await mount("workspace-mixed", { id: NODE_DETAIL });
    expect(drawn.trees).toHaveLength(0);
    const lists = text('[data-testid="graph-record-lists"]');
    expect(lists).toContain("Links to this record");
    expect(lists).toContain("Counts are published records, before access rules.");
    expect(q('[data-testid="graph-table"]')).toBeNull();
  });

  it("marks a floor and the policy basis on the lists", async () => {
    Object.defineProperty(window, "innerWidth", { configurable: true, value: 390 });
    await mount("scale-200", { id: HUB_RECORD_ID });
    const lists = text('[data-testid="graph-record-lists"]');
    expect(lists).toMatch(/Links from this record [\d,]+\+/);
    expect(lists).toContain("the access policy lets this console see");
  });

  it("says a record with no links has none, and draws no tree", async () => {
    await mount("workspace-mixed", { id: EMPTY_RECORD_ID });
    expect(text('[data-testid="graph-record-empty"]')).toBe("No relationships in this version.");
    expect(drawn.trees).toHaveLength(0);
    expect(q('[data-testid="graph-status"]')).toBeNull();
  });

  it("lists neighbours absent from this version, unlinked", async () => {
    Object.defineProperty(window, "innerWidth", { configurable: true, value: 390 });
    await mount("workspace-mixed", { id: ABSENT_RECORD_ID, group: "out:imports:" });
    const lists = text('[data-testid="graph-record-lists"]');
    expect(lists).toContain("not in this version");
    expect(lists).toContain("file:okg:src/okg/substrate/removed_a.py");
    expect(q('.rt-list a[href*="removed_a"]')).toBeNull();
  });

  it("hands the tree a record with more than ten groups a side, and lists the folded ones on request", async () => {
    await mount("workspace-mixed", { id: MANY_GROUPS_RECORD_ID });
    const tree = lastTree();
    expect(tree.read.groups.filter((g) => g.direction === "out").length).toBeGreaterThan(10);
    act(() => tree.onKinds("out"));
    expect(text('[data-testid="graph-rail"]')).toMatch(/\d+ more kinds?/);
  });

  it("says a record that is not there is not there, without offering to read again", async () => {
    await mount("scale-200", { id: MISSING_RECORD_ID });
    const line = text('[data-testid="graph-settled-record_not_found"]');
    expect(line).toBe(`This version holds no record with the id ${MISSING_RECORD_ID} that you may open.`);
    expect(host.textContent).not.toContain("Read again");
  });

  it("says an address with no id names no record, and keeps no earlier record in the crumb or the copy", async () => {
    await mount("workspace-mixed", { id: NODE_DETAIL });
    expect(text(".crumb b")).toBe("src/okg/substrate/console_views/node_detail.py");
    expect(q(".crumb a[href*='type=source_file']")).not.toBeNull();
    await mount("workspace-mixed", {});
    expect(host.textContent).toContain("This address names no record");
    expect(text(".crumb b")).toBe("No record named");
    expect(q(".okg-console__copy--page")).toBeNull();
  });
});
