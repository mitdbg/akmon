// The record page's model: the tree's plan, its folded names, the URL state
// (open group, selected neighbour, ignored parameters), the Explore links and
// the copy text. Asserted against the converted okg-workspace reads the
// harness serves, plus the harness's composed many-groups and absent-neighbour
// records.
import { describe, expect, it } from "vitest";
import graphData from "@operator-ui-design-fixtures/graph/okg-workspace.json";
import { SearchResponseError } from "@operator-ui/console/api/searchData";
import { graphRecordText } from "@operator-ui/console/model/copyText/graph";
import { parseRecordNeighbourhood, type RecordNeighbourhoodRead } from "@operator-ui/console/model/graph/reads";
import {
  exploreRecordHref, exploreTypeHref, graphIgnored, graphIgnoredSentence, graphRecordHref, groupFromParam, groupParam, groupPatch,
  mapTypeHref, patchedHref, recordChecks, recordSelection, relatedHref, selPatch,
} from "@operator-ui/console/model/graph/params";
import {
  ABSENT_TYPE, type TreeGroup, capClause, capClauses, exploreLinkText, fitText, foldNames, foldOf, hasNoRelationships, keyOf, leavesOf,
  limitNotes, planBlocks, rtSide, sideH, toTreeGroups, treeStatus, trunc,
} from "@operator-ui/console/model/graph/recordTree";
import { consoleLink } from "@operator-ui/routes/console/ConsoleRoute";
import { ABSENT_RECORD_ID, MANY_GROUPS_RECORD_ID, composedRecords } from "@operator-ui/design-harness/graphFixtures";

const data = graphData as unknown as { records: Record<string, unknown> };
const NODE_DETAIL = "file:okg:src/okg/substrate/console_views/node_detail.py";
const HUB = "file:okg:src/okg/substrate/deployment_upgrade.py";
const read = parseRecordNeighbourhood(data.records[NODE_DETAIL]);
const groups = toTreeGroups(read.groups);
const link = (route: string, params: Record<string, string> = {}) => consoleLink(route, params);
const context = { deployment: "okg-workspace", version: read.generationId, publishedAt: null, nowMs: 0 };
const composed = composedRecords();
const many = parseRecordNeighbourhood(composed[MANY_GROUPS_RECORD_ID]);
const absent = parseRecordNeighbourhood(composed[ABSENT_RECORD_ID]);
const tg = (over: Partial<TreeGroup>): TreeGroup => ({ d: "out", e: "r", t: "t", type: "t", n: 1, floor: false, items: [], ...over });

describe("the record tree plan", () => {
  it("folds past ten groups a side into 'more kinds', and an opened folded group takes the last slot", () => {
    const list = Array.from({ length: 12 }, (_, i) => tg({ t: `t${i}`, type: `t${i}`, n: 100 - i }));
    const side = rtSide(list, "out", 10, null);
    expect(side.shown).toHaveLength(10);
    expect(side.shown[9].other).toBe(true);
    expect(side.shown[9].n).toBe(side.hidden.reduce((a, g) => a + g.n, 0));
    const opened = rtSide(list, "out", 10, keyOf(list[11]));
    expect(opened.shown.map((g) => g.t)).toContain("t11");
    expect(opened.hidden.map((g) => g.t)).not.toContain("t11");
  });

  it("folds the harness's many-groups record: more than ten groups on one side", () => {
    const side = rtSide(toTreeGroups(many.groups), "out", 10, null);
    expect(side.hidden.length).toBeGreaterThan(0);
    expect(side.shown[side.shown.length - 1].t).toBe(`${side.hidden.length} more ${side.hidden.length === 1 ? "kind" : "kinds"}`);
  });

  it("folds repeated names the same way whether or not the group is fully listed, with every id kept", () => {
    const items = [
      { id: "a", label: "str", producer: "p" }, { id: "b", label: "str", producer: "p" }, { id: "c", label: null, producer: "p" },
    ];
    const partial = tg({ n: 300, items });
    expect(leavesOf(partial, 6).map((l) => [l.it.id, l.k, l.ids])).toEqual([["a", 2, ["a", "b"]], ["c", 1, ["c"]]]);
    expect(foldOf(items, "b")?.it.id).toBe("a");
    expect(foldNames(items)).toHaveLength(2);
  });

  it("counts the records a closed group does not draw, folded names included", () => {
    const items = Array.from({ length: 8 }, (_, i) => ({ id: `r${i}`, label: i < 3 ? "same" : `n${i}`, producer: "p" }));
    const g = tg({ d: "in", n: 40, items });
    const sides = { in: rtSide([g], "in", 10, null), out: rtSide([], "out", 10, null) };
    const block = planBlocks(sides, null, 2000).blocks.in[0];
    // Six names drawn: "same" (3 records) and five more.
    expect(block.leaves.map((l) => l.k)).toEqual([3, 1, 1, 1, 1, 1]);
    expect(block.rest).toBe(40 - 8);
    expect(block.rows).toBe(7);
  });

  it("lists fewer names per group until both sides fit the stage", () => {
    const sides = { in: rtSide(groups, "in", 10, null), out: rtSide(groups, "out", 10, null) };
    expect(planBlocks(sides, null, 2000).per).toBe(6);
    const short = planBlocks(sides, null, 300);
    expect(short.per).toBeLessThan(6);
    expect(Math.max(sideH(short.blocks.in), sideH(short.blocks.out))).toBeLessThanOrEqual(300);
  });

  it("gives an opened group the room the others leave, and ends it with the Explore row only when records are not drawn", () => {
    const key = keyOf(groups.find((g) => g.d === "in") as TreeGroup);
    const sides = { in: rtSide(groups, "in", 10, key), out: rtSide(groups, "out", 10, key) };
    const plan = planBlocks(sides, key, 700);
    const open = plan.blocks.in.find((b) => b.open);
    expect(open?.leaves.length).toBeGreaterThan(6);
    expect(plan.blocks.out.every((b) => b.rows === 0)).toBe(true);
    const whole = tg({ d: "in", n: 2, items: [{ id: "a", label: "a", producer: "p" }, { id: "b", label: "b", producer: "p" }] });
    const one = planBlocks({ in: rtSide([whole], "in", 10, keyOf(whole)), out: rtSide([], "out", 10, null) }, keyOf(whole), 700);
    expect(one.blocks.in[0].rows).toBe(2);
  });

  it("cuts a path in the middle, keeping its file name, and a sentence at its end", () => {
    expect(trunc("src/okg/substrate/console_views/node_detail.py", 20)).toBe("src/…/node_detail.py");
    expect(trunc("src/okg/substrate/deployment_upgrade.py", 30)).toBe("src/…/deployment_upgrade.py");
    expect(trunc("src/okg/substrate/deployment_upgrade.py", 35)).toBe("src/okg/…/deployment_upgrade.py");
    expect(trunc("src/okg/substrate/deployment_upgrade.py", 24)).toBe("…/deployment_upgrade.py");
    expect(trunc("src/okg/substrate/deployment_upgrade.py", 12)).toBe("…_upgrade.py");
    expect(trunc("Merge pull request #430 from mitdbg/dev", 12)).toBe("Merge pull …");
    expect(fitText("abcdefghij", 5, (s) => s.length)).toHaveLength(5);
  });
});

describe("the line under the tree", () => {
  it("says what the drawing cannot, never the totals the side headings show", () => {
    const at = treeStatus(read, { expanded: null, drawn: 0, per: 6, hiddenKinds: 0 });
    expect(at).toContain("Each group lists its first 6 names by id");
    expect(at).toContain("Counts are published records, before access rules");
    expect(at).not.toMatch(/related records/);
  });

  it("names a floor, the policy basis and a relationship whose types were not all reached", () => {
    const hub = parseRecordNeighbourhood(data.records[HUB]);
    const policed: RecordNeighbourhoodRead = {
      ...hub,
      countBasis: "visible_to_reader",
      scanCap: 500,
      groups: hub.groups.map((g, i) => ({ ...g, countIsFloor: i === 0 })),
      relationships: hub.relationships.map((r, i) => ({ ...r, typesComplete: i !== 0 })),
      countCap: 100000,
    };
    const line = treeStatus(policed, { expanded: null, drawn: 0, per: 0, hiddenKinds: 2 });
    expect(line).toContain("Groups shown as counts at this height");
    expect(line).toContain("2 smaller groups under “more kinds”");
    expect(line).toContain("“+” marks a count that is a floor: the access check stopped at 500 links of the group");
    expect(line).toContain("more than 100,000 links; types seen only past them are not named");
    expect(line).toContain("the access policy lets this console see");
  });

  it("names the cap that stopped each count, including a group that is not drawn", () => {
    const hub = parseRecordNeighbourhood(data.records[HUB]);
    const base: RecordNeighbourhoodRead = {
      ...hub, countBasis: "visible_to_reader", scanCap: 500, countCap: 100000, checkBudget: 400,
      groups: hub.groups.map((g) => ({ ...g, countIsFloor: false, floorReason: null })),
      relationships: hub.relationships.map((r) => ({ ...r, countIsFloor: false })),
      capped: { in: false, out: false }, cappedBy: { in: [], out: [] }, uncheckedLinks: { in: false, out: false },
    };
    expect(capClause(base, "check_budget")).toBe("the read stopped after checking 400 links");
    expect(capClause(base, "scan_cap")).toBe("the access check stopped at 500 links of the group");
    expect(capClause(base, "count_cap")).toBe("the count stopped at 100,000 links of the relationship");
    expect(capClauses(base)).toEqual([]);

    // A relationship is "3+" because a sibling group, hidden from the reader,
    // was capped: every group shown is exact, and the "+" is still explained.
    const sibling: RecordNeighbourhoodRead = {
      ...base,
      relationships: base.relationships.map((r) => (r.direction === "out" ? { ...r, countIsFloor: true } : r)),
      capped: { in: false, out: true }, cappedBy: { in: [], out: ["scan_cap"] },
    };
    expect(capClauses(sibling)).toEqual(["the access check stopped at 500 links of the group"]);
    expect(treeStatus(sibling, { expanded: null, drawn: 0, per: 5, hiddenKinds: 0 }))
      .toContain("“+” marks a count that is a floor: the access check stopped at 500 links of the group");
    expect(graphRecordText(sibling, (s) => s, context))
      .toContain("A count ending in + is a floor: the access check stopped at 500 links of the group.");
    expect(exploreLinkText(sibling, { d: "out", e: "contains", n: 3, floor: false })).not.toMatch(/All [0-9,]+ “contains”/);
  });

  it("says why a side with nothing visible is not zero, naming the cap that stopped it", () => {
    const hub = parseRecordNeighbourhood(data.records[HUB]);
    const outOnly: RecordNeighbourhoodRead = {
      ...hub, countBasis: "visible_to_reader", scanCap: 500, countCap: 100000, checkBudget: 400,
      groups: hub.groups.filter((g) => g.direction === "out").map((g) => ({ ...g, countIsFloor: false, floorReason: null })),
      relationships: hub.relationships.filter((r) => r.direction === "out"),
      capped: { in: true, out: false }, cappedBy: { in: ["check_budget"], out: [] }, uncheckedLinks: { in: false, out: false },
    };
    // The budget stopped the hidden in-group: the note names the budget, not the per-group cap.
    expect(limitNotes(outOnly)).toEqual([
      "Links to this record: none visible among the links checked, and more exist that were not checked (the read stopped after checking 400 links), so the number is not known",
    ]);
    const scan: RecordNeighbourhoodRead = { ...outOnly, cappedBy: { in: ["scan_cap"], out: [] } };
    expect(limitNotes(scan)[0]).toContain("(the access check stopped at 500 links of the group)");

    // A group the budget never reached is not named; its side says so.
    const unchecked: RecordNeighbourhoodRead = { ...outOnly, uncheckedLinks: { in: true, out: false } };
    expect(limitNotes(unchecked)).toEqual([
      "Links to this record: the read stopped before checking every relationship (it checks at most 400 links), so this side's count is a floor",
    ]);
    expect(graphRecordText(unchecked, (s) => s, context)).toContain("the read stopped before checking every relationship");

    const hidden: RecordNeighbourhoodRead = { ...outOnly, groups: [], relationships: [] };
    expect(hasNoRelationships(hidden)).toBe(false);
    expect(graphRecordText(hidden, (s) => s, context)).not.toContain("No relationships in this version.");
    const none: RecordNeighbourhoodRead = { ...hidden, capped: { in: false, out: false }, cappedBy: { in: [], out: [] } };
    expect(hasNoRelationships(none)).toBe(true);
    expect(graphRecordText(none, (s) => s, context)).toContain("No relationships in this version.");
  });

  it("refuses a floor reason it does not know, and reads an older record read without the new fields", () => {
    const raw = data.records[HUB] as { payload: Record<string, unknown> & { groups: Record<string, unknown>[] } };
    const bad = { ...raw, payload: { ...raw.payload, groups: raw.payload.groups.map((g) => ({ ...g, floor_reason: "guess" })) } };
    expect(() => parseRecordNeighbourhood(bad)).toThrow(SearchResponseError);
    const badSide = { ...raw, payload: { ...raw.payload, capped_by: { in: ["guess"], out: [] } } };
    expect(() => parseRecordNeighbourhood(badSide)).toThrow(SearchResponseError);
    const old = parseRecordNeighbourhood(raw);
    expect(old.uncheckedLinks).toEqual({ in: false, out: false });
    expect(old.cappedBy).toEqual({ in: [], out: [] });
    expect(old.checkBudget).toBeNull();
  });

  it("says how much of an open group is drawn and that the side panel lists the rest", () => {
    const g = groups.find((x) => x.n > 10) as TreeGroup;
    expect(treeStatus(read, { expanded: g, drawn: 14, per: 6, hiddenKinds: 0 })).toMatch(/^14 of [\d,]+ records drawn \(.+\); the side panel lists the first \d+ by id/);
  });
});

describe("the record page's URL", () => {
  const first = read.groups[0];

  it("names a group by direction, relationship and type, and an absent type by nothing", () => {
    expect(groupFromParam(read, groupParam(first))).toBe(first);
    const gone = absent.groups.find((g) => g.type === null);
    expect(gone).toBeDefined();
    expect(groupParam(gone!)).toMatch(/:$/);
    expect(groupFromParam(absent, groupParam(gone!))).toBe(gone);
    expect(toTreeGroups([gone!])[0].t).toBe(ABSENT_TYPE);
  });

  it("selects a listed neighbour, preferring the open group", () => {
    const id = first.items[0].id;
    expect(recordSelection(read, { sel: id }).sel?.group).toBe(first);
    const sel = recordSelection(read, { group: groupParam(first), sel: id });
    expect(sel.group).toBe(first);
    expect(graphIgnored({ id: read.record.id, group: groupParam(first), sel: id }, "record", recordChecks(read))).toEqual([]);
  });

  it("names every value it cannot honour instead of dropping it", () => {
    const params = { id: read.record.id, group: "out:nothing:nowhere", sel: "no:such", view: "table" };
    const s = recordSelection(read, params);
    expect(s.group).toBeNull();
    expect(s.sel).toBeNull();
    const ignored = graphIgnored(params, "record", recordChecks(read));
    expect(ignored.map((p) => p.name)).toEqual(["group", "sel", "view"]);
    const line = graphIgnoredSentence(ignored) ?? "";
    expect(line).toContain("group=out:nothing:nowhere");
    expect(line).toContain("sel=no:such");
    expect(line).toContain("view=table");
    expect(graphIgnoredSentence([])).toBeNull();
  });

  it("builds the record page, Explore's record page, type list and filtered Related tab", () => {
    expect(graphRecordHref(link, "a:b")).toBe("#console/graph/record?id=a%3Ab");
    expect(patchedHref(link, "graph/record", { id: "a:b", version: "x" }, { group: groupParam(first), sel: "c" }))
      .toBe(`#console/graph/record?id=a%3Ab&group=${encodeURIComponent(groupParam(first))}&sel=c`);
    expect(groupPatch(null)).toEqual({ group: null, sel: null });
    expect(selPatch(read, first, first, "c")).toEqual({ group: groupParam(first), sel: "c" });
    expect(mapTypeHref(link, "git_commit")).toBe("#console/graph?view=map&type=git_commit");
    expect(exploreRecordHref(link, "a:b")).toBe("#console/explore?record=a%3Ab");
    expect(exploreTypeHref(link, "git_commit")).toBe("#console/explore?type=git_commit");
    expect(relatedHref(link, "a:b", { direction: "in", relationship: "affects", type: "git_commit" }))
      .toBe("#console/explore?record=a%3Ab&tab=related&rel=affects&dir=incoming");
  });

  it("labels the Explore link by what Explore can filter: the relationship, not the type", () => {
    const g = toTreeGroups(many.groups).find((x) => many.groups.filter((y) => y.direction === x.d && y.relationship === x.e).length > 1) as TreeGroup;
    expect(exploreLinkText(many, g)).toMatch(/^All [\d,]+\+? “.+” links (to|from) this record in Explore$/);
    const single = toTreeGroups(read.groups).find((x) => read.groups.filter((y) => y.direction === x.d && y.relationship === x.e).length === 1) as TreeGroup;
    expect(exploreLinkText(read, single)).toMatch(/^All [\d,]+ in Explore$/);
  });
});

describe("the record read's parser", () => {
  const env = data.records[NODE_DETAIL] as { generation_id: string; payload: Record<string, unknown> };
  const rel = (env.payload.relationships as Record<string, unknown>[]);

  it("reads the server's per-relationship types-complete flag and count cap, and refuses a malformed one", () => {
    const withFlags = parseRecordNeighbourhood({
      ...env, payload: { ...env.payload, count_cap: 100000, relationships: rel.map((r) => ({ ...r, types_complete: false })) },
    });
    expect(withFlags.countCap).toBe(100000);
    expect(withFlags.relationships.every((r) => r.typesComplete === false)).toBe(true);
    expect(read.relationships.every((r) => r.typesComplete === null)).toBe(true);
    expect(() => parseRecordNeighbourhood({ ...env, payload: { ...env.payload, relationships: rel.map((r) => ({ ...r, types_complete: "no" })) } }))
      .toThrow(SearchResponseError);
    expect(() => parseRecordNeighbourhood({ ...env, payload: { ...env.payload, count_cap: -1 } })).toThrow(SearchResponseError);
  });

  it("reads the server's distinct-neighbour counts when sent, and refuses a malformed block", () => {
    expect(read.neighbours).toBeNull();
    const side = (count: number, floor: boolean) => ({ count, count_is_floor: floor });
    const withN = parseRecordNeighbourhood({
      ...env, payload: { ...env.payload, neighbours: { in: side(3, false), out: side(9, true), all: side(11, true) } },
    });
    expect(withN.neighbours).toEqual({
      in: { count: 3, countIsFloor: false }, out: { count: 9, countIsFloor: true }, all: { count: 11, countIsFloor: true },
    });
    expect(() => parseRecordNeighbourhood({ ...env, payload: { ...env.payload, neighbours: { in: side(3, false), out: side(9, true) } } }))
      .toThrow(SearchResponseError);
    expect(() => parseRecordNeighbourhood({ ...env, payload: { ...env.payload, neighbours: { in: side(-1, false), out: side(9, true), all: side(9, true) } } }))
      .toThrow(SearchResponseError);
  });
});

describe("the record page as text", () => {
  const name = (p: string) => p.replace(/^okg-workspace\./, "");

  it("copies both sides with their producers and the count basis, and no empty section", () => {
    const text = graphRecordText(read, name, context);
    expect(text).toContain("## Links to this record");
    expect(text).toContain("| Type | Relationship | Records | Made by |");
    expect(text).toContain("Counts are published records, before access rules.");
    expect(text).not.toContain("Open group");
    const inOnly = { ...read, groups: read.groups.filter((g) => g.direction === "in") };
    expect(graphRecordText(inOnly, name, context)).not.toContain("## Links from this record");
  });

  it("copies the open group's listed records and the selected record, as the page shows them", () => {
    const g = read.groups[0];
    const text = graphRecordText(read, name, context, { group: groupParam(g), sel: g.items[0].id });
    expect(text).toContain("## Open group:");
    expect(text).toContain(`\`${g.items[0].id}\``);
    expect(text).toContain("## Selected:");
  });

  it("says a record with no links has none", () => {
    const text = graphRecordText({ ...read, groups: [], relationships: [] }, name, context);
    expect(text).toContain("No relationships in this version.");
    expect(text).not.toContain("| Type |");
  });
});
