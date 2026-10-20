// The Graph view's model: the strict parsers, the derived model, the layout of
// the type map and its labels and the grid maths (the record tree's plan is
// in recordTree.test.ts).
// Asserted against the converted okg-workspace reads the harness serves, so
// the numbers are real ones.
import { describe, expect, it } from "vitest";
import graphData from "@operator-ui-design-fixtures/graph/okg-workspace.json";
import { SearchResponseError } from "@operator-ui/console/api/searchData";
import { NO_SOURCE, buildGraphModel, buildSourceLinks, clusterOrder, short } from "@operator-ui/console/model/graph/derive";
import { type Box, fitTransform, linkPath, overlap, placeLabels, qpt } from "@operator-ui/console/model/graph/geometry";
import { buildMatrix, heatShare, sourceSourceMatrix, typeTypeMatrix } from "@operator-ui/console/model/graph/grid";
import { parseGraphTypes, parseOntology, parseRecordNeighbourhood, parseSourceLinks } from "@operator-ui/console/model/graph/reads";
import { ABSENT_TYPE, sideTotal, toTreeGroups } from "@operator-ui/console/model/graph/recordTree";

const data = graphData as unknown as {
  types: unknown; source_links: unknown; ontology: unknown; records: Record<string, unknown>;
};
const types = parseGraphTypes(data.types);
const model = buildGraphModel(types);
const ontology = parseOntology(data.ontology);
const NODE_DETAIL = "file:okg:src/okg/substrate/console_views/node_detail.py";
const STAGE = { W: 1100, H: 740, left: 236, right: 48 };

describe("the Graph read parsers", () => {
  it("read the converted okg-workspace answers", () => {
    expect(types.totals).toEqual({ records: 822142, relationships: 1571775 });
    expect(types.types).toHaveLength(25);
    expect(parseSourceLinks(data.source_links).rows.length).toBeGreaterThan(0);
    expect(ontology.types).toHaveLength(140);
    const record = parseRecordNeighbourhood(data.records[NODE_DETAIL]);
    expect(record.record.type).toBe("source_file");
    expect(record.groups.reduce((a, g) => a + g.count, 0)).toBe(107);
  });

  it("refuse an answer with a missing or malformed field rather than defaulting it", () => {
    /** The same answer with some payload keys changed. */
    const changed = (read: unknown, changes: Record<string, unknown>) => {
      const env = read as { generation_id: string; payload: Record<string, unknown> };
      return { ...env, payload: { ...env.payload, ...changes } };
    };
    expect(() => parseGraphTypes(changed(data.types, { totals: undefined }))).toThrow(SearchResponseError);
    expect(() => parseGraphTypes(changed(data.types, { types: [{ type: "x", count: -1 }] }))).toThrow(SearchResponseError);
    expect(() => parseGraphTypes(changed(data.types, { sources: [{ source: "s", kind: "robot", records: 0, relationships: 0 }] }))).toThrow(/robot/);
    expect(() => parseGraphTypes((data.types as { payload: unknown }).payload)).toThrow(SearchResponseError);
    expect(() => parseRecordNeighbourhood(changed(data.records[NODE_DETAIL], { groups: [{ direction: "sideways" }] }))).toThrow(SearchResponseError);
    expect(() => parseRecordNeighbourhood(changed(data.records[NODE_DETAIL], { relationships: undefined }))).toThrow(SearchResponseError);
    expect(() => parseSourceLinks(changed(data.source_links, { cached: "no" }))).toThrow(SearchResponseError);
    expect(() => parseOntology(changed(data.ontology, { fill: { t: { population: 1, sampled: 1, method: "m", attributes: [{ name: "a", fill_rate: 1.5, declared: true }], declared_never_seen: [] } } }))).toThrow(SearchResponseError);
  });

  it("read the catalog version as the server sends it: an integer", () => {
    const env = data.ontology as { generation_id: string; payload: Record<string, unknown> };
    const read = parseOntology({ ...env, payload: { ...env.payload, catalog_version_id: 12 } });
    expect(read.catalogVersionId).toBe("12");
    expect(() => parseOntology({ ...env, payload: { ...env.payload, catalog_version_id: 1.5 } })).toThrow(SearchResponseError);
  });

  it("accept what the contract allows to be absent: a neighbour's type, a link's source", () => {
    const env = data.records[NODE_DETAIL] as { generation_id: string; payload: Record<string, unknown> };
    const groups = (env.payload.groups as Record<string, unknown>[]).map((g, i) => (i === 0 ? { ...g, type: null } : g));
    const read = parseRecordNeighbourhood({ ...env, payload: { ...env.payload, groups } });
    expect(toTreeGroups(read.groups)[0].t).toBe(ABSENT_TYPE);
    const links = data.source_links as { generation_id: string; payload: Record<string, unknown> };
    const rows = [{ from_source: null, to_source: "okg-workspace.pact", producer: "p", count: 3 }];
    const parsed = parseSourceLinks({ ...links, payload: { ...links.payload, rows } });
    expect(buildSourceLinks(parsed, model).sources).toContain(NO_SOURCE);
  });

  it("totals a record's sides from the exact per-relationship counts", () => {
    const read = parseRecordNeighbourhood(data.records[NODE_DETAIL]);
    expect(sideTotal(read, "in")).toEqual({ n: 53, floor: false });
    expect(sideTotal({ ...read, capped: { in: false, out: true } }, "out").floor).toBe(true);
  });
});

describe("the derived model", () => {
  it("orders sources by the records they make and colours types by their biggest source", () => {
    expect(model.sources[0]).toBe("okg-workspace.code-structure");
    expect(model.srcColor[model.sources[0]]).toBe("var(--okg-graph-cat-1)");
    expect(model.typeSource("source_file")).toBe("okg-workspace.git-history");
    expect(model.enrichers.map(([p]) => p).every((p) => p.startsWith("_enricher:"))).toBe(true);
    expect(model.patterns[0].n).toBeGreaterThanOrEqual(model.patterns[1].n);
  });

  it("prints counts short", () => {
    expect([short(822142), short(1571775), short(5091), short(999)]).toEqual(["822k", "1.6M", "5.1k", "999"]);
  });

  it("sets the ring's clusters largest first, whatever order they arrive in", () => {
    const clusters = [...new Set(model.types.map(model.typeSource))].reverse();
    const order = clusterOrder(clusters, model);
    expect([...order].sort()).toEqual([...clusters].sort());
    expect(order[0]).toBe("okg-workspace.code-structure");
    for (let i = 1; i < order.length; i += 1) {
      expect(model.srcTotal[order[i - 1]] ?? 0).toBeGreaterThanOrEqual(model.srcTotal[order[i]] ?? 0);
    }
  });
});

describe("geometry", () => {
  it("detects overlap only where boxes share area", () => {
    expect(overlap({ x0: 0, x1: 10, y0: 0, y1: 10 }, { x0: 5, x1: 15, y0: 5, y1: 15 })).toBe(true);
    expect(overlap({ x0: 0, x1: 10, y0: 0, y1: 10 }, { x0: 10, x1: 20, y0: 0, y1: 10 })).toBe(false);
  });

  it("ends a link on both rims", () => {
    const g = linkPath({ x: 0, y: 0, r: 10 }, { x: 100, y: 0, r: 5 }, 0);
    expect(g.x1).toBeCloseTo(12);
    expect(g.x2).toBeCloseTo(93);
    expect(qpt(g, 0)).toEqual([g.x1, g.y1]);
  });

  it("places a label in its first free box, skips a blocked one, and drops an unforced one with no room", () => {
    const blocked: Box = { x0: 0, x1: 50, y0: 0, y1: 50 };
    const a = { x0: 10, x1: 40, y0: 10, y1: 20 };
    const b = { x0: 60, x1: 90, y0: 10, y1: 20 };
    const placed = [blocked];
    const out = placeLabels([{ kind: "x", cands: [a, b] }, { kind: "y", cands: [a] }], placed, [], 200, 200);
    expect(out.map((o) => o.b)).toEqual([b]);
    const forced = placeLabels([{ kind: "z", cands: [{ x0: -20, x1: 10, y0: 10, y1: 20 }], force: true }], [], [], 200, 200);
    expect(forced[0].b.x0).toBe(4);
  });

  it("prefers the box that crosses fewer drawn lines", () => {
    const a = { x0: 10, x1: 40, y0: 10, y1: 20 };
    const b = { x0: 60, x1: 90, y0: 10, y1: 20 };
    const out = placeLabels([{ kind: "x", cands: [a, b] }], [], [[20, 15, 1]], 200, 200);
    expect(out[0].b).toEqual(b);
  });

  it("fits within the scale bounds and centres the extent", () => {
    const t = fitTransform([{ x: -100, y: 0, r: 10 }, { x: 100, y: 0, r: 10 }], STAGE, 28);
    expect(t.k).toBeGreaterThanOrEqual(0.2);
    expect(t.k).toBeLessThanOrEqual(1.5);
    const tiny = fitTransform([{ x: -5000, y: 0, r: 10 }, { x: 5000, y: 0, r: 10 }], STAGE, 28);
    expect(tiny.k).toBe(0.2);
  });
});

// The type map layout and its labels: typeMap.test.ts.

describe("the grid maths", () => {
  it("totals rows, columns and the whole", () => {
    const m = buildMatrix(["a", "b"], ["x", "y"], (r, c) => (r === "a" ? 1 : 2) * (c === "x" ? 1 : 10));
    expect(m.rowTot).toEqual([11, 22]);
    expect(m.colTot).toEqual([3, 30]);
    expect(m.grand).toBe(33);
    expect(m.max).toBe(20);
  });

  it("shades between 10 and 60 per cent", () => {
    expect(heatShare(0, 100)).toBe(10);
    expect(heatShare(100, 100)).toBe(60);
    expect(heatShare(10, 100)).toBeGreaterThan(10);
  });

  it("builds the three grids from the reads", () => {
    const tt = typeTypeMatrix(model);
    expect(tt.unlinked.sort()).toEqual(["view", "view_definition"]);
    expect(tt.matrix.grand).toBe(model.patterns.reduce((a, p) => a + p.n, 0));
    const links = parseSourceLinks(data.source_links);
    expect(sourceSourceMatrix(buildSourceLinks(links, model)).grand).toBe(links.rows.reduce((a, r) => a + r.count, 0));
  });
});
