// The type map's pure parts: the layout (every type drawn, the strip, loops,
// one circle per producer above 40 types, the stepped run), its labels, the
// URL state, the legend's entries, the selected type as text, and the
// keyboard and pan helpers. Asserted against the converted okg-workspace
// reads the harness serves (25 types) and the harness's SYNTHETIC 85- and
// 140-type reads (design-harness/graphScaleFixtures.ts).
import { describe, expect, it } from "vitest";
import graphData from "@operator-ui-design-fixtures/graph/okg-workspace.json";
import { buildGraphModel, cat, CATEGORICAL, producerNamer } from "@operator-ui/console/model/graph/derive";
import { fitTransform, overlap } from "@operator-ui/console/model/graph/geometry";
import { parseGraphTypes } from "@operator-ui/console/model/graph/reads";
import { linkLines, placeMapLabels, shortProducerName } from "@operator-ui/console/model/graph/typeMapLabels";
import { type MeasureText, layoutSteps, layoutTypeMap, sourceNodeId } from "@operator-ui/console/model/graph/typeMapLayout";
import {
  COLLAPSE_ABOVE, legendEntries, mapChecks, mapOwners, mapPatch, producerPhrase, readMapState, typeAhead, typeSelectionText,
} from "@operator-ui/console/model/graph/typeMapModel";
import { graphIgnored, graphIgnoredSentence, patchedHref } from "@operator-ui/console/model/graph/params";
import { constrainToStage, nodeInDirection } from "@operator-ui/console/views/graph/typeMapDraw";
import { syntheticGraph } from "@operator-ui/design-harness/graphScaleFixtures";

const model = buildGraphModel(parseGraphTypes((graphData as { types: unknown }).types));
const big = buildGraphModel(parseGraphTypes(syntheticGraph("graph-scale-140", 140, 16, 400).types));
const mid = buildGraphModel(parseGraphTypes(syntheticGraph("graph-scale-85", 85, 12, 220).types));
/** A fixed-width measure: the layout is then the same on every machine. */
const measure: MeasureText = (text, px, mono) => text.length * px * (mono ? 0.62 : 0.55);
const STAGE = { W: 1100, H: 740, left: 236, right: 48 };
const link = (route: string, params: Record<string, string> = {}) => `#${route}?${new URLSearchParams(params).toString()}`;
const names = producerNamer([
  { name: "git-history", ownershipId: "okg-workspace.git-history", kind: "source" },
  { name: "git-files", ownershipId: "okg-workspace.git-files", kind: "source" },
  { name: "code_reference", ownershipId: "_enricher:code_reference", kind: "enricher" },
], link);
/** The same read, named by the producers read. */
const named = buildGraphModel(parseGraphTypes((graphData as { types: unknown }).types), names);

describe("the type map layout", () => {
  const layout = layoutTypeMap(model, measure, STAGE);

  it("draws every type once, the two with no relationship in a strip under the map", () => {
    expect(layout.nodes).toHaveLength(25);
    expect(layout.isolated.sort()).toEqual(["view", "view_definition"]);
    const strip = layout.nodes.filter((n) => n.strip);
    const lowest = Math.max(...layout.nodes.filter((n) => !n.strip).map((n) => n.y + n.r));
    for (const n of strip) expect(n.y - n.r).toBeGreaterThan(lowest);
    const keys = layout.links.map((l) => l.key);
    expect(new Set(keys).size).toBe(keys.length);
  });

  it("draws a type's relationships to itself as a loop with their total", () => {
    expect(layout.loops.code_symbol.n).toBe(131888 + 16764);
    expect(layout.loops.git_commit.n).toBe(17156);
    expect(layout.loops.pact_change.n).toBe(2);
  });

  it("keeps each direction's count on a line that carries both", () => {
    const l = layout.links.find((x) => x.key === "document|document_chunk");
    expect(l).toBeDefined();
    expect(l?.fwd && l?.bwd).toBe(true);
    expect(l?.nF).toBe(72066);
    expect(l?.nB).toBe(72066 + 470);
    const text = { producer: (s: string) => s };
    expect(linkLines(l!, "document", text)).toEqual(["→ contains 72k", "← member_of, references 73k"]);
    expect(linkLines(l!, "document_chunk", text)).toEqual(["→ member_of, references 73k", "← contains 72k"]);
    expect(linkLines(l!, null, text)).toEqual(["document contains document_chunk · 72k", "document_chunk member_of, references document · 73k"]);
  });

  it("is the same on every run, and the same run in steps", () => {
    const again = layoutTypeMap(model, measure, STAGE);
    expect(again.nodes.map((n) => [n.x, n.y])).toEqual(layout.nodes.map((n) => [n.x, n.y]));
    const steps = layoutSteps(model, measure, STAGE);
    let r = steps.next();
    let pauses = 0;
    while (!r.done) { pauses += 1; r = steps.next(); }
    expect(pauses).toBeGreaterThan(10);
    expect(r.value.nodes.map((n) => [n.x, n.y])).toEqual(layout.nodes.map((n) => [n.x, n.y]));
  });

  it("leaves no two circles overlapping", () => {
    for (let i = 0; i < layout.nodes.length; i += 1) {
      for (let j = i + 1; j < layout.nodes.length; j += 1) {
        const a = layout.nodes[i];
        const b = layout.nodes[j];
        expect(Math.hypot(a.x - b.x, a.y - b.y)).toBeGreaterThan(a.r + b.r);
      }
    }
  });
});

describe("the type map above 40 types", () => {
  it("draws one circle per producer that colours several types, and each type exactly once", () => {
    expect(big.types.length).toBeGreaterThan(COLLAPSE_ABOVE);
    const layout = layoutTypeMap(big, measure, STAGE, new Set());
    const owners = mapOwners(big);
    expect(layout.nodes.length).toBe(owners.length);
    expect(layout.nodes.flatMap((n) => n.types).sort()).toEqual(big.types.slice().sort());
    for (const n of layout.nodes) if (n.kind === "source") expect(n.types.length).toBeGreaterThan(1);
  });

  it("opens one producer into its types and leaves the others folded", () => {
    const src = mapOwners(big)[0];
    const layout = layoutTypeMap(big, measure, STAGE, new Set([src]));
    const mine = big.types.filter((t) => big.typeSource(t) === src);
    for (const t of mine) expect(layout.byId[t]?.kind).toBe("type");
    expect(layout.byId[sourceNodeId(src)]).toBeUndefined();
    const other = mapOwners(big).find((s) => big.types.filter((t) => big.typeSource(t) === s).length > 1 && s !== src) as string;
    expect(layout.byId[sourceNodeId(other)]?.kind).toBe("source");
    expect(layout.drawnAs(mine[0])).toBe(mine[0]);
  });

  it("pauses often enough, with every producer open, to run a slice at a time", () => {
    const steps = layoutSteps(big, measure, STAGE, new Set(mapOwners(big)));
    let pauses = 0;
    for (let r = steps.next(); !r.done; r = steps.next()) pauses += 1;
    expect(pauses).toBeGreaterThan(20);
  });
});

describe("the type map's labels", () => {
  const layout = layoutTypeMap(model, measure, STAGE);
  const t = fitTransform(layout.nodes, STAGE, 28);
  const legend = { x0: 14, x1: 224, y0: 12, y1: 300 };
  const frame = { transform: t, W: STAGE.W, H: STAGE.H, furniture: [legend] };
  const text = { producer: named.name };

  it("labels the focused type, never overlaps two unforced labels, and keeps labels off the legend", () => {
    const out = placeMapLabels(layout, frame, "source_file", { node: null, link: null }, text, measure);
    expect(out.some((o) => o.kind === "node" && o.node?.id === "source_file")).toBe(true);
    expect(out.some((o) => o.kind === "edge")).toBe(true);
    const free = out.filter((o) => !o.force);
    for (let i = 0; i < free.length; i += 1) {
      expect(overlap(free[i].b, legend)).toBe(false);
      for (let j = i + 1; j < free.length; j += 1) expect(overlap(free[i].b, free[j].b)).toBe(false);
    }
  });

  it("names the focused type's loop, and captions the strip", () => {
    const out = placeMapLabels(layout, frame, "code_symbol", { node: null, link: null }, text, measure);
    expect(out.some((o) => o.kind === "edge" && o.lines?.[0] === "↻ calls, member_of 149k")).toBe(true);
    expect(out.some((o) => o.kind === "cap" && o.lines?.[0] === "NO RELATIONSHIPS")).toBe(true);
    expect(out.some((o) => o.kind === "cap" && o.lines?.[0] === "GIT-HISTORY")).toBe(true);
  });

  it("names a hovered type while another is selected", () => {
    const out = placeMapLabels(layout, frame, "source_file", { node: "view", link: null }, text, measure);
    expect(out.some((o) => o.kind === "node" && o.node?.id === "view")).toBe(true);
  });

  it("labels nothing a hidden producer colours", () => {
    const shown = (n: { source: string }) => n.source !== "okg-workspace.pact";
    const out = placeMapLabels(layout, frame, null, { node: null, link: null }, text, measure, shown);
    expect(out.some((o) => o.kind === "node" && o.node?.source === "okg-workspace.pact")).toBe(false);
    expect(out.some((o) => o.kind === "cap" && o.cluster === "okg-workspace.pact")).toBe(false);
  });

  it("on a 390 px stage, lets producer captions give way to circles and drops the deployment's prefix", () => {
    const PHONE = { W: 390, H: 520, left: 0, right: 0 };
    const small = layoutTypeMap(model, measure, PHONE);
    const tp = fitTransform(small.nodes, PHONE, 28);
    const raw = { producer: (s: string) => s, shortProducer: (s: string) => shortProducerName(s, "okg-workspace") };
    const none = { node: null, link: null };
    const caps = placeMapLabels(small, { transform: tp, W: 390, H: 520, furniture: [] }, null, none, raw, measure)
      .filter((o) => o.kind === "cap" && o.cluster);
    expect(caps.length).toBeGreaterThan(0);
    const circles = small.nodes.map((n) => {
      const [x, y, r] = [tp.x + n.x * tp.k, tp.y + n.y * tp.k, n.r * tp.k];
      return { x0: x - r, x1: x + r, y0: y - r, y1: y + r };
    });
    for (const c of caps) {
      expect(c.force).toBe(false);
      expect(c.lines?.[0]).not.toMatch(/^OKG-WORKSPACE\./);
      for (const b of circles) expect(overlap(c.b, b)).toBe(false);
    }
    // No caption sits in the strip's corner, where it reads as a strip
    // circle's name (GIT-HISTORY beside view_definition, walk-through 09-28).
    const strip = small.nodes.filter((n) => n.strip).map((n) => [tp.x + n.x * tp.k, tp.y + n.y * tp.k, n.r * tp.k]);
    expect(strip.length).toBeGreaterThan(0);
    const zone = {
      x0: Math.min(...strip.map(([x, , r]) => x - r)) - 12, x1: Math.max(...strip.map(([x, , r]) => x + r)) + 90,
      y0: Math.min(...strip.map(([, y, r]) => y - r)) - 40, y1: Math.max(...strip.map(([, y, r]) => y + r)) + 34,
    };
    for (const c of caps) expect(overlap(c.b, zone), c.lines?.[0]).toBe(false);
    // While the producers read is pending, a caption is the ownership id as
    // stored (drawn in monospace), never upper-cased as if it were a name.
    const pending = placeMapLabels(layout, frame, null, none, { ...raw, ids: true }, measure)
      .filter((o) => o.kind === "cap" && o.cluster);
    expect(pending.length).toBeGreaterThan(0);
    for (const c of pending) {
      expect(c.id).toBe(true);
      expect(c.lines?.[0]).toBe(c.cluster);
    }
    // The desktop stage keeps the approved captions: full name, always shown.
    const wide = placeMapLabels(layout, frame, null, none, raw, measure).filter((o) => o.kind === "cap" && o.cluster);
    expect(wide.length).toBeGreaterThan(0);
    for (const c of wide) {
      expect(c.force).toBe(true);
      expect(c.lines?.[0]).toMatch(/^OKG-WORKSPACE\./);
    }
  });
});

describe("the fit", () => {
  it("is the identity, not NaN, with nothing to fit", () => {
    const t = fitTransform([], STAGE, 28);
    expect([t.x, t.y, t.k].every(Number.isFinite)).toBe(true);
  });
});

describe("the map's URL", () => {
  it("reads the selected type, the hidden producers, and names what it cannot honour", () => {
    const s = readMapState({ type: "source_file", hide: "okg-workspace.pact,nobody" }, model);
    expect(s.type).toBe("source_file");
    expect([...s.hide]).toEqual(["okg-workspace.pact"]);
    expect(s.expand).toBeNull();
    const line = (params: Record<string, string>) => graphIgnoredSentence(graphIgnored(params, "map", mapChecks(model)));
    expect(line({ type: "source_file", hide: "okg-workspace.pact,nobody" })).toContain("hide=okg-workspace.pact,nobody");
    expect(line({ type: "source_file", hide: "okg-workspace.pact,nobody" })).toContain("ownership id nobody");
    const badParams = { type: "no_such_type", expand: "okg-workspace.pact" };
    expect(readMapState(badParams, model).type).toBeNull();
    expect(line(badParams)).toContain("type=no_such_type");
    expect(line(badParams)).toContain(`there are ${COLLAPSE_ABOVE} or fewer`);
    expect(line({})).toBeNull();
  });

  it("does not offer a producer that colours no type as something to hide", () => {
    expect([...readMapState({ hide: "okg-workspace.git-files" }, model).hide]).toEqual([]);
    expect(graphIgnoredSentence(graphIgnored({ hide: "okg-workspace.git-files" }, "map", mapChecks(model)))).toContain("hide=okg-workspace.git-files");
  });

  it("opens the selected type's producer above 40 types", () => {
    const type = big.types[5];
    const s = readMapState({ type }, big);
    expect(s.expand?.has(big.typeSource(type))).toBe(true);
  });

  it("changes only what it is asked to, keeps the view, and leaves the version to the link", () => {
    const params = { view: "map", type: "a", hide: "x", version: "v" };
    const href = (next: Parameters<typeof mapPatch>[0]) => patchedHref(link, "graph", params, mapPatch(next));
    expect(href({ type: null })).toBe("#graph?view=map&hide=x");
    expect(href({ hide: [] })).toBe("#graph?view=map&type=a");
    expect(href({ expand: ["p", "q"] })).toBe("#graph?view=map&type=a&hide=x&expand=p%2Cq");
  });
});

describe("the legend and the producers' names", () => {
  it("lists only the producers that colour a type", () => {
    const entries = legendEntries(model);
    expect(entries.map((e) => e.key)).not.toContain("okg-workspace.git-files");
    expect(entries).toHaveLength(8);
  });

  it("groups the producers past the eighth colour into one neutral entry", () => {
    const entries = legendEntries(big);
    const group = entries.find((e) => e.key === "smaller");
    expect(group?.members.length).toBeGreaterThan(1);
    expect(group?.color).toBe(cat(CATEGORICAL));
    expect(entries.filter((e) => e.key !== "smaller").every((e) => e.color !== cat(CATEGORICAL))).toBe(true);
    expect(entries.flatMap((e) => e.members).sort()).toEqual(mapOwners(big).slice().sort());
    expect(legendEntries(mid).length).toBeLessThanOrEqual(9);
  });

  it("names a producer as the producers read does, and never invents a name", () => {
    expect(named.name("okg-workspace.git-history")).toBe("git-history");
    expect(named.name("_enricher:declarative_alias:x")).toBe("_enricher:declarative_alias:x");
    expect(model.name("okg-workspace.git-history")).toBe("okg-workspace.git-history");
    expect(named.label("okg-workspace.git-history").href).toBe("#sources/git-history?");
    expect(named.label("okg-workspace.pact").href).toBeNull();
    expect(producerPhrase(named.label("_enricher:code_reference"))).toBe("the code_reference enricher");
  });
});

describe("the selected type as text", () => {
  it("names the type, its producers and its relationships in each direction", () => {
    const lines = typeSelectionText(named, "code_symbol", false);
    expect(lines[0]).toBe("## Selected type `code_symbol`");
    const text = lines.join("\n");
    expect(text).toContain("### Links to itself (2)");
    expect(text).toContain("| calls | itself | 131,888 |");
    expect(text).toContain("### Linked from");
    expect(typeSelectionText(named, "code_symbol", true).join("\n")).toContain("Hidden on the map");
  });
});

describe("the keyboard and the pan limit", () => {
  it("jumps to the largest type a typed prefix starts, else one that contains it", () => {
    expect(typeAhead(model.types, "sour")).toBe("source_file");
    expect(typeAhead(model.types, "chunk")).toBe("document_chunk");
    expect(typeAhead(model.types, "zzz")).toBeNull();
  });

  it("moves an arrow key to the nearest node in that direction", () => {
    const nodes = [{ id: "a", x: 0, y: 0 }, { id: "b", x: 100, y: 5 }, { id: "c", x: 60, y: 80 }, { id: "d", x: -50, y: 0 }];
    expect(nodeInDirection(nodes[0], nodes, "ArrowRight")?.id).toBe("b");
    expect(nodeInDirection(nodes[0], nodes, "ArrowDown")?.id).toBe("c");
    expect(nodeInDirection(nodes[0], nodes, "ArrowLeft")?.id).toBe("d");
    expect(nodeInDirection(nodes[0], nodes, "ArrowUp")).toBeNull();
  });

  it("keeps a circle on the stage however far the map is dragged", () => {
    const pts = [{ x: 0, y: 0 }, { x: 1000, y: 500 }];
    expect(constrainToStage({ x: 100, y: 100, k: 1 }, pts, 800, 600)).toEqual({ dx: 0, dy: 0 });
    // dragged far right: the nearest circle (x 0) comes back to 40 px inside
    expect(constrainToStage({ x: 5000, y: 100, k: 1 }, pts, 800, 600)).toEqual({ dx: 760 - 5000, dy: 0 });
    // dragged far up and left: the circle at (1000, 500) comes back
    expect(constrainToStage({ x: -5000, y: -5000, k: 1 }, pts, 800, 600)).toEqual({ dx: 40 - (-4000), dy: 40 - (-4500) });
    expect(constrainToStage({ x: 0, y: 0, k: 1 }, [], 800, 600)).toEqual({ dx: 0, dy: 0 });
  });
});
