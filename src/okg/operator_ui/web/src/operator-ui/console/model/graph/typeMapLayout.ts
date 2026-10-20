// The type map's layout, run to rest before the first paint (no animation).
//
// Ported from the approved prototype (v2.html, drawTypes): types cluster
// around the producer that makes most of their records; clusters sit on a
// ring, largest producer first; each node owns a box (circle plus the label
// under it) that may not overlap another, with a wider gap between clusters;
// label boxes are sized for the scale the map will be shown at. Pure: text
// width comes in as a function, so the layout is the same in a test.
//
// Above COLLAPSE_ABOVE types the map draws one circle per producer (its
// types folded in) except the producers the URL opens (`expand`). The layout
// is a function of the model and that set alone, so the same URL always
// draws the same map. Hiding a producer in the legend does not re-run it:
// hidden nodes fade in place (views/graph/typeMapDraw.ts).
//
// A type with no relationship at all is drawn in a strip under the map, so it
// can still be picked; a relationship from a type to itself is a loop on its
// circle.
//
// Cost: the box separation sweeps boxes sorted by x (only boxes that can
// touch are compared) and stops once nothing moves; the bend search checks
// only the circles and labels near each candidate curve. Measured in
// typeMap.test.ts at 140 types.
import { forceLink, forceManyBody, forceSimulation, forceX, forceY, type SimulationNodeDatum } from "d3-force";
import { type GraphModel, type Pattern, clusterOrder, ringPoint, short } from "@operator-ui/console/model/graph/derive";
import { type Box, type LinkGeometry, fitTransform, linkPath, qpt } from "@operator-ui/console/model/graph/geometry";

export type MeasureText = (text: string, px: number, mono: boolean) => number;

export interface TypeNode extends SimulationNodeDatum {
  /** The type's name, or `source:<ownership id>` for a producer's circle. */
  id: string;
  kind: "type" | "source";
  /** The producer that colours it (makes most of its records). */
  source: string;
  /** The record types it stands for: one for a type, all a folded producer colours. */
  types: string[];
  r: number;
  n: number;
  color: string;
  /** The label's width in screen pixels, as the layout sizes its box. */
  lw: number;
  /** In the "No relationships" strip under the map. */
  strip: boolean;
  x: number;
  y: number;
}

export interface TypeLink {
  key: string;
  s: TypeNode;
  d: TypeNode;
  n: number;
  /** Relationships from s to d, and from d to s. */
  nF: number;
  nB: number;
  pats: Pattern[];
  fwd: boolean;
  bwd: boolean;
  bend: number;
  geo: LinkGeometry;
}

/** Relationships from a drawn node to itself. */
export interface TypeLoop {
  node: TypeNode;
  n: number;
  pats: Pattern[];
}

export interface TypeMapLayout {
  nodes: TypeNode[];
  links: TypeLink[];
  loops: Record<string, TypeLoop>;
  /** Ids of the nodes in the "No relationships" strip. */
  isolated: string[];
  /** Where the strip's caption goes, in layout units; null with no strip. */
  strip: Box | null;
  clusters: string[];
  byId: Record<string, TypeNode>;
  /** The node a type is drawn as: itself, or its folded producer. */
  drawnAs: (type: string) => string;
  width: (n: number) => number;
  /** Recompute one link's path after a drag (the bend is kept). */
  reshape: (link: TypeLink) => void;
}

export interface Stage { W: number; H: number; left: number; right: number }

export const sourceNodeId = (source: string): string => `source:${source}`;

const BEND_CANDIDATES = [0.08, -0.08, 0.16, -0.16, 0.26, -0.26, 0];

/** The layout, run to the end at once. */
export function layoutTypeMap(
  model: GraphModel, measure: MeasureText, stage: Stage, expand: ReadonlySet<string> | null = null,
): TypeMapLayout {
  const steps = layoutSteps(model, measure, stage, expand);
  let r = steps.next();
  while (!r.done) r = steps.next();
  return r.value;
}

/**
 * The layout in steps: it pauses (yields) every few simulation ticks, every
 * few relaxation passes and every few lines' bend search, so the drawing can
 * run it a slice at a time and never hold the main thread for long
 * (views/graph/typeMapDraw.ts). The result is the same as run at once.
 */
export function* layoutSteps(
  model: GraphModel, measure: MeasureText, stage: Stage, expand: ReadonlySet<string> | null = null,
): Generator<void, TypeMapLayout, void> {
  const { count, patterns, typeSource } = model;
  // A producer that colours a single type is drawn as that type: folding it
  // would only rename the circle.
  const owned: Record<string, number> = {};
  for (const t of model.types) owned[typeSource(t)] = (owned[typeSource(t)] ?? 0) + 1;
  const folded = (s: string) => expand !== null && !expand.has(s) && owned[s] > 1;
  const drawnAs = (t: string) => (folded(typeSource(t)) ? sourceNodeId(typeSource(t)) : t);

  // The drawn nodes: a type, or one circle per folded producer.
  const specs = new Map<string, { kind: "type" | "source"; source: string; types: string[]; n: number }>();
  for (const t of model.types) {
    const id = drawnAs(t);
    const src = typeSource(t);
    const spec = specs.get(id) ?? { kind: id === t ? "type" as const : "source" as const, source: src, types: [], n: 0 };
    spec.types.push(t);
    spec.n += count[t];
    specs.set(id, spec);
  }
  const maxC = Math.max(1, ...[...specs.values()].map((s) => s.n));
  // d3.scaleSqrt().domain([1, maxC]).range([8, 38]): size grows with records.
  const rScale = (v: number) => {
    const span = Math.sqrt(maxC) - 1;
    return span > 0 ? 8 + ((Math.sqrt(Math.max(1, v)) - 1) / span) * 30 : 8;
  };
  const prefix = `${model.deployment}.`;
  const nodes: TypeNode[] = [...specs.entries()].map(([id, s]) => {
    const title = s.kind === "type" ? id : (s.source.startsWith(prefix) ? s.source.slice(prefix.length) : s.source);
    const sub = s.kind === "type" ? short(s.n) : `${s.types.length} types · ${short(s.n)} records`;
    return {
      id,
      kind: s.kind,
      source: s.source,
      types: s.types,
      r: rScale(s.n),
      n: s.n,
      color: model.srcColor[s.source] ?? model.typeColor(s.types[0]),
      lw: Math.max(measure(title, 12, s.kind === "type"), measure(sub, 11, false)) + 10,
      strip: false,
      x: 0,
      y: 0,
    };
  });
  const byId: Record<string, TypeNode> = Object.fromEntries(nodes.map((n) => [n.id, n]));

  // One line per pair of drawn nodes, whatever the number of relationship
  // kinds between them; arrowheads give each direction that exists.
  const pairs: Record<string, TypeLink> = {};
  const loops: Record<string, TypeLoop> = {};
  for (const p of patterns) {
    const a0 = drawnAs(p.s);
    const b0 = drawnAs(p.d);
    if (!byId[a0] || !byId[b0]) continue;
    if (a0 === b0) {
      const lp = (loops[a0] ??= { node: byId[a0], n: 0, pats: [] });
      lp.n += p.n;
      lp.pats.push(p);
      continue;
    }
    const [a, b] = [a0, b0].sort();
    const key = `${a}|${b}`;
    const q = (pairs[key] ??= {
      key, s: byId[a], d: byId[b], n: 0, nF: 0, nB: 0, pats: [], fwd: false, bwd: false, bend: 0, geo: linkPath(byId[a], byId[b], 0),
    });
    q.n += p.n;
    q.pats.push(p);
    if (a0 === a) { q.fwd = true; q.nF += p.n; } else { q.bwd = true; q.nB += p.n; }
  }
  const links = Object.values(pairs);
  const related = new Set([...links.flatMap((l) => [l.s.id, l.d.id]), ...Object.keys(loops)]);
  for (const n of nodes) n.strip = !related.has(n.id);
  const sim = nodes.filter((n) => !n.strip);
  const stripNodes = nodes.filter((n) => n.strip);
  const maxP = Math.max(1, ...links.map((l) => l.n), ...Object.values(loops).map((l) => l.n));
  const width = (n: number) => 1 + (2.6 * Math.log10(1 + n)) / Math.log10(1 + maxP);
  const clusters = clusterOrder([...new Set(sim.map((n) => n.source))], model);

  const ring = 110 + 34 * clusters.length;
  const centre: Record<string, [number, number]> = {};
  clusters.forEach((c, i) => {
    const [x, y] = ringPoint(i, clusters.length);
    centre[c] = [x * ring, y * ring];
  });
  sim.forEach((n, i) => {
    const c = centre[n.source];
    n.x = c[0] + Math.cos(i) * 30;
    n.y = c[1] + Math.sin(i) * 30;
  });
  let ls = 1; // label scale in layout units
  const count0 = sim.length;
  const bx0 = new Float64Array(count0);
  const bx1 = new Float64Array(count0);
  const by0 = new Float64Array(count0);
  const by1 = new Float64Array(count0);
  const order = sim.map((_, i) => i);
  const boxes = () => {
    for (let i = 0; i < count0; i += 1) {
      const n = sim[i];
      const hw = Math.max(n.r, (ls * n.lw) / 2);
      bx0[i] = n.x - hw; bx1[i] = n.x + hw; by0[i] = n.y - n.r; by1[i] = n.y + n.r + ls * 30;
    }
    order.sort((a, b) => bx0[a] - bx0[b]);
  };
  const shift = (i: number, dx: number, dy: number) => {
    sim[i].x += dx; sim[i].y += dy; bx0[i] += dx; bx1[i] += dx; by0[i] += dy; by1[i] += dy;
  };
  // Push apart every two boxes that overlap (plus a gap); returns the
  // largest push, so a relaxation can stop once nothing moves.
  const separate = (strength: number, alpha = 1): number => {
    boxes();
    const far = 48 * ls;
    let most = 0;
    for (let ii = 0; ii < count0; ii += 1) {
      const i = order[ii];
      for (let jj = ii + 1; jj < count0; jj += 1) {
        const j = order[jj];
        if (bx0[j] >= bx1[i] + far) break;
        const gap = sim[i].source === sim[j].source ? 18 * ls : far;
        const ox = Math.min(bx1[i], bx1[j]) - Math.max(bx0[i], bx0[j]) + gap;
        const oy = Math.min(by1[i], by1[j]) - Math.max(by0[i], by0[j]) + gap;
        if (ox <= 0 || oy <= 0) continue;
        const s = strength * alpha;
        if (ox < oy) {
          const d = (bx0[i] + bx1[i] < bx0[j] + bx1[j] ? -1 : 1) * (ox / 2) * s;
          shift(i, d, 0); shift(j, -d, 0);
          most = Math.max(most, Math.abs(d));
        } else {
          const d = (by0[i] + by1[i] < by0[j] + by1[j] ? -1 : 1) * (oy / 2) * s;
          shift(i, 0, d); shift(j, 0, -d);
          most = Math.max(most, Math.abs(d));
        }
      }
    }
    return most;
  };
  const simLinks = links.map((l) => ({ source: l.s.id, target: l.d.id, same: l.s.source === l.d.source }));
  type SimLink = { source: TypeNode; target: TypeNode; same: boolean };
  const simulation = forceSimulation(sim)
    .force("link", forceLink<TypeNode, SimLink>(simLinks as unknown as SimLink[]).id((d) => d.id)
      .distance((l) => (l.same ? 92 : 120) + l.source.r + l.target.r)
      .strength((l) => (l.same ? 0.5 : 0.03)))
    .force("charge", forceManyBody().strength(-240).distanceMax(300))
    .force("box", (alpha: number) => { separate(0.6, Math.min(1, alpha * 4)); })
    .force("cx", forceX<TypeNode>((d) => centre[d.source][0]).strength(0.2))
    .force("cy", forceY<TypeNode>((d) => centre[d.source][1]).strength(0.2))
    .stop();
  // Fewer steps for a big map: the relaxation below finishes the job.
  const ticks = count0 > 80 ? 120 : 200;
  for (let i = 0; i < ticks; i += 1) {
    simulation.tick();
    if (i % 10 === 9) yield;
  }
  function* relax() {
    for (let i = 0; i < 300; i += 1) {
      if (separate(0.5) < 0.01) break;
      if (i % 20 === 19) yield;
    }
  }
  yield* relax();
  // If the stage is smaller than the map, labels grow relative to it: widen
  // the boxes to match and relax again (bounded, so a phone does not blow the
  // map up).
  const fitNodes = () => nodes.map((n) => ({ x: n.x, y: n.y, r: n.r, lw: n.lw }));
  let strip: Box | null = null;
  const placeStrip = () => {
    if (!stripNodes.length) return;
    let x0 = Infinity; let x1 = -Infinity; let y1 = -Infinity;
    for (const n of sim) {
      const hw = Math.max(n.r, (ls * n.lw) / 2);
      x0 = Math.min(x0, n.x - hw); x1 = Math.max(x1, n.x + hw); y1 = Math.max(y1, n.y + n.r + ls * 30);
    }
    if (!sim.length) { x0 = 0; x1 = 600; y1 = 0; }
    const rowW = Math.max(x1 - x0, 420 * ls);
    const top = y1 + 56 * ls;
    let x = x0;
    let y = top + 30 * ls;
    let rowH = 0;
    for (const n of stripNodes.slice().sort((a, b) => b.n - a.n)) {
      const w = Math.max(2 * n.r, ls * n.lw) + 22 * ls;
      if (x > x0 && x + w > x0 + rowW) { x = x0; y += rowH + 24 * ls; rowH = 0; }
      n.x = x + w / 2;
      n.y = y + n.r;
      rowH = Math.max(rowH, 2 * n.r + 30 * ls);
      x += w;
    }
    strip = { x0, x1: x0 + rowW, y0: top, y1: y + rowH };
  };
  placeStrip();
  for (let round = 0; round < 2; round += 1) {
    const k = fitTransform(fitNodes(), stage, 28).k;
    const want = Math.min(1.4, 1 / k);
    if (want <= ls * 1.03) break;
    ls = want;
    yield* relax();
    placeStrip();
  }
  const kFit = fitTransform(fitNodes(), stage, 28).k;

  // Each line picks the bend that crosses the fewest circles and labels (as
  // they will sit at the fitted scale); straighter wins a tie. Only the
  // obstacles inside a candidate's bounding box are checked.
  const obst = nodes.map((n) => ({
    n,
    x0: Math.min(n.x - n.r - 4, n.x - n.lw / 2 / kFit),
    x1: Math.max(n.x + n.r + 4, n.x + n.lw / 2 / kFit),
    y0: n.y - n.r - 4,
    y1: n.y + n.r + 30 / kFit,
    lb: { x0: n.x - n.lw / 2 / kFit, x1: n.x + n.lw / 2 / kFit, y0: n.y + n.r + 1 / kFit, y1: n.y + n.r + 30 / kFit },
  }));
  let done = 0;
  for (const l of links) {
    done += 1;
    if (done % 25 === 0) yield;
    const dist = Math.hypot(l.d.x - l.s.x, l.d.y - l.s.y);
    // every candidate curve lies within this box
    const reach = dist * 0.3;
    const ux0 = Math.min(l.s.x, l.d.x) - reach; const ux1 = Math.max(l.s.x, l.d.x) + reach;
    const uy0 = Math.min(l.s.y, l.d.y) - reach; const uy1 = Math.max(l.s.y, l.d.y) + reach;
    const around = obst.filter((o) => o.x1 > ux0 && o.x0 < ux1 && o.y1 > uy0 && o.y0 < uy1);
    let best = 0.08;
    let bc = Infinity;
    for (const f of BEND_CANDIDATES) {
      const g = linkPath(l.s, l.d, dist * f);
      const cx0 = Math.min(g.x1, g.mx, g.x2); const cx1 = Math.max(g.x1, g.mx, g.x2);
      const cy0 = Math.min(g.y1, g.my, g.y2); const cy1 = Math.max(g.y1, g.my, g.y2);
      const near = around.filter((o) => o.x1 > cx0 && o.x0 < cx1 && o.y1 > cy0 && o.y0 < cy1);
      let c = Math.abs(f) * 2;
      if (near.length) {
        for (let u = 0.04; u < 0.97; u += 0.05) {
          const [x, y] = qpt(g, u);
          for (const o of near) {
            const onEnd = o.n === l.s || o.n === l.d;
            if (!onEnd && Math.hypot(x - o.n.x, y - o.n.y) < o.n.r + 4) c += 3;
            if (x > o.lb.x0 && x < o.lb.x1 && y > o.lb.y0 && y < o.lb.y1) c += 1;
          }
        }
      }
      if (c < bc - 1e-9) { bc = c; best = f; }
    }
    l.bend = best;
    l.geo = linkPath(l.s, l.d, dist * best);
  }
  const reshape = (l: TypeLink) => {
    l.geo = linkPath(l.s, l.d, Math.hypot(l.d.x - l.s.x, l.d.y - l.s.y) * l.bend);
  };
  return {
    nodes, links, loops, isolated: stripNodes.map((n) => n.id), strip, clusters, byId, drawnAs, width, reshape,
  };
}

/** A loop on a circle's upper right, for relationships to itself. */
export function loopPath(n: { x: number; y: number; r: number }): { d: string; x: number; y: number } {
  const a0 = -Math.PI / 2 + 0.25;
  const a1 = -0.25;
  const lr = Math.max(7, n.r * 0.55);
  const p0 = [n.x + Math.cos(a0) * (n.r + 1), n.y + Math.sin(a0) * (n.r + 1)];
  const p1 = [n.x + Math.cos(a1) * (n.r + 1), n.y + Math.sin(a1) * (n.r + 1)];
  const out = n.r + lr * 2.2;
  const c0 = [n.x + Math.cos(a0 - 0.35) * out, n.y + Math.sin(a0 - 0.35) * out];
  const c1 = [n.x + Math.cos(a1 + 0.35) * out, n.y + Math.sin(a1 + 0.35) * out];
  const mid = -Math.PI / 4;
  return {
    d: `M${p0[0]},${p0[1]} C${c0[0]},${c0[1]} ${c1[0]},${c1[1]} ${p1[0]},${p1[1]}`,
    x: n.x + Math.cos(mid) * (n.r + lr * 1.6),
    y: n.y + Math.sin(mid) * (n.r + lr * 1.6),
  };
}

/** The line's tooltip: every pattern it carries, with who made it. */
export function linkTitle(pats: Pattern[], name: (p: string) => string): string {
  return pats.map((p) => `${p.s} ${p.e} → ${p.d}: ${p.n.toLocaleString("en-US")} (by ${p.prod.map((x) => name(x[0])).join(", ")})`).join("\n");
}
