// Which labels the type map shows, and where, for one zoom and one focus.
//
// Ported from the approved prototype (v2.html, relabel). Every label is placed
// in screen space at a fixed size, whatever the zoom. Order: producer
// captions and the strip's caption, the focused node and its neighbours, the
// names of the focused node's relationships (its loop included), a hovered
// node or line, then the other nodes by record count. A label that would
// cover a circle, another label, the legend or the zoom buttons tries its
// other sides and is otherwise left out (the name stays on the tooltip and in
// the side panel). The focused node's biggest relationship is always named.
// Nodes switched off in the legend have no label and block nothing.
import { short } from "@operator-ui/console/model/graph/derive";
import { type Box, type LabelItem, type Placed, overlap, placeLabels, qpt } from "@operator-ui/console/model/graph/geometry";
import { type MeasureText, type TypeLink, type TypeMapLayout, type TypeNode, loopPath } from "@operator-ui/console/model/graph/typeMapLayout";

export interface MapLabel extends LabelItem {
  kind: "cap" | "node" | "edge";
  cluster?: string;
  /** One line per row of the label (a caption or a line's name). */
  lines?: string[];
  node?: TypeNode;
  faint?: boolean;
  /** The focused node's loop: always named, never dropped for room. */
  loop?: boolean;
  /** A caption that is an ownership id, drawn in monospace as stored. */
  id?: boolean;
}

export interface LabelFrame {
  transform: { x: number; y: number; k: number };
  W: number;
  H: number;
  /** Screen boxes no label may cover: the legend and the zoom buttons. */
  furniture: Box[];
}

/** What the labels say: producer names come from the page, not the layout. */
export interface LabelText {
  producer: (source: string) => string;
  /** The producer's name without the deployment's prefix, for a narrow stage. */
  shortProducer?: (source: string) => string;
  /** True while producers are known only by ownership id: captions print the
   *  id as stored, in monospace, not upper-cased as a name. */
  ids?: boolean;
}

/** A stage this narrow (a phone) cannot hold every producer's caption beside
 *  the circles: the same width at which the legend folds (typeMapDraw.ts). */
export const NARROW_STAGE_PX = 700;

/** A producer's name without `<deployment>.`, as a folded producer's circle is titled. */
export const shortProducerName = (name: string, deployment: string): string =>
  (name.startsWith(`${deployment}.`) ? name.slice(deployment.length + 1) : name);

export const touches = (l: TypeLink, f: string): boolean => l.s.id === f || l.d.id === f;

/** The focused node and every node one drawn line away, among those shown. */
export function neighbours(layout: TypeMapLayout, f: string | null, shown: (n: TypeNode) => boolean = () => true): Set<string> {
  if (!f) return new Set();
  return new Set(layout.links.filter((l) => touches(l, f) && shown(l.s) && shown(l.d)).flatMap((l) => [l.s.id, l.d.id]).concat([f]));
}

/** A node's name and the line under it. */
export function nodeText(n: TypeNode, text: LabelText): { title: string; sub: string; mono: boolean } {
  if (n.kind === "type") return { title: n.id, sub: short(n.n), mono: true };
  return { title: text.producer(n.source), sub: `${n.types.length} types · ${short(n.n)} records`, mono: false };
}

const rels = (ps: { e: string }[]): string => {
  const names = [...new Set(ps.map((p) => p.e))];
  return names.length > 3 ? `${names.slice(0, 2).join(", ")} +${names.length - 2} more` : names.join(", ");
};

/**
 * A line's name. Seen from the focused end: one row per direction, "→" for
 * the relationships from the focused node and "←" for those to it, when both
 * exist (with one direction, the arrowhead says it). Hovered with nothing in
 * focus: each row names both ends.
 */
export function linkLines(l: TypeLink, focus: string | null, text: LabelText): string[] {
  const fw = l.pats.filter((p) => (p.s === l.s.id || l.s.types.includes(p.s)) && (p.d === l.d.id || l.d.types.includes(p.d)));
  const bw = l.pats.filter((p) => !fw.includes(p));
  const row = (ps: typeof fw, n: number) => `${rels(ps)} ${short(n)}`;
  if (focus === l.s.id || focus === l.d.id) {
    const [mine, theirs, nm, nt] = focus === l.s.id ? [fw, bw, l.nF, l.nB] : [bw, fw, l.nB, l.nF];
    if (mine.length && theirs.length) return [`→ ${row(mine, nm)}`, `← ${row(theirs, nt)}`];
    return [row(mine.length ? mine : theirs, mine.length ? nm : nt)];
  }
  const name = (n: TypeNode) => nodeText(n, text).title;
  const out: string[] = [];
  if (fw.length) out.push(`${name(l.s)} ${rels(fw)} ${name(l.d)} · ${short(l.nF)}`);
  if (bw.length) out.push(`${name(l.d)} ${rels(bw)} ${name(l.s)} · ${short(l.nB)}`);
  return out;
}

export function placeMapLabels(
  layout: TypeMapLayout,
  frame: LabelFrame,
  focus: string | null,
  hover: { node: string | null; link: TypeLink | null },
  text: LabelText,
  measure: MeasureText,
  shown: (n: TypeNode) => boolean = () => true,
): Placed<MapLabel>[] {
  const { transform: tr, W, H } = frame;
  const { k } = tr;
  const ax = (x: number) => tr.x + x * k;
  const ay = (y: number) => tr.y + y * k;
  const nodes = layout.nodes.filter(shown);
  const links = layout.links.filter((l) => shown(l.s) && shown(l.d));
  const { byId, clusters } = layout;
  const f = focus && byId[focus] && shown(byId[focus]) ? focus : null;
  const nb = neighbours(layout, f, shown);
  const P = (n: TypeNode): [number, number] => [ax(n.x), ay(n.y)];
  // faded circles only block the faded labels placed after the focus
  const circ = (n: TypeNode): Box => {
    const [x, y] = P(n);
    const rr = n.r * k + 3;
    return { x0: x - rr, x1: x + rr, y0: y - rr, y1: y + rr };
  };
  const placed: Box[] = nodes.filter((n) => !f || nb.has(n.id)).map(circ);
  for (const b of frame.furniture) placed.push({ x0: b.x0 - 6, x1: b.x1 + 6, y0: b.y0 - 6, y1: b.y1 + 6 });
  // sampled points of the drawn lines; lines in focus weigh more
  const pts: [number, number, number][] = [];
  for (const l of links) {
    const w = f ? (touches(l, f) ? 3 : 0.2) : 1;
    for (let u = 0.03; u < 0.98; u += 0.04) {
      const [x, y] = qpt(l.geo, u);
      pts.push([ax(x), ay(y), w]);
    }
  }
  const items: MapLabel[] = [];
  // On a narrow stage the captions give way (as the strip's does) instead of
  // being forced over circles and each other -- at 390 px seven forced
  // captions covered the map (audit 2026-09-27) -- and drop the deployment's
  // prefix. The colour, the tooltips and the legend still name each producer.
  const narrow = W <= NARROW_STAGE_PX;
  const strip = nodes.filter((n) => n.strip);
  // A producer's caption set beside the strip reads as a strip circle's name
  // (at 390 px GIT-HISTORY sat by view_definition's circle): keep captions
  // out of the strip's corner whenever another place is offered.
  const stripZone: Box | null = strip.length ? {
    x0: Math.min(...strip.map((n) => ax(n.x) - n.r * k)) - 12,
    x1: Math.max(...strip.map((n) => ax(n.x) + n.r * k)) + 90,
    y0: Math.min(...strip.map((n) => ay(n.y) - n.r * k)) - 40,
    y1: Math.max(...strip.map((n) => ay(n.y) + n.r * k)) + 34,
  } : null;
  const offStrip = (cands: Box[]): Box[] => {
    const kept = stripZone ? cands.filter((b) => !overlap(b, stripZone)) : cands;
    return kept.length ? kept : cands;
  };
  for (const c of clusters) {
    const m = nodes.filter((n) => n.source === c && n.kind === "type" && !n.strip);
    if (!m.length) continue;
    const name = narrow && text.shortProducer ? text.shortProducer(c) : text.producer(c);
    const txt = text.ids ? name : name.toUpperCase();
    const w = measure(txt, 11, Boolean(text.ids)) * 1.1 + 22;
    const xs = m.map((n) => P(n)[0]);
    const top = Math.min(...m.map((n) => P(n)[1] - n.r * k));
    const bot = Math.max(...m.map((n) => P(n)[1] + n.r * k)) + 32;
    const cx = (Math.min(...xs) + Math.max(...xs)) / 2;
    const cands: Box[] = [];
    for (const dx of [0, -40, 40, -80, 80]) cands.push({ x0: cx + dx - w / 2, x1: cx + dx + w / 2, y0: top - 26, y1: top - 10 });
    for (const dx of [0, -40, 40]) cands.push({ x0: cx + dx - w / 2, x1: cx + dx + w / 2, y0: bot + 4, y1: bot + 20 });
    items.push({ kind: "cap", cluster: c, lines: [txt], cands: offStrip(cands), force: !narrow, id: Boolean(text.ids) });
  }
  if (strip.length) {
    // In screen space, above the strip's circles, whatever the zoom.
    const txt = "NO RELATIONSHIPS";
    const w = measure(txt, 11, false) * 1.1 + 12;
    const x = Math.min(...strip.map((n) => ax(n.x) - n.r * k));
    const y = Math.min(...strip.map((n) => ay(n.y) - n.r * k)) - 24;
    // Not forced: on a small stage it gives way rather than cover a
    // producer's caption; the status line names the strip in words.
    const cands = [0, 40, 80, -40].map((dx) => ({ x0: x + dx, x1: x + dx + w, y0: y, y1: y + 16 }));
    items.push({ kind: "cap", lines: [txt], cands: cands.concat(cands.map((c) => ({ ...c, y0: c.y0 - 14, y1: c.y1 - 14 }))) });
  }
  const nodeItem = (n: TypeNode, force = false): MapLabel => {
    const [x, y] = P(n);
    const rr = n.r * k;
    const t = nodeText(n, text);
    const w = Math.max(measure(t.title, 12, t.mono), measure(t.sub, 11, false)) + 10;
    return {
      kind: "node",
      node: n,
      faint: Boolean(f) && !nb.has(n.id),
      force: force || n.id === f,
      cands: [
        { x0: x - w / 2, x1: x + w / 2, y0: y + rr + 3, y1: y + rr + 31, side: "b" },
        { x0: x + rr + 6, x1: x + rr + 6 + w, y0: y - 14, y1: y + 14, side: "r" },
        { x0: x - rr - 6 - w, x1: x - rr - 6, y0: y - 14, y1: y + 14, side: "l" },
        { x0: x - w / 2, x1: x + w / 2, y0: y - rr - 31, y1: y - rr - 3, side: "t" },
        { x0: x + rr * 0.7 + 2, x1: x + rr * 0.7 + 2 + w, y0: y + rr * 0.7, y1: y + rr * 0.7 + 28, side: "r" },
        { x0: x - rr * 0.7 - 2 - w, x1: x - rr * 0.7 - 2, y0: y + rr * 0.7, y1: y + rr * 0.7 + 28, side: "l" },
        { x0: x + rr * 0.7 + 2, x1: x + rr * 0.7 + 2 + w, y0: y - rr * 0.7 - 28, y1: y - rr * 0.7, side: "r" },
        { x0: x - rr * 0.7 - 2 - w, x1: x - rr * 0.7 - 2, y0: y - rr * 0.7 - 28, y1: y - rr * 0.7, side: "l" },
      ],
    };
  };
  const byN = (a: TypeNode, b: TypeNode) => b.n - a.n;
  const first = f ? [byId[f]].concat(nodes.filter((n) => nb.has(n.id) && n.id !== f).sort(byN)) : [];
  const hovered = hover.node && hover.node !== f && byId[hover.node] && shown(byId[hover.node]) ? byId[hover.node] : null;
  const rest = nodes.filter((n) => (!f || !nb.has(n.id)) && n !== hovered).sort(byN);
  if (f) items.push(nodeItem(byId[f]));
  // A hovered node is named even while another is selected.
  if (hovered && f) items.push(nodeItem(hovered, true));
  const edgeItem = (lines: string[], cands0: [number, number, number, number][], force: boolean): MapLabel => {
    const w = Math.max(...lines.map((s) => measure(s, 11, false))) * 1.04 + 12;
    const h = 16 * lines.length;
    const cands = cands0.map(([x, y]) => ({ x0: x - w / 2, x1: x + w / 2, y0: y - h / 2 - 1, y1: y + h / 2 - 1 }));
    return { kind: "edge", lines, cands, force };
  };
  const lineCands = (l: TypeLink): [number, number, number, number][] => {
    const out: [number, number, number, number][] = [];
    for (const off of [0, 12, -12, 20, -20, 30, -30]) {
      for (const u of [0.5, 0.4, 0.6, 0.32, 0.68, 0.24, 0.76, 0.16, 0.84]) {
        const [gx, gy] = qpt(l.geo, u);
        const [hx, hy] = qpt(l.geo, Math.min(1, u + 0.01));
        const dx = ax(hx) - ax(gx);
        const dy = ay(hy) - ay(gy);
        const dl = Math.hypot(dx, dy) || 1;
        out.push([ax(gx) - (dy / dl) * off, ay(gy) + (dx / dl) * off, 0, 0]);
      }
    }
    return out;
  };
  const showE = f ? links.filter((l) => touches(l, f)).sort((a, b) => b.n - a.n) : hover.link && shown(hover.link.s) && shown(hover.link.d) ? [hover.link] : [];
  showE.forEach((l, i) => items.push(edgeItem(linkLines(l, f, text), lineCands(l), i === 0)));
  const loop = f ? layout.loops[f] : null;
  if (loop) {
    const lp = loopPath(byId[f as string]);
    const [x, y] = [ax(lp.x), ay(lp.y)];
    const txt = `↻ ${rels(loop.pats)} ${short(loop.n)}`;
    const w = measure(txt, 11, false) * 1.04 + 12;
    items.push({
      kind: "edge", lines: [txt], force: true, loop: true,
      cands: [[8, -8], [8, 8], [-8 - w, -8], [0, -26], [8, -26], [-w / 2, -30]].map(([dx, dy]) => ({ x0: x + dx, x1: x + dx + w, y0: y + dy - 9, y1: y + dy + 7 })),
    });
  }
  const nbItems = first.slice(1).map((n) => nodeItem(n));
  // Neighbour names outrank relationship names: if one does not fit, drop
  // the smallest relationship names (never the biggest) until it does.
  let edgeItems = items.filter((it) => it.kind === "edge" && !it.loop);
  const base = items.filter((it) => it.kind !== "edge" || it.loop);
  const placed0 = placed.slice();
  const wantN = nbItems.length + base.filter((it) => it.kind === "node").length;
  const run = (es: MapLabel[]) => {
    placed.length = 0;
    placed.push(...placed0);
    return placeLabels(base.concat(es, nbItems), placed, pts, W, H);
  };
  const ok = (o: Placed<MapLabel>[]) => o.filter((x) => x.kind === "node").length >= wantN;
  let out = run(edgeItems);
  // first try leaving out one name, smallest first; failing that, several
  for (let i = edgeItems.length - 1; i > 0 && !ok(out); i -= 1) {
    const o = run(edgeItems.filter((_, j) => j !== i));
    if (ok(o)) out = o;
  }
  while (!ok(out) && edgeItems.length > 1) {
    edgeItems = edgeItems.slice(0, -1);
    out = run(edgeItems);
  }
  // `run` may have left `placed` from a rejected attempt; rebuild it from
  // what was actually kept before the rest are placed.
  placed.length = 0;
  placed.push(...placed0, ...out.map((o) => o.b));
  if (f) nodes.filter((n) => !nb.has(n.id)).forEach((n) => placed.push(circ(n)));
  out.push(...placeLabels(rest.map((n) => nodeItem(n)), placed, pts, W, H));
  return out;
}
