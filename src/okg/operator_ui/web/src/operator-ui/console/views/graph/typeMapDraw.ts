// Draws the type map into an SVG with d3: lines, loops, circles, pan and
// zoom, drag, hover, the keyboard and the screen-space labels. The layout and
// the label placement are pure (model/graph/*); this is the part that touches
// the DOM, ported from the approved prototype (v2.html, stageShell and
// drawTypes).
//
// Rest: every line quiet. Focus (the selected type, or while nothing is
// selected the hovered or keyboard-focused one): the focused node ringed, its
// neighbours and lines at full strength with names, everything else faded
// (less so on hover).
//
// What changes WITHOUT a redraw: the selection, the producers switched off in
// the legend (their nodes fade in place and their lines go; nothing moves),
// and the producer names. The layout is re-run only for a new read or a new
// set of opened producers.
//
// Keyboard: the drawing is ONE tab stop (roving tabindex): the arrow keys
// move to the nearest node in that direction, Enter or Space selects a type
// or opens a producer's circle. The view handles +, −, 0, Escape and typing.
import { drag } from "d3-drag";
import { select, type Selection } from "d3-selection";
import "d3-transition";
import { type ZoomBehavior, type ZoomTransform, zoom, zoomIdentity, zoomTransform } from "d3-zoom";
import { fmt, type GraphModel } from "@operator-ui/console/model/graph/derive";
import { fitTransform } from "@operator-ui/console/model/graph/geometry";
import { type LabelText, neighbours, nodeText, placeMapLabels, touches } from "@operator-ui/console/model/graph/typeMapLabels";
import {
  type TypeLink, type TypeLoop, type TypeMapLayout, type TypeNode, layoutSteps, linkTitle, loopPath,
} from "@operator-ui/console/model/graph/typeMapLayout";
import { measureText } from "@operator-ui/console/views/graph/measure";

export interface TypeMapHandle {
  setSelection: (type: string | null) => void;
  setHidden: (hidden: ReadonlySet<string>) => void;
  setText: (text: LabelText) => void;
  zoomBy: (factor: number) => void;
  fit: (animate: boolean) => void;
  /** Move the keyboard focus to a node (the one a type is drawn as). */
  focusNode: (type: string) => void;
  transform: () => { x: number; y: number; k: number };
  destroy: () => void;
}

export interface TypeMapInput {
  svg: SVGSVGElement;
  stage: HTMLElement;
  legend: HTMLElement | null;
  zoomButtons: HTMLElement | null;
  model: GraphModel;
  /** Producers opened into their types; null draws every type. */
  expand: ReadonlySet<string> | null;
  hidden: ReadonlySet<string>;
  selected: string | null;
  text: LabelText;
  /** The view to keep across a redraw; null fits the map. */
  initial?: { x: number; y: number; k: number } | null;
  onSelect: (type: string | null) => void;
  /** `keyboard`: opened with Enter, so the focus should follow into its types. */
  onExpand: (source: string, keyboard: boolean) => void;
  onLayout?: (layout: TypeMapLayout) => void;
}

type SVG = Selection<SVGSVGElement, unknown, null, undefined>;

export function markers(svg: SVG, prefix: string, small: boolean): void {
  const defs = svg.select<SVGDefsElement>("defs").empty() ? svg.append("defs") : svg.select<SVGDefsElement>("defs");
  const list: [string, string, string][] = [
    [`${prefix}a`, "--okg-graph-edge", "auto"], [`${prefix}ah`, "--okg-graph-edge-hi", "auto"],
    [`${prefix}as`, "--okg-graph-edge", "auto-start-reverse"], [`${prefix}ahs`, "--okg-graph-edge-hi", "auto-start-reverse"],
  ];
  for (const [id, v, o] of list) {
    defs.append("marker").attr("id", id)
      .attr("viewBox", small ? "0 -4 8 8" : "0 -5 10 10").attr("refX", small ? 7.5 : 10).attr("refY", 0)
      .attr("markerUnits", "userSpaceOnUse").attr("markerWidth", small ? 7 : 9).attr("markerHeight", small ? 7 : 9)
      .attr("orient", o)
      .append("path").attr("d", small ? "M0,-3.4L8,0L0,3.4Z" : "M0,-4.5L10,0L0,4.5Z").style("fill", `var(${v})`);
  }
}

/** The stage's size and the room the legend and the zoom buttons take. */
export function stageFrame(stage: HTMLElement, legend: HTMLElement | null) {
  const W = stage.clientWidth;
  const H = stage.clientHeight;
  const left = W > 700 && legend ? legend.offsetLeft + legend.offsetWidth + 12 : 0;
  return { W, H, left, right: W > 700 ? 48 : 0 };
}

function screenBox(el: HTMLElement | null, stage: HTMLElement) {
  if (!el || el.hidden) return null;
  const a = el.getBoundingClientRect();
  const s = stage.getBoundingClientRect();
  if (!a.width && !a.height) return null;
  return { x0: a.left - s.left, x1: a.right - s.left, y0: a.top - s.top, y1: a.bottom - s.top };
}

/**
 * The node an arrow key moves to: the nearest shown node in that direction,
 * nearness counting the sideways offset double. Exported for its test.
 */
export function nodeInDirection<T extends { id: string; x: number; y: number }>(from: T, nodes: T[], key: string): T | null {
  const dir: Record<string, [number, number]> = { ArrowRight: [1, 0], ArrowLeft: [-1, 0], ArrowDown: [0, 1], ArrowUp: [0, -1] };
  const d = dir[key];
  if (!d) return null;
  let best: T | null = null;
  let bestScore = Infinity;
  for (const n of nodes) {
    if (n.id === from.id) continue;
    const dx = n.x - from.x;
    const dy = n.y - from.y;
    const along = dx * d[0] + dy * d[1];
    if (along <= 0) continue;
    const side = Math.abs(dx * d[1] - dy * d[0]);
    const score = along + 2 * side;
    if (score < bestScore) { bestScore = score; best = n; }
  }
  return best;
}

/** The pan limit: some circle always stays at least 40 px inside the
 *  stage, so the map cannot be dragged away and lost. When none would, the
 *  view moves back just far enough to bring the nearest one in. */
export function constrainToStage(
  t: { x: number; y: number; k: number }, points: { x: number; y: number }[], W: number, H: number,
): { dx: number; dy: number } {
  const m = Math.min(40, W / 2, H / 2);
  let best = { dx: 0, dy: 0, d: Infinity };
  for (const p of points) {
    const sx = t.x + t.k * p.x;
    const sy = t.y + t.k * p.y;
    const dx = sx < m ? m - sx : sx > W - m ? W - m - sx : 0;
    const dy = sy < m ? m - sy : sy > H - m ? H - m - sy : 0;
    if (!dx && !dy) return { dx: 0, dy: 0 };
    const d = Math.hypot(dx, dy);
    if (d < best.d) best = { dx, dy, d };
  }
  return points.length ? { dx: best.dx, dy: best.dy } : { dx: 0, dy: 0 };
}

let serial = 0;

/** The longest the layout holds the main thread before it lets the page
 *  run (measured in the browser: see the change's evidence). */
export const LAYOUT_SLICE_MS = 30;

/**
 * Lay the map out a slice at a time, then draw it. Until it is drawn the
 * drawing it replaces (`replacing`) stays on the stage and answers; state set
 * meanwhile is kept and applied when it is drawn.
 */
export function createTypeMap(input: TypeMapInput & { replacing?: TypeMapHandle | null }): TypeMapHandle {
  const pending = { selected: input.selected, hidden: input.hidden, text: input.text, focus: null as string | null };
  let inner: TypeMapHandle | null = null;
  let cancelled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const steps = layoutSteps(input.model, measureText, stageFrame(input.stage, input.legend), input.expand);
  const pump = () => {
    if (cancelled) return;
    const t0 = performance.now();
    let r = steps.next();
    while (!r.done && performance.now() - t0 < LAYOUT_SLICE_MS) r = steps.next();
    if (!r.done) { timer = setTimeout(pump, 0); return; }
    const initial = input.replacing ? input.replacing.transform() : input.initial ?? null;
    input.replacing?.destroy();
    inner = drawLayout({ ...input, selected: pending.selected, hidden: pending.hidden, text: pending.text, initial }, r.value);
    if (pending.focus) inner.focusNode(pending.focus);
  };
  pump();
  return {
    setSelection: (type) => { pending.selected = type; inner?.setSelection(type); },
    setHidden: (h) => { pending.hidden = h; inner?.setHidden(h); },
    setText: (t) => { pending.text = t; inner?.setText(t); },
    zoomBy: (f) => { (inner ?? input.replacing)?.zoomBy(f); },
    fit: (a) => { (inner ?? input.replacing)?.fit(a); },
    focusNode: (type) => { if (inner) inner.focusNode(type); else pending.focus = type; },
    transform: () => (inner ?? input.replacing)?.transform() ?? input.initial ?? { x: 0, y: 0, k: 1 },
    destroy: () => {
      cancelled = true;
      clearTimeout(timer);
      if (inner) inner.destroy(); else input.replacing?.destroy();
    },
  };
}

function drawLayout(input: TypeMapInput, layout: TypeMapLayout): TypeMapHandle {
  const { stage, legend, zoomButtons, model, onSelect, onExpand } = input;
  serial += 1;
  const mk = `tm${serial}`;
  const svg = select(input.svg);
  svg.selectAll("*").remove();
  svg.classed("tmap", true);
  markers(svg, mk, true);
  const gRoot = svg.append("g");
  const gLab = svg.append("g").attr("class", "labels").style("pointer-events", "none");
  input.onLayout?.(layout);
  const { nodes, links, byId } = layout;
  const loops = Object.values(layout.loops);
  let text = input.text;
  let hidden = input.hidden;
  const shown = (n: TypeNode) => !hidden.has(n.source);
  const nodeOf = (type: string | null) => (type ? byId[layout.drawnAs(type)] ?? null : null);
  let sel = nodeOf(input.selected)?.id ?? null;
  let hoverN: string | null = null;
  let hoverL: TypeLink | null = null;
  let current = sel ?? nodes.slice().sort((a, b) => b.n - a.n)[0]?.id ?? null;

  const L = gRoot.append("g").selectAll<SVGGElement, TypeLink>("g").data(links).join("g").attr("class", "l");
  L.append("path").attr("class", "hit").attr("d", (l) => l.geo.d);
  L.append("path").attr("class", "ln").attr("d", (l) => l.geo.d).attr("stroke-width", (l) => layout.width(l.n));
  const linkTitles = L.append("title");
  const O = gRoot.append("g").selectAll<SVGGElement, TypeLoop>("g").data(loops).join("g").attr("class", "l lp");
  O.append("path").attr("class", "ln").attr("d", (o) => loopPath(o.node).d).attr("stroke-width", (o) => layout.width(o.n))
    .attr("marker-end", `url(#${mk}a)`);
  const loopTitles = O.append("title");
  const N = gRoot.append("g").selectAll<SVGGElement, TypeNode>("g").data(nodes).join("g")
    .attr("class", (d) => `n${d.kind === "source" ? " src" : ""}${d.strip ? " strip" : ""}`)
    .attr("transform", (d) => `translate(${d.x},${d.y})`)
    .attr("role", "button");
  N.append("circle").attr("class", "hit").attr("r", (d) => d.r);
  N.append("circle").attr("class", "ring").attr("r", (d) => d.r + 4.5);
  N.append("circle").attr("class", "body").attr("r", (d) => d.r).style("fill", (d) => d.color);
  N.filter((d) => d.kind === "source").append("circle").attr("class", "inner").attr("r", (d) => Math.max(2, d.r - 4));
  const nodeTitles = N.append("title");

  const retext = () => {
    const name = (p: string) => text.producer(p);
    linkTitles.text((l) => linkTitle(l.pats, name));
    loopTitles.text((o) => linkTitle(o.pats, name));
    N.attr("aria-label", (d) => (d.kind === "type"
      ? `${d.id}, ${fmt(d.n)} records${d.strip ? ", no relationships" : ""}`
      : `${text.producer(d.source)}: ${d.types.length} types, ${fmt(d.n)} records; Enter shows its types`));
    nodeTitles.text((d) => (d.kind === "type"
      ? `${d.id}: ${fmt(d.n)} records`
      : `${text.producer(d.source)}: ${d.types.length} types, ${fmt(d.n)} records. Click to show its types.`));
  };

  const focus = () => {
    const f = sel ?? hoverN;
    return f && byId[f] && shown(byId[f]) ? f : null;
  };

  function relabel() {
    const t = zoomTransform(input.svg);
    const W = stage.clientWidth;
    const H = stage.clientHeight;
    const furniture = [screenBox(legend, stage), screenBox(zoomButtons, stage)].filter((b) => b !== null);
    const out = placeMapLabels(layout, { transform: { x: t.x, y: t.y, k: t.k }, W, H, furniture },
      focus(), { node: hoverN, link: hoverL }, text, measureText, shown);
    gLab.selectAll("*").remove();
    for (const o of out) {
      const isSel = o.kind === "node" && o.node?.id === sel;
      const g = gLab.append("g").attr("class", `lab ${o.kind}${o.faint ? " faint" : ""}${isSel ? " focus" : ""}`);
      const cx = (o.b.x0 + o.b.x1) / 2;
      if (o.kind === "cap") {
        const strip = o.cluster === undefined;
        const tx = g.append("text").attr("class", o.id ? "cap capid" : "cap").attr("x", strip ? o.b.x0 : cx).attr("y", o.b.y1 - 4)
          .attr("text-anchor", strip ? "start" : "middle");
        if (!strip) tx.append("tspan").style("fill", model.srcColor[o.cluster ?? ""] ?? null).text("● ");
        tx.append("tspan").text(o.lines?.[0] ?? "");
        continue;
      }
      g.append("rect").attr("class", "bg").attr("x", o.b.x0).attr("y", o.b.y0)
        .attr("width", o.b.x1 - o.b.x0).attr("height", o.b.y1 - o.b.y0).attr("rx", 4);
      if (o.kind === "node" && o.node) {
        const nt = nodeText(o.node, text);
        const [ax, anc] = o.b.side === "r" ? [o.b.x0 + 5, "start"] : o.b.side === "l" ? [o.b.x1 - 5, "end"] : [cx, "middle"];
        g.append("text").attr("class", nt.mono ? "nm" : "nm prose").attr("x", ax).attr("y", o.b.y0 + 12).attr("text-anchor", anc).text(nt.title);
        g.append("text").attr("class", "ct").attr("x", ax).attr("y", o.b.y0 + 25).attr("text-anchor", anc).text(nt.sub);
      } else {
        (o.lines ?? []).forEach((line, i) => {
          g.append("text").attr("class", "el").attr("x", cx).attr("y", o.b.y0 + 12 + 16 * i).attr("text-anchor", "middle").text(line);
        });
      }
    }
  }

  function paint() {
    const f = focus();
    const nb = neighbours(layout, f, shown);
    gRoot.classed("preview", !sel && Boolean(f));
    N.classed("off", (d) => !shown(d))
      .attr("aria-hidden", (d) => (shown(d) ? null : "true"))
      .attr("tabindex", (d) => (shown(d) && d.id === current ? 0 : -1))
      .classed("sel", (d) => d.id === sel).classed("hov", (d) => d.id === hoverN && d.id !== sel)
      .classed("dim", (d) => Boolean(f) && !nb.has(d.id));
    const on = (l: TypeLink) => shown(l.s) && shown(l.d);
    L.classed("off", (l) => !on(l))
      .classed("hi", (l) => Boolean(f) && touches(l, f as string)).classed("dim", (l) => Boolean(f) && !touches(l, f as string));
    O.classed("off", (o) => !shown(o.node))
      .classed("hi", (o) => o.node.id === f).classed("dim", (o) => Boolean(f) && o.node.id !== f);
    O.select("path.ln").attr("marker-end", (o) => (o.node.id === f ? `url(#${mk}ah)` : `url(#${mk}a)`));
    L.select("path.ln")
      .attr("marker-end", (l) => (l.fwd ? (f && touches(l, f) ? `url(#${mk}ah)` : `url(#${mk}a)`) : null))
      .attr("marker-start", (l) => (l.bwd ? (f && touches(l, f) ? `url(#${mk}ahs)` : `url(#${mk}as)`) : null));
    relabel();
  }

  const nodeEl = (id: string) => N.filter((d) => d.id === id).node();
  const moveTo = (n: TypeNode) => {
    current = n.id;
    N.attr("tabindex", (d) => (shown(d) && d.id === current ? 0 : -1));
    nodeEl(n.id)?.focus();
  };
  const activate = (d: TypeNode, keyboard: boolean) => {
    if (d.kind === "source") onExpand(d.source, keyboard);
    else onSelect(sel === d.id ? null : d.id);
  };
  N.on("click", (ev: MouseEvent, d) => { ev.stopPropagation(); current = d.id; activate(d, false); })
    .on("keydown", (ev: KeyboardEvent, d) => {
      if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); activate(d, true); return; }
      if (ev.key.startsWith("Arrow")) {
        ev.preventDefault();
        const next = nodeInDirection(d, nodes.filter(shown), ev.key);
        if (next) moveTo(next);
      }
    })
    .on("focus", (_ev, d) => { current = d.id; hoverN = d.id; paint(); })
    .on("mouseenter", (_ev, d) => { hoverN = d.id; paint(); })
    .on("mouseleave blur", () => { hoverN = null; paint(); });
  N.call(drag<SVGGElement, TypeNode>().on("drag", (ev: { x: number; y: number }, d) => {
    d.x = ev.x; d.y = ev.y;
    N.attr("transform", (x) => `translate(${x.x},${x.y})`);
    links.forEach(layout.reshape);
    L.selectAll<SVGPathElement, TypeLink>("path").attr("d", (l) => l.geo.d);
    O.select("path.ln").attr("d", (o) => loopPath(o.node).d);
    relabel();
  }));
  L.on("mouseenter", (_ev, l) => { hoverL = l; relabel(); }).on("mouseleave", () => { hoverL = null; relabel(); });

  const hitR = (k: number) => N.select<SVGCircleElement>("circle.hit").attr("r", (d) => Math.max(d.r, 12 / k));
  const zoomBehaviour: ZoomBehavior<SVGSVGElement, unknown> = zoom<SVGSVGElement, unknown>().scaleExtent([0.2, 4])
    .constrain((t: ZoomTransform) => {
      const { dx, dy } = constrainToStage(t, nodes.filter(shown), stage.clientWidth, stage.clientHeight);
      return dx || dy ? t.translate(dx / t.k, dy / t.k) : t;
    })
    .on("zoom", (ev: { transform: ZoomTransform }) => {
      gRoot.attr("transform", ev.transform.toString());
      hitR(ev.transform.k);
      relabel();
    });
  svg.call(zoomBehaviour).on("dblclick.zoom", null);
  svg.on("click", (ev: MouseEvent) => { if (ev.target === input.svg) onSelect(null); });

  const fit = (animate: boolean) => {
    const vis = nodes.filter(shown);
    if (!vis.length) return;
    const t = fitTransform(vis.map((n) => ({ x: n.x, y: n.y, r: n.r, lw: n.lw })), stageFrame(stage, legend), 28);
    const target = zoomIdentity.translate(t.x, t.y).scale(t.k);
    if (animate) svg.transition().duration(300).call(zoomBehaviour.transform, target);
    else svg.call(zoomBehaviour.transform, target);
  };
  retext();
  if (input.initial) {
    svg.call(zoomBehaviour.transform, zoomIdentity.translate(input.initial.x, input.initial.y).scale(input.initial.k));
    fit(true);
  } else {
    fit(false);
  }
  paint();
  return {
    setSelection: (type) => {
      sel = nodeOf(type)?.id ?? null;
      if (sel) current = sel;
      paint();
    },
    setHidden: (next) => {
      hidden = next;
      if (current && !shown(byId[current])) current = nodes.filter(shown).sort((a, b) => b.n - a.n)[0]?.id ?? null;
      paint();
    },
    setText: (next) => { text = next; retext(); relabel(); },
    zoomBy: (factor) => { svg.transition().duration(200).call(zoomBehaviour.scaleBy, factor); },
    fit,
    focusNode: (type) => {
      const n = nodeOf(type);
      if (n && shown(n)) moveTo(n);
    },
    transform: () => { const t = zoomTransform(input.svg); return { x: t.x, y: t.y, k: t.k }; },
    destroy: () => { svg.interrupt(); svg.on(".zoom", null); svg.selectAll("*").remove(); },
  };
}
