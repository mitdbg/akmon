// Draws one record's neighbourhood as the two-sided tree, with d3. The plan
// (which groups, how many rows) is pure, in model/graph/recordTree.ts; this
// places it in an SVG and wires the pointer and the keyboard. Ported from the
// approved prototype (v2.html, drawRecord).
//
// Keyboard: the tree is one tab stop (a roving tabindex), starting at the
// record itself. Up and Down walk one side's headings and rows; Left and
// Right move between the record and the two sides. Enter or Space on a
// heading opens or closes its group; Space on a record selects it and Enter
// centres the graph on it, as a double-click does.
import { select } from "d3-selection";
import "d3-transition";
import { zoom, zoomIdentity, type ZoomBehavior } from "d3-zoom";
import { cat, fmt, plural } from "@operator-ui/console/model/graph/derive";
import type { RecordNeighbourhoodRead } from "@operator-ui/console/model/graph/reads";
import {
  GAP, HEAD, type Leaf, ROW, SIDE_CAP, SIDE_TITLE, TOP, type TreeGroup, blockH, columns, countText, exploreLinkText, fitText,
  keyOf, kindList, nice, planBlocks, prose, relationSentence, rtSide, sideH, sideTotal, sideWidths, toTreeGroups, treeStatus,
} from "@operator-ui/console/model/graph/recordTree";
import { measureText } from "@operator-ui/console/views/graph/measure";

/** What is selected on the drawing: a group (by key), one of its records, or
 *  the folded smaller groups of one side. */
export type TreeSelection =
  | null
  | { key: string; id: string | null }
  | { kinds: "in" | "out" };

export interface TreeHandle {
  setSelection: (sel: TreeSelection) => void;
  zoomBy: (factor: number) => void;
  fit: (animate: boolean) => void;
  destroy: () => void;
}

type NodeKind = "center" | "group" | "head" | "kinds" | "leaf" | "more" | "explore";

interface TreeNode {
  id: string;
  kind: NodeKind;
  key?: string;
  g?: TreeGroup;
  x: number;
  y: number;
  r: number;
  color: string;
  sgn: number;
  dir?: "in" | "out";
  open?: boolean;
  label?: string;
  suffix?: string;
  full?: string;
  leaf?: Leaf;
  href?: string;
}

interface Link { kind: "trunk" | "twig"; key: string; a: { x: number; y: number }; b: { x: number; y: number }; w?: number; color?: string; dash?: boolean }

export interface RecordTreeInput {
  svg: SVGSVGElement;
  stage: HTMLElement;
  stageWidth: number;
  read: RecordNeighbourhoodRead;
  expanded: string | null;
  selected: TreeSelection;
  /** The node to give focus once drawn: the one focused before a redraw. */
  focusId: string | null;
  exploreHref: (g: TreeGroup) => string;
  onToggle: (key: string) => void;
  onSelectLeaf: (key: string, id: string | null) => void;
  onKinds: (dir: "in" | "out") => void;
  onBackground: () => void;
  onHome: () => void;
  onOpen: (id: string) => void;
  onNavigate: (href: string) => void;
  onStatus: (text: string) => void;
  onFocusNode: (id: string) => void;
}

export function createRecordTree(input: RecordTreeInput): TreeHandle {
  const { read, expanded, stage, stageWidth: stW } = input;
  const groups = toTreeGroups(read.groups);
  const kinds = kindList(read);
  const tcol = (t: string) => cat((kinds.indexOf(t) % 9) + 1);
  const sides = { in: rtSide(groups, "in", SIDE_CAP, expanded), out: rtSide(groups, "out", SIDE_CAP, expanded) };
  const hiddenKinds = sides.in.hidden.length + sides.out.hidden.length;
  const svg = select(input.svg);
  svg.selectAll("*").remove();
  svg.classed("tmap", false);
  const gRoot = svg.append("g");
  const avail = stage.clientHeight - TOP - 28;
  const { blocks, per } = planBlocks(sides, expanded, avail);
  const { X1, X2 } = columns(stW);
  const edgeL = 18;
  const edgeR = 54; // the zoom buttons sit on the right edge
  // Measure with the leaf style itself; canvas metrics drift from the SVG's
  // monospace face.
  const probe = gRoot.append("g").attr("class", "rt-leaf").style("visibility", "hidden").append("text");
  const mW = (s: string) => { probe.classed("prose", prose(s)).text(s); return (probe.node() as SVGTextElement).getComputedTextLength(); };
  const suffixOf = (k: number) => (k > 1 ? ` ×${fmt(k)}` : "");
  const leafText = ({ it, k }: Leaf) => mW(nice(it.label || it.id)) + (k > 1 ? measureText(suffixOf(k), 11.5, false) + 2 : 0);
  const moreText = (b: (typeof blocks)["in"][number]) => (b.rows > b.leaves.length
    ? measureText(b.open ? exploreLinkText(read, b.g) : `${countText({ n: b.rest, floor: b.g.floor })} more`, 12, false) : 0);
  const natW = (dir: "in" | "out") => Math.max(0, ...blocks[dir].flatMap((b) => [...b.leaves.map(leafText), moreText(b)]));
  const headW = (dir: "in" | "out") => Math.max(0, ...blocks[dir].map((b) => (b.g.other
    ? measureText(`${b.g.t} · ${countText(b.g)} records`, 12.5, false)
    : measureText(b.g.t, 12.5, true) + measureText(b.g.e, 11.5, true) + measureText(countText(b.g), 11.5, false) + 18)));
  const spare = stW - edgeL - edgeR - 2 * (X2 + 12);
  const widths = sideWidths(spare, { in: Math.max(natW("in"), headW("in")), out: Math.max(natW("out"), headW("out")) });
  const nodes: TreeNode[] = [];
  const links: Link[] = [];
  const centre: TreeNode = { id: "c", kind: "center", x: 0, y: 0, r: 16, color: tcol(read.record.type), sgn: 0 };
  nodes.push(centre);
  const maxN = Math.max(1, ...groups.map((g) => g.n));
  // The keyboard's walk: each side's focusable rows, top to bottom.
  const order: Record<"in" | "out", string[]> = { in: [], out: [] };
  let drawnOpen = 0;
  for (const dir of ["in", "out"] as const) {
    const sgn = dir === "in" ? -1 : 1;
    const bl = blocks[dir];
    let y = -sideH(bl) / 2;
    for (const b of bl) {
      const g = b.g;
      const key = g.other ? `kinds|${dir}` : keyOf(g);
      const h = blockH(b.rows);
      const hy = y + HEAD / 2 - 1; // heading baseline row
      const gy = b.rows ? y + HEAD + (b.rows * ROW) / 2 - ROW / 2 : hy;
      const tx = sgn * (X2 + 10);
      if (g.other) {
        const kn: TreeNode = { id: `k:${dir}`, kind: "kinds", key, g, x: sgn * X1, y: hy, r: 5, color: "var(--okg-color-muted)", sgn, dir };
        nodes.push(kn);
        links.push({ kind: "trunk", key, a: centre, b: kn, w: 1, color: "var(--okg-graph-edge)", dash: true });
        nodes.push({ id: `kh:${dir}`, kind: "head", key, g, x: tx, y: hy, r: 0, color: kn.color, sgn, dir });
        order[dir].push(`kh:${dir}`);
        y += h + GAP;
        continue;
      }
      const gn: TreeNode = { id: `g:${key}`, kind: "group", key, g, x: sgn * X1, y: gy, r: 4 + 8 * Math.sqrt(g.n / maxN), color: tcol(g.t), sgn, dir, open: b.open };
      nodes.push(gn);
      links.push({ kind: "trunk", key, a: centre, b: gn, w: 1.25 + (4 * Math.log10(1 + g.n)) / Math.log10(1 + maxN), color: gn.color });
      nodes.push({ id: `h:${key}`, kind: "head", key, g, x: tx, y: hy, r: 0, color: gn.color, sgn, dir, open: b.open });
      order[dir].push(`h:${key}`);
      if (!b.rows) links.push({ kind: "twig", key, a: gn, b: { x: sgn * (X2 - 4), y: hy }, dash: true });
      let ly = y + HEAD + ROW / 2;
      const w = widths[dir];
      if (b.open) drawnOpen = b.leaves.reduce((a, l) => a + l.k, 0);
      for (const leaf of b.leaves) {
        const suffix = suffixOf(leaf.k);
        const room = w - (suffix ? measureText(suffix, 11.5, false) + 2 : 0);
        const n: TreeNode = {
          id: `l:${key}|${leaf.it.id}`, kind: "leaf", key, g, x: sgn * X2, y: ly, r: 4, color: gn.color, sgn, dir, leaf,
          label: fitText(nice(leaf.it.label || leaf.it.id), room, mW), suffix, full: nice(leaf.it.label || leaf.it.id),
        };
        nodes.push(n);
        order[dir].push(n.id);
        links.push({ kind: "twig", key, a: gn, b: n });
        ly += ROW;
      }
      if (b.rows > b.leaves.length) {
        // Closed: the records not drawn, and a click opens the group. Open:
        // the group's whole list, which Explore pages through.
        const m: TreeNode = b.open
          ? { id: `x:${key}`, kind: "explore", key, g, x: sgn * X2, y: ly, r: 4, color: gn.color, sgn, dir, label: exploreLinkText(read, g), href: input.exploreHref(g) }
          : { id: `m:${key}`, kind: "more", key, g, x: sgn * X2, y: ly, r: 4, color: gn.color, sgn, dir, label: `${countText({ n: b.rest, floor: g.floor })} more` };
        m.label = fitText(m.label ?? "", w, (s) => measureText(s, 12, false));
        nodes.push(m);
        order[dir].push(m.id);
        links.push({ kind: "twig", key, a: gn, b: m, dash: true });
      }
      y += h + GAP;
    }
  }
  (probe.node()?.parentNode as Element | null)?.remove();
  const openGroup = expanded ? groups.find((g) => keyOf(g) === expanded) ?? null : null;
  input.onStatus(treeStatus(read, { expanded: openGroup, drawn: drawnOpen, per, hiddenKinds }));
  const curve = (a: { x: number; y: number }, b: { x: number; y: number }) => {
    const mx = (a.x + b.x) / 2;
    return `M${a.x},${a.y} C${mx},${a.y} ${mx},${b.y} ${b.x},${b.y}`;
  };
  const L = gRoot.append("g").selectAll("path").data(links).join("path")
    .attr("class", (l) => `rt-lk ${l.kind}${l.dash ? " dash" : ""}`).attr("d", (l) => curve(l.a, l.b))
    .style("stroke", (l) => (l.kind === "trunk" ? l.color ?? null : null))
    .style("stroke-width", (l) => (l.kind === "trunk" ? String(l.w) : null));
  const G = gRoot.append("g").selectAll<SVGGElement, TreeNode>("g").data(nodes.filter((n) => n.kind === "group" || n.kind === "kinds")).join("g")
    .attr("class", (d) => `n grp rt-grp${d.open ? " open" : ""}`).attr("transform", (d) => `translate(${d.x},${d.y})`);
  G.append("circle").attr("class", "body").attr("r", (d) => d.r)
    .style("fill", (d) => (d.kind === "kinds" ? "var(--okg-color-surface)" : d.color))
    .style("stroke", (d) => (d.kind === "kinds" ? "var(--okg-color-muted)" : "var(--okg-graph-halo)"));
  const recordsWord = (g: TreeGroup) => (g.n === 1 && !g.floor ? "record" : "records");
  // group headings: record type, then relationship and count, in the list column
  const H = gRoot.append("g").selectAll<SVGGElement, TreeNode>("g").data(nodes.filter((n) => n.kind === "head")).join("g")
    .attr("class", (d) => `rt-head${d.g?.other ? " other" : ""}${d.open ? " open" : ""}`).attr("transform", (d) => `translate(${d.x},${d.y})`)
    .attr("role", "button").attr("aria-expanded", (d) => (d.g?.other ? null : String(Boolean(d.open))))
    .attr("aria-label", (d) => {
      const g = d.g as TreeGroup;
      return g.other ? `${g.t}, ${countText(g)} records, listed in the side panel` : `${relationSentence(g)}: ${countText(g)} ${recordsWord(g)}`;
    });
  H.append("rect").attr("class", "hit");
  const HT = H.append("text").attr("dy", 4).attr("text-anchor", (d) => (d.sgn < 0 ? "end" : "start"));
  HT.filter((d) => !d.g?.other).append("tspan").attr("class", "t").text((d) => d.g?.t ?? "");
  HT.filter((d) => !d.g?.other).append("tspan").attr("class", "e").attr("dx", 8).text((d) => d.g?.e ?? "");
  HT.filter((d) => !d.g?.other).append("tspan").attr("class", "c").attr("dx", 6).text((d) => countText(d.g as TreeGroup));
  HT.filter((d) => Boolean(d.g?.other)).append("tspan").attr("class", "k").text((d) => `${d.g?.t} · ${countText(d.g as TreeGroup)} records`);
  H.append("title").text((d) => {
    const g = d.g as TreeGroup;
    return g.other ? "Smaller groups, listed in the side panel" : `${countText(g)} ${recordsWord(g)}: ${relationSentence(g)}`;
  });
  H.each(function eachHead() {
    const t = (this.querySelector("text") as SVGTextElement).getBBox();
    select(this).select("rect.hit").attr("x", t.x - 6).attr("y", t.y - 3).attr("width", t.width + 12).attr("height", t.height + 6).attr("rx", 5);
  });
  const F = gRoot.append("g").selectAll<SVGGElement, TreeNode>("g").data(nodes.filter((n) => n.kind === "leaf" || n.kind === "more" || n.kind === "explore")).join("g")
    .attr("class", (d) => `n leaf rt-leaf${d.kind === "leaf" ? "" : ` more ${d.kind}`}`).attr("transform", (d) => `translate(${d.x},${d.y})`)
    .attr("role", (d) => (d.kind === "explore" ? "link" : "button"))
    .attr("aria-label", (d) => (d.kind === "leaf" ? `${d.full}${d.leaf && d.leaf.k > 1 ? `, ${d.leaf.k} listed records share this name` : ""}` : d.label ?? ""));
  F.append("rect").attr("class", "rowbg");
  F.append("circle").attr("class", "ring").attr("r", (d) => d.r + 3.5);
  F.append("circle").attr("class", "body").attr("r", (d) => d.r)
    .style("fill", (d) => (d.kind === "leaf" ? d.color : "var(--okg-color-surface)"))
    .style("stroke", (d) => (d.kind === "leaf" ? "var(--okg-graph-halo)" : d.color));
  const FT = F.append("text").classed("prose", (d) => d.kind === "leaf" && prose(d.full ?? ""))
    .attr("x", (d) => d.sgn * 10).attr("dy", 4).attr("text-anchor", (d) => (d.sgn < 0 ? "end" : "start"));
  FT.append("tspan").text((d) => d.label ?? "");
  FT.filter((d) => Boolean(d.suffix)).append("tspan").attr("class", "x").text((d) => d.suffix ?? "");
  F.append("title").text((d) => {
    if (d.kind === "more") return `Open this group: ${plural(d.g?.items.length ?? 0, "record")} listed, of ${countText(d.g as TreeGroup)}`;
    if (d.kind === "explore") return "Explore's Related tab, filtered to this relationship";
    const leaf = d.leaf as Leaf;
    return `${d.full}${leaf.k > 1 ? `\n${leaf.k} listed records share this name` : ""}\n${leaf.it.id}\nDouble-click or Enter centres the graph on it`;
  });
  F.each(function eachRow(d) {
    const t = (this.querySelector("text") as SVGTextElement).getBBox();
    const x0 = Math.min(t.x, -8);
    const x1 = Math.max(t.x + t.width, 8);
    select(this).select("rect.rowbg").attr("x", x0 - 6).attr("y", -ROW / 2 + 1).attr("width", x1 - x0 + 12).attr("height", ROW - 2).attr("rx", 5);
    void d;
  });
  // side headings, aligned with the list column
  const colY = -Math.max(sideH(blocks.in), sideH(blocks.out)) / 2 - 30;
  for (const dir of ["in", "out"] as const) {
    if (!blocks[dir].length) continue;
    const sgn = dir === "in" ? -1 : 1;
    const total = sideTotal(read, dir);
    const t = gRoot.append("text").attr("class", "rt-side").attr("x", sgn * (X2 + 10)).attr("y", colY).attr("text-anchor", sgn < 0 ? "end" : "start");
    t.append("tspan").text(SIDE_TITLE[dir].toUpperCase());
    t.append("tspan").attr("class", "c").attr("dx", 8).text(countText(total));
  }
  // the record itself, in a pill below its node so the trunks pass beneath its name
  const label = read.record.label || read.record.id;
  const C = gRoot.append("g").attr("class", "n center rt-centre").attr("role", "button")
    .attr("aria-label", `${label}, ${read.record.type}: the record this graph is centred on; Enter shows every group`);
  C.append("circle").attr("class", "body").attr("r", centre.r).style("fill", centre.color);
  const pillRect = C.append("rect");
  const pillText = C.append("text").classed("prose", prose(label)).attr("y", centre.r + 25).attr("text-anchor", "middle");
  const pW = (s: string) => { pillText.text(s); return (pillText.node() as SVGTextElement).getComputedTextLength(); };
  const clabel = fitText(label, 2 * X1 - 60, pW);
  const cw = pW(clabel) + 24;
  pillRect.attr("x", -cw / 2).attr("y", centre.r + 8).attr("width", cw).attr("height", 26).attr("rx", 13);
  C.append("title").text(`${label}\n${read.record.type} · ${read.record.id}`);

  // --- focus: one tab stop, walked with the arrow keys ---------------------
  const focusable = new Map<string, SVGGElement>();
  focusable.set("c", C.node() as SVGGElement);
  H.each(function eachH(d) { focusable.set(d.id, this); });
  F.each(function eachF(d) { focusable.set(d.id, this); });
  let current = input.focusId && focusable.has(input.focusId) ? input.focusId : "c";
  const setRoving = (id: string) => {
    current = id;
    for (const [nid, el] of focusable) el.setAttribute("tabindex", nid === id ? "0" : "-1");
  };
  const focusNode = (id: string) => {
    const el = focusable.get(id);
    if (!el) return;
    setRoving(id);
    el.focus({ preventScroll: true });
    input.onFocusNode(id);
  };
  setRoving(current);
  const sideOf = (id: string): "in" | "out" | null => (order.in.includes(id) ? "in" : order.out.includes(id) ? "out" : null);
  const walk = (ev: KeyboardEvent): boolean => {
    const side = sideOf(current);
    const first = (d: "in" | "out") => order[d][0] ?? order[d === "in" ? "out" : "in"][0];
    let next: string | undefined;
    if (ev.key === "ArrowUp" || ev.key === "ArrowDown") {
      if (!side) next = first(ev.key === "ArrowDown" ? "out" : "in");
      else {
        const list = order[side];
        const i = list.indexOf(current) + (ev.key === "ArrowDown" ? 1 : -1);
        next = list[Math.max(0, Math.min(list.length - 1, i))];
      }
    } else if (ev.key === "ArrowLeft" || ev.key === "ArrowRight") {
      const toward = ev.key === "ArrowLeft" ? "in" : "out";
      if (!side) next = order[toward][0];
      else if (side !== toward) next = "c";
    } else if (ev.key === "Home" || ev.key === "End") {
      const list = side ? order[side] : [];
      next = ev.key === "Home" ? (list[0] ?? "c") : (list[list.length - 1] ?? "c");
    } else return false;
    ev.preventDefault();
    if (next) focusNode(next);
    return true;
  };

  let sel = input.selected;
  const act = (d: TreeNode, how: "click" | "enter" | "space") => {
    if (d.kind === "center") { input.onHome(); return; }
    if (d.kind === "head" && d.g?.other) { input.onKinds(d.dir as "in" | "out"); return; }
    if (d.kind === "group" || d.kind === "head" || d.kind === "more") { input.onToggle(d.key as string); return; }
    if (d.kind === "kinds") { input.onKinds(d.dir as "in" | "out"); return; }
    if (d.kind === "explore") { input.onNavigate(d.href as string); return; }
    const id = (d.leaf as Leaf).it.id;
    if (how === "enter") { input.onOpen(id); return; }
    const on = sel && "key" in sel && sel.key === d.key && sel.id !== null && (d.leaf as Leaf).ids.includes(sel.id);
    input.onSelectLeaf(d.key as string, on ? null : id);
  };
  const onClick = (ev: MouseEvent, d: TreeNode) => {
    ev.stopPropagation();
    if (focusable.has(d.id)) setRoving(d.id);
    act(d, "click");
  };
  const onKey = (ev: KeyboardEvent, d: TreeNode) => {
    if (walk(ev)) return;
    if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); ev.stopPropagation(); act(d, ev.key === "Enter" ? "enter" : "space"); }
  };
  G.on("click", onClick);
  H.on("click", onClick).on("keydown", onKey);
  F.on("click", onClick).on("keydown", onKey);
  F.on("dblclick", (_ev, d) => { if (d.kind === "leaf") input.onOpen((d.leaf as Leaf).it.id); });
  C.datum(centre).on("click", onClick).on("keydown", onKey);
  for (const [id, el] of focusable) el.addEventListener("focus", () => { if (current !== id) setRoving(id); });

  const applySel = () => {
    const key = sel && "key" in sel ? sel.key : sel && "kinds" in sel ? `kinds|${sel.kinds}` : null;
    const id = sel && "key" in sel ? sel.id : null;
    const leafSel = (d: TreeNode) => d.kind === "leaf" && d.key === key && id !== null && (d.leaf as Leaf).ids.includes(id);
    const anyLeaf = nodes.some(leafSel);
    G.classed("sel", (d) => d.key === key && !anyLeaf).classed("dim", (d) => Boolean(key) && d.key !== key);
    H.classed("sel", (d) => d.key === key && !anyLeaf).classed("dim", (d) => Boolean(key) && d.key !== key);
    F.classed("sel", leafSel).classed("dim", (d) => Boolean(key) && d.key !== key);
    L.classed("dim", (l) => Boolean(key) && l.key !== key);
  };

  const zoomBehaviour: ZoomBehavior<SVGSVGElement, unknown> = zoom<SVGSVGElement, unknown>().scaleExtent([0.2, 4])
    .on("zoom", (ev: { transform: { toString(): string } }) => { gRoot.attr("transform", ev.transform.toString()); });
  svg.call(zoomBehaviour).on("dblclick.zoom", null);
  svg.on("click", (ev: MouseEvent) => { if (ev.target === input.svg) input.onBackground(); });
  // Fit the drawn extent (headings and labels included) between the stage
  // edges and clear of the zoom buttons; shrink only if it cannot fit.
  const fit = (animate: boolean) => {
    const W = stage.clientWidth;
    const Hh = stage.clientHeight;
    const b = (gRoot.node() as SVGGElement).getBBox();
    let k = Math.min(1, (W - edgeL - edgeR) / b.width, (Hh - 24) / b.height);
    if (k > 0.97) k = 1; // a few pixels of margin beat shrunken text
    const tx = edgeL + ((W - edgeL - edgeR) - k * b.width) / 2 - k * b.x;
    const ty = k * b.height <= Hh - 24 ? (Hh - k * b.height) / 2 - k * b.y : 16 - k * b.y;
    const target = zoomIdentity.translate(tx, ty).scale(k);
    if (animate) svg.transition().duration(300).call(zoomBehaviour.transform, target);
    else svg.call(zoomBehaviour.transform, target);
  };
  fit(false);
  applySel();
  if (input.focusId) focusNode(current);
  return {
    setSelection: (next) => { sel = next; applySel(); },
    zoomBy: (factor) => { svg.transition().duration(200).call(zoomBehaviour.scaleBy, factor); },
    fit,
    destroy: () => { svg.on(".zoom", null); svg.selectAll("*").remove(); },
  };
}
