// Pure geometry shared by the type map and the record tree: boxes, the
// curved link path, screen-space label placement and the fit-to-stage scale.
// Ported from the approved prototype (v2.html, "Graph drawing shared by both
// scopes"); nothing here touches the DOM, so all of it is unit-tested.

export interface Box { x0: number; x1: number; y0: number; y1: number }

export const overlap = (a: Box, b: Box): boolean => a.x0 < b.x1 && a.x1 > b.x0 && a.y0 < b.y1 && a.y1 > b.y0;

export interface Circle { x: number; y: number; r: number }

export interface LinkGeometry { d: string; x1: number; y1: number; mx: number; my: number; x2: number; y2: number }

/** A curved path between two circles, ending on each rim; bend > 0 curves right. */
export function linkPath(a: Circle, b: Circle, bend: number): LinkGeometry {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const dist = Math.hypot(dx, dy) || 1;
  const mx = (a.x + b.x) / 2 - (dy / dist) * bend;
  const my = (a.y + b.y) / 2 + (dx / dist) * bend;
  const sa = Math.atan2(my - a.y, mx - a.x);
  const ea = Math.atan2(my - b.y, mx - b.x);
  const x1 = a.x + Math.cos(sa) * (a.r + 2);
  const y1 = a.y + Math.sin(sa) * (a.r + 2);
  const x2 = b.x + Math.cos(ea) * (b.r + 2);
  const y2 = b.y + Math.sin(ea) * (b.r + 2);
  return { d: `M${x1},${y1} Q${mx},${my} ${x2},${y2}`, x1, y1, mx, my, x2, y2 };
}

/** The point at u (0..1) along a link's quadratic curve. */
export const qpt = (g: LinkGeometry, u: number): [number, number] => [
  (1 - u) * (1 - u) * g.x1 + 2 * (1 - u) * u * g.mx + u * u * g.x2,
  (1 - u) * (1 - u) * g.y1 + 2 * (1 - u) * u * g.my + u * u * g.y2,
];

export interface LabelItem<K = unknown> { cands: (Box & { side?: string })[]; force?: boolean; kind: string; payload?: K }

export type Placed<T extends LabelItem> = T & { b: Box & { side?: string } };

/**
 * Screen-space label placement. Items arrive in priority order; each tries
 * its candidate boxes and takes the first free one that crosses the fewest
 * drawn line points (`pts`: [x, y, weight]). An item that finds no free box
 * is left out unless it is forced, in which case it takes the candidate --
 * moved inside the stage -- that covers the least already placed.
 * `placed` is extended in place.
 */
export function placeLabels<T extends LabelItem>(
  items: T[], placed: Box[], pts: [number, number, number][], W: number, H: number,
): Placed<T>[] {
  const inside = (b: Box) => b.x0 >= 4 && b.x1 <= W - 4 && b.y0 >= 4 && b.y1 <= H - 4;
  // The line points, bucketed on a coarse grid, so a box counts only the
  // points in the cells it covers (the map samples thousands).
  const CELL = 48;
  const grid = new Map<number, [number, number, number][]>();
  const cell = (cx: number, cy: number) => cx * 100003 + cy;
  for (const p of pts) {
    const key = cell(Math.floor(p[0] / CELL), Math.floor(p[1] / CELL));
    const bucket = grid.get(key);
    if (bucket) bucket.push(p); else grid.set(key, [p]);
  }
  const cross = (b: Box) => {
    let c = 0;
    for (let cx = Math.floor(b.x0 / CELL); cx <= Math.floor(b.x1 / CELL); cx += 1) {
      for (let cy = Math.floor(b.y0 / CELL); cy <= Math.floor(b.y1 / CELL); cy += 1) {
        for (const p of grid.get(cell(cx, cy)) ?? []) if (p[0] > b.x0 && p[0] < b.x1 && p[1] > b.y0 && p[1] < b.y1) c += p[2];
      }
    }
    return c;
  };
  const out: Placed<T>[] = [];
  for (const it of items) {
    let best: (Box & { side?: string }) | null = null;
    let bc = Infinity;
    for (const c of it.cands) {
      if (!inside(c) || placed.some((o) => overlap(c, o))) continue;
      const x = cross(c);
      if (x < bc) { best = c; bc = x; if (!x) break; }
    }
    if (!best && it.force && it.cands.length) {
      const clamp = (c: Box & { side?: string }) => {
        const dx = Math.max(0, 4 - c.x0) - Math.max(0, c.x1 - (W - 4));
        const dy = Math.max(0, 4 - c.y0) - Math.max(0, c.y1 - (H - 4));
        return { ...c, x0: c.x0 + dx, x1: c.x1 + dx, y0: c.y0 + dy, y1: c.y1 + dy };
      };
      const area = (c: Box) => placed.reduce((s, o) => s
        + Math.max(0, Math.min(c.x1, o.x1) - Math.max(c.x0, o.x0))
        * Math.max(0, Math.min(c.y1, o.y1) - Math.max(c.y0, o.y0)), 0);
      best = it.cands.map(clamp).reduce((m, c) => (area(c) < area(m) ? c : m));
    }
    if (!best) continue;
    placed.push(best);
    out.push({ ...it, b: best });
  }
  return out;
}

export interface FitNode { x: number; y: number; r: number; lw?: number }

export interface Transform { x: number; y: number; k: number }

/**
 * The transform that fits the map to the stage. Labels are drawn at a fixed
 * screen size, so the scale is solved with each node's label width counted in
 * screen pixels, and room is kept for the legend (`left`), the zoom buttons
 * (`right`) and the source captions (top). Scale is held to 0.2..1.5. With
 * no nodes it is the identity.
 */
export function fitTransform(
  nodes: FitNode[], stage: { W: number; H: number; left: number; right: number }, pad: number,
): Transform {
  const { W, H, left, right } = stage;
  // Nothing to fit (every node hidden, or none at all): the identity, never
  // a transform of NaN.
  if (!nodes.length) return { x: left, y: 0, k: 1 };
  const top = 30;
  const aw = W - left - right - pad * 2;
  const ah = H - pad * 2;
  const ext = (k: number) => {
    let x0 = Infinity; let x1 = -Infinity; let y0 = Infinity; let y1 = -Infinity;
    for (const n of nodes) {
      const hw = Math.max(k * n.r, (n.lw ?? 0) / 2);
      x0 = Math.min(x0, k * n.x - hw);
      x1 = Math.max(x1, k * n.x + hw);
      y0 = Math.min(y0, k * (n.y - n.r) - top);
      y1 = Math.max(y1, k * (n.y + n.r) + 32);
    }
    return { x0, x1, y0, y1 };
  };
  let k = 1;
  for (let i = 0; i < 40; i += 1) {
    const e = ext(k);
    k = Math.max(0.2, Math.min(1.5, k * Math.min(aw / (e.x1 - e.x0), ah / (e.y1 - e.y0))));
  }
  const e = ext(k);
  return { x: left + pad + aw / 2 - (e.x0 + e.x1) / 2, y: pad + ah / 2 - (e.y0 + e.y1) / 2, k };
}
