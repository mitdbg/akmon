// The Grid view's two matrices, as numbers: the cells, the row and column
// totals and each cell's shade. Ported from the approved prototype (v2.html,
// gridTable and heat).
import type { GraphModel, Pattern, SourceLinkModel } from "@operator-ui/console/model/graph/derive";

export interface Matrix {
  rows: string[];
  cols: string[];
  cells: number[][];
  max: number;
  rowTot: number[];
  colTot: number[];
  grand: number;
}

export function buildMatrix(rows: string[], cols: string[], val: (r: string, c: string) => number): Matrix {
  const cells = rows.map((r) => cols.map((c) => val(r, c)));
  const max = Math.max(1, ...cells.flat());
  const rowTot = cells.map((row) => row.reduce((a, n) => a + n, 0));
  const colTot = cols.map((_, j) => cells.reduce((a, row) => a + row[j], 0));
  return { rows, cols, cells, max, rowTot, colTot, grand: colTot.reduce((a, n) => a + n, 0) };
}

/** The heat colour's share in a cell's background, 10 to 60 per cent on a
 *  log scale. It tops out at 60 so the body text keeps at least 4.5:1 on
 *  every cell, in all four themes: each theme sets `--okg-graph-heat` to a
 *  shade that clears it (styles/tokens.css). */
export function heatShare(n: number, max: number): number {
  return Math.round((0.10 + (0.50 * Math.log10(1 + n)) / Math.log10(1 + max)) * 100);
}

export function heatBackground(n: number, max: number): string {
  return `color-mix(in srgb, var(--okg-graph-heat) ${heatShare(n, max)}%, var(--okg-color-surface))`;
}

/** Types x types: relationships from the row type to the column type. */
export function typeTypeMatrix(model: GraphModel): { matrix: Matrix; cell: Record<string, Pattern[]>; unlinked: string[] } {
  const { types, patterns } = model;
  const src = types.filter((t) => patterns.some((p) => p.s === t));
  const dst = types.filter((t) => patterns.some((p) => p.d === t));
  const cell: Record<string, Pattern[]> = {};
  for (const p of patterns) (cell[`${p.s}|${p.d}`] ??= []).push(p);
  const matrix = buildMatrix(src, dst, (s, d) => (cell[`${s}|${d}`] ?? []).reduce((a, p) => a + p.n, 0));
  const unlinked = types.filter((t) => !src.includes(t) && !dst.includes(t));
  return { matrix, cell, unlinked };
}

export function sourceSourceMatrix(links: SourceLinkModel): Matrix {
  return buildMatrix(links.sources, links.sources, (a, b) => links.cells[`${a}|${b}`]?.n ?? 0);
}

/** "view has none." / "view and view_definition have none." */
export function unlinkedSentence(types: readonly string[]): string {
  if (!types.length) return "";
  const list = types.length === 1 ? types[0] : `${types.slice(0, -1).join(", ")} and ${types[types.length - 1]}`;
  return `${list} ${types.length === 1 ? "has" : "have"} no relationships.`;
}
