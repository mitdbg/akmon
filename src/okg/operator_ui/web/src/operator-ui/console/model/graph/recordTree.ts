// The record neighbourhood as a two-sided tree, laid out without the DOM.
//
// Ported from the approved prototype (v2.html, drawRecord and rtSide): links
// to this record on the left, links from it on the right. One node per group
// (direction + relationship + record type). Each group's heading and records
// form a list in the outer column, so no label is rotated, overlapped or
// crossed by a line. Opening a group collapses the others to their headings so
// its records fit the stage. Text widths come in as functions.
//
// A group's count is its distinct neighbour RECORDS (the read's contract), so
// a record linked twice by one relationship counts once. A record linked by
// two relationships sits in two groups; the side totals add groups, so they
// count record-and-relationship pairs, which the page calls links.
import { fmt, plural } from "@operator-ui/console/model/graph/derive";
import type { FloorReason, NeighbourGroup, NeighbourItem, RecordNeighbourhoodRead } from "@operator-ui/console/model/graph/reads";

export interface TreeGroup {
  d: "in" | "out";
  e: string;
  t: string;
  /** The read's type: null for neighbours absent from this version. */
  type: string | null;
  n: number;
  floor: boolean;
  items: NeighbourItem[];
  /** The folded "N more kinds" entry. */
  other?: boolean;
}

export const SIDE_CAP = 10;
export const ROW = 22;
export const HEAD = 24;
export const GAP = 14;
export const TOP = 64;

export const keyOf = (g: Pick<TreeGroup, "d" | "e" | "t">): string => `${g.d}|${g.e}|${g.t}`;

/** What a group of neighbours absent from this version is called. */
export const ABSENT_TYPE = "not in this version";

export function toTreeGroup(g: NeighbourGroup): TreeGroup {
  return { d: g.direction, e: g.relationship, t: g.type ?? ABSENT_TYPE, type: g.type, n: g.count, floor: g.countIsFloor, items: g.items };
}

export function toTreeGroups(groups: NeighbourGroup[]): TreeGroup[] {
  return groups.map(toTreeGroup);
}

/** The direction words the whole console uses. */
export const SIDE_TITLE = { in: "Links to this record", out: "Links from this record" } as const;

/** One side's total, from the per-relationship counts, and whether any of
 *  them stopped at the cap. */
export function sideTotal(read: RecordNeighbourhoodRead, dir: "in" | "out"): { n: number; floor: boolean } {
  const rows = read.relationships.filter((r) => r.direction === dir);
  return { n: rows.reduce((a, r) => a + r.count, 0), floor: rows.some((r) => r.countIsFloor) || read.capped[dir] };
}

/** Types in colour order: the record's own type first, then by group size. */
export function kindList(read: RecordNeighbourhoodRead): string[] {
  return [...new Set([read.record.type].concat(read.groups.slice().sort((a, b) => b.count - a.count).map((g) => g.type ?? ABSENT_TYPE)))];
}

/** "git author (name withheld in this export)" for the placeholder label. */
export function nice(s: string): string {
  return s === "<git author>" ? "git author (name withheld in this export)" : s;
}

/** A listed neighbour's name: its label, or its id when it has none. */
export const itemName = (it: Pick<NeighbourItem, "id" | "label">): string => nice(it.label || it.id);

/** Paths keep their first segment and their file name, cut in the middle;
 *  other text keeps its head. */
export function trunc(s: string, n: number): string {
  const text = String(s);
  if (text.length <= n) return text;
  if (n <= 1) return "…";
  if (text.includes("/") && !text.includes(" ")) {
    const avail = n - 1;
    const fileTail = text.length - text.lastIndexOf("/");
    // Too narrow for a head as well: the file name alone, whole if it fits.
    if (fileTail > avail - 4) return `…${text.slice(-Math.min(avail, fileTail))}`;
    const tail = Math.min(avail, Math.max(Math.ceil(avail * 0.6), Math.min(fileTail, avail - 4)));
    // Cut at segment boundaries where the path has them.
    let head = text.slice(0, avail - tail);
    let back = text.slice(-tail);
    const hs = head.lastIndexOf("/");
    if (hs > 0) head = head.slice(0, hs + 1);
    const ts = back.indexOf("/");
    if (ts > 0 && back.length - ts >= fileTail) back = back.slice(ts);
    return `${head}…${back}`;
  }
  return `${text.slice(0, n - 1)}…`;
}

/** The longest truncation of `s` that measures within `w`. */
export function fitText(s: string, w: number, measure: (x: string) => number): string {
  if (measure(s) <= w) return s;
  let lo = 1;
  let hi = s.length;
  while (lo < hi) {
    const m = (lo + hi + 1) >> 1;
    if (measure(trunc(s, m)) <= w) lo = m; else hi = m - 1;
  }
  return trunc(s, lo);
}

/** Messages and titles read in the text face; identifiers stay mono. */
export const prose = (s: string): boolean => /\s/.test(s);

export interface Side { shown: TreeGroup[]; hidden: TreeGroup[] }

/** The groups one side draws, largest first, and those folded into "more
 *  kinds". An opened group that would be folded takes the last slot. */
export function rtSide(groups: TreeGroup[], dir: "in" | "out", cap: number, open: string | null): Side {
  const gs = groups.filter((g) => g.d === dir).sort((a, b) => b.n - a.n);
  if (gs.length <= cap) return { shown: gs, hidden: [] };
  let shown = gs.slice(0, cap - 1);
  let hidden = gs.slice(cap - 1);
  const oi = hidden.findIndex((g) => keyOf(g) === open);
  if (oi >= 0) {
    const g = hidden[oi];
    hidden = hidden.filter((_, i) => i !== oi).concat([shown[cap - 2]]).sort((a, b) => b.n - a.n);
    shown = shown.slice(0, cap - 2).concat([g]);
  }
  return {
    shown: shown.concat([{
      d: dir, e: "", t: plural(hidden.length, "more kind"), type: null, n: hidden.reduce((a, g) => a + g.n, 0),
      floor: hidden.some((g) => g.floor), items: [], other: true,
    }]),
    hidden,
  };
}

/** One listed name: the first record carrying it, how many of the listed
 *  records share it, and their ids. */
export interface Leaf { it: NeighbourItem; k: number; ids: string[] }

/** The listed records with repeated names folded, in the read's order. The
 *  tree and the side panel both draw these, so they always agree. */
export function foldNames(items: NeighbourItem[]): Leaf[] {
  const by = new Map<string, Leaf>();
  for (const it of items) {
    const nm = itemName(it);
    const seen = by.get(nm);
    if (seen) { seen.k += 1; seen.ids.push(it.id); } else by.set(nm, { it, k: 1, ids: [it.id] });
  }
  return [...by.values()];
}

export function leavesOf(g: TreeGroup, lim: number): Leaf[] {
  return foldNames(g.items).slice(0, lim);
}

/** The fold a record id belongs to in a group's listed records. */
export function foldOf(items: NeighbourItem[], id: string): Leaf | null {
  return foldNames(items).find((l) => l.ids.includes(id)) ?? null;
}

export interface Block {
  g: TreeGroup;
  leaves: Leaf[];
  /** Records of the group not drawn (the group's count less the drawn names' records). */
  rest: number;
  rows: number;
  open: boolean;
}

export const blockH = (rows: number): number => HEAD + rows * ROW;

export const sideH = (bl: Block[]): number => bl.reduce((a, b) => a + blockH(b.rows), 0) + GAP * Math.max(0, bl.length - 1);

function plan(g: TreeGroup, lim: number, expanded: string | null): Block {
  if (g.other) return { g, leaves: [], rest: 0, rows: 0, open: false };
  const leaves = lim > 0 ? leavesOf(g, lim) : [];
  const rest = Math.max(0, g.n - leaves.reduce((a, x) => a + x.k, 0));
  const open = expanded === keyOf(g);
  return { g, leaves, rest, rows: leaves.length + (rest > 0 && (lim > 0 || open) ? 1 : 0), open };
}

/**
 * How many names each group lists at this stage height: six each, fewer
 * until both sides fit, or none (headings only). With a group open, the
 * others collapse to headings and it takes the room that is left.
 */
export function planBlocks(
  sides: Record<"in" | "out", Side>, expanded: string | null, avail: number,
): { blocks: Record<"in" | "out", Block[]>; per: number } {
  const layout = (per: number) => {
    const out = {} as Record<"in" | "out", Block[]>;
    for (const d of ["in", "out"] as const) {
      out[d] = sides[d].shown.map((g) => plan(g, expanded ? (expanded === keyOf(g) ? 999 : 0) : per, expanded));
    }
    if (expanded) {
      for (const d of ["in", "out"] as const) {
        for (const b of out[d]) {
          if (!b.open) continue;
          const others = sideH(out[d].filter((x) => x !== b)) + GAP * (out[d].length > 1 ? 1 : 0);
          const room = Math.max(3, Math.floor((avail - others - HEAD) / ROW) - 1);
          Object.assign(b, plan(b.g, room, expanded));
        }
      }
    }
    return out;
  };
  let per = 6;
  let blocks = layout(per);
  while (!expanded && per > 0 && Math.max(sideH(blocks.in), sideH(blocks.out)) > avail) {
    per -= 1;
    blocks = layout(per);
  }
  return { blocks, per };
}

/** A count as the page prints it: "500+" when the count stopped at the cap. */
export const countText = (g: Pick<TreeGroup, "n" | "floor">): string => `${fmt(g.n)}${g.floor ? "+" : ""}`;

/** "`git_commit` affects this record" / "this record contains `code_import`",
 *  as plain text (the rail sets the identifiers in mono). */
export function relationSentence(g: Pick<TreeGroup, "d" | "e" | "t">): string {
  return g.d === "in" ? `${g.t} ${g.e} this record` : `this record ${g.e} ${g.t}`;
}

/** What the Explore link of a group opens: Explore filters by relationship
 *  and direction, not by type, so a relationship reaching several types is
 *  named as the relationship. */
export function exploreLinkText(read: RecordNeighbourhoodRead, g: Pick<TreeGroup, "d" | "e" | "n" | "floor">): string {
  const siblings = read.groups.filter((x) => x.direction === g.d && x.relationship === g.e).length;
  if (siblings <= 1) return `All ${countText(g)} in Explore`;
  const rel = read.relationships.find((r) => r.direction === g.d && r.relationship === g.e);
  const n = rel ? `${fmt(rel.count)}${rel.countIsFloor ? "+" : ""} ` : "";
  return `All ${n}“${g.e}” ${g.d === "in" ? "links to" : "links from"} this record in Explore`;
}

/** The cap that stopped a "+" count, named. An older read sends no reason:
 *  under a policy that is the per-group check, otherwise the count cap. */
export function capClause(read: RecordNeighbourhoodRead, reason: FloorReason | null): string {
  const r = reason ?? (read.countBasis === "visible_to_reader" ? "scan_cap" : "count_cap");
  if (r === "check_budget") return `the read stopped after checking ${fmt(read.checkBudget ?? 0)} links`;
  if (r === "scan_cap") return `the access check stopped at ${fmt(read.scanCap)} links of the group`;
  return `the count stopped at ${fmt(read.countCap ?? read.scanCap)} links of the relationship`;
}

/** The caps behind this read's "+" counts, each named once: those of the
 *  groups shown and those the server names for each side, which include
 *  groups the reader sees nothing of (a relationship's "+" can come from a
 *  group that is not drawn). */
export function capClauses(read: RecordNeighbourhoodRead): string[] {
  const reasons: (FloorReason | null)[] = [
    ...read.groups.filter((g) => g.countIsFloor).map((g) => g.floorReason),
    ...read.cappedBy.in, ...read.cappedBy.out,
  ];
  return [...new Set(reasons.map((r) => capClause(read, r)))];
}

/** What the read could not count, one sentence each: relationships the check
 *  budget never reached, and a side with more links than were checked but
 *  none visible among them. Never a zero. */
export function limitNotes(read: RecordNeighbourhoodRead): string[] {
  const out: string[] = [];
  for (const dir of ["in", "out"] as const) {
    if (read.uncheckedLinks[dir]) {
      out.push(`${SIDE_TITLE[dir]}: the read stopped before checking every relationship (it checks at most ${fmt(read.checkBudget ?? 0)} links), so this side's count is a floor`);
    } else if (read.capped[dir] && !read.groups.some((g) => g.direction === dir)) {
      const why = read.cappedBy[dir].length
        ? read.cappedBy[dir].map((r) => capClause(read, r)).join("; ")
        : capClause(read, null);
      out.push(`${SIDE_TITLE[dir]}: none visible among the links checked, and more exist that were not checked (${why}), so the number is not known`);
    }
  }
  return out;
}

/** True only when the record has no relationship at all: not when the read
 *  stopped before finding a visible one. */
export function hasNoRelationships(read: RecordNeighbourhoodRead): boolean {
  return !read.groups.length && !read.capped.in && !read.capped.out && !read.uncheckedLinks.in && !read.uncheckedLinks.out;
}

/** What counts mean on this read, in one clause. */
export function basisText(read: RecordNeighbourhoodRead): string {
  return read.countBasis === "visible_to_reader"
    ? "Counts are the records the access policy lets this console see"
    : "Counts are published records, before access rules";
}

/** The line under the tree: only what the drawing alone cannot say. */
export function treeStatus(
  read: RecordNeighbourhoodRead,
  state: { expanded: TreeGroup | null; drawn: number; per: number; hiddenKinds: number },
): string {
  const parts: string[] = [];
  if (!read.groups.length) return basisText(read);
  if (state.expanded) {
    const g = state.expanded;
    parts.push(`${fmt(state.drawn)} of ${countText(g)} records drawn (${relationSentence(g)}); the side panel lists ${g.n > g.items.length || g.floor ? `the first ${fmt(g.items.length)} by id` : `all ${fmt(g.n)}`}`);
  } else if (state.per) {
    parts.push(`Each group lists its first ${state.per === 1 ? "name" : `${state.per} names`} by id`);
  } else {
    parts.push("Groups shown as counts at this height");
  }
  if (state.hiddenKinds) parts.push(`${plural(state.hiddenKinds, "smaller group")} under “more kinds”`);
  const incomplete = read.relationships.filter((r) => r.typesComplete === false);
  for (const r of incomplete) {
    parts.push(`${r.relationship} ${r.direction === "in" ? "to" : "from"} this record has more than ${fmt(read.countCap ?? read.scanCap)} links; types seen only past them are not named`);
  }
  const caps = capClauses(read);
  if (caps.length) parts.push(`“+” marks a count that is a floor: ${caps.join("; ")}`);
  parts.push(...limitNotes(read));
  parts.push(basisText(read));
  return parts.join(" · ");
}

/** Column positions: groups at X1, records at X2, both scaled to the stage. */
export function columns(stW: number): { X1: number; X2: number } {
  const X1 = Math.max(120, Math.round(stW * 0.13));
  return { X1, X2: X1 + Math.max(90, Math.round(stW * 0.085)) };
}

/** Label width per side: each keeps its natural width when both fit;
 *  otherwise the wider side gives way first. */
export function sideWidths(spare: number, nat: { in: number; out: number }): { in: number; out: number } {
  return nat.in + nat.out <= spare
    ? nat
    : { in: Math.max(spare / 2, spare - nat.out), out: Math.max(spare / 2, spare - nat.in) };
}
