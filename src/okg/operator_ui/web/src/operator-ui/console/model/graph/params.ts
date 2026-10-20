// The Graph view's URL: which drawing is shown, what is selected or open on
// it, and the one line that names whatever the URL asked for that the page
// could not honour.
//
// The URL is the state. Every selection and toggle a reader would share or
// come Back to lives in the hash, under the page's own route:
//
//   map      #console/graph?view=map&type=<type>&hide=<ownership id>,...
//   record   #console/graph/record?id=<id>&group=<in|out>:<relationship>:<type>&sel=<id>
//   table    #console/graph?view=table&open=<type>&empty=1
//   grid     #console/graph?view=grid&cell=<tt|ss>:<row>|<col>
//
// Moving between the map and a record, or to another record, is a link and
// PUSHES a history entry; a selection or toggle within one page REPLACES the
// current entry (`setParams`), so Back leaves the page rather than undoing
// each click.
import { exploreQuery } from "@operator-ui/console/model/explore/params";
import type { NeighbourGroup, RecordNeighbourhoodRead } from "@operator-ui/console/model/graph/reads";
import { looksLikeVersionId } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";

export type GraphMode = "map" | "table" | "grid";

export const GRAPH_MODES: readonly { value: GraphMode; label: string }[] = [
  { value: "map", label: "Map" },
  { value: "table", label: "Table" },
  { value: "grid", label: "Grid" },
];

/** Below this width a page whose URL names no view opens on the Table. */
export const TABLE_FIRST_BELOW_PX = 700;

/** The drawing the URL asks for, or null when it names none (or names
 *  something that is not a view; `graphIgnored` says so). */
export function askedMode(params: Record<string, string>): GraphMode | null {
  const asked = params.view;
  return asked === "map" || asked === "table" || asked === "grid" ? asked : null;
}

/**
 * The drawing a page opens on when the URL names none: the Table on a phone,
 * the map above it. A record page is always its own drawing (its phone form
 * is the grouped lists, which the record view chooses), so it opens on the
 * map mode whatever the width. Decided ONCE, when the page opens: it used to
 * flip between the map and the Table as the window was resized.
 */
export function defaultMode(onRecord: boolean, windowWidth: number): GraphMode {
  if (onRecord) return "map";
  return windowWidth < TABLE_FIRST_BELOW_PX ? "table" : "map";
}

/** Where a record's neighbourhood lives. Always the drawing: Table and Grid
 *  are whole-graph views and carry no record. */
export function graphRecordHref(link: LinkFn, id: string): string {
  return link("graph/record", { id });
}

/** Change this page's URL parameters: a null or empty value removes one. A
 *  selection or toggle REPLACES the history entry; `push` adds one. */
export type SetParams = (patch: Record<string, string | null>, opts?: { push?: boolean }) => void;

/** Change the URL. `push` adds a history entry (moving to another page);
 *  otherwise the current entry is replaced (a selection within this page). */
export function goTo(href: string, push: boolean): void {
  if (typeof window === "undefined") return;
  if (push) window.location.hash = href;
  else window.location.replace(href);
}

/** The parameters a page's own links carry: everything on the URL except
 *  what the route itself adds back (`link` carries `version`, and the design
 *  harness `fixture`). */
function carried(params: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(params)) {
    if (key !== "version" && key !== "fixture") out[key] = value;
  }
  return out;
}

/** The href `setParams` goes to: the current parameters with `patch`
 *  applied, a null or empty value removing its key. */
export function patchedHref(
  link: LinkFn, route: string, params: Record<string, string>, patch: Record<string, string | null>,
): string {
  const next = carried(params);
  for (const [key, value] of Object.entries(patch)) {
    if (value === null || value === "") delete next[key];
    else next[key] = value;
  }
  return link(route, next);
}

/**
 * The href of one entry in the Map / Table / Grid switch. From a record page
 * it goes to the WHOLE-graph Table or Grid and drops the record: Table and
 * Grid describe the deployment, and a URL that kept `id=` under them named a
 * record the page no longer showed.
 */
export function modeHref(link: LinkFn, mode: GraphMode): string {
  return link("graph", { view: mode });
}

/** Parameters a page understands, by the page. Anything else on the URL is
 *  named on the ignored-parameter line. `version` is the console's own and
 *  `fixture` the design harness's. */
export const PAGE_PARAMS: Readonly<Record<GraphMode | "record", readonly string[]>> = {
  map: ["view", "type", "hide", "expand"],
  record: ["view", "id", "group", "sel"],
  table: ["view", "open", "empty"],
  grid: ["view", "cell"],
};

const SHARED_PARAMS = ["version", "fixture"];

export interface IgnoredGraphParam {
  name: string;
  /** Printed only when it is safe to: `version` never is (a DSN once
   *  arrived there), and a long value is shortened. */
  value: string | null;
  why: string;
}

/** A check a page runs on one of its parameters: the reason it cannot
 *  honour the value, or null when it can. */
export type ParamCheck = (value: string) => string | null;

export const NOT_A_VIEW = "the Graph view has a Map, a Table and a Grid";
export const NOT_ON_THIS_PAGE = "this page does not use it";
export const NOT_A_VERSION = "it is not a published version id";
export const NOT_ON_A_RECORD = "the Table and the Grid describe the whole graph, not one record";

/**
 * Every parameter the URL carries that this page could not honour, in URL
 * order. `checks` are the page's own value checks (an unknown type, a cell
 * outside the grid); the names, `view` and `version` are checked here.
 */
export function graphIgnored(
  params: Record<string, string>,
  page: GraphMode | "record",
  checks: Readonly<Record<string, ParamCheck>> = {},
  subPage: string | null = null,
): IgnoredGraphParam[] {
  const out: IgnoredGraphParam[] = [];
  if (subPage !== null && subPage !== "record") {
    out.push({ name: `graph/${subPage}`, value: null, why: "the Graph view's pages are the map and graph/record" });
  }
  const known = new Set([...PAGE_PARAMS[page], ...SHARED_PARAMS]);
  for (const [name, raw] of Object.entries(params)) {
    const value = raw.trim();
    if (name === "version") {
      if (value && !looksLikeVersionId(value)) out.push({ name, value: null, why: NOT_A_VERSION });
      continue;
    }
    if (!known.has(name)) {
      out.push({ name, value: shown(value), why: NOT_ON_THIS_PAGE });
      continue;
    }
    if (name === "view" && value && askedMode(params) === null) {
      out.push({ name, value: shown(value), why: NOT_A_VIEW });
      continue;
    }
    if (name === "view" && page === "record" && value && value !== "map") {
      out.push({ name, value: shown(value), why: NOT_ON_A_RECORD });
      continue;
    }
    const check = Object.prototype.hasOwnProperty.call(checks, name) ? checks[name] : null;
    const why = check !== null && value ? check(value) : null;
    if (why) out.push({ name, value: shown(value), why });
  }
  return out;
}

function shown(value: string): string | null {
  if (!value) return null;
  return value.length > 60 ? `${value.slice(0, 57)}…` : value;
}

/** One sentence per reason, as Explore's ignored-parameter line reads. */
export function graphIgnoredSentence(ignored: readonly IgnoredGraphParam[]): string | null {
  if (!ignored.length) return null;
  const byWhy = new Map<string, IgnoredGraphParam[]>();
  for (const entry of ignored) byWhy.set(entry.why, [...(byWhy.get(entry.why) ?? []), entry]);
  return [...byWhy.entries()].map(([why, entries]) => {
    const named = entries.map((e) => (e.value !== null ? `${e.name}=${e.value}` : e.name));
    const list = named.length === 1 ? named[0] : `${named.slice(0, -1).join(", ")} and ${named[named.length - 1]}`;
    return `This link asked for ${list}, which this page cannot honour: ${why}.`;
  }).join(" ");
}

/** A Grid cell on the URL: `cell=tt:<row>|<col>`. The row and column are
 *  record types or producer ids, and a producer id may hold a colon
 *  (`_enricher:x`), so the grid is split off at the FIRST colon and the row
 *  from the column at a `|`, which neither holds. */
export interface GridCell { grid: "tt" | "ss"; row: string; col: string }

export function parseCell(value: string | undefined): GridCell | null {
  if (!value) return null;
  const at = value.indexOf(":");
  if (at < 0) return null;
  const grid = value.slice(0, at);
  if (grid !== "tt" && grid !== "ss") return null;
  const rest = value.slice(at + 1);
  const bar = rest.indexOf("|");
  if (bar <= 0 || bar === rest.length - 1) return null;
  return { grid, row: rest.slice(0, bar), col: rest.slice(bar + 1) };
}

export function cellParam(cell: GridCell): string {
  return `${cell.grid}:${cell.row}|${cell.col}`;
}

// ---- The record page: `group=` (the open group) and `sel=` (the selected
// neighbour). A neighbour absent from the version has no type: its group is
// written with an empty type (`out:contains:`).

export interface GroupRef { direction: "in" | "out"; relationship: string; type: string | null }

export function groupParam(g: GroupRef): string {
  return `${g.direction}:${g.relationship}:${g.type ?? ""}`;
}

export function sameGroup(a: GroupRef, b: GroupRef): boolean {
  return a.direction === b.direction && a.relationship === b.relationship && a.type === b.type;
}

/** The group a `group=` value names, if this read has it. */
export function groupFromParam(read: RecordNeighbourhoodRead, value: string | undefined): NeighbourGroup | null {
  if (!value) return null;
  const first = value.indexOf(":");
  const second = first < 0 ? -1 : value.indexOf(":", first + 1);
  if (second < 0) return null;
  const direction = value.slice(0, first);
  if (direction !== "in" && direction !== "out") return null;
  const ref: GroupRef = { direction, relationship: value.slice(first + 1, second), type: value.slice(second + 1) || null };
  return read.groups.find((g) => sameGroup(g, ref)) ?? null;
}

export interface RecordSelection {
  /** The open group: its records listed, the others folded to headings. */
  group: NeighbourGroup | null;
  /** The selected neighbour and the group it was selected in. */
  sel: { id: string; group: NeighbourGroup } | null;
}

/** What the URL selects on this read. A neighbour listed in two groups is
 *  taken from the open group first, then from the first group listing it.
 *  A value the read does not have selects nothing; `recordChecks` names it. */
export function recordSelection(read: RecordNeighbourhoodRead, params: Record<string, string>): RecordSelection {
  const group = groupFromParam(read, params.group);
  let sel: RecordSelection["sel"] = null;
  if (params.sel) {
    const home = group && group.items.some((i) => i.id === params.sel)
      ? group
      : read.groups.find((g) => g.items.some((i) => i.id === params.sel)) ?? null;
    if (home) sel = { id: params.sel, group: home };
  }
  return { group, sel };
}

/** The record page's value checks for `graphIgnored`: a group or a selected
 *  record this read does not have is named, never dropped in silence. */
export function recordChecks(read: RecordNeighbourhoodRead): Record<string, ParamCheck> {
  return {
    group: (value) => (groupFromParam(read, value) ? null : "this record has no such group in this version"),
    sel: (value) => (read.groups.some((g) => g.items.some((i) => i.id === value)) ? null : "no group of this record lists that record"),
  };
}

/** The patch that opens `g` (null: closes the open group) and clears the
 *  selection. */
export function groupPatch(g: GroupRef | null): Record<string, string | null> {
  return { group: g ? groupParam(g) : null, sel: null };
}

/** The patch that selects `id` in group `g` (null: clears the selection).
 *  The open group stays open; with none open, the group is named only when
 *  the id alone would resolve to an earlier group that also lists it. */
export function selPatch(
  read: RecordNeighbourhoodRead, open: GroupRef | null, g: GroupRef, id: string | null,
): Record<string, string | null> {
  const first = id ? read.groups.find((x) => x.items.some((i) => i.id === id)) : undefined;
  const keep = open ?? (id && first && !sameGroup(first, g) ? g : null);
  return { group: keep ? groupParam(keep) : null, sel: id };
}

/** Explore's Related tab filtered to one relationship in one direction.
 *  Explore cannot filter by record type. */
export function relatedHref(link: LinkFn, centre: string, g: GroupRef): string {
  return link("explore", exploreQuery({
    record: centre, tab: "related", rel: g.relationship, dir: g.direction === "in" ? "incoming" : "outgoing",
  }));
}

/** A record's Explore page. */
export function exploreRecordHref(link: LinkFn, id: string): string {
  return link("explore", exploreQuery({ record: id }));
}

/** Explore's list of one record type, as Home and Updates link it. */
export function exploreTypeHref(link: LinkFn, type: string): string {
  return link("explore", exploreQuery({ type }));
}

/** The map with one record type selected: the breadcrumb's type link. */
export function mapTypeHref(link: LinkFn, type: string): string {
  return link("graph", { view: "map", type });
}
