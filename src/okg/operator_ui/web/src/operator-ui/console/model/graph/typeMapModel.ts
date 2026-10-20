// What the type map decides before it draws: the legend's entries, what the
// URL asks for, and the selected type as text. Producer names are the
// model's (`model.name`, `model.label`: the console's producers read, by
// ownership id; derive.ts), never this module's.
//
// Pure, so every rule is unit-tested; the drawing (views/graph/typeMapDraw.ts)
// and the side panel (views/graph/TypeMapView.tsx) only render it.
//
// URL (the hash is the state, so Back and a shared link restore the view):
//   type=<record type>             the selected type
//   hide=<ownership id>[,...]      producers switched off in the legend
//   expand=<ownership id>[,...]    producers opened into their types, on a
//                                  map drawn one circle per producer
// The page reads and writes them through model/graph/params.ts: `mapChecks`
// feeds the page's one ignored-parameter line, and `mapPatch` is what the
// map hands `setParams` (a selection or toggle replaces the history entry).
import { type GraphModel, CATEGORICAL, type Pattern, type ProducerLabel, cat, fmt } from "@operator-ui/console/model/graph/derive";
import type { ParamCheck } from "@operator-ui/console/model/graph/params";

/** Above this many record types the map opens with one circle per producer. */
export const COLLAPSE_ABOVE = 40;

/** "the code_reference enricher", "git-history": a producer as a sentence
 *  names it, from the model's label. */
export function producerPhrase(label: ProducerLabel): string {
  return label.kind === "enricher" ? `the ${label.name} enricher` : label.kind === "view" ? `the ${label.name} view` : label.name;
}

// --- the legend ----------------------------------------------------------------

/** The producers that colour at least one type (they make most of its
 *  records), largest first. Only these can be switched off: a producer that
 *  colours nothing would be a toggle that changes nothing. */
export function mapOwners(model: GraphModel): string[] {
  const owning = new Set(model.types.map((t) => model.typeSource(t)));
  return model.sources.filter((s) => owning.has(s));
}

export interface LegendEntry {
  /** One producer's ownership id, or "smaller" for the grouped tail. */
  key: string;
  members: string[];
  color: string;
  records: number;
}

/** One entry per owning producer; past the palette's eight colours the
 *  smaller producers share the neutral ninth, and are one entry. */
export function legendEntries(model: GraphModel): LegendEntry[] {
  const neutral = cat(CATEGORICAL);
  const owners = mapOwners(model);
  const shared = owners.filter((s) => model.srcColor[s] === neutral);
  const out: LegendEntry[] = [];
  for (const s of owners) {
    if (shared.length > 1 && model.srcColor[s] === neutral) continue;
    out.push({ key: s, members: [s], color: model.srcColor[s], records: model.srcTotal[s] ?? 0 });
  }
  if (shared.length > 1) {
    out.push({
      key: "smaller", members: shared, color: neutral,
      records: shared.reduce((sum, s) => sum + (model.srcTotal[s] ?? 0), 0),
    });
  }
  return out;
}

// --- the URL -------------------------------------------------------------------

export interface MapState {
  type: string | null;
  hide: ReadonlySet<string>;
  /** Null when every type is drawn (40 types or fewer). */
  expand: ReadonlySet<string> | null;
}

const list = (raw: string | undefined): string[] => (raw ?? "").split(",").map((s) => s.trim()).filter(Boolean);

export function collapsible(model: GraphModel): boolean {
  return model.types.length > COLLAPSE_ABOVE;
}

/** What the URL selects on the map. A value it cannot honour selects
 *  nothing; `mapChecks` names it on the page's ignored-parameter line. */
export function readMapState(params: Record<string, string>, model: GraphModel): MapState {
  const owners = new Set(mapOwners(model));
  const asked = (params.type ?? "").trim();
  const type = asked && model.count[asked] !== undefined ? asked : null;
  const hide = new Set(list(params.hide).filter((id) => owners.has(id)));
  let expand: Set<string> | null = null;
  if (collapsible(model)) {
    expand = new Set(list(params.expand).filter((id) => owners.has(id)));
    // A selected type is always drawn: its producer is open.
    if (type) expand.add(model.typeSource(type));
  }
  return { type, hide, expand };
}

/** The map's value checks for `graphIgnored` (params.ts): each names what
 *  the map left out and why. */
export function mapChecks(model: GraphModel): Record<string, ParamCheck> {
  const owners = new Set(mapOwners(model));
  const unknown = (value: string) => {
    const bad = list(value).filter((id) => !owners.has(id));
    return bad.length ? `no producer by the ownership id ${bad.join(", ")} colours a type here` : null;
  };
  return {
    type: (value) => (model.count[value.trim()] !== undefined ? null : "no record type by that name holds records in this version"),
    hide: unknown,
    expand: (value) => (collapsible(model) ? unknown(value) : `every type is drawn when there are ${COLLAPSE_ABOVE} or fewer`),
  };
}

/** The `setParams` patch for a change of map state; a key left out of
 *  `next` keeps its value, an empty set removes it. */
export function mapPatch(
  next: { type?: string | null; hide?: Iterable<string>; expand?: Iterable<string> | null },
): Record<string, string | null> {
  const out: Record<string, string | null> = {};
  if ("type" in next) out.type = next.type ?? null;
  if (next.hide !== undefined) out.hide = [...next.hide].join(",") || null;
  if (next.expand !== undefined) out.expand = next.expand ? [...next.expand].join(",") || null : null;
  return out;
}

// --- the selected type, as text ------------------------------------------------

export interface TypeRelations {
  out: Pattern[];
  self: Pattern[];
  inn: Pattern[];
}

export function typeRelations(model: GraphModel, type: string): TypeRelations {
  return {
    out: model.patterns.filter((p) => p.s === type && p.d !== type),
    self: model.patterns.filter((p) => p.s === type && p.d === type),
    inn: model.patterns.filter((p) => p.d === type && p.s !== type),
  };
}

/** The producers of a relationship worth naming: those that make neither
 *  end's records. */
export function otherProducers(model: GraphModel, p: Pattern): string[] {
  const own = new Set([...(model.byType[p.s] ?? []), ...(model.byType[p.d] ?? [])].map((x) => x[0]));
  return p.prod.map((x) => x[0]).filter((x) => !own.has(x));
}

/** The selected type's panel as Markdown, for the top of "Copy as text". */
export function typeSelectionText(model: GraphModel, type: string, hidden: boolean): string[] {
  const rel = typeRelations(model, type);
  const row = (p: Pattern, other: string) => {
    const by = otherProducers(model, p);
    return `| ${p.e} | ${other ? `\`${other}\`` : "itself"} | ${fmt(p.n)} |${by.length ? ` by ${by.map((x) => producerPhrase(model.label(x))).join(", ")}` : ""}`;
  };
  const table = (title: string, ps: Pattern[], other: (p: Pattern) => string) => (ps.length ? [
    "", `### ${title} (${ps.length})`, "", "| Relationship | Type | Count |", "| --- | --- | ---: |",
    ...ps.map((p) => row(p, other(p))),
  ] : []);
  return [
    `## Selected type \`${type}\``,
    "",
    `${fmt(model.count[type] ?? 0)} records, made by ${(model.byType[type] ?? [])
      .map(([s, n]) => `${model.name(s)} ${fmt(n)}`).join(", ")}.${hidden ? " Hidden on the map: its producer is switched off." : ""}`,
    ...table("Links to", rel.out, (p) => p.d),
    ...table("Links to itself", rel.self, () => ""),
    ...table("Linked from", rel.inn, (p) => p.s),
  ];
}

// --- type-ahead ----------------------------------------------------------------

/** The type a typed prefix names: the largest whose name starts with it,
 *  else the largest that contains it. */
export function typeAhead(types: readonly string[], typed: string): string | null {
  const q = typed.toLowerCase();
  if (!q) return null;
  return types.find((t) => t.toLowerCase().startsWith(q)) ?? types.find((t) => t.toLowerCase().includes(q)) ?? null;
}
