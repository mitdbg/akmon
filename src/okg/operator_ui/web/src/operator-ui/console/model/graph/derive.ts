// What every Graph surface derives from the type-map read: the types by
// count, the sources by the records they make and the colour each is drawn
// in, which source makes which type, and the relationship patterns.
//
// Ported from the approved prototype's data block (v2.html, "data"). The
// prototype hard-coded okg-workspace's source names in two places; both are
// derived from the read here, so the same view draws any deployment.
//
// A producer's NAME is the console's, not this view's: the producers read
// (`console/producers`, what Sources draws) maps each ownership id to the
// name Sources prints and links. This view used to make names up from the id
// ("alias match" for `_enricher:declarative_alias:...`, "native views"),
// which Sources never called them. An id the producers read does not list --
// or every id, while that read is pending or failed -- is printed as it is.
import type { ProducerKind } from "@operator-ui/console/model/shared/reads";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { GraphTypesRead, OntologyRead, SourceLinksRead } from "@operator-ui/console/model/graph/reads";

export const fmt = (n: number): string => Number(n).toLocaleString("en-US");

/** 822142 -> "822k"; 1571775 -> "1.6M"; 5091 -> "5.1k". */
export const short = (n: number): string => (n >= 1e6
  ? `${(n / 1e6).toFixed(1).replace(/\.0$/, "")}M`
  : n >= 1e4
    ? `${Math.round(n / 1e3)}k`
    : n >= 1e3
      ? `${(n / 1e3).toFixed(1).replace(/\.0$/, "")}k`
      : String(n));

export const plural = (n: number, word: string, many?: string): string => `${fmt(n)} ${n === 1 ? word : (many ?? `${word}s`)}`;

/** One row of the producers read, as this view needs it. */
export interface ProducerRef {
  name: string;
  ownershipId: string;
  kind: ProducerKind;
}

/** What the page prints for one producer, and where it links. */
export interface ProducerLabel {
  /** The producer's name as Sources prints it, or the ownership id when the
   *  producers read does not list it. */
  name: string;
  /** "enricher" and "view" are printed after the name where a reader could
   *  otherwise take it for a source ("code_reference" is also a type). */
  kind: ProducerKind | null;
  /** The producer's Sources page; null when the producers read does not
   *  list it, so no link promises a page that is not there. */
  href: string | null;
}

export type ProducerNamer = (ownershipId: string) => ProducerLabel;

/** Names from the producers read, by ownership id. `producers` null (the
 *  read pending or failed) names every producer by its id, unlinked. */
export function producerNamer(producers: readonly ProducerRef[] | null, link: LinkFn): ProducerNamer {
  const byId = new Map((producers ?? []).map((p) => [p.ownershipId, p]));
  return (ownershipId) => {
    const found = byId.get(ownershipId);
    return found
      ? { name: found.name, kind: found.kind, href: link(`sources/${encodeURIComponent(found.name)}`) }
      : { name: ownershipId, kind: null, href: null };
  };
}

/** "code_reference enricher": the name, and the kind where it is not a
 *  source. For text (titles, copy text); the page draws the same words. */
export function producerText(label: ProducerLabel): string {
  return label.kind === "enricher" || label.kind === "view" ? `${label.name} ${label.kind}` : label.name;
}

/** The nine categorical colours, as tokens (styles/tokens.css). The first
 *  eight name one producer each; the ninth, a neutral, is shared by every
 *  producer past the eighth and by records no producer is recorded for. */
export const CATEGORICAL = 9;
export const cat = (i: number): string => `var(--okg-graph-cat-${Math.min(Math.max(i, 1), CATEGORICAL)})`;

export interface Pattern {
  s: string;
  e: string;
  d: string;
  n: number;
  prod: [string, number][];
}

export interface GraphModel {
  deployment: string;
  generationId: string;
  /** What the read's counts count (`count_basis`); `countBasisSentence`
   *  says it in words wherever a count is printed or copied. */
  countBasis: string;
  totals: { records: number; relationships: number };
  count: Record<string, number>;
  /** Types that hold records, largest first. */
  types: string[];
  srcTotal: Record<string, number>;
  /** Sources that make records, largest first. */
  sources: string[];
  srcColor: Record<string, string>;
  /** The sources past the eighth, which share the neutral colour; the map's
   *  legend groups them as "N smaller producers". */
  smallSources: string[];
  /** type -> [source, records] largest first */
  byType: Record<string, [string, number][]>;
  /** [producer, relationships] for every enricher. */
  enrichers: [string, number][];
  /** producer -> relationships it made */
  edgeBy: Record<string, number>;
  patterns: Pattern[];
  recordsBySource: GraphTypesRead["recordsBySource"];
  /** The producer's printed name (see `producerNamer`). */
  name: (producer: string) => string;
  /** The producer's name, kind and Sources link. */
  label: ProducerNamer;
  typeSource: (type: string) => string;
  typeColor: (type: string) => string;
}

/** The type map read is the model; the producers read only names what it
 *  holds. Omitted, every producer is named by its ownership id. */
export function buildGraphModel(read: GraphTypesRead, namer: ProducerNamer = (id) => ({ name: id, kind: null, href: null })): GraphModel {
  const count: Record<string, number> = {};
  for (const row of read.types) count[row.type] = row.count;
  const types = read.types.slice().sort((a, b) => b.count - a.count).map((row) => row.type);
  const srcTotal: Record<string, number> = {};
  for (const row of read.recordsBySource) srcTotal[row.source] = (srcTotal[row.source] ?? 0) + row.count;
  const sources = Object.keys(srcTotal).sort((a, b) => srcTotal[b] - srcTotal[a] || a.localeCompare(b));
  const srcColor: Record<string, string> = Object.fromEntries(sources.map((s, i) => [s, cat(Math.min(i + 1, CATEGORICAL))]));
  const smallSources = sources.slice(CATEGORICAL - 1);
  const byType: Record<string, [string, number][]> = {};
  for (const row of read.recordsBySource) (byType[row.type] ??= []).push([row.source, row.count]);
  for (const list of Object.values(byType)) list.sort((x, y) => y[1] - x[1]);
  const enrichers = read.sources.filter((s) => s.kind === "enricher").map((s) => [s.source, s.relationships] as [string, number]);
  const edgeBy: Record<string, number> = {};
  for (const s of read.sources) if (s.relationships) edgeBy[s.source] = s.relationships;
  const patterns = read.patterns.map((p) => ({
    s: p.from, e: p.relationship, d: p.to, n: p.count,
    prod: p.producers.map((q) => [q.producer, q.count] as [string, number]),
  })).sort((a, b) => b.n - a.n);
  const typeSource = (t: string) => (byType[t] ?? [["?", 0]])[0][0];
  return {
    deployment: read.deployment,
    generationId: read.generationId,
    countBasis: read.countBasis,
    totals: read.totals,
    count,
    types,
    srcTotal,
    sources,
    srcColor,
    smallSources,
    byType,
    enrichers,
    edgeBy,
    patterns,
    recordsBySource: read.recordsBySource,
    name: (p) => namer(p).name,
    label: namer,
    typeSource,
    typeColor: (t) => srcColor[typeSource(t)] ?? cat(CATEGORICAL),
  };
}

/** The row and column for records no source is recorded for. */
export const NO_SOURCE = "no source recorded";

/** Source x source: relationships between the records each source made. */
export interface SourceLinkModel {
  cells: Record<string, { n: number; prod: [string, number][] }>;
  sources: string[];
  /** The source-links read's own count basis. */
  countBasis: string;
}

export function buildSourceLinks(read: SourceLinksRead, model: GraphModel): SourceLinkModel {
  const cells: SourceLinkModel["cells"] = {};
  for (const row of read.rows) {
    const key = `${row.fromSource ?? NO_SOURCE}|${row.toSource ?? NO_SOURCE}`;
    const cell = (cells[key] ??= { n: 0, prod: [] });
    cell.n += row.count;
    cell.prod.push([row.producer, row.count]);
  }
  const sources = [...new Set(read.rows.flatMap((r) => [r.fromSource ?? NO_SOURCE, r.toSource ?? NO_SOURCE]))]
    .sort((x, y) => (model.srcTotal[y] ?? 0) - (model.srcTotal[x] ?? 0));
  return { cells, sources, countBasis: read.countBasis };
}

/** "Counts are totals for version …: N records, M relationships." and the
 *  read's basis: the Table's and the Grid's footer, and their copy text. */
export function versionCountsSentence(model: GraphModel, versionText: (id: string) => string = (id) => id): string {
  return `Counts are totals for version ${versionText(model.generationId.slice(-12))}: `
    + `${fmt(model.totals.records)} records, ${fmt(model.totals.relationships)} relationships. ${countBasisSentence(model.countBasis)}`;
}

/** A read's count basis in words. Every Graph count -- the rail, the Table,
 *  the Grid, the copy text -- says its basis through this one sentence; an
 *  unknown basis is named, not hidden. */
export function countBasisSentence(basis: string): string {
  if (basis === "published_total_before_access_rules") return "Counted when the version was published, before access rules.";
  if (basis === "visible_to_reader") return "Counted as the access policy lets this console see them.";
  return `Counted on a basis this page does not know: ${basis}.`;
}

/** The declared type, by name. */
export function declaredByName(ontology: OntologyRead | null): Record<string, OntologyRead["types"][number]> {
  return ontology ? Object.fromEntries(ontology.types.map((t) => [t.type, t])) : {};
}

/** Where cluster i of n sits on the type map's ring (unit radius; the map
 *  scales it). The ring is an ellipse, wider than tall, starting at the top. */
export function ringPoint(i: number, n: number): [number, number] {
  const a = -Math.PI / 2 + (2 * Math.PI * i) / n;
  return [Math.cos(a) * 1.2, Math.sin(a) * 0.68];
}

/**
 * The order the type map's source clusters sit on their ring, clockwise from
 * the top: by the records each source makes, largest first (ties by name).
 *
 * The prototype listed okg-workspace's sources by hand. Three general rules
 * were measured against that hand order on okg-workspace's reads, counting
 * the type labels that fit at rest and the relationship names that fit with
 * `source_file` picked, on 1440, 1280 and 390 stages: shortest total
 * cross-cluster line (23/20/4 labels, 6/1/1 names), chaining each cluster to
 * its most-linked neighbour (23/23/8, 2/1/1), and largest first (23/22/5,
 * 7/7/1). Largest first matched or beat the hand order (23/22/5, 7/6/1) on
 * every count, and is the simplest.
 */
export function clusterOrder(clusters: string[], model: Pick<GraphModel, "srcTotal">): string[] {
  return clusters.slice().sort((x, y) => (model.srcTotal[y] ?? 0) - (model.srcTotal[x] ?? 0) || x.localeCompare(y));
}
