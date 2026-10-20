// The Table view's rows: record types with their links in, out and within the
// type, and the sources that make them; sources with what they make; and one
// type's expanded detail -- declared fields with their sampled fill, and the
// relationships the ontology allows, used first. Ported from the approved
// prototype (v2.html, drawTable and detail).
import type { GraphModel, Pattern, ProducerRef } from "@operator-ui/console/model/graph/derive";
import type { DeclaredType, FieldFill, OntologyRead } from "@operator-ui/console/model/graph/reads";

/** How field fill was sampled, from the ontology read's `method` (the
 *  server samples `first_1000_by_index`); null when nothing was sampled. */
export function fillSampleSentence(ontology: OntologyRead | null): string | null {
  if (!ontology) return null;
  const methods = [...new Set(Object.values(ontology.fill).map((f) => f.method))].sort();
  if (!methods.length) return null;
  const words = methods.map((m) => {
    const first = /^first_(\d+)_by_index$/.exec(m);
    return first ? `the first ${Number(first[1]).toLocaleString("en-US")} records of each type` : `a method this page does not know (${m})`;
  });
  const excluded = ontology.fillExcluded.length ? "; fields the access policy decides on are not sampled" : "";
  return `Field fill is sampled from ${words.join(" and ")}, before access rules, and only for declared fields${excluded}.`;
}

/** Relationships into a type from OTHER types. */
export const linksIn = (patterns: Pattern[], t: string): number => patterns.filter((p) => p.d === t && p.s !== t).reduce((a, p) => a + p.n, 0);
/** Relationships from a type to OTHER types. */
export const linksOut = (patterns: Pattern[], t: string): number => patterns.filter((p) => p.s === t && p.d !== t).reduce((a, p) => a + p.n, 0);
/** Relationships from a type to itself (`code_symbol calls code_symbol`).
 *  They used to be dropped from both columns, so a type with 148,652
 *  relationships to itself read "Links out 0" beside a Grid diagonal that
 *  said 149k (audit 2026-09-27). */
export const linksWithin = (patterns: Pattern[], t: string): number => patterns.filter((p) => p.s === t && p.d === t).reduce((a, p) => a + p.n, 0);

/** Declared types that hold no records, by name. */
export function emptyTypes(model: GraphModel, ontology: OntologyRead): string[] {
  return ontology.types.filter((t) => !model.count[t.type]).map((t) => t.type).sort();
}

/**
 * Attribute keys the PLATFORM writes on records of every type, which no
 * deployment schema declares: the read policy's lineage and classification,
 * its redaction marker, and the untrusted-content stamps. The platform's own
 * list is `ENTITY_MERGE_RESERVED_AUDIT_ATTRS` in
 * src/okg/substrate/catalog/modules_compose.py (the governance keys of
 * nomos/read_masking.py plus transcript_trust.py's two); this mirrors it,
 * because the ontology read does not mark them. They are listed as "system",
 * not flagged: `nomos_source_lineage` was "not declared" in amber on all 25
 * types, a warning on every expanded row for a field no schema could declare.
 */
export const SYSTEM_ATTRIBUTES: ReadonlySet<string> = new Set([
  "classification",
  "nomos_source_lineage",
  "nomos_redacted",
  "untrusted_content_origin",
  "untrusted_content_tool_names",
]);

export interface FieldRow {
  name: string;
  required: boolean;
  range: string | null;
  /** Fill rate 0..1; null when the field was never seen in the sample. */
  fill: number | null;
  /** Declared, but left out of the sample because the access policy decides
   *  on it: printed "not sampled", never "never seen". */
  notSampled?: boolean;
  /** "declared" by the schema, "system" (written by the platform), or
   *  "undeclared": seen on records and declared nowhere, which is real
   *  schema drift and the one kind the page flags. */
  origin: "declared" | "system" | "undeclared";
}

export interface TypeDetail {
  fields: FieldRow[];
  /** The sample, when records were sampled. */
  sample: FieldFill | null;
  declared: DeclaredType | null;
  used: OntologyRead["rules"];
  unused: OntologyRead["rules"];
  rules: number;
}

export function typeDetail(t: string, ontology: OntologyRead, declared: Record<string, DeclaredType>): TypeDetail {
  const d = declared[t] ?? null;
  const f = ontology.fill[t] ?? null;
  const fa = f ? Object.fromEntries(f.attributes.map((a) => [a.name, a])) : {};
  const withheld = new Set(ontology.fillExcluded);
  const fields: FieldRow[] = (d ? d.attributes : []).map((a) => ({
    name: a.name, required: a.required, range: a.range, fill: fa[a.name]?.fillRate ?? null, origin: "declared" as const,
    ...(withheld.has(a.name) ? { notSampled: true } : {}),
  }));
  if (f) {
    const undeclared = f.attributes.filter((a) => !a.declared);
    for (const a of undeclared.filter((x) => !SYSTEM_ATTRIBUTES.has(x.name))) {
      fields.push({ name: a.name, required: false, range: null, fill: a.fillRate, origin: "undeclared" });
    }
    for (const a of undeclared.filter((x) => SYSTEM_ATTRIBUTES.has(x.name))) {
      fields.push({ name: a.name, required: false, range: null, fill: a.fillRate, origin: "system" });
    }
  }
  const rules = ontology.rules.filter((r) => r.from === t || r.to === t)
    .sort((a, b) => b.count - a.count || a.from.localeCompare(b.from) || a.relationship.localeCompare(b.relationship));
  return {
    fields,
    sample: f,
    declared: d,
    used: rules.filter((r) => r.count),
    unused: rules.filter((r) => !r.count),
    rules: rules.length,
  };
}

/** The never-used rules listed before "and N more". A source_file row
 *  listed 24 of them. */
export const UNUSED_RULES_SHOWN = 8;

/** Patterns an enricher made: the relationships it adds. */
export function enricherPatterns(model: GraphModel, producer: string): Pattern[] {
  return model.patterns.filter((x) => x.prod.some((y) => y[0] === producer));
}

/** The sources a type is made by, largest first. */
export function madeBy(model: GraphModel, t: string): [string, number][] {
  return model.byType[t] ?? [];
}

/** A declaring module as the page prints it: "git graph", or the
 *  deployment's own schema file ("deployment:okg-workspace") by name. */
export function moduleLabel(module: string | null | undefined): string {
  if (!module) return "";
  if (module.startsWith("deployment:")) return `${module.slice("deployment:".length)} schema`;
  return module.replace(/_/g, " ");
}

/** Record types a source makes, largest first. */
export function typesOf(model: GraphModel, source: string): { type: string; count: number }[] {
  return model.recordsBySource.filter((x) => x.source === source).sort((a, b) => b.count - a.count);
}

/** How many of the deployment's producers this version holds anything from,
 *  by the producers read's own kinds -- the totals Sources prints. */
export interface ProducerCoverage {
  /** Producers the producers read lists that made records in this version. */
  withRecords: number;
  /** Sources the producers read lists (Sources' "N sources"). */
  sources: number;
  /** Enrichers that added relationships in this version. */
  enrichersWithLinks: number;
  enrichers: number;
  /** Producers this version holds records or relationships from that the
   *  producers read does not list, by ownership id. */
  unlisted: string[];
}

export function producerCoverage(model: GraphModel, producers: readonly ProducerRef[]): ProducerCoverage {
  const listed = new Set(producers.map((p) => p.ownershipId));
  const active = [...model.sources, ...model.enrichers.map(([p]) => p)];
  // Counted against the producers read's own totals, so only producers it
  // lists count toward "N of M"; the rest are named by `unlisted`.
  return {
    withRecords: model.sources.filter((p) => listed.has(p)).length,
    sources: producers.filter((p) => p.kind === "source").length,
    enrichersWithLinks: model.enrichers.filter(([p, n]) => n > 0 && listed.has(p)).length,
    enrichers: producers.filter((p) => p.kind === "enricher").length,
    unlisted: active.filter((p) => !listed.has(p)),
  };
}

/** "9 of 35 sources made records in this version; 2 of 25 enrichers added
 *  relationships." With the producers read missing, only what this read
 *  counts is said, and the sentence says the totals are not known. */
export function coverageSentence(model: GraphModel, coverage: ProducerCoverage | null): string {
  const s = model.sources.length;
  const e = model.enrichers.filter(([, n]) => n > 0).length;
  if (!coverage) {
    return `${s} ${s === 1 ? "producer" : "producers"} made records in this version; `
      + `${e} ${e === 1 ? "enricher" : "enrichers"} added relationships.`;
  }
  return `${coverage.withRecords} of ${coverage.sources} sources made records in this version; `
    + `${coverage.enrichersWithLinks} of ${coverage.enrichers} enrichers added relationships.`;
}
