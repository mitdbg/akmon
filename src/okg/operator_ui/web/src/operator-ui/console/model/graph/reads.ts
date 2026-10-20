// The four Graph reads, as this console receives them, and the strict parsers
// that turn each answer into the shape the view draws.
//
// Contract: the "Console Graph view: read contract" shared with the server
// worker (routes under /viz/api/console/graph/), with its "Changes": every
// answer is `{ generation_id, payload }`, as the record routes answer, and
// the shapes below are the payload's. A malformed answer is refused
// here with a SearchResponseError, which the read-failure surface names as
// "this console refused the server's answer" -- never filled in with a
// default, because an empty type map is indistinguishable from a deployment
// with no records.
import { SearchResponseError } from "@operator-ui/console/api/searchData";

export type SourceKind = "source" | "enricher" | "view";

export interface GraphTypesRead {
  deployment: string;
  generationId: string;
  countBasis: string;
  totals: { records: number; relationships: number };
  types: { type: string; count: number }[];
  relationshipTypes: { type: string; count: number }[];
  sources: { source: string; kind: SourceKind; records: number; relationships: number }[];
  recordsBySource: { source: string; type: string; count: number }[];
  patterns: {
    from: string;
    relationship: string;
    to: string;
    count: number;
    producers: { producer: string; count: number }[];
  }[];
}

export interface SourceLinksRead {
  deployment: string;
  generationId: string;
  countBasis: string;
  basis: string;
  computedMs: number | null;
  cached: boolean;
  /** A source is null for a record no source is recorded for. */
  rows: { fromSource: string | null; toSource: string | null; producer: string; count: number }[];
}

export interface NeighbourItem {
  id: string;
  label: string | null;
  producer: string;
}

export interface NeighbourGroup {
  direction: "in" | "out";
  relationship: string;
  /** Null for neighbours absent from this version (their labels are null). */
  type: string | null;
  count: number;
  countIsFloor: boolean;
  /** Which cap made the count a floor: the per-relationship count, the
   *  per-group access check, or the read's check budget. Null when the count
   *  is exact, or when an older server did not say. */
  floorReason: FloorReason | null;
  items: NeighbourItem[];
}

export type FloorReason = "count_cap" | "scan_cap" | "check_budget";

export interface RelationshipCount {
  direction: "in" | "out";
  relationship: string;
  count: number;
  countIsFloor: boolean;
  /** False when the relationship has more links than the server counts, so a
   *  type seen only past them is not named. Null when the server does not
   *  say (a read from before the field existed). */
  typesComplete: boolean | null;
}

export interface RecordNeighbourhoodRead {
  deployment: string;
  generationId: string;
  record: { id: string; type: string; label: string | null; source: string | null };
  /** `visible_to_reader` under an enforced policy: the counts are what the
   *  reader may see. Otherwise `published_total_before_access_rules`. */
  countBasis: string;
  perGroup: number;
  /** The cap the read's floors come from: links decided per group under a
   *  policy, links counted per relationship without one. */
  scanCap: number;
  /** Links counted per (direction, relationship); null when not sent. */
  countCap: number | null;
  /** Distinct neighbour RECORDS per direction and in all (one record linked
   *  two ways sits in two groups, so the groups do not add up to it); null
   *  when not sent.
   *
   *  `count_cap`, `types_complete` and `neighbours` come from the record
   *  read's speed rework (branch claude/console-graph-record-speed), not yet
   *  merged: each is optional, and strict when present -- a malformed value
   *  refuses the read like any other field. Make them required once that
   *  server change is on dev. */
  neighbours: Record<"in" | "out" | "all", { count: number; countIsFloor: boolean }> | null;
  capped: { in: boolean; out: boolean };
  /** Links one read sends through the access check; null without a policy. */
  checkBudget: number | null;
  /** Per side: some links were never checked because the budget ran out.
   *  No name and no count: those would come from before the access check. */
  uncheckedLinks: { in: boolean; out: boolean };
  /** Per side, the caps that made its counts floors (empty when exact),
   *  including groups the reader sees nothing of. */
  cappedBy: { in: FloorReason[]; out: FloorReason[] };
  /** Exact counts per (direction, relationship), from the edges alone. */
  relationships: RelationshipCount[];
  groups: NeighbourGroup[];
}

export interface DeclaredType {
  type: string;
  module: string | null;
  archetype: string | null;
  parent: string | null;
  count: number;
  declaredInSchema: boolean;
  attributes: { name: string; required: boolean; range: string | null }[];
}

export interface FieldFill {
  population: number;
  sampled: number;
  method: string;
  attributes: { name: string; fillRate: number; declared: boolean }[];
  declaredNeverSeen: string[];
}

export interface OntologyRead {
  deployment: string;
  generationId: string;
  catalogVersionId: string | null;
  countBasis: string;
  /** Where the declared attributes were read from. */
  attributesBasis: string;
  types: DeclaredType[];
  rules: { from: string; relationship: string; to: string; count: number }[];
  fill: Record<string, FieldFill>;
  /** Declared attributes the fill sample leaves out because the access
   *  policy decides on them (sent since the 2026-09-28 review; [] before). */
  fillExcluded: string[];
}

type Json = Record<string, unknown>;

function obj(value: unknown, what: string): Json {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new SearchResponseError(what);
  return value as Json;
}

function arr(value: unknown, what: string): unknown[] {
  if (!Array.isArray(value)) throw new SearchResponseError(what);
  return value;
}

function str(value: unknown, what: string): string {
  if (typeof value !== "string") throw new SearchResponseError(what);
  return value;
}

function optStr(value: unknown, what: string): string | null {
  return value === null || value === undefined ? null : str(value, what);
}

function count(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) throw new SearchResponseError(what);
  return value;
}

function bool(value: unknown, what: string): boolean {
  if (typeof value !== "boolean") throw new SearchResponseError(what);
  return value;
}

function rate(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 1) {
    throw new SearchResponseError(what);
  }
  return value;
}

/** `{ generation_id, payload }`: the version from the envelope, the rest
 *  from the payload. */
function envelope(raw: unknown, what: string): { payload: Json; head: { deployment: string; generationId: string } } {
  const body = obj(raw, what);
  const payload = obj(body.payload, what);
  return {
    payload,
    head: { deployment: str(payload.deployment, `${what} deployment`), generationId: str(body.generation_id, `${what} version`) },
  };
}

function floorReason(value: unknown): FloorReason | null {
  if (value === undefined || value === null) return null;
  if (value === "count_cap" || value === "scan_cap" || value === "check_budget") return value;
  throw new SearchResponseError("neighbourhood floor reason");
}

function direction(value: unknown, what: string): "in" | "out" {
  if (value !== "in" && value !== "out") throw new SearchResponseError(what);
  return value;
}

const KINDS: readonly SourceKind[] = ["source", "enricher", "view"];

export function parseGraphTypes(raw: unknown): GraphTypesRead {
  const { payload: body, head } = envelope(raw, "graph type map");
  const totals = obj(body.totals, "type map totals block");
  return {
    ...head,
    countBasis: str(body.count_basis, "graph count basis"),
    totals: {
      records: count(totals.records, "graph record total"),
      relationships: count(totals.relationships, "graph relationship total"),
    },
    types: arr(body.types, "type list").map((row) => {
      const r = obj(row, "graph type");
      return { type: str(r.type, "graph type name"), count: count(r.count, "graph type count") };
    }),
    relationshipTypes: arr(body.relationship_types, "relationship type list").map((row) => {
      const r = obj(row, "relationship type");
      return { type: str(r.type, "relationship type name"), count: count(r.count, "relationship type count") };
    }),
    sources: arr(body.sources, "source list").map((row) => {
      const r = obj(row, "graph source");
      const kind = str(r.kind, "graph source kind");
      if (!(KINDS as readonly string[]).includes(kind)) throw new SearchResponseError(`graph source kind ${JSON.stringify(kind)}`);
      return {
        source: str(r.source, "graph source name"),
        kind: kind as SourceKind,
        records: count(r.records, "graph source records"),
        relationships: count(r.relationships, "graph source relationships"),
      };
    }),
    recordsBySource: arr(body.records_by_source, "records-by-source list").map((row) => {
      const r = obj(row, "records by source row");
      return {
        source: str(r.source, "records by source source"),
        type: str(r.type, "records by source type"),
        count: count(r.count, "records by source count"),
      };
    }),
    patterns: arr(body.patterns, "relationship pattern list").map((row) => {
      const r = obj(row, "relationship pattern");
      return {
        from: str(r.from, "pattern from"),
        relationship: str(r.relationship, "pattern relationship"),
        to: str(r.to, "pattern to"),
        count: count(r.count, "pattern count"),
        producers: arr(r.producers, "pattern producer list").map((p) => {
          const q = obj(p, "pattern producer");
          return { producer: str(q.producer, "pattern producer name"), count: count(q.count, "pattern producer count") };
        }),
      };
    }),
  };
}

export function parseSourceLinks(raw: unknown): SourceLinksRead {
  const { payload: body, head } = envelope(raw, "source links");
  return {
    ...head,
    countBasis: str(body.count_basis, "source links count basis"),
    basis: str(body.basis, "source links basis"),
    computedMs: body.computed_ms === null || body.computed_ms === undefined
      ? null : count(body.computed_ms, "source links time"),
    cached: bool(body.cached, "source links cached flag"),
    rows: arr(body.rows, "source-links row list").map((row) => {
      const r = obj(row, "source link");
      return {
        fromSource: optStr(r.from_source, "source link from"),
        toSource: optStr(r.to_source, "source link to"),
        producer: str(r.producer, "source link producer"),
        count: count(r.count, "source link count"),
      };
    }),
  };
}

export function parseRecordNeighbourhood(raw: unknown): RecordNeighbourhoodRead {
  const { payload: body, head } = envelope(raw, "record neighbourhood");
  const record = obj(body.record, "neighbourhood record");
  const capped = obj(body.capped, "neighbourhood cap-flag block");
  return {
    ...head,
    record: {
      id: str(record.id, "neighbourhood record id"),
      type: str(record.type, "neighbourhood record type"),
      label: optStr(record.label, "neighbourhood record label"),
      source: optStr(record.source, "neighbourhood record source"),
    },
    countBasis: str(body.count_basis, "neighbourhood count basis"),
    perGroup: count(body.per_group, "neighbourhood group size"),
    scanCap: count(body.scan_cap, "neighbourhood scan cap"),
    countCap: body.count_cap === undefined ? null : count(body.count_cap, "neighbourhood count cap"),
    neighbours: body.neighbours === undefined ? null : (() => {
      const n = obj(body.neighbours, "neighbourhood distinct-neighbour block");
      const side = (key: "in" | "out" | "all") => {
        const x = obj(n[key], `distinct neighbours (${key})`);
        return { count: count(x.count, `distinct neighbour count (${key})`), countIsFloor: bool(x.count_is_floor, `distinct neighbour floor flag (${key})`) };
      };
      return { in: side("in"), out: side("out"), all: side("all") };
    })(),
    capped: { in: bool(capped.in, "neighbourhood in cap flag"), out: bool(capped.out, "neighbourhood out cap flag") },
    checkBudget: body.check_budget === undefined || body.check_budget === null ? null : count(body.check_budget, "neighbourhood check budget"),
    uncheckedLinks: body.unchecked_links === undefined ? { in: false, out: false } : (() => {
      const u = obj(body.unchecked_links, "unchecked-links block");
      return { in: bool(u.in, "unchecked links (in)"), out: bool(u.out, "unchecked links (out)") };
    })(),
    cappedBy: body.capped_by === undefined ? { in: [], out: [] } : (() => {
      const c = obj(body.capped_by, "capped-by block");
      const side = (key: "in" | "out") => arr(c[key], `capped-by list (${key})`).map((v) => {
        const r = floorReason(v);
        if (r === null) throw new SearchResponseError(`capped-by reason (${key})`);
        return r;
      });
      return { in: side("in"), out: side("out") };
    })(),
    relationships: arr(body.relationships, "neighbourhood relationship count list").map((row) => {
      const r = obj(row, "neighbourhood relationship count");
      return {
        direction: direction(r.direction, "relationship count direction"),
        relationship: str(r.relationship, "relationship count name"),
        count: count(r.count, "relationship count"),
        countIsFloor: bool(r.count_is_floor, "relationship count floor flag"),
        typesComplete: r.types_complete === undefined ? null : bool(r.types_complete, "relationship types-complete flag"),
      };
    }),
    groups: arr(body.groups, "neighbourhood group list").map((row) => {
      const g = obj(row, "neighbourhood group");
      return {
        direction: direction(g.direction, "neighbourhood direction"),
        relationship: str(g.relationship, "neighbourhood relationship"),
        type: optStr(g.type, "neighbourhood type"),
        count: count(g.count, "neighbourhood count"),
        countIsFloor: bool(g.count_is_floor, "neighbourhood floor flag"),
        floorReason: floorReason(g.floor_reason),
        items: arr(g.items, "neighbour list").map((item) => {
          const i = obj(item, "neighbour");
          return {
            id: str(i.id, "neighbour id"),
            label: optStr(i.label, "neighbour label"),
            producer: str(i.producer, "neighbour producer"),
          };
        }),
      };
    }),
  };
}

export function parseOntology(raw: unknown): OntologyRead {
  const { payload: body, head } = envelope(raw, "ontology");
  const fill: Record<string, FieldFill> = {};
  for (const [type, value] of Object.entries(obj(body.fill, "field-fill block"))) {
    const f = obj(value, "field fill row");
    fill[type] = {
      population: count(f.population, "field fill population"),
      sampled: count(f.sampled, "field fill sample"),
      method: str(f.method, "field fill method"),
      attributes: arr(f.attributes, "field-fill attribute list").map((a) => {
        const x = obj(a, "field fill attribute");
        return {
          name: str(x.name, "field fill name"),
          fillRate: rate(x.fill_rate, "field fill rate"),
          declared: bool(x.declared, "field fill declared flag"),
        };
      }),
      declaredNeverSeen: arr(f.declared_never_seen, "never-seen field list").map((n) => str(n, "declared never seen name")),
    };
  }
  return {
    ...head,
    // The server sends the catalog version as the integer it stores (as the
    // Updates read does); a string is accepted for older fixtures.
    catalogVersionId: typeof body.catalog_version_id === "number" && Number.isInteger(body.catalog_version_id)
      ? String(body.catalog_version_id)
      : optStr(body.catalog_version_id, "catalog version"),
    countBasis: str(body.count_basis, "ontology count basis"),
    attributesBasis: str(body.attributes_basis, "ontology attributes basis"),
    types: arr(body.types, "declared type list").map((row) => {
      const t = obj(row, "declared type");
      return {
        type: str(t.type, "declared type name"),
        module: optStr(t.module, "declared type module"),
        archetype: optStr(t.archetype, "declared type archetype"),
        parent: optStr(t.parent, "declared type parent"),
        count: count(t.count, "declared type count"),
        declaredInSchema: bool(t.declared_in_schema, "declared-in-schema flag"),
        attributes: arr(t.attributes, "declared attribute list").map((a) => {
          const x = obj(a, "declared attribute");
          return {
            name: str(x.name, "declared attribute name"),
            required: bool(x.required, "declared attribute required flag"),
            range: optStr(x.range, "declared attribute range"),
          };
        }),
      };
    }),
    rules: arr(body.rules, "ontology rule list").map((row) => {
      const r = obj(row, "ontology rule");
      return {
        from: str(r.from, "rule from"),
        relationship: str(r.relationship, "rule relationship"),
        to: str(r.to, "rule to"),
        count: count(r.count, "rule count"),
      };
    }),
    fill,
    fillExcluded: body.fill_excluded_attributes === undefined
      ? []
      : arr(body.fill_excluded_attributes, "fill-excluded attribute list").map((n) => str(n, "fill-excluded attribute")),
  };
}
