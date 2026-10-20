// SYNTHETIC Graph reads for the type map at scale: 85 and 140 record types.
//
// No deployment in reach holds this many populated types (okg-workspace has
// 25), so these reads are generated, deterministically, from a seeded
// random number generator. Every name, count and relationship below is made
// up; nothing here was read from a server. They exist to prove the map at the
// scale its design targets (fix spec, "Map: Scale"): one circle per producer
// above 40 types, colours past the palette's eight, the "No relationships"
// strip, loops for relationships to itself, and the layout's cost.
//
// They are two extra mock deployments, `graph-scale-85` and `graph-scale-140`,
// reachable by URL only (`#graph?fixture=graph-scale-140`): the switcher and
// the per-fixture tests stay on the six the redesign's PACT names. Every read
// other than the Graph ones and the producers read is workspace-mixed's, so
// the top bar and the other views still render.
import workspaceMixed from "@operator-ui-design-fixtures/workspace-mixed.json";
import type { DesignFixture } from "@operator-ui/design-harness/fixtureClient";

const WORDS = [
  "agent", "session", "ci", "job", "runner", "artifact", "claim", "decision", "finding", "metric", "bench", "host",
  "config", "issue", "review", "comment", "spec", "task", "commit", "branch", "file", "symbol", "chunk", "document",
  "mention", "package", "workflow", "check", "event", "message", "observation", "experiment", "dispatch", "brief",
  "capability", "scenario", "gate", "policy", "owner", "area",
];
const RELATIONSHIPS = ["contains", "references", "member_of", "affects", "derived_from", "mentions", "calls", "assigned_to", "depends_on"];

export interface SyntheticGraph {
  types: unknown;
  ontology: unknown;
  producers: unknown;
}

/** One synthetic deployment: `nTypes` types from `nSources` producers with
 *  `nPats` relationship kinds; the last three types have none. */
export function syntheticGraph(name: string, nTypes: number, nSources: number, nPats: number, seed = 7): SyntheticGraph {
  let s = seed;
  const rnd = () => { s = (s * 16807) % 2147483647; return s / 2147483647; };
  const pick = <T,>(xs: T[]) => xs[Math.floor(rnd() * xs.length)];
  const types: string[] = [];
  const used = new Set<string>();
  while (types.length < nTypes) {
    const t = `${pick(WORDS)}_${pick(WORDS)}${rnd() < 0.3 ? `_${pick(WORDS)}` : ""}`;
    if (!used.has(t)) { used.add(t); types.push(t); }
  }
  const sources = Array.from({ length: nSources }, (_, i) => `${name}.source-${String.fromCharCode(97 + i)}${rnd() < 0.5 ? "-history" : "-corpus"}`);
  const count: Record<string, number> = {};
  const src: Record<string, string> = {};
  for (const t of types) {
    count[t] = Math.max(1, Math.round(600000 * rnd() ** 4));
    src[t] = sources[Math.min(nSources - 1, Math.floor(rnd() ** 1.5 * nSources))];
  }
  const recordsBySource = types.map((t) => ({ source: src[t], type: t, count: count[t] }));
  const linked = types.slice(0, nTypes - 3);
  const patterns: { from: string; relationship: string; to: string; count: number; producers: { producer: string; count: number }[] }[] = [];
  const seen = new Set<string>();
  while (patterns.length < nPats) {
    const a = pick(linked);
    const same = rnd() < 0.6;
    const pool = same ? linked.filter((t) => src[t] === src[a]) : linked;
    const b = pick(pool);
    const e = pick(RELATIONSHIPS);
    const key = `${a}|${e}|${b}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const n = Math.max(1, Math.round(Math.min(count[a], count[b]) * rnd() * 2));
    patterns.push({ from: a, relationship: e, to: b, count: n, producers: [{ producer: src[a], count: n }] });
  }
  const srcRecords: Record<string, number> = {};
  const srcRels: Record<string, number> = {};
  for (const r of recordsBySource) srcRecords[r.source] = (srcRecords[r.source] ?? 0) + r.count;
  for (const p of patterns) srcRels[p.producers[0].producer] = (srcRels[p.producers[0].producer] ?? 0) + p.count;
  const generationId = `okg:${name}:branch:default:gen:20260927T000000000000Z:5ca1e0000${nTypes}`;
  const total = (xs: number[]) => xs.reduce((a, b) => a + b, 0);
  const typesRead = {
    generation_id: generationId,
    payload: {
      deployment: name,
      generation_id: generationId,
      count_basis: "published_total_before_access_rules",
      totals: { records: total(Object.values(count)), relationships: total(patterns.map((p) => p.count)) },
      types: types.map((t) => ({ type: t, count: count[t] })),
      relationship_types: RELATIONSHIPS.map((r) => ({ type: r, count: total(patterns.filter((p) => p.relationship === r).map((p) => p.count)) })),
      sources: sources.filter((x) => srcRecords[x]).map((x) => ({ source: x, kind: "source", records: srcRecords[x], relationships: srcRels[x] ?? 0 })),
      records_by_source: recordsBySource,
      patterns,
    },
  };
  const ontology = {
    generation_id: generationId,
    payload: {
      deployment: name,
      generation_id: generationId,
      catalog_version_id: 1,
      count_basis: "published_total_before_access_rules",
      attributes_basis: "deployment_schema_files_at_read_time",
      types: types.map((t) => ({ type: t, module: null, archetype: null, parent: null, count: count[t], declared_in_schema: true, attributes: [] })),
      rules: [],
      fill: {},
    },
  };
  const base = (workspaceMixed as unknown as { reads: Record<string, { payload: { producers: Record<string, unknown>[] } }> })
    .reads["/viz/api/console/producers"];
  const template = base.payload.producers[0];
  const producers = {
    ...base,
    generation_id: generationId,
    payload: {
      ...base.payload,
      deployment: name,
      generation_id: generationId,
      producer_count: sources.length,
      producers: sources.map((x) => ({ ...template, name: x.slice(name.length + 1), ownership_id: x })),
      count_disagreements: [],
    },
  };
  return { types: typesRead, ontology, producers };
}

function fixture(name: string, nTypes: number, nSources: number, nPats: number): DesignFixture {
  const g = syntheticGraph(name, nTypes, nSources, nPats);
  const mixed = workspaceMixed as unknown as DesignFixture;
  return {
    ...mixed,
    name,
    purpose: `SYNTHETIC: ${nTypes} record types from ${nSources} producers, for the type map at scale.`,
    stories: [],
    edge_cases: [],
    expect: {},
    reads: {
      ...mixed.reads,
      "/viz/api/console/graph/types": g.types,
      "/viz/api/console/graph/ontology": g.ontology,
      "/viz/api/console/producers": g.producers,
    },
  };
}

/** The two synthetic scale deployments, reachable by name only. */
export const GRAPH_SCALE_FIXTURES: DesignFixture[] = [
  fixture("graph-scale-85", 85, 12, 220),
  fixture("graph-scale-140", 140, 16, 400),
];
