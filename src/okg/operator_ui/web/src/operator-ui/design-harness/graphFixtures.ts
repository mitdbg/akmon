// The Graph view's reads for the design harness.
//
// One real data set -- the approved prototype's, read from okg-workspace and
// converted to the read contract (scripts/operator-ui-design/fixtures/graph/)
// -- served under four of the six mock deployments, each for a different
// state:
//
//   workspace-mixed  every read answers: the map, the Table, the Grid, the ten
//                    sample neighbourhoods, "Records to open" and the search,
//                    plus three COMPOSED records (composedRecords, below) for
//                    states okg-workspace's samples never reach.
//   scale-200        the map and the Table answer; the Grid's source-links
//                    read times out (503), and one record id is not found.
//   broken-runtime   the type map and ontology reads are unavailable (503);
//                    a record read is not found (404).
//   empty            nothing is published, so every Graph read refuses.
//
// archiv3-small and all-clear define no Graph read: the harness then says so
// by name (FixtureReadMissing), as it does for any read a fixture lacks.
//
// A failure is written as `{ "$fixture_failure": { status, body } }`, which
// the fixture client throws as the ApiRequestError the live client would.
import graphData from "@operator-ui-design-fixtures/graph/okg-workspace.json";

export const FIXTURE_FAILURE = "$fixture_failure";

export interface FixtureFailure { [FIXTURE_FAILURE]: { status: number; body: unknown } }

export const failure = (status: number, detail: Record<string, unknown>): FixtureFailure => ({
  [FIXTURE_FAILURE]: { status, body: { detail } },
});

interface Summary { node_id: string; type: string; label: string; source: string | null }

const data = graphData as unknown as {
  types: { generation_id: string };
  source_links: unknown;
  ontology: unknown;
  records: Record<string, unknown>;
  summaries: Summary[];
};

/** The generation the converted reads answer for. */
export const GRAPH_FIXTURE_GENERATION = data.types.generation_id;

/** A record id no fixture holds, for the not-found state. */
export const MISSING_RECORD_ID = "file:okg:src/okg/no/such/record.py";

/** The sample record with the most neighbours. */
export const HUB_RECORD_ID = "file:okg:src/okg/substrate/deployment_upgrade.py";

/**
 * The hub record as an enforced policy would answer it: counts are what the
 * reader may see, the scan stops at 500 per relationship, and the biggest
 * outgoing relationship is a floor. Used under scale-200 for the "+" state.
 */
function underPolicy(read: unknown): unknown {
  const env = read as { generation_id: string; payload: Record<string, unknown> };
  const p = env.payload as { groups: { direction: string; count: number; count_is_floor: boolean }[]; relationships: { direction: string; count: number; count_is_floor: boolean }[] };
  const biggest = (rows: { direction: string; count: number }[]) => rows
    .filter((r) => r.direction === "out").reduce((m, r) => (r.count > m.count ? r : m));
  const g = biggest(p.groups);
  const r = biggest(p.relationships);
  return {
    ...env,
    payload: {
      ...env.payload,
      count_basis: "visible_to_reader",
      scan_cap: 500,
      capped: { in: false, out: true },
      groups: p.groups.map((x) => (x === g ? { ...x, count_is_floor: true } : x)),
      relationships: p.relationships.map((x) => (x === r ? { ...x, count_is_floor: true } : x)),
    },
  };
}

/**
 * Three record reads composed for the harness, in the read's current shape
 * (per-relationship `types_complete`, `count_cap`). Their ids say they are
 * the harness's; their neighbours are real okg-workspace summaries.
 *
 *   MANY_GROUPS_RECORD_ID  twelve groups linked FROM it, so the tree folds the
 *                          smallest into "N more kinds"; one count is a floor.
 *   ABSENT_RECORD_ID       node_detail.py's neighbourhood plus a group whose
 *                          neighbours are absent from the version (type null,
 *                          labels null), as a read without a policy returns.
 *   EMPTY_RECORD_ID        a record with no links at all.
 */
export const MANY_GROUPS_RECORD_ID = "pact-change:harness-many-groups";
export const ABSENT_RECORD_ID = "file:okg:harness/absent_neighbour.py";
export const EMPTY_RECORD_ID = "view:harness.no_links";

export function composedRecords(): Record<string, unknown> {
  const base = data.records["file:okg:src/okg/substrate/console_views/node_detail.py"] as { generation_id: string; payload: Record<string, unknown> };
  const byType = (type: string) => data.summaries.filter((s) => s.type === type);
  const item = (s: Summary) => ({ id: s.node_id, label: s.label, producer: s.source ?? "okg-workspace.pact" });
  type G = { direction: "in" | "out"; relationship: string; type: string | null; count: number; count_is_floor: boolean; items: unknown[] };
  const group = (direction: "in" | "out", relationship: string, type: string, count: number, floor = false): G => ({
    direction, relationship, type, count, count_is_floor: floor, items: byType(type).slice(0, Math.min(25, count)).map(item),
  });
  const relationshipsOf = (groups: G[]) => {
    const by = new Map<string, { direction: string; relationship: string; count: number; count_is_floor: boolean; types_complete: boolean }>();
    for (const g of groups) {
      const k = `${g.direction}|${g.relationship}`;
      const r = by.get(k) ?? { direction: g.direction, relationship: g.relationship, count: 0, count_is_floor: false, types_complete: true };
      r.count += g.count;
      r.count_is_floor = r.count_is_floor || g.count_is_floor;
      by.set(k, r);
    }
    return [...by.values()];
  };
  const envelope = (record: Record<string, unknown>, groups: G[]) => ({
    generation_id: base.generation_id,
    payload: {
      deployment: "okg-workspace", generation_id: base.generation_id, record,
      count_basis: "published_total_before_access_rules", per_group: 25, scan_cap: 100000, count_cap: 100000,
      capped: { in: groups.some((g) => g.direction === "in" && g.count_is_floor), out: groups.some((g) => g.direction === "out" && g.count_is_floor) },
      relationships: relationshipsOf(groups), groups,
    },
  });
  const many: G[] = [
    group("in", "implements", "git_commit", 83),
    group("in", "mentions", "document_chunk", 4),
    group("in", "contains", "document", 1),
    group("out", "mentions", "code_reference", 100000, true),
    group("out", "mentions", "code_symbol", 108),
    group("out", "affects", "source_file", 27),
    group("out", "mentions", "code_import", 57),
    group("out", "mentions", "git_commit", 21),
    group("out", "mentions", "entity_mention", 7),
    group("out", "affects", "task_group", 2),
    group("out", "mentions", "git_branch", 2),
    group("out", "mentions", "spec_delta", 1),
    group("out", "mentions", "verification_step", 1),
    group("out", "affects", "pact_task", 1),
    group("out", "mentions", "git_author", 1),
  ];
  const nd = base.payload as { groups: G[] };
  const gone: G = {
    direction: "out", relationship: "imports", type: null, count: 3, count_is_floor: false,
    items: ["file:okg:src/okg/substrate/removed_a.py", "file:okg:src/okg/substrate/removed_b.py", "file:okg:src/okg/substrate/removed_c.py"]
      .map((id) => ({ id, label: null, producer: "okg-workspace.code-structure" })),
  };
  return {
    [MANY_GROUPS_RECORD_ID]: envelope(
      { id: MANY_GROUPS_RECORD_ID, type: "pact_change", label: "Harness: a change linked to fifteen kinds of record", source: "okg-workspace.pact" },
      many,
    ),
    [ABSENT_RECORD_ID]: envelope(
      { id: ABSENT_RECORD_ID, type: "source_file", label: "harness/absent_neighbour.py", source: "okg-workspace.git-files" },
      [...nd.groups, gone],
    ),
    [EMPTY_RECORD_ID]: envelope({ id: EMPTY_RECORD_ID, type: "view", label: "harness.no_links", source: "_system:native_views" }, []),
  };
}

/** The words the harness's search answers, and every prefix of them. */
export const GRAPH_SEARCH_TERMS = ["node_detail", "read_text", "add_task", "entity", "deployment", "cli"];

function key(route: string, params: Record<string, string | number>): string {
  const pairs = Object.entries(params).sort((a, b) => a[0].localeCompare(b[0]));
  return `${route}?${pairs.map(([n, v]) => `${n}=${encodeURIComponent(String(v))}`).join("&")}`;
}

function searchPage(q: string): unknown {
  const needle = q.toLowerCase();
  const hits = data.summaries.filter((s) => s.label.toLowerCase().includes(needle) || s.node_id.toLowerCase().includes(needle));
  return {
    generation_id: GRAPH_FIXTURE_GENERATION,
    payload: {
      query: q, type: null, filters: { type: null, source: null },
      results: hits.slice(0, 10).map((s) => ({ ...s, match: "text", excerpt: null })),
      exact_match: false, exact_match_in_text: false, total_count: hits.length, paged_count: hits.length,
      truncated: false, next_offset: hits.length > 10 ? 10 : null, rank_capped: false, zero_hit_reason: null,
    },
  };
}

function answered(searchVersion: string | null): Record<string, unknown> {
  const reads: Record<string, unknown> = {
    "/viz/api/console/graph/types": data.types,
    "/viz/api/console/graph/ontology": data.ontology,
    "/viz/api/console/graph/source-links": data.source_links,
  };
  for (const [id, read] of Object.entries({ ...data.records, ...composedRecords() })) reads[key("/viz/api/console/graph/record", { id })] = read;
  const byType = new Map<string, Summary[]>();
  for (const s of data.summaries) byType.set(s.type, [...(byType.get(s.type) ?? []), s]);
  for (const [type, list] of byType) {
    reads[key("/viz/api/console/records/list", { limit: 50, offset: 0, type, version: GRAPH_FIXTURE_GENERATION })] = {
      generation_id: GRAPH_FIXTURE_GENERATION,
      payload: { type, records: list.slice(0, 50), next_offset: list.length > 50 ? 50 : null, short_page: list.length <= 50 },
    };
  }
  if (searchVersion) {
    for (const term of GRAPH_SEARCH_TERMS) {
      for (let n = 1; n <= term.length; n += 1) {
        const q = term.slice(0, n);
        reads[key("/viz/api/console/records/search", { limit: 10, offset: 0, q, version: searchVersion })] = searchPage(q);
      }
    }
  }
  return reads;
}

/** The Graph reads one fixture serves, keyed as the fixture client keys. */
export function graphReads(name: string, latest: string | null): Record<string, unknown> {
  if (name === "workspace-mixed") return answered(latest);
  if (name === "scale-200") {
    return {
      ...answered(latest),
      [key("/viz/api/console/graph/record", { id: HUB_RECORD_ID })]: underPolicy(data.records[HUB_RECORD_ID]),
      // The body the server writes for a timeout, as the contract's
      // "Changes" give it: no sentence, so the page has to say it.
      "/viz/api/console/graph/source-links": failure(503, {
        reason: "source_links_unavailable", cause: "timeout", elapsed_ms: 18004, timeout_ms: 18000,
      }),
      [key("/viz/api/console/graph/record", { id: MISSING_RECORD_ID })]: failure(404, {
        reason: "record_not_found", error: `No record ${MISSING_RECORD_ID} in this generation.`,
      }),
    };
  }
  if (name === "broken-runtime") {
    const unavailable = (which: string) => failure(503, {
      reason: `${which}_unavailable`,
      error: `The maintained ${which.replace(/_/g, " ")} is not available for this generation.`,
    });
    return {
      "/viz/api/console/graph/types": unavailable("count_rollup"),
      "/viz/api/console/graph/ontology": unavailable("ontology_rollup"),
      "/viz/api/console/graph/source-links": unavailable("count_rollup"),
      [key("/viz/api/console/graph/record", { id: MISSING_RECORD_ID })]: failure(404, {
        reason: "record_not_found", error: `No record ${MISSING_RECORD_ID} in this generation.`,
      }),
    };
  }
  if (name === "empty") {
    const none = failure(404, { reason: "version_not_published", error: "This deployment has no published version." });
    return {
      "/viz/api/console/graph/types": none,
      "/viz/api/console/graph/ontology": none,
      "/viz/api/console/graph/source-links": none,
    };
  }
  return {};
}
