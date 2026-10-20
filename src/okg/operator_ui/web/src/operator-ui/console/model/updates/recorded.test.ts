// What the attempt page reads out of the attempts/detail payload.
//
// Every case here is a fact the live read serves and the page must not
// mangle: the retraction that must keep its minus sign, the parent that could
// not be read and must not read as a delta of zero, the blocked checkpoint
// whose counts are real but describe a version nobody published, and the
// deployment with no consistency check, which must never render as "0 checks,
// all passed".
import { describe, expect, it } from "vitest";

import {
  type RawAttemptDetail,
  type RawCountSection,
  buildRecordedDetail,
} from "@operator-ui/console/model/updates/recorded";
import type { RawAttempt } from "@operator-ui/console/model/shared/reads";

const LINK = (path: string) => `#${path}`;

function attempt(fields: Partial<RawAttempt> = {}): RawAttempt {
  return {
    generation_id: 1,
    full_generation_id: "okg:d:branch:default:gen:x:5c5404d36118",
    status: "published",
    created_at: "2026-09-21T02:48:10.000Z",
    duration_ms: null,
    nodes_added: 988,
    edges_added: 1818,
    nodes_retracted: 0,
    edges_retracted: 0,
    stopped_stage: null,
    stopped_producer: null,
    ...fields,
  };
}

function section(fields: Partial<RawCountSection> = {}): RawCountSection {
  return {
    rows: [],
    shown: 0,
    total: 0,
    capped: false,
    limit: 200,
    unavailable: null,
    unpublished_version: null,
    ...fields,
  };
}

function detail(over: Partial<RawAttemptDetail["payload"]> = {}): RawAttemptDetail {
  return {
    payload: {
      deployment: "lane-d-home-render",
      branch: "default",
      attempt_id: "okg:d:branch:default:gen:x:5c5404d36118",
      version_published: true,
      attempt: attempt(),
      holds: {
        node_count: 1723,
        edge_count: 3006,
        nodes: { added: 988, updated: 0, retracted: 0 },
        edges: { added: 1818, updated: 0, retracted: 0 },
        unavailable: [],
        unpublished_version: null,
      },
      checks: { gates: [], total: null, passed: null, blocked: null, unavailable: null },
      runs: { runs: [], total: 0, capped: false, unavailable: null },
      neighbours: {
        newer_attempt_id: null,
        older_attempt_id: "okg:d:branch:default:gen:y:e71bec30ed62",
        position: 1,
        total: 6,
        parent_generation_id: "okg:d:branch:default:gen:y:e71bec30ed62",
        parent_source: "recorded_predecessor",
      },
      configured: {
        deployment_attestations: { recorded: false, names: [], withheld_count: 0 },
        nomos_policy_bundle_ids: { recorded: true, names: [], withheld_count: 0 },
        excluded_sources: { recorded: true, names: [] },
        enricher_governance_record_count: 0,
        catalog_version_id: 4,
      },
      by_record_type: section(),
      by_relationship_type: section(),
      by_source: section(),
      ...over,
    },
  };
}

describe("the attempt page's recorded facts", () => {
  it("states what the version holds, and says the addition only once", () => {
    const page = buildRecordedDetail(detail(), { link: LINK });
    expect(page.holdsLine).toContain("1,723");
    expect(page.holdsLine).toContain("3,006");
    // The header line above this section already prints "+988 records ·
    // +1,818 relationships". A pure addition adds nothing here, so the same
    // fact is not printed twice on one page.
    expect(page.movedLine).toBeNull();
  });

  it("says what a retraction removed, which the header alone does not", () => {
    const page = buildRecordedDetail(detail({
      holds: {
        node_count: 735,
        edge_count: 1188,
        nodes: { added: 0, updated: 0, retracted: 988 },
        edges: { added: 0, updated: 0, retracted: 1818 },
        unavailable: [],
        unpublished_version: null,
      },
    }), { link: LINK });
    expect(page.movedLine).toContain("988");
    expect(page.movedLine).toContain("1,818");
    expect(page.movedLine).toContain("etracted");
    expect(page.movedLine).not.toContain("no change");
  });

  it("names an update, the one move no total-subtraction delta can see", () => {
    const page = buildRecordedDetail(detail({
      holds: {
        node_count: 1723,
        edge_count: 3006,
        nodes: { added: 10, updated: 12, retracted: 4 },
        edges: { added: 0, updated: 0, retracted: 0 },
        unavailable: [],
        unpublished_version: null,
      },
    }), { link: LINK });
    expect(page.movedLine).toContain("12");
    expect(page.movedLine).toContain("pdated");
    expect(page.movedLine).toContain("4");
  });

  it("prints a delta per row against the parent version", () => {
    const page = buildRecordedDetail(detail({
      by_record_type: section({
        rows: [
          { name: "document_chunk", count: 1490, parent_count: 587, parent_available: true },
          { name: "document", count: 233, parent_count: 148, parent_available: true },
        ],
        shown: 2,
        total: 2,
      }),
    }), { link: LINK });
    const table = page.tables.find((row) => row.key === "by_record_type");
    expect(table?.rows.map((row) => `${row.name} ${row.count} ${row.delta}`)).toEqual([
      "document_chunk 1,490 +903",
      "document 233 +85",
    ]);
  });

  it("says a parent that could not be read is unread, not a delta of zero", () => {
    const page = buildRecordedDetail(detail({
      by_source: section({
        rows: [{ name: "ontologies", count: 735, parent_count: null, parent_available: false }],
        shown: 1,
        total: 1,
        parent_unavailable: { reason: "rollup_unavailable_for_parent_version" },
      }),
    }), { link: LINK });
    const table = page.tables.find((row) => row.key === "by_source");
    expect(table?.rows[0].delta).toBeNull();
    expect(table?.rows[0].parentNote).toContain("not read");
    expect(table?.parentNote).toContain("rollup_unavailable_for_parent_version");
  });

  it("prints the read's own reason when a rollup was refused", () => {
    const page = buildRecordedDetail(detail({
      by_record_type: section({
        unavailable: { reason: "rollup_unavailable_for_version", rollup_reason: "no_sentinel" },
      }),
    }), { link: LINK });
    const table = page.tables.find((row) => row.key === "by_record_type");
    expect(table?.refusedNote).toContain("rollup_unavailable_for_version");
    expect(table?.refusedNote).toContain("no_sentinel");
  });

  it("cuts a count table the deployment recorded nothing for", () => {
    const page = buildRecordedDetail(detail(), { link: LINK });
    expect(page.tables).toEqual([]);
  });

  it("states the cap and the total when a section was capped", () => {
    const page = buildRecordedDetail(detail({
      by_source: section({
        rows: [{ name: "a", count: 1, parent_count: 0, parent_available: true }],
        shown: 1,
        total: 431,
        capped: true,
      }),
    }), { link: LINK });
    expect(page.tables[0].capLine).toContain("431");
  });

  it("says the counts describe a version nobody published, from the marker", () => {
    const page = buildRecordedDetail(detail({
      version_published: false,
      attempt: attempt({ status: "blocked" }),
      by_record_type: section({
        rows: [{ name: "page", count: 3, parent_count: 2, parent_available: true }],
        shown: 1,
        total: 1,
        // The key the composer really emits. It used to be asserted under
        // `unavailable`, which the composer never wrote there -- so the
        // sentence on the page was coming from an inference over
        // `version_published`, not from the read's own marker.
        unpublished_version: {
          reason: "counts_describe_an_unpublished_version",
          attempt_status: "blocked",
        },
      }),
    }), { link: LINK });
    expect(page.unpublishedNote).toContain("not published");
    // The marker is not a refusal: the rows are real and still render.
    expect(page.tables[0].rows).toHaveLength(1);
    expect(page.tables[0].refusedNote).toBeNull();
  });

  it("never turns an unrecorded consistency check into zero checks that passed", () => {
    const page = buildRecordedDetail(detail({
      checks: {
        gates: [],
        total: null,
        passed: null,
        blocked: null,
        unavailable: { reason: "consistency_gates_not_recorded" },
      },
    }), { link: LINK });
    expect(page.checksNote).toContain("No consistency check is recorded");
    expect(page.checksSummary).toBeNull();
    expect(page.gates).toEqual([]);
    expect(`${page.checksNote} ${page.checksSummary}`).not.toContain("0 checks");
  });

  it("serves the gates that passed, with what the read withheld named", () => {
    const page = buildRecordedDetail(detail({
      checks: {
        gates: [{
          gate_name: "transform_consistency_gate",
          check_id: "transform.consistency.v4",
          result: "evaluated",
          reason: "no_violations",
          blocks_publish: false,
          elapsed_ms: 173,
          withheld: [
            { key: "details", kind: "object", size: 36, withheld_reason: "exception_text" },
            { key: "preflight", kind: "object", size: 12, withheld_reason: "exception_text" },
          ],
        }],
        total: 1,
        passed: 1,
        blocked: 0,
        unavailable: null,
      },
    }), { link: LINK });
    expect(page.gates).toHaveLength(1);
    // In words, with the recorded name kept once (Archiv3 review).
    expect(page.gates[0].name).toBe("Consistency check");
    expect(page.gates[0].code).toBe("transform_consistency_gate");
    expect(page.gates[0].detail).toContain("no problems");
    // Passed: nothing is said about the withheld fields (review 2026-09-23).
    expect(page.gates[0].withheldNote).toBeNull();
    expect(page.checksSummary).toContain("1");
  });

  it("caps the producer runs it prints and states the recorded total", () => {
    const runs = Array.from({ length: 25 }, (_, index) => ({
      definition_id: `view.${index}`,
      artifact_kind: "views",
      run_id: `run-${index}`,
      status: "completed",
      trigger_kind: "dirty",
      error: null,
      started_at: "2026-09-21T02:48:09.000Z",
      completed_at: "2026-09-21T02:48:10.000Z",
    }));
    const page = buildRecordedDetail(detail({
      runs: { runs, total: 25, capped: false, unavailable: null },
    }), { link: LINK });
    expect(page.runs).toHaveLength(20);
    expect(page.runsCapLine).toContain("25");
  });

  it("links the neighbours the read named and nothing else", () => {
    const page = buildRecordedDetail(detail(), { link: LINK });
    expect(page.newerHref).toBeNull();
    expect(page.olderHref).toBe("#updates/okg:d:branch:default:gen:y:e71bec30ed62");
    expect(page.positionLine).toContain("6");
    expect(page.parentLine).toContain("e71bec30ed62");
  });

  it("prints what was configured and collapses the absences into one line", () => {
    const page = buildRecordedDetail(detail(), { link: LINK });
    // The live payload records exactly one thing here and four absences.
    // Four sentences of absence in a row is a section that says nothing.
    expect(page.configuredLines).toEqual(["Schema version 4."]);
    const absent = page.configuredAbsences.join(" | ");
    // One plain line for what was set to nothing; what the attempt never
    // recorded (attestation) is not listed (review 2026-09-23).
    expect(absent).toBe("No policy bundle, excluded source or enricher governance record was set.");
    expect(absent).not.toContain("attestation");
  });

  it("cuts the configured section when the attempt recorded nothing in it", () => {
    const page = buildRecordedDetail(detail({
      configured: {
        deployment_attestations: { recorded: false, names: [], withheld_count: 0 },
        nomos_policy_bundle_ids: { recorded: false, names: [], withheld_count: 0 },
        excluded_sources: { recorded: false, names: [] },
        enricher_governance_record_count: null,
        catalog_version_id: null,
      },
    }), { link: LINK });
    expect(page.configuredLines).toEqual([]);
    expect(page.configuredAbsences).toEqual([]);
  });

  it("says a gate field is withheld without the byte count", () => {
    const page = buildRecordedDetail(detail({
      checks: {
        gates: [{
          gate_name: "transform_consistency_gate",
          result: "evaluated",
          reason: "no_violations",
          withheld: [
            { key: "details", kind: "object", size: 36, withheld_reason: "exception_text" },
            { key: "preflight", kind: "object", size: 12, withheld_reason: "exception_text" },
          ],
        }],
        total: 1,
        passed: 1,
        blocked: 0,
        unavailable: null,
      },
    }), { link: LINK });
    // A passed gate: nothing failed, so nothing is said about hidden fields
    // (review 2026-09-23: it read as "errors are being hidden").
    expect(page.gates[0].withheldNote).toBeNull();
  });
});

describe("the unpublished sentence comes from the read's marker alone", () => {
  const marker = {
    reason: "counts_describe_an_unpublished_version",
    attempt_status: "blocked",
  };

  it("prints it on a marker even when version_published is true", () => {
    // version_published and the marker are two statements from the same
    // read. The page prints what the READ said; it does not arbitrate
    // between them, and it does not decide the fact for itself from a flag
    // plus a row count.
    const page = buildRecordedDetail(detail({
      version_published: true,
      by_record_type: section({
        rows: [{ name: "page", count: 3, parent_count: 2, parent_available: true }],
        shown: 1,
        total: 1,
        unpublished_version: marker,
      }),
    }), { link: LINK });
    expect(page.unpublishedNote).toContain("not published");
  });

  it("prints nothing when version_published is false and no marker was set", () => {
    const page = buildRecordedDetail(detail({
      version_published: false,
      attempt: attempt({ status: "blocked" }),
      by_record_type: section({
        rows: [{ name: "page", count: 3, parent_count: 2, parent_available: true }],
        shown: 1,
        total: 1,
      }),
    }), { link: LINK });
    expect(page.unpublishedNote).toBeNull();
  });

  it("prints it from holds when the rollup returned no table at all", () => {
    // A blocked attempt whose rollup is empty has no count table, so the
    // sentence cannot hang off one. The holds numbers still render, and they
    // are exactly what the sentence qualifies.
    const page = buildRecordedDetail(detail({
      version_published: false,
      attempt: attempt({ status: "blocked" }),
      holds: {
        node_count: 3,
        edge_count: 2,
        nodes: null,
        edges: null,
        unavailable: [],
        unpublished_version: marker,
      },
    }), { link: LINK });
    expect(page.tables).toEqual([]);
    expect(page.holdsLine).toContain("3");
    expect(page.unpublishedNote).toContain("not published");
    // And it is not filed under the absences: the totals WERE recorded.
    expect(page.holdsNotes).toEqual([]);
  });

  it("still treats a refused rollup as a refusal, not as this sentence", () => {
    const page = buildRecordedDetail(detail({
      by_record_type: section({
        unavailable: {
          reason: "rollup_unavailable_for_version",
          rollup_reason: "count_rollup_disabled",
        },
      }),
    }), { link: LINK });
    expect(page.unpublishedNote).toBeNull();
    expect(page.tables[0].refusedNote).toContain("count_rollup_disabled");
  });
});

describe("the producer runs say which kind of nothing they found", () => {
  it("names an attempt the read could not join to the runs table", () => {
    const page = buildRecordedDetail(detail({
      runs: {
        runs: [], total: 0, capped: false,
        unavailable: { reason: "target_generation_id_not_recorded" },
      },
    }), { link: LINK });
    expect(page.runsNote).toContain("target_generation_id_not_recorded");
    expect(page.runsNote).not.toContain("No per-producer result is recorded");
  });

  it("still says none recorded when the read looked and found none", () => {
    const page = buildRecordedDetail(detail({
      runs: { runs: [], total: 0, capped: false, unavailable: null },
    }), { link: LINK });
    expect(page.runsNote).toBe("No per-producer result is recorded for this attempt.");
  });
});

describe("an attempt older than the window the read scanned", () => {
  // The bounded attempt-detail read serves an attempt outside its window by
  // fetching it by id, and it cannot say where that attempt sits in the list.
  // Printed through the in-window sentence, `position: null` became "Attempt
  // null of 88".
  const outsideWindow = () => {
    const page = detail().payload;
    return {
      ...page,
      neighbours: {
        ...page.neighbours,
        position: null,
        newer_attempt_id: null,
        older_attempt_id: null,
        older_attempt_unavailable_reason:
          "attempt_is_older_than_the_window_this_read_scanned",
        unavailable_reason:
          "attempt_is_older_than_the_window_this_read_scanned",
      },
    };
  };

  it("says how many attempts exist and that its place was not read", () => {
    const model = buildRecordedDetail(
      { payload: outsideWindow() }, { link: LINK },
    );
    expect(model.positionLine).not.toContain("null");
    expect(model.positionLine).toContain("older than the window this read scanned");
    expect(model.positionLine).toContain("6");
  });

  it("offers neither neighbour link, because it read neither", () => {
    const model = buildRecordedDetail(
      { payload: outsideWindow() }, { link: LINK },
    );
    expect(model.newerHref).toBeNull();
    expect(model.olderHref).toBeNull();
  });

  it("never calls a window's edge the end of the history", () => {
    const page = outsideWindow();
    page.neighbours.parent_generation_id = null;
    page.neighbours.parent_source = null;
    const model = buildRecordedDetail({ payload: page }, { link: LINK });
    expect(model.parentLine).toContain("older than the window this read scanned");
    expect(model.parentLine).not.toContain("no earlier attempt is recorded");
  });
});
