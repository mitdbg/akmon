// The attempt page's recorded sections, in every state they can be in.
//
// These render the PRESENTATIONAL component directly. `renderToStaticMarkup`
// never runs an effect, so rendering the route instead would assert the
// loading markup for every one of these branches and pass whatever the other
// branches happened to render.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { AttemptRecorded, type RecordedState } from "@operator-ui/console/views/AttemptRecorded";
import { type RawAttemptDetail, buildRecordedDetail } from "@operator-ui/console/model/updates/recorded";
import { ApiRequestError } from "@operator-ui/shared/api/apiClient";

const LINK = (path: string) => `#${path}`;

function payload(over: Partial<RawAttemptDetail["payload"]> = {}): RawAttemptDetail {
  return {
    payload: {
      deployment: "lane-d-home-render",
      branch: "default",
      attempt_id: "okg:d:branch:default:gen:x:5c5404d36118",
      version_published: true,
      attempt: {
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
      },
      holds: {
        node_count: 1723,
        edge_count: 3006,
        nodes: { added: 988, updated: 0, retracted: 0 },
        edges: { added: 1818, updated: 0, retracted: 0 },
        unavailable: [],
        unpublished_version: null,
      },
      checks: {
        gates: [{
          gate_name: "transform_consistency_gate",
          check_id: "transform.consistency.v4",
          result: "evaluated",
          reason: "no_violations",
          blocks_publish: false,
          elapsed_ms: 173,
          withheld: [{ key: "details", kind: "object", size: 36, withheld_reason: "exception_text" }],
        }],
        total: 1,
        passed: 1,
        blocked: 0,
        unavailable: null,
      },
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
      by_record_type: {
        rows: [
          { name: "document_chunk", count: 1490, parent_count: 587, parent_available: true },
          { name: "document", count: 233, parent_count: 148, parent_available: true },
        ],
        shown: 2,
        total: 2,
        capped: false,
        limit: 200,
        unavailable: null,
        unpublished_version: null,
      },
      by_relationship_type: {
        rows: [], shown: 0, total: 0, capped: false, limit: 200,
        unavailable: null, unpublished_version: null,
      },
      by_source: {
        rows: [], shown: 0, total: 0, capped: false, limit: 200,
        unavailable: null, unpublished_version: null,
      },
      ...over,
    },
  };
}

function markup(state: RecordedState): string {
  return renderToStaticMarkup(<AttemptRecorded state={state} />);
}

function recorded(over: Partial<RawAttemptDetail["payload"]> = {}): RecordedState {
  return { kind: "recorded", recorded: buildRecordedDetail(payload(over), { link: LINK }) };
}

describe("the attempt page's recorded sections", () => {
  it("says the deployment is being read while the read is in flight", () => {
    const html = markup({ kind: "loading" });
    expect(html).toContain("updates-detail-recorded-loading");
    expect(html).not.toContain("updates-detail-holds");
  });

  it("names a refused read in place, with its route and reason code", () => {
    const html = markup({
      kind: "failed",
      error: new ApiRequestError({
        route: "/viz/api/console/attempts/detail",
        status: 500,
        responseText: JSON.stringify({
          detail: { reason: "attempt_detail_composer_failed", error_type: "OperationalError" },
        }),
      }),
    });
    expect(html).toContain("updates-detail-recorded-failed");
    expect(html).toContain("attempt_detail_composer_failed");
    expect(html).toContain("/viz/api/console/attempts/detail");
    // Nothing is substituted for the read that failed.
    expect(html).not.toContain("updates-detail-holds");
  });

  it("prints a SERVED route's 404 as a plain named line, not as a failure block", () => {
    const html = markup({
      kind: "failed",
      error: new ApiRequestError({
        route: "/viz/api/console/attempts/detail",
        status: 404,
        // The constant body this route answers with, captured live at
        // evidence/live-attempt-not-found-404.json.
        responseText: JSON.stringify({ detail: { reason: "attempt_not_found" } }),
      }),
    });
    expect(html).toContain("updates-detail-recorded-not-found");
    expect(html).toContain("attempt_not_found");
    expect(html).not.toContain("updates-detail-recorded-failed");
    expect(html).not.toContain("updates-detail-recorded-route-unserved");
  });

  // Review item B3. A read API that predates this route answers the SAME
  // status from a different cause: FastAPI's own not-found handler, whose
  // body carries no reason. Reading that as "no such attempt" told the
  // operator a fact about their history that the server never stated, and
  // printed a reason code -- `attempt_not_found` -- that nothing sent.
  it("tells a 404 from an unserved route apart from a missing attempt", () => {
    const html = markup({
      kind: "failed",
      error: new ApiRequestError({
        route: "/viz/api/console/attempts/detail",
        status: 404,
        // Verbatim from the Archiv3 read API on 127.0.0.1:8797, captured at
        // evidence/live-unknown-route-404.json.
        responseText: JSON.stringify({ detail: "unknown viz API route" }),
      }),
    });
    expect(html).toContain("updates-detail-recorded-route-unserved");
    // The read is named, so the reader knows which one is missing.
    expect(html).toContain("/viz/api/console/attempts/detail");
    // No reason code is invented, and no claim is made about the history.
    expect(html).not.toContain("attempt_not_found");
    expect(html).not.toContain("records no attempt under that id");
    expect(html).not.toContain("updates-detail-recorded-not-found");
  });

  it("draws a published attempt's holds, counts and checks", () => {
    const html = markup(recorded());
    expect(html).toContain("updates-detail-holds");
    expect(html).toContain("1,723");
    expect(html).toContain("3,006");
    expect(html).toContain("updates-detail-counts-by_record_type");
    expect(html).toContain("+903");
    // The result in words, the gate's recorded name once (Archiv3 review).
    expect(html).toContain("no problems");
    expect(html).toContain("transform_consistency_gate");
    // Cut, not blanked: this payload recorded no relationship-type or source
    // rollup, so neither section is on the page at all.
    expect(html).not.toContain("updates-detail-counts-by_relationship_type");
    expect(html).not.toContain("updates-detail-counts-by_source");
    // Every cell is selected by its column, so cutting one cannot shift a
    // rule onto its neighbour.
    expect(html).toContain('data-column="name"');
    expect(html).toContain('data-column="delta"');
  });

  it("repeats the parent delta as a second line, for the width that cuts the column", () => {
    // At 390 the three columns are two fixed 168 px ones and whatever is
    // left, which was 22 px: every name rendered as a single character and
    // an ellipsis -- `d...`, `c...`, `l...`. The Against-the-parent column
    // is cut at that width, the same way the attempts list cuts Change, and
    // the fact it carried moves under the count as a second line. The line
    // is in the markup at every width and CSS decides where it is shown, so
    // this assertion is the one thing a static render can prove about it.
    const html = markup(recorded());
    expect(html).toContain("okg-console__crow-delta-narrow");
    // It carries the SAME text as the cell it stands in for, delta or the
    // named absence, so cutting the column never cuts the fact.
    expect(
      (html.match(/okg-console__crow-delta-narrow">\+903</g) ?? []).length,
    ).toBe(1);
  });

  it("names an unread parent on the second line too", () => {
    const html = markup(recorded({
      by_record_type: {
        rows: [{ name: "page", count: 3, parent_count: null, parent_available: false }],
        shown: 1,
        total: 1,
        capped: false,
        limit: 200,
        unavailable: null,
        unpublished_version: null,
      },
    }));
    // Not a zero on either line: the parent was not read, and that is what
    // both the column and its narrow stand-in say.
    expect(html).toContain(
      'okg-console__crow-delta-narrow">the parent version was not read<',
    );
  });

  it("marks a blocked attempt's counts as describing an unpublished version", () => {
    const html = markup(recorded({
      version_published: false,
      by_record_type: {
        rows: [{ name: "page", count: 3, parent_count: 2, parent_available: true }],
        shown: 1,
        total: 1,
        capped: false,
        limit: 200,
        unavailable: null,
        // The composer's real key. Asserted under `unavailable` before, which
        // the composer never wrote -- so this test passed on a payload shape
        // the product does not serve, and the sentence on the page was
        // actually coming from an inference the requirement forbids.
        unpublished_version: {
          reason: "counts_describe_an_unpublished_version",
          attempt_status: "blocked",
        },
      },
    }));
    expect(html).toContain("updates-detail-unpublished");
    expect(html).toContain("not published");
    // The counts are real facts about that checkpoint and still render.
    expect(html).toContain("updates-detail-counts-by_record_type");
    // And they are not filed as a refusal.
    expect(html).not.toContain("updates-detail-counts-by_record_type-refused");
  });

  it("says it on a blocked attempt whose rollup returned no table at all", () => {
    // Measured shape: the rollup can come back empty for a refused cycle, so
    // every count table is cut and only the holds numbers render. The
    // sentence must still appear -- those numbers are what it qualifies.
    const html = markup(recorded({
      version_published: false,
      holds: {
        node_count: 3,
        edge_count: 2,
        nodes: null,
        edges: null,
        unavailable: [],
        unpublished_version: {
          reason: "counts_describe_an_unpublished_version",
          attempt_status: "blocked",
        },
      },
      by_record_type: {
        rows: [], shown: 0, total: 0, capped: false, limit: 200,
        unavailable: null, unpublished_version: null,
      },
    }));
    expect(html).not.toContain("updates-detail-counts-by_record_type");
    expect(html).toContain("updates-detail-holds");
    expect(html).toContain("updates-detail-unpublished");
    // Never under the totals-absence lead: the totals WERE recorded.
    expect(html).not.toContain("Part of this version&#x27;s totals");
  });

  it("names a run lookup it could not make, rather than reporting none", () => {
    const html = markup(recorded({
      runs: {
        runs: [], total: 0, capped: false,
        unavailable: { reason: "target_generation_id_not_recorded" },
      },
    }));
    expect(html).toContain("target_generation_id_not_recorded");
    expect(html).not.toContain("No per-producer result is recorded");
  });

  it("never renders an unrecorded consistency check as zero checks that passed", () => {
    const html = markup(recorded({
      checks: {
        gates: [], total: null, passed: null, blocked: null,
        unavailable: { reason: "consistency_gates_not_recorded" },
      },
    }));
    expect(html).toContain("updates-detail-checks-none");
    expect(html).toContain("No consistency check is recorded");
    expect(html).not.toContain("0 checks");
    expect(html).not.toContain("all passed");
  });

  it("has no control on it but the copy buttons its parent owns", () => {
    const html = markup(recorded());
    expect(html).not.toContain("<button");
    expect(html).not.toContain("<form");
    expect(html).not.toContain("<input");
  });
});

describe("the three 404s, on the attempt page itself", () => {
  const ROUTE = "/viz/api/console/attempts/detail";

  function failedMarkup(responseText: string | undefined): string {
    return renderToStaticMarkup(
      <AttemptRecorded
        state={{
          kind: "failed",
          error: new ApiRequestError({ route: ROUTE, status: 404, responseText }),
        } as RecordedState}
      />,
    );
  }

  it("says the deployment holds no such attempt, without the raw code", () => {
    const markup = failedMarkup(JSON.stringify({
      detail: { reason: "attempt_not_found", error: "No such attempt." },
    }));
    expect(markup).toContain('data-testid="updates-detail-recorded-not-found"');
    expect(markup).toContain("This deployment records no attempt under that id.");
    // The code is a machine's word, and the sentence is a person's. It is
    // kept where the repo keeps machine codes -- the title -- and out of the
    // prose, where it said the same thing a second time in a language the
    // reader does not read.
    expect(markup).toContain('title="attempt_not_found"');
    expect(markup).not.toContain("(attempt_not_found)");
    expect(markup).not.toContain("<code");
  });

  it("names a route this read API does not serve, for the app's own catch-all", () => {
    const markup = failedMarkup(JSON.stringify({ detail: "unknown viz API route" }));
    expect(markup).toContain('data-testid="updates-detail-recorded-route-unserved"');
    expect(markup).toContain("does not serve");
  });

  it("makes no claim about the API's routes for a 404 it cannot read", () => {
    for (const body of [
      "<html><body><h1>404 Not Found</h1><hr>nginx</body></html>",
      JSON.stringify({ error: "not_found", service: "edge-cache" }),
      undefined,
    ]) {
      const markup = failedMarkup(body);
      expect(markup).toContain('data-testid="updates-detail-recorded-404-unrecognised"');
      expect(markup).toContain("a body this console does not recognise");
      // Neither of the other two sentences: this body said nothing that
      // supports either of them.
      expect(markup).not.toContain("does not serve");
      expect(markup).not.toContain("records no attempt under that id");
      expect(markup).not.toContain("attempt_not_found");
    }
  });
});
