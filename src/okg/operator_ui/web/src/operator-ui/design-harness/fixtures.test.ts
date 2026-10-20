// Every design fixture must be readable by the application that renders it.
//
// The point is shape drift. These fixtures are hand-designed payloads standing
// in for what nine console composers emit; if a composer's shape moves, or a
// generator edit breaks one, the mocks keep rendering happily against a payload
// the real console could never receive, and the drift surfaces only when the
// view is wired to live data weeks later.
//
// So nothing here asserts against a locally-written shape. Each fixture is fed
// through the application's OWN loaders and parsers -- loadConsoleHome,
// loadConsoleSources, parseVersionList, parseRecordDetail and the rest -- and
// the assertions are the counts the fixture itself declares in `expect`. A
// payload the shipping parsers reject fails here, at the fixture, rather than
// in a browser.
import { describe, expect, it } from "vitest";
import { loadDeployments } from "@operator-ui/app/deployments";
import { loadConsoleHome } from "@operator-ui/console/api/health";
import { loadConsoleSources } from "@operator-ui/console/api/sources";
import {
  listRecords,
  loadRecord,
  loadRecordTypes,
  loadVersions,
  searchRecords,
} from "@operator-ui/console/api/searchApi";
import {
  createFailingApiClient,
  createFixtureApiClient,
  FixtureReadMissing,
  readKey,
} from "@operator-ui/design-harness/fixtureClient";
import { DESIGN_FIXTURES, DESIGN_FIXTURE_NAMES, fixtureByName } from "@operator-ui/design-harness/fixtures";

const EXPECTED_NAMES = [
  "archiv3-small",
  "workspace-mixed",
  "scale-200",
  "empty",
  "broken-runtime",
  "all-clear",
];

describe("the design fixture set", () => {
  it("is the six mock deployments the PACT names, in switcher order", () => {
    expect(DESIGN_FIXTURE_NAMES).toEqual(EXPECTED_NAMES);
  });

  it("gives every fixture a purpose, its stories and its edge cases", () => {
    for (const fixture of DESIGN_FIXTURES) {
      expect(fixture.purpose.length, `${fixture.name} purpose`).toBeGreaterThan(40);
      expect(fixture.stories.length, `${fixture.name} stories`).toBeGreaterThan(0);
      expect(fixture.edge_cases.length, `${fixture.name} edge cases`).toBeGreaterThan(2);
      expect(fixture.clock).toBe("2026-09-18T09:00:00.000Z");
    }
    // Two fixtures pinned exactly, so a regenerated README row or a dropped
    // story fails here by name rather than passing a length check.
    const small = fixtureByName("archiv3-small");
    expect(small?.purpose).toBe(
      "The real Archiv3 shape: five sources, no enrichers, no views, one published version and four blocked attempts after it, and no runtime queue evidence.",
    );
    expect(small?.stories).toEqual(["S1", "S2", "S4", "S9", "S10"]);
    expect(small?.edge_cases).toHaveLength(5);
    expect(small?.edge_cases[0]).toBe("Latest attempt is blocked while reads still serve an older published version (S1).");
    const mixed = fixtureByName("workspace-mixed");
    expect(mixed?.stories).toEqual(["S5 (see all-clear)", "S7", "S9", "S11", "S12", "S14", "S15", "S16", "S17", "S18", "S19", "S21"]);
    expect(mixed?.edge_cases).toHaveLength(11);
    expect(mixed?.edge_cases[0]).toBe("Every producer state in one table: updated, failed, running, never run, excluded (S18, S21).");
    // 31, not 30: one of them is the producer nothing configures, added so
    // the fixtures cover the `configured: false` producer page.
    expect(mixed?.expect.source_count).toBe(31);
    expect(mixed?.expect.enricher_count).toBe(3);
    expect(mixed?.expect.view_count).toBe(4);
  });

  it("pins every read to one moment, so a capture never moves under the reviewer", () => {
    for (const fixture of DESIGN_FIXTURES) {
      const serialised = JSON.stringify(fixture.reads);
      // "5h ago" and friends are precomputed against the fixture clock. A read
      // that carried a live Date would make two captures of the same fixture
      // differ, and the mismatch ledger would chase a clock.
      expect(serialised.includes("NaN"), `${fixture.name} has an unresolved time`).toBe(false);
    }
    // The precomputed ages, read back exactly: the published version is
    // five days old on archiv3-small and a day old on workspace-mixed at the
    // fixture clock.
    const healthOf = (name: string) => (fixtureByName(name)?.reads["/viz/api/console/health"] as
      { payload: { generation_status: string; generation_age: string } }).payload;
    expect(healthOf("archiv3-small")).toMatchObject({ generation_status: "published", generation_age: "5d ago" });
    expect(healthOf("workspace-mixed")).toMatchObject({ generation_status: "published", generation_age: "24h ago" });
  });
});

describe("the fixture client", () => {
  it("refuses a read the fixture does not define instead of answering blank", async () => {
    const api = createFixtureApiClient(DESIGN_FIXTURES[0]);
    await expect(api.get("/viz/api/console/nonexistent")).rejects.toBeInstanceOf(FixtureReadMissing);
  });

  it("names the fixture and the key it looked for", async () => {
    const api = createFixtureApiClient(DESIGN_FIXTURES[0]);
    await expect(api.get("/viz/api/console/records/detail", { id: "no-such-record" }))
      .rejects.toThrow(/archiv3-small/);
  });

  it("keys a read exactly as the generator does", () => {
    expect(readKey("/viz/api/console/records/list", { type: "document", offset: 0, limit: 50 }))
      .toBe("/viz/api/console/records/list?limit=50&offset=0&type=document");
    // A null parameter is dropped by buildApiUrl, so it must be dropped here too.
    expect(readKey("/viz/api/console/records/search", { q: "x", type: null }))
      .toBe("/viz/api/console/records/search?q=x");
    // `refresh` is added by the Refresh control and changes no payload.
    expect(readKey("/viz/api/console/health", { refresh: 1 })).toBe("/viz/api/console/health");
  });

  it("does not mistake one record's payload for another's", async () => {
    const api = createFixtureApiClient(fixtureByName("workspace-mixed")!);
    const detail = await api.get<{ payload: { node_id: string } }>(
      "/viz/api/console/records/detail",
      { id: "person:riley" },
    );
    expect(detail.payload.node_id).toBe("person:riley");
  });

  it("still fails loudly when the client is the failing one", async () => {
    const api = createFailingApiClient("the data server is not running");
    await expect(api.get("/viz/api/console/health")).rejects.toThrow(/503/);
  });
});

describe.each(DESIGN_FIXTURES)("fixture $name", (fixture) => {
  const api = createFixtureApiClient(fixture);
  const expected = fixture.expect as Record<string, string | number | null>;

  // The old Home and Health pages' loaders were fed every fixture here until
  // PACT console-default-remove-old-views deleted those pages. The console's
  // own loaders for the same two questions take their place.
  it("loads Home through the console's loader", async () => {
    const data = await loadConsoleHome(api);
    expect(data.inputs.health.payload.deployment).toBe(expected.deployment);
  });

  it("loads Sources through the console's loader, every producer once", async () => {
    const data = await loadConsoleSources(api);
    // No minor read failed: health, attempts and versions all answered.
    expect(data.failures).toEqual({});
    expect(data.health?.payload.deployment).toBe(expected.deployment);
    const producers = data.producers.payload.producers;
    const byKind = (kind: string) => producers.filter((row) => row.kind === kind).length;
    expect(byKind("source")).toBe(expected.source_count);
    expect(byKind("enricher")).toBe(expected.enricher_count);
    expect(byKind("view")).toBe(expected.view_count);
    expect(producers.length).toBe(data.producers.payload.producer_count);
  });

  it("lists its sibling deployments, marking itself current", async () => {
    const entries = await loadDeployments(api);
    expect(entries.length).toBe(EXPECTED_NAMES.length);
    expect(entries.filter((entry) => entry.is_current).map((entry) => entry.name))
      .toEqual([fixture.name]);
  });

  it("parses its published versions", async () => {
    const versions = await loadVersions(api);
    expect(versions.deployment).toBe(expected.deployment);
    expect(versions.latest).toBe(expected.version);
    expect(versions.versions.length).toBe(
      Math.min(expected.published_attempt_count as number, 50),
    );
  });

  it("parses its record types and their total", async () => {
    const types = await loadRecordTypes(api, String(expected.version ?? ""));
    expect(types.payload.length).toBe(expected.record_type_count);
  });

  it("parses a search page, or refuses the read when nothing is published", async () => {
    const search = searchRecords(api, {
      version: String(expected.version ?? ""),
      query: "",
      type: null,
      offset: 0,
    });
    if (expected.version === null) {
      // Story S20: with no published version there is nothing to search, so
      // Explore must say so rather than issue this read. The fixture refuses
      // it, which is how the harness proves the view did not issue it.
      await expect(search).rejects.toBeInstanceOf(FixtureReadMissing);
      return;
    }
    const page = await search;
    expect(page.payload.query).toBe("");
    expect(typeof page.payload.totalCount).toBe("number");
  });

  it("parses a record page when the fixture has records", async () => {
    if (!expected.featured_record) {
      expect(expected.record_type_count).toBe(0);
      return;
    }
    const detail = await loadRecord(api, {
      version: String(expected.version ?? ""),
      id: String(expected.featured_record),
    });
    expect(detail.payload.found).toBe(true);
    if (detail.payload.found) {
      expect(detail.payload.nodeId).toBe(expected.featured_record);
      expect(detail.payload.relatedCap).toBeGreaterThan(0);
    }
  });

  it("parses the first type's record list", async () => {
    if (expected.record_type_count === 0) return;
    const types = await loadRecordTypes(api, String(expected.version ?? ""));
    const page = await listRecords(api, {
      version: String(expected.version ?? ""),
      type: types.payload[0].type,
      offset: 0,
    });
    expect(page.payload.type).toBe(types.payload[0].type);
  });

  it("reports the attempt outcomes the fixture declares", async () => {
    const generations = await api.get<{
      payload: { total: number; generations: Array<{ status: string }> };
    }>("/viz/api/console/generations");
    expect(generations.payload.total).toBe(expected.attempt_count);
  });

  it("keeps the source read's duplicate rows, which Sources must collapse", async () => {
    const sources = await api.get<{ payload: { source_count: number } }>(
      "/viz/api/console/sources",
    );
    expect(sources.payload.source_count).toBe(expected.source_read_row_count);
  });

  it("carries the runtime state the fixture declares", async () => {
    const health = await api.get<{
      payload: {
        vitals: { queue_total: number; dlq_count: number };
        checks: Array<{ severity: string }>;
      };
    }>("/viz/api/console/health");
    expect(health.payload.vitals.queue_total).toBe(expected.queue_total);
    expect(health.payload.vitals.dlq_count).toBe(expected.dlq_count);
    expect(health.payload.checks.filter((check) => check.severity === "fail").length)
      .toBe(expected.failing_check_count);
  });

  it("offers an attempts-by-day series for every range the design uses", async () => {
    for (const range of ["1d", "7d", "14d", "30d", "90d"]) {
      const series = await api.get<{ payload: { buckets: unknown[] } }>(
        "/viz/api/console/attempts",
        { range },
      );
      // n + 1 buckets for an n-day range: the span reaches back n WHOLE days,
      // so it holds the read's own day and the n before it. That is the live
      // composer's boundary (`console_views/attempts_by_day.py::_days`) and
      // the one the console's age label shares, so a row the page calls
      // "14 d ago" sits inside the 14-day chart rather than one day outside
      // it under a strip reading "No attempt in this span".
      expect(series.payload.buckets.length, `${fixture.name} ${range}`)
        .toBe(Number(range.replace("d", "")) + 1);
    }
  });

  it("offers an ontology rollup with the relationship types the fixture declares", async () => {
    const rollup = await api.get<{ payload: { relationship_types: unknown[] } }>(
      "/viz/api/console/ontology-rollup",
    );
    expect(rollup.payload.relationship_types.length).toBe(expected.relationship_type_count);
  });
});

describe("the edge cases the stories need", () => {
  it("archiv3-small returns eleven source rows for five sources", () => {
    const fixture = fixtureByName("archiv3-small")!;
    expect(fixture.expect.source_count).toBe(5);
    expect(fixture.expect.source_read_row_count).toBe(11);
  });

  it("scale-200 carries a record capped at 100 of 400 outgoing relationships (S13)", async () => {
    const fixture = fixtureByName("scale-200")!;
    const api = createFixtureApiClient(fixture);
    const detail = await api.get<{
      payload: {
        related: {
          cap: number; outgoing: unknown[]; outgoing_total: number; outgoing_capped: boolean;
          incoming_total: number; by_relationship: Array<{ relationship: string }>;
        };
      };
    }>("/viz/api/console/records/detail", { id: "commit:9a1c02e" });
    const related = detail.payload.related;
    expect(related.cap).toBe(100);
    expect(related.outgoing.length).toBe(100);
    expect(related.outgoing_total).toBe(400);
    expect(related.outgoing_capped).toBe(true);
    expect(related.outgoing_total + related.incoming_total).toBe(500);
    expect(new Set(related.by_relationship.map((row) => row.relationship)).size)
      .toBeGreaterThanOrEqual(5);
  });

  it("scale-200 carries a blocked attempt with a forty-line recorded error", async () => {
    const api = createFixtureApiClient(fixtureByName("scale-200")!);
    const generations = await api.get<{
      payload: { generations: Array<{ status: string; error_text: string | null; stopped_stage: string | null }> };
    }>("/viz/api/console/generations");
    const blocked = generations.payload.generations.find((row) => row.status === "blocked");
    expect(blocked).toBeTruthy();
    expect(blocked!.stopped_stage).toBe("publication checks");
    expect(blocked!.error_text!.split("\n").length).toBe(40);
  });

  it("workspace-mixed carries one retained payload and one policy refusal (S14), and the same policy refuses the artifact", async () => {
    const api = createFixtureApiClient(fixtureByName("workspace-mixed")!);
    const present = await api.get<{ payload: { payload: { inline: string | null } | null; refusal: unknown } }>(
      "/viz/api/console/records/raw", { id: "document:handbook" },
    );
    expect(present.payload.payload?.inline).toBeTruthy();
    expect(present.payload.refusal).toBeNull();

    const refused = await api.get<{ payload: { payload: unknown; refusal: { policy: string } | null; row: unknown } }>(
      "/viz/api/console/records/raw", { id: "person:riley" },
    );
    expect(refused.payload.payload).toBeNull();
    expect(refused.payload.refusal?.policy).toBe("nomos.contact_details");
    expect(refused.payload.row).toBeTruthy();
    const artifact = await api.get<{ payload: { exists: boolean; reason?: string; policy?: string } }>(
      "/viz/api/console/records/artifact", { id: "person:riley" },
    );
    expect(artifact.payload).toMatchObject({ exists: false, reason: "policy_refused", policy: "nomos.contact_details" });
  });

  it("workspace-mixed serves one artifact of each rendered kind, and one payload past the inline limit", async () => {
    const api = createFixtureApiClient(fixtureByName("workspace-mixed")!);
    const kinds: Record<string, string> = {};
    for (const id of ["paper:okg-2026", "document:handbook", "file:src/main.rs", "document:status-page", "document:diagram"]) {
      const read = await api.get<{ payload: { exists: boolean; format: string; sha256: string; byte_count: number; url: string } }>(
        "/viz/api/console/records/artifact", { id },
      );
      expect(read.payload.exists, id).toBe(true);
      expect(read.payload.sha256, id).toMatch(/^[0-9a-f]{64}$/);
      expect(read.payload.url, id).toMatch(/^\/design\/fixtures\/artifacts\//);
      kinds[read.payload.format] = id;
    }
    expect(Object.keys(kinds).sort()).toEqual(["code", "html", "image", "markdown", "pdf"]);
    const large = await api.get<{ payload: { payload: { inline: string | null; head: string | null; byte_count: number } } }>(
      "/viz/api/console/records/raw", { id: "session:sess-1" },
    );
    expect(large.payload.payload.inline).toBeNull();
    expect(large.payload.payload.byte_count).toBeGreaterThan(256 * 1024);
    expect(large.payload.payload.head?.split("\n").length).toBe(200);
  });

  it("workspace-mixed hides a field's value and names no policy for it (was S15)", async () => {
    // S15 asked this fixture to name the policy behind a hidden field. The
    // live composer sends no `hidden_policy` on any field row, so the
    // generator stopped emitting one and the Explore page says "hidden by
    // policy" and names none (ui/cuts.md). This test used to pass only
    // because the fixture ON DISK still carried the key the generator had
    // already dropped. Re-aimed at the shape the server actually sends: a
    // hidden field still exists and still withholds its value, and the key
    // that was never served is asserted absent rather than truthy.
    const api = createFixtureApiClient(fixtureByName("workspace-mixed")!);
    const detail = await api.get<{
      payload: { fields: Array<{ key: string; value: string | null; hidden: boolean }> };
    }>("/viz/api/console/records/detail", { id: "document:handbook" });
    const hidden = detail.payload.fields.filter((field) => field.hidden);
    expect(hidden.length).toBeGreaterThan(0);
    for (const field of hidden) {
      expect(field.value, `${field.key} served a value it marked hidden`).toBeNull();
      expect(field, `${field.key} carries a key no read serves`).not.toHaveProperty("hidden_policy");
    }
  });

  it("workspace-mixed holds every producer state at once", async () => {
    const api = createFixtureApiClient(fixtureByName("workspace-mixed")!);
    const diagnostics = await api.get<{
      payload: { sources: Array<{ workflow: { state: string } }> };
    }>("/viz/api/console/source-diagnostics");
    const states = new Set(diagnostics.payload.sources.map((row) => row.workflow.state));
    for (const state of ["updated", "failed", "running", "never run", "excluded"]) {
      expect(states.has(state), `workspace-mixed has no ${state} source`).toBe(true);
    }
  });

  it("all-clear has nothing wrong anywhere (S5)", async () => {
    const fixture = fixtureByName("all-clear")!;
    const api = createFixtureApiClient(fixture);
    expect(fixture.expect.failed_producer_count).toBe(0);
    expect(fixture.expect.never_run_producer_count).toBe(0);
    expect(fixture.expect.blocked_attempt_count).toBe(0);
    const health = await api.get<{ payload: { verdict: string; checks: unknown[] } }>(
      "/viz/api/console/health",
    );
    expect(health.payload.verdict).toBe("healthy");
    expect(health.payload.checks).toEqual([]);
  });

  it("empty has no published version and nothing to search (S20)", async () => {
    const fixture = fixtureByName("empty")!;
    expect(fixture.expect.version).toBeNull();
    expect(fixture.expect.record_type_count).toBe(0);
    expect(fixture.expect.never_run_producer_count).toBe(3);
    expect(fixture.expect.failed_producer_count).toBe(0);
  });

  it("broken-runtime fails on the system, not on a source (S3)", async () => {
    const api = createFixtureApiClient(fixtureByName("broken-runtime")!);
    const health = await api.get<{
      payload: {
        vitals: { queue_total: number; dlq_count: number };
        checks: Array<{ scope: string; severity: string; code: string }>;
      };
    }>("/viz/api/console/health");
    expect(health.payload.vitals.dlq_count).toBe(37);
    expect(health.payload.vitals.queue_total).toBe(2566);
    const runtimeFailures = health.payload.checks
      .filter((check) => check.scope === "runtime" && check.severity === "fail")
      .map((check) => check.code);
    expect(runtimeFailures).toContain("worker_missing");
    expect(runtimeFailures).toContain("dead_letter_jobs");
    expect(runtimeFailures).toContain("queue_backlog");

    const generations = await api.get<{
      payload: { generations: Array<{ status: string; stopped_stage: string | null; stopped_producer: string | null }> };
    }>("/viz/api/console/generations");
    const blocked = generations.payload.generations.find((row) => row.status === "blocked");
    expect(blocked!.stopped_stage).toBe("publication checks");
    expect(blocked!.stopped_producer).toBeNull();
    expect(generations.payload.generations.some((row) => row.status === "running")).toBe(true);
  });
});
