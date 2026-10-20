// Sources, asserted against the six fixtures rather than against a shape
// written here.
//
// The captures are what the operator approves. These tests hold what a
// screenshot cannot: that the eleven sources-read rows on archiv3-small are
// five producers, that the default order puts failed first and last-run
// newest first, that a filter arriving on the URL filters, that a sortable
// column sorts, that the shared-cause line prints only when one cause covers
// every failure, and that a field no read records is absent rather than
// blank: there is no next-due column, no registry entry and no run error
// text, because nothing supplies them.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { createFixtureApiClient, FixtureReadMissing } from "@operator-ui/design-harness/fixtureClient";
import { DESIGN_FIXTURES, fixtureByName } from "@operator-ui/design-harness/fixtures";
import { designLink, parseDesignLocation } from "@operator-ui/design-harness/DesignHarness";
import { loadConsoleSources, loadProducerDetail } from "@operator-ui/console/api/sources";
import { ConsoleProducerDetail } from "@operator-ui/console/views/ProducerDetail";
import { ConsoleSources } from "@operator-ui/console/views/Sources";
import type { PageContext } from "@operator-ui/console/model/copyText";
import { CREDENTIAL_VALUE, isCredentialKey, REDACTED } from "@operator-ui/console/__fixtures__/credentialShapes";
import { buildProducerDetail } from "@operator-ui/console/model/sources/detail";
import { buildSourcesModel, pageOf, readinessScopeNote, type SourcesModel } from "@operator-ui/console/model/sources/list";
import { producerRecordsFrom, type ProducerRecord } from "@operator-ui/console/model/sources/producers";
import { ATTENTION_SEVERITIES, producerGlyph, UNDETERMINED_STATE } from "@operator-ui/console/model/shared/producerStates";
import type { RawProducerRecord, RawProducersRead } from "@operator-ui/console/model/sources/reads";
import { buildSourceRow, DEFAULT_SORT, foldName, ignoredSortParam, nextSort, NO_SOURCES_FILTER, parseSourcesFilter, parseSourcesSort, sharedCauseLine, sortRows, sourcesSortParams, type Sort, type SourcesFilter } from "@operator-ui/console/model/sources/rows";

/** One producers-read row, with only what a case needs spelled out. */
function row(over: Partial<RawProducerRecord> & { name: string }): RawProducerRecord {
  return {
    ownership_id: `x.${over.name}`,
    kind: "source",
    configured: true,
    health: { status: "healthy", incident_count: 0 },
    workflow: { state: "updated", latest_issue: null },
    contribution: { records_in_gen: 0, edges_in_gen: 0 },
    freshness: { status: "not_evaluated" },
    latest_run: null,
    run_count: null,
    ...over,
  };
}

function producersReadOf(rows: RawProducerRecord[]): RawProducersRead {
  return {
    payload: {
      deployment: "x",
      generation_id: null,
      producer_count: rows.length,
      producers: rows,
      selected: null,
    },
  };
}

async function sourcesFor(
  name: string,
  filter: SourcesFilter = NO_SOURCES_FILTER,
  sort: Sort = DEFAULT_SORT,
) {
  const fixture = fixtureByName(name);
  if (!fixture) throw new Error(`no fixture named ${name}`);
  const data = await loadConsoleSources(createFixtureApiClient(fixture));
  return { fixture, data, model: buildSourcesModel(data, filter, designLink(name), sort) };
}

const CONTEXT: PageContext = {
  deployment: "archiv3-small",
  version: null,
  publishedAt: null,
  nowMs: Date.parse("2026-09-18T09:00:00.000Z"),
};

function listMarkup(model: SourcesModel, pages = 1): string {
  return renderToStaticMarkup(
    ConsoleSources({
      model,
      sort: DEFAULT_SORT,
      onSort: () => {},
      nameQuery: model.filter.name,
      onNameQuery: () => {},
      pages,
      onMore: () => {},
      moreError: null,
      context: CONTEXT,
    }),
  );
}

async function detailFor(fixtureName: string, producer: string) {
  const { data, model } = await sourcesFor(fixtureName);
  const record = producerRecordsFrom(data.producers)
    .find((candidate: ProducerRecord) => candidate.name === producer);
  if (!record) throw new Error(`${fixtureName} has no producer ${producer}`);
  const fixture = fixtureByName(fixtureName);
  if (!fixture) throw new Error(`no fixture named ${fixtureName}`);
  const read = await loadProducerDetail(createFixtureApiClient(fixture), producer);
  const detail = buildProducerDetail(record, read, { nowMs: model.nowMs });
  return { record, read, detail };
}

describe("The producers read returns each producer once", () => {
  it("shows five rows for archiv3-small's five producers", async () => {
    const { fixture, data, model } = await sourcesFor("archiv3-small");
    // The old sources read still returns eleven rows for these five
    // producers; the view no longer reads it. The collapse did not go away,
    // it moved into console/producers, where the registry's declared
    // ownership_id decides identity instead of a guess at a name.
    expect(fixture.expect.source_read_row_count).toBe(11);
    expect(data.producers.payload.producers).toHaveLength(5);
    expect(model.rows).toHaveLength(5);
    expect(model.total).toBe(5);
    expect(model.rows.map((row) => row.name).sort()).toEqual(
      ["cmssw_releases", "docsite", "indico", "papers", "twiki"],
    );
    const markup = listMarkup(model);
    expect(markup.match(/data-testid="sources-row"/g)).toHaveLength(5);
    expect(markup).not.toContain("docsite:worker");
  });

  it("no longer folds runtime keys in the browser, and says nothing about them", async () => {
    // Re-aimed. This used to assert that docsite carried the runtime keys
    // `docsite:worker` and `docsite:extract`, folded client-side. The read
    // now resolves a producer by its declared ownership_id, so there is no
    // fold and no key to show; the guard asserts that absence rather than
    // being deleted.
    const { data, model } = await sourcesFor("archiv3-small");
    const records = producerRecordsFrom(data.producers);
    expect(records.every((record) => !("runtimeKeys" in record))).toBe(true);
    expect(records.every((record) => record.configured)).toBe(true);
    const docsite = records.find((record) => record.name === "docsite");
    expect(docsite?.ownershipId).toBe("archiv3-small.docsite");
    expect(listMarkup(model)).not.toContain("Runtime keys folded into this row");
  });

  it("keys a row by the ownership_id, so two names cannot become two rows", async () => {
    const { data } = await sourcesFor("archiv3-small");
    const records = producerRecordsFrom(data.producers);
    const keys = records.map((record) => record.ownershipId);
    expect(new Set(keys).size).toBe(keys.length);
  });

  it("lists a producer with runs and no configuration last, as not configured", () => {
    const records = producerRecordsFrom(producersReadOf([
      row({ name: "papers", contribution: { records_in_gen: 5, edges_in_gen: 2 } }),
      row({
        name: "orphan",
        configured: false,
        contribution: { records_in_gen: 9, edges_in_gen: 0 },
      }),
    ]));
    expect(records.map((record) => record.name)).toEqual(["papers", "orphan"]);
    expect(records[1].configured).toBe(false);
    // Sorted last, whatever its age.
    const nowMs = Date.parse("2026-09-18T09:00:00.000Z");
    // By `rowKey`, which is what the row carries and what `sortRows` looks
    // a record back up by.
    const byKey = new Map(records.map((record) => [record.rowKey, record]));
    const rows = sortRows(
      [...records].reverse().map((record) => buildSourceRow(record, {
        nowMs, link: designLink("archiv3-small"), mixedKinds: false,
      })),
      DEFAULT_SORT,
      byKey,
    );
    expect(rows.map((row) => row.name)).toEqual(["papers", "orphan"]);
  });
});

describe("Sources' default order", () => {
  it("puts failed, then never run, then running first, then the rest by last run", async () => {
    const { model } = await sourcesFor("workspace-mixed");
    const states = model.rows.map((row) => row.state);
    const firstOf = (state: string) => states.indexOf(state);
    const lastOf = (state: string) => states.lastIndexOf(state);
    expect(firstOf("failed")).toBe(0);
    expect(lastOf("failed")).toBeLessThan(firstOf("never run"));
    expect(lastOf("never run")).toBeLessThan(firstOf("running"));
    expect(lastOf("running")).toBeLessThan(
      Math.min(...["updated", "excluded"].map(firstOf).filter((index) => index >= 0)),
    );
    // Within the failed group, newest last run first.
    const failed = model.rows.filter((row) => row.state === "failed");
    const ages = failed.map((row) => row.lastRunMs as number);
    expect([...ages].sort((a, b) => b - a)).toEqual(ages);
    expect(failed[0].name).toBe("source-27-openspec");
  });

  it("holds every state the fixture has and counts them in the chips", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model } = await sourcesFor(fixture.name);
      const failed = model.stateChips.find((chip) => chip.key === "failed")?.value ?? 0;
      const neverRun = model.stateChips.find((chip) => chip.key === "never run")?.value ?? 0;
      expect(failed, `${fixture.name} failed`).toBe(fixture.expect.failed_producer_count);
      expect(neverRun, `${fixture.name} never run`).toBe(fixture.expect.never_run_producer_count);
      expect(model.total, `${fixture.name} total`).toBe(
        (fixture.expect.source_count as number)
        + (fixture.expect.enricher_count as number)
        + (fixture.expect.view_count as number),
      );
      // No chip carries a zero.
      for (const chip of [...model.kindChips, ...model.stateChips]) {
        expect(chip.value, `${fixture.name} ${chip.label}`).toBeGreaterThan(0);
      }
    }
  });
});

describe("Sources reads its filter off the route", () => {
  it("takes the kind, state and producer Home's links carry", () => {
    expect(parseSourcesFilter({ kind: "enricher" }).kind).toBe("enricher");
    expect(parseSourcesFilter({ state: "never-run" }).state).toBe("never run");
    expect(parseSourcesFilter({ state: "failed" }).state).toBe("failed");
    expect(parseSourcesFilter({ producer: "cmssw_releases" }).name).toBe("cmssw_releases");
  });

  it("drops a kind or state the console does not have", () => {
    expect(parseSourcesFilter({ kind: "pipeline" }).kind).toBeNull();
    expect(parseSourcesFilter({ state: "exploded" }).state).toBeNull();
  });

  it("keeps the filters through the harness's own hash grammar", () => {
    const location = parseDesignLocation("#sources?fixture=workspace-mixed&kind=view&state=failed");
    expect(location.view).toBe("Sources");
    expect(location.params).toEqual({ kind: "view", state: "failed" });
    const detail = parseDesignLocation("#sources/cmssw_releases?fixture=archiv3-small");
    expect(detail.detail).toBe("cmssw_releases");
  });

  it("filters the table to one kind and one state", async () => {
    const views = await sourcesFor("workspace-mixed", { ...NO_SOURCES_FILTER, kind: "view" });
    expect(views.model.rows).toHaveLength(4);
    expect(new Set(views.model.rows.map((row) => row.kind))).toEqual(new Set(["view"]));
    const failedSources = await sourcesFor(
      "workspace-mixed", { kind: "source", state: "failed", name: "" },
    );
    expect(failedSources.model.rows).toHaveLength(4);
    expect(failedSources.model.kindChips.find((chip) => chip.key === "source")?.selected).toBe(true);
    expect(failedSources.model.stateChips.find((chip) => chip.key === "failed")?.selected).toBe(true);
    // A selected chip's href clears its own filter and keeps the other.
    const failedChip = failedSources.model.stateChips.find((chip) => chip.key === "failed");
    expect(failedChip?.href).toContain("kind=source");
    expect(failedChip?.href).not.toContain("state=");
  });

  it("filters by name as the box is typed into", async () => {
    const { model } = await sourcesFor("workspace-mixed", { ...NO_SOURCES_FILTER, name: "slack" });
    expect(model.rows.length).toBeGreaterThan(0);
    expect(model.rows.every((row) => row.name.includes("slack"))).toBe(true);
    const none = await sourcesFor("workspace-mixed", { ...NO_SOURCES_FILTER, name: "no-such" });
    expect(none.model.rows).toHaveLength(0);
    expect(none.model.noMatchSentence).toBe("No producer matches this filter.");
  });
});

describe("Sources' columns sort", () => {
  it("sorts by name, by records and by last run, empty cells last", async () => {
    const byName = await sourcesFor("workspace-mixed", NO_SOURCES_FILTER, { key: "name", direction: "asc" });
    const names = byName.model.rows.map((row) => row.name);
    expect([...names].sort((a, b) => a.localeCompare(b))).toEqual(names);

    const byRecords = await sourcesFor("workspace-mixed", NO_SOURCES_FILTER, { key: "records", direction: "desc" });
    const records = byRecords.model.rows.map((row) => row.records);
    const numbers = records.filter((value): value is number => value !== null);
    expect([...numbers].sort((a, b) => b - a)).toEqual(numbers);
    // The five never-run producers have no records and sort after every number.
    expect(records.slice(-5)).toEqual([null, null, null, null, null]);

    const byRun = await sourcesFor("workspace-mixed", NO_SOURCES_FILTER, { key: "lastRun", direction: "asc" });
    const runs = byRun.model.rows.map((row) => row.lastRunMs);
    const stamps = runs.filter((value): value is number => value !== null);
    expect([...stamps].sort((a, b) => a - b)).toEqual(stamps);
    expect(runs.slice(-5)).toEqual([null, null, null, null, null]);
  });

  it("cycles a header: natural direction, flipped, then the default order", () => {
    const first = nextSort(DEFAULT_SORT, "records");
    expect(first).toEqual({ key: "records", direction: "desc" });
    const second = nextSort(first, "records");
    expect(second).toEqual({ key: "records", direction: "asc" });
    expect(nextSort(second, "records")).toEqual(DEFAULT_SORT);
    expect(nextSort(DEFAULT_SORT, "name")).toEqual({ key: "name", direction: "asc" });
  });
});

describe("Sources' shared-cause line", () => {
  it("counts scale-200's nine failures, without a cause no read records", async () => {
    // Re-aimed: this asserted "9 failed · repository not found (404) on all
    // 9". The cause came from a diagnostics field the live read does not
    // carry, so the count stays and the cause is asserted absent.
    const { model } = await sourcesFor("scale-200");
    expect(model.sharedCause).toBe("9 failed");
    const markup = listMarkup(model);
    expect(markup).toContain("9 failed");
    expect(markup).not.toContain("on all 9");
    expect(markup).not.toContain("repository not found");
  });

  it("prints the count where more than one failed, and nothing for one or none", () => {
    // Re-aimed twice over. This held two rules: print the shared cause only
    // when one cause covers every failure, and print nothing when the causes
    // differ. No read records a cause, so the "one cause" rule has no
    // subject left. What survives is the count, which is the difference
    // between five separate problems and one -- so it now prints on
    // workspace-mixed, where mixed causes used to make it null.
    return (async () => {
      const mixed = await sourcesFor("workspace-mixed");
      expect(mixed.model.rows.filter((row) => row.state === "failed")).toHaveLength(5);
      expect(mixed.model.sharedCause).toBe("5 failed");
      expect(mixed.model.sharedCause).not.toContain("refused the connection");
      const sources = await sourcesFor("workspace-mixed", { ...NO_SOURCES_FILTER, kind: "source" });
      expect(sources.model.sharedCause).toBe("4 failed");
      // One failure says nothing its row does not; none says nothing.
      const one = await sourcesFor("archiv3-small");
      expect(one.model.rows.filter((row) => row.state === "failed")).toHaveLength(1);
      expect(one.model.sharedCause).toBeNull();
      expect(sharedCauseLine([])).toBeNull();
    })();
  });
});

describe("Sources' columns exist only where a row fills them", () => {
  it("has no next-due column anywhere, because no read records one", async () => {
    // Re-aimed. This asserted the column appeared on workspace-mixed, where
    // a cadence was declared, and not on archiv3-small. No read carries a
    // next-due time or a cadence, so the column is cut and the guard holds
    // its absence on both fixtures.
    for (const name of ["archiv3-small", "workspace-mixed"]) {
      const { model } = await sourcesFor(name);
      expect("nextDue" in model.columns).toBe(false);
      expect(model.rows.every((row) => !("nextDue" in row))).toBe(true);
      const markup = listMarkup(model);
      expect(markup).not.toContain("Next due");
      expect(markup).not.toContain("next run due");
    }
  });

  it("tells three runs in one hour apart by their recorded times", () => {
    // The age alone distinguished nothing: lane-d-home-render's
    // deployment_configs ran three times inside 33 seconds and every row
    // read "13 h ago".
    const iso = (s: string) => `2026-09-18T08:${s}.000Z`;
    const record = producerRecordsFrom(producersReadOf([row({ name: "busy" })]))[0];
    const runs = ["00:05", "00:20", "40:00"].map((t2) => ({
      iso: iso(t2), outcome: "ok", items_in: 1, items_out: 2,
      duration_ms: 10, phase: "publish",
    }));
    const detail = buildProducerDetail(record, {
      row: producersReadOf([row({ name: "busy" })]).payload.producers[0],
      runs, runTotal: 3, runTotalUnavailableReason: null, moreRuns: false,
      recordsByType: null, relationshipsByType: null, enricherRunsWindow: null,
    }, { nowMs: Date.parse("2026-09-18T09:00:00.000Z") });
    const whens = detail.runs.map((run) => run.when);
    expect(new Set(whens).size).toBe(3);
    expect(whens).toEqual([
      "2026-09-18 08:00:05 UTC",
      "2026-09-18 08:00:20 UTC",
      "2026-09-18 08:40:00 UTC",
    ]);
    // The age is still there, beside it.
    expect(detail.runs.every((run) => run.whenAge)).toBe(true);
    expect(new Set(detail.runs.map((run) => run.whenAge)).size).toBeLessThan(3);
  });

  it("says that this page's readiness is not the release gate", () => {
    // The console composes only the readiness checks whose results reach this
    // table and declines the rest. That is a fair trade only if the page says
    // so: a reader who took the table for a full readiness verdict would be
    // reading a bounded check as a complete one.
    const withReadiness = (runtime_readiness: unknown): RawProducersRead => {
      const read = producersReadOf([row({ name: "papers" })]);
      return {
        payload: {
          ...read.payload,
          read_authority: { sources: { runtime_readiness } },
        },
      } as RawProducersRead;
    };

    expect(readinessScopeNote(withReadiness({
      basis: "console_bounded_readiness",
      checks_not_run_count: 23,
    }))).toBe(
      "Readiness here is source sync and freshness; 23 further checks run "
      + "only in the full status check on the command line.",
    );

    // One declined check is one check, in words.
    expect(readinessScopeNote(withReadiness({
      basis: "console_bounded_readiness", checks_not_run_count: 1,
    }))).toContain("1 further check runs only in");

    // The full composition declines nothing, so there is nothing to say.
    expect(readinessScopeNote(withReadiness({
      basis: "console_bounded_readiness", checks_not_run_count: 0,
    }))).toBe(null);
    expect(readinessScopeNote(withReadiness({ status: "available" }))).toBe(null);
    expect(readinessScopeNote(producersReadOf([row({ name: "papers" })]))).toBe(null);
  });

  it("says why a blocked producer is blocked, in words the read recorded", () => {
    // The page printed "recorded state blocked" and nothing else. The code
    // that says by WHAT was in the read all along: `_source_convergence_row`
    // decides the state and the reason together.
    const detailOf = (over: Partial<RawProducerRecord>) => {
      const raw = producersReadOf([row({ name: "papers", ...over })]);
      return buildProducerDetail(producerRecordsFrom(raw)[0], {
        row: raw.payload.producers[0],
        runs: [], runTotal: 0, runTotalUnavailableReason: null, moreRuns: false,
        recordsByType: null, relationshipsByType: null, enricherRunsWindow: null,
      }, { nowMs: Date.parse("2026-09-18T09:00:00.000Z") });
    };

    expect(detailOf({
      workflow: {
        state: "blocked", latest_issue: null,
        reason: "completed_scope_watermark_stale",
      },
    }).causeSentence).toBe("its last completed run is older than the runtime's freshness limit");

    expect(detailOf({
      workflow: {
        state: "manual_required", latest_issue: null,
        reason: "manual_or_credential_constraint",
      },
    }).causeSentence).toBe("needs a manual run or a credential to refresh");

    // A fresh producer's reason is the state said twice, so it is not printed.
    expect(detailOf({
      workflow: {
        state: "fresh", latest_issue: null,
        reason: "completed_scope_watermark_fresh",
      },
    }).causeSentence).toBe(null);

    // A code this console has never seen prints as its own words and nothing
    // more. Inventing a sentence for it would assert what no read recorded.
    expect(detailOf({
      workflow: {
        state: "blocked", latest_issue: null, reason: "some_future_code",
      },
    }).causeSentence).toBe("some future code");

    // No code recorded, nothing said.
    expect(detailOf({
      workflow: { state: "blocked", latest_issue: null },
    }).causeSentence).toBe(null);
  });

  it("counts incidents only where the read counted any", () => {
    const detailOf = (incident_count: number | null) => {
      const raw = producersReadOf([row({
        name: "papers",
        health: { status: "degraded", incident_count },
        workflow: {
          state: "blocked", latest_issue: null,
          reason: "completed_scope_watermark_stale",
        },
      })]);
      return buildProducerDetail(producerRecordsFrom(raw)[0], {
        row: raw.payload.producers[0],
        runs: [], runTotal: 0, runTotalUnavailableReason: null, moreRuns: false,
        recordsByType: null, relationshipsByType: null, enricherRunsWindow: null,
      }, { nowMs: Date.parse("2026-09-18T09:00:00.000Z") });
    };

    expect(detailOf(3).incidentCount).toBe(3);
    // "0 incidents counted" is a line that says nothing, and a read that
    // counted none is not the same as a read that counted zero to report.
    expect(detailOf(0).incidentCount).toBe(null);
    expect(detailOf(null).incidentCount).toBe(null);

    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail: detailOf(3), backHref: "#sources", onMoreRuns: null,
        moreError: null, context: CONTEXT,
      }),
    );
    expect(markup).toContain(
      "Recorded cause: its last completed run is older than the runtime&#x27;s freshness limit. 3 incidents counted.",
    );
  });

  it("keeps the sort in the URL, so a reload and a shared link hold it", () => {
    // Sort lived in component state. Reloading the page, or sending the link
    // to someone, threw it away: the reader who sorted by records to find the
    // empty producers got the default order back.
    expect(parseSourcesSort({ sort: "records", dir: "desc" }))
      .toEqual({ key: "records", direction: "desc" });
    expect(sourcesSortParams({ key: "records", direction: "desc" }))
      .toEqual({ sort: "records", dir: "desc" });

    // Round trip, every column the table sorts by.
    for (const key of ["name", "state", "lastRun", "records", "relationships"] as const) {
      for (const direction of ["asc", "desc"] as const) {
        expect(parseSourcesSort(sourcesSortParams({ key, direction })))
          .toEqual({ key, direction });
      }
    }

    // The default order carries no parameters, so a URL says nothing it does
    // not mean.
    expect(sourcesSortParams(DEFAULT_SORT)).toEqual({});

    // A hand-edited URL falls back to the default order rather than breaking
    // the page or refusing to render it.
    expect(parseSourcesSort({ sort: "nextDue", dir: "desc" })).toEqual(DEFAULT_SORT);
    expect(parseSourcesSort({})).toEqual(DEFAULT_SORT);
    // An unknown direction on a known column is ascending, not a refusal.
    expect(parseSourcesSort({ sort: "records", dir: "sideways" }))
      .toEqual({ key: "records", direction: "asc" });
  });

  it("tells a screen reader which column sorts and which one is sorted", async () => {
    const sort: Sort = { key: "records", direction: "desc" };
    const { model } = await sourcesFor("archiv3-small", NO_SOURCES_FILTER, sort);
    const markup = renderToStaticMarkup(
      ConsoleSources({
        model,
        sort,
        onSort: () => {},
        nameQuery: "",
        onNameQuery: () => {},
        pages: 1,
        onMore: () => {},
        moreError: null,
        context: CONTEXT,
      }),
    );
    expect(markup).toContain('data-col="records" aria-sort="descending"');
    // A sortable column that is not the sorted one reads `none`. Omitting the
    // attribute would say the column does not sort at all.
    expect(markup).toContain('data-col="name" aria-sort="none"');
  });

  it("says what window an enricher's run count was taken over", () => {
    // An enricher's runs come from the newest N published checkpoints, not
    // from its whole history. The count alone said "2 runs" of an enricher
    // that may have run fifty times, and an enricher whose last run predates
    // the window read as one that never ran.
    const raw = producersReadOf([row({ name: "code_reference", kind: "enricher" })]);
    const record = producerRecordsFrom(raw)[0];
    const runs = [{
      iso: "2026-09-18T08:00:00.000Z", outcome: "ok", items_in: 1, items_out: 2,
      duration_ms: 10, phase: "publish",
    }];
    const detail = buildProducerDetail(record, {
      row: raw.payload.producers[0],
      runs, runTotal: 1, runTotalUnavailableReason: null, moreRuns: false,
      recordsByType: null, relationshipsByType: null, enricherRunsWindow: 20,
    }, { nowMs: Date.parse("2026-09-18T09:00:00.000Z") });
    expect(detail.runsLine).toBe("1 run \u00b7 newest 20 published versions");

    // A source's runs are queried per source and carry no window, so the page
    // states no bound rather than inventing one.
    const sourceRaw = producersReadOf([row({ name: "papers" })]);
    const sourceDetail = buildProducerDetail(producerRecordsFrom(sourceRaw)[0], {
      row: sourceRaw.payload.producers[0],
      runs, runTotal: 1, runTotalUnavailableReason: null, moreRuns: false,
      recordsByType: null, relationshipsByType: null, enricherRunsWindow: 20,
    }, { nowMs: Date.parse("2026-09-18T09:00:00.000Z") });
    expect(sourceDetail.runsLine).toBe("1 run");
  });

  it("prints a recorded 0 items in as 0, and an unrecorded one as not recorded", async () => {
    // lane-d-home-render's runs really do record 0 items in and 2,806 out: a
    // source that read nothing new and emitted its whole extent. That 0 is a
    // measurement and prints. A run whose input_count the read did not carry
    // is a different fact, and printing 0 for it would report a measurement
    // nobody made -- so the cell stays empty.
    const record = producerRecordsFrom(producersReadOf([row({ name: "busy" })]))[0];
    const runs = [
      { iso: "2026-09-18T08:00:00.000Z", outcome: "ok", items_in: 0, items_out: 2806,
        duration_ms: 10, phase: "publish" },
      { iso: "2026-09-18T07:00:00.000Z", outcome: "ok", items_in: null, items_out: null,
        duration_ms: null, phase: "publish" },
    ];
    const detail = buildProducerDetail(record, {
      row: producersReadOf([row({ name: "busy" })]).payload.producers[0],
      runs, runTotal: 2, runTotalUnavailableReason: null, moreRuns: false,
      recordsByType: null, relationshipsByType: null, enricherRunsWindow: null,
    }, { nowMs: Date.parse("2026-09-18T09:00:00.000Z") });
    // Read is "not recorded" where the read carries no count -- never a
    // blank, never a zero (review 2026-09-23).
    expect(detail.runs.map((run) => [run.read, run.wrote, run.duration]))
      .toEqual([["0", "2,806", "10 ms"], ["not recorded", "", ""]]);
    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail, backHref: "#sources", onMoreRuns: null, moreError: null, context: CONTEXT,
      }),
    );
    expect(markup).toContain('<td data-col="itemsin" class="okg-console__rtable-num">0</td>');
    expect(markup).toContain('<td data-col="itemsin" class="okg-console__rtable-num">not recorded</td>');
  });

  it("says never run in words where no read recorded a run", async () => {
    // A blank cell makes a reader guess. The words come from the recorded
    // absence of a latest run, not from a default.
    const records = producerRecordsFrom(producersReadOf([
      row({ name: "ran", latest_run: { iso: "2026-09-18T08:00:00.000Z", outcome: "ok",
        items_in: 1, items_out: 1, duration_ms: 5, phase: "publish" } }),
      row({ name: "never", latest_run: null }),
    ]));
    const nowMs = Date.parse("2026-09-18T09:00:00.000Z");
    const rows = records.map((record) => buildSourceRow(record, {
      nowMs, link: designLink("archiv3-small"), mixedKinds: false,
    }));
    expect(rows[0].lastRun).toBe("1 h ago");
    expect(rows[1].lastRun).toBe("never run");
    expect(rows[1].lastRun).not.toBe("");
  });

  it("draws an unrecorded state muted, and a recorded one as a warning", () => {
    // Nothing is wrong with a producer whose state no read recorded: the
    // deployment has no runtime. Amber there reports a problem the reads
    // never found. "Never run", "Excluded" and "Withheld" ARE recorded and
    // keep the warning colour.
    expect(producerGlyph(UNDETERMINED_STATE).severity).toBe("unrecorded");
    expect(producerGlyph(UNDETERMINED_STATE).word).toBe("State not recorded");
    for (const recorded of ["never run", "excluded", "withheld"]) {
      expect(producerGlyph(recorded).severity, recorded).toBe("never-run");
    }
    expect(producerGlyph("failed").severity).toBe("failed");
    expect(ATTENTION_SEVERITIES.has("unrecorded")).toBe(false);
  });

  it("prints the runtime's freshness state in words, never blank", async () => {
    const { model } = await sourcesFor("archiv3-small");
    expect(model.columns.freshness).toBe(true);
    expect(model.rows.every((row) => row.freshness !== "")).toBe(true);
    // The literal state, spelled out: the read records `not_evaluated`.
    expect(model.rows.every((row) => row.freshness === "not evaluated")).toBe(true);
    expect(listMarkup(model)).toContain("not evaluated");
  });

  it("has no error column and no contribution columns on a deployment that has run nothing", async () => {
    const { model } = await sourcesFor("empty");
    expect(model.columns).toEqual({ contribution: false, freshness: true });
    expect(model.rows).toHaveLength(3);
    expect(model.rows.every((row) => row.state === "never run")).toBe(true);
    // Re-aimed: the cell was blank for a producer with no recorded run, and
    // a blank made the reader guess. It says so in words now.
    expect(model.rows.every((row) => row.lastRun === "never run")).toBe(true);
    const markup = listMarkup(model);
    // Re-aimed: "Last error" was asserted absent here because no row had
    // failed. It is now absent everywhere, and so is the next-due line.
    expect(markup).not.toContain("Last error");
    expect(markup).not.toContain("Relationships");
    expect(markup).not.toContain("next run due");
  });

  it("prints a contribution of zero for a producer that ran and added nothing", async () => {
    const { model } = await sourcesFor("workspace-mixed");
    const row = model.rows.find((candidate) => candidate.name === "source-12-openspec");
    expect(row?.records).toBe(0);
    const html = listMarkup(model);
    expect(html).toContain('<td data-col="records">0</td>');
  });

  it("names the kind on a row only where the table mixes kinds", async () => {
    const mixed = await sourcesFor("workspace-mixed");
    expect(mixed.model.rows.find((row) => row.name === "daily-brief")?.kindWord).toBe("view");
    expect(mixed.model.rows.find((row) => row.name === "source-01-slack")?.kindWord).toBe("");
    const only = await sourcesFor("scale-200");
    expect(only.model.rows.every((row) => row.kindWord === "")).toBe(true);
    expect(only.model.kindChips).toHaveLength(0);
    expect(only.model.headerCount).toBe("220 sources");
    // 38, not 37: workspace-mixed now carries one producer nothing
    // configures, so the fixtures cover the `configured: false` page.
    expect(mixed.model.headerCount).toBe("38 producers");
  });
});

describe("Sources pages at 100", () => {
  it("shows the first hundred of scale-200 and says so", async () => {
    const { model } = await sourcesFor("scale-200");
    expect(model.rows).toHaveLength(220);
    expect(pageOf(model.rows, 1)).toHaveLength(100);
    expect(pageOf(model.rows, 3)).toHaveLength(220);
    const markup = listMarkup(model);
    expect(markup).toContain("100 of 220 producers");
    expect(markup).toContain("Load 100 more");
    expect(listMarkup(model, 3)).not.toContain("Load 100 more");
  });

  it("folds a long name in the middle and keeps its tail", () => {
    const long = "github-code-repositories-mitdbg-okg-substrate-and-operator-console-mirror";
    const folded = foldName(long);
    expect(folded.length).toBeLessThanOrEqual(40);
    expect(folded.endsWith("console-mirror")).toBe(true);
    expect(folded).toContain("…");
    expect(foldName("papers")).toBe("papers");
  });
});

describe("A producer's page", () => {
  it("shows archiv3-small's failed source with its runs, and no field no read records", async () => {
    // Re-aimed. This asserted the record types, the recorded error text, its
    // code, its cause sentence, the configuration YAML with a redacted
    // credential, and the folded runtime keys. None of those is recorded by
    // any read, so each assertion is now one of absence, and what the reads
    // DO record is still asserted exactly as before.
    const { detail } = await detailFor("archiv3-small", "cmssw_releases");
    expect(detail.kind).toBe("source");
    expect(detail.mark.word).toBe("Failed");
    expect(detail.figures).toEqual([
      { value: 2414, label: "records" }, { value: 2100, label: "relationships" },
    ]);
    expect(detail.runs).toHaveLength(2);
    expect(detail.runs[0].mark.word).toBe("Failed");
    expect(detail.runs[0].phase).toBe("fetch");
    expect(detail.runsLine).toBe("2 runs · 1 failed");
    // Re-aimed. This asserted the "Open in Updates" link resolved to the
    // attempt this producer stopped. console/generations sets
    // `stopped_producer` to null on every attempt it returns, because no
    // checkpoint column records it (issue #2120), so the link could never be
    // drawn live and is cut. The guard now holds its absence.
    expect("stoppedAttemptHref" in detail).toBe(false);
    expect("stoppedAttemptLabel" in detail).toBe(false);
    for (const absent of [
      "recordTypes", "errorText", "errorCode", "errorSentence", "errorLineCount",
      "configuration", "runtimeKeys", "adapter", "scope", "cadence",
    ]) {
      expect(absent in detail).toBe(false);
    }
    expect(detail.runs.every((run) => !("errorText" in run))).toBe(true);
    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail, backHref: "#sources", onMoreRuns: null, moreError: null, context: CONTEXT,
      }),
    );
    // The page must not claim a redaction it no longer performs, nor show an
    // entry it does not have.
    expect(markup).not.toContain("credentials are redacted");
    expect(markup).not.toContain(REDACTED);
    expect(markup).not.toContain('data-testid="sources-detail-configuration"');
    expect(markup).not.toContain('data-testid="sources-detail-yaml"');
    expect(markup).not.toContain('data-testid="sources-detail-error"');
    expect(markup).not.toContain('data-testid="sources-detail-settings"');
    expect(markup).not.toContain("Runtime keys folded into this row");
    expect(markup).not.toContain("Open in Updates");
  });

  it("shows the withheld enricher's issue and the view read's own sentence", async () => {
    const enricher = await detailFor("workspace-mixed", "llm-summarizer");
    expect(enricher.detail.kind).toBe("enricher");
    expect(enricher.detail.reasonTitle).toBe("Withheld");
    expect(enricher.detail.reasonText).toBe(
      "withheld: the deployment's policy does not permit sending this source's text to an external model",
    );
    // Re-aimed: the failed view's build error was asserted here. No read
    // records a run message, so the page carries none and the reason -- which
    // IS recorded -- is what remains.
    expect("errorText" in enricher.detail).toBe(false);
    const view = await detailFor("workspace-mixed", "contributor-graph");
    expect(view.detail.kind).toBe("view");
    expect(view.detail.figures).toEqual([{ value: 0, label: "rows built" }]);
    expect("errorText" in view.detail).toBe(false);
    // A view DOES record an issue, and this page was showing none because the
    // producers composer hard-nulled it under a reason claiming the view read
    // records none (`console_views/producers.py`). It records one of three
    // fixed sentences -- never the run's exception text, which is why
    // `errorText` is still absent.
    expect(view.detail.reasonText).toBe("Latest materialization failed.");
  });

  it("draws a configured producer's records by type, summing to its own total", async () => {
    // The harness read used to omit records_by_type, so every configured
    // producer page in the design harness said the producer holds no record
    // while the Contribution figure above it showed thousands. The fixture
    // now carries the split of the producer's own total.
    const { detail } = await detailFor("archiv3-small", "cmssw_releases");
    expect(detail.recordTypesUnavailable).toBeNull();
    expect(detail.recordTypeRows.map((row) => row.type)).toEqual(["cmssw_release"]);
    expect(detail.recordTypeRows.reduce((sum, row) => sum + row.records, 0))
      .toBe(detail.figures[0].value);
    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail, backHref: "#sources", onMoreRuns: null, moreError: null, context: CONTEXT,
      }),
    );
    // The claim this test was written for is about the READ: the harness used
    // to omit records_by_type, so the page said the producer holds no record
    // while Contribution showed thousands. The split above proves the read
    // carries it. Whether a ONE-row split earns a table is review L13's
    // question, answered in the test below -- so this asserts the false
    // sentence is gone, not that a table is drawn.
    expect(markup).not.toContain("This producer holds no record in the selected version.");
    // CUT (review L13). The sentence explained a rollup dimension that does
    // not exist -- plumbing, printed on every producer page whatever the
    // read said. An operator cannot act on it and it was true forever, so it
    // is not news.
    expect(markup).not.toContain("Relationships by type are not recorded");
  });

  it("shows a one-row records-by-type table, and a two-row one", async () => {
    // Review L13: one row restates the Contribution figure directly above it,
    // in a table whose share column can only read "100%". The section earns
    // its space only when it splits the total.
    // Reversed by the Archiv3 review (2026-09-22): with the table dropped a
    // single-type source never named its type anywhere on its page. The
    // one row is shown.
    const one = await detailFor("archiv3-small", "cmssw_releases");
    expect(one.detail.recordTypeRows).toHaveLength(1);
    expect(one.detail.showRecordTypes).toBe(true);
    const oneMarkup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail: one.detail, backHref: "#sources", onMoreRuns: null,
        moreError: null, context: CONTEXT,
      }),
    );
    expect(oneMarkup).toContain('data-testid="record-types-table"');
    expect(oneMarkup).toContain(one.detail.recordTypeRows[0].type);

    // Two rows: the split is the point, so the table stays.
    const two = {
      ...one.detail,
      recordTypeRows: [
        { type: "cmssw_release", records: 60, share: 60, shareLabel: "60%", href: null },
        { type: "cmssw_tag", records: 40, share: 40, shareLabel: "40%", href: null },
      ],
      showRecordTypes: true,
    };
    const twoMarkup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail: two, backHref: "#sources", onMoreRuns: null,
        moreError: null, context: CONTEXT,
      }),
    );
    expect(twoMarkup).toContain('data-testid="record-types-table"');
    expect(twoMarkup).toContain("cmssw_tag");
  });

  it("shows a never-run source with no figures and no runs", async () => {
    const { detail } = await detailFor("empty", "papers");
    expect(detail.figures).toEqual([]);
    expect(detail.runs).toEqual([]);
    expect(detail.runsLine).toBe("No run is recorded.");
    expect(detail.meta).toContain("never run");
    // Re-aimed: the meta line used to carry "next due in 9 h". Nothing
    // records a next-due time, so the line carries the runtime's freshness
    // state instead, and no due phrase at all.
    expect(detail.meta.some((entry) => entry.startsWith("next due"))).toBe(false);
    expect(detail.meta.some((entry) => entry.startsWith("freshness "))).toBe(true);
  });

  it("never shows a credential-shaped key or value on any producer page", async () => {
    // Re-aimed, and stronger. This walked each page's configuration YAML and
    // asserted every credential-shaped key was redacted in it. There is no
    // YAML now: the registry entry is cut. So the guard asserts the whole
    // rendered page carries no credential-shaped value at all, which is what
    // the old test was protecting and which still fails if anyone puts the
    // entry back without server-side redaction.
    for (const fixture of DESIGN_FIXTURES) {
      const client = createFixtureApiClient(fixture);
      const data = await loadConsoleSources(client);
      for (const record of producerRecordsFrom(data.producers)) {
        let read;
        try {
          read = await loadProducerDetail(client, record.name);
        } catch (error) {
          // A producer this fixture defines no detail read for.
          if (error instanceof FixtureReadMissing) continue;
          if (error instanceof Error && error.message.includes("found no producer")) continue;
          throw error;
        }
        const detail = buildProducerDetail(record, read, {
          nowMs: Date.parse(fixture.clock),
        });
        expect("configuration" in detail).toBe(false);
        const markup = renderToStaticMarkup(
          ConsoleProducerDetail({
            detail, backHref: "#sources", onMoreRuns: null, moreError: null, context: CONTEXT,
          }),
        );
        const where = `${fixture.name} ${record.name}`;
        expect(CREDENTIAL_VALUE.test(markup), where).toBe(false);
        expect(markup.includes("hunter2"), where).toBe(false);
        expect(markup.includes(REDACTED), where).toBe(false);
        for (const line of markup.split(/[<>]/)) {
          const match = /^\s*([A-Za-z_][A-Za-z0-9_]*):\s*(.+)$/.exec(line);
          if (match && isCredentialKey(match[1])) {
            throw new Error(`${where} printed a credential-shaped key: ${line}`);
          }
        }
      }
    }
  });

  it("shows no part of workspace-mixed's DSN, redacted or not", async () => {
    // Re-aimed. This asserted the DSN's password was replaced in place and
    // the host printed beside it. The page prints no settings at all now, so
    // neither the secret NOR the host reaches it -- which is the safer answer
    // and the one to hold until a redacting read exists.
    const { detail } = await detailFor("workspace-mixed", "source-05-docs");
    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail, backHref: "#sources", onMoreRuns: null, moreError: null, context: CONTEXT,
      }),
    );
    expect(markup).not.toContain("hunter2");
    expect(markup).not.toContain("db.workspace.invalid");
    expect(markup).not.toContain(REDACTED);
    expect(markup).not.toContain("repo_path");
  });

  it("refuses a producer the read answers for without selecting it", async () => {
    // Re-aimed. The three old detail routes had no bare form, so a missing
    // fixture read raised FixtureReadMissing. console/producers has one: the
    // fixture client falls back from `?name=` to the bare route, which is the
    // LIST read and selects nobody. The loader must refuse that rather than
    // build a page with no runs, which would read as a producer that never
    // ran -- so the guard now holds the refusal and its message.
    const fixture = fixtureByName("scale-200");
    if (!fixture) throw new Error("no fixture");
    await expect(
      loadProducerDetail(createFixtureApiClient(fixture), "code-repo-050"),
    ).rejects.toThrow(/found no producer named code-repo-050/);
  });
});

describe("Sources' copy", () => {
  it("never says serves, never prints a raw code on the list, and never prints a zero unit", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model } = await sourcesFor(fixture.name);
      // The recorded first line, verbatim, is carried in a cell's title
      // tooltip by design; what is printed is the sentence without its code.
      const html = listMarkup(model).replace(/ title="[^"]*"/g, "");
      expect(html, fixture.name).not.toMatch(/\bserves\b/);
      expect(html, fixture.name).not.toMatch(/[a-z_]+_failed:/);
      expect(html, fixture.name).not.toMatch(/\b0 (s|m|h)\b/);
      // Every state colour on the page has a word beside it.
      expect(html, fixture.name).not.toMatch(/okg-console__status"[^>]*>(?:(?!status-word).)*<\/span><\/span>/);
      expect(html, fixture.name).toMatch(/[Aa]s of 2026-09-18 09:00 UTC/);
    }
  });

  it("mounts the same record in every fixture's Sources view without a network read", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model } = await sourcesFor(fixture.name);
      expect(model.total, fixture.name).toBeGreaterThan(0);
      const records: ProducerRecord[] = producerRecordsFrom(
        (await loadConsoleSources(createFixtureApiClient(fixture))).producers,
      );
      expect(records.length, fixture.name).toBe(model.total);
    }
  });
});

describe("A read that came back short says so", () => {
  // The producers read composes three diagnostics reads. When the enricher
  // or the view composer raises, it records the failure and returns the rows
  // it has. Nothing on the page read that, so a table missing every enricher
  // was indistinguishable from a deployment that configures none.
  function shortRead(): RawProducersRead {
    const read = producersReadOf([row({ name: "papers" })]);
    read.payload.read_authority = {
      generation_id: null,
      sources: { status: "available" },
      enrichers: {
        status: "unavailable",
        reason: "enricher_diagnostics_composer_failed",
        error_type: "UndefinedTable",
        error: 'relation "okg.enricher_runs" does not exist',
      },
      views: { status: "available" },
    };
    read.payload.omitted_data = [
      { authority: "enricher_diagnostics", kind: "enricher" },
    ];
    return read;
  }

  function modelFor(read: RawProducersRead) {
    const { data } = { data: null } as never;
    void data;
    return buildSourcesModel(
      {
        health: { payload: { deployment: "x", generation_id: null } },
        producers: read,
        attempts: { payload: { now: "2026-09-18T09:00:00.000Z" } },
      } as never,
      NO_SOURCES_FILTER,
      designLink("x"),
      DEFAULT_SORT,
    );
  }

  it("names the enrichers the read could not list, inline on the page", () => {
    const model = modelFor(shortRead());
    expect(model.partialReads).toHaveLength(1);
    const markup = listMarkup(model);
    expect(markup).toContain('data-testid="sources-partial-read"');
    expect(markup).toContain("No enrichers are listed");
    expect(markup).toContain("enricher diagnostics composer failed");
  });

  it("serves no exception class and no exception text", () => {
    const markup = listMarkup(modelFor(shortRead()));
    expect(markup).not.toContain("UndefinedTable");
    expect(markup).not.toContain("okg.enricher_runs");
  });

  it("says nothing where the read answered in full", async () => {
    const { model } = await sourcesFor("archiv3-small");
    expect(model.partialReads).toEqual([]);
    expect(listMarkup(model)).not.toContain('data-testid="sources-partial-read"');
  });

  it("still says it when every producer the read could list is gone", () => {
    const read = shortRead();
    read.payload.producers = [];
    read.payload.producer_count = 0;
    const markup = listMarkup(modelFor(read));
    // "No producer is configured" alone would be a false empty state.
    expect(markup).toContain("No enrichers are listed");
  });

  // A sub-read of the sources composer can fail without any producer going
  // missing. When the runtime readiness read fails, every source is still
  // listed -- each one with `not_evaluated` freshness and a null state, which
  // the page prints as "not evaluated" and "State not recorded". Those are
  // the same words a healthy deployment whose runtime supplies no watermark
  // gets, so without a sentence the two are indistinguishable.
  function runtimeDownRead(): RawProducersRead {
    const read = producersReadOf([row({ name: "papers" })]);
    read.payload.read_authority = {
      generation_id: null,
      sources: {
        runtime_readiness: {
          status: "unavailable",
          reason: "runtime_readiness_composer_failed",
          error_type: "OperationalError",
        },
      },
    };
    read.payload.omitted_data = [{
      authority: "runtime_readiness",
      reason: "runtime_readiness_composer_failed",
      affects: "source_runtime_state",
    }];
    return read;
  }

  it("says a failed runtime read failed, not that no state is recorded", () => {
    const model = modelFor(runtimeDownRead());
    expect(model.partialReads).toHaveLength(1);
    const sentence = model.partialReads[0];
    // Never the kind-level sentence: no producer went missing.
    expect(sentence).not.toContain("No sources are listed");
    expect(sentence).not.toContain("No producers are listed");
    expect(sentence).toContain("runtime readiness");
    expect(sentence).toContain("State not recorded");
    const markup = listMarkup(model);
    expect(markup).toContain('data-testid="sources-partial-read"');
    // The row is still drawn, and its state word is still there.
    expect(markup).toContain("papers");
  });

  it("serves no exception class for a failed runtime read either", () => {
    const markup = listMarkup(modelFor(runtimeDownRead()));
    expect(markup).not.toContain("OperationalError");
  });

  it("names a failed last-run query as its own read", () => {
    const read = producersReadOf([row({ name: "papers" })]);
    read.payload.read_authority = {
      sources: {
        source_run_diagnostics: {
          status: "unavailable",
          reason: "source_run_diagnostics_query_failed",
        },
      },
    };
    read.payload.omitted_data = [{
      authority: "source_run_diagnostics",
      reason: "source_run_diagnostics_query_failed",
      affects: "source_latest_run",
    }];
    const sentence = modelFor(read).partialReads[0];
    expect(sentence).toContain("never run");
    expect(sentence).not.toContain("No sources are listed");
  });
});

describe("A producer with no configuration says what was not read", () => {
  // No detail read is asked for such a producer, so its runs and its record
  // types were never looked for. The page used to report both as absences.
  async function unconfiguredDetail() {
    const { data, model } = await sourcesFor("workspace-mixed");
    const record = producerRecordsFrom(data.producers)
      .find((candidate: ProducerRecord) => !candidate.configured);
    if (!record) throw new Error("workspace-mixed has no unconfigured producer");
    const detail = buildProducerDetail(record, null, { nowMs: model.nowMs });
    return { record, detail };
  }

  it("has a fixture with one", async () => {
    const { record } = await unconfiguredDetail();
    expect(record.configured).toBe(false);
  });

  it("does not report the record types as absent", async () => {
    const { detail } = await unconfiguredDetail();
    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail, backHref: "#sources", onMoreRuns: null, moreError: null, context: CONTEXT,
      }),
    );
    expect(markup).not.toContain("This producer holds no record in the selected version.");
    expect(markup).toContain("No detail read was made for this producer");
  });

  it("does not report the run history as absent", async () => {
    const { detail } = await unconfiguredDetail();
    expect(detail.detailRead).toBe(false);
    expect(detail.runsLine).not.toBe("No run is recorded.");
    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail, backHref: "#sources", onMoreRuns: null, moreError: null, context: CONTEXT,
      }),
    );
    expect(markup).toContain('data-testid="runs-not-read"');
    expect(markup).not.toContain("No run is recorded.");
  });

  it("does not claim the producer never ran", async () => {
    const { detail } = await unconfiguredDetail();
    expect(detail.figuresNote).not.toContain("has not run");
  });

  it("leaves a configured producer's page saying what the read found", async () => {
    const { detail } = await detailFor("workspace-mixed", "source-03-docs");
    expect(detail.detailRead).toBe(true);
    expect(detail.runsNote).toBeNull();
  });
});

describe("A figure the read did not record is absent, not zero", () => {
  // `edges_in_gen ?? 0` printed "0 relationships" for a producer whose
  // relationship count the read did not carry. A real zero and an unrecorded
  // figure are different facts and the page must not spell them the same way.
  function detailOf(over: Partial<RawProducerRecord["contribution"]>) {
    const record = producerRecordsFrom(producersReadOf([
      row({
        name: "papers",
        latest_run: {
          iso: "2026-09-18T06:00:00.000Z", outcome: "ok",
          items_in: 1, items_out: 1, duration_ms: 10, phase: "publish",
        },
        contribution: { records_in_gen: 12, edges_in_gen: 0, ...over },
      }),
    ]))[0];
    return buildProducerDetail(record, null, { nowMs: CONTEXT.nowMs });
  }

  it("prints a recorded zero", () => {
    expect(detailOf({}).figures).toEqual([
      { value: 12, label: "records" },
      { value: 0, label: "relationships" },
    ]);
  });

  it("prints nothing where the read carried no relationship count", () => {
    const detail = detailOf({ edges_in_gen: null });
    expect(detail.figures).toEqual([{ value: 12, label: "records" }]);
    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail, backHref: "#", onMoreRuns: null, moreError: null, context: CONTEXT,
      }),
    );
    expect(markup).not.toContain("relationships");
  });
});

describe("A producer's recorded issue, in every state that records one", () => {
  // `withheld_reason` and `excluded_reason` were null on every source of
  // every deployment: nothing in the runtime readiness path records a policy
  // reason. What IS recorded is the enricher read's `latest_issue`, in three
  // states and not one.
  function detailWith(over: Partial<RawProducerRecord>) {
    const record = producerRecordsFrom(producersReadOf([
      row({ name: "llm-summarizer", kind: "enricher", ...over }),
    ]))[0];
    return buildProducerDetail(record, null, { nowMs: CONTEXT.nowMs });
  }

  it("titles a withheld producer's issue Withheld", () => {
    const detail = detailWith({
      workflow: { state: "withheld", latest_issue: "withheld:input_source_held:papers" },
    });
    expect(detail.reasonTitle).toBe("Withheld");
    expect(detail.reasonText).toBe("withheld:input_source_held:papers");
  });

  it("carries a failing producer's issue too, under its own title", () => {
    const detail = detailWith({
      workflow: { state: "failing", latest_issue: "failed:IncompleteInputs" },
    });
    expect(detail.reasonTitle).toBe("Latest issue");
    expect(detail.reasonText).toBe("failed:IncompleteInputs");
  });

  it("says nothing where no read records an issue", () => {
    const detail = detailWith({
      workflow: {
        state: "updated",
        latest_issue: null,
        latest_issue_unavailable_reason: "enricher_records_no_issue_in_this_state",
      },
    });
    expect(detail.reasonTitle).toBeNull();
    expect(detail.reasonText).toBeNull();
    const markup = renderToStaticMarkup(
      ConsoleProducerDetail({
        detail, backHref: "#", onMoreRuns: null, moreError: null, context: CONTEXT,
      }),
    );
    expect(markup).not.toContain('data-testid="sources-detail-reason"');
  });
});

describe("two recorded counts that do not agree", () => {
  // Review 4, finding 1. The read used to file this in `omitted_data`, which
  // the view renders one way and one way only: "the read behind this table
  // came back short ... every producer is still listed ... what the rows show
  // in their place is this failed read". Every clause of that is false of a
  // disagreement. Nothing came back short; both figures were read in full.
  const disagreeing = (): RawProducersRead => {
    const read = producersReadOf([
      row({ name: "code_reference", kind: "enricher", ownership_id: "_enricher:code_reference" }),
    ]);
    return {
      payload: {
        ...read.payload,
        count_disagreements: [{
          authority: "console_count_rollup",
          reason: "source_dimension_exceeds_enricher_dimension",
          ownership_id: "_enricher:code_reference",
          name: "code_reference",
          kind: "enricher",
          source_dimension_records: 412,
          enricher_dimension_records: 400,
        }],
      },
    } as RawProducersRead;
  };

  it("is one sentence stating both figures, not a partial read", () => {
    const record = producerRecordsFrom(disagreeing())[0];
    expect(record.countDisagreement).toBe(
      "The rollup counts 412 records under this enricher's source name and "
      + "400 under its enricher dimension; the two are not reconciled, and "
      + "neither is shown in place of the other.",
    );
    // The words the partial-read sentence is made of, which would be untrue
    // here, are absent.
    expect(record.countDisagreement).not.toContain("came back short");
    expect(record.countDisagreement).not.toContain("failed read");
  });

  it("says nothing where the reads agree, or where the key is absent", () => {
    const plain = producersReadOf([row({ name: "papers" })]);
    expect(producerRecordsFrom(plain)[0].countDisagreement).toBe(null);
  });

  it("prints on the producer page, beside the figure it is about", () => {
    const record = producerRecordsFrom(disagreeing())[0];
    const detail = buildProducerDetail(record, null, { nowMs: Date.parse("2026-09-22T05:36:00Z") });
    expect(detail.countDisagreement).toContain("412");
    expect(detail.countDisagreement).toContain("400");
    const markup = renderToStaticMarkup(
      <ConsoleProducerDetail
        detail={detail}
        backHref="#"
        onMoreRuns={null}
        moreError={null}
        context={{ deployment: "x", version: null, publishedAt: null, nowMs: 0 } as PageContext}
      />,
    );
    expect(markup).toContain('data-testid="sources-detail-count-disagreement"');
    expect(markup).toContain("are not reconciled");
    expect(markup).not.toContain("came back short");
  });
});

describe("a sort parameter this table does not sort by", () => {
  // Review 4, finding 9. The substitution is deliberate -- a hand-edited URL
  // must not break the page -- and doing it silently was not: the rows came
  // back in the default order with nothing on the page to say the link had
  // asked for something else.
  it("is named, with its value, and the default order is stated", () => {
    const note = ignoredSortParam({ sort: "bogus", dir: "asc" });
    expect(note).toContain("sort=bogus");
    expect(note).toContain("default order");
    expect(note).toContain("name, state, lastRun, records, relationships");
    // And the table really is in the default order.
    expect(parseSourcesSort({ sort: "bogus", dir: "asc" })).toEqual(DEFAULT_SORT);
  });

  it("names an unusable direction beside a column it can sort by", () => {
    expect(ignoredSortParam({ sort: "records", dir: "sideways" }))
      .toContain("dir=sideways");
    // Not beside an unusable column: with no column there is no direction to
    // have weighed, and naming it would say the page looked at one.
    expect(ignoredSortParam({ sort: "bogus", dir: "sideways" }))
      .not.toContain("dir=");
  });

  it("says nothing about parameters it used, or about none at all", () => {
    expect(ignoredSortParam({})).toBe(null);
    expect(ignoredSortParam({ sort: "records", dir: "desc" })).toBe(null);
    // A direction with no column asks nothing of an unsorted table.
    expect(ignoredSortParam({ dir: "desc" })).toBe(null);
  });

  it("reaches the page", () => {
    const markup = renderToStaticMarkup(
      <ConsoleSources
        model={buildSourcesModel(
          {
            health: { payload: { deployment: "x", generation_id: null } },
            producers: producersReadOf([row({ name: "papers" })]),
            attempts: { payload: { now: "2026-09-22T05:36:00.000Z" } },
          } as never,
          NO_SOURCES_FILTER, designLink("x"), DEFAULT_SORT,
        )}
        sort={DEFAULT_SORT}
        onSort={() => {}}
        nameQuery=""
        onNameQuery={() => {}}
        pages={1}
        onMore={() => {}}
        moreError={null}
        context={{ deployment: "x", version: null, publishedAt: null, nowMs: 0 } as PageContext}
        ignoredSort={ignoredSortParam({ sort: "bogus" })}
      />,
    );
    expect(markup).toContain('data-testid="sources-ignored-sort"');
    expect(markup).toContain("sort=bogus");
  });
});
