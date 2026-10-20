// Home's composition, asserted against the six fixtures rather than against a
// shape written here.
//
// The rendered captures are what the operator approves; these tests hold the
// things a screenshot cannot: that the counts are the fixture's own numbers,
// that every list is bounded and says what it omitted, that the empty and
// all-clear sentences carry facts, and that the chart's buckets sum to the
// totals the read declares. A mock that drifts from its fixture fails here
// instead of being approved on a lie.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { createFixtureApiClient, FixtureReadMissing } from "@operator-ui/design-harness/fixtureClient";
import { DESIGN_FIXTURES, fixtureByName } from "@operator-ui/design-harness/fixtures";
import { designLink } from "@operator-ui/design-harness/DesignHarness";
import { RANGES } from "@operator-ui/console/api/client";
import { loadConsoleHome, attemptsFor, VERSIONS_ROUTE } from "@operator-ui/console/api/health";
import { ApiRequestError } from "@operator-ui/shared/api/apiClient";
import {
  namedReadFailure, readFailureLine,
} from "@operator-ui/console/model/shared/readFailure";
import { ConsoleHome } from "@operator-ui/console/views/Home";
import { versionControlLabel, versionOptionLabel } from "@operator-ui/console/views/TopBar";
import { checkCause, producerCause, NEEDS_ATTENTION_CAP } from "@operator-ui/console/model/attention";
import { collectProducers } from "@operator-ui/console/model/home";
import { producerCounts } from "@operator-ui/console/model/counts";
import { buildHomeModel, type HomeModel } from "@operator-ui/console/model/home";
import { medianPublishedDuration } from "@operator-ui/console/model/pace";
import { namesDatabaseObject, plainCause } from "@operator-ui/console/model/shared/format";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import { duration } from "@operator-ui/console/model/shared/time";
import { buildMetricSeries } from "@operator-ui/console/model/metrics/series";

async function modelFor(name: string, range = "14d") {
  const fixture = fixtureByName(name);
  if (!fixture) throw new Error(`no fixture named ${name}`);
  const data = await loadConsoleHome(createFixtureApiClient(fixture));
  const inputs = {
    ...data.inputs,
    attempts: attemptsFor(data, range as (typeof RANGES)[number]),
  };
  return {
    fixture,
    data,
    inputs,
    link: designLink(name),
    // The producer count strip, which only the Sources block renders and only
    // where one good state does NOT cover every producer. Asserted from the
    // function rather than from a field on the model: the model used to carry
    // a `counts` block no view read.
    counts: producerCounts(collectProducers(inputs), designLink(name)),
    model: buildHomeModel(inputs, designLink(name)),
  };
}

function markup(model: HomeModel, total: number): string {
  return renderToStaticMarkup(
    ConsoleHome({
      model,
      total,
      range: "14d",
      onRange: () => {},
      link: designLink("archiv3-small"),
    }),
  );
}

describe("Home's count strip", () => {
  it("counts producers by kind and by state from the fixture's own rows", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { counts } = await modelFor(fixture.name);
      // A state with nothing in it is not printed, so an absent count is a
      // zero -- and the assertion has to say so rather than read `undefined`.
      const value = (...labels: string[]) => counts
        .find((count) => labels.includes(count.label))?.value ?? 0;
      expect(value("sources", "source"), `${fixture.name} sources`)
        .toBe(fixture.expect.source_count);
      expect(value("enrichers", "enricher"), `${fixture.name} enrichers`)
        .toBe(fixture.expect.enricher_count);
      expect(value("views", "view"), `${fixture.name} views`).toBe(fixture.expect.view_count);
      expect(value("failed"), `${fixture.name} failed`)
        .toBe(fixture.expect.failed_producer_count);
      expect(value("never run"), `${fixture.name} never run`)
        .toBe(fixture.expect.never_run_producer_count);
    }
  });

  it("accounts for every configured producer, not only the three healthy states", async () => {
    const { counts } = await modelFor("workspace-mixed");
    const kinds = ["sources", "enrichers", "views"];
    const total = counts
      .filter((count) => kinds.includes(count.label))
      .reduce((sum, count) => sum + count.value, 0);
    const byState = counts
      .filter((count) => !kinds.includes(count.label))
      .reduce((sum, count) => sum + count.value, 0);
    // 38, not 37: workspace-mixed now carries one producer nothing
    // configures. What this test holds is the next line -- the strip's
    // kind counts and its state counts cover the same producers.
    expect(total).toBe(38);
    expect(byState).toBe(total);
  });

  it("links every count into the view that filters it, carrying the fixture", async () => {
    const { counts } = await modelFor("archiv3-small");
    for (const count of counts) {
      expect(count.href, count.label).toMatch(/^#sources\?/);
      expect(count.href, count.label).toContain("fixture=archiv3-small");
    }
    expect(counts.find((c) => c.label === "failed")?.href)
      .toBe("#sources?fixture=archiv3-small&state=failed");
  });

  // "0 failed, 0 running, 0 never run, 0 excluded, 0 withheld" is five counts
  // that say nothing, in the same weight as the two that do.
  it("prints no zero anywhere on the page", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { counts } = await modelFor(fixture.name);
      for (const count of counts) {
        expect(count.value, `${fixture.name} ${count.label}`).toBeGreaterThan(0);
      }
    }
  });
});

describe("Home's attempts read", () => {
  it("buckets one day at a time and sums to the totals the read declares", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      for (const range of RANGES) {
        const { model } = await modelFor(fixture.name, range);
        const days = Number(range.replace("d", ""));
        const { buckets, totals } = chartOf(model).attempts.payload;
        // n + 1 days for an n-day range: the span reaches back n whole days,
        // so it holds the read's own day and the n before it.
        expect(buckets.length, `${fixture.name} ${range}`).toBe(days + 1);
        const summed = buckets.reduce(
          (sum, bucket) => ({
            published: sum.published + bucket.published,
            blocked: sum.blocked + bucket.blocked,
            running: sum.running + bucket.running,
          }),
          { published: 0, blocked: 0, running: 0 },
        );
        expect(summed, `${fixture.name} ${range}`).toEqual(totals);
        expect(chartOf(model).attempts.payload.range).toBe(range);
      }
    }
  });

  it("counts every archiv3-small attempt inside the 14-day window", async () => {
    const { model } = await modelFor("archiv3-small");
    expect(chartOf(model).attempts.payload.totals)
      .toEqual({ published: 1, blocked: 5, running: 0 });
  });

  it("sees scale-200's rhythm shrink with the window", async () => {
    const wide = chartOf((await modelFor("scale-200", "30d")).model).attempts.payload.totals;
    const narrow = chartOf((await modelFor("scale-200", "7d")).model).attempts.payload.totals;
    expect(wide.published).toBeGreaterThan(narrow.published);
    expect(narrow.published + narrow.blocked).toBeGreaterThan(0);
  });
});

// The metric chart's derivations are held in metricsModel.test.ts. Home's
// part is that it hands the chart its reads, opens on records added, and
// draws no chart at all where there is no attempt.
describe("Home's metric chart", () => {
  it("opens on records added with the span control beside it", async () => {
    const { model, data } = await modelFor("archiv3-small");
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).toContain('data-testid="console-metric-chart"');
    expect(html).toContain('data-metric="records"');
    expect(html).toContain("Records added");
    for (const label of ["7 d", "14 d", "30 d", "90 d"]) expect(html).toContain(label);
    // The old headline over the plot is gone: the axis labels and the hover
    // carry the numbers.
    expect(html).not.toContain("records on 2026-09-13");
  });

  it("draws the records the day's published attempts added, as bars", async () => {
    const { model, data } = await modelFor("archiv3-small");
    const series = buildMetricSeries("records", chartOf(model).attempts, chartOf(model).generations);
    const withRecords = series.days.filter((day) => (day.values[0] ?? 0) > 0);
    expect(withRecords).toHaveLength(1);
    expect(withRecords[0]).toMatchObject({ day: "2026-09-13", values: [2605] });
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).toContain("okg-console__bar--records");
    expect(html).not.toContain("okg-console__line-main");
  });

  it("draws no chart and no span control where there is no attempt", async () => {
    const { model, data } = await modelFor("empty");
    expect(model.noAttemptsSentence)
      .toBe("No update attempt has been recorded for this deployment.");
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).not.toContain("console-metric-chart");
    expect(html).not.toContain("14 d");
  });
});

describe("Home's durations", () => {
  // "40 m 0 s", "6 h 0 m" and "2 m 0 s" state a fact of rounding, not of the
  // run.
  it("drops a unit that is zero", () => {
    expect(duration(21_600_000)).toBe("6 h");
    expect(duration(2_400_000)).toBe("40 m");
    expect(duration(128_000)).toBe("2 m 8 s");
    expect(duration(120_000)).toBe("2 m");
    expect(duration(42_000)).toBe("42 s");
    expect(duration(24_300_000)).toBe("6 h 45 m");
  });

  // The rows as rendered, subject and state, on the two fixtures whose
  // attention lists carry a time: a fact each, not the absence of a "0 s".
  it("renders each attention row's subject, state and time on archiv3-small and broken-runtime", async () => {
    const rowsOf = (model: HomeModel) => model.attention.rows
      .map((row) => [row.subject, row.outcome.word, row.time, row.kindLabel] as const);

    const small = await modelFor("archiv3-small");
    expect(rowsOf(small.model)).toEqual([
      ["cmssw_releases failed its last run and blocked the update", "Failed", "blocked 2 h ago", null],
      ["The background worker's job records are missing, so queued work cannot be checked", "Failed", "", "Runtime check"],
    ]);
    const smallHtml = markup(small.model, small.data.inputs.generations.payload.total);
    expect(smallHtml).toContain("cmssw_releases failed its last run and blocked the update");
    expect(smallHtml).toContain("The background worker&#x27;s job records are missing");
    expect(smallHtml).toContain("2 h ago");

    const broken = await modelFor("broken-runtime");
    expect(rowsOf(broken.model)).toEqual([
      ["Update attempt 3d7278f4dc58 was blocked", "Blocked", "blocked 2 d ago", null],
      // The kind is a tag beside the state, not "(view)" in the sentence.
      ["daily-brief failed its last run", "Failed", "last run 6 h ago", "View"],
      ["No worker has reported since 03:11 UTC", "Failed", "", "Runtime check"],
      ["37 jobs are in the dead-letter queue", "Failed", "", "Runtime check"],
      ["The queue has a backlog of 2,566 jobs and is not draining", "Failed", "", "Runtime check"],
    ]);
    const brokenHtml = markup(broken.model, broken.data.inputs.generations.payload.total);
    for (const [subject, word, time] of rowsOf(broken.model)) {
      expect(brokenHtml).toContain(subject);
      expect(brokenHtml).toContain(word);
      if (time) expect(brokenHtml).toContain(time);
    }
    // The running attempt's six hours print as "6 h", never "6 h 0 m".
    expect(broken.model.identity.chip.label).toContain("Updating \u00b7 6 h \u00b7");
    expect(brokenHtml).not.toMatch(/\b0 s\b|h 0 m\b/);
  });
});

// The chip's first element said "fine" while the fourth said no worker had
// reported for six hours. What normal is comes from the deployment's own last
// ten published attempts, not from a constant.
describe("Home's chip says how long an update usually takes", () => {
  it("turns amber once a running attempt passes three times the median", async () => {
    const { model } = await modelFor("broken-runtime");
    expect(model.identity.chip.severity).toBe("never-run");
    expect(model.identity.chip.label).toContain("usually 6 m 42 s");
  });

  it("names the median from the last ten published attempts", async () => {
    const { data } = await modelFor("workspace-mixed");
    expect(medianPublishedDuration(data.inputs.generations)).toBe(265946);
  });

  it("says nothing about pace where nothing has published", async () => {
    const { model } = await modelFor("empty");
    expect(model.identity.chip.label).toBe("Never published");
  });
});

// The header said "Up to date" while the same health read said `failing` and
// the page under it printed a red check and "Needs attention · 1".
describe("Home's chip agrees with the health read's verdict", () => {
  it("names the verdict where the newest attempt published and a check did not pass", async () => {
    // scale-200 publishes cleanly and its read is degraded: three invariants
    // failed and nine sources failed their last run.
    const { model, data } = await modelFor("scale-200");
    expect(model.identity.chip).toMatchObject({ label: "Degraded", severity: "never-run" });
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).toContain("Degraded");
    expect(html).not.toContain("Up to date");
  });

  it("keeps up to date only where the read agrees nothing is wrong", async () => {
    const { model } = await modelFor("all-clear");
    expect(model.identity.chip).toEqual({ label: "Up to date", severity: "ok" });
  });

  it("prints a calm chip on no fixture whose health read is not healthy", async () => {
    let calm = 0;
    for (const fixture of DESIGN_FIXTURES) {
      const { model, data } = await modelFor(fixture.name);
      if (model.identity.chip.severity !== "ok") continue;
      calm += 1;
      expect(data.inputs.health.payload.verdict, fixture.name).toBe("healthy");
    }
    // One fixture is genuinely healthy, so the loop is not vacuous.
    expect(calm).toBe(1);
  });
});

describe("Home's identity line", () => {
  it("names the version being read, its absolute time and its age", async () => {
    const { model } = await modelFor("archiv3-small");
    expect(model.identity.version).toBe("eea4e0d4e88d");
    expect(model.identity.version).toMatch(/^[0-9a-f]{12}$/);
    expect(model.identity.publishedAt).toBe("2026-09-13T09:00:00.000Z");
    expect(model.identity.publishedAge).toBe("5 d ago");
  });

  // The chip already says what a blocked attempt stopped on and when, so the
  // sentence survives only where the chip cannot carry the fact: a running
  // attempt names no producer.
  // No second sentence at all: the chip says the state, and the top bar's
  // version picker says which version reads use and how old it is.
  it("carries the state in the chip and nothing else in the header", async () => {
    const blocked = (await modelFor("archiv3-small")).model.identity;
    expect(blocked.chip.label)
      .toBe("Blocked \u00b7 cmssw_releases \u00b7 2 h ago \u00b7 reads a 5 d old version");
    const running = (await modelFor("broken-runtime")).model.identity;
    expect(running.chip.label).toBe("Updating \u00b7 6 h \u00b7 usually 6 m 42 s"
      + " \u00b7 reads a 3 d old version");
    for (const fixture of DESIGN_FIXTURES) {
      const { model, data } = await modelFor(fixture.name);
      const html = markup(model, data.inputs.generations.payload.total);
      expect(html, fixture.name).not.toContain("reads still use");
      expect(html, fixture.name).not.toContain("previous");
    }
  });


  it("says no version has been published, and names the producers that never ran", async () => {
    const { model } = await modelFor("empty");
    expect(model.identity.version).toBeNull();
    expect(model.identity.neverPublishedSentence)
      .toBe("No version has been published for empty.");
    expect(model.identity.chip).toEqual({ label: "Never published", severity: "never-run" });
    const html = markup(model, 0);
    expect(html).toContain("No version has been published for empty.");
  });
});

describe("Home's copy", () => {
  // "Serving is the system's word for itself" -- the operator, 2026-09-18.
  // A word boundary, not a substring: `preserveAspectRatio` on the chart's SVG
  // contains "serve" and is not copy.
  it("does not say the deployment serves anything", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model, data } = await modelFor(fixture.name);
      const html = markup(model, data.inputs.generations.payload.total);
      expect(html, fixture.name).not.toMatch(/\b(serves|serving|serve)\b/i);
    }
  });

  it("carries the deployment's state in the chip and prints no version line", async () => {
    const { model, data } = await modelFor("archiv3-small");
    const html = markup(model, data.inputs.generations.payload.total);
    expect(model.identity.chip.severity).toBe("blocked");
    expect(model.identity.chip.label)
      .toBe("Blocked \u00b7 cmssw_releases \u00b7 2 h ago \u00b7 reads a 5 d old version");
    expect(html).toContain("Blocked \u00b7 cmssw_releases \u00b7 2 h ago");
  });

  it("prints no machine code on Home", async () => {
    let checked = 0;
    for (const fixture of DESIGN_FIXTURES) {
      const { model } = await modelFor(fixture.name);
      // Every fixture but all-clear has something to attend to; an empty
      // list would make the loop below vacuous, so the count is asserted first.
      if (fixture.name === "all-clear") {
        expect(model.attention.rows).toEqual([]);
        expect(model.attention.allClearSentence).toBeTruthy();
      } else {
        expect(model.attention.rows.length, `${fixture.name} attention rows`).toBeGreaterThan(0);
      }
      for (const row of model.attention.rows) {
        checked += 1;
        expect(`${row.subject} ${row.cause}`, `${fixture.name} ${row.key}`)
          .not.toMatch(/\b[a-z][a-z0-9]*(_[a-z0-9]+)+:/);
        expect(row.subject, `${fixture.name} ${row.key}`)
          .not.toMatch(/^Runtime check /);
      }
    }
    expect(checked).toBeGreaterThanOrEqual(9);
  });

  it("keeps the sentence and drops the code in front of it", () => {
    expect(plainCause("clone_failed: repository not found (404)"))
      .toBe("repository not found (404)");
    expect(plainCause("view_build_failed: relation \"okg.contributor_edge\" does not exist"))
      .toBe("relation \"okg.contributor_edge\" does not exist");
    // No code in front: the line is already the sentence, so nothing is cut.
    expect(plainCause("the remote refused the connection"))
      .toBe("the remote refused the connection");
    expect(plainCause(null)).toBeNull();
  });

  it("titles a runtime check with the check's own sentence", async () => {
    const { model } = await modelFor("broken-runtime");
    const checks = model.attention.rows.filter((row) => row.key.startsWith("check:"));
    expect(checks.map((row) => row.subject)).toEqual([
      "No worker has reported since 03:11 UTC",
      "37 jobs are in the dead-letter queue",
      "The queue has a backlog of 2,566 jobs and is not draining",
    ]);
    // No "Recorded:" prefix and no trailing stop: the cause is a fragment. A
    // measurement made of words is printed as the check recorded it.
    expect(checks[0].cause).toBe("0 workers reporting, last heartbeat 5h 49m ago");
  });

  // Live Archiv3 printed `dbos.workflow_status missing` under the failed
  // runtime check: a database object on the page an operator opens to see
  // whether the graph is current.
  it("withholds a measurement that names a database object, and says so", async () => {
    const { model, data } = await modelFor("archiv3-small");
    const archiv = model.attention.rows.find((row) => row.key.startsWith("check:"));
    expect(archiv?.subject).toBe(
      "The background worker's job records are missing, so queued work cannot be checked",
    );
    expect(archiv?.cause).toBe("");
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).not.toContain("dbos.workflow_status");
    expect(html).not.toContain("workflow_status");
  });

  // No row on Home may name a database object, whoever wrote the string: a
  // check the health read composed, or a producer's own run.
  it("prints no database object name in any fixture's attention rows", async () => {
    let checked = 0;
    for (const fixture of DESIGN_FIXTURES) {
      const { model, data } = await modelFor(fixture.name);
      const html = markup(model, data.inputs.generations.payload.total);
      for (const row of model.attention.rows) {
        checked += 1;
        expect(namesDatabaseObject(row.cause), `${fixture.name} ${row.key}`).toBe(false);
      }
      expect(html, fixture.name).not.toContain("okg.");
    }
    // Twelve rows across the six fixtures, so the loop is not vacuous. It
    // was thirteen while Home read the three `*-diagnostics` routes: the
    // producers read carries no error text for a SOURCE at all, on any
    // deployment, so one fixture row's cause is now empty rather than a
    // string those routes never served either (see below).
    expect(checked).toBe(12);
  });

  // A view's cause is the VIEW READ's own sentence, not the run's exception.
  //
  // The mock used to give this row `relation "okg.contributor_edge" does not
  // exist`, and Home withheld it by shape. No live read serves it: `_issue`
  // in `console_views/view_diagnostics.py` composes one of three fixed
  // sentences from the latest materialization run and never touches the
  // error text, so the mock was drawing a page no deployment can. The
  // fixture now carries what that composer carries -- and `console/producers`
  // now carries it too, which it was hard-nulling under a reason claiming the
  // view read records no issue.
  it("prints the view read's own sentence for a failed view", async () => {
    const { model } = await modelFor("workspace-mixed");
    const row = model.attention.rows.find((r) => r.key === "producer:view:contributor-graph");
    expect(row?.subject).toBe("contributor-graph failed its last run");
    expect(row?.kindLabel).toBe("View");
    expect(row?.cause).toBe("Latest materialization failed");
  });

  // The withholding rule itself, asserted on the model rather than through a
  // fixture, because no live read gives Home a string that could trip it any
  // more: a source's error text reaches no console read at all, and the
  // enricher and view reads compose their own sentences. The rule stays --
  // an enricher issue is the one field a writer could still fill -- and this
  // is what keeps it proven now that nothing exercises it by accident.
  it("withholds a recorded issue that names a database object", () => {
    const producer = {
      name: "contributor-graph",
      ownershipId: "_enricher:contributor-graph",
      rowKey: "enricher:_enricher:contributor-graph",
      kind: "enricher" as const,
      configured: true,
      health: { status: "degraded", incident_count: 1 },
      workflow: {
        state: "failing",
        latest_issue: 'relation "okg.contributor_edge" does not exist',
        reason: null,
      },
      contribution: { records_in_gen: 0, edges_in_gen: 0 },
      freshness: { status: "last_run_age_only" },
      latestRun: null,
      latestRunUnavailableReason: null,
      runCount: null,
      runCountUnavailableReason: null,
      countDisagreement: null,
    };
    expect(producerCause(producer)).toBe("degraded \u00b7 detail withheld");
    // And one that says nothing about the database keeps its sentence.
    expect(producerCause({
      ...producer,
      workflow: { ...producer.workflow, latest_issue: "repository not found (404)" },
    })).toBe("repository not found (404)");
  });

  // A fix hint is either words or something to type. Home prints the words.
  it("prints a fix hint that is a sentence and not one that is a command", () => {
    expect(checkCause({
      severity: "fail", code: "c", scope: "runtime", summary: "S.",
      actual: "dbos.workflow_status missing",
      fix_hint: "inspect DBOS history volume and the runtime queue authority",
    })).toBe("detail withheld · inspect DBOS history volume and the runtime queue authority");
    expect(checkCause({
      severity: "fail", code: "c", scope: "runtime", summary: "S.",
      actual: "okg.node_current missing", fix_hint: "okg queue dlq list",
    })).toBe("detail withheld");
    expect(checkCause({
      severity: "fail", code: "c", scope: "runtime", summary: "S.", actual: null,
    })).toBe("no measurement recorded");
  });
});

describe("Home's Needs attention", () => {
  it("ranks a blocked attempt, then failed producers, then runtime checks", async () => {
    const { model } = await modelFor("broken-runtime");
    expect(model.attention.rows.map((row) => row.key)).toEqual([
      "attempt:okg:broken-runtime:branch:default:gen:20260916T090000000000Z:3d7278f4dc58",
      "producer:view:daily-brief",
      "check:worker_missing",
      "check:dead_letter_jobs",
      "check:queue_backlog",
    ]);
    expect(model.attention.omitted).toBe(0);
    expect(model.attention.allClearSentence).toBeNull();
  });

  it("does not repeat a blocked attempt the version being read already superseded", async () => {
    const { model } = await modelFor("scale-200");
    expect(model.attention.rows.every((row) => !row.key.startsWith("attempt:"))).toBe(true);
    // Nine sources, one cause: one row that says nine, not nine rows.
    expect(model.attention.rows).toHaveLength(1);
    expect(model.attention.rows[0].subject).toBe("9 sources failed their last run");
    expect(model.attention.rows[0].count).toBe(9);
    expect(model.attention.rows[0].members).toHaveLength(9);
    expect(model.attention.omitted).toBe(0);
  });

  it("caps at eight rows and says how many it left out", async () => {
    const { model } = await modelFor("workspace-mixed");
    // Ten rows collapse to four groups, all of which fit under the cap.
    expect(model.attention.rows.length).toBeLessThanOrEqual(NEEDS_ATTENTION_CAP);
    expect(model.attention.rows.reduce((sum, row) => sum + row.count, 0)).toBe(10);
    expect(model.attention.omitted).toBe(0);
    const html = markup(model, 22);
    // The count sits in the section title, not in a pill.
    // The count is its own element, so it can carry the failure colour.
    expect(html).toContain("Needs attention");
    expect(html).toContain(">10<");
    expect(html).toContain("show 4");
  });

  it("carries the fact that makes all-clear clear, and never a bare reassurance", async () => {
    const { model } = await modelFor("all-clear");
    expect(model.attention.rows).toHaveLength(0);
    expect(model.attention.allClearSentence).toBe(
      "All 10 configured producers updated in the version published 2026-09-18 08:00 UTC; "
      + "the queue and the dead-letter queue are empty; no check is failing.",
    );
    const html = markup(model, 15);
    expect(html).not.toContain("Nothing needs your attention");
    expect(html).not.toContain("All good");
  });

  it("shows a never-run producer as never run, not as failed", async () => {
    const { model } = await modelFor("empty");
    expect(model.attention.rows).toHaveLength(1);
    const [row] = model.attention.rows;
    expect(row.outcome.word).toBe("Never run");
    // No age: there is no last run to be old, and "never" repeated the state.
    expect(row.time).toBe("");
    expect(row.count).toBe(3);
    expect(row.subject).toBe("3 sources have never run");
  });

  // The page that answers "what do I do next" answered it with a click.
  it("names the members when a never-run group is the whole list", async () => {
    const { model, data } = await modelFor("empty");
    const [row] = model.attention.rows;
    // By name within a rank: the reads return producers in no declared order,
    // so the page had been printing whichever order the fixture happened to
    // hold.
    // No cadence. The console has ONE producers read and it carries no
    // next-due field for any kind of producer; only the design harness's
    // `*-diagnostics` rows ever had one, and a sentence built from a field no
    // deployment serves is the page inventing a schedule.
    expect(row.cause).toBe("git-files, papers, sessions \u00b7 no schedule is recorded");
    expect(row.members).toEqual([]);
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).not.toContain("show 3");
  });

  // Three sources waiting to run is not a count of three problems.
  it("colours the count by the worst state in the list", async () => {
    expect((await modelFor("empty")).model.attention.severity).toBe("never-run");
    expect((await modelFor("archiv3-small")).model.attention.severity).toBe("failed");
    expect((await modelFor("broken-runtime")).model.attention.severity).toBe("failed");
    expect((await modelFor("all-clear")).model.attention.severity).toBeNull();
  });

  // Runtime checks are evaluated when the page is read, and the footer says
  // when that was.
  it("gives a runtime check no age of its own", async () => {
    const { model } = await modelFor("broken-runtime");
    const runtime = model.attention.rows.filter((row) => row.kindLabel === "Runtime check");
    expect(runtime.length).toBeGreaterThan(0);
    for (const row of runtime) expect(row.time).toBe("");
    expect(model.attention.rows.some((row) => row.kindLabel === "system")).toBe(false);
  });
});

describe("Home's hero", () => {
  it("opens with the contents of the graph, as four exact figures", async () => {
    const { model, data } = await modelFor("scale-200");
    expect(model.hero.map((figure) => [figure.label, figure.value])).toEqual([
      ["records", 4_182_044],
      ["relationships", 11_904_882],
      ["record types", 214],
      // "sources configured", not "sources": the figure counts the registry,
      // including sources that contributed nothing to this version, and
      // beside three counts of what the version HOLDS the bare word read as
      // "220 sources contributed".
      ["sources configured", 220],
    ]);
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).toContain("4,182,044");
    expect(html).toContain("11,904,882");
    // The page's subject leads its own hierarchy again: pass 7 printed it as a
    // 13 px eyebrow, the smallest text on a page about that deployment.
    expect(html).toContain("<h1 class=\"okg-console__title\">scale-200</h1>");
  });

  it("renders the same four figures on a deployment with nothing in it", async () => {
    const { model } = await modelFor("empty");
    expect(model.hero.map((figure) => [figure.label, figure.value])).toEqual([
      ["records", 0],
      ["relationships", 0],
      ["record types", 0],
      ["sources configured", 3],
    ]);
  });

  it("uses the singular when there is one of something", async () => {
    const { model } = await modelFor("all-clear");
    expect(model.hero.find((figure) => figure.label.startsWith("record type"))?.label)
      .toBe("record types");
    expect((await modelFor("all-clear")).counts.map((count) => count.label))
      .toEqual(["sources", "enricher", "view", "updated"]);
  });
});

describe("Home's Updates panel", () => {
  // Five attempts blocked on the same producer are one fact, not five rows.
  it("collapses a run of identical attempts into one row", async () => {
    const { model } = await modelFor("archiv3-small");
    expect(model.recentAttempts).toHaveLength(2);
    const [run, published] = model.recentAttempts;
    expect(run.count).toBe(5);
    // The outcome word beside the glyph says "Blocked"; the headline says
    // what it stopped on and the count is its own "5\u00d7".
    expect(run.headline).toBe("on cmssw_releases");
    expect(run.when).toBe("4 d ago to 2 h ago");
    expect(run.version).toBe("bd2648936849");
    expect(published.count).toBe(1);
    expect(published.outcome.word).toBe("Published");
    expect(published.detail).toBe("+2,605 records, +2,296 relationships");
  });

  it("does not repeat the outcome word in the headline of a single attempt", async () => {
    const { model } = await modelFor("all-clear");
    for (const row of model.recentAttempts) {
      expect(row.count).toBe(1);
      expect(row.headline).toBe("");
    }
  });

  it("prints the count of a collapsed run once, as a multiplier", async () => {
    const { model, data } = await modelFor("archiv3-small");
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).toContain("5\u00d7");
    expect(html).not.toContain("attempts blocked on");
  });

  it("marks a superseded blocked attempt with the same tag style as every other annotation", async () => {
    const { model, data } = await modelFor("scale-200");
    expect(model.recentAttempts.some((row) => row.superseded)).toBe(true);
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).toContain('okg-console__tag">superseded<');
    expect(html).not.toContain("okg-console__reading");
  });

  it("keeps distinct attempts as their own rows", async () => {
    const { model } = await modelFor("broken-runtime");
    expect(model.recentAttempts.map((row) => [row.outcome.word, row.count]))
      .toEqual([["Running", 1], ["Blocked", 1], ["Published", 1]]);
  });
});

describe("Home's figures", () => {
  // The sparklines beside the record and relationship figures were cut in pass
  // 12: a fourteen-point line beside a seven-digit number was decoration.
  it("draws no sparkline", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model, data } = await modelFor(fixture.name);
      const html = markup(model, data.inputs.generations.payload.total);
      expect(html, fixture.name).not.toContain("okg-console__spark");
      expect(html, fixture.name).not.toContain("polyline");
    }
  });

  it("carries the newest published version's delta where an attempt records one", async () => {
    // A delta needs a previous published version. archiv3-small has exactly
    // one, so "+2,605" beside "2,605 records" would be the same number twice.
    expect((await modelFor("archiv3-small")).model.hero.map((f) => f.delta))
      .toEqual([null, null, null, null]);
    expect((await modelFor("empty")).model.hero.map((f) => f.delta))
      .toEqual([null, null, null, null]);
    expect((await modelFor("workspace-mixed")).model.hero.map((f) => f.delta))
      .toEqual([467, 856, null, null]);
  });
});

describe("Home's Producers panel", () => {
  // The block is the census: a state bar, the counts, and one link. The
  // producers that need something -- and the next run one of them declares --
  // are named under Needs attention, once. `ui/cuts.md` records it.
  it("is the state bar, the counts and the link, and names no producer", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model } = await modelFor(fixture.name);
      expect(Object.keys(sourcesBlock(model)).sort(), fixture.name)
        .toEqual(["allGoodLine", "counts", "href", "linkLabel", "segments", "total"]);
    }
  });

  it("leaves every producer's name to Needs attention", async () => {
    const { model, data } = await modelFor("workspace-mixed");
    const html = markup(model, data.inputs.generations.payload.total);
    const sources = html.slice(html.indexOf('data-testid="console-sources"'));
    const block = sources.slice(0, sources.indexOf('data-testid="console-contents"'));
    expect(block).not.toContain("contributor-graph");
    expect(html).toContain("contributor-graph");
  });
});

/** The Sources block, which every fixture's producer reads answer for. Null
 *  means the reads had not landed, which cannot happen behind
 *  `loadConsoleHome`; the test says so rather than reading through a `!`. */
function sourcesBlock(model: HomeModel): NonNullable<HomeModel["producers"]> {
  if (model.producers === null) {
    throw new Error("the fixture's producer reads did not answer");
  }
  return model.producers;
}

/** The chart inputs, or a failure naming why there are none. `chart` is null
 *  only when the attempts read failed; every fixture defines that read. */
function chartOf(model: HomeModel): NonNullable<HomeModel["chart"]> {
  if (model.chart === null) {
    throw new Error("the fixture's attempts read did not reach the model");
  }
  return model.chart;
}

/** The Contents model, or a failure naming why there is none. `contents` is
 *  null only when the ontology-rollup read failed; every fixture here defines
 *  that read, so a null in these tests is a real defect, not a state to
 *  tolerate. */
function contentsOf(model: HomeModel): NonNullable<HomeModel["contents"]> {
  if (model.contents === null) {
    throw new Error("the fixture's ontology-rollup read did not reach the model");
  }
  return model.contents;
}

describe("Home's Contents panel", () => {
  it("shows the three largest record types with their share", async () => {
    const { model } = await modelFor("archiv3-small");
    expect(contentsOf(model).topRecordTypes.slice(0, 3)).toEqual([
      { name: "cmssw_release", count: 2414, share: 93, shareLabel: "93%" },
      { name: "document", count: 96, share: 4, shareLabel: "4%" },
      { name: "paper", count: 58, share: 2, shareLabel: "2%" },
    ]);
    expect(contentsOf(model).topRecordTypes).toHaveLength(5);
    expect(contentsOf(model).moreTypes).toBe(1);
  });

  // The operator on the approved Home: "why are the relationships just some
  // weird text thing". They are a table now, in the same shape as the record
  // types, and this holds it there.
  it("shows relationship types as rows, not as a sentence", async () => {
    const { model } = await modelFor("archiv3-small");
    expect(contentsOf(model).topRelationshipTypes).toEqual([
      { name: "supersedes", count: 1204, share: 52, shareLabel: "52%" },
      { name: "mentions", count: 812, share: 35, shareLabel: "35%" },
      { name: "authored_by", count: 198, share: 9, shareLabel: "9%" },
      { name: "part_of", count: 82, share: 4, shareLabel: "4%" },
    ]);
    // Four types, all four shown, so nothing is left to state.
    expect(contentsOf(model).moreRelationshipTypes).toBe(0);
    expect(contentsOf(model).moreRelationshipCount).toBe(0);
  });

  it("states what the five relationship rows leave out", async () => {
    const { model } = await modelFor("scale-200");
    expect(contentsOf(model).topRelationshipTypes).toHaveLength(5);
    expect(contentsOf(model).moreRelationshipTypes).toBe(595);
    expect(contentsOf(model).moreRelationshipCount).toBeGreaterThan(0);
  });

  it("renders both tables in the same shape", async () => {
    const { model, data } = await modelFor("archiv3-small");
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).toContain("Record types");
    expect(html).toContain("Relationship types");
    expect(html).toContain("Relationships</th>");
    expect(html).toContain("supersedes");
    // The rejected form is gone.
    expect(html).not.toContain("supersedes 52%");
  });

  it("shows five and no more, whatever the deployment holds", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model } = await modelFor(fixture.name);
      expect(contentsOf(model).topRecordTypes.length, fixture.name).toBeLessThanOrEqual(5);
      expect(contentsOf(model).topRelationshipTypes.length, fixture.name).toBeLessThanOrEqual(5);
    }
    const scale = contentsOf((await modelFor("scale-200")).model);
    expect(scale.topRecordTypes).toHaveLength(5);
    expect(scale.moreTypes).toBe(209);
  });

  it("says a version holds nothing rather than naming a type that is not there", async () => {
    const { model, data } = await modelFor("empty");
    expect(contentsOf(model).topRecordTypes).toHaveLength(0);
    expect(contentsOf(model).topRelationshipTypes).toHaveLength(0);
    expect(contentsOf(model).recordsEmptyLine).toBe("This version holds no records.");
    expect(contentsOf(model).relationshipsEmptyLine)
      .toBe("This version holds no relationships.");
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).not.toContain("okg-console__types-count");
  });


});

describe("Home's footer and version identifiers", () => {
  it("states the count basis, and only that", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model } = await modelFor(fixture.name);
      expect(model.footer, fixture.name)
        .toBe(fixture.name === "empty"
          // Nothing is counted on a deployment with no version, so the first
          // clause goes and the read time stays.
          ? "As of 2026-09-18 09:00 UTC."
          : `Counts are totals for version ${shortVersion(
            fixture.name === "empty" ? "" : model.identity.fullVersion,
          )}, before access rules \u00b7 as of 2026-09-18 09:00 UTC.`);
    }
  });

  it("sets the footer's version id in mono (audit B21)", async () => {
    const { model, data } = await modelFor("archiv3-small");
    const short = shortVersion(model.identity.fullVersion);
    expect(short).toMatch(/^[0-9a-f]{12}$/);
    const html = markup(model, data.inputs.generations.payload.total);
    expect(html).toContain(`<span class="okg-console__version-id">${short}</span>`);
  });

  // The queue state is a Needs attention row when it is wrong and nobody's
  // business when it is not; the footer no longer repeats it.
  it("leaves the queue state to Needs attention", async () => {
    const { model } = await modelFor("archiv3-small");
    expect(model.footer).not.toContain("queue");
    expect(model.attention.rows.some((row) => row.subject.includes("queue"))).toBe(true);
  });


  it("prints twelve hex characters for a version, in the bar and on the page", async () => {
    const { data } = await modelFor("workspace-mixed");
    const first = data.inputs.versions!.payload.versions[0];
    expect(shortVersion(first.generation_id)).toBe("31caba7be14a");
    expect(versionOptionLabel(first.generation_id, first.published_at))
      .toBe("31caba7be14a · 2026-09-17 09:00 UTC");
    expect(versionControlLabel(first.generation_id, first.published_at))
      .toBe("31caba7be14a · 09-17 09:00");
  });

  it("renders from the reads that answered when the versions read fails", async () => {
    // The reported defect: one failed read on the page frame blanked all four
    // views while health, generations, attempts and the producer diagnostics
    // still answered. Home now loses what the versions read carried and
    // nothing else, and says which read it lost.
    const fixture = fixtureByName("workspace-mixed")!;
    const inner = createFixtureApiClient(fixture);
    const refusing = {
      get: <T,>(route: `/viz/api/${string}`, params?: Record<string, string>): Promise<T> => (
        route === VERSIONS_ROUTE
          ? Promise.reject(new ApiRequestError({ route, status: 503 }))
          : inner.get<T>(route, params)
      ),
    };
    const data = await loadConsoleHome(refusing);
    expect(data.versionsFailure).toBeInstanceOf(ApiRequestError);
    expect(data.inputs.versions).toBeNull();
    // The five reads that answered are all present, not discarded.
    expect(data.inputs.health).toBeTruthy();
    expect(data.inputs.generations).toBeTruthy();
    expect(data.inputs.producers).toBeTruthy();

    const model = buildHomeModel(
      { ...data.inputs, attempts: attemptsFor(data, "14d") }, designLink("workspace-mixed"),
    );
    const markup = renderToStaticMarkup(
      <ConsoleHome
        model={model}
        total={data.inputs.generations.payload.total}
        range="14d"
        onRange={() => {}}
        link={designLink("workspace-mixed")}
      />,
    );
    // The page rendered from what answered.
    expect(markup).toContain('data-testid="console-home"');
    // Re-aimed, not dropped. The line naming the lost read left this page
    // when the Sources branch moved the versions read into the top bar,
    // which makes it for itself and prints its own failure. What is held
    // here is that the words the bar prints name THIS route and status --
    // the same `namedReadFailure` the bar calls -- so the fact cannot go
    // unreported by either surface.
    const line = readFailureLine(namedReadFailure(VERSIONS_ROUTE, data.versionsFailure));
    expect(line).toContain(VERSIONS_ROUTE);
    expect(line).toContain("503");
    // The deployment's own facts are still on it.
    expect(markup).toContain(data.inputs.health.payload.deployment);
    expect(markup).toContain('data-testid="console-figures"');
    // What the lost read carried is CUT, not defaulted: no delta is claimed
    // against a previous version this page could not confirm exists.
    expect(model.hero.every((figure) => figure.delta === null)).toBe(true);
  });
});

describe("Home under a read the fixture does not define", () => {
  it("refuses rather than rendering a healthy-looking blank", async () => {
    const fixture = fixtureByName("empty");
    if (!fixture) throw new Error("no empty fixture");
    const api = createFixtureApiClient({ ...fixture, reads: {} });
    await expect(loadConsoleHome(api)).rejects.toBeInstanceOf(FixtureReadMissing);
  });
});

describe("Home's rendered markup", () => {
  it("renders every block on every fixture; CSS decides which a width shows", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model, data } = await modelFor(fixture.name);
      const html = markup(model, data.inputs.generations.payload.total);
      for (const testid of [
        "console-home", "console-header", "console-figures", "console-attention",
        "console-updates", "console-sources", "console-contents",
        "console-contents-records", "console-contents-relationships", "console-footer",
      ]) {
        expect(html, `${fixture.name} ${testid}`).toContain(`data-testid="${testid}"`);
      }
      // The chart is the one block a fixture may not have: `empty` has no
      // attempt to draw.
      expect(html.includes("console-metric-chart"), fixture.name)
        .toBe(fixture.name !== "empty");
    }
  });

  it("gives every status a word beside its glyph", async () => {
    const { model, data } = await modelFor("broken-runtime");
    const html = markup(model, data.inputs.generations.payload.total);
    for (const word of ["Published", "Blocked", "Running", "Failed"]) {
      expect(html, word).toContain(`okg-console__status-word">${word}<`);
    }
  });
});
