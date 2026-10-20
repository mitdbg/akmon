// Updates, asserted against the six fixtures rather than against a shape
// written here.
//
// The captures are what the operator approves. These tests hold what a
// screenshot cannot: that a filter arriving on the URL actually filters, that
// five identical blocked attempts are one row and can be opened back into
// five, that the stage list follows the stage the attempt recorded, that the
// verbatim error appears only where there is one, that the row for the version
// being read is the one marked, and that a cursor page which repeats a row is
// refused instead of doubling the table.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { createFixtureApiClient } from "@operator-ui/design-harness/fixtureClient";
import { DESIGN_FIXTURES, fixtureByName } from "@operator-ui/design-harness/fixtures";
import { designLink, parseDesignLocation } from "@operator-ui/design-harness/DesignHarness";
import { loadConsoleUpdates, loadMoreAttempts } from "@operator-ui/console/api/generations";
import { ConsoleAttemptDetail } from "@operator-ui/console/views/AttemptDetail";
import { ConsoleUpdates } from "@operator-ui/console/views/Updates";
import type { RawAttempt, RawAttemptsByDay } from "@operator-ui/console/model/shared/reads";
import { buildAttemptDetail, newestBlockedAttempt } from "@operator-ui/console/model/updates/detail";
import { parseUpdatesFilter, type UpdatesFilter } from "@operator-ui/console/model/updates/filters";
import { blockedNote, buildUpdatesModel, type UpdatesModel } from "@operator-ui/console/model/updates/list";
import { buildUpdateRows } from "@operator-ui/console/model/updates/rows";

const NO_FILTER: UpdatesFilter = { outcome: null, producer: null, attempt: null };

/** One attempt, for the cases a fixture cannot express. */
function attemptWith(fields: Partial<RawAttempt>): RawAttempt {
  return {
    generation_id: 1,
    full_generation_id: "okg:d:branch:default:gen:x:aaaaaaaaaaaa",
    status: "published",
    created_at: "2026-09-21T02:48:10.000Z",
    duration_ms: null,
    nodes_added: 0,
    edges_added: 0,
    nodes_retracted: 0,
    edges_retracted: 0,
    stopped_stage: null,
    stopped_producer: null,
    ...fields,
  };
}

async function updatesFor(name: string, filter: UpdatesFilter = NO_FILTER) {
  const fixture = fixtureByName(name);
  if (!fixture) throw new Error(`no fixture named ${name}`);
  const data = await loadConsoleUpdates(createFixtureApiClient(fixture));
  return {
    fixture,
    data,
    model: buildUpdatesModel(
      {
        health: data.health,
        generations: data.generations,
        versions: data.versions,
        producers: data.producers,
        attempts: data.attemptsByRange["14d"]!,
      },
      filter,
      designLink(name),
    ),
  };
}

function listMarkup(
  model: UpdatesModel,
  attempts: RawAttemptsByDay,
  data?: Awaited<ReturnType<typeof loadConsoleUpdates>>,
): string {
  const generations = data?.generations ?? { payload: { generations: [], total: 0 } };
  const newest = newestBlockedAttempt(generations.payload.generations);
  return renderToStaticMarkup(
    ConsoleUpdates({
      model,
      chart: { attempts, generations },
      range: "14d",
      onRange: () => {},
      onMore: null,
      moreError: null,
      loadingMore: false,
      context: {
        deployment: model.deployment, version: null, publishedAt: null, nowMs: model.nowMs,
      },
      inline: newest
        ? {
          detail: buildAttemptDetail(newest.attempt, {
            previous: newest.previous, nowMs: model.nowMs, readingVersion: null,
          }),
          href: "#updates/x",
        }
        : null,
    }),
  );
}

describe("Updates reads its filter off the route", () => {
  it("takes the outcome, producer and attempt Home's links carry", () => {
    expect(parseUpdatesFilter({ outcome: "blocked" }).outcome).toBe("blocked");
    expect(parseUpdatesFilter({ producer: "cmssw_releases" }).producer).toBe("cmssw_releases");
    expect(parseUpdatesFilter({ attempt: "eea4e0d4e88d" }).attempt).toBe("eea4e0d4e88d");
    // And NOT the top bar's own parameter, whatever shape it holds.
    expect(parseUpdatesFilter({ version: "eea4e0d4e88d" }).attempt).toBeNull();
  });

  // A filter that can only empty the table would look like a deployment with
  // no attempts, which is the one thing an empty table must not be confused
  // with.
  it("drops an outcome the console does not have", () => {
    expect(parseUpdatesFilter({ outcome: "exploded" }).outcome).toBeNull();
  });

  it("keeps the filters through the harness's own hash grammar", () => {
    const location = parseDesignLocation("#updates?fixture=archiv3-small&outcome=blocked");
    expect(location.view).toBe("Updates");
    expect(location.fixture).toBe("archiv3-small");
    expect(location.params).toEqual({ outcome: "blocked" });
    expect(location.detail).toBeNull();
  });

  it("reads the attempt id out of #updates/<id>", () => {
    const location = parseDesignLocation("#updates/bd2648936849?fixture=archiv3-small");
    expect(location.view).toBe("Updates");
    expect(location.detail).toBe("bd2648936849");
  });

  it("filters the table to one outcome", async () => {
    const all = await updatesFor("workspace-mixed");
    const blocked = await updatesFor("workspace-mixed", { ...NO_FILTER, outcome: "blocked" });
    const outcomes = new Set(blocked.model.rows.map((row) => row.outcome.word));
    expect(outcomes).toEqual(new Set(["Blocked"]));
    expect(blocked.model.rows.length).toBeLessThan(all.model.rows.length);
  });

  it("filters the table to one producer", async () => {
    const { model } = await updatesFor("archiv3-small", {
      ...NO_FILTER, producer: "cmssw_releases",
    });
    expect(model.rows).toHaveLength(1);
    expect(model.rows[0].stoppedOn).toBe("on cmssw_releases");
    expect(model.rows[0].count).toBe(5);
  });

  it("says a filter matched nothing, which is not the same as no attempts", async () => {
    const { model } = await updatesFor("all-clear", { ...NO_FILTER, outcome: "blocked" });
    expect(model.noAttemptsSentence).toBeNull();
    expect(model.noMatchSentence).toBe("No attempt the read returned matches this filter.");
  });
});

describe("Updates collapses a run of identical attempts", () => {
  it("makes archiv3-small's five blocked attempts one row that opens into five", async () => {
    const { model } = await updatesFor("archiv3-small");
    expect(model.rows).toHaveLength(2);
    const [blocked, published] = model.rows;
    expect(blocked.count).toBe(5);
    expect(blocked.members).toHaveLength(5);
    expect(blocked.stoppedOn).toBe("on cmssw_releases");
    // The span, not one age: five attempts over four days is the fact.
    expect(blocked.when).toBe("4 d ago to 2 h ago");
    expect(published.count).toBe(1);
    expect(published.version).toBe("eea4e0d4e88d");
    expect(published.change).toBe("+2,605 records \u00b7 +2,296 relationships");
    // The count IS the expander. It used to sit beside a duration; that
    // column is cut, so the control moved into the Outcome cell.
    expect(blocked.repeat).toBe("5\u00d7");
    expect(blocked).not.toHaveProperty("duration");
  });

  it("never collapses published attempts, however alike they look", async () => {
    const { model } = await updatesFor("all-clear");
    expect(model.rows).toHaveLength(15);
    for (const row of model.rows) expect(row.count).toBe(1);
  });

  // A published attempt between two blocked ones means the blockage was fixed
  // and came back. That is a different story from a run and must read as one.
  it("breaks a run when something else happened in the middle", () => {
    const blocked = (id: string, at: string): RawAttempt => ({
      generation_id: id,
      full_generation_id: `okg:x:gen:${id}`,
      status: "blocked",
      created_at: at,
      duration_ms: 1000,
      nodes_added: null,
      edges_added: null,
      nodes_retracted: null,
      edges_retracted: null,
      stopped_stage: "sources",
      stopped_producer: "papers",
    });
    const rows = buildUpdateRows(
      [
        blocked("c", "2026-09-18T03:00:00.000Z"),
        {
          ...blocked("b", "2026-09-18T02:00:00.000Z"),
          status: "published",
          stopped_stage: null,
          stopped_producer: null,
          nodes_added: 1,
          edges_added: 2,
        },
        blocked("a", "2026-09-18T01:00:00.000Z"),
      ],
      {
        nowMs: Date.parse("2026-09-18T09:00:00.000Z"),
        readingVersion: null,
        limit: 50,
        link: designLink("archiv3-small"),
      },
    );
    expect(rows.map((row) => row.count)).toEqual([1, 1, 1]);
  });

  it("caps the table at one page and says what it is showing", async () => {
    const { model } = await updatesFor("scale-200");
    expect(model.readCount).toBe(50);
    expect(model.rows.length).toBeLessThanOrEqual(50);
    expect(model.total).toBe(4800);
    expect(model.shownSentence).toBe("50 of 4,800 attempts");
  });
});

describe("Updates prints no fact the read does not record", () => {
  // These guards replace the ones that asserted a duration, a running pace
  // and a "late" marker. Their subjects are CUT, not moved: the attempts read
  // returns duration_ms null on every attempt of every deployment, so the
  // Duration column, the pace phrase and the late marker had no source. What
  // is guarded now is that they stay cut.
  it("carries no duration, pace or late marker on any row", async () => {
    for (const fixture of ["broken-runtime", "all-clear", "archiv3-small"]) {
      const { model } = await updatesFor(fixture);
      expect(model.rows.length).toBeGreaterThan(0);
      for (const row of model.rows) {
        expect(row).not.toHaveProperty("duration");
        expect(row).not.toHaveProperty("pace");
        expect(row).not.toHaveProperty("late");
        for (const member of row.members) {
          expect(member).not.toHaveProperty("duration");
        }
      }
    }
  });

  it("renders no Duration column and no duration string", async () => {
    const { model, data } = await updatesFor("broken-runtime");
    const markup = listMarkup(model, data.attemptsByRange["14d"]!, data);
    expect(markup).not.toContain("Duration");
    expect(markup).not.toContain("usually");
    // A running attempt used to print "Running · 6 h · usually 6 m 42 s".
    expect(markup).toContain("Running");
    expect(markup).not.toMatch(/\d+\s*h\s*\u00b7\s*usually/);
  });

  it("keeps the expander, in the Outcome cell", async () => {
    const { model, data } = await updatesFor("archiv3-small");
    const markup = listMarkup(model, data.attemptsByRange["14d"]!, data);
    expect(markup).toContain("5\u00d7");
    expect(markup).toContain("Show the 5 attempts");
  });
});

describe("Updates names the producer behind the blocked count", () => {
  it("says so when every blocked attempt stopped on one producer", async () => {
    const { model } = await updatesFor("archiv3-small");
    expect(model.chips.find((chip) => chip.outcome === "blocked")?.note)
      .toBe("all on cmssw_releases");
  });

  it("says nothing where no blocked attempt names a producer", async () => {
    const { model } = await updatesFor("scale-200");
    expect(model.chips.find((chip) => chip.outcome === "blocked")?.note).toBe("");
  });

  it("counts the producers when they differ", () => {
    const rows = ["a", "b", "a"].map((producer, index) => ({
      generation_id: String(index),
      full_generation_id: `okg:x:gen:${index}`,
      status: "blocked",
      created_at: "2026-09-18T01:00:00.000Z",
      duration_ms: 1,
      nodes_added: null,
      edges_added: null,
      nodes_retracted: null,
      edges_retracted: null,
      stopped_stage: "sources",
      stopped_producer: producer,
    }));
    expect(blockedNote(rows)).toBe("2 producers");
  });
});

describe("Updates points a new deployment at what is configured", () => {
  it("names the sources and links to them", async () => {
    const { model } = await updatesFor("empty");
    expect(model.configuredSentence).toBe("3 sources are configured and none has run yet.");
    expect(model.configuredLink).toBe("View sources (3)");
  });

  it("says nothing of the kind where attempts exist", async () => {
    for (const name of ["archiv3-small", "scale-200"]) {
      expect((await updatesFor(name)).model.configuredSentence, name).toBeNull();
    }
  });
});

describe("Updates states its row count once, not three times", () => {
  // Review item L7: the page said "3 attempts recorded" beside the title, "3
  // published" in the chip strip, and then drew three rows; "newest 30 h ago"
  // repeated the first row's own When cell. The chips are the outcome census
  // and the rows are the attempts; the header carried neither fact, only a
  // second and third statement of both.
  it("puts no count and no newest-age beside the title", async () => {
    for (const name of ["archiv3-small", "workspace-mixed", "scale-200"]) {
      const { model, data } = await updatesFor(name);
      const markup = listMarkup(model, data.attemptsByRange["14d"]!, data);
      const header = markup.slice(0, markup.indexOf("updates-filters"));
      expect(header, `${name} header`).not.toContain("attempts recorded");
      expect(header, `${name} header`).not.toContain("attempt recorded");
      expect(header, `${name} header`).not.toContain("newest ");
      expect(header, `${name} header`).toContain("Updates");
    }
  });

  // The census itself is untouched: it is the filter strip, and it is the one
  // place the counts are stated.
  it("keeps the outcome chips that carry the census", async () => {
    const { model, data } = await updatesFor("archiv3-small");
    const markup = listMarkup(model, data.attemptsByRange["14d"]!, data);
    expect(model.chips.length).toBeGreaterThan(0);
    expect(markup).toContain("updates-filters");
  });
});

describe("Updates says how much of the table it is showing, once", () => {
  // The count strip above the table already carries the totals. "22 of 22
  // attempts in 21 rows" under it was the same fact a second time.
  it("says nothing when the read returned every attempt", async () => {
    for (const name of ["archiv3-small", "workspace-mixed", "all-clear", "broken-runtime"]) {
      const { model } = await updatesFor(name);
      expect(model.shownSentence, name).toBe("");
    }
  });

  it("says so when the read returned one page of many", async () => {
    expect((await updatesFor("scale-200")).model.shownSentence).toBe("50 of 4,800 attempts");
  });
});

describe("Updates marks the version being read", () => {
  it("marks the row whose version the console is reading, and only it", async () => {
    const { model, data } = await updatesFor("archiv3-small");
    const reading = model.rows.filter((row) => row.reading);
    expect(reading).toHaveLength(1);
    expect(reading[0].version).toBe("eea4e0d4e88d");
    expect(data.health.payload.generation_id).toContain("eea4e0d4e88d");
    const html = listMarkup(model, data.attemptsByRange["14d"]!);
    expect(html).toContain('okg-console__tag">reading<');
  });

  it("marks nothing when no version has been published", async () => {
    const { model } = await updatesFor("empty");
    expect(model.rows).toHaveLength(0);
    expect(model.noAttemptsSentence).toBe("No update attempt has been recorded for empty.");
  });

  it("gives a blocked attempt no version, because it published none", async () => {
    const { model } = await updatesFor("archiv3-small");
    expect(model.rows[0].version).toBeNull();
    expect(model.rows[0].change).toBe("");
  });
});

describe("The attempt detail shows what the attempt recorded", () => {
  // These guards replace the ones that asserted a five-stage list with the
  // failure's position, and per-producer rows joined from the enricher and
  // view diagnostics reads. Both subjects are CUT: `stopped_stage` is never
  // recorded, so a stage list could only say "Not recorded" five times or
  // derive a position no run measured; and the diagnostics reads describe
  // whatever generation they were last taken from, not this attempt. What
  // replaces them is what IS recorded per attempt.
  async function detailOf(name: string, pick: (attempt: RawAttempt) => boolean) {
    const { data } = await updatesFor(name);
    const all = data.generations.payload.generations;
    const index = all.findIndex(pick);
    // Named, so a fixture that stops carrying the attempt this test is about
    // fails here instead of throwing on `all[-1]` several lines later.
    expect(index, `${name} has no attempt matching this test's predicate`)
      .toBeGreaterThanOrEqual(0);
    return buildAttemptDetail(all[index], {
      previous: all[index + 1] ?? null,
      nowMs: Date.parse(data.attemptsByRange["14d"]!.payload.now),
      readingVersion: data.health.payload.generation_id,
    });
  }

  function detailMarkup(detail: Awaited<ReturnType<typeof detailOf>>): string {
    return renderToStaticMarkup(
      ConsoleAttemptDetail({
        detail,
        backHref: "#updates",
        onRead: null,
        context: { deployment: "test", version: null, publishedAt: null, nowMs: 0 },
      }),
    );
  }

  it("carries no stage list and names no stage", async () => {
    const detail = await detailOf("archiv3-small", (a) => a.status === "blocked");
    expect(detail).not.toHaveProperty("stages");
    const markup = detailMarkup(detail);
    for (const stage of [
      "Publication checks", "Extraction and enrichers", "Not reached", "Not recorded",
    ]) {
      expect(markup, `${stage} is a cut subject`).not.toContain(stage);
    }
  });

  // Review item C5: the sentence stated the plumbing (a run id, a generation)
  // and a newcomer could not parse it. The fact it carries is unchanged.
  it("says in plain words that no per-source result belongs to this attempt", async () => {
    const detail = await detailOf("archiv3-small", (a) => a.status === "blocked");
    expect(detail.sourcesNote).toBe(
      "Source runs are recorded on their own, not against this attempt, "
      + "so no per-source result is shown here.",
    );
    const markup = detailMarkup(detail);
    expect(markup).toContain("so no per-source result is shown here");
    expect(markup).not.toContain("run id");
  });

  // Review item C6: "View producers" read as a control and nothing under it
  // was viewable. The section holds the attempt's producer runs, which is
  // what the recorded section on the same page is called.
  it("titles the producer section by its contents, not as a control", async () => {
    const detail = await detailOf("archiv3-small", (a) => a.status === "blocked");
    const markup = detailMarkup(detail);
    expect(markup).toContain("Producer runs");
    expect(markup).not.toContain("View producers");
  });

  it("says no per-producer result is recorded, rather than counting zero", async () => {
    const detail = await detailOf("archiv3-small", (a) => a.status === "blocked");
    expect(detail.viewRuns).toEqual([]);
    expect(detail.viewRunsNote).toBe(
      "No per-producer result is recorded for this attempt.",
    );
    const markup = detailMarkup(detail);
    expect(markup).toContain("No per-producer result is recorded");
    // Not "0 producers", which would claim the deployment measured something.
    expect(markup).not.toContain("0 producers");
  });

  it("names an attempt the read could not join, rather than reporting none", () => {
    // Three answers, never merged: the server serves no such field at all;
    // the server looked and this attempt has no run; the server could not
    // look, because the attempt records no target generation to join on.
    const detail = buildAttemptDetail(attemptWith({
      status: "published",
      view_runs: [],
      view_runs_total: 0,
      view_runs_capped: false,
      view_runs_unavailable: { reason: "target_generation_id_not_recorded" },
    }), { previous: null, nowMs: 0, readingVersion: null });

    expect(detail.viewRunsNote).toContain("target_generation_id_not_recorded");
    expect(detail.viewRunsNote).not.toContain("No per-producer result is recorded");
    expect(detailMarkup(detail)).toContain("target_generation_id_not_recorded");
  });

  it("lists a view producer's run, its measured span and its error", () => {
    const detail = buildAttemptDetail(attemptWith({
      status: "published",
      view_runs: [{
        definition_id: "view:contributor_graph",
        run_id: "r1",
        status: "failed",
        trigger_kind: "full",
        error: { message: "renderer raised", lines: ["one", "two"] },
        started_at: "2026-09-21T02:48:00.000Z",
        completed_at: "2026-09-21T02:49:04.000Z",
      }],
    }), { previous: null, nowMs: Date.parse("2026-09-21T03:00:00Z"), readingVersion: null });

    expect(detail.viewRuns).toHaveLength(1);
    const [run] = detail.viewRuns;
    expect(run.name).toBe("view:contributor_graph");
    expect(run.mark.word).toBe("Failed");
    // A view run DOES record both ends, unlike the attempt, so this span is
    // measured rather than guessed.
    expect(run.timing).toBe("took 1 m 4 s");
    expect(run.errorText).toContain("renderer raised");
    expect(detail.viewRunsNote).toBeNull();
  });

  it("names every excluded source, and uses the singular only for one", () => {
    const one = buildAttemptDetail(
      attemptWith({ excluded_sources: ["cmssw_releases"] }),
      { previous: null, nowMs: 0, readingVersion: null },
    );
    expect(one.excludedLine).toBe(
      "Source excluded by the publication gate: cmssw_releases",
    );
    const many = buildAttemptDetail(
      attemptWith({ excluded_sources: ["cmssw_releases", "papers"] }),
      { previous: null, nowMs: 0, readingVersion: null },
    );
    // Both names, not a count and not a chosen one.
    expect(many.excludedLine).toBe(
      "Sources excluded by the publication gate: cmssw_releases, papers",
    );
    const none = buildAttemptDetail(
      attemptWith({ excluded_sources: [] }),
      { previous: null, nowMs: 0, readingVersion: null },
    );
    expect(none.excludedLine).toBeNull();
  });

  it("claims no progress for an attempt that is still running", async () => {
    const detail = await detailOf("broken-runtime", (a) => a.status === "running");
    expect(detail.runningNote).toContain("has not finished");
    expect(detail.viewRunsNote).toBeNull();
  });
});

describe("The recorded blocker is one level down, verbatim and as data", () => {
  it("prints the recorded blocker as text, escaped, never as markup", () => {
    const hostile = {
      reason: "checkpoint_attestation_failed",
      detail: "<img src=x onerror=alert(1)>",
    };
    const detail = buildAttemptDetail(
      attemptWith({ status: "blocked", blocked_detail: hostile, blocked_reason: "x" }),
      { previous: null, nowMs: 0, readingVersion: null },
    );
    const markup = renderToStaticMarkup(
      ConsoleAttemptDetail({
        detail,
        backHref: "#updates",
        onRead: null,
        context: { deployment: "t", version: null, publishedAt: null, nowMs: 0 },
      }),
    );
    // The recorded value is DATA. It reaches the page escaped, so the tag is
    // shown rather than mounted.
    expect(markup).toContain("&lt;img src=x onerror=alert(1)&gt;");
    expect(markup).not.toContain("<img src=x");
  });

  it("prints every line of the record, unedited, and counts them", () => {
    const record = { reason: "consistency_publish_gate_blocked", gates: ["a", "b"] };
    const detail = buildAttemptDetail(
      attemptWith({ status: "blocked", blocked_detail: record, blocked_reason: "c" }),
      { previous: null, nowMs: 0, readingVersion: null },
    );
    expect(detail.blockedText).toBe(JSON.stringify(record, null, 2));
    expect(detail.blockedLineCount).toBe(detail.blockedText!.split("\n").length);
  });

  it("says how large a withheld record was rather than showing a blank", () => {
    const detail = buildAttemptDetail(attemptWith({
      status: "blocked",
      blocked_reason: "consistency_publish_gate_blocked",
      blocked_detail: null,
      blocked_detail_oversize: {
        reason: "blocked_detail_too_large",
        bytes: 91234,
        limit: 8192,
        keys: ["failed_consistency_gates", "reason"],
      },
    }), { previous: null, nowMs: 0, readingVersion: null });
    expect(detail.blockedOversizeLine).toContain("91,234 bytes");
    expect(detail.blockedOversizeLine).toContain("failed_consistency_gates");
    expect(detail.blockedOversizeLine).toContain("okg generation describe");
    expect(detail.blockedText).toBeNull();
  });

  it("keeps the machine code beside the record", () => {
    const detail = buildAttemptDetail(attemptWith({
      status: "blocked",
      blocked_reason: "checkpoint_attestation_failed",
      blocked_detail: { reason: "checkpoint_attestation_failed" },
    }), { previous: null, nowMs: 0, readingVersion: null });
    expect(detail.code).toBe("checkpoint_attestation_failed");
  });

  it("gives a published attempt no blocker block at all", async () => {
    const { data } = await updatesFor("all-clear");
    const all = data.generations.payload.generations;
    const detail = buildAttemptDetail(all[0], {
      previous: all[1] ?? null, nowMs: 0, readingVersion: null,
    });
    expect(detail.blockedText).toBeNull();
    expect(detail.blockedOversizeLine).toBeNull();
  });
});

describe("Updates prints a signed delta", () => {
  // `+${exact(-185)}` printed "+-185" on every version that retracted more
  // than it added, and workspace-mixed and scale-200 both have one.
  it("never prints a plus in front of a minus", async () => {
    for (const name of ["workspace-mixed", "scale-200"]) {
      const { model, data } = await updatesFor(name);
      const html = listMarkup(model, data.attemptsByRange["14d"]!);
      expect(html, name).not.toContain("+-");
    }
  });

  it("keeps the minus sign on a version that retracted more than it added", async () => {
    const { model } = await updatesFor("workspace-mixed");
    const negative = model.rows.filter((row) => row.change.includes("-15 relationships"));
    expect(negative).toHaveLength(1);
  });
});

describe("Updates pages with a cursor", () => {
  const rows = (ids: string[]): RawAttempt[] => ids.map((id) => ({
    generation_id: id,
    full_generation_id: `okg:x:gen:${id}`,
    status: "published",
    created_at: "2026-09-18T01:00:00.000Z",
    duration_ms: 1000,
    nodes_added: 1,
    edges_added: 1,
    nodes_retracted: 0,
    edges_retracted: 0,
    stopped_stage: null,
    stopped_producer: null,
  }));

  it("appends a genuine second page", async () => {
    const api = {
      async get() {
        return { payload: { generations: rows(["c", "d"]), total: 4 } };
      },
    } as never;
    const next = await loadMoreAttempts(api, "2", rows(["a", "b"]));
    expect(next.map((row) => row.generation_id)).toEqual(["c", "d"]);
  });

  // A fixture -- or a backend -- that ignores the cursor answers with the first
  // page again. Appending it would list every attempt twice.
  it("refuses a page that repeats a row already in the table", async () => {
    const api = {
      async get() {
        return { payload: { generations: rows(["a", "b"]), total: 4 } };
      },
    } as never;
    await expect(loadMoreAttempts(api, "2", rows(["a", "b"])))
      .rejects.toThrow(/already in the table/);
  });

  it("offers the next page only where the read says there is one", async () => {
    expect((await updatesFor("scale-200")).model.moreCursor).toBe("4750");
    expect((await updatesFor("archiv3-small")).model.moreCursor).toBeNull();
  });
});

describe("Updates puts the newest blocked attempt under the table", () => {
  it("picks the newest finished attempt only when it is blocked", async () => {
    const expected: Record<string, string | null> = {
      "archiv3-small": "bd2648936849",
      "broken-runtime": "3d7278f4dc58",
      "workspace-mixed": null,
      "scale-200": null,
      "all-clear": null,
      empty: null,
    };
    for (const fixture of DESIGN_FIXTURES) {
      const { data } = await updatesFor(fixture.name);
      const pick = newestBlockedAttempt(data.generations.payload.generations);
      expect(pick?.attempt.full_generation_id.slice(-12) ?? null, fixture.name)
        .toBe(expected[fixture.name]);
    }
  });

  it("skips a running attempt and reads the one that finished", async () => {
    const { data } = await updatesFor("broken-runtime");
    const pick = newestBlockedAttempt(data.generations.payload.generations);
    expect(data.generations.payload.generations[0].status).toBe("running");
    expect(pick?.attempt.status).toBe("blocked");
    expect(pick?.previous?.status).toBe("published");
  });

  it("renders what the attempt recorded, and a link to it", async () => {
    const { model, data } = await updatesFor("archiv3-small");
    const html = listMarkup(model, data.attemptsByRange["14d"]!, data);
    expect(html).toContain('data-testid="updates-inline-detail"');
    expect(html).toContain("Newest blocked attempt");
    expect(html).toContain("Open attempt");
    // The producers section replaces the stage list, which is cut. This
    // fixture records no per-producer run, so the section says so.
    expect(html).toContain('data-testid="updates-detail-producers"');
    expect(html).toContain("No per-producer result is recorded");
    expect(html).not.toContain('data-testid="updates-detail-stages"');
    // The headline no longer names a stage or a producer: nothing records
    // which stage stopped, and the row above already says what it stopped on.
    expect(html).not.toContain("Blocked on cmssw_releases<");
  });

  it("renders nothing of the kind where the newest finished attempt published", async () => {
    for (const name of ["workspace-mixed", "scale-200", "all-clear"]) {
      const { model, data } = await updatesFor(name);
      const html = listMarkup(model, data.attemptsByRange["14d"]!, data);
      expect(html, name).not.toContain("updates-inline-detail");
    }
  });
});

describe("Updates draws the metric chart on attempts by outcome", () => {
  it("opens on attempts by outcome, with the span control in the chart", async () => {
    const { model, data } = await updatesFor("scale-200");
    const html = listMarkup(model, data.attemptsByRange["14d"]!, data);
    expect(html).toContain('data-testid="updates-chart"');
    expect(html).toContain('data-metric="attempts"');
    expect(html).toContain("okg-console__bar--published");
    expect(html).toContain("okg-console__bar--blocked");
    for (const label of ["7 d", "14 d", "30 d", "90 d"]) expect(html).toContain(label);
    // The key names each colour.
    for (const word of ["published", "blocked"]) {
      expect(html).toContain(`okg-console__legend-swatch--${word}`);
    }
  });

  it("marks the row being read with the same tag style as every other annotation", async () => {
    const { model, data } = await updatesFor("archiv3-small");
    const html = listMarkup(model, data.attemptsByRange["14d"]!, data);
    expect(html).toContain('okg-console__tag">reading<');
    expect(html).not.toContain("okg-console__reading");
  });
});

describe("Updates renders on every fixture", () => {
  it("renders its blocks, and only the header and one sentence when empty", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model, data } = await updatesFor(fixture.name);
      const html = listMarkup(model, data.attemptsByRange["14d"]!, data);
      expect(html, fixture.name).toContain('data-testid="updates-header"');
      if (fixture.name === "empty") {
        expect(html).not.toContain('data-testid="updates-table"');
        expect(html).toContain("No update attempt has been recorded for empty.");
      } else {
        expect(html, fixture.name).toContain('data-testid="updates-filters"');
        expect(html, fixture.name).toContain('data-testid="updates-chart"');
        expect(html, fixture.name).toContain('data-testid="updates-table"');
      }
    }
  });

  it("prints no zero in the count strip", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const { model } = await updatesFor(fixture.name);
      for (const chip of model.chips) expect(chip.value, fixture.name).toBeGreaterThan(0);
    }
  });

  it("gives every outcome a word beside its glyph", async () => {
    const { model, data } = await updatesFor("broken-runtime");
    const html = listMarkup(model, data.attemptsByRange["14d"]!);
    for (const word of ["Published", "Blocked", "Running"]) {
      expect(html, word).toContain(`okg-console__status-word">${word}<`);
    }
  });
});

// "Read this version" is a harness-only control. It pins which generation the
// mocked views read so the operator can see the published-attempt page both
// before and after the choice; it writes nothing and calls no API. The live
// console has nowhere to put that choice -- the top bar's version picker is
// the control there -- so it passes `onRead: null` and the button must not
// render. The invariant is behavioural, not textual: the string ships inside
// AttemptDetail wherever that component ships, so a bundle string search would
// pass today only because the redesigned console is not wired live yet, and
// would red the pull request that wires it correctly. These assert the gate.
describe("\"Read this version\" is harness-only", () => {
  async function publishedDetail(name: string) {
    const { data } = await updatesFor(name);
    const all = data.generations.payload.generations;
    const index = all.findIndex((attempt) => attempt.status === "published");
    expect(index, `${name} has no published attempt`).toBeGreaterThanOrEqual(0);
    // PACT operator-console-live-updates re-aimed this helper, not the gate
    // below it. The mocked build took `producers` from the enricher and view
    // diagnostics reads and `generations` for the version row; on live data
    // the per-producer results arrive on the attempt itself, as `view_runs`
    // out of okg.derived_artifact_runs joined on target_generation_id, so
    // `buildAttemptDetail` needs neither option. The subject of these three
    // tests -- that the button renders only when a host supplies a handler --
    // is unchanged.
    return buildAttemptDetail(all[index], {
      previous: all[index + 1] ?? null,
      nowMs: Date.parse(data.attemptsByRange["14d"]!.payload.now),
      readingVersion: data.health.payload.generation_id,
    });
  }

  function markup(
    detail: Awaited<ReturnType<typeof publishedDetail>>,
    onRead: (() => void) | null,
  ): string {
    return renderToStaticMarkup(
      ConsoleAttemptDetail({
        detail,
        backHref: "#updates",
        onRead,
        context: { deployment: "test", version: null, publishedAt: null, nowMs: 0 },
      }),
    );
  }

  it("renders no button on a published attempt when the host passes no handler", async () => {
    for (const name of ["archiv3-small", "workspace-mixed", "all-clear"]) {
      const html = markup(await publishedDetail(name), null);
      expect(html, name).not.toContain("Read this version");
      expect(html, name).not.toContain("okg-console__readbutton");
    }
  });

  it("renders the button only when a handler is supplied, as the harness does", async () => {
    const html = markup(await publishedDetail("archiv3-small"), () => {});
    expect(html).toContain("Read this version");
    expect(html).toContain("okg-console__readbutton");
  });

  it("emits no form, link or method that could mutate anything", async () => {
    const html = markup(await publishedDetail("archiv3-small"), () => {});
    expect(html).not.toContain("<form");
    expect(html).not.toContain("method=");
    // A plain button with no surrounding form cannot navigate or submit; the
    // handler is the only way out, and it is a state setter in the harness.
    expect(html).toContain('<button type="button" class="okg-console__readbutton">');
  });
});
