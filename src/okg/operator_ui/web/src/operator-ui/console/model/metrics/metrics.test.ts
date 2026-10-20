// The metric chart's derivations, asserted against the fixtures rather than
// against a shape written here.
//
// Four metrics, each derived on archiv3-small, scale-200, broken-runtime and
// empty; the y axis; the x labels; the geometry; and the hover tooltip. The
// hover is tested on the derived series, not on a screenshot: what the tooltip
// says for a day is a pure function of the data.
import { describe, expect, it } from "vitest";

import { createFixtureApiClient } from "@operator-ui/design-harness/fixtureClient";
import { fixtureByName } from "@operator-ui/design-harness/fixtures";
import { RANGES, type Range } from "@operator-ui/console/api/client";
import { loadConsoleHome, attemptsFor } from "@operator-ui/console/api/health";
import type { RawAttemptsByDay, RawGenerations } from "@operator-ui/console/model/shared/reads";
import { AXIS_GUTTER, chartGeometry, PLOT_FLOOR, PLOT_HEIGHT } from "@operator-ui/console/model/metrics/geometry";
import { abbreviate, buildMetricSeries, METRICS, niceCount, niceDurationMs, type MetricId } from "@operator-ui/console/model/metrics/series";
import { tooltipFor } from "@operator-ui/console/model/metrics/tooltip";

async function readsFor(name: string, range: Range = "14d") {
  const fixture = fixtureByName(name);
  if (!fixture) throw new Error(`no fixture named ${name}`);
  const data = await loadConsoleHome(createFixtureApiClient(fixture));
  return { attempts: attemptsFor(data, range), generations: data.inputs.generations };
}

async function seriesFor(name: string, metric: MetricId, range: Range = "14d") {
  const { attempts, generations } = await readsFor(name, range);
  return buildMetricSeries(metric, attempts, generations);
}

/** A seven-day attempts read ending 2026-09-18 09:00 with the given days'
 *  single attempt each. */
function syntheticAttempts(outcomes: Record<string, "published" | "blocked" | "running">): RawAttemptsByDay {
  const days = ["12", "13", "14", "15", "16", "17", "18"].map((d) => `2026-09-${d}`);
  const buckets = days.map((day) => ({
    day,
    published: outcomes[day] === "published" ? 1 : 0,
    blocked: outcomes[day] === "blocked" ? 1 : 0,
    running: outcomes[day] === "running" ? 1 : 0,
  }));
  return {
    payload: {
      range: "7d",
      now: "2026-09-18T09:00:00.000Z",
      buckets,
      totals: {
        published: buckets.reduce((sum, b) => sum + b.published, 0),
        blocked: buckets.reduce((sum, b) => sum + b.blocked, 0),
        running: buckets.reduce((sum, b) => sum + b.running, 0),
      },
      other_status_count: 0,
      undated_count: 0,
    },
  };
}

function syntheticAttempt(
  day: string,
  status: "published" | "blocked" | "running",
  durationMs = 1000,
  nodesAdded = 10,
): RawGenerations["payload"]["generations"][number] {
  return {
    generation_id: 1,
    full_generation_id: `okg:x:gen:${day}`,
    status,
    created_at: `${day}T09:00:00.000Z`,
    duration_ms: durationMs,
    nodes_added: status === "published" ? nodesAdded : null,
    edges_added: status === "published" ? 5 : null,
    nodes_retracted: status === "published" ? 0 : null,
    edges_retracted: status === "published" ? 0 : null,
    stopped_stage: null,
    stopped_producer: null,
  };
}

describe("The metric list", () => {
  it("offers four metrics, each with a form that fits its fact", () => {
    expect(METRICS.map((metric) => [metric.id, metric.form])).toEqual([
      ["records", "bars"],
      ["relationships", "bars"],
      ["attempts", "stacked"],
      ["duration", "line"],
    ]);
  });
});

describe("Records and relationships added per day", () => {
  it("sums what each day's published attempts added on archiv3-small", async () => {
    const records = await seriesFor("archiv3-small", "records");
    const withValue = records.days.filter((day) => (day.values[0] ?? 0) !== 0);
    expect(withValue).toHaveLength(1);
    expect(withValue[0]).toMatchObject({ day: "2026-09-13", values: [2605], known: true });
    expect(records.domain).toEqual({ min: 0, max: 4000 });
    expect(records.gridlines.map((line) => line.label)).toEqual(["0", "2k", "4k"]);
    expect(records.gridlines[2].exact).toBe("4,000 records added");

    const relationships = await seriesFor("archiv3-small", "relationships");
    expect(relationships.days.find((day) => day.day === "2026-09-13")?.values).toEqual([2296]);
  });

  it("carries a blocked day as a zero, never as a record count", async () => {
    const records = await seriesFor("archiv3-small", "records");
    const blocked = records.days.filter((day) => day.blocked > 0);
    expect(blocked).toHaveLength(5);
    for (const day of blocked) expect(day.values).toEqual([0]);
  });

  it("stops where the capped read stops on scale-200, and says where that is", async () => {
    const records = await seriesFor("scale-200", "records");
    const unknown = records.days.filter((day) => !day.known);
    expect(unknown.length).toBeGreaterThan(0);
    for (const day of unknown) expect(day.values).toEqual([null]);
    expect(records.reachNote)
      .toBe("Records added is drawn from the newest 50 attempts, which reach back to 2026-09-14.");
    const known = records.days.filter((day) => day.known);
    expect(known[0].day).toBe("2026-09-14");
    expect(known.every((day) => (day.values[0] ?? 0) > 0)).toBe(true);
  });

  it("claims nothing beyond the read where the read covers the whole span", async () => {
    for (const name of ["archiv3-small", "broken-runtime", "workspace-mixed", "all-clear"]) {
      const records = await seriesFor(name, "records");
      expect(records.reachNote, name).toBeNull();
      expect(records.days.every((day) => day.known), name).toBe(true);
    }
  });

  it("keeps a baseline and no other gridline on a span that added nothing", () => {
    // Blocked attempts only: the span is not empty, but no day added a record.
    const series = buildMetricSeries("records", syntheticAttempts({ "2026-09-15": "blocked" }), {
      payload: { total: 1, generations: [syntheticAttempt("2026-09-15", "blocked")] },
    });
    expect(series.days.every((day) => day.values[0] === 0)).toBe(true);
    expect(series.domain.max).toBe(0);
    expect(series.gridlines.map((line) => line.value)).toEqual([0]);
    expect(series.emptyLine).toBeNull();
    expect(chartGeometry(series, 912).bars).toHaveLength(0);
  });

  it("is an empty span on the empty deployment", async () => {
    for (const metric of METRICS) {
      const series = await seriesFor("empty", metric.id);
      expect(series.emptyLine, metric.id).toBe("No attempt in this span.");
      expect(series.days.every((day) => day.published + day.blocked + day.running === 0)).toBe(true);
    }
  });
});

describe("Attempts by outcome per day", () => {
  it("is the attempts read, day for day, on every span", async () => {
    for (const name of ["archiv3-small", "scale-200", "broken-runtime", "empty"]) {
      for (const range of RANGES) {
        const { attempts, generations } = await readsFor(name, range);
        const series = buildMetricSeries("attempts", attempts, generations);
        expect(series.days.length, `${name} ${range}`).toBe(attempts.payload.buckets.length);
        series.days.forEach((day, index) => {
          const bucket = attempts.payload.buckets[index];
          expect(day.values, `${name} ${range} ${day.day}`)
            .toEqual([bucket.published, bucket.blocked, bucket.running]);
          expect(day.known).toBe(true);
        });
        expect(series.reachNote).toBeNull();
      }
    }
  });

  it("scales the axis to the busiest day's stack", async () => {
    const scale = await seriesFor("scale-200", "attempts");
    const busiest = Math.max(...scale.days.map((day) => day.published + day.blocked + day.running));
    expect(busiest).toBe(14);
    expect(scale.domain.max).toBe(20);
    expect(scale.gridlines.map((line) => line.label)).toEqual(["0", "10", "20"]);

    const small = await seriesFor("archiv3-small", "attempts");
    expect(small.domain.max).toBe(1);
    // No "0.5 attempts" gridline: the mid line exists only where it is whole.
    expect(small.gridlines.map((line) => line.value)).toEqual([0, 1]);
  });

  it("keeps a running attempt in its own series on broken-runtime", async () => {
    const series = await seriesFor("broken-runtime", "attempts");
    const running = series.days.filter((day) => (day.values[2] ?? 0) > 0);
    expect(running).toHaveLength(1);
    expect(running[0].day).toBe("2026-09-18");
  });
});

describe("Attempt duration per day", () => {
  it("takes the median and the longest finished attempt of each day", async () => {
    const series = await seriesFor("archiv3-small", "duration");
    const measured = series.days.filter((day) => day.values[0] !== null);
    // Six attempts on six days, one each: the median IS the attempt.
    expect(measured).toHaveLength(6);
    expect(measured.find((day) => day.day === "2026-09-13")?.values).toEqual([128000, 128000]);
    expect(measured.find((day) => day.day === "2026-09-18")?.values).toEqual([42000, 42000]);
    expect(series.domain.max).toBe(180000);
    expect(series.gridlines.map((line) => line.label)).toEqual(["0", "1 m 30 s", "3 m"]);
  });

  it("leaves a running attempt out: it has not finished", async () => {
    const series = await seriesFor("broken-runtime", "duration");
    const today = series.days.find((day) => day.day === "2026-09-18");
    expect(today?.running).toBe(1);
    expect(today?.values).toEqual([null, null]);
    expect(today?.finished).toBe(0);
    // The six-hour running attempt does not stretch the axis.
    expect(series.domain.max).toBe(480000);
  });

  it("separates the median from the longest on a day with many attempts", async () => {
    const series = await seriesFor("scale-200", "duration");
    const busy = series.days.filter((day) => day.finished > 2);
    expect(busy.length).toBeGreaterThan(0);
    for (const day of busy) {
      const [median, longest] = day.values;
      expect(median).not.toBeNull();
      expect(longest).not.toBeNull();
      expect(longest as number).toBeGreaterThanOrEqual(median as number);
    }
    expect(series.reachNote)
      .toBe("Attempt duration is drawn from the newest 50 attempts, which reach back to 2026-09-14.");
  });

  it("joins two consecutive measured days on broken-runtime and marks the longest faint", async () => {
    const series = await seriesFor("broken-runtime", "duration");
    const plot = chartGeometry(series, 912);
    const median = plot.lines.find((line) => line.name === "median");
    // The published attempt on the 15th and the blocked one on the 16th are
    // consecutive days, so one segment joins them; the running attempt on
    // the 18th is not a point at all.
    expect(median?.points.map((point) => point.day)).toEqual(["2026-09-15", "2026-09-16"]);
    expect(median?.paths).toHaveLength(1);
    const longest = plot.lines.find((line) => line.name === "longest");
    expect(longest?.faint).toBe(true);
  });

  it("breaks the line across a day with no finished attempt", () => {
    const series = buildMetricSeries(
      "duration",
      syntheticAttempts({ "2026-09-14": "published", "2026-09-16": "published" }),
      {
        payload: {
          total: 2,
          generations: [
            syntheticAttempt("2026-09-16", "published", 90000),
            syntheticAttempt("2026-09-14", "published", 60000),
          ],
        },
      },
    );
    const plot = chartGeometry(series, 912);
    const median = plot.lines.find((line) => line.name === "median");
    // Two dots and no segment: the 15th has no attempt, and a line across it
    // would draw a duration nobody measured.
    expect(median?.points).toHaveLength(2);
    expect(median?.paths).toHaveLength(0);
  });

  it("joins consecutive measured days and nothing else", async () => {
    const series = await seriesFor("all-clear", "duration");
    const plot = chartGeometry(series, 912);
    const median = plot.lines.find((line) => line.name === "median");
    // 15 days in a 14-day span: it reaches back fourteen WHOLE days, so it
    // holds the read's own day and the fourteen before it.
    expect(median?.points).toHaveLength(15);
    expect(median?.paths).toHaveLength(1);
    expect(median?.paths[0].split("L")).toHaveLength(15);
  });
});

describe("The axes", () => {
  it("rounds a count up to a number whose half is whole where it can", () => {
    expect([0, 1, 2, 3, 5, 7, 14, 30, 2605, 30749, 999_999].map(niceCount))
      .toEqual([0, 1, 2, 4, 5, 8, 20, 40, 4000, 40000, 1_000_000]);
  });

  it("rounds a duration up to a unit an operator reads", () => {
    expect(niceDurationMs(42000)).toBe(45000);
    expect(niceDurationMs(128000)).toBe(180000);
    expect(niceDurationMs(2400000)).toBe(2400000);
    expect(niceDurationMs(6 * 3600000)).toBe(6 * 3600000);
    expect(niceDurationMs(30 * 3600000)).toBe(48 * 3600000);
  });

  it("abbreviates with k and M and keeps the exact value for the title", () => {
    expect([0, 999, 1000, 1500, 30000, 1_000_000, 2_500_000, -185].map(abbreviate))
      .toEqual(["0", "999", "1k", "1.5k", "30k", "1M", "2.5M", "-185"]);
  });

  it("labels the first, middle and last day, and the cut-off on a partial last day", async () => {
    const series = await seriesFor("archiv3-small", "records");
    expect(series.xLabels).toEqual([
      { index: 0, text: "2026-09-04" },
      { index: 7, text: "2026-09-11" },
      { index: 14, text: "2026-09-18 · to 09:00 UTC" },
    ]);
    const wide = await seriesFor("scale-200", "attempts", "90d");
    expect(wide.days).toHaveLength(91);
    expect(wide.xLabels.map((label) => label.index)).toEqual([0, 45, 90]);
  });
});

describe("The geometry", () => {
  it("lays equal bars 70 per cent of the pitch wide, after the axis gutter", async () => {
    const series = await seriesFor("scale-200", "attempts");
    const plot = chartGeometry(series, 912);
    const pitch = (912 - AXIS_GUTTER) / 15;
    expect(plot.slot).toBeCloseTo(pitch, 2);
    expect(plot.bars).toHaveLength(15);
    for (const bar of plot.bars) {
      expect(bar.width).toBeCloseTo(pitch * 0.7, 1);
      expect(bar.x).toBeGreaterThanOrEqual(AXIS_GUTTER);
    }
    // The busiest day fills 14/20 of the plot.
    const tallest = Math.max(
      ...plot.bars.map((bar) => bar.parts.reduce((sum, part) => sum + part.height, 0)),
    );
    expect(tallest).toBeCloseTo((14 / 20) * PLOT_HEIGHT, 0);
  });

  it("stacks blocked on published from the baseline", async () => {
    const series = await seriesFor("scale-200", "attempts");
    const plot = chartGeometry(series, 912);
    const mixed = plot.bars.find((bar) => bar.parts.length === 2);
    expect(mixed).toBeDefined();
    const [published, blocked] = mixed?.parts ?? [];
    expect(published.name).toBe("published");
    expect(blocked.name).toBe("blocked");
    expect(published.y + published.height).toBeCloseTo(PLOT_FLOOR, 1);
    expect(blocked.y + blocked.height).toBeCloseTo(published.y, 1);
  });

  it("draws a bar below the baseline for a day that retracted more than it added", () => {
    const attempts: RawAttemptsByDay = {
      payload: {
        range: "7d",
        now: "2026-09-18T09:00:00.000Z",
        buckets: ["12", "13", "14", "15", "16", "17", "18"].map((d) => ({
          day: `2026-09-${d}`, published: d === "15" ? 1 : 0, blocked: 0, running: 0,
        })),
        totals: { published: 1, blocked: 0, running: 0 },
        other_status_count: 0,
        undated_count: 0,
      },
    };
    const generations: RawGenerations = {
      payload: {
        total: 1,
        generations: [{
          generation_id: 1,
          full_generation_id: "okg:x:gen:abc",
          status: "published",
          created_at: "2026-09-15T09:00:00.000Z",
          duration_ms: 1000,
          nodes_added: -185,
          edges_added: 5,
          nodes_retracted: 0,
          edges_retracted: 0,
          stopped_stage: null,
          stopped_producer: null,
        }],
      },
    };
    const series = buildMetricSeries("records", attempts, generations);
    expect(series.domain).toEqual({ min: -200, max: 0 });
    expect(series.gridlines.map((line) => line.label)).toEqual(["-200", "0"]);
    const plot = chartGeometry(series, 912);
    expect(plot.bars).toHaveLength(1);
    expect(plot.bars[0].parts[0].y).toBeCloseTo(plot.zeroY, 1);
    expect(plot.bars[0].parts[0].height).toBeCloseTo(PLOT_HEIGHT * (185 / 200), 1);
  });

  it("puts a blocked or running tick under every such day, on every metric", async () => {
    for (const metric of METRICS) {
      const series = await seriesFor("archiv3-small", metric.id);
      const plot = chartGeometry(series, 912);
      expect(plot.ticks.filter((tick) => tick.kind === "blocked"), metric.id).toHaveLength(5);
      const running = await seriesFor("broken-runtime", metric.id);
      expect(chartGeometry(running, 912).ticks.filter((tick) => tick.kind === "running"), metric.id)
        .toHaveLength(1);
    }
  });

  it("scales to the width it is given: only x changes", async () => {
    const series = await seriesFor("all-clear", "records");
    const narrow = chartGeometry(series, 358);
    const wide = chartGeometry(series, 912);
    expect(narrow.bars.map((bar) => bar.parts[0].height))
      .toEqual(wide.bars.map((bar) => bar.parts[0].height));
    expect(narrow.slot).toBeLessThan(wide.slot);
    // The phone keeps the first and last date; the middle one ran into the
    // last, which carries the cut-off clock.
    expect(wide.xLabels.map((label) => label.anchor)).toEqual(["start", "middle", "end"]);
    expect(narrow.xLabels.map((label) => label.anchor)).toEqual(["start", "end"]);
  });
});

describe("The hover tooltip", () => {
  it("says the date and the exact value for a records day", async () => {
    const series = await seriesFor("archiv3-small", "records");
    const index = series.days.findIndex((day) => day.day === "2026-09-13");
    expect(tooltipFor(series, index)).toEqual({
      day: "2026-09-13",
      lines: ["+2,605 records added", "1 published · 1 attempt"],
    });
    const blocked = series.days.findIndex((day) => day.day === "2026-09-17");
    expect(tooltipFor(series, blocked).lines).toEqual(["0 records added", "1 blocked · 1 attempt"]);
    const quiet = series.days.findIndex((day) => day.day === "2026-09-05");
    expect(tooltipFor(series, quiet).lines).toEqual(["0 records added", "no attempt"]);
  });

  it("says median and longest for a duration day, and no finished attempt where there is none", async () => {
    const series = await seriesFor("broken-runtime", "duration");
    const published = series.days.findIndex((day) => day.day === "2026-09-15");
    expect(tooltipFor(series, published).lines)
      .toEqual(["median 6 m 42 s", "longest 6 m 42 s", "1 published · 1 attempt"]);
    const running = series.days.findIndex((day) => day.day === "2026-09-18");
    expect(tooltipFor(series, running).lines)
      .toEqual(["no finished attempt", "1 running · 1 attempt"]);
  });

  it("counts every outcome for an attempts day", async () => {
    const series = await seriesFor("scale-200", "attempts");
    const mixed = series.days.findIndex((day) => day.blocked > 0 && day.published > 0);
    const tip = tooltipFor(series, mixed);
    const day = series.days[mixed];
    expect(tip.lines).toEqual([
      `${day.published} published, ${day.blocked} blocked · ${day.published + day.blocked} attempts`,
    ]);
  });

  it("says a day is beyond the read's reach rather than inventing a zero", async () => {
    const series = await seriesFor("scale-200", "records");
    const unknown = series.days.findIndex((day) => !day.known);
    expect(tooltipFor(series, unknown).lines[0]).toBe("beyond the reach of the attempts read");
  });
});
