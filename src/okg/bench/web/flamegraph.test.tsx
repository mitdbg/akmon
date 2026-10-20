/* The benchmark flamegraph viewer's unit suite.
 *
 * These cases came from the operator UI's
 * routes/performance/telemetry.test.tsx when the viewer moved under the
 * benchmark tree. Only the flamegraph cases moved: the Performance view's own
 * snapshot, coverage, timeline and metric cases stayed with that view.
 */

import { describe, expect, it } from "vitest";
import {
  convertAggregateFlamegraph,
  frameBreakdown,
  flamegraphCompletenessLabel,
  flamegraphNormalizationLabel,
  humanizeFlamegraphChildName,
  humanizeFlamegraphFrameName,
  rankFlamegraphFrames,
  remapFlamegraphPathByName,
  renderFlamegraphNodeWithSelfTime,
  summarizeFlamegraphFocus,
} from "./ProfileFlamegraph";
import {
  convertSpeedscopeToFlamegraph,
  type SpeedscopeFile,
} from "./speedscopeFlamegraph";
import type { TelemetryAggregateFlamegraph } from "./telemetry";

const speedscope: SpeedscopeFile = {
  $schema: "https://www.speedscope.app/file-format-schema.json",
  shared: {
    frames: [
      { name: "source.code-structure" },
      { name: "publisher.cycle" },
    ],
  },
  profiles: [
    {
      type: "evented",
      name: "okg trace",
      unit: "milliseconds",
      startValue: 0,
      endValue: 1250,
      events: [
        { type: "O", frame: 0, at: 0 },
        { type: "C", frame: 0, at: 300 },
        { type: "O", frame: 1, at: 320 },
        { type: "C", frame: 1, at: 740 },
      ],
    },
  ],
};

const aggregateFlamegraph: TelemetryAggregateFlamegraph = {
  schema: "okg.telemetry.aggregate_flamegraph.v1",
  deployment: "okg-workspace",
  generated_at: "2026-06-29T04:00:00Z",
  since_seconds: 86400,
  window_start: "2026-06-29T03:59:00Z",
  window_end: "2026-06-29T04:01:00Z",
  limit: null,
  source: "spans",
  group_by: "run",
  profile_type: "evented",
  measure: "wall_time",
  unit: "milliseconds",
  status: "available",
  unavailable_reason: null,
  candidate_profile_count: 0,
  candidate_trace_count: 2,
  profile_count: 0,
  trace_count: 2,
  span_count: 6,
  covered_duration_ms: 1670,
  gap_duration_ms: 0,
  unavailable_profile_count: 0,
  truncated: false,
  omitted_trace_count: 0,
  root: {
    name: "okg-workspace workload window",
    value: 1670,
    children: [
      {
        name: "execution a1b2c3d4e5f6 - source.slow",
        value: 1120,
        children: [
          { name: "publisher.cycle", value: 840 },
          { name: "source.code-structure", value: 280 },
        ],
      },
      {
        name: "execution f6e5d4c3b2a1 - source.fast",
        value: 550,
        children: [
          { name: "source.git-files", value: 230 },
          { name: "publisher.cycle", value: 320 },
        ],
      },
    ],
  },
  unavailable: [],
};

describe("benchmark flamegraph viewer", () => {
  it("runs under a suite timezone pinned to UTC", () => {
    // vitest.bench-web.config.ts sets TZ=UTC and pool="forks" so the pin takes
    // effect. Assert the pin itself, so a config edit that drops either line
    // fails here by name instead of surfacing as a confusing date mismatch in
    // whichever assertion happens to render a host-zone timestamp.
    expect(Intl.DateTimeFormat().resolvedOptions().timeZone).toBe("UTC");
  });

  it("reports aggregate flamegraph attribution quality", () => {
    const converted = convertAggregateFlamegraph({
      ...aggregateFlamegraph,
      root: { ...aggregateFlamegraph.root, value: 1_000 },
      covered_duration_ms: 1_000,
      gap_duration_ms: 900,
    });
    expect(converted.attributedDuration).toBe(100);
    expect(converted.unattributedDuration).toBe(900);
    expect(converted.attributionPct).toBe(10);
  });

  it("normalizes pipeline bars within attributed time while preserving the trust ratio", () => {
    const converted = convertAggregateFlamegraph({
      ...aggregateFlamegraph,
      group_by: "stage",
      root: {
        ...aggregateFlamegraph.root,
        value: 1_000,
        children: [
          { name: "Sources", value: 60, children: [] },
          { name: "Chronos / SQL", value: 40, children: [] },
          { name: "Other / gap", value: 900, children: [] },
        ],
      },
      covered_duration_ms: 1_000,
      gap_duration_ms: 900,
    });

    expect(converted.root.value).toBe(100);
    expect(converted.totalValue).toBe(100);
    expect(converted.root.children?.map((child) => child.name)).toEqual([
      "Sources",
      "Chronos / SQL",
    ]);
    expect(converted.attributionPct).toBe(10);
    expect(converted.profileType).toContain("recorded pipeline time");
  });

  it("derives pipeline attribution from the stage bars rather than an incomplete gap counter", () => {
    const converted = convertAggregateFlamegraph({
      ...aggregateFlamegraph,
      group_by: "stage",
      root: {
        ...aggregateFlamegraph.root,
        value: 1_000,
        children: [
          { name: "Sources", value: 100, children: [] },
          { name: "Other / gap", value: 900, children: [] },
        ],
      },
      covered_duration_ms: 1_000,
      gap_duration_ms: 200,
    });

    expect(converted.attributedDuration).toBe(100);
    expect(converted.unattributedDuration).toBe(900);
    expect(converted.attributionPct).toBe(10);
  });

  it("converts Speedscope artifacts to flamegraph data", () => {
    // The operator UI paired this conversion with an ApiClient read of
    // /viz/api/telemetry/artifacts/<id>. The viewer has no ApiClient and no
    // artifact route, so only the conversion contract moved.
    const converted = convertSpeedscopeToFlamegraph(speedscope);
    expect(converted.profileType).toBe("evented profile");
    expect(converted.unit).toBe("milliseconds");
    expect(converted.root.value).toBe(1250);
    expect(converted.root.children?.map((child) => child.name)).toEqual([
      "source.code-structure",
      "publisher.cycle",
    ]);
  });

  it("preserves omitted span counts in flamegraph completeness copy", () => {
    const converted = convertAggregateFlamegraph({
      ...aggregateFlamegraph,
      truncated: true,
      candidate_trace_count: 9,
      candidate_span_count: 5_294,
      trace_count: 8,
      span_count: 100,
      omitted_trace_count: 0,
      omitted_span_count: 5_194,
    });

    expect(converted.omittedTraceCount).toBe(0);
    expect(converted.omittedSpanCount).toBe(5_194);
    expect(flamegraphCompletenessLabel(converted)).toBe(
      " · 5,194 operations omitted",
    );
  });

  it("discloses when flamegraph widths are normalized for overlapping operations", () => {
    const converted = convertAggregateFlamegraph({
      ...aggregateFlamegraph,
      normalization: {
        mode: "scale_overlapping_children",
        overlap_node_count: 151,
        overlap_duration_ms: 1_121_989.426,
        reason: "overlapping children",
      },
    });

    expect(converted.normalizationMode).toBe("scale_overlapping_children");
    expect(converted.overlapNodeCount).toBe(151);
    expect(converted.overlapDuration).toBe(1_121_989.426);
    expect(flamegraphNormalizationLabel(converted)).toBe(
      " · widths normalized for 151 overlapping operations; raw overlap remains in details",
    );
  });

  it("ranks flamegraph frames for explicit drilldown", () => {
    const frames = rankFlamegraphFrames({
      name: "window",
      value: 2000,
      children: [
        {
          name: "execution abc - ingest.run",
          value: 1200,
          children: [
            {
              name: "publisher.cycle",
              value: 800,
              children: [
                {
                  name: "publisher.cycle.enrichers",
                  value: 260,
                  children: [
                    { name: "publisher.cycle.enrichers.linker.enrich", value: 180 },
                  ],
                },
                {
                  name: "publisher.cycle.native_views",
                  value: 220,
                  children: [
                    { name: "publisher.cycle.native_views.graph_outline.build_corpus", value: 90 },
                  ],
                },
                {
                  name: "publisher.cycle.chronos_dirty_scope.query",
                  value: 140,
                },
              ],
            },
          ],
        },
      ],
    }, "chronos");

    expect(frames.map((frame) => frame.node.name)).toEqual([
      "publisher.cycle.chronos_dirty_scope.query",
    ]);
    expect(frames[0].depth).toBe(3);
    expect(frames[0].parentName).toBe("publisher.cycle");

    const allFrames = rankFlamegraphFrames({
      name: "window",
      value: 2000,
      children: [
        { name: "source.git-files", value: 400 },
        { name: "publisher.cycle.enrichers.linker.enrich", value: 650 },
        { name: "publisher.cycle.native_views.graph_outline.build_corpus", value: 250 },
      ],
    });
    expect(allFrames.map((frame) => frame.node.name)).toEqual([
      "publisher.cycle.enrichers.linker.enrich",
      "source.git-files",
      "publisher.cycle.native_views.graph_outline.build_corpus",
    ]);
  });

  it("summarizes selected frame child timings and overlap", () => {
    const breakdown = frameBreakdown({
      name: "publisher.cycle.native_views.bench_symbol_calls",
      value: 250,
      children: [
        { name: "publisher.cycle.native_views.bench_symbol_calls.build_corpus", value: 225 },
        { name: "publisher.cycle.native_views.bench_symbol_calls.lineage", value: 110 },
        { name: "publisher.cycle.native_views.bench_symbol_calls.schedule", value: 75 },
      ],
    });

    expect(breakdown.rows.map((row) => row.name)).toEqual([
      "build_corpus",
      "lineage",
      "schedule",
    ]);
    expect(breakdown.childTotal).toBe(410);
    expect(breakdown.selfTime).toBe(0);
    expect(breakdown.overlap).toBe(160);
    expect(breakdown.rows[0].percentOfSelected).toBe(90);
  });

  it("renders self time as an explicit flamegraph child", () => {
    const rendered = renderFlamegraphNodeWithSelfTime({
      name: "ingest.run",
      value: 100,
      children: [
        { name: "source.git-files", value: 40 },
        { name: "publisher.cycle", value: 25 },
      ],
    }, "milliseconds");

    expect(rendered.children?.map((child) => child.name)).toEqual([
      "source.git-files",
      "publisher.cycle",
      "ingest.run self / unattributed",
    ]);
    expect(rendered.children?.[2]).toMatchObject({
      value: 35,
      tooltip: "35ms in ingest.run without a more specific child bar",
      children: [],
    });
    expect(renderFlamegraphNodeWithSelfTime({
      name: "leaf",
      value: 20,
    }, "milliseconds").children).toEqual([]);
  });

  it("summarizes flamegraph focus for the page-level selection rail", () => {
    const summary = summarizeFlamegraphFocus([
      {
        id: "root",
        path: [],
        depth: 0,
        node: { name: "okg-workspace workload window", value: 1670 },
        parentName: null,
      },
      {
        id: "0",
        path: [0],
        depth: 1,
        node: {
          name: "execution a1b2c3d4e5f6 - source.slow",
          value: 1120,
          children: [{ name: "publisher.cycle", value: 840 }],
        },
        parentName: "okg-workspace workload window",
      },
      {
        id: "0.0",
        path: [0, 0],
        depth: 2,
        node: { name: "publisher.cycle", value: 840, children: [] },
        parentName: "execution a1b2c3d4e5f6 - source.slow",
      },
    ], "milliseconds");

    expect(summary).toMatchObject({
      name: "Publisher · Cycle",
      path: [0, 0],
      pathLabel: "All stages / Slow / Publisher · Cycle",
      depth: 2,
      childSummary: "leaf bar",
      valueLabel: "840ms",
    });
  });

  it("turns raw flamegraph operations into operator-facing labels", () => {
    expect(humanizeFlamegraphFrameName("source.okg-workspace.git-history"))
      .toBe("Git history");
    expect(humanizeFlamegraphFrameName("source.okg-workspace.git-history.stream_walk"))
      .toBe("Git history · Scan source records");
    expect(humanizeFlamegraphFrameName("source.okg-workspace.pact.adapter_run"))
      .toBe("PACT · Read source");
    expect(humanizeFlamegraphFrameName("source.okg-workspace.git-history.flush.chronos_apply"))
      .toBe("Git history · Write graph");
    expect(humanizeFlamegraphFrameName("publisher overhead"))
      .toBe("Publisher orchestration");
    expect(humanizeFlamegraphFrameName("source-producer.okg-workspace.git-history"))
      .toBe("Git history");
    expect(humanizeFlamegraphChildName(
      "source-producer.okg-workspace.git-history",
      "source.okg-workspace.git-history.stream_walk",
    )).toBe("Scan source records");
    expect(humanizeFlamegraphChildName(
      "source-producer.okg-workspace.git-history",
      "source.okg-workspace.git-history",
    )).toBe("Recorded producer work");
  });

  it("keeps flamegraph drill-down focused when refreshed values reorder siblings", () => {
    const previous = {
      name: "root",
      value: 100,
      children: [
        { name: "Sources", value: 70, children: [{ name: "source-producer.git", value: 50 }] },
        { name: "Publisher", value: 30 },
      ],
    };
    const next = {
      name: "root",
      value: 120,
      children: [
        { name: "Publisher", value: 70 },
        { name: "Sources", value: 50, children: [{ name: "source-producer.git", value: 45 }] },
      ],
    };

    expect(remapFlamegraphPathByName(previous, next, [0, 0])).toEqual([1, 0]);
  });
});
