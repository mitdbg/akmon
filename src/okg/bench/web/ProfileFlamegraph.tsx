/* The aggregate flamegraph the benchmark viewer renders. Moved out of the
 * operator UI's Performance view, which also rendered a single-artifact
 * `ProfileFlamegraph` over a Speedscope file fetched by artifact id; the
 * benchmark viewer only ever mounts the aggregate component, so that component
 * and its `ProfileFlamegraphStatus` type did not come with it. Everything
 * below is unchanged apart from the imports. */

import { useEffect, useMemo, useRef, useState } from "react";

import { DiagnosticRow, EmptyState, formatCount } from "./components";
import type { ConvertedFlamegraph, FlamegraphNode } from "./speedscopeFlamegraph";
import { telemetryFailureDiagnostic, type TelemetryAggregateFlamegraph } from "./telemetry";

type AggregateFlamegraphStatus =
  | { kind: "idle" }
  | { kind: "loading" }
  | {
      kind: "ready";
      converted: ConvertedFlamegraph;
      updating: boolean;
      readFailure: string | null;
    }
  | {
      kind: "failed";
      reason: string;
    };

const FRAME_TIME_EPSILON = 0.001;
const FRAME_DRILLDOWN_LIMIT = 8;
const aggregateFlamegraphCache = new Map<string, ConvertedFlamegraph>();

export interface AggregateFlamegraphOptions {
  sinceSeconds?: number;
  limit?: number;
  source?: "spans" | "profiles";
  groupBy?: "run" | "stage";
  startAt?: string | null;
  endAt?: string | null;
  scope?: string;
  traceId?: string | null;
  workflowId?: string | null;
  generationId?: string | null;
  sourceName?: string | null;
  runId?: string | null;
  component?: string | null;
  signal?: AbortSignal;
}

export interface FlamegraphFocusSummary {
  name: string;
  path: number[];
  pathLabel: string;
  depth: number;
  childSummary: string;
  valueLabel: string;
}

const flamegraphStageLegend = [
  { label: "Sources", color: "var(--okg-graph-source)" },
  { label: "Chronos / SQL", color: "var(--okg-graph-family-3)" },
  { label: "Publisher", color: "var(--okg-graph-node-type)" },
  { label: "Enrichers", color: "var(--okg-graph-enricher)" },
  { label: "Views", color: "var(--okg-graph-family-2)" },
  { label: "Gates", color: "var(--okg-graph-selected)" },
  { label: "Checkpoint", color: "var(--okg-graph-layer-other)" },
  { label: "Tools", color: "var(--okg-graph-family-4)" },
  { label: "Other / gap", color: "var(--okg-color-muted)" },
];

function flamegraphFrameColor(name: string, originalColor: string): string {
  const normalized = name.toLowerCase();
  if (
    normalized.includes("self / unattributed")
    || normalized.includes("unattributed")
    || normalized.includes("gap")
  ) {
    return "var(--okg-color-muted)";
  }
  if (normalized === "sources" || normalized.includes("source.")) {
    return "var(--okg-graph-source)";
  }
  if (
    normalized.includes("chronos")
    || normalized.includes("sql")
    || normalized.includes("transaction")
    || normalized.includes("dirty_scope")
    || normalized.includes("dedupe_persisted")
  ) {
    return "var(--okg-graph-family-3)";
  }
  if (normalized.includes("enricher")) return "var(--okg-graph-enricher)";
  if (
    normalized === "views"
    || normalized.includes("view")
    || normalized.includes("native")
    || normalized.includes("effective attribute")
  ) {
    return "var(--okg-graph-family-2)";
  }
  if (normalized.includes("gate")) return "var(--okg-graph-selected)";
  if (normalized.includes("checkpoint")) return "var(--okg-graph-layer-other)";
  if (
    normalized === "tools"
    || normalized.startsWith("mcp.")
    || normalized.startsWith("tool.")
  ) {
    return "var(--okg-graph-family-4)";
  }
  if (normalized.includes("publisher")) return "var(--okg-graph-node-type)";
  return originalColor;
}

export function AggregateProfileFlamegraph({
  sinceSeconds,
  limit,
  source = "spans",
  groupBy = "run",
  startAt,
  endAt,
  scope = "system",
  traceId,
  workflowId,
  generationId,
  sourceName,
  runId,
  component,
  focusQuery,
  focusSpanId,
  onFocusChange,
  loadAggregate,
  initialPayload = null,
  cacheVersion = "live",
}: {
  sinceSeconds: number;
  limit?: number;
  source?: "spans" | "profiles";
  groupBy?: "run" | "stage";
  startAt?: string | null;
  endAt?: string | null;
  scope?: string;
  traceId?: string | null;
  workflowId?: string | null;
  generationId?: string | null;
  sourceName?: string | null;
  runId?: string | null;
  component?: string | null;
  focusQuery?: string | null;
  focusSpanId?: string | null;
  onFocusChange?: (summary: FlamegraphFocusSummary) => void;
  loadAggregate: (options: AggregateFlamegraphOptions) => Promise<TelemetryAggregateFlamegraph>;
  initialPayload?: TelemetryAggregateFlamegraph | null;
  cacheVersion?: string;
}) {
  const requestKey = useMemo(() => JSON.stringify({
    cacheVersion,
    sinceSeconds,
    limit,
    source,
    groupBy: source === "spans" ? groupBy : undefined,
    startAt,
    endAt,
    scope,
    traceId,
    workflowId,
    generationId,
    sourceName,
    runId,
    component,
  }), [
    cacheVersion,
    component,
    endAt,
    generationId,
    groupBy,
    limit,
    runId,
    scope,
    sinceSeconds,
    source,
    sourceName,
    startAt,
    traceId,
    workflowId,
  ]);
  const initialPayloadKey = initialPayload?.generated_at ?? null;
  const appliedInitialPayload = useRef(initialPayloadKey);
  const skipInitialRead = useRef(Boolean(initialPayload));
  const [status, setStatus] = useState<AggregateFlamegraphStatus>(() => {
    if (!initialPayload) {
      const cached = aggregateFlamegraphCache.get(requestKey);
      return cached
        ? { kind: "ready", converted: cached, updating: false, readFailure: null }
        : { kind: "idle" };
    }
    try {
      const converted = convertAggregateFlamegraph(initialPayload);
      aggregateFlamegraphCache.set(requestKey, converted);
      return {
        kind: "ready",
        converted,
        updating: false,
        readFailure: null,
      };
    } catch (error) {
      return {
        kind: "failed",
        reason: error instanceof Error ? error.message : "aggregate flamegraph conversion failed",
      };
    }
  });

  useEffect(() => {
    if (skipInitialRead.current) {
      skipInitialRead.current = false;
      return;
    }
    if (initialPayload && initialPayloadKey !== appliedInitialPayload.current) {
      appliedInitialPayload.current = initialPayloadKey;
      try {
        const converted = convertAggregateFlamegraph(initialPayload);
        aggregateFlamegraphCache.set(requestKey, converted);
        setStatus({
          kind: "ready",
          converted,
          updating: false,
          readFailure: null,
        });
      } catch (error) {
        setStatus({
          kind: "failed",
          reason: error instanceof Error ? error.message : "aggregate flamegraph conversion failed",
        });
      }
      return;
    }
    const cached = aggregateFlamegraphCache.get(requestKey);
    if (cached) {
      setStatus({
        kind: "ready",
        converted: cached,
        updating: false,
        readFailure: null,
      });
      return;
    }
    let cancelled = false;
    const controller = new AbortController();
    setStatus((current) => current.kind === "ready"
      ? { ...current, updating: true, readFailure: null }
      : { kind: "loading" });
    loadAggregate({
      sinceSeconds,
      limit,
      source,
      groupBy: source === "spans" ? groupBy : undefined,
      startAt,
      endAt,
      scope,
      traceId,
      workflowId,
      generationId,
      sourceName,
      runId,
      component,
      signal: controller.signal,
    }).then(
      (payload) => {
        if (!cancelled) {
          try {
            const converted = convertAggregateFlamegraph(payload);
            aggregateFlamegraphCache.set(requestKey, converted);
            setStatus({
              kind: "ready",
              converted,
              updating: false,
              readFailure: null,
            });
          } catch (error) {
            setStatus({
              kind: "failed",
              reason: error instanceof Error ? error.message : "aggregate flamegraph conversion failed",
            });
          }
        }
      },
      (error: unknown) => {
        if (!cancelled) {
          const reason = telemetryFailureDiagnostic(error, "/viz/api/telemetry/flamegraph");
          setStatus((current) => current.kind === "ready"
            ? { ...current, updating: false, readFailure: reason }
            : { kind: "failed", reason });
        }
      },
    );
    return () => {
      cancelled = true;
      controller.abort("superseded flamegraph request");
    };
  }, [
    sinceSeconds,
    limit,
    source,
    groupBy,
    startAt,
    endAt,
    scope,
    traceId,
    workflowId,
    generationId,
    sourceName,
    runId,
    component,
    loadAggregate,
    initialPayload,
    initialPayloadKey,
    requestKey,
  ]);

  if (status.kind === "loading" || status.kind === "idle") {
    return <EmptyState loading>Loading workload time for this window.</EmptyState>;
  }

  if (status.kind === "failed") {
    return (
      <div className="okg-perf-flamegraph-fallback">
        <EmptyState>Workload time unavailable: {status.reason}</EmptyState>
      </div>
    );
  }

  return (
    <div aria-busy={status.updating || undefined}>
      {status.updating ? (
        <DiagnosticRow
          tone="info"
          title="Updating workload time"
          meta="The previous flamegraph remains visible until the read completes."
        />
      ) : null}
      {status.readFailure ? (
        <DiagnosticRow tone="warn" title="Workload time refresh failed" meta={status.readFailure} />
      ) : null}
      <FlamegraphRenderer
        converted={status.converted}
        title={source === "spans"
          ? (groupBy === "stage" ? "Pipeline breakdown" : "Workload time")
          : "Profile aggregate"}
        showTopLevelList={source === "spans" && groupBy === "run"}
        showStageSemantics={source === "spans" && groupBy === "stage"}
        focusQuery={focusQuery}
        focusSpanId={focusSpanId}
        onFocusChange={onFocusChange}
      />
    </div>
  );
}

function FlamegraphRenderer({
  converted,
  title,
  showTopLevelList = false,
  showStageSemantics = false,
  focusQuery = null,
  focusSpanId = null,
  onFocusChange,
}: {
  converted: ConvertedFlamegraph;
  title: string;
  showTopLevelList?: boolean;
  showStageSemantics?: boolean;
  focusQuery?: string | null;
  focusSpanId?: string | null;
  onFocusChange?: (summary: FlamegraphFocusSummary) => void;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<{
    search(term: string): void;
    clear(): void;
    destroy(): void;
  } | null>(null);
  const [rendererError, setRendererError] = useState<string | null>(null);
  const [detail, setDetail] = useState("Hover a bar for details. Click a bar to zoom.");
  const [searchTerm, setSearchTerm] = useState("");
  const [focusPath, setFocusPath] = useState<number[]>([]);
  const [showOperationDetails, setShowOperationDetails] = useState(false);
  const [chartWidth, setChartWidth] = useState(0);
  const previousRootRef = useRef(converted.root);
  const focusNode = useMemo(
    () => nodeAtPath(converted.root, focusPath) ?? converted.root,
    [converted.root, focusPath],
  );
  const renderRoot = useMemo(
    () => renderFlamegraphNodeWithSelfTime(focusNode, converted.unit),
    [converted.unit, focusNode],
  );
  const focusTrail = useMemo(
    () => collectFocusTrail(converted.root, focusPath),
    [converted.root, focusPath],
  );
  const focusSummary = useMemo(
    () => summarizeFlamegraphFocus(focusTrail, converted.unit),
    [converted.unit, focusTrail],
  );
  const rankedFrames = useMemo(
    () => rankFlamegraphFrames(converted.root, searchTerm),
    [converted.root, searchTerm],
  );
  const focusShare = converted.root.value > 0
    ? (focusNode.value / converted.root.value) * 100
    : 0;
  const topContributors = useMemo(
    () => (focusNode.children ?? [])
      .map((node, index) => ({ node, index }))
      .filter(({ node }) => node.value > 0)
      .sort((left, right) => right.node.value - left.node.value)
      .slice(0, 4),
    [focusNode],
  );
  const producerDetailUnavailable = focusNode.name.startsWith("source-producer.")
    && topContributors.length > 0
    && topContributors.every(({ node }) => (
      humanizeFlamegraphChildName(focusNode.name, node.name) === "Recorded producer work"
    ));
  const cellHeight = 24;
  const chartMinHeight = Math.max(96, maxDepth(renderRoot) * cellHeight);
  const focused = focusPath.length > 0;

  useEffect(() => {
    const previousRoot = previousRootRef.current;
    previousRootRef.current = converted.root;
    setFocusPath((current) => remapFlamegraphPathByName(
      previousRoot,
      converted.root,
      current,
    ));
  }, [converted.root]);

  useEffect(() => {
    if (!focused) {
      setDetail("Hover a bar for details. Click a bar to zoom.");
      return;
    }
    setDetail(frameDetail(focusNode, converted.unit));
  }, [converted.unit, focusNode, focused]);

  useEffect(() => {
    onFocusChange?.(focusSummary);
  }, [focusSummary, onFocusChange]);

  useEffect(() => {
    const spanId = focusSpanId?.trim();
    if (spanId) {
      const frame = collectFrameRefs(converted.root).find((candidate) => (
        candidate.node.span_id === spanId
      ));
      if (frame) {
        setSearchTerm("");
        setFocusPath(frame.path);
        setDetail(frameDetail(frame.node, converted.unit));
        return undefined;
      }
    }
    const next = focusQuery?.trim();
    if (!next) return undefined;
    setSearchTerm(next);
    setFocusPath([]);
    setDetail(`Search matches bars containing "${next}".`);
    const timer = window.setTimeout(() => {
      chartRef.current?.search(next);
    }, 0);
    return () => window.clearTimeout(timer);
  }, [converted.root, converted.unit, focusQuery, focusSpanId]);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return undefined;
    const updateWidth = (width: number) => {
      setChartWidth(Math.max(280, Math.floor(width)));
    };
    updateWidth(element.getBoundingClientRect().width);
    const observer = new ResizeObserver((entries) => {
      const width = entries[0]?.contentRect.width;
      if (typeof width === "number") updateWidth(width);
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [converted]);

  useEffect(() => {
    if (!containerRef.current || chartWidth <= 0) return undefined;
    let cancelled = false;
    let cleanup: (() => void) | undefined;

    async function renderFlamegraph() {
      const [{ default: flamegraph }, { select }] = await Promise.all([
        import("d3-flame-graph"),
        import("d3-selection"),
      ]);
      if (cancelled || !containerRef.current) return;

      const chart = flamegraph()
        .width(chartWidth)
        .height(chartMinHeight)
        .cellHeight(cellHeight)
        .minHeight(chartMinHeight)
        .minFrameSize(1)
        .transitionDuration(180)
        .sort(false)
        .inverted(true)
        .getName((node) => {
          const parentName = node.parent?.data?.name;
          return parentName
            ? humanizeFlamegraphChildName(parentName, node.data.name)
            : humanizeFlamegraphFrameName(node.data.name);
        })
        .setLabelHandler((node) => {
          const percent = ((node.x1 - node.x0) * 100).toFixed(2);
          const parentName = node.parent?.data?.name;
          const label = parentName
            ? humanizeFlamegraphChildName(parentName, node.data.name)
            : humanizeFlamegraphFrameName(node.data.name);
          return `${label} (${percent}%, ${formatFrameValue(node.data.value, converted.unit)})`;
        })
        .setColorMapper((node, originalColor) => flamegraphFrameColor(
          node.data.name,
          originalColor,
        ))
        .setDetailsHandler((text) => {
          setDetail(text || "Hover a bar for details. Click a bar to zoom.");
        })
        .onClick((node) => {
          const relativePath = pathForRenderedNode(renderRoot, node.data);
          if (!relativePath) {
            setDetail(frameDetail(node.data, converted.unit));
            return;
          }
          const nextPath = [...focusPath, ...relativePath];
          if (nodeAtPath(converted.root, nextPath)) {
            setSearchTerm("");
            setFocusPath(nextPath);
            setDetail(frameDetail(node.data, converted.unit));
          } else {
            setDetail(frameDetail(node.data, converted.unit));
            setFocusPath((current) => [...current]);
          }
        })
        .setSearchHandler((_results, searchSum, totalValue) => {
          const percent = totalValue > 0 ? ((searchSum / totalValue) * 100).toFixed(2) : "0.00";
          setDetail(`Search matches ${searchSum.toFixed(2)} of ${totalValue.toFixed(2)} ${converted.unit} (${percent}%).`);
        });

      containerRef.current.replaceChildren();
      select(containerRef.current).datum(renderRoot).call(chart);
      const keydownCleanups: Array<() => void> = [];
      const frames = Array.from(containerRef.current.querySelectorAll<SVGGElement>("g.frame"));
      for (const frame of frames) {
        frame.setAttribute("tabindex", "0");
        frame.setAttribute("role", "button");
        const label = frame.querySelector("title")?.textContent?.trim()
          || frame.textContent?.trim()
          || "operation";
        frame.setAttribute("aria-label", `Focus ${label}`);
        const onKeyDown = (event: KeyboardEvent) => {
          if (event.key !== "Enter" && event.key !== " ") return;
          event.preventDefault();
          frame.dispatchEvent(new MouseEvent("click", {
            bubbles: true,
            cancelable: true,
            view: window,
          }));
        };
        frame.addEventListener("keydown", onKeyDown);
        keydownCleanups.push(() => frame.removeEventListener("keydown", onKeyDown));
      }
      if (searchTerm.trim() && !focused) chart.search(searchTerm.trim());
      chartRef.current = chart;

      cleanup = () => {
        keydownCleanups.forEach((dispose) => dispose());
        chart.destroy();
        containerRef.current?.replaceChildren();
        if (chartRef.current === chart) chartRef.current = null;
      };
    }

    void renderFlamegraph().catch((error: unknown) => {
      if (!cancelled) {
        setRendererError(
          error instanceof Error ? error.message : "profile flamegraph renderer failed",
        );
      }
    });
    return () => {
      cancelled = true;
      cleanup?.();
    };
  }, [renderRoot, converted.root, converted.unit, chartWidth, chartMinHeight, focusPath, focused]);

  const summary = useMemo(() => {
    const truncation = flamegraphCompletenessLabel(converted);
    const visibleFrames = focused ? ` · focused ${countFrames(renderRoot)} bars` : "";
    const normalization = flamegraphNormalizationLabel(converted);
    return `${converted.profileType} · ${converted.frameCount} bars${visibleFrames} · ${formatFrameValue(converted.totalValue, converted.unit)}${truncation}${normalization}`;
  }, [converted, focused, renderRoot]);
  const topLevelFrames = useMemo(
    () => (converted.root.children ?? []).filter((child) => child.value > 0),
    [converted],
  );

  if (rendererError) {
    return (
      <div className="okg-perf-flamegraph-fallback">
        <EmptyState>Profile flamegraph renderer failed: {rendererError}</EmptyState>
      </div>
    );
  }

  return (
    <div className="okg-perf-profile-flamegraph">
      <div className="okg-perf-profile-flamegraph__toolbar">
        <div>
          <strong>{title}</strong>
          <span>{summary}</span>
        </div>
        <button
          type="button"
          onClick={() => {
            setSearchTerm("");
            setFocusPath([]);
            chartRef.current?.clear();
          }}
        >
          Reset zoom
        </button>
      </div>
      {typeof converted.attributionPct === "number" ? (
        <div
          className="okg-perf-flamegraph-trust"
          data-tone={converted.attributionPct < 50 ? "warn" : "ok"}
          role="status"
        >
          <strong>Stage attribution {converted.attributionPct.toFixed(1)}%</strong>
          <span>
            {converted.attributionPct < 50
              ? `Coverage is low: ${Math.max(0, 100 - converted.attributionPct).toFixed(1)}% of execution time has no child timing. The bars below compare recorded stage time only.`
              : `${formatFrameValue(converted.attributedDuration ?? 0, converted.unit)} assigned to recorded pipeline operations.`}
          </span>
        </div>
      ) : null}
      <div className="okg-perf-flamegraph-focus" aria-label="Focused workload operation">
        <span>Viewing</span>
        <nav aria-label={`${title} focus path`}>
          <ol>
            {focusTrail.map((frame) => (
              <li key={frame.id}>
                <button type="button" onClick={() => setFocusPath(frame.path)}>
                  {frame.path.length === 0 ? "All stages" : humanizeFlamegraphFrameName(frame.node.name)}
                </button>
              </li>
            ))}
          </ol>
        </nav>
        <strong>{focusPath.length === 0 ? "Recorded pipeline time" : humanizeFlamegraphFrameName(focusNode.name)}</strong>
        <small>{focusShare.toFixed(1)}% of recorded time · {formatFrameValue(focusNode.value, converted.unit)}</small>
      </div>
      {showStageSemantics && topContributors.length > 0 ? (
        <section className="okg-perf-flamegraph-readout" aria-label="Top contributors to selected pipeline time">
          <div>
            <strong>{focusPath.length === 0 ? "Where recorded time went" : `Inside ${humanizeFlamegraphFrameName(focusNode.name)}`}</strong>
            <span>
              {producerDetailUnavailable
                ? "This producer reports total runtime, but no lower-level operation timing."
                : "Percentages use recorded stage time, not total execution wall time."}
            </span>
          </div>
          {producerDetailUnavailable ? (
            <p>
              <strong>{formatFrameValue(focusNode.value, converted.unit)} total recorded time</strong>
              <span>Add child spans around adapter, scan, transform, and write operations to make this producer diagnosable.</span>
            </p>
          ) : (
            <ol>
              {topContributors.map(({ node, index }) => {
                const percent = focusNode.value > 0 ? (node.value / focusNode.value) * 100 : 0;
                return (
                  <li key={`${node.name}:${index}`}>
                    <button type="button" onClick={() => setFocusPath([...focusPath, index])}>
                      <span>{humanizeFlamegraphChildName(focusNode.name, node.name)}</span>
                      <strong>{percent.toFixed(1)}%</strong>
                      <small>{formatFrameValue(node.value, converted.unit)}</small>
                    </button>
                  </li>
                );
              })}
            </ol>
          )}
        </section>
      ) : null}
      <div className="okg-perf-flamegraph-legend" aria-label="Flamegraph color legend">
        {flamegraphStageLegend.filter((item) => (
          !showStageSemantics || item.label !== "Other / gap"
        )).map((item) => (
          <span key={item.label}>
            <i style={{ background: item.color }} aria-hidden="true" />
            {item.label}
          </span>
        ))}
      </div>
      <div className="okg-perf-profile-flamegraph__detail">
        {detail === "Hover a bar for details. Click a bar to zoom."
          ? "Click a bar to inspect what makes it up. Use the path above to move back out."
          : detail}
      </div>
      <div
        ref={containerRef}
        className="okg-perf-profile-flamegraph__chart"
        style={{ minHeight: chartMinHeight }}
      />
      <section className="okg-perf-flamegraph-details">
        <button
          type="button"
          className="okg-perf-flamegraph-details__header"
          aria-expanded={showOperationDetails}
          onClick={() => setShowOperationDetails((current) => !current)}
          aria-label={`Focused operation: child timing, ${rankedFrames.length} ranked operations${
            showTopLevelList && topLevelFrames.length > 0 ? `, ${topLevelFrames.length} executions` : ""
          }`}
        >
          <span>{showOperationDetails ? "Hide full breakdown" : "See full breakdown"}</span>
          <small>
            child timing · {rankedFrames.length} largest operations
            {showTopLevelList && topLevelFrames.length > 0 ? ` · ${topLevelFrames.length} executions` : ""}
          </small>
        </button>
        {showOperationDetails ? (
          <div className="okg-perf-flamegraph-details__body">
            <FrameTimeBreakdown node={focusNode} unit={converted.unit} />
            <div className="okg-perf-flamegraph-drilldown" aria-label="Largest operations in flamegraph">
              <div>
                <strong>Largest operations</strong>
                <span>{rankedFrames.length} ranked bars{searchTerm.trim() ? " matching search" : ""}</span>
              </div>
              <ol>
                {rankedFrames.map((frame) => {
                  const label = splitFrameLabel(
                    focused
                      ? relativeFrameName(focusNode.name, frame.node.name)
                      : frame.node.name,
                  );
                  const parentLabel = frame.parentName
                    ? relativeFrameName(focusNode.name, frame.parentName)
                    : "";
                  return (
                    <li key={frame.id}>
                      <button type="button" onClick={() => setFocusPath(frame.path)}>
                        <span>{humanizeFlamegraphFrameName(label.primary)}</span>
                        {label.secondary ? <small>{label.secondary}</small> : null}
                        <small>
                          level {frame.depth} · {frameChildSummary(frame.node)}
                          {parentLabel ? ` · under ${parentLabel}` : ""}
                        </small>
                      </button>
                      <code>{formatFrameValue(frame.node.value, converted.unit)}</code>
                    </li>
                  );
                })}
              </ol>
            </div>
            {showTopLevelList && topLevelFrames.length > 0 ? (
              <div className="okg-perf-flamegraph-lanes" aria-label="Executions in window">
                <strong>Executions shown</strong>
                <ol>
                  {topLevelFrames.map((frame) => (
                    <li key={`${frame.name}:${frame.value}`}>
                      <span>
                        <strong>{splitFrameLabel(frame.name).primary}</strong>
                        {splitFrameLabel(frame.name).secondary ? (
                          <small>{splitFrameLabel(frame.name).secondary}</small>
                        ) : null}
                      </span>
                      <code>{formatFrameValue(frame.value, converted.unit)}</code>
                    </li>
                  ))}
                </ol>
              </div>
            ) : null}
          </div>
        ) : null}
      </section>
    </div>
  );
}

export interface FlamegraphFrameRef {
  id: string;
  path: number[];
  depth: number;
  node: FlamegraphNode;
  parentName: string | null;
}

export function summarizeFlamegraphFocus(
  trail: FlamegraphFrameRef[],
  unit: string,
): FlamegraphFocusSummary {
  const current = trail.at(-1);
  const node = current?.node ?? {
    name: "workload",
    value: 0,
    children: [],
  };
  return {
    name: humanizeFlamegraphFrameName(node.name),
    path: current?.path ?? [],
    pathLabel: trail
      .map((frame) => (frame.path.length === 0 ? "All stages" : humanizeFlamegraphFrameName(frame.node.name)))
      .join(" / ") || "root",
    depth: current?.depth ?? 0,
    childSummary: frameChildSummary(node),
    valueLabel: formatFrameValue(node.value, unit),
  };
}

export interface FlamegraphFrameBreakdownRow {
  name: string;
  value: number;
  percentOfSelected: number | null;
  childCount: number;
}

export interface FlamegraphFrameBreakdown {
  selectedValue: number;
  childTotal: number;
  selfTime: number;
  overlap: number;
  rows: FlamegraphFrameBreakdownRow[];
  omittedCount: number;
}

function FrameTimeBreakdown({
  node,
  unit,
}: {
  node: FlamegraphNode;
  unit: string;
}) {
  const breakdown = frameBreakdown(node);
  return (
    <div className="okg-perf-frame-breakdown" aria-label="Focused operation time breakdown">
      <div className="okg-perf-frame-breakdown__header">
        <strong>Where selected time goes</strong>
        <span>
          selected {formatFrameValue(breakdown.selectedValue, unit)}
          {breakdown.childTotal > 0
            ? ` · child bars ${formatFrameValue(breakdown.childTotal, unit)}`
            : ""}
        </span>
      </div>
      {breakdown.rows.length > 0 ? (
        <ol>
          {breakdown.rows.map((row, index) => (
            <li key={`${row.name}:${index}`}>
              <div className="okg-perf-frame-breakdown__bar">
                <i style={{ width: `${Math.min(row.percentOfSelected ?? 0, 100)}%` }} />
              </div>
              <span>{humanizeFlamegraphFrameName(row.name)}</span>
              <code>{formatFrameValue(row.value, unit)}</code>
              <small>
                {row.percentOfSelected == null
                  ? "not comparable"
                  : `${row.percentOfSelected.toFixed(1)}%`}
                {row.childCount > 0 ? ` · ${row.childCount} child bars` : ""}
              </small>
            </li>
          ))}
        </ol>
      ) : (
        <p>No child timings recorded for this bar.</p>
      )}
      {breakdown.selfTime > 0 || breakdown.overlap > 0 || breakdown.omittedCount > 0 ? (
        <div className="okg-perf-frame-breakdown__notes">
          {breakdown.selfTime > 0 ? (
            <span>parent/self time {formatFrameValue(breakdown.selfTime, unit)}</span>
          ) : null}
          {breakdown.overlap > 0 ? (
            <span>overlapping child bars +{formatFrameValue(breakdown.overlap, unit)}</span>
          ) : null}
          {breakdown.omittedCount > 0 ? (
            <span>{breakdown.omittedCount} smaller bars omitted</span>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function nodeAtPath(root: FlamegraphNode, path: number[]): FlamegraphNode | null {
  let current: FlamegraphNode | undefined = root;
  for (const index of path) {
    current = current.children?.[index];
    if (!current) return null;
  }
  return current;
}

export function remapFlamegraphPathByName(
  previousRoot: FlamegraphNode,
  nextRoot: FlamegraphNode,
  path: number[],
): number[] {
  const nextPath: number[] = [];
  let previousNode = previousRoot;
  let nextNode = nextRoot;
  for (const previousIndex of path) {
    const previousChild = previousNode.children?.[previousIndex];
    if (!previousChild) break;
    const nextIndex = (nextNode.children ?? []).findIndex(
      (child) => child.name === previousChild.name,
    );
    if (nextIndex < 0) break;
    nextPath.push(nextIndex);
    previousNode = previousChild;
    nextNode = nextNode.children?.[nextIndex] ?? nextNode;
  }
  return nextPath;
}

function pathForRenderedNode(
  root: FlamegraphNode,
  target: FlamegraphNode,
  path: number[] = [],
): number[] | null {
  if (root === target) return path;
  const children = root.children ?? [];
  for (let index = 0; index < children.length; index += 1) {
    const childPath = pathForRenderedNode(children[index], target, [...path, index]);
    if (childPath) return childPath;
  }
  return null;
}

function collectFrameRefs(
  root: FlamegraphNode,
  path: number[] = [],
  depth = 0,
  parentName: string | null = null,
): FlamegraphFrameRef[] {
  const current: FlamegraphFrameRef = {
    id: path.length ? path.join(".") : "root",
    path,
    depth,
    node: root,
    parentName,
  };
  return [
    current,
    ...(root.children ?? []).flatMap((child, index) =>
      collectFrameRefs(child, [...path, index], depth + 1, root.name)
    ),
  ];
}

function collectFocusTrail(root: FlamegraphNode, path: number[]): FlamegraphFrameRef[] {
  const trail: FlamegraphFrameRef[] = [];
  let current: FlamegraphNode | undefined = root;
  let currentPath: number[] = [];
  trail.push({
    id: "root",
    path: [],
    depth: 0,
    node: root,
    parentName: null,
  });
  for (const index of path) {
    const next: FlamegraphNode | undefined = current?.children?.[index];
    if (!next) break;
    currentPath = [...currentPath, index];
    trail.push({
      id: currentPath.join("."),
      path: currentPath,
      depth: currentPath.length,
      node: next,
      parentName: current?.name ?? null,
    });
    current = next;
  }
  return trail;
}

export function rankFlamegraphFrames(
  root: FlamegraphNode,
  searchTerm = "",
): FlamegraphFrameRef[] {
  const normalizedSearch = searchTerm.trim().toLowerCase();
  return collectFrameRefs(root)
    .filter((frame) => frame.path.length > 0)
    .filter((frame) => (
      normalizedSearch
        ? frame.node.name.toLowerCase().includes(normalizedSearch)
        : true
    ))
    .sort((left, right) => right.node.value - left.node.value)
    .slice(0, FRAME_DRILLDOWN_LIMIT);
}

export function frameBreakdown(
  node: FlamegraphNode,
  limit = 8,
): FlamegraphFrameBreakdown {
  const children = (node.children ?? [])
    .filter((child) => child.value > 0)
    .sort((left, right) => right.value - left.value);
  const visibleChildren = children.slice(0, limit);
  const childTotal = children.reduce((total, child) => total + child.value, 0);
  const selfTime = clampFrameDelta(node.value - childTotal);
  const overlap = clampFrameDelta(childTotal - node.value);
  return {
    selectedValue: node.value,
    childTotal,
    selfTime,
    overlap,
    rows: visibleChildren.map((child) => ({
      name: relativeFrameName(node.name, child.name),
      value: child.value,
      percentOfSelected: node.value > 0 ? (child.value / node.value) * 100 : null,
      childCount: (child.children ?? []).filter((grandchild) => grandchild.value > 0).length,
    })),
    omittedCount: Math.max(0, children.length - visibleChildren.length),
  };
}

export function renderFlamegraphNodeWithSelfTime(
  node: FlamegraphNode,
  unit = "units",
  minSelfTime = FRAME_TIME_EPSILON,
): FlamegraphNode {
  const children = (node.children ?? [])
    .filter((child) => child.value > 0)
    .map((child) => renderFlamegraphNodeWithSelfTime(child, unit, minSelfTime));
  const childTotal = children.reduce((total, child) => total + child.value, 0);
  const selfTime = Math.max(0, node.value - childTotal);
  const explicitChildren = [...children];
  if (children.length > 0 && selfTime > minSelfTime) {
    explicitChildren.push({
      name: `${node.name} self / unattributed`,
      value: selfTime,
      tooltip: (
        `${formatFrameValue(selfTime, unit)} in ${node.name} ` +
        "without a more specific child bar"
      ),
      children: [],
    });
  }
  return {
    ...node,
    children: explicitChildren,
  };
}

function clampFrameDelta(value: number): number {
  if (value <= FRAME_TIME_EPSILON) return 0;
  return value;
}

function relativeFrameName(parentName: string, childName: string): string {
  if (childName === parentName) {
    return lastFrameSegment(childName);
  }
  const childPrefix = `${parentName}.`;
  if (childName.startsWith(childPrefix)) {
    return childName.slice(childPrefix.length);
  }
  return splitFrameLabel(childName).primary;
}

function lastFrameSegment(name: string): string {
  const parts = name.split(".").filter(Boolean);
  return parts.at(-1) || name;
}

const FLAMEGRAPH_STAGE_NAMES = new Set([
  "Sources",
  "Chronos / SQL",
  "Publisher",
  "Enrichers",
  "Views",
  "Gates",
  "Checkpoint",
  "Tools",
]);

function titleCaseFrameWords(value: string): string {
  const normalized = value
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return "Unknown operation";
  return normalized
    .split(" ")
    .map((word, index) => {
      const lower = word.toLowerCase();
      if (lower === "sql") return "SQL";
      if (lower === "dbos") return "DBOS";
      if (lower === "mcp") return "MCP";
      if (lower === "pact") return "PACT";
      if (lower === "okg") return "OKG";
      return index === 0 ? `${lower.charAt(0).toUpperCase()}${lower.slice(1)}` : lower;
    })
    .join(" ");
}

function flamegraphOperationLabel(parts: string[]): string | null {
  const operation = parts.join(".").toLowerCase();
  if (!operation) return null;
  if (operation.includes("stream_walk")) return "Scan source records";
  if (operation.includes("adapter_run")) return "Read source";
  if (operation.includes("chronos_apply") || operation.includes("graph_store")) return "Write graph";
  if (operation.includes("canonicalize") || operation.includes("sanitize")) return "Normalize records";
  if (operation.includes("dedupe")) return "Remove duplicates";
  if (operation.includes("build_rows")) return "Build graph rows";
  if (operation.includes("record_set")) return "Compare record set";
  if (operation.includes("change_probe")) return "Check for changes";
  if (operation.includes("diagnostic")) return "Run diagnostics";
  if (operation.includes("checkpoint")) return "Checkpoint generation";
  if (operation.includes("query")) return "Query graph state";
  return titleCaseFrameWords(parts.at(-1) ?? operation);
}

export function humanizeFlamegraphFrameName(name: string): string {
  const raw = name.trim();
  if (!raw) return "Unknown operation";
  if (FLAMEGRAPH_STAGE_NAMES.has(raw)) return raw;
  if (raw === "okg-workspace workload window") return "All recorded stages";
  if (raw === "publisher overhead") return "Publisher orchestration";
  if (raw.includes("unattributed") || raw.includes("self /")) return "Unattributed time";

  if (raw.startsWith("source-producer.")) {
    const sourceName = raw.slice("source-producer.".length);
    return titleCaseFrameWords(sourceName.split(".").at(-1) ?? sourceName);
  }

  if (raw.startsWith("source.")) {
    const parts = raw.split(".").filter(Boolean);
    const producerIndex = parts[1] === "okg-workspace" ? 2 : 1;
    const producer = titleCaseFrameWords(parts[producerIndex] ?? "source");
    const operation = flamegraphOperationLabel(parts.slice(producerIndex + 1));
    return operation ? `${producer} · ${operation}` : producer;
  }

  if (raw.startsWith("dbos.workflow.")) {
    return `${titleCaseFrameWords(raw.slice("dbos.workflow.".length))} workflow`;
  }
  if (raw.startsWith("mcp.")) {
    return `${titleCaseFrameWords(raw.slice("mcp.".length))} request`;
  }
  if (raw.startsWith("publisher.")) {
    return `Publisher · ${titleCaseFrameWords(raw.slice("publisher.".length))}`;
  }

  return titleCaseFrameWords(raw.split(".").at(-1) ?? raw);
}

export function humanizeFlamegraphChildName(parentName: string, childName: string): string {
  if (!parentName.startsWith("source-producer.")) {
    return humanizeFlamegraphFrameName(childName);
  }
  const sourceName = parentName.slice("source-producer.".length);
  const sourcePrefix = `source.${sourceName}`;
  if (childName === sourcePrefix || childName === `${sourcePrefix}.unattributed`) {
    return "Recorded producer work";
  }
  if (!childName.startsWith(`${sourcePrefix}.`)) {
    return humanizeFlamegraphFrameName(childName);
  }
  return flamegraphOperationLabel(
    childName.slice(sourcePrefix.length + 1).split(".").filter(Boolean),
  ) ?? humanizeFlamegraphFrameName(childName);
}

function frameChildSummary(node: FlamegraphNode): string {
  const childCount = (node.children ?? []).filter((child) => child.value > 0).length;
  return childCount > 0 ? `${childCount} child bars` : "leaf bar";
}

function frameDetail(node: FlamegraphNode, unit: string): string {
  const tooltip = node.tooltip ? `${node.tooltip} · ` : "";
  const rawValue = typeof node.raw_value === "number" && node.raw_value > node.value
    ? ` · raw overlapping operation time ${formatFrameValue(node.raw_value, unit)}`
    : "";
  return `${tooltip}${frameChildSummary(node)} · ${formatFrameValue(node.value, unit)}${rawValue}`;
}

function formatFrameValue(value: number, unit: string): string {
  if (unit.toLowerCase().startsWith("millisecond")) {
    if (value >= 1000) return `${(value / 1000).toFixed(2)}s`;
    return `${value.toFixed(0)}ms`;
  }
  return `${value.toFixed(2)} ${unit}`;
}

function splitFrameLabel(name: string): { primary: string; secondary: string | null } {
  const separator = " - ";
  const index = name.indexOf(separator);
  if (index === -1) return { primary: name, secondary: null };
  return {
    primary: name.slice(0, index),
    secondary: name.slice(index + separator.length),
  };
}

export function convertAggregateFlamegraph(payload: TelemetryAggregateFlamegraph): ConvertedFlamegraph {
  if (payload.status !== "available") {
    throw new Error(payload.unavailable_reason || "aggregate_flamegraph_unavailable");
  }
  if (!payload.root.children?.length || payload.root.value <= 0) {
    throw new Error("aggregate flamegraph has no positive-weight bars");
  }
  const source = payload.source ?? "profiles";
  const pipelineGrouping = source === "spans" && payload.group_by === "stage";
  const profileType = source === "spans"
    ? `${pipelineGrouping ? "recorded pipeline time" : "operation wall time"} across ${payload.trace_count} executions`
    : `${payload.measure.replace("_", " ")} profiles across ${payload.trace_count} executions`;
  const profileName = payload.window_start && payload.window_end
    ? `${formatDateTime(payload.window_start)} - ${formatDateTime(payload.window_end)}`
    : `last ${formatWindow(payload.since_seconds)}`;
  const workloadTotalValue = payload.covered_duration_ms ?? payload.root.value;
  const attributedChildren = pipelineGrouping
    ? (payload.root.children ?? []).filter((child) => (
        child.value > 0 && child.name !== "Other / gap"
      ))
    : [];
  const attributedTotal = attributedChildren.reduce(
    (sum, child) => sum + child.value,
    0,
  );
  const unattributedDuration = source === "spans"
    ? Math.max(0, Math.min(
        workloadTotalValue,
        pipelineGrouping
          ? workloadTotalValue - attributedTotal
          : (payload.gap_duration_ms ?? 0),
      ))
    : undefined;
  const attributedDuration = source !== "spans"
    ? undefined
    : (pipelineGrouping
        ? attributedTotal
        : Math.max(0, workloadTotalValue - (unattributedDuration ?? 0)));
  const renderRoot = pipelineGrouping && attributedTotal > 0
    ? {
        ...payload.root,
        value: attributedTotal,
        tooltip: `${payload.root.tooltip ?? payload.root.name}; pipeline bars are normalized within recorded stage time`,
        children: attributedChildren,
      }
    : payload.root;
  const totalValue = pipelineGrouping && attributedTotal > 0
    ? attributedTotal
    : workloadTotalValue;
  return {
    root: renderRoot as FlamegraphNode,
    profileName,
    profileType,
    unit: payload.unit,
    frameCount: countFrames(payload.root as FlamegraphNode),
    totalValue,
    truncated: payload.truncated === true,
    omittedTraceCount: payload.omitted_trace_count ?? 0,
    omittedSpanCount: payload.omitted_span_count ?? 0,
    attributedDuration,
    unattributedDuration,
    attributionPct: attributedDuration === undefined || workloadTotalValue <= 0
      ? undefined
      : (attributedDuration / workloadTotalValue) * 100,
    normalizationMode: payload.normalization?.mode,
    overlapNodeCount: payload.normalization?.overlap_node_count,
    overlapDuration: payload.normalization?.overlap_duration_ms,
  };
}

export function flamegraphNormalizationLabel(converted: ConvertedFlamegraph): string {
  if (converted.normalizationMode !== "scale_overlapping_children") return "";
  const count = converted.overlapNodeCount ?? 0;
  if (count <= 0) return "";
  return ` · widths normalized for ${formatCount(count, { compact: false, locale: true })} overlapping operations; raw overlap remains in details`;
}

export function flamegraphCompletenessLabel(converted: ConvertedFlamegraph): string {
  if (!converted.truncated) return "";
  const parts: string[] = [];
  if ((converted.omittedTraceCount ?? 0) > 0) {
    parts.push(`${formatCount(converted.omittedTraceCount, { compact: false, locale: true })} executions omitted`);
  }
  if ((converted.omittedSpanCount ?? 0) > 0) {
    parts.push(`${formatCount(converted.omittedSpanCount, { compact: false, locale: true })} operations omitted`);
  }
  return parts.length > 0 ? ` · ${parts.join(" · ")}` : " · partial data";
}

function countFrames(root: FlamegraphNode): number {
  return 1 + (root.children ?? []).reduce((sum, child) => sum + countFrames(child), 0);
}

function maxDepth(root: FlamegraphNode): number {
  const childDepth = (root.children ?? []).reduce(
    (depth, child) => Math.max(depth, maxDepth(child)),
    0,
  );
  return 1 + childDepth;
}

function formatWindow(seconds: number): string {
  if (seconds >= 86_400 && seconds % 86_400 === 0) return `${seconds / 86_400}d`;
  if (seconds >= 3_600 && seconds % 3_600 === 0) return `${seconds / 3_600}h`;
  if (seconds >= 60 && seconds % 60 === 0) return `${seconds / 60}m`;
  return `${seconds}s`;
}

function formatDateTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  // "en-US" wording, host-local zone -- the Performance display contract.
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  });
}
