// One chart for both pages: a metric, a span, a plot.
//
// The operator: "what about something like amazon cloud watch? I can select
// the metric and the timespan?" Home's Updates section and the Updates page
// draw this same component; Home opens on records added, Updates on attempts
// by outcome, both on 14 days.
//
// Everything numeric is in metricsModel.ts and is pure. This file measures its
// own width, draws what the geometry says, and tracks the hovered day.
import { useEffect, useMemo, useRef, useState, type ReactElement } from "react";
import { Switcher } from "@operator-ui/console/components/Switcher";

import { RANGE_LABELS, RANGES, type Range } from "@operator-ui/console/api/client";
import type { RawAttemptsByDay, RawGenerations } from "@operator-ui/console/model/shared/reads";
import { chartGeometry, PLOT_FLOOR, PLOT_TOP, TICK_Y, X_LABEL_Y } from "@operator-ui/console/model/metrics/geometry";
import { buildMetricSeries, METRICS, type MetricId } from "@operator-ui/console/model/metrics/series";
import { tooltipFor } from "@operator-ui/console/model/metrics/tooltip";

/** The nominal width a server render uses before the browser has measured the
 *  real one. 912 px is the content width at 1440. */
const NOMINAL_WIDTH = 912;

/**
 * The metric name (or picker) and the span control.
 *
 * Its own component because the chart is not the only thing that draws it: a
 * span the reader asked for that has not answered yet keeps the frame and
 * these controls, with the span it is LOADING selected. Before this pass the
 * whole block vanished and was replaced by a read-failure panel built from a
 * null error, which printed the word "null" and the sentence "The read did not
 * reach the server" about a read that was still in flight.
 */
function ChartControls({
  offered, metric, onMetric, range, onRange, testid,
}: {
  offered: typeof METRICS;
  metric: MetricId | null;
  onMetric: ((next: MetricId) => void) | null;
  range: Range;
  onRange: (next: Range) => void;
  testid: string;
}): ReactElement {
  return (
    <div className="okg-console__metric-controls">
      {offered.length > 1 && metric !== null && onMetric ? (
        <select
          className="okg-console__metric-select"
          aria-label="Metric"
          value={metric}
          onChange={(event) => onMetric(event.target.value as MetricId)}
        >
          {offered.map((candidate) => (
            <option key={candidate.id} value={candidate.id}>{candidate.label}</option>
          ))}
        </select>
      ) : (
        <span className="okg-console__metric-name" data-testid={`${testid}-metric-name`}>
          {offered[0].label}
        </span>
      )}
      <Switcher<Range>
        className="okg-console__ranges"
        label="Span"
        value={range}
        onChange={onRange}
        options={RANGES.map((candidate) => ({
          value: candidate,
          label: RANGE_LABELS[candidate],
        }))}
      />
    </div>
  );
}

/**
 * The chart's place while the span the reader just chose is still being read.
 *
 * The frame and the span control stay, and the control shows the span it is
 * LOADING -- not the old span's bars under a new label, and not a failure
 * panel. Nothing numeric is drawn: a chart of the previous span under the new
 * span's name is the one thing worse than an empty plot.
 */
export function MetricChartPending({
  range, onRange, testid, offeredMetrics,
}: {
  range: Range;
  onRange: (next: Range) => void;
  testid: string;
  offeredMetrics?: readonly MetricId[];
}): ReactElement {
  const offered = offeredMetrics
    ? METRICS.filter((candidate) => offeredMetrics.includes(candidate.id))
    : METRICS;
  return (
    <div className="okg-console__metric" data-testid={testid} data-pending="true">
      <ChartControls
        offered={offered}
        metric={null}
        onMetric={null}
        range={range}
        onRange={onRange}
        testid={testid}
      />
      <p className="okg-console__metric-empty" data-testid={`${testid}-pending`}>
        {`Reading the last ${RANGE_LABELS[range]}\u2026`}
      </p>
    </div>
  );
}

export function MetricChart({
  attempts, generations, defaultMetric, range, onRange, testid, offeredMetrics,
}: {
  attempts: RawAttemptsByDay;
  generations: RawGenerations;
  defaultMetric: MetricId;
  range: Range;
  onRange: (next: Range) => void;
  testid: string;
  /** The metrics this build can honestly draw. Omitted, all four, which is
   *  what the fixtures carry. The live console passes `["attempts"]`: the
   *  other three are derived per attempt from a duration no checkpoint
   *  records and from a records-added figure the capped generations read
   *  runs out of, so offering them would put an empty or a truncated chart
   *  behind a control that promises data. With one metric there is no
   *  select at all -- a picker with a single option is furniture -- and the
   *  metric's name is printed instead. */
  offeredMetrics?: readonly MetricId[];
}): ReactElement {
  const host = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(NOMINAL_WIDTH);
  const [metric, setMetric] = useState<MetricId>(defaultMetric);
  const [hover, setHover] = useState<number | null>(null);

  useEffect(() => {
    const node = host.current;
    if (!node || typeof ResizeObserver === "undefined") return undefined;
    const measure = () => {
      const next = node.getBoundingClientRect().width;
      if (next > 0) setWidth(next);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const offered = useMemo(
    () => (offeredMetrics
      ? METRICS.filter((candidate) => offeredMetrics.includes(candidate.id))
      : METRICS),
    [offeredMetrics],
  );

  const series = useMemo(
    () => buildMetricSeries(metric, attempts, generations),
    [metric, attempts, generations],
  );
  const plot = useMemo(() => chartGeometry(series, width), [series, width]);
  const tip = hover === null || hover >= series.days.length ? null : tooltipFor(series, hover);
  const multi = series.metric.series.length > 1;

  return (
    <div className="okg-console__metric" data-testid={testid} data-metric={metric}>
      <ChartControls
        offered={offered}
        metric={metric}
        onMetric={(next) => { setMetric(next); setHover(null); }}
        range={range}
        onRange={onRange}
        testid={testid}
      />

      {/* What the chart does not draw, before the chart itself: a reader who
          sees the bars first and the caveat after has already formed a
          number. Absent when everything the span covers is on a bar. */}
      {series.omittedLine ? (
        <p className="okg-console__metric-omitted" data-testid={`${testid}-omitted`}>
          {series.omittedLine}
        </p>
      ) : null}

      <div className="okg-console__metric-plot" ref={host} onMouseLeave={() => setHover(null)}>
        {series.emptyLine ? (
          <p className="okg-console__metric-empty" style={{ height: plot.height }}>
            {series.emptyLine}
          </p>
        ) : (
          <svg
            className="okg-console__metric-svg"
            width={width}
            height={plot.height}
            role="img"
            aria-label={`${series.metric.label} per day over ${series.range}.`}
          >
            {plot.gridlines.map((line) => (
              <g key={line.value}>
                <line
                  className={line.value === 0
                    ? "okg-console__chart-base"
                    : "okg-console__chart-grid"}
                  x1={plot.plotLeft}
                  x2={plot.plotRight}
                  y1={line.y}
                  y2={line.y}
                />
                <text
                  className="okg-console__axis-label"
                  x={plot.plotLeft - 8}
                  y={line.y + 4}
                  textAnchor="end"
                >
                  <title>{line.exact}</title>
                  {line.label}
                </text>
              </g>
            ))}
            {hover !== null && hover < plot.centres.length ? (
              <line
                className="okg-console__metric-guide"
                x1={plot.centres[hover]}
                x2={plot.centres[hover]}
                y1={PLOT_TOP}
                y2={PLOT_FLOOR}
              />
            ) : null}
            {plot.bars.map((bar) => (
              <g key={bar.day}>
                {bar.parts.map((part) => (
                  <rect
                    key={part.name}
                    className={`okg-console__bar okg-console__bar--${part.name}`}
                    x={bar.x}
                    y={part.y}
                    width={bar.width}
                    height={part.height}
                  />
                ))}
              </g>
            ))}
            {plot.lines.map((line) => (
              <g key={line.name} className={`okg-console__line okg-console__line--${line.name}`}>
                {line.paths.map((path) => (
                  <path
                    key={path}
                    className={line.faint ? "okg-console__line-faint" : "okg-console__line-main"}
                    d={path}
                  />
                ))}
                {line.faint ? null : line.points.map((point) => (
                  <circle
                    className="okg-console__line-dot"
                    key={point.day}
                    cx={point.x}
                    cy={point.y}
                    r={2.5}
                  />
                ))}
              </g>
            ))}
            {plot.ticks.map((tick) => (
              <rect
                className={`okg-console__metric-tick okg-console__metric-tick--${tick.kind}`}
                key={tick.day}
                x={tick.x}
                y={TICK_Y}
                width={tick.width}
                height={3}
              />
            ))}
            {plot.xLabels.map((label) => (
              <text
                className="okg-console__axis-label"
                key={label.index}
                x={label.x}
                y={X_LABEL_Y}
                textAnchor={label.anchor}
              >
                {label.text}
              </text>
            ))}
            {plot.centres.map((centre, index) => (
              <rect
                className="okg-console__metric-hit"
                key={series.days[index].day}
                x={Math.round((centre - plot.slot / 2) * 100) / 100}
                y={PLOT_TOP}
                width={plot.slot}
                height={PLOT_FLOOR - PLOT_TOP}
                onMouseEnter={() => setHover(index)}
              />
            ))}
          </svg>
        )}
        {tip && hover !== null ? (
          <div
            className="okg-console__metric-tip"
            role="status"
            style={{
              left: Math.min(Math.max(plot.centres[hover], plot.plotLeft + 80), width - 80),
            }}
          >
            <span className="okg-console__metric-tip-day">{tip.day}</span>
            {tip.lines.map((line) => <span key={line}>{line}</span>)}
          </div>
        ) : null}
      </div>

      {multi && !series.emptyLine ? (
        <p className="okg-console__metric-legend">
          {series.metric.series.map((def) => (
            <span key={def.name} className="okg-console__metric-key">
              <span
                className={`okg-console__legend-swatch okg-console__legend-swatch--${def.name}`}
                data-form={series.metric.form}
              />
              {def.word}
            </span>
          ))}
        </p>
      ) : null}
      {series.reachNote && !series.emptyLine
        ? <p className="okg-console__chart-note">{series.reachNote}</p>
        : null}
    </div>
  );
}
