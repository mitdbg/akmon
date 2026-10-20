// The metric chart's model: one metric over one span, as a series of days.
//
// The operator asked for "something like Amazon CloudWatch: I can select the
// metric and the timespan". One chart component serves Home's Updates section
// and the Updates page; this module is what it draws, and it is pure, so every
// number on the plot and in its hover tooltip is a unit test rather than a
// screenshot.
//
// Two reads feed it, and they do not reach equally far. The attempts-by-day
// read covers every day in the span, so attempts by outcome and the blocked and
// running ticks are complete. Records, relationships and durations have to come
// from the generations read, which is CAPPED at 50 rows: where that cap cuts
// the span short, the older days carry `null` rather than a zero, the shape
// stops there, and `reachNote` says how far back the numbers go. A zero would
// say "nothing happened that day", which is not what the read knows.
//
// The form follows the fact. A daily count is a bar: one per day, so an empty
// day is an empty slot and not a point on a line. A duration is a line with a
// dot on each day that has attempts and a break across days that have none:
// interpolating across an empty day would draw a duration nobody measured.
import { exact } from "@operator-ui/console/model/shared/format";
import type { RawAttemptsByDay, RawGenerations } from "@operator-ui/console/model/shared/reads";
import { duration, utcClock } from "@operator-ui/console/model/shared/time";

export type MetricId = "records" | "relationships" | "attempts" | "duration";
export type MetricForm = "bars" | "stacked" | "line";
export type MetricUnit = "count" | "duration";

export interface SeriesDef {
  name: string;
  /** The word the legend and the tooltip use. */
  word: string;
  /** Drawn faint and without dots: a second reading beside the main one. */
  faint?: boolean;
}

export interface MetricDef {
  id: MetricId;
  /** The option in the metric select. */
  label: string;
  form: MetricForm;
  unit: MetricUnit;
  /** The series the form draws, in stacking order for bars and drawing order
   *  for lines. */
  series: SeriesDef[];
  /** True when the metric is derived from the capped generations read and can
   *  therefore run out before the span does. */
  fromGenerations: boolean;
}

export const METRICS: readonly MetricDef[] = [
  {
    id: "records",
    label: "Records added",
    form: "bars",
    unit: "count",
    series: [{ name: "records", word: "records added" }],
    fromGenerations: true,
  },
  {
    id: "relationships",
    label: "Relationships added",
    form: "bars",
    unit: "count",
    series: [{ name: "relationships", word: "relationships added" }],
    fromGenerations: true,
  },
  {
    id: "attempts",
    label: "Attempts by outcome",
    form: "stacked",
    unit: "count",
    series: [
      { name: "published", word: "published" },
      { name: "blocked", word: "blocked" },
      { name: "running", word: "running" },
    ],
    fromGenerations: false,
  },
  {
    id: "duration",
    label: "Attempt duration",
    form: "line",
    unit: "duration",
    series: [
      { name: "median", word: "median" },
      { name: "longest", word: "longest", faint: true },
    ],
    fromGenerations: true,
  },
];

export function metricById(id: MetricId): MetricDef {
  const found = METRICS.find((metric) => metric.id === id);
  if (!found) throw new Error(`no metric is named ${id}`);
  return found;
}

export interface MetricDay {
  day: string;
  /** One value per series, in the metric's series order. Null where the read
   *  the metric depends on does not reach, or where a duration has no
   *  finished attempt to be measured from. */
  values: (number | null)[];
  /** From the attempts read, complete for every day in the span. */
  published: number;
  blocked: number;
  running: number;
  /** True when the generations read reaches this day. Always true for a
   *  metric drawn from the attempts read. */
  known: boolean;
  /** Finished attempts the generations read holds for this day. */
  finished: number;
}

export interface Gridline {
  value: number;
  /** Abbreviated, for the axis: "1.5k", "3k", "2 m 30 s". */
  label: string;
  /** Exact, for the title tooltip: "1,500 records added". */
  exact: string;
}

export interface XLabel {
  index: number;
  text: string;
}

export interface MetricSeries {
  metric: MetricDef;
  range: string;
  days: MetricDay[];
  /** The y domain, already widened to a round number. */
  domain: { min: number; max: number };
  gridlines: Gridline[];
  /** Under the first, middle and last day. */
  xLabels: XLabel[];
  /** Null when the read behind the metric covers the whole span. */
  reachNote: string | null;
  /** Null unless there is no attempt at all in the span. */
  emptyLine: string | null;
  /** What the chart does NOT draw, when the read says some attempts are not
   *  in it. Null when every attempt the span covers is on a bar. */
  omittedLine: string | null;
}

/** `2026-09-18` from an ISO stamp. */
export function dayOf(iso: string): string {
  return iso.slice(0, 10);
}

export function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

/**
 * A round number at or above `value`, for the top of the axis.
 *
 * The candidates all halve to a whole number except 1 and 5, so a mid
 * gridline exists for most maxima and is left out for the rest rather than
 * printed as "2.5 attempts".
 */
export function niceCount(value: number): number {
  if (value <= 0) return 0;
  const exponent = 10 ** Math.floor(Math.log10(value));
  const mantissa = value / exponent;
  const pick = [1, 2, 4, 5, 6, 8, 10].find((candidate) => candidate >= mantissa) ?? 10;
  return pick * exponent;
}

/** A round duration at or above `ms`, in the units an operator reads: whole
 *  seconds under a minute, minutes that divide an hour above it, then hours. */
export function niceDurationMs(ms: number): number {
  if (ms <= 0) return 0;
  const seconds = ms / 1000;
  if (seconds <= 60) {
    return ([5, 10, 15, 20, 30, 45, 60].find((s) => s >= seconds) ?? 60) * 1000;
  }
  const minutes = seconds / 60;
  const minuteSteps = [
    2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 30, 40, 45, 60, 90, 120, 180, 240, 360, 480, 720, 1440,
  ];
  const step = minuteSteps.find((m) => m >= minutes);
  if (step) return step * 60000;
  return Math.ceil(minutes / 1440) * 1440 * 60000;
}

/** "1.5k", "3k", "2M", or the exact number below a thousand. */
export function abbreviate(value: number): string {
  const sign = value < 0 ? "-" : "";
  const size = Math.abs(value);
  const trim = (n: number) => (Number.isInteger(n) ? String(n) : n.toFixed(1).replace(/\.0$/, ""));
  if (size >= 1_000_000) return `${sign}${trim(size / 1_000_000)}M`;
  if (size >= 1000) return `${sign}${trim(size / 1000)}k`;
  return `${sign}${exact(size)}`;
}

function axisLabel(value: number, unit: MetricUnit): string {
  return unit === "duration" ? (value === 0 ? "0" : duration(value)) : abbreviate(value);
}

function axisExact(value: number, metric: MetricDef): string {
  if (metric.unit === "duration") return value === 0 ? "0" : duration(value);
  return `${exact(value)} ${metric.id === "attempts" ? "attempts" : metric.series[0].word}`;
}

function gridlinesFor(metric: MetricDef, min: number, max: number): {
  domain: { min: number; max: number };
  gridlines: Gridline[];
} {
  const niceMax = metric.unit === "duration" ? niceDurationMs(max) : niceCount(max);
  const niceMin = min < 0 ? -niceCount(-min) : 0;
  const values: number[] = [];
  if (niceMin < 0) values.push(niceMin);
  values.push(0);
  if (niceMax > 0) {
    const mid = niceMax / 2;
    if (metric.unit === "duration" || Number.isInteger(mid)) values.push(mid);
    values.push(niceMax);
  }
  return {
    domain: { min: niceMin, max: niceMax },
    gridlines: values.map((value) => ({
      value,
      label: axisLabel(value, metric.unit),
      exact: axisExact(value, metric),
    })),
  };
}

/** One metric over the span the attempts read covers. */
export function buildMetricSeries(
  id: MetricId,
  attempts: RawAttemptsByDay,
  generations: RawGenerations,
): MetricSeries {
  const metric = metricById(id);
  const buckets = attempts.payload.buckets;
  const rows = generations.payload.generations;
  const capped = rows.length < generations.payload.total;
  const reach = capped && rows.length ? dayOf(rows[rows.length - 1].created_at) : null;

  const byDay = new Map<string, { records: number; relationships: number; durations: number[] }>();
  for (const attempt of rows) {
    const day = dayOf(attempt.created_at);
    const entry = byDay.get(day) ?? { records: 0, relationships: 0, durations: [] };
    if (attempt.status === "published") {
      entry.records += attempt.nodes_added ?? 0;
      entry.relationships += attempt.edges_added ?? 0;
    }
    // A running attempt's duration is how long it has run so far, which is not
    // a duration anyone finished measuring.
    if (attempt.status !== "running" && attempt.duration_ms !== null) {
      entry.durations.push(attempt.duration_ms);
    }
    byDay.set(day, entry);
  }

  const days: MetricDay[] = buckets.map((bucket) => {
    const known = !metric.fromGenerations || reach === null || bucket.day >= reach;
    const entry = byDay.get(bucket.day);
    let values: (number | null)[];
    if (!known) {
      values = metric.series.map(() => null);
    } else if (metric.id === "records") {
      values = [entry?.records ?? 0];
    } else if (metric.id === "relationships") {
      values = [entry?.relationships ?? 0];
    } else if (metric.id === "attempts") {
      values = [bucket.published, bucket.blocked, bucket.running];
    } else {
      const durations = entry?.durations ?? [];
      values = durations.length
        ? [median(durations), Math.max(...durations)]
        : [null, null];
    }
    return {
      day: bucket.day,
      values,
      published: bucket.published,
      blocked: bucket.blocked,
      running: bucket.running,
      known,
      finished: entry?.durations.length ?? 0,
    };
  });

  let max = 0;
  let min = 0;
  for (const day of days) {
    const present = day.values.filter((value): value is number => value !== null);
    if (!present.length) continue;
    if (metric.form === "stacked") {
      max = Math.max(max, present.reduce((sum, value) => sum + value, 0));
    } else {
      max = Math.max(max, ...present);
      min = Math.min(min, ...present);
    }
  }

  const nowDay = dayOf(attempts.payload.now);
  const last = buckets.length - 1;
  const labelFor = (index: number) => {
    const day = buckets[index].day;
    // The last day is usually partial: the span ends at the read, not at
    // midnight, and a short last column read as a slowdown rather than as
    // nine hours of a day. The cut-off is the fact that settles it.
    return index === last && day === nowDay
      ? `${day} · to ${utcClock(attempts.payload.now)}`
      : day;
  };
  const xLabels: XLabel[] = buckets.length
    ? [...new Set([0, Math.floor(last / 2), last])].map((index) => ({
      index,
      text: labelFor(index),
    }))
    : [];

  const anyAttempt = days.some((day) => day.published + day.blocked + day.running > 0);
  // A line with no point on it is not a chart. Archiv3 recorded no duration
  // on any of its three attempts, and the Attempt duration panel drew an
  // empty plot over a lone "0" axis (review 2026-09-22) -- a zero nobody
  // measured. The same absence, said in words.
  const anyDuration = metric.id !== "duration"
    || days.some((day) => day.values.some((value) => value !== null));

  return {
    metric,
    range: attempts.payload.range,
    days,
    ...gridlinesFor(metric, min, max),
    xLabels,
    reachNote: metric.fromGenerations && reach && buckets.length && reach > buckets[0].day
      ? `${metric.label} is drawn from the newest ${exact(rows.length)} attempts, `
        + `which reach back to ${reach}.`
      : null,
    emptyLine: !anyAttempt
      ? "No attempt in this span."
      : anyDuration ? null : "No attempt in this span recorded a duration.",
    omittedLine: omittedAttemptsLine(attempts),
  };
}

/**
 * What the chart leaves out, in the read's own numbers.
 *
 * The attempts read counts every checkpoint it could not place on a bar: one
 * whose status is not published, blocked or running, and one whose timestamp
 * it could not read. Without this line a deployment with forty attempts in
 * some other status would show a chart of zeros and say nothing at all, which
 * is the failure the whole read exists to avoid.
 */
export function omittedAttemptsLine(attempts: RawAttemptsByDay): string | null {
  const { other_status_count: otherStatus, undated_count: undated } = attempts.payload;
  const parts: string[] = [];
  if (otherStatus > 0) {
    parts.push(
      `${exact(otherStatus)} ${otherStatus === 1 ? "attempt" : "attempts"} `
      + `with another status ${otherStatus === 1 ? "is" : "are"} not drawn`,
    );
  }
  if (undated > 0) {
    parts.push(
      `${exact(undated)} ${undated === 1 ? "attempt" : "attempts"} `
      + `without a readable timestamp ${undated === 1 ? "is" : "are"} not drawn`,
    );
  }
  return parts.length ? `${parts.join("; ")}.` : null;
}
