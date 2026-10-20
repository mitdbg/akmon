// The chart's geometry: the plot box, bars, lines, ticks and labels for a
// series at a width. Pure numbers; the component draws them.
import type { Gridline, MetricSeries, XLabel } from "@operator-ui/console/model/metrics/series";

// --- geometry -------------------------------------------------------------

/** The plot is 96 px tall. Above it, 6 px so the top gridline's label has
 *  room; below it, the blocked and running ticks and the date labels. */
export const PLOT_HEIGHT = 96;
export const PLOT_TOP = 6;
export const PLOT_FLOOR = PLOT_TOP + PLOT_HEIGHT;
export const TICK_Y = PLOT_FLOOR + 4;
export const X_LABEL_Y = PLOT_FLOOR + 22;
export const SVG_HEIGHT = PLOT_FLOOR + 28;
/** The y-axis labels live in this gutter, right-aligned; the plot starts after
 *  it. 64 px holds "1 m 30 s" at 12 px with 8 px clear of the plot. */
export const AXIS_GUTTER = 64;
/** Below this plot width the middle date label is dropped. */
export const NARROW_PLOT = 560;

export interface BarPart {
  name: string;
  y: number;
  height: number;
}

export interface Bar {
  day: string;
  index: number;
  x: number;
  width: number;
  parts: BarPart[];
}

export interface LinePoint {
  x: number;
  y: number;
  day: string;
  value: number;
}

export interface Line {
  name: string;
  faint: boolean;
  /** One path per unbroken run of days that have a value. */
  paths: string[];
  points: LinePoint[];
}

export interface Tick {
  x: number;
  width: number;
  day: string;
  kind: "blocked" | "running";
}

export interface ChartGeometry {
  width: number;
  height: number;
  plotLeft: number;
  plotRight: number;
  top: number;
  floor: number;
  slot: number;
  /** The y of zero, which is the baseline. */
  zeroY: number;
  bars: Bar[];
  lines: Line[];
  ticks: Tick[];
  gridlines: (Gridline & { y: number })[];
  xLabels: (XLabel & { x: number; anchor: "start" | "middle" | "end" })[];
  /** The centre x of each day, for the hover guide. */
  centres: number[];
}

/**
 * The plot, in real pixels.
 *
 * The component measures its own width and passes it in; nothing here guesses
 * one. No viewBox: one SVG user unit is one CSS pixel, so a 1 px gridline is
 * 1 px and a bar is the width it says it is at 1440 and at 390 alike.
 */
export function chartGeometry(series: MetricSeries, width: number): ChartGeometry {
  const round = (value: number) => Math.round(value * 100) / 100;
  const plotLeft = AXIS_GUTTER;
  const plotRight = Math.max(width, plotLeft + 1);
  const plotWidth = plotRight - plotLeft;
  const count = Math.max(series.days.length, 1);
  const slot = plotWidth / count;
  const centre = (index: number) => plotLeft + slot * (index + 0.5);
  const span = series.domain.max - series.domain.min;
  const y = (value: number) => (span === 0
    ? PLOT_FLOOR
    : PLOT_FLOOR - ((value - series.domain.min) / span) * PLOT_HEIGHT);
  const zeroY = round(y(0));

  const bars: Bar[] = [];
  const lines: Line[] = [];

  if (series.metric.form !== "line") {
    series.days.forEach((day, index) => {
      const parts: BarPart[] = [];
      let up = 0;
      let down = 0;
      day.values.forEach((value, seriesIndex) => {
        if (value === null || value === 0) return;
        const name = series.metric.series[seriesIndex].name;
        if (value > 0) {
          const from = y(up);
          const to = y(up + value);
          parts.push({ name, y: round(to), height: round(from - to) });
          up += value;
        } else {
          const from = y(down);
          const to = y(down + value);
          parts.push({ name, y: round(from), height: round(to - from) });
          down += value;
        }
      });
      if (parts.length) {
        bars.push({
          day: day.day,
          index,
          x: round(plotLeft + slot * index + slot * 0.15),
          width: round(slot * 0.7),
          parts,
        });
      }
    });
  } else {
    series.metric.series.forEach((def, seriesIndex) => {
      const paths: string[] = [];
      const points: LinePoint[] = [];
      let run: LinePoint[] = [];
      const close = () => {
        if (run.length >= 2) {
          paths.push(run.map((p, i) => `${i === 0 ? "M" : "L"}${p.x} ${p.y}`).join(" "));
        }
        run = [];
      };
      series.days.forEach((day, index) => {
        const value = day.values[seriesIndex];
        if (value === null) { close(); return; }
        const point = { x: round(centre(index)), y: round(y(value)), day: day.day, value };
        run.push(point);
        points.push(point);
      });
      close();
      lines.push({ name: def.name, faint: Boolean(def.faint), paths, points });
    });
  }

  const ticks: Tick[] = [];
  series.days.forEach((day, index) => {
    const kind = day.blocked > 0 ? "blocked" : day.running > 0 ? "running" : null;
    if (!kind) return;
    const tickWidth = Math.min(slot * 0.6, 12);
    ticks.push({
      x: round(centre(index) - tickWidth / 2), width: round(tickWidth), day: day.day, kind,
    });
  });

  return {
    width,
    height: SVG_HEIGHT,
    plotLeft,
    plotRight,
    top: PLOT_TOP,
    floor: PLOT_FLOOR,
    slot: round(slot),
    zeroY,
    bars,
    lines,
    ticks,
    gridlines: series.gridlines.map((line) => ({ ...line, y: round(y(line.value)) })),
    // Under about 560 px of plot the middle date runs into the last one, which
    // carries the cut-off clock; the phone keeps the first and last.
    xLabels: series.xLabels.filter((label, index, all) => (
      plotWidth >= NARROW_PLOT || index === 0 || index === all.length - 1
    )).map((label) => {
      const last = series.days.length - 1;
      const anchor = label.index === 0 ? "start" : label.index === last ? "end" : "middle";
      const x = anchor === "start"
        ? plotLeft
        : anchor === "end" ? plotRight : round(centre(label.index));
      return { ...label, x, anchor };
    }),
    centres: series.days.map((_, index) => round(centre(index))),
  };
}
