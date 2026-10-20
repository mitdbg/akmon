// The hover tooltip: one day's facts in words.
import type { MetricDay, MetricSeries } from "@operator-ui/console/model/metrics/series";
import { exact, signed } from "@operator-ui/console/model/shared/format";
import { duration } from "@operator-ui/console/model/shared/time";

// --- the hover tooltip ----------------------------------------------------

export interface DayTooltip {
  day: string;
  lines: string[];
}

/** "1 published, 5 blocked · 6 attempts", or "no attempt". */
export function attemptsPhrase(day: MetricDay): string {
  const parts = ([
    ["published", day.published],
    ["blocked", day.blocked],
    ["running", day.running],
  ] as const)
    .filter(([, count]) => count > 0)
    .map(([word, count]) => `${exact(count)} ${word}`);
  if (!parts.length) return "no attempt";
  const total = day.published + day.blocked + day.running;
  return `${parts.join(", ")} · ${exact(total)} ${total === 1 ? "attempt" : "attempts"}`;
}

/** What the hover shows for one day: the date, then the exact values. */
export function tooltipFor(series: MetricSeries, index: number): DayTooltip {
  const day = series.days[index];
  const { metric } = series;
  const lines: string[] = [];
  if (!day.known) {
    lines.push("beyond the reach of the attempts read");
  } else if (metric.id === "records" || metric.id === "relationships") {
    lines.push(`${signed(day.values[0] ?? 0)} ${metric.series[0].word}`);
  } else if (metric.id === "duration") {
    const [med, longest] = day.values;
    if (med === null || longest === null) {
      lines.push("no finished attempt");
    } else {
      lines.push(`median ${duration(med)}`);
      lines.push(`longest ${duration(longest)}`);
    }
  }
  lines.push(attemptsPhrase(day));
  return { day: day.day, lines };
}
