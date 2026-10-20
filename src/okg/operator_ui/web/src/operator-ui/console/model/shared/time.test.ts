// The age label and the chart's span must share ONE arithmetic.
//
// Live okg-workspace on 2026-09-22 drew "No attempt in this span" on Home's
// 14-day chart with a row three lines below it reading "14 d ago". Both were
// computed correctly and by different rules: the label floored the elapsed
// hours (14.47 days -> 14), while the span was fourteen UTC calendar days
// ending today, which starts the day AFTER a 14.47-day-old attempt.
//
// The rule now: past 48 hours an age is the number of whole UTC days between
// the two dates, and a span of n days holds the read's own day and the n
// before it (`console_views/attempts_by_day.py::_days`). So a row the page
// calls "n d ago" is inside an n-day span for every n up to the span, and the
// strip can no longer contradict the list.
import { describe, expect, it } from "vitest";

import { duration, relativeAge, utcDaysBetween } from "@operator-ui/console/model/shared/time";

/** The read time of the live payload the defect was found in. */
const NOW = Date.parse("2026-09-22T16:24:40.026Z");
const DAY = 86_400_000;

/** The span `attempts_by_day._days` composes, as ISO days, oldest first. */
function spanDays(nowMs: number, days: number): string[] {
  const end = Math.floor(nowMs / DAY) * DAY;
  return Array.from({ length: days + 1 }, (_, index) =>
    new Date(end - (days - index) * DAY).toISOString().slice(0, 10));
}

describe("relativeAge past 48 hours", () => {
  it("counts whole UTC days, not elapsed hours divided by 24", () => {
    // 14.47 days: the age of okg-workspace's newest attempt at the read above.
    expect(relativeAge("2026-09-08T05:04:40.865Z", NOW)).toBe("14 d ago");
    // Two moments on the same UTC day are the same number of days back,
    // whatever the clock says.
    expect(relativeAge("2026-09-08T23:59:59.999Z", NOW)).toBe("14 d ago");
    expect(relativeAge("2026-09-08T00:00:00.000Z", NOW)).toBe("14 d ago");
    expect(relativeAge("2026-09-07T23:59:59.999Z", NOW)).toBe("15 d ago");
  });

  it("still reports hours under two days, where a calendar day is not the unit", () => {
    expect(relativeAge("2026-09-22T14:24:40.026Z", NOW)).toBe("2 h ago");
    expect(relativeAge("2026-09-21T00:24:40.026Z", NOW)).toBe("40 h ago");
  });

  it("is the same count the span is built from", () => {
    expect(utcDaysBetween(Date.parse("2026-09-08T05:04:40.865Z"), NOW)).toBe(14);
    expect(utcDaysBetween(Date.parse("2026-09-22T00:00:00.000Z"), NOW)).toBe(0);
  });
});

describe("a row's age and the chart's span cannot disagree", () => {
  const span = spanDays(NOW, 14);

  it("holds fifteen days: the read's own and the fourteen before it", () => {
    expect(span).toHaveLength(15);
    expect(span[0]).toBe("2026-09-08");
    expect(span[14]).toBe("2026-09-22");
  });

  it.each([14.0, 14.5])(
    "puts an attempt %s days old, labelled \"14 d ago\", inside the 14-day span",
    (ageDays) => {
      const iso = new Date(NOW - ageDays * DAY).toISOString();
      expect(relativeAge(iso, NOW)).toBe("14 d ago");
      expect(span).toContain(iso.slice(0, 10));
    },
  );

  it("leaves an attempt 15 days old, labelled \"15 d ago\", outside it", () => {
    const iso = new Date(NOW - 15 * DAY).toISOString();
    expect(relativeAge(iso, NOW)).toBe("15 d ago");
    expect(span).not.toContain(iso.slice(0, 10));
  });

  it("never labels an in-span attempt with a number larger than the span", () => {
    for (const day of span) {
      for (const clock of ["00:00:00.000Z", "12:00:00.000Z", "23:59:59.999Z"]) {
        const iso = `${day}T${clock}`;
        if (Date.parse(iso) > NOW) continue;
        const age = relativeAge(iso, NOW);
        const days = /^(\d+) d ago$/.exec(age);
        if (!days) continue;
        expect(Number(days[1]), `${iso} -> ${age}`).toBeLessThanOrEqual(14);
      }
    }
  });
});

// Issue #2114: console health served 0.0 s for a publish nobody timed. The
// composer now serves null; the formatter must print that as "not recorded",
// and must print a recorded zero as the zero it is.
describe("a duration nobody recorded is never a zero", () => {
  it("prints null and a non-number as not recorded", () => {
    expect(duration(null)).toBe("duration not recorded");
    expect(duration(Number.NaN)).toBe("duration not recorded");
    expect(duration(Number.POSITIVE_INFINITY)).toBe("duration not recorded");
  });

  it("prints a recorded zero as zero", () => {
    expect(duration(0)).toBe("0 ms");
  });
});
