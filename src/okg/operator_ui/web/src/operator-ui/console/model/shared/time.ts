// Stamps, ages and durations, pinned to UTC and to the fixture's `now`, shared
// by every console model.

/** `2026-09-13 09:00 UTC`. Absolute, unambiguous, sortable, no month names to
 *  mis-read across locales. */
export function utcStamp(iso: string | null): string {
  if (!iso) return "time not recorded";
  const date = new Date(iso);
  if (Number.isNaN(date.valueOf())) return "time not recorded";
  return `${date.toISOString().slice(0, 10)} ${date.toISOString().slice(11, 16)} UTC`;
}

/**
 * `2026-09-21 02:48:08 UTC` — to the second.
 *
 * A producer can run several times inside one minute: lane-d-home-render's
 * deployment_configs has three runs spanning 33 seconds. Printed to the
 * minute, two of them read as the same instant, and a column meant to tell
 * runs apart tells the reader nothing.
 */
export function utcStampSeconds(iso: string | null): string {
  if (!iso) return "time not recorded";
  const date = new Date(iso);
  if (Number.isNaN(date.valueOf())) return "time not recorded";
  const text = date.toISOString();
  return `${text.slice(0, 10)} ${text.slice(11, 19)} UTC`;
}

/** `09:00 UTC` — the clock alone, for a sentence that already said the day. */
export function utcClock(iso: string | null): string {
  if (!iso) return "time not recorded";
  const date = new Date(iso);
  if (Number.isNaN(date.valueOf())) return "time not recorded";
  return `${date.toISOString().slice(11, 16)} UTC`;
}

/** Whole UTC days between two moments, counted the way a calendar counts
 *  them: the difference between the two UTC dates, not the elapsed hours
 *  divided by 24.
 *
 *  This is the chart's arithmetic. `console_views/attempts_by_day.py` buckets
 *  attempts by UTC day and a span of n days covers the n days before the read
 *  and the read's own day, so a row this function calls "n d ago" is inside an
 *  n-day span and one it calls "n + 1 d ago" is outside it. The two cannot
 *  disagree, which is the whole reason it exists: with the age floored from
 *  elapsed hours instead, live okg-workspace printed "No attempt in this span"
 *  on a 14-day chart three lines above a row reading "14 d ago" -- the newest
 *  attempt was 14.47 days old, so the label floored to 14 while the day it
 *  fell on sat one day outside the span. */
export function utcDaysBetween(fromMs: number, toMs: number): number {
  const day = 86_400_000;
  const startOfDay = (ms: number) => Math.floor(ms / day) * day;
  return Math.round((startOfDay(toMs) - startOfDay(fromMs)) / day);
}

export function relativeAge(iso: string | null, nowMs: number): string {
  if (!iso) return "age not recorded";
  const thenMs = Date.parse(iso);
  const ms = nowMs - thenMs;
  if (!Number.isFinite(ms)) return "age not recorded";
  if (ms < 0) return "in the future";
  const minutes = Math.floor(ms / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours} h ago`;
  // Days, counted as the chart counts them. Past 48 hours the reader is
  // placing the moment on a calendar rather than timing it, and this is the
  // one arithmetic the span and the label share.
  return `${utcDaysBetween(thenMs, nowMs)} d ago`;
}

/**
 * A duration, without a unit that is zero.
 *
 * "40 m 0 s", "6 h 0 m" and "2 m 0 s" state a fact of rounding, not of the
 * run. "40 m", "6 h" and "2 m" say the same thing in less space, and the
 * smaller unit is printed whenever it carries something.
 */
export function duration(ms: number | null): string {
  if (ms === null || !Number.isFinite(ms)) return "duration not recorded";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds} s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return seconds % 60 === 0 ? `${minutes} m` : `${minutes} m ${seconds % 60} s`;
  }
  const hours = Math.floor(minutes / 60);
  return minutes % 60 === 0 ? `${hours} h` : `${hours} h ${minutes % 60} m`;
}

/** `5 d` from `5 d ago`: the same measurement, as an adjective. */
export function ageAlone(iso: string | null, nowMs: number): string {
  return relativeAge(iso, nowMs).replace(/ ago$/, "");
}

/** `in 4 h (13:00 UTC)`, or null when the moment is not recorded. Every other
 *  time on the page is an age; a bare stamp made the reader do arithmetic to
 *  compare them. */
export function untilStamp(iso: string | null, nowMs: number): string | null {
  if (!iso) return null;
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return null;
  const clock = utcClock(iso);
  if (ms <= nowMs) return `now (${clock})`;
  const minutes = Math.round((ms - nowMs) / 60_000);
  if (minutes < 60) return `in ${minutes} min (${clock})`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `in ${hours} h (${clock})`;
  return `in ${Math.round(hours / 24)} d (${utcStamp(iso)})`;
}
