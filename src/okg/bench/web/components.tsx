/* The three operator-UI primitives the flamegraph renders, and the one number
 * formatter its disclosure copy calls. Copied from the operator UI's shared
 * component and format modules so the benchmark viewer carries no dependency on
 * `src/okg/operator_ui/`; the markup, class names and formatting rules are
 * unchanged, so the stylesheet moved alongside them still applies. */

import type { PropsWithChildren, ReactNode } from "react";

export type Tone = "neutral" | "ok" | "warn" | "bad" | "info";

export function StatusDot({ tone = "neutral" }: { tone?: Tone }) {
  return <span aria-hidden="true" className="okg-status-dot" data-tone={tone} />;
}

export function DiagnosticRow({
  tone,
  title,
  meta,
  next,
}: {
  tone: Tone;
  title: string;
  meta: string;
  next?: ReactNode;
}) {
  return (
    <div className="okg-row">
      <StatusDot tone={tone} />
      <div>
        <div className="okg-row__title">{title}</div>
        <div className="okg-row__meta">{meta}</div>
      </div>
      {next ? <div className="okg-row__next">{next}</div> : null}
    </div>
  );
}

export function EmptyState({
  children,
  loading = false,
}: PropsWithChildren<{ loading?: boolean }>) {
  return (
    <div
      className={`okg-empty${loading ? " okg-empty--loading" : ""}`}
      aria-busy={loading || undefined}
      aria-live={loading ? "polite" : undefined}
    >
      {loading ? <span className="okg-empty__pulse" aria-hidden="true" /> : null}
      {children}
    </div>
  );
}

export function formatCount(
  value: number | null | undefined,
  options: { missingLabel?: string; compact?: boolean; locale?: boolean } = {},
): string {
  const { missingLabel = "not reported", compact = true, locale = false } = options;
  if (value === null || value === undefined || Number.isNaN(value)) {
    return missingLabel;
  }
  const abs = Math.abs(value);
  if (compact && abs >= 1_000_000) {
    return `${(value / 1_000_000)
      .toFixed(abs >= 10_000_000 ? 1 : 2)
      .replace(/\.0$/, "")}M`;
  }
  if (compact && abs >= 1_000) {
    return `${(value / 1_000)
      .toFixed(abs >= 100_000 ? 0 : 1)
      .replace(/\.0$/, "")}k`;
  }
  // "en-US" grouping, matching the date formatters this viewer pins. The bare
  // `toLocaleString()` took the host locale, so a host under LC_ALL=fr_FR
  // rendered "5 194" (narrow no-break space) instead of "5,194" inside
  // otherwise-English copy.
  return locale ? value.toLocaleString("en-US") : String(value);
}
