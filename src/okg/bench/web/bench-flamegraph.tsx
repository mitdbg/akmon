import { StrictMode, useCallback } from "react";
import { createRoot } from "react-dom/client";
import { AggregateProfileFlamegraph } from "./ProfileFlamegraph";
import type { TelemetryAggregateFlamegraph } from "./telemetry";
// One stylesheet: the design tokens, the shared primitives and the flamegraph
// rules this page renders, carried over from the operator UI's tokens.css and
// primitives.css when the viewer moved under the benchmark tree.
import "./bench-flamegraph.css";

type FlamegraphNavEntry = {
  id: string;
  label: string;
  detail?: string | null;
  viewer: string;
  report: string;
};

function readPayload(): TelemetryAggregateFlamegraph {
  const element = document.getElementById("okg-flamegraph-payload");
  const raw = element?.textContent?.trim();
  if (!raw) {
    throw new Error("missing embedded flamegraph payload");
  }
  const payload = JSON.parse(raw) as TelemetryAggregateFlamegraph;
  if (payload.schema !== "okg.telemetry.aggregate_flamegraph.v1") {
    throw new Error("embedded payload is not an OKG aggregate flamegraph");
  }
  return payload;
}

function readNavigationEntries(): FlamegraphNavEntry[] {
  const element = document.getElementById("okg-flamegraph-manifest");
  const raw = element?.textContent?.trim();
  if (!raw) {
    return [];
  }
  const parsed = JSON.parse(raw) as unknown;
  if (!Array.isArray(parsed)) {
    return [];
  }
  return parsed.flatMap((entry) => {
    if (!entry || typeof entry !== "object") {
      return [];
    }
    const rawEntry = entry as Record<string, unknown>;
    if (
      typeof rawEntry.id !== "string"
      || typeof rawEntry.label !== "string"
      || typeof rawEntry.viewer !== "string"
      || typeof rawEntry.report !== "string"
    ) {
      return [];
    }
    return [{
      id: rawEntry.id,
      label: rawEntry.label,
      detail: typeof rawEntry.detail === "string" ? rawEntry.detail : null,
      viewer: rawEntry.viewer,
      report: rawEntry.report,
    }];
  });
}

function benchMetadata(payload: TelemetryAggregateFlamegraph): Record<string, unknown> {
  const raw = payload as TelemetryAggregateFlamegraph & {
    bench?: Record<string, unknown>;
  };
  return raw.bench ?? {};
}

function currentViewerName(): string {
  const rawName = window.location.pathname.split("/").pop() || "bench-flamegraph.html";
  try {
    return decodeURIComponent(rawName);
  } catch {
    return rawName;
  }
}

function FlamegraphNavigation({ entries }: { entries: FlamegraphNavEntry[] }) {
  const currentViewer = currentViewerName();
  const selected = entries.find((entry) => entry.viewer === currentViewer);
  const reportHref = selected?.report ?? "../index.html";
  const selectValue = selected?.viewer ?? "";
  const selectedIndex = selected ? entries.indexOf(selected) + 1 : null;

  return (
    <nav className="okg-bench-flamegraph-nav" aria-label="Flamegraph navigation">
      <a className="okg-bench-flamegraph-back" href={reportHref}>
        Back to report
      </a>
      {entries.length > 1 ? (
        <label className="okg-bench-flamegraph-picker">
          <span>Timing profile</span>
          <select
            aria-label="Select timing flamegraph"
            value={selectValue}
            onChange={(event) => {
              const target = event.currentTarget.value;
              if (target && target !== currentViewer) {
                window.location.href = target;
              }
            }}
          >
            {selectValue ? null : <option value="">Current viewer</option>}
            {entries.map((entry) => (
              <option key={entry.id} value={entry.viewer}>
                {entry.detail ? `${entry.label} - ${entry.detail}` : entry.label}
              </option>
            ))}
          </select>
        </label>
      ) : null}
      {selectedIndex ? (
        <span className="okg-bench-flamegraph-position">
          {selectedIndex} of {entries.length}
        </span>
      ) : null}
    </nav>
  );
}

function BenchFlamegraphApp({
  payload,
  navigationEntries,
}: {
  payload: TelemetryAggregateFlamegraph;
  navigationEntries: FlamegraphNavEntry[];
}) {
  const loadAggregate = useCallback(async () => payload, [payload]);
  const metadata = benchMetadata(payload);
  const source = payload.source === "profiles" ? "profiles" : "spans";
  const groupBy = payload.group_by === "stage" ? "stage" : "run";
  const commit = typeof metadata.commit === "string" ? metadata.commit : null;
  const experiment = typeof metadata.experiment === "string"
    ? metadata.experiment
    : payload.deployment;
  const artifact = typeof metadata.artifact === "string" ? metadata.artifact : null;
  const slowestPhase = typeof metadata.slowest_phase === "string"
    ? metadata.slowest_phase
    : null;

  return (
    <main className="okg-bench-flamegraph-shell">
      <FlamegraphNavigation entries={navigationEntries} />
      <header className="okg-bench-flamegraph-header">
        <div>
          <span>OKG CI benchmark</span>
          <h1>Timing flamegraph</h1>
        </div>
        <dl aria-label="Benchmark flamegraph metadata">
          <div>
            <dt>Experiment</dt>
            <dd>{experiment}</dd>
          </div>
          {commit ? (
            <div>
              <dt>Commit</dt>
              <dd>{commit}</dd>
            </div>
          ) : null}
          {slowestPhase ? (
            <div>
              <dt>Slowest phase</dt>
              <dd>{slowestPhase}</dd>
            </div>
          ) : null}
          {artifact ? (
            <div>
              <dt>Source artifact</dt>
              <dd>{artifact}</dd>
            </div>
          ) : null}
        </dl>
      </header>
      <section className="okg-bench-flamegraph-stage">
        <AggregateProfileFlamegraph
          sinceSeconds={payload.since_seconds}
          limit={payload.limit ?? undefined}
          source={source}
          groupBy={groupBy}
          startAt={payload.window_start ?? null}
          endAt={payload.window_end ?? null}
          loadAggregate={loadAggregate}
        />
      </section>
    </main>
  );
}

function ErrorApp({ error }: { error: unknown }) {
  return (
    <main className="okg-bench-flamegraph-shell">
      <section className="okg-bench-flamegraph-error">
        <h1>Flamegraph unavailable</h1>
        <p>{error instanceof Error ? error.message : "unknown viewer error"}</p>
      </section>
    </main>
  );
}

const root = document.getElementById("root");
if (!root) {
  throw new Error("OKG benchmark flamegraph root element was not found");
}

const appRoot = createRoot(root);

try {
  const payload = readPayload();
  const navigationEntries = readNavigationEntries();
  appRoot.render(
    <StrictMode>
      <BenchFlamegraphApp payload={payload} navigationEntries={navigationEntries} />
    </StrictMode>,
  );
} catch (error) {
  appRoot.render(
    <StrictMode>
      <ErrorApp error={error} />
    </StrictMode>,
  );
}
