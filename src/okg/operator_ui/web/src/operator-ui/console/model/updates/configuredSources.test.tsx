// Updates' configured-source count comes from `console/producers`, sources
// only (issue #2257, PACT unknown-s6-updates-producers).
//
// The sentence a deployment with no attempt gets -- "N sources are configured
// and none has run yet." -- used to count the rows of `source-diagnostics`.
// That read lists every key in the count rollup's `source` dimension,
// including derived `_enricher:` keys no registry declares (`in_registry:
// false`), so it counted enrichers as sources. The client below answers BOTH
// routes, the diagnostics one with two such enricher rows, and records every
// route asked: the count must be the producers read's two sources, and the
// diagnostics route must never be asked.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { loadConsoleUpdates } from "@operator-ui/console/api/generations";
import { buildUpdatesModel } from "@operator-ui/console/model/updates/list";
import { EMPTY_FILTER } from "@operator-ui/console/model/updates/filters";
import { ConsoleUpdatesState } from "@operator-ui/console/views/Updates";
import { consoleLink } from "@operator-ui/routes/console/ConsoleRoute";
import { ApiRequestError, type ApiClient } from "@operator-ui/shared/api/apiClient";

const PRODUCERS = "/viz/api/console/producers";
const SOURCE_DIAGNOSTICS = "/viz/api/console/source-diagnostics";

const HEALTH = {
  payload: {
    deployment: "fresh-deployment",
    generation_id: null,
    generation_status: "none",
    generation_age: null,
    verdict: "healthy",
    vitals: { queue_total: 0, dlq_count: 0, nodes_live: 0, edges_live: 0 },
    checks: [],
  },
};

const NO_ATTEMPTS = {
  payload: { deployment: "fresh-deployment", generations: [], total: 0, shown: 0 },
};

const VERSIONS = { payload: { versions: [], latest: null } };

const ATTEMPTS = {
  payload: {
    range: "14d",
    now: "2026-09-27T12:00:00.000Z",
    buckets: [],
    totals: { published: 0, blocked: 0, running: 0 },
    other_status_count: 0,
    undated_count: 0,
  },
};

function producer(kind: "source" | "enricher" | "view", name: string) {
  return {
    name,
    ownership_id: name,
    row_key: `${kind}:${name}`,
    kind,
    configured: true,
    health: { status: "ok", incident_count: 0 },
    workflow: { state: null, latest_issue: null },
    contribution: { records_in_gen: 0, edges_in_gen: 0, horizon: "pinned_generation" },
    freshness: { status: "not_evaluated" },
    latest_run: null,
  };
}

/** Two sources, one enricher (the one the `_enricher:` row IS), one view. */
const PRODUCERS_READ = {
  payload: {
    deployment: "fresh-deployment",
    generation_id: null,
    producer_count: 4,
    producers: [
      producer("source", "git-files"),
      producer("source", "papers"),
      producer("enricher", "code_reference"),
      producer("view", "file_summary"),
    ],
  },
};

function diagnosticsRow(name: string, inRegistry: boolean) {
  return {
    name,
    source_key: name,
    in_registry: inRegistry,
    health: { status: "ok", incident_count: 0 },
    workflow: { state: null },
    contribution: { records_in_gen: 0, edges_in_gen: 0 },
    freshness: { status: "not_evaluated" },
  };
}

/** The shape okg-workspace returned: the two sources plus two enricher keys
 *  the registry does not declare. Four rows, two of them not sources. */
const SOURCE_DIAGNOSTICS_READ = {
  payload: {
    deployment: "fresh-deployment",
    sources: [
      diagnosticsRow("git-files", true),
      diagnosticsRow("papers", true),
      diagnosticsRow("_enricher:code_reference", false),
      diagnosticsRow("_enricher:declarative:agent_message_mentions_pact_change_dir", false),
    ],
  },
};

function recordingClient(failing: Record<string, unknown> = {}, overrides: Record<string, unknown> = {}) {
  const asked: string[] = [];
  const answers: Record<string, unknown> = {
    "/viz/api/console/health": HEALTH,
    "/viz/api/console/generations": NO_ATTEMPTS,
    "/viz/api/console/versions": VERSIONS,
    "/viz/api/console/attempts": ATTEMPTS,
    [PRODUCERS]: PRODUCERS_READ,
    [SOURCE_DIAGNOSTICS]: SOURCE_DIAGNOSTICS_READ,
    ...overrides,
  };
  const client = {
    async get<T>(route: string): Promise<T> {
      asked.push(route);
      if (route in failing) throw failing[route];
      if (!(route in answers)) throw new Error(`no answer for ${route}`);
      return answers[route] as T;
    },
  } as ApiClient;
  return { client, asked };
}

async function modelFor(client: ApiClient) {
  const data = await loadConsoleUpdates(client);
  const model = buildUpdatesModel(
    { ...data, attempts: data.attemptsByRange["14d"] ?? null },
    EMPTY_FILTER,
    consoleLink,
  );
  return { data, model };
}

function pageMarkup(data: Awaited<ReturnType<typeof loadConsoleUpdates>>): string {
  return renderToStaticMarkup(
    <ConsoleUpdatesState
      data={data}
      error={null}
      extra={[]}
      attemptId={null}
      link={consoleLink}
      params={{}}
      readingVersion={null}
      onRead={null}
      range="14d"
      onRange={() => undefined}
      moreError={null}
      loadingMore={false}
      onLoadMore={() => undefined}
    />,
  );
}

describe("Updates counts configured sources from the producers read", () => {
  it("counts sources only: no enricher row, no view, no diagnostics key", async () => {
    const { client } = recordingClient();
    const { model, data } = await modelFor(client);
    expect(model.configuredSentence).toBe("2 sources are configured and none has run yet.");
    expect(model.configuredLink).toBe("View sources (2)");
    expect(model.configuredHref).toBe("#console/sources");
    const markup = pageMarkup(data);
    expect(markup).toContain("2 sources are configured and none has run yet.");
    expect(markup).not.toContain("4 sources");
  });

  it("makes no source-diagnostics request, and reads producers", async () => {
    const { client, asked } = recordingClient();
    await loadConsoleUpdates(client);
    expect(asked).not.toContain(SOURCE_DIAGNOSTICS);
    expect(asked).toContain(PRODUCERS);
  });

  it("says the count is unknown, with no number, when producers fails", async () => {
    const failure = new ApiRequestError({
      route: PRODUCERS,
      status: 503,
      responseText: JSON.stringify({ detail: { reason: "producers_composer_failed" } }),
    });
    const { client, asked } = recordingClient({ [PRODUCERS]: failure });
    const { model, data } = await modelFor(client);
    // The page still renders: this read feeds one sentence.
    expect(model.noAttemptsSentence).toBe(
      "No update attempt has been recorded for fresh-deployment.",
    );
    expect(model.configuredSentence).toBe(
      "The number of configured sources is unknown because that read failed.",
    );
    expect(model.configuredSentence).not.toMatch(/\d/);
    expect(model.configuredLink).toBe("View sources");
    // Still no fallback to the diagnostics read.
    expect(asked).not.toContain(SOURCE_DIAGNOSTICS);
    const markup = pageMarkup(data);
    expect(markup).toContain("The number of configured sources is unknown because that read failed.");
    expect(markup).toContain(PRODUCERS);
    expect(markup).not.toMatch(/\d+ sources? (is|are) configured/);
  });
});

describe("Updates' count is unknown when the producers read left enrichers out", () => {
  // What `console_views/producers.py` returns when its enricher read fails:
  // the `_enricher:` source row is NOT folded into its enricher, so it stays a
  // row of kind `source` (unregistered), and `omitted_data` names the enricher
  // kind as missing. Counting kind `source` here would print the #2257
  // miscount -- 3, of which 1 is an enricher.
  const ENRICHERS_OMITTED = {
    payload: {
      deployment: "fresh-deployment",
      generation_id: null,
      producer_count: 3,
      producers: [
        producer("source", "git-files"),
        producer("source", "papers"),
        { ...producer("source", "_enricher:code_reference"), configured: false },
      ],
      read_authority: {
        enrichers: { status: "unavailable", reason: "enricher_diagnostics_composer_failed" },
      },
      omitted_data: [{ authority: "enricher_diagnostics", kind: "enricher" }],
    },
  };

  it("says the count is unknown and why, with no number", async () => {
    const { client, asked } = recordingClient({}, { [PRODUCERS]: ENRICHERS_OMITTED });
    const { model, data } = await modelFor(client);
    expect(model.configuredSentence).toBe(
      "The number of configured sources is unknown because the list of enrichers could "
      + "not be read, so some enrichers may be listed as sources.",
    );
    expect(model.configuredSentence).not.toMatch(/\d/);
    expect(model.configuredLink).toBe("View sources");
    expect(asked).not.toContain(SOURCE_DIAGNOSTICS);
    expect(pageMarkup(data)).not.toMatch(/\d+ sources? (is|are) configured/);
  });

  it("still counts when an omission cost only a field, not a kind", async () => {
    const fieldOnly = {
      payload: {
        ...PRODUCERS_READ.payload,
        omitted_data: [{ authority: "runtime_readiness", affects: "source_fields" }],
      },
    };
    const { client } = recordingClient({}, { [PRODUCERS]: fieldOnly });
    const { model } = await modelFor(client);
    expect(model.configuredSentence).toBe("2 sources are configured and none has run yet.");
  });
});

describe("Updates with no source configured", () => {
  it("prints no configured-source sentence, and no number", async () => {
    // Only an enricher and a view: the producers read has no source. The
    // diagnostics read still holds four rows, which must not be counted.
    const noSources = {
      payload: {
        ...PRODUCERS_READ.payload,
        producer_count: 2,
        producers: PRODUCERS_READ.payload.producers.filter((row) => row.kind !== "source"),
      },
    };
    const { client } = recordingClient({}, { [PRODUCERS]: noSources });
    const { model, data } = await modelFor(client);
    expect(model.noAttemptsSentence).toBe(
      "No update attempt has been recorded for fresh-deployment.",
    );
    expect(model.configuredSentence).toBeNull();
    expect(model.configuredLink).toBeNull();
    expect(pageMarkup(data)).not.toMatch(/sources? (is|are) configured/);
  });
});
