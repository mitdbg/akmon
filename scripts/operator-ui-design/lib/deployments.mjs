// The six mock deployments, as compact specs. `reads.mjs` turns each one into
// the console read payloads; `generate-fixtures.mjs` writes them out.
//
// They exist to STRESS the design, not to flatter it. Between them they carry
// every edge case named by the user stories in section 1a of
// scripts/operator-ui-design/redesign-requirements-v2.md. The README beside the generated
// files maps fixture to story.
import { mulberry32, intBetween, pick } from "./rng.mjs";
import { generationId } from "./reads.mjs";
import { HANDBOOK_MARKDOWN, MAIN_RS, PAPER_CHUNKS, STATUS_HTML } from "./artifacts.mjs";

export const CLOCK = "2026-09-18T09:00:00.000Z";
const NOW_MS = Date.parse(CLOCK);
const HOUR = 3600000;
const DAY = 24 * HOUR;

function iso(offsetMs) {
  return new Date(NOW_MS - offsetMs).toISOString();
}

function hex(rand, length) {
  let out = "";
  while (out.length < length) out += Math.floor(rand() * 16).toString(16);
  return out.slice(0, length);
}

function attempt(deployment, rand, { offset, status, nodesDelta = 0, edgesDelta = 0, durationMs = 42000, blockedReason = null, stoppedStage = null, stoppedProducer = null, errorText = null, excludedSourceCount = 0 }) {
  const at = iso(offset);
  const full = generationId(deployment, at, hex(rand, 12));
  return {
    iso: at,
    generationId: full,
    compatId: Math.floor(Date.parse(at) * 1000 + rand() * 999),
    status,
    nodesDelta,
    edgesDelta,
    durationMs,
    blockedReason,
    stoppedStage,
    stoppedProducer,
    errorText,
    excludedSourceCount,
  };
}

function run(offset, outcome, itemsIn, itemsOut, durationMs, phase, errorText = null) {
  return { iso: iso(offset), outcome, itemsIn, itemsOut, durationMs, phase, errorText };
}

function record(nodeId, type, label, source, extra = {}) {
  return { nodeId, type, label, source, ...extra };
}

const NO_RUNTIME_EVIDENCE = {
  queueByLane: [0, 0, 0, 0, 0],
  dlqCount: 0,
  workflowTablesPresent: false,
  resourcesStatus: "missing",
  resourceOmissions: [
    "Host resource series are unavailable: no collector is configured.",
    "Runtime queue evidence is unavailable: dbos.workflow_status is missing.",
  ],
  checks: [
    {
      severity: "fail",
      code: "runtime_queue_unavailable",
      scope: "runtime",
      summary: "Runtime queue evidence is unavailable.",
      actual: "dbos.workflow_status missing",
      expected: "bounded current queue evidence",
      fix_hint: "inspect DBOS history volume and the runtime queue authority",
    },
  ],
};

const HEALTHY_RUNTIME = {
  queueByLane: [0, 0, 0, 0, 0],
  dlqCount: 0,
  workflowTablesPresent: true,
  resourcesStatus: "ok",
  resourceOmissions: [],
  checks: [],
};

// --------------------------------------------------------------------------
// 1. archiv3-small -- the real Archiv3 shape.
// --------------------------------------------------------------------------

function archiv3Small() {
  const rand = mulberry32(0x0a3c1);
  const name = "archiv3-small";
  const sourceNames = ["docsite", "cmssw_releases", "papers", "twiki", "indico"];
  const producers = sourceNames.map((sourceName, index) => ({
    name: sourceName,
    kind: "source",
    adapter: `okg.sources.${sourceName}`,
    settings: { endpoint: `https://archiv3.invalid/${sourceName}`, credentials: "omitted" },
    scope: "full",
    // Deliberately no cadence anywhere in this deployment: story S9 requires
    // the "next due" column to be ABSENT, not blank, when none is declared.
    cadenceHours: null,
    state: sourceName === "cmssw_releases" ? "failed" : "updated",
    lastRunIso: iso(2 * HOUR + index * 7 * 60000),
    records: sourceName === "cmssw_releases" ? 2414 : intBetween(rand, 2, 96),
    edges: sourceName === "cmssw_releases" ? 2100 : intBetween(rand, 1, 80),
    recordTypes: [sourceName === "cmssw_releases" ? "cmssw_release" : "document"],
    // The live read returns 11 rows for these 5 sources: each source also
    // appears under its runtime key. Sources must collapse them.
    runtimeKeys: index < 3 ? [`${sourceName}:worker`, `${sourceName}:extract`] : [],
    error: sourceName === "cmssw_releases"
      ? "ingest_source_incomplete: cmssw-releases returned 412 rows of an expected 2,414;\n"
        + "the upstream index was still being rebuilt when the fetch ran."
      : null,
    runs: [
      run(2 * HOUR, sourceName === "cmssw_releases" ? "failed" : "ok",
        intBetween(rand, 10, 2500), intBetween(rand, 2, 2414), intBetween(rand, 900, 41000),
        sourceName === "cmssw_releases" ? "fetch" : "publish",
        sourceName === "cmssw_releases" ? "ingest_source_incomplete: 412 of 2414 rows" : null),
      run(26 * HOUR, "ok", intBetween(rand, 10, 2500), intBetween(rand, 2, 2414),
        intBetween(rand, 900, 41000), "publish"),
    ],
  }));

  const attempts = [
    attempt(name, rand, { offset: 5 * DAY, status: "published", nodesDelta: 2605, edgesDelta: 2296, durationMs: 128000 }),
    attempt(name, rand, { offset: 4 * DAY, status: "blocked", blockedReason: "ingest_source_incomplete", stoppedStage: "sources", stoppedProducer: "cmssw_releases", errorText: producers[1].error }),
    attempt(name, rand, { offset: 3 * DAY, status: "blocked", blockedReason: "ingest_source_incomplete", stoppedStage: "sources", stoppedProducer: "cmssw_releases", errorText: producers[1].error }),
    attempt(name, rand, { offset: 2 * DAY, status: "blocked", blockedReason: "ingest_source_incomplete", stoppedStage: "sources", stoppedProducer: "cmssw_releases", errorText: producers[1].error }),
    attempt(name, rand, { offset: 1 * DAY, status: "blocked", blockedReason: "ingest_source_incomplete", stoppedStage: "sources", stoppedProducer: "cmssw_releases", errorText: producers[1].error }),
    attempt(name, rand, { offset: 2 * HOUR, status: "blocked", blockedReason: "ingest_source_incomplete", stoppedStage: "sources", stoppedProducer: "cmssw_releases", errorText: producers[1].error }),
  ];

  const recordTypes = [
    { type: "cmssw_release", count: 2414 },
    { type: "document", count: 96 },
    { type: "paper", count: 58 },
    { type: "twiki_page", count: 21 },
    { type: "indico_event", count: 14 },
    { type: "person", count: 2 },
  ];

  const records = [
    record("cmssw_release:CMSSW_14_0_0", "cmssw_release", "CMSSW_14_0_0", "cmssw_releases", {
      text: "CMSSW release 14.0.0, cycle 14_0, announced 2026-02-11.",
      fields: [
        { key: "release", value: "CMSSW_14_0_0", echoesTitle: true },
        { key: "cycle", value: "14_0" },
        { key: "announced", value: "2026-02-11" },
        { key: "architecture", value: "el9_amd64_gcc12" },
      ],
      sections: [{ key: "notes", title: "Release notes", value: "Production release for Run 3 reprocessing." }],
      outgoing: [{ relationship: "supersedes", nodeId: "cmssw_release:CMSSW_13_3_0", type: "cmssw_release", label: "CMSSW_13_3_0" }],
      incoming: [{ relationship: "mentions", nodeId: "document:release-plan", type: "document", label: "Run 3 release plan" }],
      sourceRecordId: "CMSSW_14_0_0",
      sourceRevision: "2026-02-11T00:00:00Z",
    }),
    record("document:release-plan", "document", "Run 3 release plan", "docsite", {
      text: "The plan naming each CMSSW release used in Run 3 reprocessing.",
      fields: [
        { key: "path", value: "/docs/run3/release-plan.md", echoesTitle: false },
        { key: "format", value: "markdown" },
      ],
      sections: [{ key: "body", title: "Body", value: "Run 3 reprocessing uses CMSSW_14_0_0 onwards." }],
      outgoing: [{ relationship: "mentions", nodeId: "cmssw_release:CMSSW_14_0_0", type: "cmssw_release", label: "CMSSW_14_0_0" }],
      incoming: [],
      // The record names a file the corpus root no longer holds: the third
      // way an artifact can be absent, beside "not retained" and "refused".
      artifact: {
        missing: "file_missing", format: "markdown", path: "docs/run3/release-plan.md",
        detail: "the corpus root for docsite no longer holds docs/run3/release-plan.md",
      },
    }),
  ];

  return {
    name,
    purpose: "The real Archiv3 shape: five sources, no enrichers, no views, one published "
      + "version and four blocked attempts after it, and no runtime queue evidence.",
    nowMs: NOW_MS,
    verdict: "failing",
    nodesLive: 2605,
    edgesLive: 2296,
    databaseSizeBytes: 63674035,
    invariants: { cross_source_total: 1, cross_source_failed: 0, cross_source_timed_out: 0, source_local_total: 0, source_local_failed: 0, total: 1, failed: 0, budget_exceeded: false, duration_ms: 9 },
    producers,
    attempts,
    recordTypes,
    relationshipTypes: [
      { type: "supersedes", count: 1204 },
      { type: "mentions", count: 812 },
      { type: "authored_by", count: 198 },
      { type: "part_of", count: 82 },
    ],
    records,
    traces: [],
    runtime: NO_RUNTIME_EVIDENCE,
    featured: { record: "cmssw_release:CMSSW_14_0_0", type: "cmssw_release", query: "CMSSW_14_0_0", producer: "cmssw_releases" },
    stories: ["S1", "S2", "S4", "S9", "S10"],
    edgeCases: [
      "Latest attempt is blocked while reads still serve an older published version (S1).",
      "One source failed with a two-line recorded error; the same text must appear in Updates and in the Sources rail (S2).",
      "No cadence is declared anywhere, so the Sources 'next due' column must be absent, not blank (S9).",
      "The source read returns 11 rows for 5 sources; Sources must show 5.",
      "Runtime queue evidence is unavailable and says so rather than reading as an empty queue.",
    ],
  };
}

// --------------------------------------------------------------------------
// 2. workspace-mixed -- okg-workspace-like: every producer kind and state.
// --------------------------------------------------------------------------

function workspaceMixed() {
  const rand = mulberry32(0x5eed2);
  const name = "workspace-mixed";
  const states = ["updated", "updated", "updated", "updated", "failed", "never run", "excluded", "running"];
  const producers = [];

  for (let index = 0; index < 30; index += 1) {
    const state = index < 4 ? "updated" : pick(rand, states);
    const long = index === 7;
    const sourceName = long
      ? "github-code-repositories-mitdbg-okg-substrate-and-operator-console-mirror"
      : `source-${String(index + 1).padStart(2, "0")}-${pick(rand, ["git", "papers", "slack", "sessions", "openspec", "docs"])}`;
    producers.push({
      name: sourceName,
      kind: "source",
      adapter: `okg.sources.${pick(rand, ["git", "papers", "slack", "sessions"])}`,
      // One source carries a DSN with an embedded password and a repository
      // path, so the producer page's Configuration block has a password to
      // hide inside a value and a path it must print.
      settings: index === 4
        ? {
          dsn: "postgres://okg:hunter2@db.workspace.invalid:5432/okg",
          repo_path: "/data/okg/workspace/sources/docs",
          credentials: "omitted",
        }
        : { endpoint: `https://workspace.invalid/${index}`, credentials: "omitted" },
      scope: index % 3 === 0 ? "incremental" : "full",
      cadenceHours: index % 2 === 0 ? 6 : null,
      nextDueIso: index % 2 === 0 ? new Date(NOW_MS + 4 * HOUR).toISOString() : null,
      state,
      lastRunIso: state === "never run" ? null : iso(intBetween(rand, 1, 70) * HOUR),
      records: state === "never run" ? 0 : (index === 11 ? 0 : intBetween(rand, 3, 1800)),
      edges: state === "never run" ? 0 : intBetween(rand, 0, 900),
      recordTypes: ["document", "commit"],
      runtimeKeys: index < 6 ? [`${sourceName}:worker`] : [],
      excludedReason: state === "excluded"
        ? "excluded by the deployment manifest: this mirror duplicates source-03-git"
        : null,
      error: state === "failed"
        ? "source_fetch_failed: the remote refused the connection after 3 retries\n"
          + "  endpoint https://workspace.invalid/" + index + "\n"
          + "  last status 503 Service Unavailable"
        : null,
      runs: state === "never run" ? [] : [
        run(intBetween(rand, 1, 70) * HOUR, state === "failed" ? "failed" : "ok",
          intBetween(rand, 5, 2000), intBetween(rand, 0, 1800), intBetween(rand, 400, 90000),
          state === "failed" ? "fetch" : "publish",
          state === "failed" ? "source_fetch_failed: 503 Service Unavailable" : null),
        run(intBetween(rand, 71, 140) * HOUR, "ok", intBetween(rand, 5, 2000),
          intBetween(rand, 0, 1800), intBetween(rand, 400, 90000), "publish"),
      ],
    });
  }

  const enricherSpecs = [
    { enricherName: "semantic-linker", state: "updated", withheldReason: null },
    { enricherName: "entity-resolver", state: "updated", withheldReason: null },
    {
      enricherName: "llm-summarizer",
      state: "withheld",
      withheldReason: "withheld: the deployment's policy does not permit sending this "
        + "source's text to an external model",
    },
  ];
  for (const { enricherName, state, withheldReason } of enricherSpecs) {
    producers.push({
      name: enricherName,
      kind: "enricher",
      adapter: `okg.enrichers.${enricherName}`,
      settings: { model: "local", credentials: "omitted" },
      scope: "full",
      cadenceHours: null,
      state,
      withheldReason,
      lastRunIso: iso(9 * HOUR),
      records: state === "withheld" ? 0 : intBetween(rand, 40, 900),
      edges: state === "withheld" ? 0 : intBetween(rand, 100, 2400),
      // An enricher writes records too, and the count rollup's
      // `source_subtype` dimension carries their type. Declared here so the
      // producers read can answer records-by-type for an enricher rather than
      // leaving its page saying it holds no record.
      recordTypes: ["summary"],
      runtimeKeys: [],
      error: null,
      runs: [run(9 * HOUR, state === "withheld" ? "withheld" : "ok", 1800, 940, 61000, "enrich")],
    });
  }

  const viewSpecs = [
    { viewName: "daily-brief", state: "updated", error: null },
    { viewName: "source-rollup", state: "updated", error: null },
    { viewName: "ontology-rollup", state: "updated", error: null },
    {
      viewName: "contributor-graph",
      state: "failed",
      error: "view_build_failed: relation \"okg.contributor_edge\" does not exist\n"
        + "  LINE 4:   FROM okg.contributor_edge e\n"
        + "                 ^\n"
        + "  the view was built against a schema this generation does not carry",
    },
  ];
  for (const { viewName, state, error } of viewSpecs) {
    producers.push({
      name: viewName,
      kind: "view",
      adapter: `okg.views.${viewName}`,
      settings: {},
      scope: "generation",
      cadenceHours: null,
      state,
      lastRunIso: iso(9 * HOUR),
      records: state === "failed" ? 0 : intBetween(rand, 10, 400),
      edges: 0,
      recordTypes: ["rollup_row"],
      runtimeKeys: [],
      error,
      runs: [run(9 * HOUR, state === "failed" ? "failed" : "ok", 1, state === "failed" ? 0 : 310, 4200, state === "failed" ? "build" : "publish", error)],
    });
  }

  // A producer nothing configures. The count rollup and source_progress
  // observed `_enricher:affiliations` running; no registry entry names it, so
  // the live read lists it with `configured: false` and the console asks for
  // no detail read. Every fixture had only configured producers, so the
  // producer page for this case -- the one that must say what was NOT read --
  // was never drawn.
  producers.push({
    name: "_enricher:affiliations",
    ownershipId: "workspace-mixed._enricher:affiliations",
    kind: "source",
    configured: false,
    adapter: null,
    settings: {},
    scope: null,
    cadenceHours: null,
    state: "updated",
    // The run diagnostics view is keyed by registry_source_name, so a
    // producer outside the registry has no run row there.
    lastRunIso: null,
    records: 318,
    edges: 0,
    recordTypes: [],
    runtimeKeys: [],
    excludedReason: null,
    error: null,
    runs: [],
  });

  const attempts = [];
  for (let day = 21; day >= 1; day -= 1) {
    const status = day % 7 === 0 ? "blocked" : "published";
    attempts.push(attempt(name, rand, {
      offset: day * DAY,
      status,
      nodesDelta: status === "published" ? intBetween(rand, -40, 900) : 0,
      edgesDelta: status === "published" ? intBetween(rand, -20, 1600) : 0,
      durationMs: intBetween(rand, 90000, 600000),
      blockedReason: status === "blocked" ? "source_fetch_failed" : null,
      stoppedStage: status === "blocked" ? "sources" : null,
      stoppedProducer: status === "blocked" ? producers.find((p) => p.state === "failed")?.name ?? null : null,
      errorText: status === "blocked" ? producers.find((p) => p.state === "failed")?.error ?? null : null,
    }));
  }
  attempts.push(attempt(name, rand, { offset: 40 * 60000, status: "running", durationMs: 2400000 }));

  const recordTypes = [
    { type: "document", count: 18420 }, { type: "commit", count: 12044 },
    { type: "file", count: 9810 }, { type: "session", count: 4102 },
    { type: "person", count: 812 }, { type: "paper", count: 640 },
    { type: "pact", count: 318 }, { type: "issue", count: 244 },
    { type: "decision", count: 96 }, { type: "deployment", count: 12 },
  ];

  const docsSource = producers.find((p) => p.name === "source-03-docs");
  if (!docsSource) throw new Error("workspace-mixed needs a docs source for the docsite page");
  const gitSource = "source-07-git";
  const blob = (path) => `https://github.com/example-org/example-repo/blob/a41c9e2c0b7d4e1f/${path}`;
  const paperChunks = PAPER_CHUNKS.map((chunk, index) => record(
    `document_chunk:okg-2026#${index}`, "document_chunk", `${chunk.heading} (chunk ${index})`, "source-11-papers", {
      text: chunk.text,
      fields: [
        { key: "ordinal", value: String(index) },
        { key: "page", value: String(chunk.page) },
        { key: "heading_path", value: chunk.heading },
      ],
      sections: [{ key: "text", title: "Text", value: chunk.text }],
      chunk: { ordinal: index, page: chunk.page, headingPath: chunk.heading },
      outgoing: [],
      incoming: [{ relationship: "contains", nodeId: "paper:okg-2026", type: "paper", label: "Operational knowledge graphs" }],
      sourceRecordId: `okg-2026.pdf#${index}`,
      sourceRevision: "2026-09-10",
      factLog: [{ at: iso(9 * HOUR), fact: "node_present", source: "source-11-papers" }],
    },
  ));

  const records = [
    record("document:handbook", "document", "Operator handbook", gitSource, {
      text: "How to run the deployment day to day.",
      fields: [
        { key: "path", value: "docs/operate/handbook.md", echoesTitle: false },
        { key: "format", value: "markdown" },
        { key: "byte_count", value: "827" },
        { key: "author_email", hidden: true, policy: "nomos.contact_details" },
        { key: "updated", value: "2026-09-11" },
        { key: "citation_url", value: blob("docs/operate/handbook.md") },
      ],
      sections: [{ key: "body", title: "Body", value: HANDBOOK_MARKDOWN }],
      outgoing: [{ relationship: "mentions", nodeId: "person:riley", type: "person", label: "Riley" }],
      incoming: [{ relationship: "cites", nodeId: "paper:okg-2026", type: "paper", label: "Operational knowledge graphs" }],
      sourceRecordId: "docs/operate/handbook.md",
      sourceRevision: "a41c9e2",
      citationUrl: blob("docs/operate/handbook.md"),
      // The original file is on disk under the source's corpus root.
      artifact: { file: "handbook.md", format: "markdown", path: "docs/operate/handbook.md" },
      // S14, case 1: the canonical record is retained and readable.
      payload: {
        source_record_id: "docs/operate/handbook.md",
        subtype: "document",
        title: "Operator handbook",
        format: "markdown",
        revision: "a41c9e2",
        observed_at: iso(9 * HOUR),
        attrs: { path: "docs/operate/handbook.md", updated: "2026-09-11", author_email: "[nomos:masked]" },
        body: HANDBOOK_MARKDOWN,
      },
      factLog: [
        { at: iso(9 * HOUR), fact: "node_present", source: gitSource },
        { at: iso(9 * HOUR), fact: "edge_present", source: "semantic-linker" },
        { at: iso(33 * HOUR), fact: "node_present", source: gitSource },
      ],
    }),
    record("person:riley", "person", "Riley", "source-04-sessions", {
      text: "A person record built from session and commit authorship.",
      fields: [
        { key: "display_name", value: "Riley", echoesTitle: true },
        { key: "email", hidden: true, policy: "nomos.contact_details" },
      ],
      sections: [],
      outgoing: [],
      incoming: [{ relationship: "mentions", nodeId: "document:handbook", type: "document", label: "Operator handbook" }],
      // S14, case 2: the payload exists but policy refuses it, and the same
      // policy refuses the artifact: an original file never bypasses masking.
      artifact: {
        missing: "policy_refused", policy: "nomos.contact_details", format: "json", path: "people/riley.json",
        detail: "policy nomos.contact_details refuses the original file for person records",
      },
      payloadRefused: {
        policy: "nomos.contact_details",
        detail: "policy nomos.contact_details refuses the retained payload for person records; the stored row and edge rows are unaffected",
      },
      factLog: [{ at: iso(30 * HOUR), fact: "node_present", source: "source-04-sessions" }],
    }),
    record("paper:okg-2026", "paper", "Operational knowledge graphs", "source-11-papers", {
      text: "A paper on the substrate: versioned graph, invariants, pinned reads.",
      fields: [
        { key: "title", value: "Operational knowledge graphs", echoesTitle: true },
        { key: "path", value: "papers/okg-2026.pdf" },
        { key: "format", value: "pdf" },
        { key: "byte_count", value: "2692" },
        { key: "page_count", value: "2" },
        { key: "year", value: "2026" },
      ],
      sections: [{ key: "body", title: "Extracted text", value: PAPER_CHUNKS.map((chunk) => `${chunk.heading}\n${chunk.text}`).join("\n\n") }],
      outgoing: [
        ...paperChunks.map((chunk) => ({ relationship: "contains", nodeId: chunk.nodeId, type: "document_chunk", label: chunk.label })),
        { relationship: "cites", nodeId: "document:handbook", type: "document", label: "Operator handbook" },
      ],
      incoming: [],
      sourceRecordId: "okg-2026.pdf",
      sourceRevision: "2026-09-10",
      artifact: { file: "okg-2026.pdf", format: "pdf", path: "papers/okg-2026.pdf" },
      payload: {
        source_record_id: "okg-2026.pdf",
        subtype: "paper",
        title: "Operational knowledge graphs",
        format: "pdf",
        page_count: 2,
        observed_at: iso(9 * HOUR),
        chunks: PAPER_CHUNKS.map((chunk, index) => ({ ordinal: index, page: chunk.page, heading_path: chunk.heading, text: chunk.text })),
      },
      factLog: [
        { at: iso(9 * HOUR), fact: "node_present", source: "source-11-papers" },
        { at: iso(9 * HOUR), fact: "edge_present", source: "semantic-linker" },
      ],
    }),
    ...paperChunks,
    record("file:src/main.rs", "file", "src/main.rs", gitSource, {
      text: "The ingest worker's entry point.",
      fields: [
        { key: "path", value: "src/main.rs", echoesTitle: true },
        { key: "format", value: "code" },
        { key: "language", value: "rust" },
        { key: "byte_count", value: "2487" },
        { key: "citation_url", value: blob("src/main.rs") },
      ],
      sections: [{ key: "content", title: "Content", value: MAIN_RS }],
      outgoing: [],
      incoming: [{ relationship: "touches", nodeId: "commit:9a1c02e", type: "commit", label: "9a1c02e Rework the module graph" }],
      sourceRecordId: "src/main.rs",
      sourceRevision: "a41c9e2",
      citationUrl: blob("src/main.rs"),
      artifact: { file: "main.rs", format: "code", path: "src/main.rs" },
      payload: {
        source_record_id: "src/main.rs", subtype: "file", format: "code", language: "rust",
        revision: "a41c9e2", observed_at: iso(9 * HOUR), content: MAIN_RS,
      },
      factLog: [{ at: iso(9 * HOUR), fact: "node_present", source: gitSource }],
    }),
    record("document:status-page", "document", "Deployment status", docsSource.name, {
      text: "The docsite's status page, rebuilt after every published version.",
      fields: [
        { key: "path", value: "status/index.html" },
        { key: "format", value: "html" },
        { key: "byte_count", value: "933" },
        { key: "citation_url", value: "https://docs.example.invalid/status" },
      ],
      sections: [{ key: "body", title: "Body", value: "Deployment status\n\nThis page is rebuilt after every published version.\n\nProducer | State\nsource-07-git | updated\nsemantic-linker | updated\ncontributor-graph | failed" }],
      outgoing: [],
      incoming: [],
      sourceRecordId: "https://docs.example.invalid/status",
      sourceRevision: "2026-09-17T09:00:00Z",
      citationUrl: "https://docs.example.invalid/status",
      artifact: { file: "status.html", format: "html", path: "status/index.html" },
      payload: {
        source_record_id: "https://docs.example.invalid/status", subtype: "document", format: "html",
        observed_at: iso(9 * HOUR), title: "Deployment status", body: STATUS_HTML,
      },
      factLog: [{ at: iso(9 * HOUR), fact: "node_present", source: docsSource.name }],
    }),
    record("document:diagram", "document", "Producer graph diagram", docsSource.name, {
      text: "A diagram of three producers, one of them failed.",
      fields: [
        { key: "path", value: "images/producer-graph.png" },
        { key: "format", value: "image" },
        { key: "byte_count", value: "16192" },
        { key: "mime_type", value: "image/png" },
      ],
      sections: [{ key: "caption", title: "Caption", value: "Three producers joined by edges; the third one failed." }],
      outgoing: [],
      incoming: [],
      sourceRecordId: "images/producer-graph.png",
      sourceRevision: "2026-09-12",
      artifact: { file: "diagram.png", format: "image", path: "images/producer-graph.png" },
      payload: {
        source_record_id: "images/producer-graph.png", subtype: "document", format: "image", mime_type: "image/png",
        observed_at: iso(9 * HOUR), caption: "Three producers joined by edges; the third one failed.",
      },
      factLog: [{ at: iso(9 * HOUR), fact: "node_present", source: docsSource.name }],
    }),
    ...ingestRecords(rand, producers),
    record("session:sess-1", "session", "which sources failed last night", "source-04-sessions", {
      text: "An agent session of 1,400 turns reviewing last night's update.",
      fields: [
        { key: "question_label", value: "which sources failed last night", echoesTitle: true },
        { key: "started_at", value: "2026-09-17T22:04:00Z" },
        { key: "turn_count", value: "1400" },
      ],
      sections: [{ key: "summary", title: "Summary", value: "1,400 turns over 61 minutes; the session read the health, the sources and one attempt, and answered from them." }],
      outgoing: [{ relationship: "mentions", nodeId: "person:riley", type: "person", label: "Riley" }],
      incoming: [],
      sourceRecordId: "sess-1",
      sourceRevision: null,
      // No corpus root: the source keeps the canonical record only. Its 350 KB
      // payload is past the raw read's inline limit, so Raw shows the head.
      artifact: { missing: "not_retained", detail: "source-04-sessions keeps no corpus root; the record is its own canonical JSON" },
      payloadFile: "session-sess-1.json",
      factLog: [{ at: iso(9 * HOUR), fact: "node_present", source: "source-04-sessions" }],
    }),
  ];

  return {
    name,
    purpose: "An okg-workspace-like deployment: 30 sources in every state, three enrichers "
      + "including one withheld, four views including one that failed, several published "
      + "versions and one attempt still running.",
    nowMs: NOW_MS,
    verdict: "degraded",
    nodesLive: 46498,
    edgesLive: 128440,
    databaseSizeBytes: 2846102938,
    invariants: { cross_source_total: 14, cross_source_failed: 0, cross_source_timed_out: 0, source_local_total: 22, source_local_failed: 0, total: 36, failed: 0, budget_exceeded: false, duration_ms: 412 },
    producers,
    attempts,
    recordTypes,
    relationshipTypes: [
      { type: "mentions", count: 48120 }, { type: "authored_by", count: 22044 },
      { type: "touches", count: 18110 }, { type: "cites", count: 9800 },
      { type: "part_of", count: 6402 }, { type: "supersedes", count: 1804 },
      { type: "decided_by", count: 620 },
    ],
    records,
    traces: [
      { session_id: "sess-1", question_label: "which sources failed last night", last_call_at: iso(2 * HOUR), tool_call_count: 6 },
      { session_id: "sess-2", question_label: "who owns the operator console", last_call_at: iso(20 * HOUR), tool_call_count: 3 },
    ],
    runtime: {
      queueByLane: [0, 2, 0, 0, 1],
      dlqCount: 0,
      workflowTablesPresent: true,
      resourcesStatus: "ok",
      resourceOmissions: [],
      checks: [
        {
          severity: "warn", code: "view_build_failed", scope: "views",
          summary: "One view failed to build in the latest attempt.",
          actual: "contributor-graph: relation okg.contributor_edge does not exist",
          expected: "all configured views built",
          fix_hint: "okg view diagnose contributor-graph",
        },
      ],
    },
    featured: { record: "document:handbook", type: "document", query: "handbook", producer: producers.find((p) => p.state === "failed").name },
    // okg-workspace's case (issue #2545): the deployment's default search
    // profile names types this version's catalog does not hold. The read
    // searches the types it does hold and names the rest.
    searchProfile: { profile: "analyst", skipped: ["chat_message", "chat_session", "conference", "venue"] },
    // The search pass: one query with about forty matches, read on two
    // pages, with one type checked, sorted newest, within the past month,
    // and one query that matches nothing.
    searches: [
      { q: "ingest", filters: {}, offsets: [0, 20] },
      { q: "ingest", filters: { type: "issue" }, offsets: [0] },
      // sort and since are gone with the controls that sent them: the live
      // read refuses both with filter_has_no_stored_basis.
      { q: "ingest", filters: { source: "source-07-git" }, offsets: [0] },
      { q: "ingest", filters: { type: "file" }, offsets: [0] },
      { q: "zzz-no-such-record", filters: {}, offsets: [0] },
    ],
    stories: ["S5 (see all-clear)", "S7", "S9", "S11", "S12", "S14", "S15", "S16", "S17", "S18", "S19", "S21"],
    edgeCases: [
      "Every producer state in one table: updated, failed, running, never run, excluded (S18, S21).",
      "An enricher withheld with a stated reason, and a view whose build failed with a four-line error (S19).",
      "One source contributed zero records in the selected version, shown as 0 rather than blank (S7).",
      "A 78-character source name that must truncate without hiding its distinguishing tail.",
      "A cadence on half the sources, so 'next due' is populated for some rows and empty for others (S9).",
      "A policy-hidden field on two records; Fields says hidden and names the policy, Raw shows it masked (S15).",
      "One record whose retained payload is present and one whose payload policy refuses (S14); the same policy refuses that record's original file.",
      "Real artifacts on disk: a PDF with three document_chunk records, a Markdown document, a Rust file, an HTML page and a PNG, each rendered on the Content tab.",
      "A session whose retained payload is 350 KB, past the raw read's 256 KB inline limit, so Raw shows the first 200 lines and offers the file.",
      "Several published versions, so the version picker has something to switch between (S16).",
      "An attempt still running now (S3's running case).",
    ],
  };
}

/** Forty records that match "ingest": issues, decisions and documents from
 *  four sources, updated over the past fourteen months, so the results page
 *  has two pages, facet counts, a date filter with something to exclude, and
 *  a sort that changes the order. Seeded, like everything else here. */
function ingestRecords(rand, producers) {
  const sources = ["source-07-git", "source-03-docs", "source-02-openspec", "source-04-sessions"];
  const kinds = [
    ["issue", "Ingest", (n) => `Ingest of ${n} stalls on a 412 from the upstream index`],
    ["decision", "Decision", (n) => `Retry an incomplete ingest ${n} times before blocking`],
    ["document", "Runbook", (n) => `Runbook: recover a blocked ingest (${n})`],
  ];
  const texts = [
    "The ingest stopped at 412 of 2,414 rows; the upstream index was being rebuilt.",
    "Agreed: an incomplete ingest blocks the attempt rather than publishing a partial version.",
    "Steps to diagnose an ingest that reads fewer rows than the source declares.",
    "An ingest that times out is retried with the same cursor, never from the start.",
  ];
  const records = [];
  for (let index = 0; index < 40; index += 1) {
    const [type, word, title] = kinds[index % kinds.length];
    const source = sources[index % sources.length];
    const daysAgo = Math.round((index * 425) / 40) + intBetween(rand, 0, 6);
    const updated = new Date(NOW_MS - daysAgo * DAY).toISOString().slice(0, 10);
    const id = `${type}:ingest-${String(index + 1).padStart(3, "0")}`;
    const isIssue = type === "issue";
    records.push(record(id, type, title(index + 1), source, {
      text: texts[index % texts.length],
      fields: [
        { key: "title", value: title(index + 1), echoesTitle: true },
        { key: "updated", value: updated },
        ...(isIssue ? [{ key: "state", value: index % 5 === 0 ? "open" : "closed" }, { key: "citation_url", value: `https://github.com/example-org/example-repo/issues/${1200 + index}` }] : []),
        { key: "kind", value: word.toLowerCase() },
      ],
      sections: [{ key: "body", title: "Body", value: texts[index % texts.length] }],
      outgoing: index % 3 === 0 && index > 0
        ? [{ relationship: "decided_by", nodeId: `decision:ingest-${String(index).padStart(3, "0")}`, type: "decision", label: kinds[1][2](index) }]
        : [],
      incoming: [],
      sourceRecordId: isIssue ? String(1200 + index) : `ingest-${index + 1}`,
      sourceRevision: null,
      citationUrl: isIssue ? `https://github.com/example-org/example-repo/issues/${1200 + index}` : null,
      factLog: [{ at: iso(intBetween(rand, 2, 40) * HOUR), fact: "node_present", source }],
    }));
  }
  void producers;
  return records;
}

// --------------------------------------------------------------------------
// 3. scale-200 -- nothing on Home may grow.
// --------------------------------------------------------------------------

function scale200() {
  const rand = mulberry32(0xc0ffee);
  const name = "scale-200";
  const producers = [];
  for (let index = 0; index < 220; index += 1) {
    const failed = index % 24 === 7 && producers.filter((p) => p.state === "failed").length < 9;
    producers.push({
      name: `code-repo-${String(index + 1).padStart(3, "0")}`,
      kind: "source",
      adapter: "okg.sources.git",
      settings: { endpoint: `https://git.invalid/repo-${index + 1}`, credentials: "omitted" },
      scope: "incremental",
      cadenceHours: 12,
      nextDueIso: new Date(NOW_MS + intBetween(rand, 1, 12) * HOUR).toISOString(),
      state: failed ? "failed" : "updated",
      lastRunIso: iso(intBetween(rand, 1, 30) * HOUR),
      records: intBetween(rand, 20, 9000),
      edges: intBetween(rand, 10, 24000),
      recordTypes: ["commit", "file"],
      runtimeKeys: [],
      error: failed ? "clone_failed: repository not found (404)" : null,
      runs: [run(intBetween(rand, 1, 30) * HOUR, failed ? "failed" : "ok",
        intBetween(rand, 10, 9000), intBetween(rand, 0, 9000), intBetween(rand, 500, 300000),
        failed ? "clone" : "publish", failed ? "clone_failed: 404" : null)],
    });
  }

  // A forty-line recorded error. Updates must show it verbatim, one level down.
  const longError = [
    "publication_check_failed: 3 of 412 invariants failed in the publish stage",
    "",
    "invariant okg.cross_source.commit_author_resolves",
    "  scope     cross-source",
    "  expected  every commit has exactly one resolved author",
    "  actual    1,204 commits resolve to zero authors",
    "  first     commit:9a1c02e in code-repo-018",
    "  query     SELECT c.node_id FROM okg.commit c",
    "              LEFT JOIN okg.authored_by a ON a.dst = c.node_id",
    "             WHERE a.src IS NULL",
    "  duration  4,182 ms",
    "",
    "invariant okg.source_local.file_path_unique",
    "  scope     source-local (code-repo-141)",
    "  expected  one file node per path per repository",
    "  actual    38 paths carry two file nodes",
    "  first     file:src/main.rs in code-repo-141",
    "  query     SELECT path, count(*) FROM okg.file GROUP BY path HAVING count(*) > 1",
    "  duration  912 ms",
    "",
    "invariant okg.cross_source.no_orphan_edges",
    "  scope     cross-source",
    "  expected  every edge endpoint resolves to a live node",
    "  actual    77 edges point at retracted nodes",
    "  first     touches(commit:0bb31f4, file:deleted/old.rs)",
    "  query     SELECT e.src, e.dst FROM okg.edge e",
    "              LEFT JOIN okg.node n ON n.node_id = e.dst",
    "             WHERE n.node_id IS NULL",
    "  duration  2,401 ms",
    "",
    "budget    18,000 ms allowed, 7,495 ms used",
    "stage     publish",
    "producer  (none: the checks run after every producer completed)",
    "",
    "The generation was not published. Reads continue to serve the previous",
    "published version. Re-running the update without changing the inputs will",
    "fail the same three invariants.",
    "",
    "okg generation describe <id> --invariants",
    "okg generation describe <id> --invariants --format json",
  ].join("\n");

  const attempts = [];
  // 4,800 attempts over a year. The fixture carries every attempt so the Home
  // chart's day buckets and the Updates cursor are both exercised honestly.
  for (let index = 4799; index >= 0; index -= 1) {
    const offset = Math.round(index * (365 * DAY) / 4800);
    const roll = rand();
    const status = roll < 0.06 ? "blocked" : "published";
    attempts.push(attempt(name, rand, {
      offset,
      status,
      nodesDelta: status === "published" ? intBetween(rand, -200, 4000) : 0,
      edgesDelta: status === "published" ? intBetween(rand, -100, 9000) : 0,
      durationMs: intBetween(rand, 240000, 2400000),
      blockedReason: status === "blocked" ? "publication_check_failed" : null,
      stoppedStage: status === "blocked" ? "publication checks" : null,
      stoppedProducer: null,
      errorText: status === "blocked" ? longError : null,
    }));
  }

  const recordTypes = [];
  for (let index = 0; index < 214; index += 1) {
    recordTypes.push({
      type: index === 0 ? "commit" : index === 1 ? "file" : `type_${String(index).padStart(3, "0")}`,
      count: Math.max(1, Math.round(900000 / (index + 1))),
    });
  }
  const relationshipTypes = [];
  for (let index = 0; index < 600; index += 1) {
    relationshipTypes.push({
      type: index === 0 ? "touches" : index === 1 ? "authored_by" : `rel_${String(index).padStart(3, "0")}`,
      count: Math.max(1, Math.round(2400000 / (index + 1))),
    });
  }

  // S13: one record with about 500 relationships across several types, so the
  // Related tab must state its cap and how many rows it omitted.
  const hubRelationshipTypes = ["touches", "authored_by", "supersedes", "part_of", "reviews", "reverts"];
  const outgoing = [];
  const incoming = [];
  for (let index = 0; index < 500; index += 1) {
    const relationship = hubRelationshipTypes[index % hubRelationshipTypes.length];
    const row = {
      relationship,
      nodeId: `file:src/module_${String(index).padStart(3, "0")}.rs`,
      type: "file",
      label: `src/module_${String(index).padStart(3, "0")}.rs`,
    };
    if (index % 5 === 0) incoming.push(row); else outgoing.push(row);
  }

  const records = [
    record("commit:9a1c02e", "commit", "9a1c02e Rework the module graph", "code-repo-018", {
      text: "A commit that touches 400 files and is referenced by 100 others.",
      fields: [
        { key: "sha", value: "9a1c02e41b7d3f0a", echoesTitle: false },
        { key: "authored_at", value: "2026-08-02T11:04:00Z" },
        { key: "message", value: "Rework the module graph", echoesTitle: true },
        { key: "citation_url", value: "https://github.com/example-org/example-repo/commit/9a1c02e41b7d3f0a" },
      ],
      sections: [{ key: "diff_summary", title: "Diff summary", value: "400 files changed, 12,880 insertions, 9,402 deletions" }],
      outgoing,
      incoming,
      relatedCap: 100,
      sourceRecordId: "9a1c02e41b7d3f0a",
      sourceRevision: "9a1c02e41b7d3f0a",
      citationUrl: "https://github.com/example-org/example-repo/commit/9a1c02e41b7d3f0a",
    }),
    record("file:src/main.rs", "file", "src/main.rs", "code-repo-141", {
      text: "An entry point file that appears twice under one path.",
      fields: [
        { key: "path", value: "src/main.rs", echoesTitle: true },
        { key: "format", value: "code" },
        { key: "citation_url", value: "https://github.com/example-org/example-repo/blob/9a1c02e41b7d3f0a/src/main.rs" },
      ],
      sections: [{ key: "content", title: "Content", value: MAIN_RS }],
      outgoing: [],
      incoming: [{ relationship: "touches", nodeId: "commit:9a1c02e", type: "commit", label: "9a1c02e Rework the module graph" }],
      citationUrl: "https://github.com/example-org/example-repo/blob/9a1c02e41b7d3f0a/src/main.rs",
      artifact: { file: "main.rs", format: "code", path: "src/main.rs" },
    }),
  ];

  return {
    name,
    purpose: "220 sources, 214 record types, 600 relationship types and 4,800 attempts over "
      + "a year. Nothing on Home may grow because of any of it.",
    nowMs: NOW_MS,
    verdict: "degraded",
    nodesLive: 4182044,
    edgesLive: 11904882,
    databaseSizeBytes: 418204488192,
    invariants: { cross_source_total: 288, cross_source_failed: 2, cross_source_timed_out: 0, source_local_total: 124, source_local_failed: 1, total: 412, failed: 3, budget_exceeded: false, duration_ms: 7495 },
    producers,
    attempts,
    recordTypes,
    relationshipTypes,
    records,
    traces: [],
    runtime: {
      queueByLane: [4, 18, 2, 0, 6],
      dlqCount: 0,
      workflowTablesPresent: true,
      resourcesStatus: "ok",
      resourceOmissions: [],
      checks: [
        {
          severity: "fail", code: "publication_check_failed", scope: "publisher",
          summary: "The latest attempt failed three publication invariants.",
          actual: "3 of 412 invariants failed",
          expected: "all invariants pass",
          fix_hint: "okg generation describe <id> --invariants",
        },
        {
          severity: "warn", code: "source_failed", scope: "sources",
          summary: "Nine configured sources failed their last run.",
          actual: "9 of 220 sources failed",
          expected: "every configured source ran",
          fix_hint: "okg source diagnose --failed",
        },
      ],
    },
    featured: { record: "commit:9a1c02e", type: "commit", query: "module graph", producer: "code-repo-018" },
    stories: ["S6", "S8", "S13"],
    edgeCases: [
      "220 sources and 214 record types: Home must render the same elements it does at five (S8).",
      "4,800 attempts over a year, so the chart has a real rhythm and the Updates read needs a cursor (S6).",
      "A blocked attempt carrying a forty-line recorded error, shown verbatim one level down.",
      "One record with 500 relationships across six relationship types, capped at 100 per direction, "
        + "so the Related tab must state the cap and the omitted count (S13).",
      "600 relationship types, so Home's relationship-type list must bound itself.",
    ],
  };
}

// --------------------------------------------------------------------------
// 4. empty -- configured but never published.
// --------------------------------------------------------------------------

function emptyDeployment() {
  const name = "empty";
  const producers = ["papers", "git-files", "sessions"].map((sourceName) => ({
    name: sourceName,
    kind: "source",
    adapter: `okg.sources.${sourceName}`,
    settings: { endpoint: `https://new.invalid/${sourceName}`, credentials: "omitted" },
    scope: "full",
    cadenceHours: 24,
    nextDueIso: new Date(NOW_MS + 9 * HOUR).toISOString(),
    state: "never run",
    lastRunIso: null,
    records: 0,
    edges: 0,
    recordTypes: [],
    runtimeKeys: [],
    error: null,
    runs: [],
  }));
  return {
    name,
    purpose: "A deployment with three configured sources that has never published. "
      + "No version, no records, nothing to search.",
    nowMs: NOW_MS,
    verdict: "degraded",
    nodesLive: 0,
    edgesLive: 0,
    databaseSizeBytes: 8388608,
    invariants: { cross_source_total: 0, cross_source_failed: 0, cross_source_timed_out: 0, source_local_total: 0, source_local_failed: 0, total: 0, failed: 0, budget_exceeded: false, duration_ms: 0 },
    producers,
    attempts: [],
    recordTypes: [],
    relationshipTypes: [],
    records: [],
    traces: [],
    runtime: {
      queueByLane: [0, 0, 0, 0, 0],
      dlqCount: 0,
      workflowTablesPresent: true,
      resourcesStatus: "ok",
      resourceOmissions: [],
      checks: [
        {
          severity: "warn", code: "no_published_generation", scope: "publisher",
          summary: "This deployment has never published a version.",
          actual: "0 published generations",
          expected: "at least one published generation",
          fix_hint: "okg update run",
        },
      ],
    },
    featured: { record: null, type: null, query: "anything", producer: "papers" },
    stories: ["S20"],
    edgeCases: [
      "No published version at all: every 'version read from' line has nothing to name (S20).",
      "Three sources configured, all never run, none of them shown as failed.",
      "Explore must say there is nothing to search yet rather than rendering an empty result list.",
      "Zero record types and zero relationship types, so Home's contents section has no rows.",
    ],
  };
}

// --------------------------------------------------------------------------
// 5. broken-runtime -- the system is wrong, not the data.
// --------------------------------------------------------------------------

function brokenRuntime() {
  const rand = mulberry32(0xb00b);
  const name = "broken-runtime";
  const producers = ["papers", "git-files", "sessions", "slack"].map((sourceName, index) => ({
    name: sourceName,
    kind: "source",
    adapter: `okg.sources.${sourceName}`,
    settings: { endpoint: `https://broken.invalid/${sourceName}`, credentials: "omitted" },
    scope: "full",
    cadenceHours: 6,
    nextDueIso: new Date(NOW_MS + 2 * HOUR).toISOString(),
    state: "updated",
    lastRunIso: iso((8 + index) * HOUR),
    records: intBetween(rand, 40, 4000),
    edges: intBetween(rand, 20, 9000),
    recordTypes: ["document"],
    runtimeKeys: [],
    error: null,
    runs: [run((8 + index) * HOUR, "ok", 900, 820, 42000, "publish")],
  }));
  producers.push({
    name: "daily-brief",
    kind: "view",
    adapter: "okg.views.daily-brief",
    settings: {},
    scope: "generation",
    cadenceHours: 24,
    state: "failed",
    lastRunIso: iso(6 * HOUR),
    records: 0,
    edges: 0,
    recordTypes: [],
    runtimeKeys: [],
    error: "view_build_failed: statement timeout after 300,000 ms\n"
      + "  the worker holding the build lock has not reported since 03:11 UTC",
    runs: [run(6 * HOUR, "failed", 1, 0, 300000, "build",
      "view_build_failed: statement timeout after 300,000 ms")],
  });

  const attempts = [
    attempt(name, rand, { offset: 3 * DAY, status: "published", nodesDelta: 18400, edgesDelta: 41200, durationMs: 402000 }),
    attempt(name, rand, {
      offset: 2 * DAY, status: "blocked",
      blockedReason: "publication_check_failed",
      stoppedStage: "publication checks",
      stoppedProducer: null,
      errorText: "publication_check_failed: invariant okg.cross_source.document_has_source failed\n"
        + "  expected  every document resolves to exactly one configured source\n"
        + "  actual    62 documents resolve to none\n"
        + "  This is a check on the published graph, not a source failure.",
    }),
    attempt(name, rand, { offset: 6 * HOUR, status: "running", durationMs: 6 * HOUR }),
  ];

  return {
    name,
    purpose: "The system is wrong, not the data: the worker is missing, jobs are in the "
      + "dead-letter queue, the queue has a backlog, an attempt has been running for six "
      + "hours, and the last completed attempt was blocked on a publication check.",
    nowMs: NOW_MS,
    verdict: "failing",
    nodesLive: 184022,
    edgesLive: 411880,
    databaseSizeBytes: 18401284096,
    invariants: { cross_source_total: 18, cross_source_failed: 1, cross_source_timed_out: 0, source_local_total: 6, source_local_failed: 0, total: 24, failed: 1, budget_exceeded: false, duration_ms: 3104 },
    producers,
    attempts,
    recordTypes: [
      { type: "document", count: 104200 }, { type: "person", count: 4820 },
      { type: "message", count: 62400 }, { type: "session", count: 12602 },
    ],
    relationshipTypes: [
      { type: "mentions", count: 210400 }, { type: "authored_by", count: 98200 },
      { type: "part_of", count: 42800 },
    ],
    records: [
      record("document:orphan-0001", "document", "Quarterly ingest note", null, {
        text: "A document that resolves to no configured source.",
        fields: [{ key: "path", value: "/notes/quarterly.md", echoesTitle: false }],
        sections: [],
        outgoing: [],
        incoming: [],
      }),
    ],
    traces: [],
    runtime: {
      queueByLane: [1840, 620, 94, 12, 0],
      dlqCount: 37,
      workflowTablesPresent: true,
      resourcesStatus: "degraded",
      resourceOmissions: ["Host resource series stop at 03:11 UTC: the collector is not reporting."],
      checks: [
        {
          severity: "fail", code: "worker_missing", scope: "runtime",
          summary: "No worker has reported since 03:11 UTC.",
          actual: "0 workers reporting, last heartbeat 5h 49m ago",
          expected: "at least one worker reporting within 5 minutes",
          fix_hint: "okg worker status",
        },
        {
          severity: "fail", code: "dead_letter_jobs", scope: "runtime",
          summary: "37 jobs are in the dead-letter queue.",
          actual: "37 dead-letter jobs, oldest 2d 4h",
          expected: "0 dead-letter jobs",
          fix_hint: "okg queue dlq list",
        },
        {
          severity: "fail", code: "queue_backlog", scope: "runtime",
          summary: "The queue has a backlog of 2,566 jobs and is not draining.",
          actual: "2,566 queued, 0 completed in the last hour",
          expected: "the queue drains",
          fix_hint: "okg queue status",
        },
        {
          severity: "warn", code: "attempt_running_long", scope: "publisher",
          summary: "An update attempt has been running for six hours.",
          actual: "running since 03:00 UTC",
          expected: "attempts complete within one hour",
          fix_hint: "okg generation describe <id>",
        },
        {
          severity: "warn", code: "view_build_failed", scope: "views",
          summary: "The daily-brief view timed out building.",
          actual: "statement timeout after 300,000 ms",
          expected: "the view builds",
          fix_hint: "okg view diagnose daily-brief",
        },
      ],
    },
    featured: { record: "document:orphan-0001", type: "document", query: "quarterly", producer: "daily-brief" },
    stories: ["S2", "S3"],
    edgeCases: [
      "Runtime checks fail while every source succeeded: Home must separate them from attempts (S3).",
      "The last completed attempt was blocked on a publication check, not on a source (S3).",
      "An attempt has been running for six hours and must read as running, not as failed.",
      "37 dead-letter jobs and a 2,566-job backlog, on Home only.",
      "A view whose last run failed with a two-line timeout error.",
    ],
  };
}

// --------------------------------------------------------------------------
// 6. all-clear -- nothing is wrong. No red anywhere. (Story S5.)
// --------------------------------------------------------------------------

function allClear() {
  const rand = mulberry32(0xa11c);
  const name = "all-clear";
  const producers = ["papers", "git-files", "sessions", "slack", "docsite", "issues", "pacts", "decisions"]
    .map((sourceName, index) => ({
      name: sourceName,
      kind: "source",
      adapter: `okg.sources.${sourceName}`,
      settings: { endpoint: `https://clear.invalid/${sourceName}`, credentials: "omitted" },
      scope: "incremental",
      cadenceHours: 6,
      nextDueIso: new Date(NOW_MS + (2 + index % 5) * HOUR).toISOString(),
      state: "updated",
      lastRunIso: iso((1 + index) * HOUR),
      records: intBetween(rand, 60, 3200),
      edges: intBetween(rand, 40, 7000),
      recordTypes: ["document"],
      runtimeKeys: [],
      error: null,
      runs: [
        run((1 + index) * HOUR, "ok", intBetween(rand, 80, 3600), intBetween(rand, 60, 3200), intBetween(rand, 2000, 60000), "publish"),
        run((7 + index) * HOUR, "ok", intBetween(rand, 80, 3600), intBetween(rand, 60, 3200), intBetween(rand, 2000, 60000), "publish"),
      ],
    }));
  producers.push({
    name: "semantic-linker",
    kind: "enricher",
    adapter: "okg.enrichers.semantic-linker",
    settings: { model: "local", credentials: "omitted" },
    scope: "full",
    cadenceHours: null,
    state: "updated",
    lastRunIso: iso(1 * HOUR),
    records: 0,
    edges: 4820,
    recordTypes: [],
    runtimeKeys: [],
    error: null,
    runs: [run(1 * HOUR, "ok", 9400, 4820, 88000, "enrich")],
  });
  producers.push({
    name: "daily-brief",
    kind: "view",
    adapter: "okg.views.daily-brief",
    settings: {},
    scope: "generation",
    cadenceHours: 24,
    state: "updated",
    lastRunIso: iso(1 * HOUR),
    records: 214,
    edges: 0,
    recordTypes: ["rollup_row"],
    runtimeKeys: [],
    error: null,
    runs: [run(1 * HOUR, "ok", 1, 214, 3900, "publish")],
  });

  const attempts = [];
  for (let day = 14; day >= 0; day -= 1) {
    attempts.push(attempt(name, rand, {
      offset: day * DAY + HOUR,
      status: "published",
      nodesDelta: intBetween(rand, 40, 900),
      edgesDelta: intBetween(rand, 60, 2200),
      durationMs: intBetween(rand, 60000, 240000),
    }));
  }

  return {
    name,
    purpose: "A small, entirely healthy deployment: eight sources, one enricher, one view, "
      + "every producer ran, the latest attempt published, no failing check, empty queue.",
    nowMs: NOW_MS,
    verdict: "healthy",
    nodesLive: 18402,
    edgesLive: 44120,
    databaseSizeBytes: 1048576000,
    invariants: { cross_source_total: 8, cross_source_failed: 0, cross_source_timed_out: 0, source_local_total: 12, source_local_failed: 0, total: 20, failed: 0, budget_exceeded: false, duration_ms: 288 },
    producers,
    attempts,
    recordTypes: [
      { type: "document", count: 9420 }, { type: "commit", count: 4820 },
      { type: "person", count: 2104 }, { type: "session", count: 1204 },
      { type: "issue", count: 620 }, { type: "pact", count: 234 },
    ],
    relationshipTypes: [
      { type: "mentions", count: 18400 }, { type: "authored_by", count: 12040 },
      { type: "part_of", count: 8620 }, { type: "cites", count: 5060 },
    ],
    records: [
      record("document:weekly-note", "document", "Weekly operations note", "docsite", {
        text: "Everything ran. Nothing needs attention.",
        fields: [
          { key: "path", value: "/notes/weekly.md", echoesTitle: false },
          { key: "updated", value: "2026-09-18" },
        ],
        sections: [{ key: "body", title: "Body", value: "Everything ran. Nothing needs attention." }],
        outgoing: [],
        incoming: [],
        sourceRecordId: "notes/weekly.md",
        sourceRevision: "c0ffee1",
      }),
    ],
    traces: [],
    runtime: HEALTHY_RUNTIME,
    featured: { record: "document:weekly-note", type: "document", query: "weekly", producer: "docsite" },
    stories: ["S5"],
    edgeCases: [
      "Nothing is wrong: Home's Needs attention must be one sentence carrying the fact that "
        + "makes it so, and no red may appear anywhere on the page (S5).",
      "The queue is empty and the worker is reporting, so the runtime checks list is empty "
        + "rather than absent.",
      "Every producer state is 'updated', so the Sources count strip has one populated bucket.",
    ],
  };
}

export function allDeployments() {
  return [
    archiv3Small(),
    workspaceMixed(),
    scale200(),
    emptyDeployment(),
    brokenRuntime(),
    allClear(),
  ];
}
