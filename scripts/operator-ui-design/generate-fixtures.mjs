#!/usr/bin/env node
// Write the six mock deployments to design/fixtures/<name>.json.
//
//   node scripts/operator-ui-design/generate-fixtures.mjs
//   node scripts/operator-ui-design/generate-fixtures.mjs --check
//
// `--check` regenerates every fixture in memory and exits 1 on any byte
// difference, so a hand edit to a generated file, or a generator that stopped
// being deterministic, is a failure rather than a silent drift between the
// fixture the tests read and the one the mocks were drawn against.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { allDeployments, CLOCK } from "./lib/deployments.mjs";
import { ARTIFACT_DIR_NAME, buildArtifacts } from "./lib/artifacts.mjs";
import {
  attemptDetailRead,
  attemptsByDayRead,
  consoleHealthRead,
  deploymentsRead,
  enricherDiagnosticsRead,
  generationsRead,
  latestPublished,
  livenessRead,
  ontologyRollupRead,
  recordArtifactRead,
  recordDetailRead,
  recordListRead,
  recordPayloadFile,
  recordRawRead,
  recordSearchRead,
  recordTypesRead,
  producersRead,
  sourceDiagnosticsRead,
  sourcesRead,
  tracesRead,
  versionsRead,
  viewDiagnosticsRead,
  workflowsRead,
} from "./lib/reads.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(HERE, "fixtures");
const ARTIFACT_DIR = join(OUT_DIR, ARTIFACT_DIR_NAME);
const PAYLOAD_DIR = join(OUT_DIR, "payloads");

/** The lookup key a read becomes. Must match `readKey()` in
 *  src/operator-ui/design-harness/fixtureClient.ts exactly: route, then the params
 *  that are neither null nor `refresh`, sorted by name. */
export function readKey(route, params = {}) {
  const pairs = Object.entries(params)
    .filter(([name, value]) => value !== null && value !== undefined && name !== "refresh")
    .map(([name, value]) => [name, String(value)])
    .sort((a, b) => a[0].localeCompare(b[0]));
  if (pairs.length === 0) return route;
  return `${route}?${pairs.map(([n, v]) => `${n}=${encodeURIComponent(v)}`).join("&")}`;
}

function buildReads(spec, allNames, artifacts) {
  const version = latestPublished(spec)?.generationId ?? null;
  const reads = {};
  const put = (route, params, payload) => {
    reads[readKey(route, params)] = payload;
  };

  put("/viz/api/deployments", {}, deploymentsRead(spec, allNames));
  put("/viz/api/health", {}, livenessRead(spec));
  put("/viz/api/traces", {}, tracesRead(spec));
  put("/viz/api/workflows", {}, workflowsRead(spec));

  put("/viz/api/console/health", {}, consoleHealthRead(spec));
  put("/viz/api/console/generations", {}, generationsRead(spec));
  // A detail read per attempt the mocks will actually open: every attempt
  // that did not publish, plus the six newest, capped at twelve. A read for
  // all fifty rows the list returns would put fifty copies of a 200-source
  // rollup in scale-200 and grow that fixture sixfold; the harness reports a
  // read it does not hold by name, which is the same answer the product
  // gives for an attempt it cannot find.
  const newestFirst = [...spec.attempts].reverse();
  const openable = [
    ...newestFirst.filter((a) => a.status !== "published"),
    ...newestFirst.filter((a) => a.status === "published").slice(0, 6),
  ].slice(0, 12);
  for (const attempt of openable) {
    put("/viz/api/console/attempts/detail", { id: attempt.generationId },
      attemptDetailRead(spec, attempt.generationId));
  }
  put("/viz/api/console/sources", {}, sourcesRead(spec));
  put("/viz/api/console/versions", {}, versionsRead(spec));
  put("/viz/api/console/ontology-rollup", {}, ontologyRollupRead(spec));

  // The metric chart: attempts by outcome per day, over each span the design
  // offers (7, 14, 30 and 90 days), plus the one-day read the health view uses.
  for (const days of [1, 7, 14, 30, 90]) {
    put("/viz/api/console/attempts", { range: `${days}d` }, attemptsByDayRead(spec, days));
  }

  // Diagnostics: the counts read the overview uses, the unparameterised read
  // Sources uses, and one detail read per configured producer.
  // The one producers read the live Sources view is built on: every
  // producer once, keyed by its declared ownership_id.
  put("/viz/api/console/producers", {}, producersRead(spec, null));
  put("/viz/api/console/source-diagnostics", {}, sourceDiagnosticsRead(spec, null));
  put("/viz/api/console/enricher-diagnostics", {}, enricherDiagnosticsRead(spec, null));
  put("/viz/api/console/view-diagnostics", {}, viewDiagnosticsRead(spec, null));
  // One detail read per producer the mocks will actually open: every failed,
  // withheld, excluded or running one, the featured one, and enough healthy
  // ones to exercise the rail. At 220 producers a detail read for each would
  // put 220 copies of the same table in one file.
  const interesting = spec.producers.filter(
    (p) => p.state !== "updated" || p.name === spec.featured.producer,
  );
  const healthy = spec.producers.filter((p) => !interesting.includes(p)).slice(0, 4);
  for (const producer of [...interesting, ...healthy]) {
    const build = producer.kind === "source" ? sourceDiagnosticsRead
      : producer.kind === "enricher" ? enricherDiagnosticsRead : viewDiagnosticsRead;
    const route = `/viz/api/console/${producer.kind}-diagnostics`;
    const params = producer.kind === "view"
      ? { name: producer.name, run_limit: 20 }
      : { name: producer.name, mode: "detail", run_limit: 20 };
    put(route, params, build(spec, producer.name));
    put(
      "/viz/api/console/producers",
      { name: producer.name, run_limit: 20 },
      producersRead(spec, producer.name, 20),
    );
  }

  // Record reads, pinned to the published version and also under no version at
  // all, because Explore reads before the version picker has resolved.
  const versions = version ? [version, null] : [null];
  for (const pinnedVersion of versions) {
    const versionParam = pinnedVersion ? { version: pinnedVersion } : {};
    put("/viz/api/console/records/types", versionParam, recordTypesRead(spec));
    for (const { type } of spec.recordTypes.slice(0, 8)) {
      put("/viz/api/console/records/list", { ...versionParam, type, offset: 0, limit: 50 },
        recordListRead(spec, type, 0, 50));
    }
    // The search reads: the empty query, the featured one, and whatever
    // else the spec lists (`searches`: query, filters, offsets), so the
    // results page's paging, rail filters and empty state have real reads.
    const searches = [
      { q: "", filters: {}, offsets: [0] },
      { q: spec.featured.query, filters: {}, offsets: [0] },
      ...(spec.searches ?? []),
    ];
    for (const search of searches) {
      for (const offset of search.offsets ?? [0]) {
        put("/viz/api/console/records/search", { ...versionParam, q: search.q, ...search.filters, offset, limit: 20 },
          recordSearchRead(spec, search.q, search.filters, offset, 20));
      }
    }
    for (const rec of spec.records) {
      put("/viz/api/console/records/detail", { ...versionParam, id: rec.nodeId },
        recordDetailRead(spec, rec.nodeId));
      // The record page's two other reads (backend gap 4): the original file
      // and the raw layers, both under the source policy.
      put("/viz/api/console/records/artifact", { ...versionParam, id: rec.nodeId },
        recordArtifactRead(spec, rec.nodeId, artifacts));
      put("/viz/api/console/records/raw", { ...versionParam, id: rec.nodeId },
        recordRawRead(spec, rec.nodeId, artifacts));
    }
  }

  return reads;
}

function buildFixture(spec, allNames, artifacts) {
  const published = latestPublished(spec);
  return {
    name: spec.name,
    purpose: spec.purpose,
    clock: CLOCK,
    stories: spec.stories,
    edge_cases: spec.edgeCases,
    // The numbers design/fixtures.test.ts asserts after loading this fixture
    // through the application's own loaders. They are written here, not derived
    // in the test, so a generator change that quietly moves a count fails
    // instead of re-baselining itself.
    expect: {
      deployment: spec.name,
      version: published ? published.generationId : null,
      nodes_live: spec.nodesLive,
      edges_live: spec.edgesLive,
      source_count: spec.producers.filter((p) => p.kind === "source").length,
      enricher_count: spec.producers.filter((p) => p.kind === "enricher").length,
      view_count: spec.producers.filter((p) => p.kind === "view").length,
      failed_producer_count: spec.producers.filter((p) => p.state === "failed").length,
      never_run_producer_count: spec.producers.filter((p) => p.state === "never run").length,
      attempt_count: spec.attempts.length,
      published_attempt_count: spec.attempts.filter((a) => a.status === "published").length,
      blocked_attempt_count: spec.attempts.filter((a) => a.status === "blocked").length,
      running_attempt_count: spec.attempts.filter((a) => a.status === "running").length,
      record_type_count: spec.recordTypes.length,
      relationship_type_count: spec.relationshipTypes.length,
      // The row count the LIVE sources read returns, before Sources collapses
      // registry name and runtime key onto one row.
      source_read_row_count: spec.producers
        .filter((p) => p.kind === "source")
        .reduce((total, p) => total + 1 + (p.runtimeKeys ?? []).length, 0),
      featured_record: spec.featured.record,
      failing_check_count: spec.runtime.checks.filter((c) => c.severity === "fail").length,
      queue_total: spec.runtime.queueByLane.reduce((a, b) => a + b, 0),
      dlq_count: spec.runtime.dlqCount,
    },
    reads: buildReads(spec, allNames, artifacts),
  };
}

function serialise(fixture) {
  return `${JSON.stringify(fixture, null, 2)}\n`;
}

/** Write one file, or in check mode compare it byte for byte. */
function emit(path, bytes, check, label) {
  if (!check) {
    writeFileSync(path, bytes);
    console.log(`wrote ${path} (${bytes.length} bytes)`);
    return 0;
  }
  let onDisk;
  try {
    onDisk = readFileSync(path);
  } catch {
    console.error(`MISSING: ${path} has not been generated`);
    return 1;
  }
  if (!onDisk.equals(bytes)) {
    console.error(`DRIFT: ${path} differs from the generator (${onDisk.length} bytes on disk, ${bytes.length} regenerated)`);
    return 1;
  }
  console.log(`ok   ${label}: ${bytes.length} bytes`);
  return 0;
}

function main() {
  const check = process.argv.includes("--check");
  const specs = allDeployments();
  const names = specs.map((spec) => spec.name);
  const artifacts = buildArtifacts();
  mkdirSync(OUT_DIR, { recursive: true });
  mkdirSync(ARTIFACT_DIR, { recursive: true });
  mkdirSync(PAYLOAD_DIR, { recursive: true });
  let failures = 0;
  // The real files the Content tab renders, then each record's retained
  // payload as the file the raw read's URL names.
  for (const file of Object.values(artifacts)) {
    failures += emit(join(ARTIFACT_DIR, file.name), file.bytes, check, `artifacts/${file.name}`);
  }
  for (const spec of specs) {
    for (const record of spec.records) {
      const payload = recordPayloadFile(record, artifacts);
      if (!payload || payload.artifact) continue;
      failures += emit(join(PAYLOAD_DIR, payload.name), payload.bytes, check, `payloads/${payload.name}`);
    }
  }
  for (const spec of specs) {
    const path = join(OUT_DIR, `${spec.name}.json`);
    const text = serialise(buildFixture(spec, names, artifacts));
    if (check) {
      let onDisk;
      try {
        onDisk = readFileSync(path, "utf8");
      } catch {
        console.error(`MISSING: ${path} has not been generated`);
        failures += 1;
        continue;
      }
      if (onDisk !== text) {
        console.error(
          `DRIFT: ${path} differs from the generator `
          + `(${onDisk.length} bytes on disk, ${text.length} regenerated)`,
        );
        failures += 1;
      } else {
        console.log(`ok   ${spec.name}: ${text.length} bytes`);
      }
    } else {
      writeFileSync(path, text);
      console.log(`wrote ${path} (${text.length} bytes)`);
    }
  }
  if (failures) {
    console.error(`${failures} fixture(s) do not match the generator; re-run without --check`);
    return 1;
  }
  return 0;
}

process.exit(main());
