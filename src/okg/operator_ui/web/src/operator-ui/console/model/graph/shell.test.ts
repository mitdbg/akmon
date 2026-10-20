// The Graph view's shell, Table and Grid models: the URL (what each page
// understands, what it names as ignored, the default drawing), producer names
// from the producers read, the Table's rows and coverage sentence, the
// failures reading again cannot change, and the copy text of the Table and
// the Grid. Asserted against the converted okg-workspace reads the harness
// serves, so the numbers are real ones.
import { describe, expect, it } from "vitest";
import graphData from "@operator-ui-design-fixtures/graph/okg-workspace.json";
import { graphGridText, graphTableText } from "@operator-ui/console/model/copyText/graph";
import { type ProducerRef, buildGraphModel, buildSourceLinks, producerNamer, producerText } from "@operator-ui/console/model/graph/derive";
import {
  isSettled, settledFailure, settledSentence, sourceLinksBasisClause, sourceLinksTimeout, sourceLinksTimeoutSentence,
} from "@operator-ui/console/model/graph/failures";
import { heatBackground, typeTypeMatrix, unlinkedSentence } from "@operator-ui/console/model/graph/grid";
import {
  NOT_A_VERSION, NOT_A_VIEW, NOT_ON_A_RECORD, NOT_ON_THIS_PAGE, askedMode, cellParam, defaultMode, graphIgnored, graphIgnoredSentence,
  modeHref, parseCell, patchedHref,
} from "@operator-ui/console/model/graph/params";
import { parseGraphTypes, parseOntology, parseSourceLinks } from "@operator-ui/console/model/graph/reads";
import {
  SYSTEM_ATTRIBUTES, coverageSentence, emptyTypes, fillSampleSentence, linksIn, linksOut, linksWithin, producerCoverage, typeDetail,
} from "@operator-ui/console/model/graph/table";
import { consoleLink } from "@operator-ui/routes/console/ConsoleRoute";
import { ApiRequestError } from "@operator-ui/shared/api/apiClient";

const data = graphData as unknown as { types: unknown; source_links: unknown; ontology: unknown };
const types = parseGraphTypes(data.types);
const ontology = parseOntology(data.ontology);
const link = (route: string, params: Record<string, string> = {}) => consoleLink(route, params);
const VERSION = types.generationId;

/** The producers read's rows for okg-workspace, as Sources lists them. */
const PRODUCERS: ProducerRef[] = [
  ...["code-structure", "doc-corpus", "pact", "git-history", "git-files", "package-manifests", "github-issues"]
    .map((name) => ({ name, ownershipId: `okg-workspace.${name}`, kind: "source" as const })),
  { name: "_system:native_views", ownershipId: "_system:native_views", kind: "source" },
  { name: "catalog:views_registry", ownershipId: "catalog:views_registry", kind: "source" },
  ...Array.from({ length: 26 }, (_, i) => ({ name: `idle-${i}`, ownershipId: `okg-workspace.idle-${i}`, kind: "source" as const })),
  { name: "code_reference", ownershipId: "_enricher:code_reference", kind: "enricher" },
  { name: "declarative_alias:entity_mention_resolves_via_alias_table", ownershipId: "_enricher:declarative_alias:entity_mention_resolves_via_alias_table", kind: "enricher" },
  ...Array.from({ length: 23 }, (_, i) => ({ name: `enricher-${i}`, ownershipId: `_enricher:e${i}`, kind: "enricher" as const })),
];

const named = buildGraphModel(types, producerNamer(PRODUCERS, link));
const unnamed = buildGraphModel(types);

describe("the Graph URL", () => {
  it("draws what `view` names, and nothing else", () => {
    expect(askedMode({ view: "grid" })).toBe("grid");
    expect(askedMode({ view: "map" })).toBe("map");
    expect(askedMode({ view: "graph" })).toBeNull();
    expect(askedMode({})).toBeNull();
  });

  it("opens on the Table under 700 px and the map above, and a record page on its drawing", () => {
    expect(defaultMode(false, 390)).toBe("table");
    expect(defaultMode(false, 1440)).toBe("map");
    expect(defaultMode(true, 390)).toBe("map");
  });

  it("switches to the whole-graph Table or Grid, dropping a record", () => {
    expect(modeHref(link, "table")).toBe("#console/graph?view=table");
    const versioned = (route: string, params: Record<string, string> = {}) => consoleLink(route, { version: VERSION, ...params });
    expect(modeHref(versioned, "grid")).toBe(`#console/graph?version=${encodeURIComponent(VERSION)}&view=grid`);
  });

  it("changes one parameter and keeps the rest, removing a cleared one", () => {
    const params = { view: "table", open: "git_commit", version: VERSION };
    const versioned = (route: string, p: Record<string, string> = {}) => consoleLink(route, { version: VERSION, ...p });
    expect(patchedHref(versioned, "graph", params, { open: null, empty: "1" }))
      .toBe(`#console/graph?version=${encodeURIComponent(VERSION)}&view=table&empty=1`);
  });

  it("writes a Grid cell with a producer id that holds a colon, and reads it back", () => {
    const cell = { grid: "ss" as const, row: "_enricher:code_reference", col: "okg-workspace.pact" };
    expect(parseCell(cellParam(cell))).toEqual(cell);
    expect(parseCell("tt:a|b")).toEqual({ grid: "tt", row: "a", col: "b" });
    for (const bad of ["", "ts:a|b", "tt:a", "tt:|b", "tt:a|", "nonsense"]) expect(parseCell(bad)).toBeNull();
  });

  it("names every parameter a page cannot honour, and why, never printing a version", () => {
    const ignored = graphIgnored(
      { view: "tabel", version: "postgres://u:secret@h/db", q: "x", fixture: "empty" }, "map",
    );
    expect(ignored).toEqual([
      { name: "view", value: "tabel", why: NOT_A_VIEW },
      { name: "version", value: null, why: NOT_A_VERSION },
      { name: "q", value: "x", why: NOT_ON_THIS_PAGE },
    ]);
    const line = graphIgnoredSentence(ignored) ?? "";
    expect(line).toContain("view=tabel");
    expect(line).not.toContain("secret");
    expect(graphIgnored({ view: "table", open: "nope" }, "table", { open: (v) => (v === "git_commit" ? null : "no such type") }))
      .toEqual([{ name: "open", value: "nope", why: "no such type" }]);
    // A parameter another page uses is not honoured on this one.
    expect(graphIgnored({ view: "grid", open: "git_commit" }, "grid")).toEqual([{ name: "open", value: "git_commit", why: NOT_ON_THIS_PAGE }]);
    expect(graphIgnoredSentence([])).toBeNull();
    // A record page is its neighbourhood: an old link's view=table is named, not obeyed silently.
    expect(graphIgnored({ id: "a", view: "table" }, "record")).toEqual([{ name: "view", value: "table", why: NOT_ON_A_RECORD }]);
    expect(graphIgnored({ id: "a", view: "map" }, "record")).toEqual([]);
  });
});

describe("producer names", () => {
  it("come from the producers read, linked to Sources, with the kind after an enricher", () => {
    const label = named.label("_enricher:code_reference");
    expect(label).toEqual({ name: "code_reference", kind: "enricher", href: "#console/sources/code_reference" });
    expect(producerText(label)).toBe("code_reference enricher");
    expect(named.name("okg-workspace.pact")).toBe("pact");
    // No invented names: the alias enricher is named as Sources names it.
    expect(named.name("_enricher:declarative_alias:entity_mention_resolves_via_alias_table"))
      .toBe("declarative_alias:entity_mention_resolves_via_alias_table");
  });

  it("are the ownership id, unlinked, while the producers read has not answered or does not list one", () => {
    expect(unnamed.label("okg-workspace.pact")).toEqual({ name: "okg-workspace.pact", kind: null, href: null });
    expect(named.label("okg-workspace.gone")).toEqual({ name: "okg-workspace.gone", kind: null, href: null });
  });

  it("colour the first eight sources and share the neutral among the rest", () => {
    expect(named.sources).toHaveLength(9);
    expect(named.srcColor[named.sources[0]]).toBe("var(--okg-graph-cat-1)");
    expect(named.srcColor[named.sources[8]]).toBe("var(--okg-graph-cat-9)");
    expect(named.smallSources).toEqual([named.sources[8]]);
  });
});

describe("the Table rows", () => {
  it("count links in, out and within a type, so every relationship is in one column", () => {
    expect(linksIn(named.patterns, "code_symbol")).toBe(262789);
    expect(linksOut(named.patterns, "code_symbol")).toBe(0);
    expect(linksWithin(named.patterns, "code_symbol")).toBe(148652);
    const all = named.types.reduce((a, t) => a + linksOut(named.patterns, t) + linksWithin(named.patterns, t), 0);
    expect(all).toBe(named.patterns.reduce((a, p) => a + p.n, 0));
  });

  it("list platform-written fields as system, and flag only real drift", () => {
    const declared = Object.fromEntries(ontology.types.map((t) => [t.type, t]));
    const d = typeDetail("acceptance_criterion", ontology, declared);
    expect(d.sample?.sampled).toBe(1000);
    expect(d.fields.find((f) => f.name === "nomos_source_lineage")?.origin).toBe("system");
    expect(d.fields.filter((f) => f.origin === "undeclared").every((f) => !SYSTEM_ATTRIBUTES.has(f.name))).toBe(true);
    expect(d.fields.find((f) => f.name === "created_at")?.fill).toBeNull();
    expect(d.used.length + d.unused.length).toBe(d.rules);
    expect(emptyTypes(named, ontology)).toHaveLength(115);
  });

  it("say the fill sample is taken before access rules, and mark a field the policy decides on as not sampled", () => {
    expect(fillSampleSentence(ontology)).toBe(
      "Field fill is sampled from the first 1,000 records of each type, before access rules, and only for declared fields.",
    );
    const withheld = { ...ontology, fillExcluded: ["created_at"] };
    expect(fillSampleSentence(withheld)).toBe(
      "Field fill is sampled from the first 1,000 records of each type, before access rules, and only for declared fields; "
        + "fields the access policy decides on are not sampled.",
    );
    const declared = Object.fromEntries(withheld.types.map((t) => [t.type, t]));
    const field = typeDetail("acceptance_criterion", withheld, declared).fields.find((f) => f.name === "created_at");
    expect(field?.notSampled).toBe(true);
    expect(field?.fill).toBeNull();
  });

  it("say how many of the deployment's producers this version holds anything from", () => {
    const coverage = producerCoverage(named, PRODUCERS);
    expect(coverage).toEqual({ withRecords: 9, sources: 35, enrichersWithLinks: 2, enrichers: 25, unlisted: [] });
    expect(coverageSentence(named, coverage)).toBe("9 of 35 sources made records in this version; 2 of 25 enrichers added relationships.");
    expect(coverageSentence(named, null)).toBe("9 producers made records in this version; 2 enrichers added relationships.");
    const partial = producerCoverage(named, PRODUCERS.slice(1));
    expect(partial.unlisted).toEqual(["okg-workspace.code-structure"]);
    expect(partial.withRecords).toBe(8);
  });
});

describe("the Grid", () => {
  it("shades with the theme's heat token, and says which types have no relationships in words", () => {
    expect(heatBackground(1, 10)).toContain("var(--okg-graph-heat)");
    expect(typeTypeMatrix(named).unlinked.sort()).toEqual(["view", "view_definition"]);
    expect(unlinkedSentence(["view"])).toBe("view has no relationships.");
    expect(unlinkedSentence(["view", "view_definition"])).toBe("view and view_definition have no relationships.");
  });

  it("says what the source-links answer says about itself", () => {
    expect(sourceLinksBasisClause(11600, true)).toBe("Computed in 11.6 s when first asked for, and kept for this version.");
    expect(sourceLinksBasisClause(900, false)).toBe("Computed in 0.9 s for this read.");
    expect(sourceLinksBasisClause(null, false)).toBeNull();
  });
});

const failure = (status: number, detail: Record<string, unknown>) => new ApiRequestError({
  route: "/viz/api/console/graph/types", status, responseText: JSON.stringify({ detail }),
});

describe("failures reading again cannot change", () => {
  it("read an unpublished deployment as empty, and an unpublished asked version by name", () => {
    const none = failure(404, { reason: "version_not_published" });
    expect(settledFailure(none, null)).toEqual({ kind: "nothing_published" });
    expect(settledSentence(settledFailure(none, null)!, "empty", "x")).toBe("No version has been published for empty; there is nothing to draw yet.");
    const asked = settledFailure(none, VERSION);
    expect(asked).toEqual({ kind: "version_not_published", version: VERSION });
    expect(settledSentence(asked!, null, "x")).toBe(`Version ${VERSION.slice(-12)} is not published in this deployment. Choose a published version from the version picker.`);
  });

  it("name a missing record and a policy refusal plainly", () => {
    const missing = settledFailure(failure(404, { reason: "record_not_found" }), null, "a:b");
    expect(settledSentence(missing!, null, "x")).toBe("This version holds no record with the id a:b that you may open.");
    const refused = settledFailure(failure(403, { reason: "policy_denied", error: "The policy hides this read." }), null);
    expect(settledSentence(refused!, null, "The source links")).toBe(
      "The source links was refused by the read policy (policy_denied). The policy hides this read. A refusal is not a fault; reading again gives the same answer.",
    );
    expect(isSettled(failure(403, {}))).toBe(true);
  });

  it("leave every other failure to the shared surface and its Read again", () => {
    expect(settledFailure(failure(503, { reason: "count_rollup_unavailable" }), null)).toBeNull();
    expect(settledFailure(failure(404, { reason: "something_else" }), null)).toBeNull();
    expect(settledFailure(new Error("x"), null)).toBeNull();
    expect(isSettled(failure(503, {}))).toBe(false);
  });
});

describe("the source-links timeout", () => {
  const timeout = (elapsed: number) => new ApiRequestError({
    route: "/viz/api/console/graph/source-links", status: 503,
    responseText: JSON.stringify({ detail: { reason: "source_links_unavailable", cause: "timeout", elapsed_ms: elapsed, timeout_ms: 18000 } }),
  });

  it("is said in words, without repeating the limit as the elapsed time", () => {
    expect(sourceLinksTimeoutSentence(sourceLinksTimeout(timeout(18004))!)).toBe(
      "This grid is computed when it is asked for, and the computation stopped at its 18 s limit. Nothing is missing from the graph.",
    );
    expect(sourceLinksTimeoutSentence(sourceLinksTimeout(timeout(19500))!)).toContain("at its 18 s limit, after 19.5 s.");
  });

  it("is not claimed for any other failure", () => {
    const other = new ApiRequestError({ route: "r", status: 503, responseText: JSON.stringify({ detail: { reason: "count_rollup_unavailable" } }) });
    expect(sourceLinksTimeout(other)).toBeNull();
    expect(sourceLinksTimeout(new Error("x"))).toBeNull();
    expect(sourceLinksTimeout(new ApiRequestError({ route: "r", status: 502 }))).toBeNull();
  });
});

describe("copy text", () => {
  const context = { deployment: "okg-workspace", version: types.generationId, publishedAt: null, nowMs: 0 };

  it("copies the Table as shown: the three link columns, the open row, the listed empties", () => {
    const text = graphTableText(named, context, {
      ontology, coverage: producerCoverage(named, PRODUCERS), open: "acceptance_criterion", showEmpty: true,
    });
    expect(text).toContain("| Type | Records | Links in | Links out | Within type | Made by | Declared in |");
    expect(text).toContain("| `code_symbol` | 74,681 | 262,789 | none | 148,652 | code-structure 74,681 | code |");
    expect(text).toContain("## Declared, no records");
    expect(text).toContain("### `acceptance_criterion`: fields");
    expect(text).toContain("| `nomos_source_lineage` | system |");
    expect(text).toContain("9 of 35 sources made records in this version; 2 of 25 enrichers added relationships.");
    expect(text).toContain("## Enrichers");
    expect(text).toContain("822,142 records, 1,571,775 relationships");
    const closed = graphTableText(named, context, { ontology: null, coverage: null, open: null, showEmpty: false });
    expect(closed).not.toContain("Declared in");
    expect(closed).not.toContain("fields");
  });

  it("copies the Grid with exact counts and the open cell", () => {
    const links = buildSourceLinks(parseSourceLinks(data.source_links), named);
    const text = graphGridText(named, context, { links, cell: { grid: "tt", row: "source_file", col: "code_symbol" }, linksFailed: false });
    expect(text).toContain("## Which types link to which");
    expect(text).toContain("## Which sources link to which");
    expect(text).toContain("### Open cell: `source_file` → `code_symbol`");
    expect(text).toContain("| contains | 74,681 | code-structure 74,681 |");
    const failed = graphGridText(named, context, { links: null, cell: null, linksFailed: true });
    expect(failed).toContain("Not read: the source-links read failed.");
  });
});
