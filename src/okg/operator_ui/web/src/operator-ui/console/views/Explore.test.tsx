// Explore, asserted against the six fixtures rather than against a shape
// written here.
//
// The captures are what the operator approves. These tests hold what a
// screenshot cannot: that the chips come from the types read in count order
// and bound themselves at 214 types, that an exact id match is listed before
// the text matches, that the match is marked, that the arrow keys walk the
// rows and stop at the ends, that the Related tab states the cap and the
// omitted count, that a policy-hidden field says "hidden" and names the
// policy, that a refused payload is one sentence naming the condition, that
// a deep link reads at the version it names, that a record read from an older
// version says a newer one exists, and that the search read on `empty` is
// refused rather than answered blank.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { createFixtureApiClient, FixtureReadMissing } from "@operator-ui/design-harness/fixtureClient";
import { DESIGN_FIXTURES, fixtureByName } from "@operator-ui/design-harness/fixtures";
import { designLink } from "@operator-ui/design-harness/DesignHarness";
import { ConsoleExplore, landingLine, type ExplorePageProps } from "@operator-ui/console/views/Explore";
import { ConsoleRecordPage } from "@operator-ui/console/views/Record";
import { Results } from "@operator-ui/console/views/SearchResults";
import { exploreText, type PageContext } from "@operator-ui/console/model/copyText";
import { extractedText, hasContent, viewerFor, withoutPolicyPrefix } from "@operator-ui/console/model/explore/content";
import { buildChips, buildListing, CHIP_CAP, contentsLine, nothingToSearchSentence } from "@operator-ui/console/model/explore/landing";
import { NO_STORED_DATE, exploreQuery, exploreRecordHash, ignoredSentence, parseExploreParams, versionToRead } from "@operator-ui/console/model/explore/params";
import { highlight, effectiveFilters, parseQuery, readQuery, searchRequest } from "@operator-ui/console/model/explore/query";
import { INLINE_PAYLOAD_LIMIT, parseArtifactRead, parseRawRead, payloadText, refusalSentence } from "@operator-ui/console/model/explore/raw";
import { buildRecordPage, FIELD_FOLD, foldValue, newerVersionSentence, openInSourceUrl, parseExploreRecord, type ExploreRecord } from "@operator-ui/console/model/explore/record";
import { capSentence, RELATED_PAGE } from "@operator-ui/console/model/explore/related";
import { buildResults, moveFocus, aboutLine, filterHref, parseExploreSearch, showingLine, skippedTypesLine, type ExploreSearchPage } from "@operator-ui/console/model/explore/search";
import {
  listRecords,
  loadRecordTypes,
  loadVersions,
  SEARCH_PAGE_SIZE,
} from "@operator-ui/console/api/searchApi";
import {
  parseVersionList,
  SearchResponseError,
} from "@operator-ui/console/api/searchData";

const NOW_MS = Date.parse("2026-09-18T09:00:00.000Z");
const NONE = parseExploreParams({});

function fixtureNamed(name: string) {
  const fixture = fixtureByName(name);
  if (!fixture) throw new Error(`no fixture named ${name}`);
  return fixture;
}

async function baseFor(name: string) {
  const fixture = fixtureNamed(name);
  const api = createFixtureApiClient(fixture);
  const versions = await loadVersions(api);
  const version = versions.latest;
  const types = version ? (await loadRecordTypes(api, version)).payload : [];
  return { fixture, api, versions, version, types, link: designLink(name) };
}

function contextFor(deployment: string, version: string | null): PageContext {
  return { deployment, version, publishedAt: null, nowMs: NOW_MS };
}

/** The page's props with the shell-owned pieces filled in. */
function pageProps(name: string, props: Omit<ExplorePageProps, "link" | "search" | "help" | "onHelp">): ExplorePageProps {
  return { ...props, link: designLink(name), search: null, help: false, onHelp: () => {} };
}

/** The record page as the route builds it: the detail, artifact and raw
 *  reads, then the model. */
async function recordFor(name: string, id: string, params = NONE) {
  const base = await baseFor(name);
  if (!base.version) throw new Error(`${name} has no version`);
  const envelope = await base.api.get("/viz/api/console/records/detail", { version: base.version, id });
  const detail = parseExploreRecord(envelope);
  if (!detail.payload.found) throw new Error(`${name} has no record ${id}`);
  const artifact = parseArtifactRead(await base.api.get("/viz/api/console/records/artifact", { version: base.version, id })).payload;
  const raw = parseRawRead(await base.api.get("/viz/api/console/records/raw", { version: base.version, id })).payload;
  const model = buildRecordPage(
    detail.payload.record, { artifact, raw }, detail.version, base.versions.versions, params, base.link,
    base.versions.deployment, NOW_MS, SEARCH_PAGE_SIZE,
  );
  return { ...base, detail, record: detail.payload.record, artifact, raw, model };
}

describe("the URL", () => {
  it("reads q, type, record, version, offset and tab, and defaults the rest", () => {
    // sort and since are no longer fields of the parsed URL. They are kept as
    // `ignored` entries instead, so nothing downstream can read them as a
    // filter in force while the page reports them as refused.
    expect(parseExploreParams({ q: " handbook ", type: "document", offset: "20", tab: "raw", rel: "touches", dir: "incoming", roffset: "50", source: "s", sort: "newest", since: "month", help: "1" })).toEqual({
      q: "handbook", type: "document", record: null, version: null, offset: 20, tab: "raw", rel: "touches", dir: "incoming", roffset: 50,
      source: "s", help: true, overlong: null, tabAsked: true,
      ignored: [
        { name: "sort", value: "newest", why: NO_STORED_DATE, showValue: true },
        { name: "since", value: "month", why: NO_STORED_DATE, showValue: true },
      ],
    });
    const parsed = parseExploreParams({ dir: "sideways", roffset: "-1", sort: "sideways" });
    expect(parsed).toMatchObject({ dir: "both", roffset: 0, help: false });
    expect("sort" in parsed).toBe(false);
    expect("since" in parsed).toBe(false);
    // Even a sort value this view never offered is reported rather than
    // dropped: what was refused is what the URL said.
    expect(parsed.ignored).toEqual([{ name: "sort", value: "sideways", why: NO_STORED_DATE, showValue: true }]);
    // Content is the default tab: a reader who opens a record wants what it
    // says, not the source keys under it.
    expect(parseExploreParams({ offset: "-3", tab: "nope" })).toMatchObject({ offset: 0, tab: "content", ignored: [] });
  });

  it("carries only the parameters that are set, and never tab=content", () => {
    expect(exploreQuery({ q: "", type: null, record: "a:b", tab: "content", offset: 0 })).toEqual({ record: "a:b" });
    // Fields is no longer the default, so a link to it says so.
    expect(exploreQuery({ record: "a:b", tab: "fields" })).toEqual({ record: "a:b", tab: "fields" });
    expect(exploreQuery({ q: "x", tab: "related", offset: 20 })).toEqual({ q: "x", tab: "related", offset: "20" });
    expect(exploreQuery({ rel: "touches", dir: "both", roffset: 50 })).toEqual({ rel: "touches", roffset: "50" });
    // Cut: an href this view builds never re-adds a parameter the read
    // refuses, so following any link drops a stale sort= or since= behind.
    expect(exploreQuery({ q: "x", source: "s", help: true })).toEqual({ q: "x", source: "s", help: "1" });
  });

  it("opens a chat deep link at the version the chat read, in one hop", () => {
    // A REAL generation id, not "okg:v:1". The parser honours `?version=` only
    // where the value is shaped like one -- Explore is the only view whose
    // reads take a version, and a value this console will not print or put on
    // a link must not be the one thing it sends to a read. A chat deep link
    // always carries a real id, so nothing about this case changed; what the
    // shorthand id was standing in for is spelled out instead.
    const id = "okg:archiv3:branch:default:gen:20260921T024809349784Z:5c5404d36118";
    const link = designLink("archiv3-small");
    const hash = exploreRecordHash(link, "cmssw_release:CMSSW_14_0_0", id);
    expect(hash).toBe(
      "#explore?fixture=archiv3-small&record=cmssw_release%3ACMSSW_14_0_0"
      + `&version=${encodeURIComponent(id)}`,
    );
    const params = parseExploreParams({ record: "cmssw_release:CMSSW_14_0_0", version: id });
    // The URL's version outranks the console's and the latest.
    expect(versionToRead(params, "okg:v:reading", "okg:v:latest")).toBe(id);
    expect(versionToRead(NONE, "okg:v:reading", "okg:v:latest")).toBe("okg:v:reading");
    expect(versionToRead(NONE, null, "okg:v:latest")).toBe("okg:v:latest");
    expect(versionToRead(NONE, null, null)).toBeNull();
  });

  it("honours no `?version=` that is not shaped like a generation id", () => {
    // Each of these arrived on a real URL or is one keystroke from doing so.
    // None reaches a read: the view answers for the version the console is
    // reading, exactly as it does when no version was asked for.
    for (const junk of [
      "x",
      "5c5404d36118",
      "postgresql://okg_admin:hunter2@10.0.0.4:5432/okg_prod",
      "why is this page empty",
      "okg:v:1",
    ]) {
      const params = parseExploreParams({ record: "cmssw_release:CMSSW_14_0_0", version: junk });
      expect(params.version, junk).toBeNull();
      expect(versionToRead(params, "okg:v:reading", "okg:v:latest"), junk)
        .toBe("okg:v:reading");
    }
  });
});

describe("the landing", () => {
  it("draws the chips from the types read, ordered by count, with the version's counts", async () => {
    const base = await baseFor("archiv3-small");
    const strip = buildChips(base.types, NONE, base.link, null);
    expect(strip.chips.map((chip) => [chip.type, chip.count])).toEqual([
      ["cmssw_release", 2414], ["document", 96], ["paper", 58], ["twiki_page", 21], ["indico_event", 14], ["person", 2],
    ]);
    expect(strip.moreTypes).toBe(0);
    expect(strip.perQuery).toBe(false);
    expect(strip.chips[0].href).toBe("#explore?fixture=archiv3-small&type=cmssw_release");
    expect(contentsLine(base.types, base.version as string)).toBe("2,605 records · 6 types · version eea4e0d4e88d");
  });

  it("bounds itself at 214 types: the top chips, then how many more and how many records they hold", async () => {
    const base = await baseFor("scale-200");
    const strip = buildChips(base.types, NONE, base.link, null);
    expect(base.types.length).toBe(214);
    expect(strip.chips.length).toBe(CHIP_CAP);
    expect(strip.moreTypes).toBe(214 - CHIP_CAP);
    expect(strip.moreRecords).toBe(
      base.types.reduce((sum, row) => sum + row.count, 0) - strip.chips.reduce((sum, chip) => sum + (chip.count ?? 0), 0),
    );
    expect(buildChips(base.types, NONE, base.link, null, true).chips.length).toBe(214);
  });

  it("keeps a selected type past the cap visible, and a click on it clears the filter", async () => {
    const base = await baseFor("scale-200");
    const last = base.types[base.types.length - 1].type;
    const strip = buildChips(base.types, parseExploreParams({ type: last }), base.link, null);
    const chip = strip.chips.find((candidate) => candidate.type === last);
    expect(chip?.selected).toBe(true);
    expect(chip?.href).toBe("#explore?fixture=scale-200");
  });

  it("lists one type's records as a table in id order, paged at 50, with the count line first", async () => {
    const base = await baseFor("archiv3-small");
    const params = parseExploreParams({ type: "document" });
    const page = await listRecords(base.api, { version: base.version as string, type: "document", offset: 0 });
    const listing = buildListing(page.payload, base.types, params, base.link);
    expect(listing.countLine).toBe("Records of type document · 1–1 of 96, in id order");
    expect(listing.rows[0].href).toBe("#explore?fixture=archiv3-small&type=document&record=document%3Arelease-plan");
    const markup = renderToStaticMarkup(ConsoleExplore(pageProps("archiv3-small", {
      deployment: "archiv3-small", version: base.version, types: base.types, params,
      chips: buildChips(base.types, params, base.link, null), onMoreChips: null,
      results: null, listing, pending: null, focus: null, context: contextFor("archiv3-small", base.version),
    })));
    expect(markup.indexOf("Records of type document")).toBeLessThan(markup.indexOf("<table"));
    expect(markup).toContain('data-copy="record id"');
  });

  it("says there is nothing to search on empty, and the fixture refuses the search read", async () => {
    const base = await baseFor("empty");
    expect(base.version).toBeNull();
    expect(nothingToSearchSentence("empty")).toBe("No version has been published for empty; there is nothing to search yet.");
    const markup = renderToStaticMarkup(ConsoleExplore(pageProps("empty", {
      deployment: "empty", version: null, types: [], params: NONE, chips: null, onMoreChips: null,
      results: null, listing: null, pending: null, focus: null, context: contextFor("empty", null),
    })));
    expect(markup).toContain("there is nothing to search yet");
    expect(markup).not.toContain("explore-chips");
    await expect(base.api.get("/viz/api/console/records/search", searchRequest("unpublished", effectiveFilters(parseExploreParams({ q: "anything" })), 0, SEARCH_PAGE_SIZE)))
      .rejects.toBeInstanceOf(FixtureReadMissing);
  });
});

/** The search read as the route asks for it, parsed with the pass's fields. */
async function searchFor(name: string, raw: Record<string, string>) {
  const base = await baseFor(name);
  const params = parseExploreParams(raw);
  const request = searchRequest(base.version as string, effectiveFilters(params), params.offset, SEARCH_PAGE_SIZE);
  const page = parseExploreSearch(await base.api.get("/viz/api/console/records/search", request)).payload;
  const model = buildResults(page, params, base.link, name, base.version as string, SEARCH_PAGE_SIZE);
  return { ...base, params, page, model };
}

describe("the query syntax", () => {
  it("reads bare words, quoted phrases and the three operators", () => {
    expect(parseQuery("ingest stalled")).toMatchObject({ text: "ingest stalled", words: ["ingest", "stalled"], phrases: [], id: null, type: null, source: null });
    expect(parseQuery('"blocked ingest" retry type:issue source:source-07-git')).toMatchObject({
      text: "blocked ingest retry", phrases: ["blocked ingest"], words: ["retry"], type: "issue", source: "source-07-git", id: null,
    });
    expect(parseQuery("id:issue:ingest-001 other")).toMatchObject({ id: "issue:ingest-001", words: ["other"] });
    expect(readQuery(parseQuery("id:issue:ingest-001 other"))).toBe("issue:ingest-001");
    expect(readQuery(parseQuery('"a b" c'))).toBe("a b c");
    expect(parseQuery("type:a type:b").type).toBe("b");
  });

  it("turns the query and the URL into one set of filters, the query's operators winning", () => {
    const filters = effectiveFilters(parseExploreParams({ q: "ingest type:issue", type: "document", source: "s", sort: "newest", since: "month" }));
    expect(filters).toMatchObject({ type: "issue", source: "s" });
    // Cut, and cut all the way down: a refused parameter is not a filter in
    // force, so it is absent from the effective filters AND from the request.
    expect("sort" in filters).toBe(false);
    expect("since" in filters).toBe(false);
    expect(searchRequest("v", filters, 20, 20)).toEqual({ version: "v", q: "ingest", type: "issue", source: "s", offset: 20, limit: 20 });
    expect(searchRequest("v", effectiveFilters(parseExploreParams({ q: "x" })), 0, 20)).toEqual({ version: "v", q: "x", type: null, source: null, offset: 0, limit: 20 });
  });

  it("changes an operator-set filter in the query, and a URL-set one beside it", () => {
    const link = designLink("workspace-mixed");
    expect(filterHref(link, parseExploreParams({ q: "ingest type:issue" }), { type: null })).toBe("#explore?fixture=workspace-mixed&q=ingest");
    expect(filterHref(link, parseExploreParams({ q: "ingest type:issue" }), { type: "decision" })).toBe("#explore?fixture=workspace-mixed&q=ingest+type%3Adecision");
    // Cut: sort is not a filter this read has, so it is not a href this view
    // can build. Changing the page resets the offset and keeps the rest.
    expect(filterHref(link, parseExploreParams({ q: "ingest", type: "issue", offset: "20" }), { offset: 0 })).toBe("#explore?fixture=workspace-mixed&q=ingest&type=issue");
  });
});

describe("the results", () => {
  it("counts, times and pages the forty-three matches, with the rail's counts from the read", async () => {
    const { model, page } = await searchFor("workspace-mixed", { q: "ingest" });
    expect(page.totalCount).toBe(43);
    expect(model.aboutLine).toBe("43 results");
    expect(model.textMatches.length).toBe(20);
    // Two facts on the meta line, not three: no published generation carries
    // a per-record date, so "updated ..." is cut rather than left blank.
    expect(model.textMatches[3].meta).toEqual(["issue", "source-07-git"]);
    expect("updated" in model.textMatches[3].hit).toBe(false);
    expect("citationUrl" in model.textMatches[3].hit).toBe(false);
    expect("relatedCount" in model.textMatches[3].hit).toBe(false);
    expect("hasArtifact" in model.textMatches[3].hit).toBe(false);
    // The rail is two sections. Sort, Updated and Has artifact are cut with
    // the reads that could not serve them.
    expect(model.rail.map((section) => section.key)).toEqual(["type", "source"]);
    const type = model.rail.find((section) => section.key === "type");
    // This page is 20 of 43, so the tally is a page tally and carries no
    // counts at all rather than passing the page's own off as the total.
    expect(type?.options.every((option) => option.count === null)).toBe(true);
    expect(type?.note).toBeNull();
    // No prose under the pages: the numbered controls say what they are.
    expect(model.paging).toMatchObject({ kind: "pages", prevHref: null, note: "" });
    if (model.paging.kind !== "pages") throw new Error("pages");
    expect(model.paging.pages.map((p) => p.number)).toEqual([1, 2, 3]);
    expect(model.paging.pages[0].current).toBe(true);
    expect(model.paging.nextHref).toContain("offset=20");
    expect(model.unappliedNote).toBeNull();
    const markup = renderToStaticMarkup(ConsoleExplore(pageProps("workspace-mixed", {
      deployment: "workspace-mixed", version: model.filters.parsed.text ? (await baseFor("workspace-mixed")).version : null, types: [], params: parseExploreParams({ q: "ingest" }),
      chips: null, onMoreChips: null, results: model, listing: null, pending: null, focus: 1, context: contextFor("workspace-mixed", "v"),
    })));
    expect(markup).toContain('data-testid="explore-rail"');
    // Each rail option is a real button, so Space and Enter both work on it.
    // They were anchors carrying `role="checkbox"`: a screen reader announced
    // a checkbox, and Space -- which is what a checkbox takes -- scrolled the
    // page instead of filtering, because an anchor's keyboard is Enter alone.
    expect(markup).toContain('class="okg-console__rail-option"');
    expect(markup).toContain('aria-pressed=');
    expect(markup).not.toContain('role="checkbox"');
    expect(markup).not.toContain('aria-checked=');
    expect(markup).toContain('data-testid="explore-about"');
    expect(markup).toContain('data-testid="explore-pages"');
    expect(markup).toContain('<mark class="okg-console__mark">ingest</mark>');
    expect(markup).toContain('data-focused="true"');
    // No per-row "Copy id" and no record id on the row: both are cut, and
    // the record page one click away carries them.
    expect(markup).not.toContain("Copy id");
  });

  it("pages from the URL: page 2 is current and page 3 is last", async () => {
    const second = await searchFor("workspace-mixed", { q: "ingest", offset: "20" });
    if (second.model.paging.kind !== "pages") throw new Error("pages");
    expect(second.model.paging.pages.find((p) => p.current)?.number).toBe(2);
    expect(second.model.paging.prevHref).not.toContain("offset=");
    expect(second.model.paging.nextHref).toContain("offset=40");
  });

  it("filters by a checked type, with the whole result counted when the page is all of it", async () => {
    const typed = await searchFor("workspace-mixed", { q: "ingest", type: "issue" });
    expect(typed.page.totalCount).toBe(14);
    // 14 of 14, uncapped: the page IS the result, so the tally is a total and
    // is the one case where a count is shown.
    expect(typed.model.rail.find((s) => s.key === "type")?.options).toEqual([expect.objectContaining({ value: "issue", selected: true, count: 14 })]);
    expect(typed.model.rail.find((s) => s.key === "type")?.note).toBeNull();
    expect(typed.model.paging.kind).toBe("none");
    expect(typed.model.cli).toContain("--subtype 'issue'");
  });

  it("leaves a refused sort or since out of the read and names it on the page", async () => {
    // The whole point of the cut: the server answers sort= and since= with
    // 422 filter_has_no_stored_basis, so the request must not carry them --
    // and a reader whose bookmark did must be told, not quietly re-ranked.
    const params = parseExploreParams({ q: "ingest", sort: "newest", since: "2026-01-01", type: "issue" });
    expect(params.ignored.map((entry) => [entry.name, entry.value]))
      .toEqual([["sort", "newest"], ["since", "2026-01-01"]]);
    const request = searchRequest("v", effectiveFilters(params), 0, SEARCH_PAGE_SIZE);
    expect("sort" in request).toBe(false);
    expect("since" in request).toBe(false);
    expect(request).toMatchObject({ version: "v", q: "ingest", type: "issue", source: null });
    const line = ignoredSentence(params.ignored, true);
    expect(line).toContain("sort=newest and since=2026-01-01");
    expect(line).toContain("the published graph stores no per-record date");
    // With results on the page, the line also says what order they ARE in.
    expect(line).toContain("The results below are ranked by relevance over every date.");
    expect(ignoredSentence(parseExploreParams({ q: "ingest" }).ignored, true)).toBeNull();
  });

  it("does not promise results below the line when there are none below it", () => {
    // #console/explore?sort=newest with no query is the landing page: the
    // chips and nothing else. "The results below are ranked by relevance"
    // names results the page does not have.
    const params = parseExploreParams({ sort: "newest" });
    const line = ignoredSentence(params.ignored, false);
    expect(line).toContain("sort=newest");
    expect(line).toContain("the published graph stores no per-record date");
    expect(line).not.toContain("The results below");
  });

  it("says in one line when the search profile left out types this version lacks", async () => {
    // workspace-mixed carries okg-workspace's case (issue #2545): the default
    // profile names four types this version does not hold.
    const { model, page } = await searchFor("workspace-mixed", { q: "ingest" });
    expect(page.profileScope).toMatchObject({ profile: "analyst", skipped: ["chat_message", "chat_session", "conference", "venue"] });
    expect(model.skippedTypes?.line).toBe(
      `Searched ${page.profileScope?.searched?.length} types (the analyst profile's types and their subtypes); 4 types the profile names are not in this version.`,
    );
    expect(model.skippedTypes?.names).toEqual(["chat_message", "chat_session", "conference", "venue"]);
    const html = renderToStaticMarkup(<Results model={model} query="ingest" focus={null} />);
    expect(html).toContain('data-testid="explore-skipped-types"');
    // The names are behind the expand, not inline in the line.
    expect(html).toContain("<summary");
    expect(html).toContain("Not in this version: chat_message, chat_session, conference, venue.");
  });

  it("says the searched count includes subtypes and gets one and many right", () => {
    expect(skippedTypesLine({ profile: "analyst", searched: ["page", "page_child"], skipped: ["paper"] })).toBe(
      "Searched 2 types (the analyst profile's types and their subtypes); 1 type the profile names is not in this version.",
    );
    expect(skippedTypesLine({ profile: null, searched: ["page"], skipped: ["paper", "venue"] })).toBe(
      "Searched 1 type (the search profile's types and their subtypes); 2 types the profile names are not in this version.",
    );
    expect(skippedTypesLine({ profile: "analyst", searched: ["page"], skipped: [] })).toBeNull();
  });

  it("copies the left-out line with the results, on a page with results and on one with none", async () => {
    for (const q of ["ingest", "zzz-no-such-record"]) {
      const base = await searchFor("workspace-mixed", { q });
      expect(base.model.skippedTypes).not.toBeNull();
      // The second query is the no-match page, whose copy takes the other branch.
      expect(base.model.noMatchSentence === null).toBe(q === "ingest");
      const text = exploreText(pageProps("workspace-mixed", {
        deployment: "workspace-mixed", version: base.version, types: base.types, params: base.params,
        chips: null, onMoreChips: null, results: base.model, listing: null, pending: null, focus: null,
        context: contextFor("workspace-mixed", base.version),
      }));
      expect(text).toContain(base.model.skippedTypes!.line);
      expect(text).toContain("Not in this version: chat_message, chat_session, conference, venue.");
    }
  });

  it("names nothing when a type filter is set or nothing was left out", async () => {
    const filtered = await searchFor("workspace-mixed", { q: "ingest", type: "issue" });
    expect(filtered.page.profileScope).toEqual({ profile: null, searched: null, skipped: [] });
    expect(filtered.model.skippedTypes).toBeNull();
    const archiv3 = await searchFor("archiv3-small", { q: "CMSSW_14_0_0" });
    expect(archiv3.model.skippedTypes).toBeNull();
  });

  it("refuses a malformed skipped-types answer instead of dropping it", () => {
    const envelope = (extra: Record<string, unknown>) => ({
      generation_id: "v",
      payload: {
        query: "q", type: null, filters: { type: null, source: null }, results: [],
        exact_match: false, exact_match_in_text: false, total_count: 0, truncated: false,
        next_offset: null, rank_capped: false, zero_hit_reason: null, ...extra,
      },
    });
    expect(parseExploreSearch(envelope({})).payload.profileScope).toBeNull();
    expect(() => parseExploreSearch(envelope({ skipped_types: "paper" }))).toThrow(SearchResponseError);
    expect(() => parseExploreSearch(envelope({ skipped_types: ["paper"], searched_types: null }))).toThrow(SearchResponseError);
  });

  it("says by name when the read did not honour a filter it was asked for", async () => {
    const { page, link } = await searchFor("workspace-mixed", { q: "ingest", source: "source-07-git" });
    const params = parseExploreParams({ q: "ingest", source: "source-07-git" });
    // The read echoes what it honoured under `filters`. An echo that drops the
    // source is the read saying it ignored it, and the page must say so too.
    const silent: ExploreSearchPage = { ...page, applied: { type: null, source: null } };
    const model = buildResults(silent, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(model.unappliedNote).toBe("This read did not apply source source-07-git: the results are as the read returned them.");
    expect(model.aboutLine).toBe("11 results");
    const partial: ExploreSearchPage = { ...silent, totalCount: 40, truncated: true, nextOffset: 20 };
    const floor = buildResults(partial, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(floor.rail.find((s) => s.key === "type")?.options.every((o) => o.count === null)).toBe(true);
    // The apology under the section is cut: a name with no number beside it
    // already says the read carried no count (ui/cuts.md).
    expect(floor.rail.find((s) => s.key === "type")?.note).toBeNull();
  });

  it("lists label matches after the id match and before the text matches, on the page, the keys and the copy", async () => {
    // #2761: `match: "label"` failed the whole page, and once parsed it fell
    // out of both runs. The server orders id, then label, then ranked text.
    const { link, page } = await searchFor("workspace-mixed", { q: "ingest" });
    const mixed: ExploreSearchPage = {
      ...page,
      query: "handbook",
      results: [
        { ...page.results[0], nodeId: "document:handbook", label: "Operator guide", match: "typed_record_id", excerpt: null },
        { ...page.results[1], nodeId: "issue:label-hit", label: "handbook", match: "label", excerpt: "the label hit's own words" },
        { ...page.results[2], nodeId: "issue:text-hit", label: "Review notes", match: "text", excerpt: "read the handbook first" },
      ],
      exactMatch: true, totalCount: 3, nextOffset: null, truncated: false, rankCapped: false,
    };
    const params = parseExploreParams({ q: "handbook" });
    const model = buildResults(mixed, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(model.idMatches.map((row) => row.hit.nodeId)).toEqual(["document:handbook"]);
    expect(model.labelMatches.map((row) => row.hit.nodeId)).toEqual(["issue:label-hit"]);
    expect(model.textMatches.map((row) => row.hit.nodeId)).toEqual(["issue:text-hit"]);
    // The arrow keys walk the rows in the order they are drawn.
    expect([...model.idMatches, ...model.labelMatches, ...model.textMatches].map((row) => row.index)).toEqual([0, 1, 2]);
    const props = pageProps("workspace-mixed", {
      deployment: "workspace-mixed", version: "v", types: [], params,
      chips: null, onMoreChips: null, results: model, listing: null, pending: null, focus: null, context: contextFor("workspace-mixed", "v"),
    });
    const markup = renderToStaticMarkup(ConsoleExplore(props));
    // The query word is marked wherever it appears, so the probes avoid it.
    const order = ["ID match", "Operator guide", "Label match", "the label hit&#x27;s own words", "Text match", "Review notes"]
      .map((text) => markup.indexOf(text));
    expect(order.every((at) => at >= 0)).toBe(true);
    expect([...order].sort((x, y) => x - y)).toEqual(order);
    const text = exploreText(props);
    const labelAt = text.indexOf("Label match");
    expect(labelAt).toBeGreaterThan(text.indexOf("ID match"));
    expect(text.indexOf("issue:label-hit")).toBeGreaterThan(labelAt);
    expect(text.indexOf("Text match")).toBeGreaterThan(text.indexOf("issue:label-hit"));
    expect(text.indexOf("issue:text-hit")).toBeGreaterThan(text.indexOf("Text match"));
    // Label rows with no id row above them still head the text run, in the
    // copy as on the page.
    const noId = buildResults({ ...mixed, results: mixed.results.slice(1), totalCount: 2 }, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(noId.idMatches).toEqual([]);
    const noIdProps = { ...props, results: noId };
    const noIdText = exploreText(noIdProps);
    expect(noIdText).not.toContain("ID match");
    expect(noIdText.indexOf("Label match")).toBeGreaterThanOrEqual(0);
    expect(noIdText.indexOf("Text match")).toBeGreaterThan(noIdText.indexOf("issue:label-hit"));
    expect(noIdText.indexOf("issue:text-hit")).toBeGreaterThan(noIdText.indexOf("Text match"));
    expect(renderToStaticMarkup(ConsoleExplore(noIdProps))).toContain("Text match");
    // A page of text matches alone carries no heading on the page or in the copy.
    const plain = buildResults({ ...mixed, results: [mixed.results[2]], exactMatch: false, totalCount: 1 }, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    const plainProps = { ...props, results: plain };
    expect(renderToStaticMarkup(ConsoleExplore(plainProps))).not.toContain("Text match");
    expect(exploreText(plainProps)).not.toContain("Text match");
  });

  it("lists the exact id match first, and offers Load 20 more when the read gives no total", async () => {
    const { params, link, page } = await searchFor("workspace-mixed", { q: "ingest" });
    const withId: ExploreSearchPage = {
      ...page,
      query: "document:handbook",
      results: [
        { ...page.results[0], nodeId: "document:other", label: "Another one", match: "text" },
        { ...page.results[1], nodeId: "document:handbook", label: "Operator handbook", match: "record_id", excerpt: null },
      ],
      exactMatch: true, totalCount: null, pagedCount: null, nextOffset: 20, rankCapped: false,
    };
    const model = buildResults(withId, parseExploreParams({ q: "document:handbook" }), link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(model.idMatches.map((row) => row.hit.nodeId)).toEqual(["document:handbook"]);
    expect(model.textMatches.map((row) => row.hit.nodeId)).toEqual(["document:other"]);
    // No read time on the results line, and no prose under the button: both
    // are cut. The control stays.
    expect(model.aboutLine).toBe("Results");
    expect(model.paging).toMatchObject({ kind: "more", note: "" });
    expect(aboutLine(500, true)).toBe("More than 500 results");
    expect(aboutLine(1, false)).toBe("1 result");
    void params;
    const markup = renderToStaticMarkup(ConsoleExplore(pageProps("workspace-mixed", {
      deployment: "workspace-mixed", version: "v", types: [], params: parseExploreParams({ q: "document:handbook" }),
      chips: null, onMoreChips: null, results: model, listing: null, pending: null, focus: null, context: contextFor("workspace-mixed", "v"),
    })));
    // The id match is above the text match. Both rows are titled by their
    // served label -- these fixtures record no source path -- and the record
    // id is no longer printed on a row, so the order is read off the titles.
    expect(markup.indexOf("ID match")).toBeLessThan(markup.indexOf("Operator handbook"));
    expect(markup.indexOf("Operator handbook")).toBeLessThan(markup.indexOf("Another one"));
    expect(markup).not.toContain("document:other");
    expect(markup).toContain("Load 20 more");
  });

  it("marks the match in the label and the snippet, case-insensitively, falling back to words", () => {
    expect(highlight("Operator handbook", "HANDBOOK")).toEqual([
      { text: "Operator ", match: false }, { text: "handbook", match: true },
    ]);
    expect(highlight("How to run the deployment day to day.", "run day")).toEqual([
      { text: "How to ", match: false }, { text: "run", match: true }, { text: " the deployment ", match: false },
      { text: "day", match: true }, { text: " to ", match: false }, { text: "day", match: true }, { text: ".", match: false },
    ]);
    expect(highlight("nothing here", "zzz")).toEqual([{ text: "nothing here", match: false }]);
  });

  it("states the range honestly: a rank-capped total is 'more than'", () => {
    expect(showingLine(0, 20, 67, false)).toBe("Showing 1–20 of 67");
    expect(showingLine(0, 20, 500, true)).toBe("Showing 1–20 of more than 500");
    expect(showingLine(0, 0, 0, false)).toBe("Showing 0");
  });

  it("walks the rows with the arrow keys, stops at both ends, and Escape clears the focus", () => {
    expect(moveFocus(null, 3, "ArrowDown")).toBe(0);
    expect(moveFocus(0, 3, "ArrowDown")).toBe(1);
    expect(moveFocus(2, 3, "ArrowDown")).toBe(2);
    expect(moveFocus(2, 3, "ArrowUp")).toBe(1);
    expect(moveFocus(0, 3, "ArrowUp")).toBeNull();
    expect(moveFocus(null, 3, "End")).toBe(2);
    expect(moveFocus(2, 3, "Home")).toBe(0);
    expect(moveFocus(2, 3, "Escape")).toBeNull();
    expect(moveFocus(1, 0, "ArrowDown")).toBeNull();
    expect(moveFocus(1, 3, "x")).toBe(1);
  });

  it("says by name when nothing matches, and offers the two cheapest ways to broaden", async () => {
    const empty = await searchFor("workspace-mixed", { q: "zzz-no-such-record" });
    expect(empty.model.noMatchSentence).toBe("No record matches “zzz-no-such-record” in version 31caba7be14a.");
    expect(empty.model.broaden.map((a) => a.label)).toEqual(["browsing the types"]);
    const { page, link } = empty;
    // The scope and the ways out no longer mention a date window: "any time"
    // was a way to broaden a filter this read never applied.
    const filtered = buildResults(page, parseExploreParams({ q: '"zzz" more', type: "paper", since: "month" }), link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(filtered.noMatchSentence).toBe("No record matches “zzz-no-such-record” in paper in version v.");
    expect(filtered.broaden.map((a) => a.label)).toEqual(["all types", "the words without the quotes"]);
    expect(filtered.broaden[0].href).not.toContain("type=");
    expect(filtered.broaden.some((a) => a.label === "any time")).toBe(false);
    // The read's own diagnosis is carried when it makes one: "these words did
    // not match" is false for a type stored without searchable text.
    const diagnosed = buildResults({ ...page, zeroHitReason: "not_indexed" }, parseExploreParams({ q: "zzz-no-such-record", type: "paper" }), link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(diagnosed.noMatchSentence).toContain("stored without searchable text");
    const markup = renderToStaticMarkup(ConsoleExplore(pageProps("workspace-mixed", {
      deployment: "workspace-mixed", version: "v", types: [], params: parseExploreParams({ q: "zzz-no-such-record" }),
      chips: null, onMoreChips: null, results: empty.model, listing: null, pending: null, focus: null, context: contextFor("workspace-mixed", "v"),
    })));
    expect(markup).toContain('data-testid="explore-empty-results"');
    expect(markup).not.toContain('data-testid="explore-cli"');
    expect(markup).not.toContain('data-section="type"');
  });

  it("counts pages from the rows later pages hold, not from a total that counts the exact rows too", async () => {
    // #2789: 21 matches, one of them a label match. The server lists the
    // label row on page 1 only and pages the other 20, so page 1 holds all
    // 21. A page count taken from total_count offered a page 2 that was empty.
    const { link, page } = await searchFor("workspace-mixed", { q: "ingest" });
    const text = Array.from({ length: 20 }, (_, n) => ({
      ...page.results[n % page.results.length], nodeId: `issue:text-${n}`, label: `Text ${n}`, match: "text" as const,
    }));
    const one: ExploreSearchPage = {
      ...page,
      query: "handbook",
      results: [{ ...page.results[0], nodeId: "issue:label-hit", label: "handbook", match: "label", excerpt: null }, ...text],
      exactMatch: true, totalCount: 21, pagedCount: 20, nextOffset: null, truncated: false, rankCapped: false,
    };
    const params = parseExploreParams({ q: "handbook" });
    const first = buildResults(one, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(first.labelMatches.length + first.textMatches.length).toBe(21);
    expect(first.aboutLine).toBe("21 results");
    expect(first.paging).toEqual({ kind: "none" });
    // One more paged row is a real page 2, and its link is offered.
    const two = buildResults({ ...one, totalCount: 22, pagedCount: 21, nextOffset: 20, truncated: true }, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    if (two.paging.kind !== "pages") throw new Error("pages");
    expect(two.paging.pages.map((p) => p.number)).toEqual([1, 2]);
    expect(two.paging.nextHref).toContain("offset=20");
    // A read that does not serve the paged count gets the next link its
    // next_offset vouches for, never a count taken from the total.
    const unsaid = buildResults({ ...one, pagedCount: null }, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(unsaid.paging).toEqual({ kind: "none" });
  });

  it("says an empty page past the last result is past it, never that no record matches", async () => {
    const { link, page, types } = await searchFor("workspace-mixed", { q: "ingest" });
    // The zero-hit chips as the route builds them: the version's types.
    const emptyChips = buildChips(types, NONE, link, null);
    expect(emptyChips.chips.length).toBeGreaterThan(0);
    const params = parseExploreParams({ q: "handbook", offset: "20" });
    const beyond: ExploreSearchPage = {
      ...page, query: "handbook", results: [], exactMatch: false, totalCount: 21, pagedCount: 20,
      nextOffset: null, truncated: false, rankCapped: false,
    };
    const model = buildResults(beyond, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(model.pastEnd).toBe(true);
    expect(model.noMatchSentence).toBe("The results for “handbook” in version v end before this page.");
    expect(model.broaden.map((a) => a.label)).toEqual(["the first page"]);
    expect(model.broaden[0].href).not.toContain("offset=");
    const props = pageProps("workspace-mixed", {
      deployment: "workspace-mixed", version: "v", types: [], params,
      chips: null, onMoreChips: null, results: model, listing: null, pending: null, focus: null, context: contextFor("workspace-mixed", "v"),
      emptyChips,
    });
    const markup = renderToStaticMarkup(ConsoleExplore(props));
    expect(markup).not.toContain("No record matches");
    expect(markup).toContain("the first page");
    // The chips are the way out of a search that matched nothing; this one
    // matched, so they are not offered.
    expect(markup).not.toContain('data-testid="explore-chips"');
    expect(exploreText(props)).not.toContain("No record matches");
    // The failure case: when the read counts no match at all, an empty later
    // page still says so, and offers the chips.
    const none = buildResults({ ...beyond, totalCount: 0, pagedCount: 0 }, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(none.pastEnd).toBe(false);
    expect(none.noMatchSentence).toBe("No record matches “handbook” in version v.");
    expect(renderToStaticMarkup(ConsoleExplore({ ...props, results: none }))).toContain('data-testid="explore-chips"');
    // A read that diagnosed a zero-hit keeps its diagnosis even when its
    // total is unknown: the reason sentence is never replaced.
    const diagnosed = buildResults({ ...beyond, totalCount: null, pagedCount: null, zeroHitReason: "not_indexed" }, params, link, "workspace-mixed", "v", SEARCH_PAGE_SIZE);
    expect(diagnosed.pastEnd).toBe(false);
    expect(diagnosed.noMatchSentence).toContain("stored without searchable text");
  });
});

describe("the record page", () => {
  it("says hidden by policy for the masked field and names no policy, never the token, and keeps long text off Fields", async () => {
    // Explicitly on Fields: the record page opens on Content now.
    const { model, record } = await recordFor("workspace-mixed", "document:handbook", parseExploreParams({ tab: "fields" }));
    const hidden = record.fields.find((field) => field.hidden);
    expect(hidden).toMatchObject({ key: "author_email", value: null });
    // No field row carries a policy name on any read. Cut, not faked.
    expect(hidden).not.toHaveProperty("hiddenPolicy");
    expect(model.fieldCount).toBe(6);
    expect(model.hiddenCount).toBe(1);
    expect(model.fields.map((field) => field.key)).not.toContain("body");
    expect(model.content.extractedText).toContain("# Operator handbook");
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("workspace-mixed", model.version) }));
    expect(markup).toContain('<span class="okg-console__hidden">hidden</span>');
    expect(markup).toContain("by policy");
    expect(markup).not.toContain("nomos.contact_details");
    expect(markup).not.toContain("by a policy the read did not name");
    // The Fields panel: from its body to the start of the next panel. The
    // old slice ran from Fields to Related, and Related comes FIRST in the
    // strip now, so it ran backwards and matched nothing.
    const panel = (rendered: string, testid: string) => {
      const start = rendered.indexOf(testid);
      const end = rendered.indexOf("okg-console__tabpanel", start);
      return rendered.slice(start, end === -1 ? undefined : end);
    };
    const fieldsTab = panel(markup, "explore-record-fields");
    expect(fieldsTab).not.toContain("[nomos:masked]");
    expect(markup).toContain("Fields (6)");
    expect(markup).toContain("Related (2)");
    // Raw shows the same key masked, not absent (S15): the stored row as stored.
    expect(markup).toContain("&quot;[nomos:masked]&quot;");
    expect(model.tabs).toEqual(["content", "related", "fields", "provenance", "raw"]);
  });

  it("names a field value the read did not carry, rather than drawing a blank cell", async () => {
    // The record read sends a string for every field it does not hide, so
    // this is the read listing a key and dropping its value. A blank cell
    // reads as a value that is empty, which is a different fact.
    const { model } = await recordFor("workspace-mixed", "document:handbook", parseExploreParams({ tab: "fields" }));
    const shown = model.fields.find((field) => !field.hidden && field.value !== null);
    if (!shown) throw new Error("no shown field to blank");
    const dropped = {
      ...model,
      fields: model.fields.map((field) => (
        field.key === shown.key ? { ...field, value: null } : field
      )),
    };
    const context = contextFor("workspace-mixed", model.version);
    // The Fields tab only: this record prints the same value elsewhere on
    // the page, and the claim is about the cell, not about the page.
    const fieldsTab = (rendered: string) => {
      const start = rendered.indexOf("explore-record-fields");
      const end = rendered.indexOf("okg-console__tabpanel", start);
      return rendered.slice(start, end === -1 ? undefined : end);
    };
    const before = fieldsTab(renderToStaticMarkup(ConsoleRecordPage({ model, context })));
    const after = fieldsTab(renderToStaticMarkup(ConsoleRecordPage({ model: dropped, context })));

    expect(after).toContain("not recorded by the source");
    expect(before).not.toContain("not recorded by the source");
    // One cell stopped printing the value, and it is the one that was blanked.
    const count = (text: string) => text.split(shown.value as string).length - 1;
    expect(count(after)).toBe(count(before) - 1);
  });

  it("offers Open source from citation_url and from nothing else", async () => {
    const { model } = await recordFor("workspace-mixed", "document:handbook");
    expect(model.openInSource).toBe("https://github.com/example-org/example-repo/blob/a41c9e2c0b7d4e1f/docs/operate/handbook.md");
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("workspace-mixed", model.version) }));
    expect(markup).toContain('data-testid="explore-open-in-source"');
    expect(markup).toContain('rel="noopener noreferrer"');
    const commit = await recordFor("scale-200", "commit:9a1c02e");
    expect(commit.model.openInSource).toBe("https://github.com/example-org/example-repo/commit/9a1c02e41b7d3f0a");
    const none = await recordFor("workspace-mixed", "person:riley");
    expect(none.model.openInSource).toBeNull();
    // A field that merely LOOKS like a URL is not where this record can be
    // read: a schema URL, a licence link or a repository home would have sent
    // the reader to a page that is not this record.
    expect(openInSourceUrl([{ key: "homepage", value: "https://a.invalid/x", hidden: false, echoesTitle: false }])).toBeNull();
    expect(openInSourceUrl([{ key: "citation_url", value: "not a url", hidden: false, echoesTitle: false }])).toBeNull();
  });
});

describe("the artifact read and the Content tab", () => {
  it("renders an existing artifact: toolbar facts, the viewer for its media type, the text beside it", async () => {
    const { model, artifact } = await recordFor("workspace-mixed", "paper:okg-2026", parseExploreParams({ tab: "content" }));
    expect(artifact.exists).toBe(true);
    expect(model.content.toolbar).toMatchObject({ format: "pdf", byteCount: 2699, fileName: "okg-2026.pdf", url: "/design/fixtures/artifacts/okg-2026.pdf" });
    expect(model.content.toolbar?.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(model.content.viewer).toEqual({ kind: "pdf", language: null });
    expect(model.content.absenceSentence).toBeNull();
    expect(model.content.chunks.map((chunk) => chunk.ordinal)).toEqual([0, 1, 2]);
    expect(model.tab).toBe("content");
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("workspace-mixed", model.version) }));
    expect(markup).toContain('data-testid="explore-content-toolbar"');
    expect(markup).toContain('data-viewer="pdf"');
    expect(markup).toContain('data-testid="explore-content-text"');
    expect(markup).toContain("Content (3 chunks)");
    expect(markup).toContain('href="/design/fixtures/artifacts/okg-2026.pdf" download="okg-2026.pdf"');
  });

  it("says which condition is unmet for a refused, a missing and an unretained artifact, then shows the text", async () => {
    const refused = await recordFor("workspace-mixed", "person:riley", parseExploreParams({ tab: "content" }));
    expect(refused.artifact).toMatchObject({ exists: false, reason: "policy_refused", policy: "nomos.contact_details" });
    expect(refused.model.content.absenceSentence).toBe(
      "The original file is withheld by policy nomos.contact_details: refuses the original file for person records.",
    );
    expect(refused.model.content.toolbar).toBeNull();
    expect(refused.model.tabs).toContain("content");
    const missing = await recordFor("archiv3-small", "document:release-plan", parseExploreParams({ tab: "content" }));
    expect(missing.artifact).toMatchObject({ exists: false, reason: "file_missing" });
    expect(missing.model.content.absenceSentence).toMatch(/^This record names retained bytes \(docs\/run3\/release-plan\.md\) that the store no longer holds: /);
    expect(missing.model.content.extractedText).toBe("Run 3 reprocessing uses CMSSW_14_0_0 onwards.");
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model: missing.model, context: contextFor("archiv3-small", missing.model.version) }));
    expect(markup).toContain('data-testid="explore-content-absent"');
    expect(markup).toContain('data-testid="explore-content-text"');
    expect(markup).not.toContain('data-testid="explore-content-toolbar"');
    const unretained = await recordFor("archiv3-small", "cmssw_release:CMSSW_14_0_0");
    expect(unretained.artifact).toMatchObject({ exists: false, reason: "not_retained" });
    // A record with long text still has a Content tab; one with none does not.
    expect(unretained.model.tabs).toContain("content");
    const bare = { ...unretained.model.content, extractedText: null, chunks: [] };
    expect(hasContent(bare)).toBe(false);
    expect(hasContent({ ...bare, artifact: refused.artifact })).toBe(true);
  });

  it("names the read that is missing when the backend serves no artifact read", async () => {
    const { detail, raw, version, versions, link } = await recordFor("workspace-mixed", "document:handbook");
    if (!detail.payload.found) throw new Error("not found");
    const model = buildRecordPage(detail.payload.record, { artifact: null, raw }, version as string, versions.versions, NONE, link, "workspace-mixed", NOW_MS, SEARCH_PAGE_SIZE);
    expect(model.content.absenceSentence).toBe("The original file cannot be shown: this page made no artifact read.");
    expect(model.content.toolbar).toBeNull();
  });

  it("refuses a malformed artifact read rather than rendering around it", () => {
    expect(() => parseArtifactRead({ generation_id: "v", payload: { exists: false, reason: "gone", detail: "x" } })).toThrow(SearchResponseError);
    expect(() => parseArtifactRead({ generation_id: "v", payload: { exists: true, media_type: "text/plain" } })).toThrow(SearchResponseError);
    expect(() => parseRawRead({ generation_id: "v", payload: { row: {}, edges: [], edge_total: 0, edge_cap: 200, payload: null, refusal: null } })).toThrow(SearchResponseError);
    expect(() => parseRawRead({ generation_id: "v", payload: { row: {}, edges: [], edge_total: 0, edge_cap: 200, payload: { sha256: "a", byte_count: INLINE_PAYLOAD_LIMIT + 1, inline: "{}", url: "/x" }, refusal: null } })).toThrow(SearchResponseError);
  });

  it("routes media types to viewers", () => {
    expect(viewerFor({ mediaType: "application/pdf", format: "pdf", path: "a.pdf" })).toEqual({ kind: "pdf", language: null });
    expect(viewerFor({ mediaType: "text/markdown", format: "markdown", path: "a.md" })).toEqual({ kind: "markdown", language: null });
    expect(viewerFor({ mediaType: "text/html", format: "html", path: "a.html" })).toEqual({ kind: "html", language: null });
    expect(viewerFor({ mediaType: "image/png", format: "image", path: "a.png" })).toEqual({ kind: "image", language: null });
    expect(viewerFor({ mediaType: "text/x-rust", format: "code", path: "src/main.rs" })).toEqual({ kind: "code", language: "rust" });
    expect(viewerFor({ mediaType: "text/plain", format: "code", path: "x.py" })).toEqual({ kind: "code", language: "python" });
    expect(viewerFor({ mediaType: "text/plain", format: "text", path: "notes.txt" })).toEqual({ kind: "code", language: null });
    expect(viewerFor({ mediaType: "application/json", format: "json", path: "x.json" })).toEqual({ kind: "code", language: "json" });
    expect(viewerFor({ mediaType: "application/vnd.ms-excel", format: "xlsx", path: "x.xlsx" })).toEqual({ kind: "none", language: null });
  });

  it("orders chunks by ordinal and says what a chunk is part of", async () => {
    const paper = await recordFor("workspace-mixed", "paper:okg-2026");
    const shuffled = { ...paper.detail, payload: { ...paper.detail.payload } };
    void shuffled;
    const envelope = await paper.api.get<{ payload: { chunks: unknown[] } }>("/viz/api/console/records/detail", { version: paper.version as string, id: "paper:okg-2026" });
    const reversed = { ...envelope, payload: { ...envelope.payload, chunks: [...envelope.payload.chunks].reverse() } };
    const parsed = parseExploreRecord(reversed);
    if (!parsed.payload.found) throw new Error("not found");
    expect(parsed.payload.record.chunks?.map((chunk) => chunk.ordinal)).toEqual([0, 1, 2]);
    expect(parsed.payload.record.chunks?.[2].headingPath).toBe("2 Retained payloads and artifacts");
    const chunk = await recordFor("workspace-mixed", "document_chunk:okg-2026#0", parseExploreParams({ tab: "content" }));
    expect(chunk.model.content.partOf).toMatchObject({ id: "paper:okg-2026", label: "Operational knowledge graphs" });
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model: chunk.model, context: contextFor("workspace-mixed", chunk.model.version) }));
    expect(markup.indexOf("part of ")).toBeLessThan(markup.indexOf("explore-content-text"));
    expect(extractedText([{ key: "a", title: "A", value: "one", hidden: false }, { key: "b", title: "B", value: null, hidden: true }]))
      .toBe("A\none\n\nB\n[hidden by policy]");
  });
});

describe("Related", () => {
  it("is one table with relationship chips carrying the read's counts, a direction, and pages of 50", async () => {
    const { model } = await recordFor("scale-200", "commit:9a1c02e", parseExploreParams({ tab: "related" }));
    const related = model.related;
    expect(related.chips.map((chip) => [chip.relationship, chip.count, chip.countIsFloor])).toEqual([
      ["authored_by", 84, false], ["touches", 84, false], ["part_of", 83, false], ["reverts", 83, false], ["reviews", 83, false], ["supersedes", 83, false],
    ]);
    expect(related.rows.length).toBe(RELATED_PAGE);
    expect(related.countLine).toBe("1–50 of 200 rows · 500 related: 400 outgoing, 100 incoming");
    // The served total first, then every cap flag the read set, in its
    // own words. Both are read values; neither is derived here.
    expect(related.capSentences).toEqual([
      "100 of 400 outgoing shown",
      "This read lists at most 100 outgoing relationships; this record has more.",
    ]);
    expect(related.nextHref).toContain("roffset=50");
    expect(related.prevHref).toBeNull();
    // The tab counts the RECORDS it lists, not the server's edge totals.
    // 500 edges reach 200 neighbours here, and the read served 100 of 400
    // outgoing, so the number is a floor and says so.
    expect(model.relatedCount).toBe(200);
    expect(model.relatedCountIsFloor).toBe(true);
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("scale-200", model.version) }));
    expect(markup).toContain("Related (200+)");
    expect(markup).not.toContain("Related (500)");
    expect(markup).toContain("100 of 400 outgoing shown");
    expect(markup).toContain('data-testid="explore-related-table"');
  });

  it("filters by one chip and by direction, and pages from the URL", async () => {
    const second = await recordFor("scale-200", "commit:9a1c02e", parseExploreParams({ tab: "related", roffset: "150" }));
    expect(second.model.related.rows.length).toBe(50);
    expect(second.model.related.countLine).toBe("151–200 of 200 rows · 500 related: 400 outgoing, 100 incoming");
    expect(second.model.related.nextHref).toBeNull();
    expect(second.model.related.prevHref).toContain("roffset=100");
    const chip = await recordFor("scale-200", "commit:9a1c02e", parseExploreParams({ tab: "related", rel: "touches", dir: "incoming" }));
    const rows = chip.model.related.rows;
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.relationship === "touches" && row.direction === "incoming")).toBe(true);
    expect(chip.model.related.chips.find((c) => c.relationship === "touches")?.selected).toBe(true);
    expect(chip.model.related.chips.find((c) => c.relationship === "touches")?.href).not.toContain("rel=");
    expect(chip.model.related.countLine).toMatch(/^1–\d+ of \d+ incoming `touches` rows/);
    const none = await recordFor("scale-200", "commit:9a1c02e", parseExploreParams({ tab: "related", rel: "cites" }));
    expect(none.model.related.emptySentence).toBe("No cites relationship is visible on this record in this version.");
    // The Related filters travel with the Related tab only.
    expect(chip.model.tabHref("raw")).not.toContain("rel=");
    expect(chip.model.tabHref("related")).toContain("rel=touches");
  });

  it("marks the chip counts as a floor when the read carries no tally", async () => {
    const base = await recordFor("scale-200", "commit:9a1c02e", parseExploreParams({ tab: "related" }));
    if (!base.detail.payload.found) throw new Error("not found");
    const without: ExploreRecord = { ...base.detail.payload.record, byRelationship: null, outgoingTotal: null, incomingTotal: null };
    const model = buildRecordPage(without, { artifact: base.artifact, raw: base.raw }, base.version as string, base.versions.versions, parseExploreParams({ tab: "related" }), base.link, "scale-200", NOW_MS, SEARCH_PAGE_SIZE);
    expect(model.related.chips.every((chip) => chip.countIsFloor)).toBe(true);
    expect(model.relatedCountIsFloor).toBe(true);
    // The cap is now stated from the read's own flags, by relatedCaps, and
    // capSentence says only what the served totals support.
    expect(model.related.capSentences.join(" ")).toContain("This read lists at most");
    expect(capSentence("outgoing", 3, 41)).toBe("3 of 41 outgoing shown");
    expect(capSentence("incoming", 3, 3)).toBeNull();
    expect(capSentence("incoming", 3, null)).toBeNull();
  });
});

describe("Provenance", () => {
  it("shows the source, its identity and what the policy withheld, and nothing the read cannot source", async () => {
    const { model } = await recordFor("workspace-mixed", "document:handbook", parseExploreParams({ tab: "provenance" }));
    expect(model.provenance.policyLine).toBe("hid 1 value of this record");
    expect(model.provenance).not.toHaveProperty("factLog");
    expect(model.provenance).not.toHaveProperty("lastWrite");
    expect(model.provenance).not.toHaveProperty("enrichers");
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("workspace-mixed", model.version) }));
    expect(markup).toContain('data-testid="explore-record-provenance"');
    expect(markup).toContain("hid 1 value of this record");
    // The three cut blocks, by every string that would show one.
    expect(markup).not.toContain("Fact log");
    expect(markup).not.toContain("explore-provenance-log");
    expect(markup).not.toContain("explore-provenance-attempt");
    expect(markup).not.toContain("explore-provenance-enrichers");
    expect(markup).not.toContain("Last written by");
    expect(markup).not.toContain("Enricher contributions");
    expect(markup).not.toContain('data-copy="fact log"');
  });

  it("does not parse a provenance key even when one is on the wire", async () => {
    const empty = await recordFor("archiv3-small", "cmssw_release:CMSSW_14_0_0", parseExploreParams({ tab: "provenance" }));
    expect(empty.record).not.toHaveProperty("factLog");
    const envelope = await empty.api.get<{ payload: Record<string, unknown> }>("/viz/api/console/records/detail", { version: empty.version as string, id: "cmssw_release:CMSSW_14_0_0" });
    // A malformed fact log used to fail the read. Nothing reads it now, so
    // the read succeeds and the page is the same page.
    const parsed = parseExploreRecord({ ...envelope, payload: { ...envelope.payload, provenance: { fact_log: "nope" } } });
    expect(parsed.payload.found).toBe(true);
  });
});

describe("Raw", () => {
  it("shows the stored row as a tree, the edge rows, and an inline payload up to 256 KB", async () => {
    const { model, raw } = await recordFor("workspace-mixed", "document:handbook", parseExploreParams({ tab: "raw" }));
    expect(raw.row).toMatchObject({ node_id: "document:handbook", author_email: "[nomos:masked]" });
    expect(raw.edges.length).toBe(2);
    expect(raw.payload?.inline).toContain("\"title\": \"Operator handbook\"");
    expect(raw.payload?.byteCount).toBeLessThanOrEqual(INLINE_PAYLOAD_LIMIT);
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("workspace-mixed", model.version) }));
    expect(markup).toContain('data-testid="explore-raw-row-tree"');
    expect(markup).toContain('data-testid="explore-raw-payload" data-payload="inline"');
    expect(markup).toContain('data-copy="stored row"');
    expect(markup).toContain('data-copy="edge rows"');
    expect(markup).toContain('data-copy="retained payload"');
  });

  it("shows the first 200 lines and offers the file for a payload past the limit", async () => {
    const { model, raw } = await recordFor("workspace-mixed", "session:sess-1", parseExploreParams({ tab: "raw" }));
    expect(raw.payload?.inline).toBeNull();
    expect(raw.payload?.byteCount).toBeGreaterThan(INLINE_PAYLOAD_LIMIT);
    expect(raw.payload?.head?.split("\n").length).toBe(200);
    expect(payloadText(raw.payload!)).toMatch(/\[first 200 of 12,608 lines; 357,975 bytes at \/design\/fixtures\/artifacts\/session-sess-1\.json\]$/);
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("workspace-mixed", model.version) }));
    expect(markup).toContain('data-payload="head"');
    expect(markup).toContain(">Open raw</a>");
    expect(markup).toContain("past the 262,144-byte inline limit");
  });

  it("names the refused payload in one sentence, with the row and edges still shown", async () => {
    const { model, raw } = await recordFor("workspace-mixed", "person:riley", parseExploreParams({ tab: "raw" }));
    expect(raw.payload).toBeNull();
    expect(refusalSentence(raw.refusal!)).toBe(
      "The retained payload is withheld by policy nomos.contact_details: refuses the retained payload for person records; the stored row and edge rows are unaffected.",
    );
    expect(withoutPolicyPrefix("policy a.b refuses it", "a.b")).toBe("refuses it");
    expect(withoutPolicyPrefix("something else", "a.b")).toBe("something else");
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("workspace-mixed", model.version) }));
    expect(markup).toContain("The retained payload is withheld by policy nomos.contact_details: refuses");
    expect(markup).toContain("&quot;email&quot;: </span>");
    expect(markup).toContain("&quot;[nomos:masked]&quot;");
    expect(markup).toContain('data-testid="explore-raw-edges"');
    const unretained = await recordFor("archiv3-small", "cmssw_release:CMSSW_14_0_0", parseExploreParams({ tab: "raw" }));
    expect(refusalSentence(unretained.raw.refusal!)).toMatch(/^No payload is kept for this record: cmssw_releases does not retain/);
  });

  it("says by name when the read carries no raw layers at all, and fails a read in place", async () => {
    const base = await recordFor("archiv3-small", "cmssw_release:CMSSW_14_0_0", parseExploreParams({ tab: "raw" }));
    if (!base.detail.payload.found) throw new Error("not found");
    const model = buildRecordPage(base.detail.payload.record, { artifact: base.artifact, raw: null }, base.version as string, base.versions.versions, parseExploreParams({ tab: "raw" }), base.link, "archiv3-small", NOW_MS, SEARCH_PAGE_SIZE);
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model, context: contextFor("archiv3-small", model.version) }));
    expect(markup).toContain("This read returned no raw layers");
    const failed = renderToStaticMarkup(ConsoleRecordPage({
      model, context: contextFor("archiv3-small", model.version),
      failures: { artifact: null, raw: new Error("the raw read answered 503") },
    }));
    expect(failed).toContain("the raw read answered 503");
  });

  it("prints the version read from and its publish time, and says when a newer one exists", async () => {
    const base = await baseFor("workspace-mixed");
    const versions = base.versions.versions;
    const latest = versions[0].generationId;
    const older = versions[1].generationId;
    expect(newerVersionSentence(latest, versions, NOW_MS)).toBeNull();
    expect(newerVersionSentence(older, versions, NOW_MS)).toBe("A newer version (31caba7be14a, 24 h old) exists.");
    expect(newerVersionSentence("okg:unknown", versions, NOW_MS)).toBeNull();
    const { model } = await recordFor("workspace-mixed", "document:handbook");
    expect(model.readLine).toBe("read from 31caba7be14a, published 2026-09-17 09:00 UTC");
    expect(model.newerVersionSentence).toBeNull();
    const older_model = { ...model, version: older, newerVersionSentence: newerVersionSentence(older, versions, NOW_MS) };
    const markup = renderToStaticMarkup(ConsoleRecordPage({ model: older_model, context: contextFor("workspace-mixed", older) }));
    expect(markup).toContain("A newer version (31caba7be14a, 24 h old) exists.");
    expect(markup).toContain('data-testid="explore-newer-version"');
  });

  it("folds a long value at 240 characters, never inside a policy token", () => {
    expect(foldValue("short")).toEqual({ shown: "short", folded: false });
    const long = `${"a".repeat(230)} [nomos:masked] ${"b".repeat(50)}`;
    const folded = foldValue(long);
    expect(folded.folded).toBe(true);
    expect(folded.shown.endsWith("[nomos:masked]")).toBe(true);
    expect(foldValue("x".repeat(FIELD_FOLD + 1)).shown.length).toBe(FIELD_FOLD);
  });

  it("keeps the query on the way back, and draws no CLI transcript", async () => {
    const { model } = await recordFor("archiv3-small", "cmssw_release:CMSSW_14_0_0", parseExploreParams({ q: "CMSSW_14_0_0" }));
    expect(model.backHref).toBe("#explore?fixture=archiv3-small&q=CMSSW_14_0_0");
    const markup = renderToStaticMarkup(ConsoleRecordPage({
      model, context: contextFor("archiv3-small", model.version),
    }));
    // The block restated the record id that is already in the meta line with
    // its own copy control, under a command that cannot fetch the record --
    // `okg search` has no exact id lookup, which the block itself said.
    expect(markup).not.toContain('data-testid="explore-cli"');
    expect(markup).not.toContain("The same search from the CLI");
    // And the page's copy control says what it copies: the whole record,
    // every tab. "Copy as text" sat inside a tab strip and read as an offer
    // to copy the tab on screen.
    expect(markup).toContain("Copy record as text");
    const plain = await recordFor("archiv3-small", "cmssw_release:CMSSW_14_0_0");
    expect(plain.model.tabHref("raw")).toBe("#explore?fixture=archiv3-small&record=cmssw_release%3ACMSSW_14_0_0&tab=raw");
  });
});

describe("every fixture", () => {
  it("renders a landing, or the nothing-to-search sentence, from its own reads with no failure", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const base = await baseFor(fixture.name);
      const chips = base.version ? buildChips(base.types, NONE, base.link, null) : null;
      const markup = renderToStaticMarkup(ConsoleExplore(pageProps(fixture.name, {
        deployment: fixture.name, version: base.version, types: base.types, params: NONE, chips, onMoreChips: null,
        results: null, listing: null, pending: null, focus: null, context: contextFor(fixture.name, base.version),
      })));
      expect(markup, fixture.name).toContain('data-copy="Explore as text"');
      if (base.version) {
        expect(markup, fixture.name).toContain('data-testid="explore-chips"');
        expect(markup, fixture.name).toContain('data-testid="explore-searchbox"');
        expect(markup, fixture.name).toContain('placeholder="Search records by text or id"');
        // The line reads as the model wrote it once the mono span is
        // dropped, and the id sits in that span (audit B21).
        expect(markup.replace(/<span class="okg-console__version-id">([0-9a-f]{12})<\/span>/g, "$1"), fixture.name)
          .toContain(landingLine(base.types, base.version));
        const short = base.version.slice(-12);
        expect(
          markup.split(`<span class="okg-console__version-id">${short}</span>`).length - 1,
          fixture.name,
        ).toBe(2);
      } else {
        expect(markup, fixture.name).toContain('data-testid="explore-empty"');
      }
    }
  });

  it("opens its featured record through the console's own parser", async () => {
    for (const fixture of DESIGN_FIXTURES) {
      const featured = fixture.expect.featured_record as string | null;
      if (!featured) continue;
      const { model } = await recordFor(fixture.name, featured);
      expect(model.id, fixture.name).toBe(featured);
      expect(model.readLine, fixture.name).toMatch(/^read from [0-9a-f]{12}, published \d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC$/);
    }
  });

  it("parses the versions read the top bar and Explore share", () => {
    for (const fixture of DESIGN_FIXTURES) {
      const list = parseVersionList(fixture.reads["/viz/api/console/versions"]);
      expect(list.deployment).toBe(fixture.name);
    }
  });
});

describe("an explore record without sections", () => {
  it("still counts its fields", async () => {
    const { record } = await recordFor("workspace-mixed", "person:riley");
    const fields: ExploreRecord["fields"] = record.fields;
    expect(fields.map((field) => field.key)).toEqual(["display_name", "email"]);
  });
});
