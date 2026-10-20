/**
 * The record read parsers and loaders, asserted directly (PACT
 * operator-ui-search-view, moved here with the code).
 *
 * #1931 wrote these against its Search view. That view is not shipped, but
 * `console/api/searchData.ts` and `console/api/searchApi.ts` are: Explore
 * reads through them. The thirteen cases below are the ones from
 * routes/search/search.test.tsx that test THIS code rather than the retired
 * view, carried over unchanged so removing the view did not quietly remove
 * its parsers' guards. The view's own seventeen cases went with the view,
 * and the five that tested the `#search` URL grammar went with that grammar.
 */
import { describe, expect, it } from "vitest";
import { ApiRequestError, type ApiClient } from "@operator-ui/shared/api/apiClient";
import {
  describeSearchFailure,
  listRecords,
  loadRecord,
  searchRecords,
} from "@operator-ui/console/api/searchApi";
import {
  cliSearchCommand,
  describeIdentity,
  parseListPage,
  parseRecordDetail,
  parseSearchPage,
  parseVersionList,
  SearchResponseError,
  shortVersion,
  splitMaskedText,
  tidyExcerpt,
  truncateText,
} from "@operator-ui/console/api/searchData";

const LATEST = "okg:archiv3:branch:default:gen:20260914T182831091484Z:fa0201765352";
const OLDER = "okg:archiv3:branch:default:gen:20260914T181948336267Z:6d156f910a61";

const searchEnvelope = {
  generation_id: LATEST,
  payload: {
    query: "documentation_page:intro",
    type: null,
    results: [
      {
        node_id: "documentation_page:intro",
        type: "documentation_page",
        label: "Introduction · FTS3 Documentation",
        source: "archiv3.docsite_snapshot",
        match: "record_id",
        excerpt: null,
        trust: null,
        source_record_id: null,
        source_revision: null,
      },
      {
        node_id: "chunk:bf31",
        type: "document_chunk",
        label: "Document Chunk · bf31",
        source: "archiv3.docsite_snapshot",
        match: "text",
        excerpt: "release notes for the documentation site",
        trust: "trusted",
        source_record_id: null,
        source_revision: null,
      },
    ],
    exact_match: true,
    exact_match_in_text: false,
    total_count: 2,
    truncated: false,
    next_offset: null,
    rank_capped: false,
    zero_hit_reason: null,
  },
};

const recordEnvelope = {
  generation_id: OLDER,
  payload: {
    found: true,
    node_id: "documentation_page:intro",
    type: "documentation_page",
    label: "Introduction · FTS3 Documentation",
    source: "archiv3.docsite_snapshot",
    source_record_id: { nomos_pseudonym: "directidentifier_e1_ad89" },
    source_revision: { v: 2 },
    policy_redacted: true,
    fields: [
      { key: "title", value: "Introduction · FTS3 Documentation", hidden: false, echoes_title: false },
      { key: "url", value: null, hidden: true, echoes_title: false },
    ],
    sections: [
      { key: "body", title: "Content", value: "Overview of the release [nomos:masked] process", hidden: false },
      { key: "summary", title: "Summary", value: null, hidden: true },
    ],
    related: {
      outgoing: [
        { relationship: "contains", node_id: "chunk:bf31", type: "document_chunk", label: "Document Chunk · bf31" },
      ],
      incoming: [],
      cap: 40,
      outgoing_capped: false,
      incoming_capped: false,
    },
  },
};

describe("Search response parsing", () => {
  // #2761: the server sends four match kinds (compose_record_search in
  // src/okg/substrate/console_views/records.py), and the parser took two, so
  // any page with an exact label match failed whole.
  const withMatch = (match: unknown) => ({
    ...searchEnvelope,
    payload: { ...searchEnvelope.payload, results: [{ ...searchEnvelope.payload.results[0], match }] },
  });

  it.each(["record_id", "typed_record_id", "label", "text"])("reads a %s match as the server sent it", (match) => {
    expect(parseSearchPage(withMatch(match)).payload.results[0].match).toBe(match);
  });

  it.each(["exact", "Label", "", 3, null])("refuses an unknown match kind %j instead of guessing one", (match) => {
    expect(() => parseSearchPage(withMatch(match))).toThrow(SearchResponseError);
  });

  it("reads a record page and a missing record", () => {
    const found = parseRecordDetail(recordEnvelope);
    const missing = parseRecordDetail({ generation_id: LATEST, payload: { found: false, node_id: "x:1" } });

    expect(found.version).toBe(OLDER);
    expect(found.payload.found && found.payload.fields[1]).toEqual({ key: "url", value: null, hidden: true, echoesTitle: false });
    expect(found.payload.found && found.payload.sections[1]).toEqual({ key: "summary", title: "Summary", value: null, hidden: true });
    expect(missing.payload).toEqual({ found: false, nodeId: "x:1" });
  });

  it("refuses page positions that are negative or fractional, and a visible field without a value", () => {
    const listPage = (next_offset: unknown) => ({ generation_id: LATEST, payload: { type: "t", records: [], next_offset, short_page: false } });
    const visibleNull = {
      ...recordEnvelope,
      payload: { ...recordEnvelope.payload, fields: [{ key: "title", value: null, hidden: false, echoes_title: false }] },
    };

    expect(() => parseListPage(listPage(-1))).toThrow(SearchResponseError);
    expect(() => parseListPage(listPage(2.5))).toThrow(SearchResponseError);
    expect(parseListPage(listPage(50)).payload.nextOffset).toBe(50);
    expect(() => parseSearchPage({ ...searchEnvelope, payload: { ...searchEnvelope.payload, next_offset: -20 } })).toThrow(SearchResponseError);
    // The paged count (#2789) is a count of rows: absent reads as null, a
    // negative or fractional one is refused.
    expect(parseSearchPage(searchEnvelope).payload.pagedCount).toBeNull();
    expect(parseSearchPage({ ...searchEnvelope, payload: { ...searchEnvelope.payload, paged_count: 1 } }).payload.pagedCount).toBe(1);
    expect(() => parseSearchPage({ ...searchEnvelope, payload: { ...searchEnvelope.payload, paged_count: -1 } })).toThrow(SearchResponseError);
    expect(() => parseSearchPage({ ...searchEnvelope, payload: { ...searchEnvelope.payload, paged_count: 1.5 } })).toThrow(SearchResponseError);
    expect(() => parseRecordDetail(visibleNull)).toThrow(SearchResponseError);
  });

  it("refuses a malformed response instead of showing an empty result", () => {
    const withoutCap = {
      ...recordEnvelope,
      payload: { ...recordEnvelope.payload, related: { ...recordEnvelope.payload.related, cap: undefined } },
    };
    const sectionWithoutFlag = {
      ...recordEnvelope,
      payload: { ...recordEnvelope.payload, sections: [{ key: "body", title: "Content", value: "x" }] },
    };

    expect(() => parseSearchPage({ generation_id: LATEST, payload: { query: "x" } })).toThrow(SearchResponseError);
    expect(() => parseListPage({ payload: { type: "t", records: [] } })).toThrow(SearchResponseError);
    expect(() => parseVersionList({ payload: { versions: "nope" } })).toThrow(SearchResponseError);
    expect(() => parseRecordDetail(withoutCap)).toThrow(SearchResponseError);
    expect(() => parseRecordDetail(sectionWithoutFlag)).toThrow(SearchResponseError);
  });

  it("shows every version by the same twelve-character short form", () => {
    expect(shortVersion(LATEST)).toBe("fa0201765352");
    expect(shortVersion(OLDER)).toBe("6d156f910a61");
  });

  it("describes stored source identities for an operator", () => {
    expect(describeIdentity(null)).toEqual({ kind: "none" });
    expect(describeIdentity({})).toEqual({ kind: "none" });
    expect(describeIdentity({ nomos_pseudonym: "p1" })).toEqual({ kind: "pseudonymized", value: "p1" });
    expect(describeIdentity({ v: 2 })).toEqual({ kind: "value", value: "2" });
  });

  it("shows policy-hidden values inside text as hidden", () => {
    expect(splitMaskedText("a [nomos:masked] b [nomos:masked]")).toEqual([
      { kind: "text", value: "a " },
      { kind: "hidden" },
      { kind: "text", value: " b " },
      { kind: "hidden" },
    ]);
  });

  it("starts a search excerpt at a word boundary", () => {
    expect(tidyExcerpt("...imizer Staging from Tapes")).toBe("…Staging from Tapes");
    expect(tidyExcerpt("Staging from Tapes")).toBe("Staging from Tapes");
  });

  it("shows a hidden value cut off at either end of an excerpt as hidden", () => {
    expect(splitMaskedText(tidyExcerpt("...s Upgrade FTS 3.12 FTS [nom..."))).toEqual([
      { kind: "text", value: "…Upgrade FTS 3.12 FTS " },
      { kind: "hidden" },
      { kind: "text", value: "…" },
    ]);
    expect(splitMaskedText(tidyExcerpt("...sked]"))).toEqual([{ kind: "text", value: "…" }, { kind: "hidden" }]);
    expect(tidyExcerpt("see [link] and more...")).toBe("see [link] and more…");
    expect(tidyExcerpt("usage: cmd [...")).toBe("usage: cmd […");
  });

  it("never cuts long text inside a hidden-value token", () => {
    const text = `${"a".repeat(595)}[nomos:masked] tail`;

    expect(truncateText(text, 600)).toBe(`${"a".repeat(595)}[nomos:masked]`);
    expect(truncateText("short", 600)).toBe("short");
    expect(truncateText(`${"b".repeat(700)}`, 600)).toHaveLength(600);
  });

  it("quotes the CLI search so a query cannot break it, and names the page", () => {
    expect(cliSearchCommand({ deployment: "archiv3", query: "a'b", type: null, version: LATEST, limit: 20, offset: 0 })).toBe(
      `okg search --deployment 'archiv3' --query 'a'\\''b' --generation-id '${LATEST}' --limit 20 --json`,
    );
    expect(cliSearchCommand({ deployment: "archiv3", query: "notes", type: "document_chunk", version: LATEST, limit: 20, offset: 40 })).toBe(
      `okg search --deployment 'archiv3' --query 'notes' --subtype 'document_chunk' --generation-id '${LATEST}' --limit 20 --offset 40 --json`,
    );
  });
});

describe("Search reads", () => {
  function recordingClient(response: unknown) {
    const calls: Array<{ route: string; params: Record<string, unknown> | undefined }> = [];
    const api: ApiClient = {
      get: async <T,>(route: `/viz/api/${string}`, params?: Record<string, string | number | boolean | null | undefined>) => {
        calls.push({ route, params });
        return response as T;
      },
    };
    return { api, calls };
  }

  it("names the version on every record read", async () => {
    const search = recordingClient(searchEnvelope);
    const list = recordingClient({ generation_id: LATEST, payload: { type: "t", records: [], next_offset: null, short_page: false } });
    const record = recordingClient(recordEnvelope);

    await searchRecords(search.api, { version: LATEST, query: "q", type: null, offset: 0 });
    await listRecords(list.api, { version: LATEST, type: "t", offset: 50 });
    await loadRecord(record.api, { version: OLDER, id: "documentation_page:intro" });

    expect(search.calls[0]).toMatchObject({ route: "/viz/api/console/records/search", params: { version: LATEST, q: "q" } });
    expect(list.calls[0]).toMatchObject({ route: "/viz/api/console/records/list", params: { version: LATEST, type: "t", offset: 50 } });
    expect(record.calls[0]).toMatchObject({ route: "/viz/api/console/records/detail", params: { version: OLDER, id: "documentation_page:intro" } });
  });

  it("explains a policy refusal with the server's next check", () => {
    const error = new ApiRequestError({
      route: "/viz/api/console/records/search",
      status: 403,
      responseText: JSON.stringify({
        detail: {
          reason: "policy_denied",
          error: "Nomos denied graph.read.local_operator: nomos.default_deny",
          next_check: "run okg deployment apply archiv3",
        },
      }),
    });

    expect(describeSearchFailure(error, LATEST)).toMatchObject({
      title: "The access policy refused this read",
      nextCheck: "run okg deployment apply archiv3",
    });
  });

  it("says a refused version was not replaced by another", () => {
    const error = new ApiRequestError({
      route: "/viz/api/console/records/search",
      status: 404,
      responseText: JSON.stringify({ detail: { reason: "version_not_published", version: OLDER } }),
    });

    const failure = describeSearchFailure(error, OLDER);
    expect(failure.title).toBe(`Version ${OLDER} is not a published version of this deployment`);
    expect(failure.detail).toContain("Nothing was substituted");
  });
});
