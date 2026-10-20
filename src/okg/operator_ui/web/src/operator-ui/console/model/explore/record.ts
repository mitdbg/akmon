// Explore: the pure model. Find a record and inspect it.
//
// Reference: ui/references/explore.md. DataHub's search page is the results
// design (facet counts that move with the query, match highlighting, "Showing
// 1-10 of 67"); its entity page is the record page (a fixed header, then tabs
// whose labels carry counts); OpenMetadata's type tree is the landing (the
// estate as types with counts); lakeFS prints the ref being read.
//
// The data layer is #1931's: `searchData.ts` parses the four record reads and
// this module adds what the redesign needs on top of them -- the policy name
// behind a hidden field, the raw layers, and the related totals -- read
// strictly from the same envelope. Nothing here fills a gap with a default: a
// read without raw layers is `raw: null`, and the page says so by name.
import { type ContentModel, buildContent, hasContent } from "@operator-ui/console/model/explore/content";
import { type Json, type MaskedPathsNotice, asBoolean, asCount, asObject, asOptionalCount, asOptionalString, asString, maskedPathsNotice } from "@operator-ui/console/model/explore/json";

export { type MaskedPathsNotice, maskedPathsNotice };
import { type IdentityField, identityFields } from "@operator-ui/console/model/explore/identity";
import { type ExploreParams, RECORD_TABS, type RecordTab, exploreQuery } from "@operator-ui/console/model/explore/params";
import type { ArtifactRead, RawRead } from "@operator-ui/console/model/explore/raw";
import { type RelatedModel, buildRelated } from "@operator-ui/console/model/explore/related";
import { recordTitle } from "@operator-ui/console/model/explore/titles";
import { exact } from "@operator-ui/console/model/shared/format";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import { relativeAge, utcStamp } from "@operator-ui/console/model/shared/time";
import { type FoundRecord, type MissingRecord, type Pinned, type PublishedVersion, SearchResponseError, parseOrigin, parseRecordDetail } from "@operator-ui/console/api/searchData";

// --- the record read, with the redesign's extra layers ---------------------

export interface ExploreField {
  key: string;
  value: string | null;
  hidden: boolean;
  echoesTitle: boolean;
}

/** A record's long text: #1931's "sections". Content shows it as the
 *  extracted text; Fields holds scalars only. */
export interface ExploreSection {
  key: string;
  title: string;
  value: string | null;
  hidden: boolean;
}

/** The recorded order a chunk list is in, by the read's own name for it.
 *  `chunk_ordinal` is the ordinal the source stored on the membership edge;
 *  `char_offset` is where the chunk starts in its document, used when no
 *  chunk of the record carries a usable ordinal (issue #2178). */
export type ChunkOrderBasis = "chunk_ordinal" | "char_offset";

const ORDER_BASES: readonly string[] = ["chunk_ordinal", "char_offset"];

/** Two words for a basis, for the sentences that name it. */
const BASIS_WORDS: Record<ChunkOrderBasis, string> = {
  chunk_ordinal: "stored ordinal",
  char_offset: "character offset",
};

export interface ChunkRow {
  nodeId: string;
  /** The stored ordinal, or null on the `char_offset` basis, where the read
   *  placed the chunk without one. Never a position this page invented. */
  ordinal: number | null;
  /** Where the chunk starts in its document, when the read recorded it. */
  charOffset: number | null;
  page: number | null;
  headingPath: string | null;
  text: string;
}

export interface RelationshipTally {
  direction: "outgoing" | "incoming";
  relationship: string;
  count: number;
}

/** A document's chunk caps, as the record read reports them. The list is
 *  ordered on a basis the READ names -- the ordinal the source stored, or
 *  the character offset it recorded -- and every count is taken after the
 *  access rules ran, so a short page never reports how many rows the policy
 *  hid. */
export interface ChunkCaps {
  cap: number;
  shown: number;
  capped: boolean;
  /** The order the read put these chunks in, by its own name for it, or
   *  null when it listed none and so ordered nothing. */
  orderBasis: ChunkOrderBasis | null;
  /** That basis in one sentence, or null when there is no order to name. */
  orderSentence: string | null;
  withoutOrdinal: number;
  withoutOrdinalCapped: boolean;
  /** Each cap this read reported, in words. Empty when it reported none. */
  sentences: string[];
}

/** The basis the read named, refused when it is a word this console does not
 *  know: listing chunks in an order we cannot name is worse than not listing
 *  them, because nothing on the page would say what the order is. */
export function chunkOrderBasis(payload: Json): ChunkOrderBasis | null {
  const value = payload.chunk_order_basis;
  if (value === null) return null;
  if (typeof value !== "string" || !ORDER_BASES.includes(value)) {
    throw new SearchResponseError("chunk order basis");
  }
  return value as ChunkOrderBasis;
}

export function chunkCaps(payload: Json, shown: number): ChunkCaps {
  const cap = asCount(payload.chunk_cap, "chunk cap");
  const capped = asBoolean(payload.chunks_capped, "chunk cap flag");
  const basis = chunkOrderBasis(payload);
  // The two must agree. A basis with nothing listed, or a list in no named
  // order, is a response this page cannot describe truthfully.
  if ((basis === null) !== (shown === 0)) throw new SearchResponseError("chunk order basis");
  const withoutOrdinal = asCount(payload.chunks_without_ordinal, "chunks without an ordinal");
  const withoutOrdinalCapped = asBoolean(
    payload.chunks_without_ordinal_capped, "chunks-without-an-ordinal cap flag",
  );
  const sentences: string[] = [];
  if (capped) {
    sentences.push(
      `This read returns at most ${exact(cap)} chunks. The ${exact(shown)} shown are the `
      + `first by ${basis ? BASIS_WORDS[basis] : "the order the read gave"}, counted after `
      + "this reader's access rules were applied; more chunks follow.",
    );
  }
  if (withoutOrdinal > 0) {
    const rows = withoutOrdinal === 1 ? "chunk is" : "chunks are";
    sentences.push(
      `${exact(withoutOrdinal)} ${rows} recorded against this record with no usable `
      + "ordinal, so this read cannot place them in order and does not list them.",
    );
  }
  if (withoutOrdinalCapped) {
    sentences.push(
      `More chunks than the ${exact(cap)} this read counts have no usable ordinal, so `
      + `${exact(withoutOrdinal)} is a floor and not the total.`,
    );
  }
  // One chunk is placed, not ordered; and "no chunk of this record carries a
  // usable ordinal" is gone. It sat beside part labels reading "Part 1" to
  // "Part 46", which come from each chunk's own source identity -- a claim
  // about ordinals this read never checked (review 2026-09-22). The basis the
  // list IS in is what the sentence names.
  const words = basis === null ? null : BASIS_WORDS[basis];
  const orderSentence = words === null
    ? null
    : shown === 1
      ? `This chunk is placed by ${words}.`
      : `These ${exact(shown)} chunks are ordered by ${words}.`;
  return {
    cap, shown, capped, orderBasis: basis, orderSentence,
    withoutOrdinal, withoutOrdinalCapped, sentences,
  };
}

/** The related lists' caps and the tally ceiling, as the record read
 *  reports them. A total the read could not compute is null AND flagged;
 *  the two must agree, and a response where they do not is unreadable. */
export interface RelatedCaps {
  cap: number;
  outgoingCapped: boolean;
  incomingCapped: boolean;
  outgoingTotal: number | null;
  incomingTotal: number | null;
  outgoingTotalCapped: boolean;
  incomingTotalCapped: boolean;
  tallyCeiling: number;
  sentences: string[];
}

export function relatedCaps(related: Json): RelatedCaps {
  const cap = asCount(related.cap, "related record cap");
  const tallyCeiling = asCount(related.tally_ceiling, "related tally ceiling");
  const outgoingCapped = asBoolean(related.outgoing_capped, "outgoing cap flag");
  const incomingCapped = asBoolean(related.incoming_capped, "incoming cap flag");
  const outgoingTotal = asOptionalCount(related.outgoing_total, "outgoing total");
  const incomingTotal = asOptionalCount(related.incoming_total, "incoming total");
  const outgoingTotalCapped = asBoolean(related.outgoing_total_capped, "outgoing total cap flag");
  const incomingTotalCapped = asBoolean(related.incoming_total_capped, "incoming total cap flag");
  // The composer sets each flag as `total is None`. A response where they
  // disagree would let a null total print as "0 related" under a flag that
  // says the count is fine, so it is refused here instead.
  if (outgoingTotalCapped !== (outgoingTotal === null)) throw new SearchResponseError("outgoing total");
  if (incomingTotalCapped !== (incomingTotal === null)) throw new SearchResponseError("incoming total");
  const sentences: string[] = [];
  if (outgoingCapped) {
    sentences.push(`This read lists at most ${exact(cap)} outgoing relationships; this record has more.`);
  }
  if (incomingCapped) {
    sentences.push(`This read lists at most ${exact(cap)} incoming relationships; this record has more.`);
  }
  if (outgoingTotalCapped) {
    sentences.push(
      `This record has more outgoing relationships than the ${exact(tallyCeiling)} this read `
      + "counts, so it reports no outgoing total.",
    );
  }
  if (incomingTotalCapped) {
    sentences.push(
      `This record has more incoming relationships than the ${exact(tallyCeiling)} this read `
      + "counts, so it reports no incoming total.",
    );
  }
  return {
    cap, outgoingCapped, incomingCapped, outgoingTotal, incomingTotal,
    outgoingTotalCapped, incomingTotalCapped, tallyCeiling, sentences,
  };
}

export interface ExploreRecord {
  base: FoundRecord;
  fields: ExploreField[];
  sections: ExploreSection[];
  /** Rows per relationship type before the cap, when the read carries them
   *  (`related.by_relationship`, a backend gap); null otherwise. */
  byRelationship: RelationshipTally[] | null;
  /** The record's chunks in order with their text, when the read carries
   *  them; null when the record has none or the read does not say. */
  chunks: ChunkRow[] | null;
  outgoingTotal: number | null;
  incomingTotal: number | null;
  /** What the record read rewrote, or null when it rewrote nothing. */
  maskedPaths: MaskedPathsNotice | null;
  /** The chunk caps, or null when the read carries no chunk keys at all --
   *  a record with no retained chunks, where the composer omits all five
   *  keys rather than sending an empty list. */
  chunkCaps: ChunkCaps | null;
  /** The related caps and the tally ceiling, always served. */
  relatedCaps: RelatedCaps;
}

export type ExploreDetail = { found: true; record: ExploreRecord } | MissingRecord;

export function parseExploreRecord(envelope: unknown): Pinned<ExploreDetail> {
  const parsed = parseRecordDetail(envelope);
  if (!parsed.payload.found) return { version: parsed.version, payload: parsed.payload };
  const base = parsed.payload;
  const payload = asObject(asObject(envelope, "record").payload, "record");
  // No field row carries a policy name. `hidden` is the whole of what the
  // read says, so the page says "hidden by policy" and names none.
  const fields: ExploreField[] = base.fields.map((field) => ({ ...field }));
  const related = asObject(payload.related, "related records");
  let byRelationship: RelationshipTally[] | null = null;
  if (related.by_relationship !== undefined && related.by_relationship !== null) {
    if (!Array.isArray(related.by_relationship)) throw new SearchResponseError("relationship tally");
    byRelationship = related.by_relationship.map((entry) => {
      const row = asObject(entry, "relationship tally");
      const direction = row.direction;
      if (direction !== "outgoing" && direction !== "incoming") throw new SearchResponseError("relationship tally");
      return { direction, relationship: asString(row.relationship, "relationship tally"), count: asCount(row.count, "relationship tally count") };
    });
  }
  let chunks: ChunkRow[] | null = null;
  if (payload.chunks !== undefined && payload.chunks !== null) {
    if (!Array.isArray(payload.chunks)) throw new SearchResponseError("record chunks");
    const basis = chunkOrderBasis(payload);
    chunks = payload.chunks.map((entry) => {
      const row = asObject(entry, "record chunk");
      const ordinal = asOptionalCount(row.ordinal, "chunk ordinal");
      // The ordinal and the basis are one statement. On `chunk_ordinal` the
      // read placed every chunk by an ordinal, so a null is a chunk it says
      // it ordered and did not; on `char_offset` it placed none by one, so a
      // number is a position it did not order by. Either way the list would
      // be printed under a heading that does not describe it.
      if ((basis === "chunk_ordinal") !== (ordinal !== null)) {
        throw new SearchResponseError("chunk ordinal");
      }
      return {
        nodeId: asString(row.node_id, "chunk id"),
        ordinal,
        charOffset: asOptionalCount(row.char_offset, "chunk character offset"),
        page: asOptionalCount(row.page, "chunk page"),
        headingPath: asOptionalString(row.heading_path, "chunk heading"),
        text: asString(row.text, "chunk text"),
      };
    });
    // Re-sorted on the read's OWN basis, never on one this page picked: the
    // server already ordered them and this only holds that order against a
    // transport that reshuffles. On `char_offset` a missing offset cannot
    // sort, so the served order stands.
    if (basis === "chunk_ordinal") {
      chunks.sort((a, b) => (a.ordinal as number) - (b.ordinal as number));
    } else if (basis === "char_offset" && chunks.every((chunk) => chunk.charOffset !== null)) {
      chunks.sort((a, b) => (a.charOffset as number) - (b.charOffset as number));
    }
  }
  return {
    version: parsed.version,
    payload: {
      found: true,
      record: {
        base,
        fields,
        sections: base.sections.map((section) => ({ ...section })),
        byRelationship,
        chunks,
        outgoingTotal: asOptionalCount(related.outgoing_total, "outgoing total"),
        incomingTotal: asOptionalCount(related.incoming_total, "incoming total"),
        maskedPaths: maskedPathsNotice(payload, "record read"),
        chunkCaps: chunks === null ? null : chunkCaps(payload, chunks.length),
        relatedCaps: relatedCaps(related),
      },
    },
  };
}


/** A field value longer than this folds, with "show all" in its place. */
export const FIELD_FOLD = 240;


// --- Provenance -----------------------------------------------------------------

export interface ProvenanceModel {
  /** What the page PRINTS for the source: the producer's own name when the
   *  producers read names one for this ownership id, else the ownership id
   *  the record carries. */
  source: string | null;
  /** The ownership id as recorded, when it is not what is printed. */
  sourceOwnershipId: string | null;
  sourceHref: string | null;
  sourceRecordId: IdentityField[];
  sourceRevision: IdentityField[];
  /** Null when the policy neither hid nor shortened anything. */
  policyLine: string | null;
}

/** Enough of the producers read to resolve one source. Passed in rather than
 *  read here: this module is pure. */
export interface ProducerName {
  name: string;
  ownershipId: string;
}

/** What a policy did to this record, in one line, or null when it did
 *  nothing. */
export function policyLine(hidden: number, shortened: number): string | null {
  const parts: string[] = [];
  if (hidden) parts.push(`hid ${exact(hidden)} ${hidden === 1 ? "value" : "values"}`);
  if (shortened) {
    parts.push(`shortened ${exact(shortened)} ${shortened === 1 ? "path" : "paths"}`);
  }
  return parts.length ? `${parts.join(" and ")} of this record` : null;
}

/** What the record read says about where the record came from. The fact
 *  log, the last-write attempt and the enricher tally are NOT here: the
 *  detail payload carries no `provenance` key, so nothing sources them.
 *  See ui/cuts.md. */
function buildProvenance(
  detail: ExploreRecord, link: LinkFn, hiddenCount: number, producers: readonly ProducerName[] | null,
): ProvenanceModel {
  const base = detail.base;
  // "open in Sources" linked the record's `source` straight into a Sources
  // URL. On the live graphs that is an OWNERSHIP ID -- `cern-r3final.twiki-eos`
  // -- and Sources is addressed by producer NAME, `twiki_eos`, so every link
  // landed on not-found. The producers read maps the one to the other; when
  // it names no producer for this id (or the read failed), the source is
  // printed and not linked, rather than linked somewhere that is not there.
  const shortenedPaths = detail.maskedPaths?.keys ?? [];
  const owned = base.source && producers
    ? producers.find((producer) => producer.ownershipId === base.source) ?? null
    : null;
  return {
    source: owned ? owned.name : base.source,
    sourceOwnershipId: owned && owned.name !== base.source ? base.source : null,
    sourceHref: owned ? link(`sources/${encodeURIComponent(owned.name)}`) : null,
    sourceRecordId: identityFields(base.sourceRecordId, "source_record_id", shortenedPaths),
    sourceRevision: identityFields(base.sourceRevision, "source_revision", shortenedPaths),
    // One line, counting BOTH things a policy did to this record: the values
    // it hid and the paths the read shortened. The masking banner counted the
    // second and this line counted only the first, so a record with one
    // shortened path read "hid no value" under a banner saying one value was
    // shortened. When neither happened there is nothing to report and the
    // line is absent, not a sentence saying nothing was done.
    policyLine: policyLine(hiddenCount, shortenedPaths.length),
  };
}

// --- the page ---------------------------------------------------------------------

export interface RecordPageModel {
  id: string;
  /** The label the server served, which for a chunk is its own hash. */
  label: string;
  /** What the page is TITLED: the document this record came out of and
   *  where in it this record sits, by the same rule a results row is
   *  titled by (`titles.ts`). The label when the source records no path. */
  title: string;
  type: string;
  source: string | null;
  /** "Open in source": the record's citation_url, else the first field
   *  whose value is an http(s) URL. */
  openInSource: string | null;
  /** The version the read answered from, its publish time, and its age. */
  version: string;
  publishedAt: string | null;
  readLine: string;
  newerVersionSentence: string | null;
  backHref: string;
  /** This record's neighbourhood on the Graph view. */
  graphHref: string;
  fields: ExploreField[];
  fieldCount: number;
  hiddenCount: number;
  content: ContentModel;
  related: RelatedModel;
  relatedCount: number;
  /** True when the related count is the rows shown, not the total. */
  relatedCountIsFloor: boolean;
  provenance: ProvenanceModel;
  /** What the RECORD read rewrote before answering, or null when it
   *  rewrote nothing. Printed once under the header: it applies to every
   *  tab built from that read, not to one of them. */
  maskedPaths: MaskedPathsNotice | null;
  /** Null when the page made no raw read (the live read does not serve it
   *  yet). The Raw tab then says so by name. */
  raw: RawRead | null;
  /** The tabs this record has: Content only when hasContent. */
  tabs: RecordTab[];
  tab: RecordTab;
  tabHref: (tab: RecordTab) => string;
  /** Said once under the header on a record that holds no text: which kind
   *  of metadata it is, and that its text is not in this graph. Null on a
   *  record with content. */
  noTextSentence: string | null;
  /** Why the page is not on the tab the URL asked for, when it is not. */
  tabNote?: string | null;
}

/** What a record of a type that holds no text IS, where this page knows. */
const NO_TEXT_WORDS: Readonly<Record<string, string>> = {
  cmssw_release: "This record holds release metadata only; the notes are not in this graph.",
};

/** The sentence for a record with no text: the type's own words, else the
 *  general one. */
export function noTextSentence(type: string): string {
  return NO_TEXT_WORDS[type] ?? "This record holds fields only; no text of it is in this graph.";
}

const URL_VALUE = /^https?:\/\/\S+$/;

/**
 * Where this record can be read at its source: `citation_url`, and only that.
 *
 * It used to fall back to the first field whose value LOOKED like a URL,
 * which on a record carrying a schema URL, a licence link or a repository
 * home sent the reader somewhere that is not this record. A link that may be
 * the wrong page is worse than no link: the record page has the source name
 * and the source record id either way.
 */
export function openInSourceUrl(fields: ExploreField[]): string | null {
  const citation = fields.find((field) => field.key === "citation_url" && field.value && URL_VALUE.test(field.value));
  return citation?.value ?? null;
}

/** "A newer version (1ab22aac0d83, 2 d ago) exists." when one was published
 *  after the version this record was read from. The versions list is newest
 *  first and carries the publish time of each. */
export function newerVersionSentence(
  readVersion: string, versions: PublishedVersion[], nowMs: number,
): string | null {
  const read = versions.find((row) => row.generationId === readVersion);
  const readAt = read?.publishedAt ? Date.parse(read.publishedAt) : null;
  const newer = versions.find((row) => {
    if (row.generationId === readVersion) return false;
    if (readAt === null || !row.publishedAt) return false;
    return Date.parse(row.publishedAt) > readAt;
  });
  if (!newer) return null;
  return `A newer version (${shortVersion(newer.generationId)}, `
    + `${relativeAge(newer.publishedAt, nowMs).replace(/ ago$/, " old")}) exists.`;
}

export function buildRecordPage(
  detail: ExploreRecord,
  reads: {
    artifact: ArtifactRead | null;
    raw: RawRead | null;
    /** The producers this deployment lists, when that read answered. Null
     *  when it did not, which costs the source link and nothing else. */
    producers?: readonly ProducerName[] | null;
  },
  version: string,
  versions: PublishedVersion[],
  params: ExploreParams,
  link: LinkFn,
  deployment: string,
  nowMs: number,
  pageSize: number,
): RecordPageModel {
  const base = detail.base;
  const publishedAt = versions.find((row) => row.generationId === version)?.publishedAt ?? null;
  const open = (id: string) => link("explore", exploreQuery({
    q: params.q, type: params.type, record: id, version: params.version,
  }));
  const hiddenCount = detail.fields.filter((field) => field.hidden).length;
  const content = buildContent(detail, reads.artifact, open);
  const related = buildRelated(detail, params, link, open);
  // The tab counts RECORDS, which is what the tab lists.
  //
  // It counted the server's edge totals, and the graph records a chunk and
  // its document twice -- `contains` one way, `member_of` the other -- so the
  // live document read "Related (4)" over a sentence saying "Contains 2
  // document_chunk records" and a table of two neighbours. The distinct
  // neighbours among the rows the read served is a number the page can
  // stand behind; it is marked a floor whenever a cap or a total says the
  // read did not serve them all.
  const neighbours = new Set(
    [...base.outgoing, ...base.incoming].map((record) => record.nodeId),
  );
  const relatedCount = neighbours.size;
  const moreOutgoing = detail.outgoingTotal !== null && detail.outgoingTotal > base.outgoing.length;
  const moreIncoming = detail.incomingTotal !== null && detail.incomingTotal > base.incoming.length;
  const tabs: RecordTab[] = RECORD_TABS.filter((tab) => tab !== "content" || hasContent(content));
  // A record with no content cannot open on Content: the tab is not in the
  // strip, so the first one that IS becomes the page -- unless that is an
  // empty Related, which opened CMSSW_13_0_X on "Related (0)" and a sentence
  // saying there was nothing to show (review 2026-09-23). Fields, then.
  const emptyRelated = relatedCount === 0 && !moreOutgoing && !moreIncoming;
  const fallback = tabs[0] === "related" && emptyRelated ? "fields" : tabs[0];
  const tab = tabs.includes(params.tab) ? params.tab : fallback;
  return {
    id: base.nodeId,
    label: base.label,
    // The same origin the results page titles a row from, read off this
    // record's own `source_record_id`.
    title: recordTitle({ label: base.label, origin: parseOrigin(base.sourceRecordId) }),
    type: base.type,
    source: base.source,
    openInSource: openInSourceUrl(detail.fields),
    version,
    publishedAt,
    readLine: `read from ${shortVersion(version)}, published ${utcStamp(publishedAt)}`,
    newerVersionSentence: newerVersionSentence(version, versions, nowMs),
    backHref: link("explore", exploreQuery({ q: params.q, type: params.type })),
    graphHref: link("graph/record", { id: base.nodeId }),
    fields: detail.fields,
    fieldCount: detail.fields.length,
    hiddenCount,
    content,
    related,
    relatedCount,
    relatedCountIsFloor: base.outgoingCapped || base.incomingCapped || moreOutgoing || moreIncoming,
    provenance: buildProvenance(detail, link, hiddenCount, reads.producers ?? null),
    maskedPaths: detail.maskedPaths,
    raw: reads.raw,
    tabs,
    // `&tab=content` on a record with no text used to switch tabs in
    // silence (audit 2026-09-23). The page says why it opened elsewhere.
    tabNote: params.tabAsked && params.tab !== tab && params.tab === "content"
      ? `This record holds no text, so it opens on ${tab[0].toUpperCase()}${tab.slice(1)}.`
      : null,
    tab,
    noTextSentence: hasContent(content) ? null : noTextSentence(base.type),
    tabHref: (next) => link("explore", exploreQuery({
      ...params, record: base.nodeId, tab: next,
      // Related's own filters travel with its tab only.
      ...(next === "related" ? {} : { rel: null, dir: "both", roffset: 0 }),
    })),
  };
}


export function foldValue(value: string): { shown: string; folded: boolean } {
  if (value.length <= FIELD_FOLD) return { shown: value, folded: false };
  const token = "[nomos:masked]";
  const start = value.lastIndexOf(token, FIELD_FOLD - 1);
  const inside = start !== -1 && start + token.length > FIELD_FOLD;
  return { shown: value.slice(0, inside ? start + token.length : FIELD_FOLD).trimEnd(), folded: true };
}
