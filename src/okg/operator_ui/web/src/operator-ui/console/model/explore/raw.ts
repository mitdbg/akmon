// The artifact read and the raw read (stored row, fact log, retained payload
// or refusal), parsed strictly, and the text forms the Raw tab prints.
import { type Json, type MaskedPathsNotice, asCount, asObject, asOptionalCount, asOptionalString, asString, maskedPathsNotice, noMaskedPaths } from "@operator-ui/console/model/explore/json";
import { exact } from "@operator-ui/console/model/shared/format";
import { type Pinned, SearchResponseError } from "@operator-ui/console/api/searchData";

/** A detail that starts by naming the policy, without that prefix, so the
 *  sentence around it names the policy once. */
export function withoutPolicyPrefix(detail: string, policy: string | null): string {
  if (!policy) return detail;
  const prefix = new RegExp(`^policy\\s+${policy.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s+`, "i");
  return detail.replace(prefix, "");
}

// --- the artifact read and the raw read -------------------------------------

export type ArtifactAbsence = "not_retained" | "policy_refused" | "file_missing" | "not_found";

export type ArtifactRead =
  | {
    exists: true;
    mediaType: string;
    format: string | null;
    path: string | null;
    byteCount: number;
    sha256: string;
    url: string;
  }
  | {
    exists: false;
    reason: ArtifactAbsence;
    policy: string | null;
    format: string | null;
    path: string | null;
    detail: string;
  };

/** Exactly the codes `compose_record_artifact` can return. A code outside
 *  this set fails the read by name; it is never rendered. */
export const ABSENCES: ArtifactAbsence[] = ["not_retained", "policy_refused", "file_missing", "not_found"];

/** One sentence for each artifact absence the composer can emit. The
 *  served `detail` is printed in every branch -- it is what distinguishes
 *  one instance of a code from another -- and an unknown code throws. */
export function artifactAbsenceSentence(
  read: { reason: ArtifactAbsence; policy: string | null; path: string | null; detail: string },
): string {
  const where = read.path ? ` (${read.path})` : "";
  switch (read.reason) {
    case "not_found":
      return `This version holds no such record${where}, so it names no original file: ${read.detail}.`;
    case "policy_refused":
      return `The original file is withheld by policy ${read.policy ?? "(the read named none)"}: `
        + `${withoutPolicyPrefix(read.detail, read.policy)}.`;
    case "file_missing":
      return `This record names retained bytes${where} that the store no longer holds: ${read.detail}.`;
    case "not_retained":
      return `No original file is retained for this record${where}: ${read.detail}.`;
    default: {
      const unknown: never = read.reason;
      throw new SearchResponseError(`artifact absence ${String(unknown)}`);
    }
  }
}

/** `records/artifact?id=`: whether the original file exists, what it is,
 *  and where its bytes are served; or why not, by name. */
export function parseArtifactRead(envelope: unknown): Pinned<ArtifactRead> {
  const body = asObject(envelope, "artifact read");
  const version = asString(body.generation_id, "artifact read version");
  const payload = asObject(body.payload, "artifact read");
  if (payload.exists === true) {
    return {
      version,
      payload: {
        exists: true,
        mediaType: asString(payload.media_type, "artifact media type"),
        format: asOptionalString(payload.format, "artifact format"),
        path: asOptionalString(payload.path, "artifact path"),
        byteCount: asCount(payload.byte_count, "artifact byte count"),
        sha256: asString(payload.sha256, "artifact hash"),
        url: asString(payload.url, "artifact url"),
      },
    };
  }
  if (payload.exists !== false) throw new SearchResponseError("artifact read");
  const reason = payload.reason;
  if (typeof reason !== "string" || !ABSENCES.includes(reason as ArtifactAbsence)) throw new SearchResponseError("artifact absence");
  return {
    version,
    payload: {
      exists: false,
      reason: reason as ArtifactAbsence,
      policy: asOptionalString(payload.policy, "artifact policy"),
      format: asOptionalString(payload.format, "artifact format"),
      path: asOptionalString(payload.path, "artifact path"),
      detail: asString(payload.detail, "artifact absence detail"),
    },
  };
}

export interface RawEdge {
  src: string;
  dst: string;
  relationship: string;
  /** The edge row as stored, every column. */
  row: Json;
}

interface RawPayloadFacts {
  sha256: string;
  byteCount: number;
  lineCount: number | null;
  url: string;
  /** `pinning_basis`, always the literal PINNING_BASIS today. */
  pinningBasis: string;
  /** What that basis does not promise, in words. */
  pinningCaveat: string;
}

/**
 * A retained payload, in the two shapes the composer can send it.
 *
 * `inline` is the whole payload as pretty JSON when it is at most
 * INLINE_PAYLOAD_LIMIT bytes. Past that the composer sends `head` instead --
 * the first RAW_HEAD_LINES lines -- and the Raw tab's own sentence is "the
 * first 200 lines follow". A payload with NEITHER has nothing to put under
 * that sentence, so the union does not spell it and `parseRawRead` refuses
 * it by name. It used to parse, and the tab printed an empty fence, which
 * reads as a retained file that is empty.
 */
export type RawPayload = RawPayloadFacts & (
  | { inline: string; head: string | null }
  | { inline: null; head: string }
);

/** Exactly the reasons `compose_record_raw` and its helpers can return.
 *  `not_retained` is ONE code with several distinct causes; the composer
 *  tells them apart only in `detail`, so `detail` is printed verbatim and
 *  never summarised away. */
export type RawRefusalReason =
  | "not_found"
  | "not_retained"
  | "policy_refused"
  | "payload_not_pinned_to_version"
  | "payload_identity_ambiguous"
  | "payload_has_several_fields";

export const RAW_REFUSALS: RawRefusalReason[] = [
  "not_found",
  "not_retained",
  "policy_refused",
  "payload_not_pinned_to_version",
  "payload_identity_ambiguous",
  "payload_has_several_fields",
];

export interface RawRefusal {
  reason: RawRefusalReason;
  policy: string | null;
  detail: string;
}

/** The only value `pinning_basis` takes, and what it does NOT promise. */
export const PINNING_BASIS = "bound_before_publish";

/** What the pin rests on, in words. The composer names this basis rather
 *  than implying a stronger one, and the caveat is the window it leaves
 *  open: nothing records which authority a generation's facts came from. */
export function pinningBasisCaveat(basis: string): string {
  if (basis !== PINNING_BASIS) throw new SearchResponseError(`retained payload pinning basis ${basis}`);
  return "This payload is pinned only in that the source's authority was bound before "
    + "this version was published. Nothing records which authority a version's facts "
    + "were built from, so an authority rebound between the ingest that wrote the facts "
    + "and the publish would pass this check, and the payload would belong to a later "
    + "ingest than the facts do.";
}

export interface RawRead {
  row: Json | null;
  edges: RawEdge[];
  /** How many edge rows the record has, or null when the read stopped at its
   *  cap without counting them (`edge_total: null` with `edges_truncated`,
   *  served by the sibling change console-archiv3-read-layer-fixes). */
  edgeTotal: number | null;
  /** True when the record has more edge rows than the read returned. Read
   *  from `edges_truncated` where served, else from a total above the rows. */
  edgesTruncated: boolean;
  edgeCap: number;
  payload: RawPayload | null;
  refusal: RawRefusal | null;
  /** What the raw read rewrote, or null when it rewrote nothing. */
  maskedPaths: MaskedPathsNotice | null;
}

/** "First 100 shown; this record has more" where the read stopped without
 *  counting, "100 of 140 shown" where it counted, else the row count. */
export function edgeCountLine(raw: RawRead): string {
  if (raw.edgesTruncated) {
    return raw.edgeTotal === null
      ? `First ${exact(raw.edges.length)} shown; this record has more`
      : `${exact(raw.edges.length)} of ${exact(raw.edgeTotal)} shown`;
  }
  const count = raw.edgeTotal ?? raw.edges.length;
  return `${exact(count)} ${count === 1 ? "row" : "rows"}`;
}

/** The raw read inlines a payload up to this many bytes; the fixture's
 *  generator holds the same number. */
export const INLINE_PAYLOAD_LIMIT = 256 * 1024;
export const RAW_HEAD_LINES = 200;

/** `records/raw?id=`: the stored row, the stored edge rows and the retained
 *  payload -- or its refusal, with the row and edges still served. */
export function parseRawRead(envelope: unknown): Pinned<RawRead> {
  const body = asObject(envelope, "raw read");
  const version = asString(body.generation_id, "raw read version");
  const payload = asObject(body.payload, "raw read");
  if (!Array.isArray(payload.edges)) throw new SearchResponseError("raw edges");
  const edges: RawEdge[] = payload.edges.map((entry) => {
    const row = asObject(entry, "raw edge");
    return {
      src: asString(row.src, "raw edge source"),
      dst: asString(row.dst, "raw edge target"),
      relationship: asString(row.relationship, "raw edge relationship"),
      row,
    };
  });
  let retained: RawPayload | null = null;
  if (payload.payload !== null && payload.payload !== undefined) {
    const row = asObject(payload.payload, "retained payload");
    const inline = asOptionalString(row.inline, "retained payload text");
    const byteCount = asCount(row.byte_count, "retained payload byte count");
    const basis = asString(row.pinning_basis, "retained payload pinning basis");
    if (inline !== null && byteCount > INLINE_PAYLOAD_LIMIT) throw new SearchResponseError("retained payload inline past the limit");
    const head = asOptionalString(row.head, "retained payload head");
    const facts: RawPayloadFacts = {
      sha256: asString(row.sha256, "retained payload hash"),
      byteCount,
      lineCount: asOptionalCount(row.line_count, "retained payload line count"),
      url: asString(row.url, "retained payload url"),
      pinningBasis: basis,
      pinningCaveat: pinningBasisCaveat(basis),
    };
    if (inline === null) {
      // Past the inline limit, so the head is the only text this read has.
      if (head === null) throw new SearchResponseError("retained payload past the inline limit with no head");
      retained = { ...facts, inline: null, head };
    } else {
      retained = { ...facts, inline, head };
    }
  }
  let refusal: RawRefusal | null = null;
  if (payload.refusal !== null && payload.refusal !== undefined) {
    const row = asObject(payload.refusal, "raw refusal");
    const reason = asString(row.reason, "raw refusal reason");
    // A code this parser does not know fails the READ. Rendering it as a
    // generic sentence would hide which of six conditions fired.
    if (!RAW_REFUSALS.includes(reason as RawRefusalReason)) {
      throw new SearchResponseError(`raw refusal reason ${reason}`);
    }
    refusal = {
      reason: reason as RawRefusalReason,
      policy: asOptionalString(row.policy, "raw refusal policy"),
      detail: asString(row.detail, "raw refusal detail"),
    };
  }
  if (retained === null && refusal === null) throw new SearchResponseError("raw read: neither a payload nor a refusal");
  return {
    version,
    payload: {
      row: payload.row === null || payload.row === undefined ? null : asObject(payload.row, "stored row"),
      edges,
      ...edgeCount(payload, edges.length),
      edgeCap: asCount(payload.edge_cap, "raw edge cap"),
      payload: retained,
      refusal,
      // The not-found answer is assembled without reading a row, so it
      // carries no masking keys; every other raw read must carry both.
      maskedPaths: refusal !== null && refusal.reason === "not_found"
        ? noMaskedPaths(payload, "raw read")
        : maskedPathsNotice(payload, "raw read"),
    },
  };
}

export function prettyJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

/** The retained payload as text for the clipboard and the fence: the whole
 *  of it when inline, else its head with a line saying what was cut. */
export function payloadText(payload: RawPayload): string {
  if (payload.inline !== null) return payload.inline.replace(/\n$/, "");
  const head = payload.head.replace(/\n$/, "");
  const cut = payload.lineCount !== null
    ? `[first ${exact(RAW_HEAD_LINES)} of ${exact(payload.lineCount)} lines; ${exact(payload.byteCount)} bytes at ${payload.url}]`
    : `[first ${exact(RAW_HEAD_LINES)} lines; ${exact(payload.byteCount)} bytes at ${payload.url}]`;
  return `${head}\n${cut}`;
}

/**
 * The edge count as the read states it.
 *
 * Archiv3's raw read served `edge_total: 101` beside a hundred rows and a
 * cap of 100 -- the probe row the read fetched to learn there were more, not
 * a count -- and the tab printed "100 of 101 shown; the read stops at 100"
 * (review 2026-09-22). A read that did not count serves `edge_total: null`
 * with `edges_truncated: true`, and the page says there are more without a
 * number. A null total with no truncation flag, or with a false one, is a
 * read that says neither, and fails here.
 */
function edgeCount(payload: Json, shown: number): { edgeTotal: number | null; edgesTruncated: boolean } {
  const flag = payload.edges_truncated;
  if (flag !== undefined && typeof flag !== "boolean") throw new SearchResponseError("raw edges truncated flag");
  if (payload.edge_total === null) {
    if (flag !== true) throw new SearchResponseError("raw edge total");
    return { edgeTotal: null, edgesTruncated: true };
  }
  const total = asCount(payload.edge_total, "raw edge total");
  return { edgeTotal: total, edgesTruncated: flag ?? total > shown };
}

/**
 * The server's `not_retained` details this page can say in words. The
 * detail itself is still printed, once, muted, by `refusalDetail`.
 */
const NOT_RETAINED_WORDS: Readonly<Record<string, string>> = {
  "this source stores a record-set cursor, not record content; only an indexed-v2 source authority retains a record's own payload":
    "this source stores a pointer to a batch of records, not each record's own content",
};

/** `not_retained` details the server already writes for a reader. */
const NOT_RETAINED_PLAIN: ReadonlySet<string> = new Set([
  "the original payload is kept only for sources that store records individually; this source does not",
]);

/** The server's own detail, when the sentence put it in words; null when
 *  the sentence already carries it verbatim. */
export function refusalDetail(refusal: RawRefusal): string | null {
  return refusal.reason === "not_retained" && NOT_RETAINED_WORDS[refusal.detail]
    ? refusal.detail
    : null;
}

/** One sentence per refusal code, naming the condition that fired.
 *
 *  `detail` is printed in every branch. It is put in words only where
 *  `NOT_RETAINED_WORDS` knows it -- "indexed-v2 source authority" is the
 *  runtime's vocabulary, not the reader's (review 2026-09-22) -- and then the
 *  verbatim detail is printed once beside it, muted (`refusalDetail`). The
 *  composer emits `not_retained` for seven different causes -- no source, no
 *  source-record identity, no authority row, a record-set cursor rather than
 *  record content, a payload past the byte ceiling, stored bytes larger than
 *  the recorded count, and bytes recorded as JSON that are not valid UTF-8
 *  JSON -- and the detail string is the only thing that tells them apart.
 *
 *  An unknown code throws rather than falling through to a generic
 *  sentence, so a code added to the composer surfaces as a failed read
 *  instead of quietly reading like one of the six this page knows. */
export function refusalSentence(refusal: RawRefusal): string {
  switch (refusal.reason) {
    case "not_found":
      return `This version holds no such record, so it retains no payload: ${refusal.detail}.`;
    case "policy_refused":
      return `The retained payload is withheld by policy ${refusal.policy ?? "(the read named none)"}: `
        + `${withoutPolicyPrefix(refusal.detail, refusal.policy)}.`;
    case "not_retained":
      // The server's own sentence where it is already plain words: the page
      // prints it once, and needs no muted "the read says" line beside it.
      if (NOT_RETAINED_PLAIN.has(refusal.detail)) {
        return `${refusal.detail.charAt(0).toUpperCase()}${refusal.detail.slice(1)}.`;
      }
      return `No payload is kept for this record: ${NOT_RETAINED_WORDS[refusal.detail] ?? refusal.detail}.`;
    case "payload_not_pinned_to_version":
      return "A payload is retained, but it is not the one this version was built from, "
        + `so this read will not show it: ${refusal.detail}.`;
    case "payload_identity_ambiguous":
      return "More than one stored payload matches this record's identity, and this read "
        + `cannot tell which one is the record: ${refusal.detail}.`;
    case "payload_has_several_fields":
      return "This record's retained payload is several separate fields, and this read "
        + `will not present one of them as the record: ${refusal.detail}.`;
    default: {
      const unknown: never = refusal.reason;
      throw new SearchResponseError(`raw refusal reason ${String(unknown)}`);
    }
  }
}
