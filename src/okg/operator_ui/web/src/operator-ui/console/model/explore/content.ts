// The Content tab: which viewer a media type gets, and the model of what the
// tab shows or says is absent.
import { type ArtifactRead, artifactAbsenceSentence, withoutPolicyPrefix } from "@operator-ui/console/model/explore/raw";

// Re-exported from its new home so the cycle content -> raw -> content is
// gone; raw.ts owns it because the refusal sentences are there.
export { withoutPolicyPrefix };
import type { ChunkCaps, ChunkRow, ExploreRecord, ExploreSection } from "@operator-ui/console/model/explore/record";
import { partPosition } from "@operator-ui/console/model/explore/titles";
import { parseOrigin as identityOrigin } from "@operator-ui/console/api/searchData";
import { exact } from "@operator-ui/console/model/shared/format";

// --- media-type routing --------------------------------------------------------

export type ViewerKind = "pdf" | "markdown" | "code" | "html" | "image" | "none";

/** The language a code viewer highlights with, from the record's `format`
 *  and the path's extension. Null is plain text with line numbers. */
export const LANGUAGE_BY_EXTENSION: Record<string, string> = {
  rs: "rust", py: "python", ts: "typescript", tsx: "typescript", js: "javascript", mjs: "javascript",
  json: "json", yaml: "yaml", yml: "yaml", toml: "toml", html: "html", htm: "html",
  sh: "bash", bash: "bash", csv: "csv", md: "markdown",
};

const LANGUAGE_BY_FORMAT: Record<string, string> = {
  json: "json", yaml: "yaml", toml: "toml", csv: "csv",
};

/** Which viewer renders an artifact. The media type decides; `format` and
 *  the extension refine code. Anything else is "none": the toolbar's Open
 *  and Download are the viewer. */
export function viewerFor(
  artifact: { mediaType: string; format: string | null; path: string | null },
): { kind: ViewerKind; language: string | null } {
  const media = artifact.mediaType.split(";")[0].trim().toLowerCase();
  const extension = (artifact.path ?? "").split(".").pop()?.toLowerCase() ?? "";
  const format = artifact.format ?? "";
  if (media === "application/pdf") return { kind: "pdf", language: null };
  if (media === "text/markdown" || format === "markdown") return { kind: "markdown", language: null };
  if (media === "text/html" || format === "html") return { kind: "html", language: null };
  if (media.startsWith("image/") || format === "image") return { kind: "image", language: null };
  const language = LANGUAGE_BY_FORMAT[format]
    ?? (format === "code" || media.startsWith("text/") || media === "application/json"
      ? LANGUAGE_BY_EXTENSION[extension] ?? (media === "application/json" ? "json" : null)
      : undefined);
  if (language !== undefined) return { kind: "code", language };
  return { kind: "none", language: null };
}

/** "1.2 KB", "358 KB", "2.4 MB": a byte count a person reads. The exact
 *  count goes in the title. */
export function bytesLabel(bytes: number): string {
  if (bytes < 1024) return `${exact(bytes)} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(bytes < 10 * 1024 ? 1 : 0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

// --- Content --------------------------------------------------------------------

export interface ContentToolbar {
  format: string;
  byteCount: number;
  bytesLabel: string;
  sha256: string;
  url: string;
  fileName: string;
}

export interface ContentModel {
  /** The artifact read, or null when the page made none (the live read
   *  does not serve it yet, backend change 4). */
  artifact: ArtifactRead | null;
  /** One sentence naming why there is nothing to render, when the artifact
   *  is absent or the read was not served. Null when it renders. */
  absenceSentence: string | null;
  viewer: { kind: ViewerKind; language: string | null } | null;
  toolbar: ContentToolbar | null;
  /** The record's long text, its sections in order, or null when it has
   *  none. A hidden section is named as hidden inside the text. */
  extractedText: string | null;
  /** True when this record IS code or structured data, so its text is set in
   *  the monospace face. Everything else is prose and reads as prose: a
   *  twiki page in 13 px mono inside a grey box was the same words made
   *  harder to read. */
  isCode: boolean;
  chunks: ChunkRow[];
  /** The record's content as readable text, built from the chunks above, or
   *  null when the read listed none. */
  readable: ReadableContent | null;
  /** Every cap the record read reported on this record's chunks, in words:
   *  a capped list, and chunks it counted but could not order. Empty when
   *  it reported none, and absent entirely on a record carrying no chunk
   *  keys at all. */
  capSentences: string[];
  /** The document this chunk is part of, with the chunk's own position in
   *  it: "Part 3 of 9 of <document>", or "Part 3 of <document>" when no read
   *  says how many there are. Null when the record is not part of anything
   *  the read names. */
  partOf: {
    id: string;
    label: string;
    href: string;
    /** "Part 3 of 9" / "Part 3", or null when the record carries no
     *  position -- then the line is the old "part of <document>". */
    position: string | null;
  } | null;
  /** The href that opens a chunk's own page. */
  open: (id: string) => string;
}

// --- the record's content, as readable text -------------------------------------

/** One chunk, rendered as a block of the document's text. */
export interface ReadableBlock {
  nodeId: string;
  /** The heading path the read recorded for this chunk, or null when it
   *  recorded none. Nothing is inferred from the text. */
  heading: string | null;
  /** Where the read placed this block, in its own terms: the ordinal it
   *  stored, or the character the chunk starts at. */
  placement: string;
  /** The page the read recorded, or null. */
  page: number | null;
  text: string;
}

/** A record's content as something a person can read: the chunks the read
 *  listed, in the order it served them, with what it recorded about each. */
export interface ReadableContent {
  /** The ordering basis, printed once above the text. */
  orderSentence: string;
  /** One sentence when the offsets and lengths the read sent show that
   *  consecutive chunks overlap, so the text repeats what they share. Null
   *  when they do not overlap, or when the read recorded no offsets. */
  overlapSentence: string | null;
  /** The heading path of every block that recorded one, in served order.
   *  Empty when the read recorded no heading at all. */
  outline: string[];
  blocks: ReadableBlock[];
  /** Every block as one string -- what "Copy as text" yields, and the same
   *  text the blocks show, in the same order. */
  text: string;
}

/** How far consecutive chunks overlap, from the offsets and text lengths the
 *  read sent and from nothing else. Zero when it recorded no offset, because
 *  an unrecorded offset is not an overlap of zero. */
function overlapChars(blocks: ChunkRow[]): number {
  let total = 0;
  for (let index = 1; index < blocks.length; index += 1) {
    const before = blocks[index - 1];
    const here = blocks[index];
    if (before.charOffset === null || here.charOffset === null) continue;
    total += Math.max(0, before.charOffset + before.text.length - here.charOffset);
  }
  return total;
}

/** The record's content as readable text, or null when the read listed no
 *  chunk -- in which case the cap sentences are the whole statement. */
export function readableContent(chunks: ChunkRow[], caps: ChunkCaps | null): ReadableContent | null {
  if (!chunks.length || !caps?.orderSentence) return null;
  const blocks: ReadableBlock[] = chunks.map((chunk, index) => ({
    nodeId: chunk.nodeId,
    heading: chunk.headingPath,
    placement: chunk.ordinal !== null
      ? `chunk ${exact(chunk.ordinal)}`
      : chunk.charOffset !== null
        ? `character ${exact(chunk.charOffset)}`
        : `chunk ${exact(index + 1)} as served`,
    page: chunk.page,
    text: chunk.text,
  }));
  const overlap = overlapChars(chunks);
  return {
    orderSentence: caps.orderSentence,
    overlapSentence: overlap > 0
      ? `Consecutive chunks overlap by ${exact(overlap)} characters in total: the read `
        + "records where each one starts, and one begins inside the one before it, so "
        + "the text below repeats what they share."
      : null,
    outline: blocks.map((block) => block.heading).filter((heading): heading is string => heading !== null),
    blocks,
    text: blocks.map((block) => (block.heading ? `${block.heading}\n${block.text}` : block.text)).join("\n\n"),
  };
}

/**
 * What the Content tab says about the original file, in ONE sentence.
 *
 * `hasBody` is "this page is about to print the record's text", and it is the
 * whole of the test. It used to be the extracted text alone -- and the live
 * document's body is its CHUNKS, with `sections` empty, so on every
 * chunk-backed record "no text" was read as "no body" and the tab printed the
 * long absence sentence above two chunks of the very text it said it could
 * not show, with a second sentence under it quoting the same `detail` again.
 */
function absenceSentence(artifact: ArtifactRead | null, hasBody: boolean): string | null {
  if (!artifact) {
    return "The original file cannot be shown: this page made no artifact read.";
  }
  if (artifact.exists) return null;
  // The ordinary case, and the one the operator meets on nearly every record:
  // this deployment keeps the extracted text and not the file it came from.
  // That took two paragraphs and printed the read's `detail` twice -- once
  // under "No original file is retained", once under "What follows is the
  // text this deployment extracted". It is one clause before the body now.
  // The other four reasons are NOT ordinary and keep their own sentence,
  // including the detail the read gave.
  if (artifact.reason === "not_retained" && hasBody) {
    return "Extracted text; the original file is not retained.";
  }
  // Every code the composer can emit has its own sentence, and an unknown
  // one throws there rather than reading as a generic absence here. With no
  // body to introduce, this sentence is what the page has -- and it quotes
  // the read's own detail, so nothing further is owed.
  return artifactAbsenceSentence(artifact);
}

/** The sections as one text: each section's title as a line, then its
 *  text; a hidden one says so in place of its text. */
export function extractedText(sections: ExploreSection[]): string | null {
  if (!sections.length) return null;
  return sections.map((section) => {
    const body = section.hidden ? "[hidden by policy]" : (section.value ?? "");
    return sections.length === 1 ? body : `${section.title}\n${body}`;
  }).join("\n\n");
}

// `extractionNote` stood here: a SECOND sentence about the same missing
// original, "The artifact read gives the reason: <detail>." There is no state
// left that owes the reader one. Where the page has a body, the one clause
// above it says it; where it has none, `artifactAbsenceSentence` already
// quotes that very `detail`. On the lane graph's chunk-backed documents both
// fired at once and the tab said it twice.

/** Whether this record's text is code or structured data, by what the record
 *  says it is -- its subtype, or a `format` field the source recorded.
 *
 *  By kind, never by looking at the text: a prose page that happens to
 *  contain a YAML block is still a prose page, and guessing from the bytes
 *  would flip the whole tab's typography on a record's content changing.
 */
const CODE_SUBTYPES = new Set([
  "code_file", "source_file", "config_file", "schema", "notebook",
]);
const CODE_FORMATS = new Set([
  "yaml", "yml", "json", "toml", "ini", "xml", "csv", "sql", "python",
  "typescript", "javascript", "rust", "go", "java", "c", "cpp", "shell", "bash",
  "code", "diff", "patch",
]);

export function isCodeByKind(detail: ExploreRecord): boolean {
  const subtype = detail.base.type.toLowerCase();
  if (CODE_SUBTYPES.has(subtype)) return true;
  const format = detail.fields.find((field) => field.key === "format" && !field.hidden)?.value;
  return typeof format === "string" && CODE_FORMATS.has(format.trim().toLowerCase());
}

export function buildContent(
  detail: ExploreRecord, artifact: ArtifactRead | null, open: (id: string) => string,
): ContentModel {
  const base = detail.base;
  // Either shape: the document `contains` the chunk (incoming here), or the
  // chunk is a `member_of` the document (outgoing here). The two readers on
  // the live graphs write one each, and a chunk that cannot name its document
  // is a page with no way back up.
  const container = base.incoming.find((row) => row.relationship === "contains")
    ?? base.outgoing.find((row) => row.relationship === "member_of")
    ?? null;
  const position = partPosition(identityOrigin(base.sourceRecordId));
  const text = extractedText(detail.sections);
  const chunks = detail.chunks ?? [];
  // The body the tab is about to print, from EITHER source.
  const hasBody = text !== null || chunks.length > 0;
  const exists = artifact?.exists ? artifact : null;
  return {
    artifact,
    absenceSentence: absenceSentence(artifact, hasBody),
    viewer: exists ? viewerFor(exists) : null,
    toolbar: exists
      ? {
        format: exists.format ?? exists.mediaType,
        byteCount: exists.byteCount,
        bytesLabel: bytesLabel(exists.byteCount),
        sha256: exists.sha256,
        url: exists.url,
        fileName: (exists.path ?? base.nodeId).split("/").pop() || base.nodeId,
      }
      : null,
    extractedText: text,
    isCode: isCodeByKind(detail),
    chunks,
    readable: readableContent(chunks, detail.chunkCaps),
    capSentences: detail.chunkCaps?.sentences ?? [],
    partOf: container
      ? {
        id: container.nodeId,
        label: container.label,
        href: open(container.nodeId),
        position,
      }
      : null,
    open,
  };
}

/** Content is a tab when there is something to show on it: a rendered or
 *  refused artifact, long text, or chunks. A record with nothing retained
 *  and no text has no Content tab rather than an empty one. */
export function hasContent(content: ContentModel): boolean {
  if (content.toolbar) return true;
  if (content.artifact && !content.artifact.exists && content.artifact.reason !== "not_retained") return true;
  // A live document returns `chunks: []` with `chunks_without_ordinal: 2`:
  // it HAS content the read counted and could not place. Without this the
  // tab is dropped and the only statement of that fact goes with it.
  if (content.capSentences.length > 0) return true;
  return content.extractedText !== null || content.chunks.length > 0;
}
