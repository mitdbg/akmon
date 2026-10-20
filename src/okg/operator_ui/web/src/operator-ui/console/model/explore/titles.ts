// What a record is CALLED on a results page.
//
// The server labels a chunk "Document Chunk · e734d84ffd5e9620", which is the
// record's own hash and tells an operator nothing about what they found. The
// same row carries `source_record_id` -- on the twiki-eos reader
// `{path: "HcalIsoTrackValidationSim.txt", chunk_index: 0}` -- and that is the
// document the words are in and where in it they are.
//
// So: the path is the heading, the position is the row, consecutive chunks of
// one document are one group, and a record whose source records no path keeps
// the label the server served. Nothing here invents a name.
import type { RecordOrigin, RecordSummary } from "@operator-ui/console/api/searchData";
import { sha1Hex } from "@operator-ui/console/model/shared/digest";

/** The heading for a record: what the source calls the document it came from,
 *  else the label the server served. */
export function recordHeading(summary: Pick<RecordSummary, "label" | "origin">): string {
  return summary.origin.title ?? summary.origin.path ?? summary.label;
}

/**
 * Where in its document a chunk sits, as a phrase, or null when the record is
 * not a chunk.
 *
 * "chunk 3" when only the index is served, "chunk 3 of 9" when the parent's
 * count is served too. A search row carries the index and not the count, so
 * the second form is a promise this page keeps only when it can.
 */
export function chunkPosition(origin: RecordOrigin): string | null {
  if (origin.chunkIndex === null) return null;
  const at = origin.chunkIndex + 1;
  return origin.chunkCount === null ? `chunk ${at}` : `chunk ${at} of ${origin.chunkCount}`;
}

/** What one row is titled: its position in its document when it is a chunk,
 *  else the heading. */
export function rowTitle(summary: Pick<RecordSummary, "label" | "origin">): string {
  return chunkPosition(summary.origin) ?? recordHeading(summary);
}

/** A run of rows the page prints under one heading. */
export interface TitledGroup<Row> {
  /** The heading over the rows. */
  heading: string;
  /** True when the rows are chunks of one document, so the heading is the
   *  document and each row is a position in it. A group of one whole record
   *  prints its heading as the row itself and this is false. */
  chunks: boolean;
  rows: Row[];
}

/**
 * Group the chunks of one document under one heading, across the page.
 *
 * In the order each document is FIRST seen, so the best-ranked document
 * leads and a document's later chunks join it. This used to group
 * consecutive rows only, on the grounds that gathering would put a worse
 * match above a better one; on Archiv3 the page for "muon" drew
 * MuonRun2DetectorRecoReview.txt as five separate groups with other files
 * between them (review 2026-09-22), and the reader had to assemble one
 * document from five places. The groups keep rank order among themselves
 * and inside each group. A record that is not a chunk of a named document is
 * its own group, where it was ranked.
 */
export function groupByDocument<Row extends { hit: Pick<RecordSummary, "label" | "origin"> }>(
  rows: readonly Row[],
): TitledGroup<Row>[] {
  const groups: TitledGroup<Row>[] = [];
  const byHeading = new Map<string, TitledGroup<Row>>();
  for (const row of rows) {
    const origin = row.hit.origin;
    const isChunk = origin.chunkIndex !== null && (origin.path !== null || origin.title !== null);
    const heading = recordHeading(row.hit);
    const found = isChunk ? byHeading.get(heading) : undefined;
    if (found) {
      found.rows.push(row);
      continue;
    }
    const group: TitledGroup<Row> = { heading, chunks: isChunk, rows: [row] };
    groups.push(group);
    if (isChunk) byHeading.set(heading, group);
  }
  return groups;
}

/**
 * A chunk's position as the RECORD page says it: "Part 3", "Part 3 of 9".
 *
 * The same fact as `chunkPosition` in the word the record page already uses
 * for it above its content, so the heading and the line under it agree.
 */
export function partPosition(origin: RecordOrigin): string | null {
  const position = chunkPosition(origin);
  return position === null ? null : position.replace(/^chunk /, "Part ");
}

/**
 * What the RECORD page is titled.
 *
 * The same rule as a results row, drawn on one line: the document the words
 * are in, then where in it this record sits. The server's label for a chunk
 * is its own hash -- "Document Chunk · e734d84ffd5e9620" -- which named the
 * page one click after a result titled "HcalIsoTrackValidationSim.txt ·
 * chunk 2". The hash is still on the page, in the meta line with its copy
 * control, where an id belongs.
 *
 * A record whose source records no path keeps the label the server served,
 * and a position with no document to hang it on is not printed: "Part 3" on
 * its own names nothing.
 */
export function recordTitle(summary: Pick<RecordSummary, "label" | "origin">): string {
  const heading = recordHeading(summary);
  const origin = summary.origin;
  const named = origin.path !== null || origin.title !== null;
  const part = named ? partPosition(origin) : null;
  return part === null ? heading : `${heading} · ${part}`;
}

/**
 * The tail of a record id, short enough to read beside a title.
 *
 * Only the part after the last colon is the record's own name -- what comes
 * before it is the type and the source that made it -- and a hash tail is cut
 * to eight characters, which is how long a generation id is shown in the
 * header. A tail that is NOT a hash is left whole: `deployment.yaml` cut to
 * `deployme` names nothing.
 *
 * This is the id as an id. What a colliding TITLE gets appended to it is
 * `recordIdSuffix` below, which is a different question.
 */
export function shortRecordId(nodeId: string): string {
  const tail = nodeId.slice(nodeId.lastIndexOf(":") + 1);
  return /^[0-9a-f]{12,}$/i.test(tail) ? tail.slice(0, 8) : tail;
}

/**
 * What separates two rows a title alone does not tell apart.
 *
 * `shortRecordId` was doing this job and it does not fit. For
 * `doc:deployment_configs:kubernetes-smoke/deployment.yaml` the tail after
 * the last colon IS the title, so the row drew
 * "kubernetes-smoke/deployment.yaml · kubernetes-smoke/deployment.yaml": the
 * suffix said nothing, took a line's width to say it, and the rows still read
 * the same. Where that repetition collided the list then printed the WHOLE
 * internal id in a results title -- `doc:repo_a:deployment.yaml` -- which is
 * the one place this console has decided an id does not belong.
 *
 * So: the record's own hash tail where the id has one, because that is the
 * name a reader would copy; otherwise the type prefix and eight hex of a
 * digest of the FULL id, which is short, stable across builds and different
 * for every id, and reads as what it is rather than as a path. Never the
 * title back again -- a chunk the server labels with its own hash would
 * otherwise get that hash twice.
 */
export function recordIdSuffix(nodeId: string, title: string): string {
  const tail = nodeId.slice(nodeId.lastIndexOf(":") + 1);
  const type = nodeId.slice(0, nodeId.indexOf(":"));
  if (/^[0-9a-f]{12,}$/i.test(tail) && !title.startsWith(tail.slice(0, 8))) {
    return tail.slice(0, 8);
  }
  return `${type || "id"}:${sha1Hex(nodeId).slice(0, 8)}`;
}

/** What one row of a list contributes to the question "does any other row
 *  read the same?": its title, the record behind it, and the heading it sits
 *  under, if any. */
export interface ListedRow {
  title: string;
  /** The record the row points at. Two rows for ONE record are not a
   *  collision: an id would repeat rather than separate them. */
  id: string;
  /** A heading already printed above this row. Rows under different headings
   *  do not read the same even when their titles match -- two documents each
   *  matched at their own "chunk 1". */
  scope?: string;
}

/**
 * Every row's title, with the record id appended to the rows a title alone
 * does not tell apart.
 *
 * A list must not show two rows that read the same. On `lane-d-home-render`
 * the doc_corpus reader records `{path, repo, chunk_id}` and no position, so
 * both chunks of one file title as the file, and a document's Related tab
 * drew four rows reading "beaver-okg/deployment.yaml". Nothing here invents a
 * position it was not given; it appends what the read already served.
 *
 * Only the colliding rows are touched. A title unique in its list stays as it
 * is, and the page a row opens keeps its own title -- the id is in its meta
 * line, not in its heading -- so following a row does not rename the record.
 *
 * The suffix is `recordIdSuffix`, which never repeats the row's own title and
 * never prints a full internal id. The whole id is the last resort and is now
 * reachable only on a digest collision: it used to be reached by every pair
 * of rows for one path recorded by two sources, because their tails were
 * equal.
 */
export function titlesInList<Row>(
  rows: readonly Row[], read: (row: Row) => ListedRow,
): string[] {
  const listed = rows.map(read);
  const groups = new Map<string, number[]>();
  listed.forEach((row, index) => {
    const key = `${row.scope ?? ""}\u0000${row.title}`;
    const at = groups.get(key);
    if (at) at.push(index); else groups.set(key, [index]);
  });
  const titles = listed.map((row) => row.title);
  for (const indexes of groups.values()) {
    const ids = new Set(indexes.map((index) => listed[index].id));
    if (ids.size < 2) continue;
    const shortened = new Set(
      indexes.map((index) => recordIdSuffix(listed[index].id, listed[index].title)),
    );
    const whole = shortened.size < ids.size;
    for (const index of indexes) {
      const row = listed[index];
      titles[index] = `${row.title} · ${whole ? row.id : recordIdSuffix(row.id, row.title)}`;
    }
  }
  return titles;
}
