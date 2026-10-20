// Explore's landing, results and record page as Markdown.
import type { IdentityField } from "@operator-ui/console/model/explore/identity";
import { type PageContext, code, fence, heading, section, table } from "@operator-ui/console/model/copyText/shared";
import { ignoredSentence } from "@operator-ui/console/model/explore/params";
import { contentsLine, nothingToSearchSentence } from "@operator-ui/console/model/explore/landing";
import { payloadText, prettyJson, refusalDetail, refusalSentence } from "@operator-ui/console/model/explore/raw";
import type { RecordPageModel } from "@operator-ui/console/model/explore/record";
import { exact } from "@operator-ui/console/model/shared/format";
import { utcStamp } from "@operator-ui/console/model/shared/time";
import type { ExplorePageProps } from "@operator-ui/console/views/Explore";

// --- Explore ---------------------------------------------------------------

/** The copied table carries the columns the read serves and no others. Three
 *  columns went with the fields behind them -- Updated, Related and Open in
 *  source -- because an always-empty column in copied text reads as "this
 *  record has none", not as "this read does not say". */
function hitCells(row: { hit: { label: string; type: string; source: string | null; nodeId: string; match: string; excerpt: string | null } }): string[] {
  const hit = row.hit;
  return [
    hit.label,
    hit.type,
    hit.source ?? "",
    code(hit.nodeId),
    (hit.excerpt ?? "").replace(/\[nomos:masked\]/g, "hidden"),
  ];
}

const HIT_HEADERS = ["Label", "Type", "Source", "Record id", "Snippet"];

export function exploreText(page: ExplorePageProps): string {
  const lines = heading("Explore", page.context);
  if (!page.version) {
    lines.push("", nothingToSearchSentence(page.deployment), "");
    return lines.join("\n");
  }
  lines.push("", contentsLine(page.types, page.version));
  if (page.params.q) lines.push(`Query: ${JSON.stringify(page.params.q)}`);
  if (page.results) {
    const filters = page.results.filters;
    const applied = [
      filters.type ? `type ${code(filters.type)}` : "",
      filters.source ? `source ${code(filters.source)}` : "",
    ].filter(Boolean);
    if (applied.length) lines.push(`Filtered: ${applied.join(", ")}`);
    // A parameter the read refuses travels with the copied text too, or the
    // copy quietly claims a relevance ranking the reader did not ask for.
    const ignored = ignoredSentence(page.params.ignored, true);
    if (ignored) lines.push(ignored);
  } else if (page.params.type) {
    lines.push(`Type: ${code(page.params.type)}`);
  }

  if (page.chips && !page.results) {
    const chips = page.chips;
    lines.push(...section("Record types", [
      table(["Type", "Records"], chips.chips.map((chip) => [code(chip.type), chip.count === null ? "" : exact(chip.count)])),
      ...(chips.moreTypes ? ["", `and ${exact(chips.moreTypes)} more types \u00b7 ${exact(chips.moreRecords)} records`] : []),
      ...(chips.countNote ? ["", chips.countNote] : []),
    ]));
  }
  if (page.results) {
    const results = page.results;
    // The left-out profile types travel with the copy on a page with results
    // and on one with none: the screen shows the line on both.
    const skipped = results.skippedTypes
      ? [results.skippedTypes.line, `Not in this version: ${results.skippedTypes.names.join(", ")}.`]
      : [];
    lines.push(...section("Results", results.noMatchSentence
      ? [
        ...skipped,
        results.noMatchSentence,
        ...(results.broaden.length ? [`Try: ${results.broaden.map((action) => action.label).join(", ")}`] : []),
      ]
      : [
        results.aboutLine,
        ...(results.unappliedNote ? [results.unappliedNote] : []),
        ...skipped,
        "",
        ...(results.idMatches.length
          ? ["ID match", "", table(HIT_HEADERS, results.idMatches.map(hitCells)), ""]
          : []),
        ...(results.labelMatches.length
          ? ["Label match", "", table(HIT_HEADERS, results.labelMatches.map(hitCells)), ""]
          : []),
        ...(results.textMatches.length && (results.idMatches.length || results.labelMatches.length)
          ? ["Text match", ""]
          : []),
        table(HIT_HEADERS, results.textMatches.map(hitCells)),
        ...(results.paging.kind !== "none" ? ["", results.paging.note] : []),
        "",
        "The same search from the CLI:",
        fence(results.cli),
      ]));
    // Every rail section is a facet section now: sort, time and artifact were
    // cut with the reads that could not serve them, so there is nothing to skip.
    for (const rail of results.rail) {
      lines.push(...section(`${rail.title} facets`, [
        table([rail.title, "Records"], rail.options.map((option) => [
          `${code(option.value)}${option.selected ? " (checked)" : ""}`, option.count === null ? "" : exact(option.count),
        ])),
        ...(rail.more ? [`and ${exact(rail.more)} more`] : []),
        ...(rail.note ? [rail.note] : []),
      ], 3));
    }
  }
  if (page.listing) {
    const listing = page.listing;
    lines.push(...section(`Records of type ${listing.type}`, listing.emptySentence
      ? [listing.emptySentence]
      : [
        listing.countLine,
        "",
        table(["Label", "Type", "Source", "Record id"], listing.rows.map(({ summary }) => [
          summary.label, summary.type, summary.source ?? "", code(summary.nodeId),
        ])),
        ...(listing.shortPageNote ? ["", listing.shortPageNote] : []),
      ]));
  }
  lines.push("");
  return lines.join("\n");
}

/** The identity the Provenance tab draws as rows, as one line of text.
 *
 *  The same rows, so what is copied is what is on the page: an unlabelled
 *  identity is the value alone, and a keyed one is "Label value" pairs. The
 *  full value is used, never the shortened hash the row shows. */
function identityText(rows: IdentityField[]): string {
  if (!rows.length) return "not recorded by the source";
  if (rows.length === 1 && !rows[0].label) return code(rows[0].full);
  return rows.map((row) => `${row.label} ${code(row.full)}`).join(" · ");
}

export function recordText(model: RecordPageModel, context: PageContext): string {
  // Headed by what the page is headed by -- the document and the position in
  // it -- not by the record's hash. The hash is the line below, as it is in
  // the meta line on the page.
  const lines = heading(model.title, context);
  lines.push(
    "",
    ...(model.label === model.title ? [] : [`Label: ${model.label}`]),
    `Record: ${code(model.id)}`,
    `Type: ${code(model.type)}`,
    `Source: ${model.source ? code(model.source) : "no source recorded"}`,
    ...(model.openInSource ? [`Open in source: ${model.openInSource}`] : []),
    `Read from: ${code(model.version)}${model.publishedAt ? `, published ${utcStamp(model.publishedAt)}` : ""}`,
    ...(model.newerVersionSentence ? [model.newerVersionSentence] : []),
    ...(model.noTextSentence ? [model.noTextSentence] : []),
    ...(model.maskedPaths ? [model.maskedPaths.sentence, `Rule: ${model.maskedPaths.rule}`] : []),
  );

  lines.push(...section(`Fields (${exact(model.fieldCount)})`, model.fieldCount
    ? [table(["Field", "Value"], model.fields.map((field) => [
      code(field.key),
      field.hidden
        ? "hidden by policy"
        : (field.value ?? "").replace(/\[nomos:masked\]/g, "hidden"),
    ]))]
    : ["The read returned no fields for this record."]));

  if (model.tabs.includes("content")) {
    const content = model.content;
    const body: string[] = [];
    if (content.partOf) body.push(`Part of: ${content.partOf.label} ${code(content.partOf.id)}`, "");
    if (content.toolbar) {
      body.push(
        `Artifact: ${code(content.toolbar.fileName)} · ${content.toolbar.format} · ${exact(content.toolbar.byteCount)} bytes`,
        `SHA-256: ${code(content.toolbar.sha256)}`,
        `Original: ${content.toolbar.url}`,
      );
    }
    if (content.absenceSentence) body.push(content.absenceSentence);
    body.push(...content.capSentences);
    if (content.extractedText !== null) {
      body.push("", "### Extracted text", "", fence(content.extractedText));
    }
    const readable = content.readable;
    if (readable) {
      // "Chunks", not "Content": this sits inside the "## Content" section
      // and used to head itself "### Content (2 chunks)", which reads as a
      // subsection of itself.
      body.push("", `### Chunks (${exact(readable.blocks.length)})`, "", readable.orderSentence);
      if (readable.overlapSentence) body.push(readable.overlapSentence);
      body.push("");
      for (const block of readable.blocks) {
        body.push(
          `- ${block.heading ? `${block.heading} · ` : ""}${block.placement}`
          + `${block.page !== null ? ` (page ${exact(block.page)})` : ""} ${code(block.nodeId)}`,
          "",
          `  ${block.text.replace(/\r?\n/g, "\n  ")}`,
          "",
        );
      }
    }
    lines.push(...section("Content", body));
  }

  const related = model.related;
  // One line per neighbour, not a table: the copy is for pasting into a
  // chat or a ticket, where a five-column table of ids reads worse than
  // "supersedes (outgoing): CMSSW_13_3_0 `cmssw_release:CMSSW_13_3_0`".
  lines.push(...section(`Related (${exact(model.relatedCount)}${model.relatedCountIsFloor ? "+" : ""})`, [
    related.summary ?? related.countLine,
    ...related.capSentences,
    "",
    ...(related.emptySentence
      ? [related.emptySentence]
      : related.rows.map(({ record, relationship, direction }) => (
        `- ${relationship}${related.showDirection ? ` (${direction})` : ""}: `
        + `${record.label} ${code(record.nodeId)}`
      ))),
  ]));

  const provenance = model.provenance;
  // One line. The source's lineage used to be copied again with every edge
  // row of the Raw block, which made an 8-field record 15 KB of text.
  lines.push("", `Provenance: source ${provenance.source ? code(provenance.source) : "not recorded"}`
    + ` · record id ${identityText(provenance.sourceRecordId)}`
    + ` · revision ${identityText(provenance.sourceRevision)}`
    + `${provenance.policyLine ? ` · policy: ${provenance.policyLine}` : ""}`);

  // Raw only when the reader is looking at it: it is the stored rows and
  // their lineage, which is what made the copy unusable anywhere else.
  const raw = model.tab === "raw" ? model.raw : undefined;
  if (raw !== undefined) lines.push(...section("Raw", raw
    ? [
      ...(raw.maskedPaths ? [raw.maskedPaths.sentence, `Rule: ${raw.maskedPaths.rule}`, ""] : []),
      "### Stored row",
      "",
      raw.row === null ? "No stored row was returned for this record." : fence(prettyJson(raw.row), "json"),
      "",
      `### Edge rows (${exact(raw.edges.length)}${raw.edgesTruncated ? (raw.edgeTotal === null ? ", and more" : ` of ${exact(raw.edgeTotal)}`) : ""})`,
      "",
      raw.edges.length ? fence(prettyJson(raw.edges.map((edge) => edge.row)), "json") : "No edge row is stored for this record.",
      "",
      "### Retained payload",
      "",
      ...(raw.payload
        ? [
          `${exact(raw.payload.byteCount)} bytes · SHA-256 ${code(raw.payload.sha256)} · ${raw.payload.url}`,
          `Pinning basis ${code(raw.payload.pinningBasis)}. ${raw.payload.pinningCaveat}`,
          "",
          fence(payloadText(raw.payload), "json"),
        ]
        : [raw.refusal ? refusalSentence(raw.refusal) : "No retained payload: the read gave no reason.",
          ...(raw.refusal && refusalDetail(raw.refusal) ? [`The read says: ${refusalDetail(raw.refusal)}.`] : [])]),
    ]
    : ["This read returned no raw layers: the record detail read does not serve the stored row, the edge rows or the retained payload yet."]));
  // No CLI transcript. It is cut from the record page (`ui/cuts.md`), and a
  // copy offering a section the page does not have is a second account of
  // the record.
  lines.push("");
  return lines.join("\n");
}
