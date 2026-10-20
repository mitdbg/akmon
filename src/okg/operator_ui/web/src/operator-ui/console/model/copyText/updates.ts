// Updates and one attempt as Markdown.
import { type PageContext, code, fence, heading, outcomeCell, section, table } from "@operator-ui/console/model/copyText/shared";
import { exact } from "@operator-ui/console/model/shared/format";
import type { AttemptDetail } from "@operator-ui/console/model/updates/detail";
import { DELTA_HEADER, type RecordedDetail } from "@operator-ui/console/model/updates/recorded";
import type { UpdatesModel } from "@operator-ui/console/model/updates/list";

// --- Updates --------------------------------------------------------------

/**
 * The producer runs the DETAIL read carries, as the page's own block.
 *
 * Emitted only where `ProducerList` is drawn. The attempt page draws it when
 * the attempt-detail read has NOT answered with runs, and draws the recorded
 * runs instead when it has; the Updates page's inline block always draws it,
 * because it issues no detail read. Copying both handed the reader two
 * "## Producer runs" headings over one block on screen, and the second copy
 * carried `sourcesNote` -- "Source runs are recorded on their own..." -- a
 * sentence the page in that state never printed.
 */
function producerRunsSection(detail: AttemptDetail, level: number): string[] {
  return section("Producer runs", [
    ...(detail.runningNote ? [detail.runningNote] : []),
    ...(detail.viewRunsNote ? [detail.viewRunsNote] : []),
    ...detail.viewRuns.flatMap((run) => [
      `- ${code(run.name)}: ${run.mark.word}${run.timing ? ` \u2014 ${run.timing}` : ""}`,
      ...(run.errorText ? [fence(run.errorText)] : []),
    ]),
    detail.sourcesNote,
  ], level);
}

/** The recorded blocker, which every state of the page draws when there is
 *  one: `BlockedBlock` is outside the branch that chooses a producer list. */
function blockedSection(detail: AttemptDetail, level: number): string[] {
  if (!detail.blockedText && !detail.blockedOversizeLine) return [];
  return section("Recorded blocker", [
    ...(detail.code ? [`Code: ${code(detail.code)}`, ""] : []),
    ...(detail.blockedOversizeLine ? [detail.blockedOversizeLine, ""] : []),
    ...(detail.blockedText ? [fence(detail.blockedText)] : []),
  ], level);
}

/** The attempt's facts. `level` is the heading level of its sections: 2 on
 *  its own page, 3 under "Newest blocked attempt" on the Updates page.
 *  `producerRuns` says whether this state of the page drew `ProducerList`. */
function attemptBody(
  detail: AttemptDetail,
  level: number,
  { producerRuns }: { producerRuns: boolean },
): string[] {
  return [
    "",
    `Attempt: ${code(detail.fullId)}`,
    `${detail.outcome.word}${detail.headline.startsWith(detail.outcome.word) ? detail.headline.slice(detail.outcome.word.length) : ` ${detail.headline}`}`.trim(),
    detail.meta.join(" \u00b7 "),
    ...(detail.change ? [detail.change] : []),
    ...(detail.excludedLine ? [detail.excludedLine] : []),
    ...(detail.reading ? ["This is the version being read."] : []),
    ...(producerRuns ? producerRunsSection(detail, level) : []),
    ...blockedSection(detail, level),
  ];
}

/**
 * The recorded sections, as Markdown.
 *
 * Only what the page shows. A section the page cut is absent here too: a
 * "Copy as text" that handed a coding agent a heading the reader never saw
 * would be describing a different page.
 */
function recordedSections(recorded: RecordedDetail, level: number): string[] {
  const lines: string[] = [];
  if (recorded.holdsLine || recorded.movedLine || recorded.holdsNotes.length) {
    lines.push(...section("What this version holds", [
      ...(recorded.holdsLine ? [recorded.holdsLine] : []),
      ...(recorded.movedLine ? [recorded.movedLine] : []),
      ...recorded.holdsNotes,
    ], level));
  }
  // Once, above the tables it qualifies -- where the page prints it. It is
  // one fact about the attempt, not one fact about each table, and repeating
  // it per table hands a coding agent three claims where the page made one.
  if (recorded.unpublishedNote) lines.push("", recorded.unpublishedNote);
  for (const countTable of recorded.tables) {
    lines.push(...section(countTable.title, [
      ...(countTable.refusedNote ? [countTable.refusedNote] : []),
      ...(countTable.parentNote ? [countTable.parentNote] : []),
      ...(countTable.rows.length
        ? [table(["Name", "At this version", DELTA_HEADER], countTable.rows.map(
          (row) => [code(row.name), row.count, row.delta ?? (row.parentNote ?? "")],
        ))]
        : []),
      ...(countTable.capLine ? [countTable.capLine] : []),
    ], level));
  }
  if (recorded.checksNote || recorded.gates.length) {
    lines.push(...section("Publication checks", [
      ...(recorded.checksNote ? [recorded.checksNote] : []),
      ...(recorded.checksSummary ? [recorded.checksSummary] : []),
      ...recorded.gates.map(
        (gate) => `- ${gate.name}${gate.code && gate.code !== gate.name ? ` (\`${gate.code}\`)` : ""}: ${gate.mark.word}`
          + `${gate.detail ? ` \u2014 ${gate.detail}` : ""}`
          + `${gate.withheldNote ? ` ${gate.withheldNote}` : ""}`,
      ),
    ], level));
  }
  if (recorded.showRuns) {
    lines.push(...section("Producer runs", [
      ...(recorded.runsNote ? [recorded.runsNote] : []),
      ...(recorded.runsCapLine ? [recorded.runsCapLine] : []),
      ...recorded.runs.flatMap((run) => [
        `- ${code(run.name)}: ${run.mark.word}${run.timing ? ` \u2014 ${run.timing}` : ""}`,
        ...(run.errorText ? [fence(run.errorText)] : []),
      ]),
    ], level));
  }
  lines.push(...section("Where this attempt sits", [
    ...(recorded.positionLine ? [recorded.positionLine] : []),
    ...(recorded.parentLine ? [recorded.parentLine] : []),
  ], level));
  if (recorded.configuredLines.length) {
    lines.push(...section("Configured when it ran", [
      ...recorded.configuredLines,
      ...recorded.configuredAbsences,
    ], level));
  }
  return lines;
}

/** `recorded` is absent wherever the attempts/detail read is: the Updates
 *  page's inline block issues no such read, and the copy it hands over says
 *  only what that page showed. */
export function attemptText(
  detail: AttemptDetail,
  context: PageContext,
  recorded: RecordedDetail | null = null,
): string {
  const lines = heading(`Attempt ${detail.attemptId}`, context);
  // `recorded` non-null is exactly the state in which `ConsoleAttemptDetail`
  // renders no `ProducerList` -- it is passed only where `recordedRuns` is
  // true. One block on screen, one "## Producer runs" here.
  lines.push(...attemptBody(detail, 2, { producerRuns: recorded === null }));
  if (recorded) lines.push(...recordedSections(recorded, 2));
  lines.push("");
  return lines.join("\n");
}

export function updatesText(
  model: UpdatesModel,
  inline: AttemptDetail | null,
  context: PageContext,
): string {
  const lines = heading("Updates", context);
  if (model.noAttemptsSentence) {
    lines.push("", model.noAttemptsSentence);
    if (model.configuredSentence) lines.push(`${model.configuredSentence} ${model.configuredLink ?? ""}`.trim());
    lines.push("");
    return lines.join("\n");
  }
  // No "N attempts recorded \u00b7 newest X ago": the header on screen prints
  // nothing beside the title, and the chip line under it is the same N. A copy
  // that carried a sentence the page had cut would describe a different page.
  lines.push(
    "",
    model.chips.map((chip) => `${exact(chip.value)} ${chip.label.toLowerCase()}${chip.note ? ` (${chip.note})` : ""}`).join(" \u00b7 "),
  );
  const filters = [
    model.filter.outcome ? `outcome ${model.filter.outcome}` : "",
    model.filter.producer ? `producer ${code(model.filter.producer)}` : "",
    model.filter.attempt ? `attempt ${code(model.filter.attempt)}` : "",
  ].filter(Boolean);
  if (filters.length) lines.push(`Filtered: ${filters.join(", ")}`);

  lines.push(...section("Attempts", model.noMatchSentence
    ? [model.noMatchSentence]
    : [
      // The page's four columns, and no fifth. There WAS an Attempt column
      // beside Version holding `row.fullId`: on a published attempt the
      // generation id IS the version id, so the two cells printed the same
      // string twice, and the table on screen has no such column. No Duration
      // column either -- nothing records one.
      //
      // A repeat group's members go in the Outcome cell, which is where the
      // page puts them: that row spans Outcome and Version and holds the
      // member's own id.
      table(["When", "Outcome", "Version", "Change"], model.rows.flatMap((row) => [
        [
          row.when,
          `${outcomeCell(row, row.stoppedOn)}${row.repeat ? ` ${row.repeat}` : ""}`,
          row.versionFullId ? `${code(row.versionFullId)}${row.reading ? " (reading)" : ""}` : "",
          row.change,
        ],
        ...row.members.map((member) => [
          member.when, code(member.fullId), "", "",
        ]),
      ])),
      ...(model.shownSentence ? ["", model.shownSentence] : []),
    ]));

  if (inline) {
    lines.push("", "## Newest blocked attempt", ...attemptBody(inline, 3, { producerRuns: true }));
  }
  lines.push("");
  return lines.join("\n");
}
