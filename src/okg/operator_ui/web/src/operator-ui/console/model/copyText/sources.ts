// Sources and one producer as Markdown.
import { type PageContext, code, fence, heading, section, table } from "@operator-ui/console/model/copyText/shared";
import { exact } from "@operator-ui/console/model/shared/format";
import type { ProducerDetail } from "@operator-ui/console/model/sources/detail";
import type { SourcesModel } from "@operator-ui/console/model/sources/list";
import type { SourceRow } from "@operator-ui/console/model/sources/rows";

// --- Sources --------------------------------------------------------------

function sourceRowCells(row: SourceRow, columns: SourcesModel["columns"], mixed: boolean): string[] {
  return [
    code(row.name),
    ...(mixed ? [row.kindWord || row.kind] : []),
    // The word a reader sees, and the code the read served beside it: a
    // pasted table that says only "Blocked" cannot be matched back to the
    // `blocked` in a payload or a log.
    row.configured ? `${row.mark.word} (${row.stateCode})` : "not configured",
    row.lastRunExact || row.lastRun,
    ...(columns.freshness ? [row.freshness] : []),
    ...(columns.contribution
      ? [
        row.records === null ? "" : exact(row.records),
        row.relationships === null ? row.relationshipsNote : exact(row.relationships),
      ]
      : []),
  ];
}

export function sourcesText(model: SourcesModel, shown: SourceRow[], context: PageContext): string {
  const lines = heading("Sources", context);
  if (model.total === 0) {
    lines.push("", `No producer is configured for ${model.deployment}.`, "");
    return lines.join("\n");
  }
  lines.push(
    "",
    model.headerCount,
    [...model.kindChips, ...model.stateChips]
      .map((chip) => `${exact(chip.value)} ${chip.label}`).join(" · "),
  );
  const filters = [
    model.filter.kind ? `kind ${model.filter.kind}` : "",
    model.filter.state ? `state ${model.filter.state}` : "",
    model.filter.name ? `name contains ${JSON.stringify(model.filter.name)}` : "",
  ].filter(Boolean);
  if (filters.length) lines.push(`Filtered: ${filters.join(", ")}`);
  if (model.sharedCause) lines.push(model.sharedCause);
  // Every sentence the page prints under the table. They were left out, so a
  // coding agent handed this text was told the states and not what they are a
  // reading OF, and was told a readiness verdict without being told it is a
  // bounded one. The copy is the page; `copyText.test.ts` holds it to that.
  for (const sentence of model.partialReads) lines.push(sentence);
  if (model.stateBasisNote) lines.push(model.stateBasisNote);
  if (model.readinessScopeNote) lines.push(model.readinessScopeNote);

  const headers = [
    "Name",
    ...(model.mixedKinds ? ["Kind"] : []),
    "State",
    "Last run",
    ...(model.columns.freshness ? ["Freshness"] : []),
    ...(model.columns.contribution ? ["Records", "Relationships"] : []),
  ];
  lines.push(...section("Producers", model.noMatchSentence
    ? [model.noMatchSentence]
    : [
      table(headers, shown.map((row) => sourceRowCells(row, model.columns, model.mixedKinds))),
      ...(shown.length < model.rows.length
        ? ["", `${exact(shown.length)} of ${exact(model.rows.length)} producers`]
        : []),
    ]));
  lines.push("", model.footer, "");
  return lines.join("\n");
}

export function producerText(detail: ProducerDetail, context: PageContext): string {
  const lines = heading(`Producer ${detail.name}`, context);
  lines.push(
    "",
    `Producer: ${code(detail.name)} (${detail.kind})`,
    detail.configured ? `State: ${detail.mark.word}` : "State: not configured",
    detail.meta.join(" \u00b7 "),
    ...(detail.stateCode ? [`Recorded as ${code(detail.stateCode)}.`] : []),
  );

  lines.push(...section("Contribution", [
    ...(detail.figures.length
      ? [detail.figures.map((figure) => `${exact(figure.value)} ${figure.label}`).join(" · ") + ` ${detail.figuresNote}`]
      : [detail.figuresNote]),
    // Two recorded counts that contradict each other, beside the figure they
    // are about, exactly as the page has it.
    ...(detail.countDisagreement ? [detail.countDisagreement] : []),
  ]));

  // "Recorded cause" and the incident count. The page prints both, in one
  // line under Contribution, and the copy printed neither: a reader pasting a
  // blocked producer into an agent handed over the state and not the reason
  // the read recorded for it.
  const cause = [
    detail.causeSentence ? `Recorded cause: ${detail.causeSentence}.` : "",
    detail.incidentCount !== null
      ? `${exact(detail.incidentCount)} `
        + `${detail.incidentCount === 1 ? "incident" : "incidents"} counted.`
      : "",
  ].filter(Boolean);
  if (cause.length) lines.push("", cause.join(" "));

  lines.push(...section("Run history", [
    detail.runsLine,
    ...(detail.runTotalNote ? [detail.runTotalNote] : []),
    ...(detail.runs.length
      ? ["", table(["When", "Outcome", "Read", "Wrote", "Duration", "Slowest step"], detail.runs.map((run) => [
        run.whenExact, run.mark.word, run.read, run.wrote, run.duration, run.phaseCode ? `${run.phase} (${code(run.phaseCode)})` : code(run.phase || null),
      ]))]
      : []),
  ]));

  // No "Recorded error" section: no read records a run's message.
  if (detail.reasonTitle && detail.reasonText) {
    lines.push(...section(detail.reasonTitle, [fence(detail.reasonText)]));
  }

  // No "Configuration" section: no read returns the recorded registry entry,
  // and serving one means serving credentials-adjacent configuration, which
  // needs server-side redaction of its own.
  lines.push("");
  return lines.join("\n");
}
