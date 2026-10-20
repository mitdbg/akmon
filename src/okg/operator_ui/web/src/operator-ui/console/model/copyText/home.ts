// Home as Markdown.
import { type PageContext, code, heading, section, table } from "@operator-ui/console/model/copyText/shared";
import type { AttemptRow } from "@operator-ui/console/model/attempts";
import type { HomeModel } from "@operator-ui/console/model/home";
import { exact } from "@operator-ui/console/model/shared/format";

// --- Home -----------------------------------------------------------------

function homeAttemptRow(row: AttemptRow): string[] {
  return [
    row.when,
    row.count > 1
      ? `${row.outcome.word} ${row.headline} ${exact(row.count)}×`.trim()
      : `${row.outcome.word} ${row.headline}`.trim(),
    row.fullVersion ? code(row.fullVersion) : "",
    row.detail + (row.superseded ? " (superseded)" : ""),
  ];
}

export function homeText(model: HomeModel, total: number, context: PageContext): string {
  const lines = heading("Home", context);
  lines.push(...section("Status", [
    model.identity.chip.label,
    ...(model.identity.neverPublishedSentence ? [model.identity.neverPublishedSentence] : []),
    "",
    table(["Figure", "Value", "Change"], model.hero.map((figure) => [
      figure.label,
      exact(figure.value),
      figure.delta === null ? "" : (figure.delta >= 0 ? `+${exact(figure.delta)}` : `-${exact(-figure.delta)}`),
    ])),
  ]));

  const attention = model.attention;
  const open = attention.openCount;
  lines.push(...section(`Needs attention${open ? ` · ${exact(open)}` : ""}`, attention.allClearSentence
    ? [attention.allClearSentence]
    : [
      table(["Subject", "State", "Cause", "When"], attention.rows.map((row) => [
        row.count > 1 ? `${row.subject} (${exact(row.count)})` : row.subject,
        row.kindLabel ? `${row.outcome.word} (${row.kindLabel})` : row.outcome.word,
        row.cause,
        row.time,
      ])),
      ...(attention.omitted ? ["", `and ${exact(attention.omitted)} more`] : []),
      ...attention.rows
        .filter((row) => row.count > 1 && row.members.length)
        .map((row) => `${row.subject}: ${row.members.map((member) => code(member)).join(", ")}`),
    ]));

  lines.push(...section("Updates", model.noAttemptsSentence
    ? [model.noAttemptsSentence]
    : [
      table(["When", "Outcome", "Version", "Change"], model.recentAttempts.map(homeAttemptRow)),
      "",
      `${exact(total)} update ${total === 1 ? "attempt" : "attempts"} recorded`,
    ]));

  // The producers read had not answered, or it failed. Either way the copied
  // text says so in the block's place rather than dropping the section: a
  // reader pasting this into an issue must not be able to read "no source is
  // listed" out of a read that never came back.
  const block = model.producers;
  lines.push(...section("Sources", block === null
    ? ["The producer reads did not answer, so no source is listed in this copy."]
    : [
      ...(block.allGoodLine ? [block.allGoodLine] : []),
      ...(block.counts.length
        ? [block.counts.map((count) => `${exact(count.value)} ${count.label}`).join(" · ")]
        : []),
      // The producers that need something are under Needs attention above,
      // named once. This block is the census, in the copy as on the page.
      // The same last line the page prints, so the copy and the page do not
      // state the count in two different forms.
      block.allGoodLine ?? block.linkLabel,
    ]));

  const contents = model.contents;
  type ContentsRows = NonNullable<HomeModel["contents"]>["topRecordTypes"];
  const typeTable = (rows: ContentsRows, unit: string, more: number, moreCount: number) => [
    table(["Type", unit, "Share"], rows.map((row) => [code(row.name), exact(row.count), row.shareLabel])),
    ...(more ? [`${exact(more)} more ${more === 1 ? "type" : "types"} · ${exact(moreCount)} ${unit.toLowerCase()}`] : []),
  ];
  // The contents read failed: the copied text says so in the panel's place.
  // Copying a page with a section silently missing is how a reader pastes an
  // incomplete account into an issue without knowing it.
  if (contents === null) {
    lines.push(...section("Contents", [
      "The ontology-rollup read failed, so this version's record and "
      + "relationship types are not in this copy.",
    ]));
    lines.push("", model.footer, "");
    return lines.join("\n");
  }
  lines.push(...section("Contents", [
    `${exact(contents.records)} records · ${exact(contents.relationships)} relationships · `
      + `${exact(contents.recordTypes)} record types · ${exact(contents.relationshipTypes)} relationship types`,
    "",
    `### Record types · ${exact(contents.recordTypes)}`,
    "",
    ...(contents.recordsEmptyLine
      ? [contents.recordsEmptyLine]
      : typeTable(contents.topRecordTypes, "Records", contents.moreTypes, contents.moreTypeRecords)),
    "",
    `### Relationship types · ${exact(contents.relationshipTypes)}`,
    "",
    ...(contents.relationshipsEmptyLine
      ? [contents.relationshipsEmptyLine]
      : typeTable(
        contents.topRelationshipTypes, "Relationships",
        contents.moreRelationshipTypes, contents.moreRelationshipCount,
      )),
  ]));

  lines.push("", model.footer, "");
  return lines.join("\n");
}
