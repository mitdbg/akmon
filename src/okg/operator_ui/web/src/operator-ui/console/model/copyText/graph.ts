// The Graph view as plain Markdown, for "Copy as text": the type map's facts
// (totals, types, sources, the biggest relationships), the Table or the Grid
// as the page shows them, or one record's neighbourhood. Built from the same
// model the page draws.
import {
  type GraphModel, type SourceLinkModel, countBasisSentence, fmt, versionCountsSentence,
} from "@operator-ui/console/model/graph/derive";
import { type Matrix, sourceSourceMatrix, typeTypeMatrix, unlinkedSentence } from "@operator-ui/console/model/graph/grid";
import { type GridCell, recordSelection } from "@operator-ui/console/model/graph/params";
import type { NeighbourGroup, OntologyRead, RecordNeighbourhoodRead } from "@operator-ui/console/model/graph/reads";
import {
  ABSENT_TYPE, SIDE_TITLE, basisText, capClauses, countText, foldNames, hasNoRelationships, itemName, limitNotes, relationSentence,
  sideTotal, toTreeGroup,
} from "@operator-ui/console/model/graph/recordTree";
import {
  type ProducerCoverage, coverageSentence, emptyTypes, enricherPatterns, fillSampleSentence, linksIn, linksOut, linksWithin,
  moduleLabel, typeDetail, typesOf,
} from "@operator-ui/console/model/graph/table";
import { type MapState, typeSelectionText } from "@operator-ui/console/model/graph/typeMapModel";
import { type PageContext, code, heading, section, table } from "@operator-ui/console/model/copyText/shared";

/**
 * The type map as the page shows it: the totals and what they count, the
 * selected type's panel when one is selected, the producers switched off in
 * the legend, then every type, producer and relationship.
 */
export function graphTypesText(model: GraphModel, context: PageContext, state: MapState | null = null): string {
  const hidden = state ? [...state.hide] : [];
  return [
    ...heading("Graph", context),
    "",
    `${fmt(model.totals.records)} records, ${fmt(model.totals.relationships)} relationships. ${countBasisSentence(model.countBasis)}`,
    ...(hidden.length ? ["", `Switched off in the legend: ${hidden.map((s) => model.name(s)).join(", ")}.`] : []),
    ...(state?.type ? ["", ...typeSelectionText(model, state.type, state.hide.has(model.typeSource(state.type)))] : []),
    ...section("Record types", [table(
      ["Type", "Records", "Made by"],
      model.types.map((t) => [code(t), fmt(model.count[t]), (model.byType[t] ?? []).map(([s, n]) => `${model.name(s)} ${fmt(n)}`).join(", ")]),
    )]),
    ...section("Sources", [table(
      ["Source", "Records", "Relationships"],
      [
        ...model.sources.map((s) => [model.name(s), fmt(model.srcTotal[s]), fmt(model.edgeBy[s] ?? 0)]),
        ...model.enrichers.map(([p, n]) => [`${model.name(p)} (enricher)`, "none", fmt(n)]),
      ],
    )]),
    ...section("Relationships", [table(
      ["From", "Relationship", "To", "Count", "Made by"],
      model.patterns.map((p) => [code(p.s), p.e, code(p.d), fmt(p.n), p.prod.map(([q]) => model.name(q)).join(", ")]),
    )]),
  ].join("\n");
}

const zero = (n: number): string => (n ? fmt(n) : "none");

function footer(model: GraphModel): string {
  return versionCountsSentence(model, code);
}

/**
 * The Table as the page shows it: the types with their links in, out and
 * within, who makes them and where they are declared; the declared types
 * with no records when that list is open; the open row's fields and rules;
 * the sources and the enrichers; and the count basis.
 */
export function graphTableText(model: GraphModel, context: PageContext, state: {
  ontology: OntologyRead | null;
  coverage: ProducerCoverage | null;
  open: string | null;
  showEmpty: boolean;
}): string {
  const { ontology } = state;
  const declared = ontology ? Object.fromEntries(ontology.types.map((t) => [t.type, t])) : {};
  const typeRow = (t: string) => [
    code(t), zero(model.count[t] ?? 0),
    zero(linksIn(model.patterns, t)), zero(linksOut(model.patterns, t)), zero(linksWithin(model.patterns, t)),
    (model.byType[t] ?? []).map(([s, n]) => `${model.name(s)} ${fmt(n)}`).join(", "),
    ...(ontology ? [moduleLabel(declared[t]?.module)] : []),
  ];
  const headers = ["Type", "Records", "Links in", "Links out", "Within type", "Made by", ...(ontology ? ["Declared in"] : [])];
  const empties = ontology && state.showEmpty ? emptyTypes(model, ontology) : [];
  const lines: string[] = [
    ...heading("Graph · Table", context),
    "",
    ontology
      ? `${model.types.length} of ${ontology.types.length} declared types hold records.`
      : `${model.types.length} types hold records; the declared types were not read.`,
    ...section("Record types", [table(headers, model.types.map(typeRow))]),
  ];
  if (empties.length) lines.push(...section("Declared, no records", [table(headers, empties.map(typeRow))]));
  const open = state.open && ontology && (model.count[state.open] !== undefined || declared[state.open]) ? state.open : null;
  if (open && ontology) {
    const d = typeDetail(open, ontology, declared);
    lines.push(...section(`${code(open)}: fields`, [
      d.sample ? `${fmt(d.sample.sampled)} of ${fmt(d.sample.population)} records sampled.` : "No records to sample.",
      "",
      table(["Field", "Status", "Range", "Filled"], d.fields.map((f) => [
        code(f.name),
        f.origin === "declared" ? (f.required ? "required" : "") : f.origin === "system" ? "system" : "not declared",
        f.range ?? "",
        !d.sample ? "" : f.fill === null ? "never seen" : `${Math.round(f.fill * 100)}%`,
      ])),
    ], 3));
    lines.push(...section(`${code(open)}: relationships the ontology allows (${d.used.length} of ${d.rules} used)`, [
      table(["From", "Relationship", "To", "Count"], [...d.used, ...d.unused].map((r) => [code(r.from), r.relationship, code(r.to), zero(r.count)])),
    ], 3));
  }
  lines.push(
    ...section("Sources", [
      coverageSentence(model, state.coverage),
      "",
      table(["Source", "Records", "Relationships", "Types it makes"], model.sources.map((s) => [
        model.name(s), zero(model.srcTotal[s]), zero(model.edgeBy[s] ?? 0), typesOf(model, s).map((x) => code(x.type)).join(" "),
      ])),
    ]),
  );
  const enrichers = model.enrichers.filter(([, n]) => n > 0);
  if (enrichers.length) {
    lines.push(...section("Enrichers", [table(["Enricher", "Relationships", "Relationships it adds"], enrichers.map(([p, n]) => [
      model.name(p), zero(n), enricherPatterns(model, p).map((x) => `${code(x.s)} ${x.e} ${code(x.d)}`).join("; "),
    ]))]));
  }
  const fill = fillSampleSentence(ontology);
  lines.push("", `${footer(model)}${fill ? ` ${fill}` : ""}`);
  return lines.join("\n");
}

function matrixTable(m: Matrix, head: (x: string) => string, corner: string): string {
  return table(
    [corner, ...m.cols.map(head), "Total"],
    [
      ...m.rows.map((r, i) => [head(r), ...m.cells[i].map((n) => (n ? fmt(n) : "")), fmt(m.rowTot[i])]),
      ["Total", ...m.colTot.map((n) => (n ? fmt(n) : "")), fmt(m.grand)],
    ],
  );
}

/** The Grid as the page shows it: both matrices with exact counts (the page
 *  prints short ones), the open cell's detail, and the count basis. */
export function graphGridText(model: GraphModel, context: PageContext, state: {
  links: SourceLinkModel | null;
  cell: GridCell | null;
  linksFailed: boolean;
}): string {
  const tt = typeTypeMatrix(model);
  const lines: string[] = [
    ...heading("Graph · Grid", context),
    ...section("Which types link to which", [
      `Relationships from the row type to the column type.${tt.unlinked.length ? ` ${unlinkedSentence(tt.unlinked)}` : ""}`,
      "",
      matrixTable(tt.matrix, code, "from \\ to"),
    ]),
  ];
  const { links, cell } = state;
  lines.push(...section("Which sources link to which", links
    ? [
      `Relationships by the source that made each end record. ${countBasisSentence(links.countBasis)}`,
      "",
      matrixTable(sourceSourceMatrix(links), (s) => model.name(s), "from \\ to"),
    ]
    : [state.linksFailed ? "Not read: the source-links read failed." : "Not read yet."]));
  if (cell?.grid === "tt") {
    const pats = tt.cell[`${cell.row}|${cell.col}`] ?? [];
    lines.push(...section(`Open cell: ${code(cell.row)} → ${code(cell.col)}`, [table(["Relationship", "Count", "Made by"], pats.map((p) => [
      p.e, fmt(p.n), p.prod.map(([q, n]) => `${model.name(q)} ${fmt(n)}`).join(", "),
    ]))], 3));
  } else if (cell?.grid === "ss" && links) {
    const c = links.cells[`${cell.row}|${cell.col}`];
    if (c) {
      lines.push(...section(`Open cell: ${model.name(cell.row)} → ${model.name(cell.col)}`, [
        `${fmt(c.n)} relationships, made by ${c.prod.map(([q, n]) => `${model.name(q)} ${fmt(n)}`).join(", ")}.`,
      ], 3));
    }
  }
  lines.push("", footer(model));
  return lines.join("\n");
}

/**
 * One record's neighbourhood, as the page shows it: the record, each side's
 * groups with their producers, then the open group's listed records and the
 * selected record when the URL names them, and what the counts count.
 */
export function graphRecordText(
  read: RecordNeighbourhoodRead,
  name: (ownershipId: string) => string,
  context: PageContext,
  params: Record<string, string> = {},
): string {
  const { group, sel } = recordSelection(read, params);
  const makers = (g: NeighbourGroup) => [...new Set(g.items.map((i) => name(i.producer)))].join(", ");
  const side = (dir: "in" | "out") => {
    const gs = read.groups.filter((g) => g.direction === dir).sort((a, b) => b.count - a.count);
    if (!gs.length) return [];
    return section(`${SIDE_TITLE[dir]} (${countText(sideTotal(read, dir))})`, [table(
      ["Type", "Relationship", "Records", "Made by"],
      gs.map((g) => [g.type ? code(g.type) : ABSENT_TYPE, g.relationship, countText(toTreeGroup(g)), makers(g)]),
    )]);
  };
  const open = group ? (() => {
    const tg = toTreeGroup(group);
    const head = group.count > group.items.length || group.countIsFloor
      ? `First ${fmt(group.items.length)} of ${countText(tg)}, by id:`
      : `All ${fmt(group.count)}, by id:`;
    return section(`Open group: ${relationSentence(tg)}`, [
      head,
      "",
      ...foldNames(group.items).map((f) => `- ${itemName(f.it)}${f.k > 1 ? ` ×${fmt(f.k)}` : ""}: ${f.ids.map(code).join(", ")}`),
    ]);
  })() : [];
  const picked = sel ? (() => {
    const item = sel.group.items.find((i) => i.id === sel.id);
    const tg = toTreeGroup(sel.group);
    return section(`Selected: ${item ? itemName(item) : sel.id}`, [
      `${code(sel.id)} · ${sel.group.type ? code(sel.group.type) : ABSENT_TYPE} · ${relationSentence(tg)}${item ? ` · made by ${name(item.producer)}` : ""}`,
    ]);
  })() : [];
  const notes = [
    `${basisText(read)}.`,
    ...(capClauses(read).length ? [`A count ending in + is a floor: ${capClauses(read).join("; ")}.`] : []),
    ...limitNotes(read).map((n) => `${n}.`),
    `Each group lists at most ${read.perGroup} records, by id.`,
  ];
  return [
    ...heading(`Graph · ${read.record.label || read.record.id}`, context),
    "",
    `Record ${code(read.record.id)} · ${code(read.record.type)}${read.record.source ? ` · from ${name(read.record.source)}` : ""}`,
    ...(read.groups.length
      ? [...side("in"), ...side("out"), ...open, ...picked, "", notes.join(" ")]
      : ["", hasNoRelationships(read) ? "No relationships in this version." : `${limitNotes(read).join(". ")}.`]),
  ].join("\n");
}
