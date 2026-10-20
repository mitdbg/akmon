// The Explore stylesheets, held to two rules.
//
// 1. A table cell is selected by its data-col attribute, never by position.
//    Explore's tables can cut a column -- the record page drops Content when
//    a record has none, and the cut list took the per-row date and citation
//    with them -- and an nth-child rule then styles whichever column slid
//    into that position. The rule reads as a width on the wrong column,
//    which no test that counts cells can see.
// 2. No table sets a fixed pixel width that the narrow stylesheet does not
//    release, because a fixed width is what scrolls the page sideways at
//    390. The Raw tab's edge table did exactly that: From and To at 240 px
//    each laid the table out at 656 px in a 390 px viewport.
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const STYLES = resolve(process.cwd(), "src/okg/operator_ui/web/src/operator-ui/console/styles");

function css(name: string): string {
  return readFileSync(resolve(STYLES, name), "utf8");
}

/** A rule's selector lines, with comments stripped. */
function selectors(source: string): string[] {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .split("{")
    .map((chunk) => chunk.split("}").pop() ?? "")
    .map((chunk) => chunk.trim())
    .filter(Boolean);
}

describe("Explore's stylesheets", () => {
  it("selects no table cell by position", () => {
    for (const sheet of ["explore.css", "record.css", "results.css"]) {
      const offenders = selectors(css(sheet)).filter((rule) => /nth-(child|of-type)/.test(rule));
      expect(offenders, sheet).toEqual([]);
    }
  });

  it("styles its cells by data-col, and carries the columns the views render", () => {
    const sheets = ["record.css", "explore.css", "results.css"].map(css).join("\n");
    for (const column of ["src", "dst", "rel", "open", "name", "type", "id"]) {
      expect(sheets, column).toContain(`[data-col="${column}"]`);
    }
  });

  it("releases the edge table's fixed id widths at narrow width", () => {
    const record = css("record.css");
    const narrow = css("narrow.css");
    // The wide rule that caused the sideways scroll, and the narrow release
    // that undoes it. If either is edited away alone, this says so.
    expect(record).toMatch(/\[data-col="src"\][^}]*width: 240px/);
    expect(narrow).toMatch(/\.okg-console__edgetable \{[^}]*table-layout: fixed/);
    expect(narrow).toMatch(/\.okg-console__edgetable \[data-col="dst"\][^}]*width: auto/);
  });

  it("lets a long record id and a long excerpt wrap at narrow width", () => {
    const narrow = css("narrow.css");
    expect(narrow).toMatch(/\.okg-console__result-meta \.okg-console__id[^}]*overflow-wrap: anywhere/);
    expect(narrow).toMatch(/\.okg-console__result-snippet \{ overflow-wrap: anywhere; \}/);
  });

  it("has no antd Tabs rule left, because the tabs are links", () => {
    expect(css("record.css")).not.toContain("ant-tabs");
    expect(css("narrow.css")).not.toContain("ant-tabs");
    expect(css("record.css")).toContain(".okg-console__tab[data-current=\"true\"]");
  });
});
