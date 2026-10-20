// The 2026-09-23 visual audit's styling findings, held where a test can hold
// them: the markup a view renders, and the rule a stylesheet declares. Layout,
// focus and contrast are proved in a browser (the PACT's screenshots and the
// contrast computation); what is asserted here is that the rule doing the
// work is present and that the markup gives it something to select.
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { Section } from "@operator-ui/console/components/Section";
import { Breakable, VersionIds } from "@operator-ui/console/components/VersionIds";

const STYLES = resolve(process.cwd(), "src/okg/operator_ui/web/src/operator-ui/console/styles");

/** Every rule in a sheet, comments stripped, with the @media block it sits in
 *  ("" at the top level). */
function rules(name: string): Array<{ media: string; selector: string; body: string }> {
  const source = readFileSync(resolve(STYLES, name), "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
  const out: Array<{ media: string; selector: string; body: string }> = [];
  const media: string[] = [];
  let open: { media: string; selector: string } | null = null;
  let buffer = "";
  for (const char of source) {
    if (char === "{") {
      const head = buffer.trim();
      buffer = "";
      if (head.startsWith("@")) media.push(head);
      else open = { media: media[media.length - 1] ?? "", selector: head };
    } else if (char === "}") {
      if (open) { out.push({ ...open, body: buffer.trim() }); open = null; } else media.pop();
      buffer = "";
    } else {
      buffer += char;
    }
  }
  return out;
}

function body(sheet: string, selector: string, media = ""): string {
  const found = rules(sheet).filter((rule) => rule.media === media
    && rule.selector.split(",").map((part) => part.trim()).includes(selector));
  if (found.length === 0) throw new Error(`${sheet}: no rule for ${selector} in "${media}"`);
  return found.map((rule) => rule.body).join("; ");
}

const PHONE = "@media (max-width: 760px)";

describe("the top bar (B1, B2)", () => {
  it("pins the controls to the third column and keeps a hidden search's cell", () => {
    expect(body("shell.css", ".okg-console-bar__controls")).toMatch(/grid-column:\s*3/);
    expect(body("shell.css", ".okg-console-bar__search")).toMatch(/grid-column:\s*2/);
    expect(body("shell.css", '.okg-console-bar__search[data-hidden="true"]'))
      .toMatch(/visibility:\s*hidden/);
  });

  it("draws the focus ring on the search field, as the landing box does", () => {
    expect(body("shell.css", ".okg-console-bar__search-field:focus-within"))
      .toMatch(/box-shadow:\s*var\(--okg-focus-ring\)/);
  });
});

describe("the attempt page's count column at 390 (B5)", () => {
  it("is at least 136 px and lets the delta wrap", () => {
    const width = /width:\s*(\d+)px/.exec(body("narrow.css", '.okg-console__ctable [data-column="count"]', PHONE));
    expect(Number(width?.[1])).toBeGreaterThanOrEqual(136);
    expect(body("narrow.css", ".okg-console__crow-delta-narrow", PHONE)).not.toMatch(/nowrap/);
  });
});

describe("warning words (B6)", () => {
  it("colours the never-run WORD with the text amber and keeps the amber for the glyph", () => {
    expect(body("home.css", '.okg-console__status[data-severity="never-run"] .okg-console__status-word'))
      .toMatch(/color:\s*var\(--okg-color-warn-text\)/);
    expect(body("home.css", '.okg-console__status[data-severity="never-run"] .okg-console__icon'))
      .toMatch(/color:\s*var\(--okg-color-warn\)/);
  });
});

describe("a section's count (B9)", () => {
  it("is neutral by default and red only for a failed or blocked state", () => {
    expect(body("home.css", ".okg-console__section-count")).not.toMatch(/okg-color-bad/);
    expect(body("home.css", '.okg-console__section-count[data-severity="failed"]')).toMatch(/okg-color-bad/);
    expect(body("home.css", '.okg-console__section-count[data-severity="blocked"]')).toMatch(/okg-color-bad/);
  });

  it("carries no severity when none is given, so 'reading…' is not marked failed", () => {
    const reading = renderToStaticMarkup(
      <Section title="Needs attention" testid="s" count={"reading…"}>x</Section>,
    );
    expect(reading).toContain('class="okg-console__section-count">reading…<');
    expect(reading).not.toContain("data-severity");
    const failed = renderToStaticMarkup(
      <Section title="Needs attention" testid="s" count="3" countSeverity="failed">x</Section>,
    );
    expect(failed).toContain('data-severity="failed"');
  });
});

describe("copy icons (B10)", () => {
  it("are at full strength when focused, even in a hovered row", () => {
    const focused = rules("components.css")
      .filter((rule) => rule.selector.includes(":focus-visible") && rule.selector.includes("copy--icon"));
    expect(focused.length).toBeGreaterThan(0);
    for (const rule of focused) expect(rule.body, rule.selector).toMatch(/opacity:\s*1\b/);
    expect(focused.some((rule) => rule.selector.includes(".okg-console__copyrow:hover"))).toBe(true);
  });
});

describe("source names at 390 (B16)", () => {
  it("wrap rather than ellipsing", () => {
    const rule = body("narrow.css", '.okg-console__ptable td[data-col="name"] .okg-console__prow-name', PHONE);
    expect(rule).toMatch(/white-space:\s*normal/);
    expect(rule).toMatch(/overflow-wrap:\s*anywhere/);
  });
});

describe("a registry name's break points (B16)", () => {
  it("offers a break after each separator and changes no character", () => {
    expect(renderToStaticMarkup(<Breakable text="cmssw_releases" />)).toBe("cmssw_<wbr/>releases");
    expect(renderToStaticMarkup(<Breakable text="cern-r3final.twiki_eos" />))
      .toBe("cern-<wbr/>r3final.<wbr/>twiki_<wbr/>eos");
    expect(renderToStaticMarkup(<Breakable text="jira" />)).toBe("jira");
  });
});

describe("the view links at 320 (B17)", () => {
  it("drop the printed wordmark below 360 px so the four links fit on one line", () => {
    expect(body("narrow.css", ".okg-console-bar__brand-label", "@media (max-width: 359px)"))
      .toMatch(/display:\s*none/);
  });
});

describe("version ids in a sentence (B21)", () => {
  it("sets each id in the mono class and leaves the words as written", () => {
    const html = renderToStaticMarkup(
      <VersionIds text="Counts are totals for version efc517cac053, before access rules" />,
    );
    expect(html).toBe(
      'Counts are totals for version <span class="okg-console__version-id">efc517cac053</span>, before access rules',
    );
    expect(renderToStaticMarkup(<VersionIds text="As of 2026-09-18 09:00 UTC." />))
      .toBe("As of 2026-09-18 09:00 UTC.");
    // Not an id: thirteen hex characters, or a word that merely follows "version".
    expect(renderToStaticMarkup(<VersionIds text="version efc517cac0531 and version history" />))
      .not.toContain("okg-console__version-id");
    expect(body("components.css", ".okg-console__version-id")).toMatch(/font-family:\s*var\(--okg-font-mono\)/);
  });
});
