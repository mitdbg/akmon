#!/usr/bin/env node
// Audit the design harness against the rules the research settled.
//
//   node scripts/operator-ui-design/audit-rules.mjs
//
// Five checks, per view per fixture, from section 3 of
// scripts/operator-ui-design/redesign-requirements-v2.md:
//
//   rule 11  no rendered text below 12 px computed
//   rule 14  no horizontal overflow at 390
//   rule 2   every status colour is accompanied by a word or a glyph
//   rule 1   a count line precedes every table
//   rule 13  a read the fixture does not define fails loudly and by name
//   rule 15  every copy control (`data-copy`) is named "Copy <what>", no
//            "copied" state persists in a capture, and every view carries at
//            least one
//
// Plus, per view: the blocks Home, Updates and Sources render on every
// fixture; the attempt detail at 1440 -- its blocks, and that scale-200's
// forty-line recorded error renders all forty lines (reference
// ui/references/updates.md); and the producer page at 1440 -- its blocks, and
// that archiv3-small's Sources table has five rows for its five sources, not
// the eleven the sources read returns (reference ui/references/sources.md);
// and Explore's results and record pages at 1440 -- their blocks, that the
// workspace-mixed record page says "hidden" and names the policy for its
// masked field, that scale-200's Related tab states its cap and the omitted
// count, and that the refused-payload record's Raw tab says so by name
// (reference ui/references/explore.md).
//
// These are measured in the browser, on the real DOM, with computed styles --
// not read off the source. "The tokens are present" and "the page is not blank"
// are not visual approval and are not these checks either; the operator still
// approves every view. What this removes is the class of defect that no
// reviewer should have to catch twice.
//
// Exits non-zero on any breach and writes the findings to
// pact/changes/operator-ui-redesign-mocks/ui/evidence/rules-audit.json.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { chromium } from "playwright";
import {
  currentDetailAttemptFor,
  DESIGN_DIR,
  designDetailUrl,
  designUrl,
  detailAttemptFor,
  designExploreUrl,
  exploreTargetsFor,
  foreignRequestListener,
  viewerStates,
  fixtureNames,
  openDetail,
  openExplore,
  openSourcesDetail,
  openView,
  sourcesDetailTargetsFor,
  PACT_UI_DIR,
  requirePactUiDir,
  startDevServer,
  VIEWS,
  WIDTHS,
} from "./lib/harness.mjs";

const MIN_FONT_PX = 12;

// Home's CEILING, per width.
//
// Every list on Home is capped in console/model/home.ts -- eight
// attention rows, four attempt rows, six type rows plus a tail -- so the page
// has a height it cannot exceed however large the deployment is. It is NOT a
// fixed height: a block shrinks to its content, because reserving 300 px for
// rows that do not exist wastes the screen.
//
// So the measurement is a ceiling, not an equality. Two things are asserted:
// every fixture's Home is under the cap, and the 220-source deployment is no
// taller than the tallest of the others -- which is the shape a growth
// regression breaks first, long before the cap does.
// The ceiling catches a list that lost its bound, not a design that gained a
// line: it is re-set whenever the layout changes, and the number that matters
// is the distance between it and the tallest page, which is hundreds of pixels
// short of what 220 uncollapsed source rows would add.
const HOME_HEIGHT_CAP = { 1440: 1850, 390: 3100 };

// How much taller scale-200 may be than the next fixture before it counts as
// growth. A page that grew with the deployment would put scale-200 hundreds of
// pixels clear of the rest -- 220 source rows alone is 8,800 px. A handful is
// the ordinary difference between "4,182,044" and "2,605" wrapping differently,
// and failing on it teaches everyone to ignore the check.
const GROWTH_TOLERANCE_PX = 24;

// The blocks Home must render on every fixture, per width. A fixture whose data
// is empty still renders all of them: an absent block and an empty one are
// different things.
//
// The two lists differ because 390 renders a different page, not a squeezed
// one. A phone reader arrives for "is it current, what is in it, is anything
// wrong"; the chart, the four attempt rows and the contents line answer none of
// those, so they are not rendered and three links stand in their place.
// Blocks a named fixture does NOT render, and why. `empty` has no attempt, so
// it has no metric chart: an empty axis with a sentence under it is a block
// that says nothing, and the section already says it in words.
const HOME_BLOCKS_ABSENT = { empty: ["console-metric-chart"] };

const HOME_BLOCKS = {
  1440: [
    "console-header", "console-figures", "console-attention",
    "console-updates", "console-metric-chart", "console-sources", "console-contents",
    "console-contents-records", "console-contents-relationships", "console-footer",
  ],
  390: [
    "console-header", "console-figures", "console-attention",
    "console-updates", "console-metric-chart", "console-sources", "console-contents",
    "console-contents-records", "console-contents-relationships", "console-footer",
  ],
};

// The blocks Updates must render, per width.
//
// `empty` is the one fixture with a different list, and deliberately: a
// deployment with no attempt at all gets the header and one sentence, and a
// filter strip, a chart and a table with nothing in them would be four blocks
// saying the same nothing. Every other fixture renders all four at both
// widths.
const UPDATES_BLOCKS = {
  full: ["updates-header", "updates-filters", "updates-chart", "updates-table"],
  empty: ["updates-header"],
};

// The newest finished attempt's detail renders under the table when, and only
// when, that attempt is blocked. Read from the fixture rather than pinned
// here, so a regenerated fixture cannot leave this list stale.
function updatesInlineDetail(fixture) {
  const rows = JSON.parse(
    readFileSync(join(DESIGN_DIR, "fixtures", `${fixture}.json`), "utf8"),
  ).reads["/viz/api/console/generations"]?.payload?.generations ?? [];
  const newestFinished = rows.find((row) => row.status !== "running");
  return Boolean(newestFinished && newestFinished.status === "blocked");
}

// The blocks Sources must render. The state bar, the chips and the table are
// present on every fixture that has a producer -- all six do. `empty` has
// three never-run sources, so it has a table like the rest; it drops columns,
// not blocks.
const SOURCES_BLOCKS = ["sources-header", "sources-statebar", "sources-filters", "sources-table"];

// The producer page, at 1440. The error block is asserted only where the
// producer failed, the reason block only where it was withheld or excluded.
const SOURCES_DETAIL_BLOCKS = [
  "sources-detail-header", "sources-detail-contribution", "sources-detail-runs",
  "sources-detail-configuration",
];

// archiv3-small: five sources, eleven sources-read rows. One row per source.
const ARCHIV3_SOURCE_ROWS = 5;

// The blocks Explore's landing must render. `empty` has no published version
// and gets the header and one sentence; every other fixture gets the header,
// the searchable-types line, the type chips and the footer.
const EXPLORE_BLOCKS = {
  full: ["explore-header", "explore-searchbox", "explore-searchable", "explore-chips", "explore-footer"],
  empty: ["explore-header", "explore-empty"],
};

// The results page and the record page, at 1440. A results page is the
// rail, the about line and the results; the CLI block and the pages only
// when there are results.
const EXPLORE_RESULTS_BLOCKS = ["explore-header", "explore-rail", "explore-about", "explore-results"];
const EXPLORE_RESULTS_FULL_BLOCKS = [...EXPLORE_RESULTS_BLOCKS, "explore-cli", "explore-pages"];
const EXPLORE_RECORD_BLOCKS = ["explore-record-header", "explore-record-tabs"];

// The record page's tabs: one block per tab, visible only when that tab is
// open; a capture named for a tab must render that tab's block and no other
// tab's. Content's block set is the toolbar and two panes when an artifact
// renders, the absence sentence when it does not.
const EXPLORE_TAB_BLOCKS = {
  fields: "explore-record-fields",
  content: "explore-record-content",
  related: "explore-record-related",
  provenance: "explore-record-provenance",
  raw: "explore-record-raw",
};
const EXPLORE_CONTENT_RENDERED_BLOCKS = ["explore-content-toolbar", "explore-content-rendered", "explore-content-text"];
const EXPLORE_RAW_BLOCKS = ["explore-raw-row", "explore-raw-edges", "explore-raw-payload"];
const EXPLORE_PROVENANCE_BLOCKS = ["explore-provenance-attempt", "explore-provenance-log"];

// The policy the HTML frame must carry, verbatim from viewers/HtmlFrame.tsx.
const FRAME_CSP = "default-src 'none'; style-src 'unsafe-inline'; img-src data:";

// workspace-mixed's featured record carries the policy-hidden field (S15);
// scale-200's carries 400 outgoing rows capped at 100 (S13).
const HIDDEN_POLICY = "nomos.contact_details";
const SCALE_200_CAP_SENTENCE = "100 of 400 outgoing shown";

// The attempt detail, at 1440. The error block is asserted only where the
// attempt is blocked: a published attempt has no error, and a "Recorded error"
// heading over nothing would be the empty block this audit exists to catch.
const UPDATES_DETAIL_BLOCKS = ["updates-detail-header", "updates-detail-stages"];

// `scale-200`'s blocked attempt carries a forty-line recorded error and the
// pinned reference requires all forty to render. Counted in the DOM, from the
// text the reader actually sees.
const SCALE_200_ERROR_LINES = 40;

// Measured inside the page. Returns plain data only; nothing here may reach
// back into Node.
const MEASURE = () => {
  const out = {
    smallest_font_px: null,
    smallest_font_sample: null,
    under_min: [],
    // font-size: 0 is the visually-hidden-label idiom (an icon button whose
    // name only a screen reader reads). It renders NO text, so it is not text
    // under 12 px. Recorded rather than dropped, so the exemption is visible.
    zero_font_elements: [],
    // Under 12 px but not type: see the two exemptions below.
    small_text_exempt: [],
    horizontal_overflow: document.documentElement.scrollWidth
      - document.documentElement.clientWidth,
    document_height: document.documentElement.scrollHeight,
    colour_only_status: [],
    tables: 0,
    tables_without_count_line: [],
    // A table cell whose text is wider than its column is cut with an
    // ellipsis, and a cut figure is a fact the reader cannot finish reading.
    truncated_cells: [],
    // A cut cell that carries its whole text in a `title` is recorded here
    // instead: the reader can finish reading it. Only a cell the view marks
    // as a truncating one qualifies -- an error's first line, a long name.
    clipped_with_title: [],
    text_length: (document.body.innerText || "").trim().length,
    // Rule 7 / "the same set of elements on every fixture": the testids
    // present, sorted, so two fixtures that render different blocks differ
    // here rather than only in a screenshot nobody diffed.
    testids: [],
    // Rule 15: how many copy controls the view carries, the ones whose
    // accessible name is not "Copy <what>", and the ones still saying
    // "copied" -- a capture never clicks, so that state must never be in one.
    copy_controls: 0,
    copy_controls_unnamed: [],
    copied_visible: [],
    rendered_document_tables: 0,
    // The Content tab's frames and canvases: every frame's sandbox and
    // policy, and whether each PDF canvas has drawn anything.
    // The frame's document is PARSED, not searched as text: the policy must
    // be the first child of the real head, and no other http-equiv meta (a
    // refresh) or base element may remain anywhere in it.
    frames: [...document.querySelectorAll('[data-testid="explore-record-content"] iframe')].map((frame) => {
      const parsed = new DOMParser().parseFromString(frame.getAttribute("srcdoc") || "", "text/html");
      const first = parsed.head ? parsed.head.firstElementChild : null;
      const firstIsPolicy = Boolean(first && first.tagName === "META"
        && (first.getAttribute("http-equiv") || "").toLowerCase() === "content-security-policy");
      return {
        sandbox: frame.getAttribute("sandbox"),
        csp: frame.getAttribute("data-csp"),
        srcdoc_policy_first: firstIsPolicy,
        srcdoc_policy_content: firstIsPolicy ? first.getAttribute("content") : null,
        srcdoc_other_http_equiv: parsed.querySelectorAll("meta[http-equiv]").length - (firstIsPolicy ? 1 : 0),
        srcdoc_base_elements: parsed.querySelectorAll("base").length,
        src: frame.getAttribute("src"),
      };
    }),
    scripts_off_origin: [...document.querySelectorAll('[data-testid="explore-record-content"] script[src]')]
      .map((script) => script.getAttribute("src")),
    pdf_canvases: [...document.querySelectorAll('[data-testid="explore-content-pdf"] canvas')].map((canvas) => {
      const context = canvas.getContext("2d");
      if (!context) return { page: canvas.getAttribute("data-page"), drawn: false, reason: "no 2d context" };
      const { data } = context.getImageData(0, 0, canvas.width, canvas.height);
      let dark = 0;
      for (let at = 0; at < data.length; at += 4) {
        if (data[at + 3] > 0 && data[at] + data[at + 1] + data[at + 2] < 600) dark += 1;
      }
      return { page: canvas.getAttribute("data-page"), width: canvas.width, height: canvas.height, dark_pixels: dark, drawn: dark > 100 };
    }),
  };

  const visible = (element) => {
    const rect = element.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) return false;
    const style = getComputedStyle(element);
    return style.visibility !== "hidden" && style.display !== "none" && style.opacity !== "0";
  };

  // VISIBLE testids only. A block hidden at this width is not rendered as far
  // as a reader is concerned, and counting it would let a page that hides
  // everything pass the same-blocks check. Computed here, after `visible`
  // exists.
  out.testids = [...document.querySelectorAll("[data-testid]")]
    .filter((element) => visible(element))
    .map((element) => element.getAttribute("data-testid"))
    .sort();

  // --- rule 15: every copy control is named, and none says "copied" ------
  for (const control of document.querySelectorAll("[data-copy]")) {
    out.copy_controls += 1;
    const what = control.getAttribute("data-copy") || "";
    const name = control.getAttribute("aria-label") || "";
    if (!/^Copy \S/.test(name) || !name.includes(what)) {
      out.copy_controls_unnamed.push({ what, name, tag: control.tagName.toLowerCase() });
    }
    if (
      control.getAttribute("data-copied") === "true"
      || /\bcopied\b/i.test(control.textContent || "")
    ) {
      out.copied_visible.push({ what, text: (control.textContent || "").slice(0, 40) });
    }
  }

  // --- rule 11: nothing under 12 px -------------------------------------
  // Only elements that actually RENDER text: a font-size on a wrapper with no
  // text of its own is not text the reader sees.
  for (const element of document.querySelectorAll("body *")) {
    const ownText = [...element.childNodes]
      .filter((node) => node.nodeType === Node.TEXT_NODE)
      .map((node) => node.textContent.trim())
      .join("");
    if (!ownText || !visible(element)) continue;
    const size = parseFloat(getComputedStyle(element).fontSize);
    if (!Number.isFinite(size)) continue;
    if (size === 0) {
      out.zero_font_elements.push({
        tag: element.tagName.toLowerCase(),
        text: ownText.slice(0, 60),
      });
      continue;
    }
    if (out.smallest_font_px === null || size < out.smallest_font_px) {
      out.smallest_font_px = size;
      out.smallest_font_sample = ownText.slice(0, 60);
    }
    if (size >= 12) continue;

    // Two narrow exemptions, recorded rather than applied in silence. Rule 11
    // governs the TYPE SCALE -- "14 px base, two sizes, two greys" -- and both
    // of these are marks, which rule 2 treats as glyphs:
    //
    //   1. the AKMON brand lockup, which the operator approved as the identity
    //      on 2026-09-16 ("repo tokens.css, four firings, meander band, AKMON
    //      mark") and which is not prose or data;
    //   2. a single non-alphanumeric character, such as a dropdown caret.
    //
    // Anything else under 12 px is a breach. Both lists are written to the
    // report, so an exemption that starts absorbing real text is visible.
    const inBrandLockup = Boolean(element.closest(".okg-shell__brand"));
    const isBareGlyph = ownText.length <= 2 && !/[a-z0-9]/i.test(ownText);
    if (inBrandLockup || isBareGlyph) {
      out.small_text_exempt.push({
        px: size,
        tag: element.tagName.toLowerCase(),
        text: ownText.slice(0, 60),
        reason: inBrandLockup ? "inside the AKMON brand lockup" : "a single glyph, not type",
      });
      continue;
    }
    out.under_min.push({ px: size, tag: element.tagName.toLowerCase(), text: ownText.slice(0, 60) });
  }

  // --- rule 2: status is never colour alone ------------------------------
  // An element is "carrying status" when its class names it one, or when its
  // own colour departs from the page's body colour. Either way it must also
  // carry a word or a glyph: an empty coloured box says nothing to a reader
  // who cannot see the colour.
  const STATUS_CLASS = /(status|state|tone|badge|pill|severity|health|outcome|verdict)/i;
  const bodyColour = getComputedStyle(document.body).color;
  const saturated = (colour) => {
    const parts = colour.match(/\d+(\.\d+)?/g);
    if (!parts || parts.length < 3) return false;
    const [r, g, b] = parts.slice(0, 3).map(Number);
    return Math.max(r, g, b) - Math.min(r, g, b) > 40;
  };
  // A glyph SATISFIES rule 2 -- "a word plus a glyph" -- so an SVG, an image or
  // an element whose whole job is to be a mark is not a breach. The breach is a
  // coloured region carrying neither word nor glyph nor accessible name.
  const DECORATIVE_CLASS = /(brand|mark|meander|glyph|icon|logo|avatar|spark|swatch)/i;
  const classOf = (element) => {
    const raw = element.className;
    if (raw && raw.baseVal !== undefined) return String(raw.baseVal);
    return String(raw || "");
  };
  for (const element of document.querySelectorAll("body *")) {
    if (!visible(element)) continue;
    // Skip SVG and its whole subtree: `<svg>` and every `<path>` inside it
    // inherit the colour and would each be reported as a separate breach.
    if (element.closest("svg") || element.tagName.toLowerCase() === "img") continue;
    const className = classOf(element);
    if (DECORATIVE_CLASS.test(className)) continue;

    const style = getComputedStyle(element);
    const named = STATUS_CLASS.test(className);
    const colourful = style.color !== bodyColour && saturated(style.color);
    const boxed = style.backgroundColor !== "rgba(0, 0, 0, 0)" && saturated(style.backgroundColor);
    if (!named && !colourful && !boxed) continue;

    const ownText = [...element.childNodes]
      .filter((node) => node.nodeType === Node.TEXT_NODE)
      .map((node) => node.textContent.trim())
      .join("");
    const childText = (element.innerText || "").trim();
    const label = element.getAttribute("aria-label") || element.getAttribute("title") || "";
    const carriesGlyph = Boolean(element.querySelector("svg, img"));
    if (ownText || label || carriesGlyph) continue;
    // A container that merely holds a coloured child is not itself the signal.
    if (element.children.length > 0 && childText) continue;
    out.colour_only_status.push({
      tag: element.tagName.toLowerCase(),
      class: className.slice(0, 60),
      colour: style.color,
      background: style.backgroundColor,
    });
  }

  // --- rule 1: counts before lists ---------------------------------------
  // A table states its own count in a caption, or the element just before it
  // does. "Just before" is the previous sibling, or the heading above it.
  const hasDigit = (text) => /\d/.test(text || "");
  for (const table of document.querySelectorAll("table")) {
    if (!visible(table)) continue;
    // A table inside a rendered document (Markdown the source wrote) is the
    // document's, not a table the console draws; rule 1 governs the latter.
    // Counted, so an exemption that starts absorbing console tables shows.
    if (table.closest("[data-rendered-document]")) { out.rendered_document_tables += 1; continue; }
    out.tables += 1;
    const caption = table.querySelector("caption");
    if (caption && hasDigit(caption.innerText)) continue;
    // A table a component wraps (Ant Design's Descriptions puts its table
    // two elements deep) is "just before" whatever precedes its wrapper:
    // climb while the element is the first thing in its parent.
    let found = false;
    let node = table;
    for (let level = 0; level < 4 && node && !found; level += 1) {
      let previous = node.previousElementSibling;
      for (let step = 0; step < 3 && previous; step += 1) {
        if (hasDigit(previous.innerText)) { found = true; break; }
        previous = previous.previousElementSibling;
      }
      if (node.previousElementSibling) break;
      node = node.parentElement;
    }
    if (!found) {
      out.tables_without_count_line.push(
        (caption && caption.innerText.slice(0, 60)) || table.className || "(unnamed table)",
      );
    }
  }

  // --- rule 13, in the table: nothing rendered cut ------------------------
  for (const cell of document.querySelectorAll("table td, table th")) {
    if (!visible(cell)) continue;
    if (cell.scrollWidth > cell.clientWidth + 1) {
      const title = cell.getAttribute("title") || "";
      const text = (cell.innerText || "").trim();
      // The title carries the whole text: the recorded first line for an
      // error (with the code the cell drops), the whole name for a folded one.
      const prefix = text.replace(/\u2026.*$/, "").slice(0, 8);
      if (title && title.length >= text.length && prefix && title.includes(prefix)) {
        out.clipped_with_title.push({
          text: text.slice(0, 60),
          title: title.slice(0, 80),
          scroll_width: cell.scrollWidth,
          client_width: cell.clientWidth,
        });
        continue;
      }
      out.truncated_cells.push({
        text: (cell.innerText || "").trim().slice(0, 60),
        scroll_width: cell.scrollWidth,
        client_width: cell.clientWidth,
      });
    }
  }

  return out;
};

/** Rule 15, judged from one MEASURE result. `required` is true on every
 *  page: each of the four views and their detail pages carries at least one
 *  copy control. */
function copyBreaches(where, measured, breaches, required) {
  if (required && measured.copy_controls === 0) {
    breaches.push(`${where}: rule 15, the page carries no copy control`);
  }
  if (measured.copy_controls_unnamed.length) {
    breaches.push(
      `${where}: rule 15, ${measured.copy_controls_unnamed.length} copy control(s) are not `
      + "named \"Copy <what>\": "
      + measured.copy_controls_unnamed.map((c) => `${c.tag} "${c.name}" (${c.what})`).join(", "),
    );
  }
  if (measured.copied_visible.length) {
    breaches.push(
      `${where}: rule 15, ${measured.copied_visible.length} copy control(s) still say "copied" `
      + "in a capture that clicked nothing",
    );
  }
}

async function auditLoudFailure(page, base) {
  // Rule 13. Ask for a fixture that does not exist: the page must say so, by
  // name, in visible text -- not render an empty healthy-looking view.
  await page.goto(`${base}#home?fixture=no-such-fixture`, { waitUntil: "load" });
  await page.reload({ waitUntil: "load" });
  const alert = page.locator('[data-testid="design-read-failed"]');
  try {
    await alert.waitFor({ timeout: 20000 });
  } catch {
    return { ok: false, detail: "no failure element appeared for an unknown fixture" };
  }
  const text = (await alert.innerText()).trim();
  const namesIt = text.includes("no-such-fixture");
  const namesAlternatives = text.includes("archiv3-small");
  return {
    ok: Boolean(text) && namesIt && namesAlternatives,
    detail: text.slice(0, 200),
    names_the_missing_read: namesIt,
    names_what_exists: namesAlternatives,
  };
}

async function main() {
  requirePactUiDir();
  const fixtures = fixtureNames();
  const server = await startDevServer();
  const browser = await chromium.launch();
  const findings = [];
  const breaches = [];
  const detailRows = [];
  const sourcesDetailRows = [];
  const exploreRows = [];

  try {
    for (const fixture of fixtures) {
      for (const { width, height } of WIDTHS) {
        const context = await browser.newContext({
          viewport: { width, height },
          timezoneId: "UTC",
          colorScheme: "light",
        });
        const page = await context.newPage();
        for (const view of VIEWS) {
          const where = `${fixture}/${view}@${width}`;
          const apiRequests = await openView(page, server.url, view, fixture);
          const measured = await page.evaluate(MEASURE);
          const row = { fixture, view, width, url: designUrl(server.url, view, fixture), ...measured };
          findings.push(row);

          if (apiRequests.length) {
            breaches.push(`${where}: read ${apiRequests.length} URL(s) over the network`);
          }
          if (measured.text_length < 40) {
            breaches.push(`${where}: rendered only ${measured.text_length} characters of text`);
          }
          if (measured.under_min.length) {
            breaches.push(
              `${where}: rule 11, ${measured.under_min.length} element(s) render text below `
              + `${MIN_FONT_PX} px, smallest ${measured.smallest_font_px} px `
              + `("${measured.smallest_font_sample}")`,
            );
          }
          copyBreaches(where, measured, breaches, true);
          if (width === 390 && measured.horizontal_overflow > 0) {
            breaches.push(
              `${where}: rule 14, the document is ${measured.horizontal_overflow} px wider than `
              + "the viewport",
            );
          }
          if (measured.colour_only_status.length) {
            breaches.push(
              `${where}: rule 2, ${measured.colour_only_status.length} element(s) carry a status `
              + "colour with no word or glyph",
            );
          }
          if (measured.tables_without_count_line.length) {
            breaches.push(
              `${where}: rule 1, ${measured.tables_without_count_line.length} table(s) have no `
              + `count line: ${measured.tables_without_count_line.join("; ")}`,
            );
          }
          if (width === 1440 && measured.truncated_cells.length) {
            breaches.push(
              `${where}: ${measured.truncated_cells.length} table cell(s) are cut: `
              + measured.truncated_cells.map((cell) => `"${cell.text}"`).join(", "),
            );
          }
          if (view === "Sources") {
            const present = measured.testids.filter((id) => SOURCES_BLOCKS.includes(id));
            const absent = SOURCES_BLOCKS.filter((id) => !present.includes(id));
            if (absent.length) breaches.push(`${where}: renders no ${absent.join(", ")}`);
            const producerRows = await page.evaluate(
              () => document.querySelectorAll('[data-testid="sources-row"]').length,
            );
            row.producer_rows = producerRows;
            if (fixture === "archiv3-small" && producerRows !== ARCHIV3_SOURCE_ROWS) {
              breaches.push(
                `${where}: the table has ${producerRows} producer row(s), not the `
                + `${ARCHIV3_SOURCE_ROWS} sources archiv3-small configures`,
              );
            }
          }
          if (view === "Explore") {
            const expected = fixture === "empty" ? EXPLORE_BLOCKS.empty : EXPLORE_BLOCKS.full;
            const known = new Set([...EXPLORE_BLOCKS.full, ...EXPLORE_BLOCKS.empty]);
            const present = measured.testids.filter((id) => known.has(id));
            const absent = expected.filter((id) => !present.includes(id));
            const extra = present.filter((id) => !expected.includes(id));
            if (absent.length) breaches.push(`${where}: renders no ${absent.join(", ")}`);
            if (extra.length) {
              breaches.push(`${where}: renders ${extra.join(", ")}, which this fixture does not`);
            }
          }
          if (view === "Updates") {
            const expected = fixture === "empty"
              ? UPDATES_BLOCKS.empty
              : [...UPDATES_BLOCKS.full, ...(updatesInlineDetail(fixture) ? ["updates-inline-detail"] : [])];
            const known = new Set([
              ...UPDATES_BLOCKS.full, ...UPDATES_BLOCKS.empty, "updates-inline-detail",
            ]);
            const present = measured.testids.filter((id) => known.has(id));
            const absent = expected.filter((id) => !present.includes(id));
            const extra = present.filter((id) => !expected.includes(id));
            if (absent.length) breaches.push(`${where}: renders no ${absent.join(", ")}`);
            if (extra.length) {
              breaches.push(`${where}: renders ${extra.join(", ")}, which this fixture does not`);
            }
          }
        }
        await context.close();
      }
    }

    // --- the attempt detail, at 1440 ------------------------------------
    // Two per fixture where both exist: the attempt the view is for (the
    // newest blocked one, or the newest published one), and the attempt the
    // enricher and view diagnostics describe, which is the only one that can
    // carry per-producer rows (story S19).
    const detailTargets = fixtures.flatMap((fixture) => [
      ["blocked", fixture, detailAttemptFor(fixture)],
      ["current", fixture, currentDetailAttemptFor(fixture)],
    ].filter(([, , id]) => Boolean(id)));
    for (const [kind, fixture, attempt] of detailTargets) {
      const context = await browser.newContext({
        viewport: WIDTHS[0], timezoneId: "UTC", colorScheme: "light",
      });
      const page = await context.newPage();
      const where = `${fixture}/Updates-detail-${kind}@1440`;
      const apiRequests = await openDetail(page, server.url, "Updates", fixture, attempt);
      const measured = await page.evaluate(MEASURE);
      const blocked = await page.evaluate(
        () => Boolean(document.querySelector('[data-testid="updates-detail-error"]')),
      );
      const errorLines = await page.evaluate(() => {
        const block = document.querySelector(".okg-console__verbatim");
        return block ? (block.textContent || "").split("\n").length : 0;
      });
      const producerRows = await page.evaluate(
        () => document.querySelectorAll(".okg-console__stage-row").length,
      );
      const row = {
        fixture,
        view: `Updates detail (${kind})`,
        attempt,
        width: 1440,
        url: designDetailUrl(server.url, "Updates", fixture, attempt),
        has_error_block: blocked,
        error_lines: errorLines,
        ...measured,
      };
      findings.push(row);
      detailRows.push({
        fixture, kind, attempt, has_error_block: blocked, error_lines: errorLines,
        producer_rows: producerRows,
      });
      row.producer_rows = producerRows;

      if (apiRequests.length) {
        breaches.push(`${where}: read ${apiRequests.length} URL(s) over the network`);
      }
      const absent = UPDATES_DETAIL_BLOCKS.filter((id) => !measured.testids.includes(id));
      if (absent.length) breaches.push(`${where}: renders no ${absent.join(", ")}`);
      copyBreaches(where, measured, breaches, true);
      if (measured.under_min.length) {
        breaches.push(
          `${where}: rule 11, ${measured.under_min.length} element(s) render text below `
          + `${MIN_FONT_PX} px, smallest ${measured.smallest_font_px} px`,
        );
      }
      if (measured.colour_only_status.length) {
        breaches.push(
          `${where}: rule 2, ${measured.colour_only_status.length} element(s) carry a status `
          + "colour with no word or glyph",
        );
      }
      // Story S19: the attempt the diagnostics describe must list them. Seven
      // producers on workspace-mixed -- three enrichers and four views -- one
      // of which was withheld and one of which failed to build.
      if (kind === "current" && fixture === "workspace-mixed" && producerRows !== 7) {
        breaches.push(
          `${where}: renders ${producerRows} producer row(s) under the stages, not the `
          + "7 the enricher and view reads name for this generation",
        );
      }
      if (fixture === "scale-200" && kind === "blocked") {
        if (!blocked) breaches.push(`${where}: the blocked attempt shows no recorded error`);
        if (errorLines !== SCALE_200_ERROR_LINES) {
          breaches.push(
            `${where}: rule 3, the recorded error renders ${errorLines} line(s), not the `
            + `${SCALE_200_ERROR_LINES} the attempt recorded`,
          );
        }
      }
      if (fixture === "all-clear" && kind === "blocked" && blocked) {
        breaches.push(`${where}: a published attempt renders a "Recorded error" block`);
      }
      await context.close();
    }

    // --- the producer page, at 1440 ----------------------------------------
    for (const fixture of fixtures) {
      for (const [name, producer] of sourcesDetailTargetsFor(fixture)) {
        const context = await browser.newContext({
          viewport: WIDTHS[0], timezoneId: "UTC", colorScheme: "light",
        });
        const page = await context.newPage();
        const where = `${fixture}/${name}@1440`;
        const apiRequests = await openSourcesDetail(page, server.url, fixture, producer);
        const measured = await page.evaluate(MEASURE);
        const hasError = measured.testids.includes("sources-detail-error");
        const hasReason = measured.testids.includes("sources-detail-reason");
        const state = await page.evaluate(() => {
          const word = document.querySelector(
            '[data-testid="sources-detail-header"] .okg-console__status-word',
          );
          return word ? word.textContent : null;
        });
        const row = {
          fixture,
          view: `Sources detail (${name === "sources-detail" ? "producer" : "enricher"})`,
          producer,
          width: 1440,
          url: designDetailUrl(server.url, "Sources", fixture, producer),
          state,
          has_error_block: hasError,
          has_reason_block: hasReason,
          ...measured,
        };
        findings.push(row);
        sourcesDetailRows.push({
          fixture, name, producer, state, has_error_block: hasError, has_reason_block: hasReason,
        });
        if (apiRequests.length) {
          breaches.push(`${where}: read ${apiRequests.length} URL(s) over the network`);
        }
        if (measured.testids.includes("design-read-failed")) {
          breaches.push(`${where}: the producer page for ${producer} rendered a read failure`);
        }
        const absent = SOURCES_DETAIL_BLOCKS.filter((id) => !measured.testids.includes(id));
        if (absent.length) breaches.push(`${where}: renders no ${absent.join(", ")}`);
        copyBreaches(where, measured, breaches, true);
        if (measured.under_min.length) {
          breaches.push(
            `${where}: rule 11, ${measured.under_min.length} element(s) render text below `
            + `${MIN_FONT_PX} px, smallest ${measured.smallest_font_px} px`,
          );
        }
        if (measured.colour_only_status.length) {
          breaches.push(
            `${where}: rule 2, ${measured.colour_only_status.length} element(s) carry a status `
            + "colour with no word or glyph",
          );
        }
        if (measured.tables_without_count_line.length) {
          breaches.push(
            `${where}: rule 1, ${measured.tables_without_count_line.length} table(s) have no `
            + `count line: ${measured.tables_without_count_line.join("; ")}`,
          );
        }
        if (measured.truncated_cells.length) {
          breaches.push(
            `${where}: ${measured.truncated_cells.length} table cell(s) are cut: `
            + measured.truncated_cells.map((cell) => `"${cell.text}"`).join(", "),
          );
        }
        // Rule 3: a failed producer's page carries its recorded error; a
        // healthy one carries none.
        if (state === "Failed" && !hasError) {
          breaches.push(`${where}: a failed producer's page shows no recorded error`);
        }
        if (state === "Updated" && hasError) {
          breaches.push(`${where}: an updated producer's page renders a "Recorded error" block`);
        }
        if (state === "Withheld" && !hasReason) {
          breaches.push(`${where}: a withheld producer's page shows no reason`);
        }
        await context.close();
      }
    }

    // --- Explore's results and record pages, at 1440 ---------------------
    for (const fixture of fixtures) {
      for (const [name, params] of exploreTargetsFor(fixture)) {
        const context = await browser.newContext({
          viewport: WIDTHS[0], timezoneId: "UTC", colorScheme: "light",
        });
        const page = await context.newPage();
        const where = `${fixture}/${name}@1440`;
        const foreign = foreignRequestListener(page, server.url);
        const apiRequests = await openExplore(page, server.url, fixture, params);
        foreign.stop();
        const viewers = await viewerStates(page);
        const measured = await page.evaluate(MEASURE);
        const visibleText = await page.evaluate(() => (document.body.innerText || ""));
        const marks = await page.evaluate(
          () => document.querySelectorAll(".okg-console__mark").length,
        );
        const row = {
          fixture,
          view: `Explore ${name.replace("explore-", "")}`,
          explore: params,
          width: 1440,
          url: designExploreUrl(server.url, fixture, params),
          marks,
          ...measured,
        };
        row.viewers = viewers;
        row.foreign_requests = foreign.foreign;
        findings.push(row);
        exploreRows.push({ fixture, name, params, marks, document_height: measured.document_height, viewers, foreign_requests: foreign.foreign });
        if (apiRequests.length) {
          breaches.push(`${where}: read ${apiRequests.length} URL(s) over the network`);
        }
        // A Content page never reaches another origin: the frame's remote
        // script, the Markdown's remote image, nothing.
        if (foreign.foreign.length) {
          breaches.push(`${where}: requested ${foreign.foreign.length} URL(s) off the dev server, first ${foreign.foreign[0]}`);
        }
        // The record page's tabs: the named tab's block visible, no other.
        if (name.startsWith("explore-record")) {
          const tab = params.tab ?? "fields";
          const wanted = EXPLORE_TAB_BLOCKS[tab];
          const others = Object.values(EXPLORE_TAB_BLOCKS).filter((id) => id !== wanted);
          if (!measured.testids.includes(wanted)) breaches.push(`${where}: the ${tab} tab renders no ${wanted}`);
          const leaked = others.filter((id) => measured.testids.includes(id));
          if (leaked.length) breaches.push(`${where}: the ${tab} tab also shows ${leaked.join(", ")}`);
          if (tab === "content") {
            const rendered = viewers.length > 0;
            const expectedBlocks = rendered ? EXPLORE_CONTENT_RENDERED_BLOCKS : ["explore-content-absent"];
            const missing = expectedBlocks.filter((id) => !measured.testids.includes(id));
            if (missing.length) breaches.push(`${where}: Content renders no ${missing.join(", ")}`);
            for (const viewer of viewers) {
              if (viewer.state !== "ready") {
                breaches.push(`${where}: the ${viewer.viewer} viewer is ${viewer.state}${viewer.reason ? ` (${viewer.reason})` : ""}`);
              }
            }
            for (const frame of measured.frames) {
              if (frame.sandbox === null || /allow-scripts/.test(frame.sandbox)) {
                breaches.push(`${where}: a Content frame has sandbox="${frame.sandbox}"`);
              }
              if (frame.csp !== FRAME_CSP || !frame.srcdoc_policy_first || frame.srcdoc_policy_content !== FRAME_CSP || frame.src) {
                breaches.push(`${where}: a Content frame does not carry the policy "${FRAME_CSP}" as the first child of its document's head`);
              }
              if (frame.srcdoc_other_http_equiv > 0 || frame.srcdoc_base_elements > 0) {
                breaches.push(`${where}: a Content frame's document still carries ${frame.srcdoc_other_http_equiv} other http-equiv meta and ${frame.srcdoc_base_elements} base element(s)`);
              }
            }
            if (measured.scripts_off_origin.length) {
              breaches.push(`${where}: Content carries ${measured.scripts_off_origin.length} script tag(s) with a src`);
            }
            if (viewers.some((viewer) => viewer.viewer === "pdf")) {
              if (!measured.pdf_canvases.length) breaches.push(`${where}: the PDF viewer drew no canvas`);
              for (const canvas of measured.pdf_canvases) {
                if (!canvas.drawn) breaches.push(`${where}: PDF page ${canvas.page} canvas is blank (${canvas.dark_pixels ?? 0} dark pixels)`);
              }
            }
          }
          if (tab === "raw") {
            const missing = EXPLORE_RAW_BLOCKS.filter((id) => !measured.testids.includes(id));
            if (missing.length) breaches.push(`${where}: Raw renders no ${missing.join(", ")}`);
          }
          if (tab === "provenance") {
            const missing = EXPLORE_PROVENANCE_BLOCKS.filter((id) => !measured.testids.includes(id));
            if (missing.length) breaches.push(`${where}: Provenance renders no ${missing.join(", ")}`);
          }
        }
        if (measured.testids.includes("design-read-failed")) {
          breaches.push(`${where}: the Explore page for ${JSON.stringify(params)} rendered a read failure`);
        }
        const isResults = name.startsWith("explore-results") || name === "explore-query-help";
        // Pages exist only when the about line counts more than one page's
        // worth; a one-page result set has no pages block to render.
        const about = /(\d[\d,]*) results?/.exec(visibleText);
        const total = about ? Number(about[1].replace(/,/g, "")) : 0;
        const expected = isResults
          ? (name === "explore-results-empty"
            ? [...EXPLORE_RESULTS_BLOCKS, "explore-empty-results"]
            : (total > 20 ? EXPLORE_RESULTS_FULL_BLOCKS : [...EXPLORE_RESULTS_BLOCKS, "explore-cli"]))
          : EXPLORE_RECORD_BLOCKS;
        const absent = expected.filter((id) => !measured.testids.includes(id));
        if (absent.length) breaches.push(`${where}: renders no ${absent.join(", ")}`);
        if (name === "explore-query-help" && !measured.testids.includes("explore-query-help")) {
          breaches.push(`${where}: the query syntax panel is not open`);
        }
        if (name === "explore-results-filtered" && !(await page.locator('[data-section="type"] [data-selected="true"]').count())) {
          breaches.push(`${where}: no type is checked in the rail`);
        }
        if (name === "explore-results-page2" && !(await page.locator('[data-testid="explore-pages"] [aria-current="page"]').filter({ hasText: "2" }).count())) {
          breaches.push(`${where}: page 2 is not the current page`);
        }
        if (name === "explore-results-empty" && !(await page.locator('[data-testid="explore-empty-results"] a').count())) {
          breaches.push(`${where}: the empty result offers no broadening action`);
        }
        copyBreaches(where, measured, breaches, true);
        if (measured.under_min.length) {
          breaches.push(
            `${where}: rule 11, ${measured.under_min.length} element(s) render text below `
            + `${MIN_FONT_PX} px, smallest ${measured.smallest_font_px} px `
            + `("${measured.smallest_font_sample}")`,
          );
        }
        if (measured.colour_only_status.length) {
          breaches.push(
            `${where}: rule 2, ${measured.colour_only_status.length} element(s) carry a status `
            + "colour with no word or glyph",
          );
        }
        if (measured.tables_without_count_line.length) {
          breaches.push(
            `${where}: rule 1, ${measured.tables_without_count_line.length} table(s) have no `
            + `count line: ${measured.tables_without_count_line.join("; ")}`,
          );
        }
        if (measured.truncated_cells.length) {
          breaches.push(
            `${where}: ${measured.truncated_cells.length} table cell(s) are cut: `
            + measured.truncated_cells.map((cell) => `"${cell.text}"`).join(", "),
          );
        }
        // Story S12: a text match is marked.
        if (name.startsWith("explore-results") && name !== "explore-results-empty" && marks === 0 && !visibleText.includes("No record matches")) {
          breaches.push(`${where}: the results mark no match for ${JSON.stringify(params.q)}`);
        }
        // Story S15: the masked field says "hidden" and names the policy.
        if (fixture === "workspace-mixed" && name === "explore-record") {
          const said = /\bhidden\b/.test(visibleText) && visibleText.includes(HIDDEN_POLICY);
          if (!said) {
            breaches.push(`${where}: the record page does not say "hidden" and name ${HIDDEN_POLICY}`);
          }
          if (visibleText.includes("[nomos:masked]")) {
            breaches.push(`${where}: the policy token renders as text`);
          }
        }
        // Story S13: the cap and the omitted count are stated.
        if (fixture === "scale-200" && name === "explore-record" && !visibleText.includes(SCALE_200_CAP_SENTENCE)) {
          breaches.push(`${where}: the Related tab does not say "${SCALE_200_CAP_SENTENCE}"`);
        }
        // Story S14: the refused payload is one sentence naming the condition.
        if (name === "explore-record-raw" && !visibleText.includes("No retained payload:")) {
          breaches.push(`${where}: the Raw tab does not name the refused payload`);
        }
        // A payload past the inline limit shows its head and offers the file.
        if (name === "explore-record-raw-large" && !(visibleText.includes("Open raw") && /past the [\d,]+-byte inline limit/.test(visibleText))) {
          breaches.push(`${where}: the Raw tab does not offer the oversize payload as a file`);
        }
        // A refused or missing artifact says which, then the text.
        if (name === "explore-record-content-refused" && !/refused by policy \S+/.test(visibleText)) {
          breaches.push(`${where}: Content does not name the policy that refuses the artifact`);
        }
        if (name === "explore-record-chunk" && !visibleText.includes("part of ")) {
          breaches.push(`${where}: the chunk's page does not say what it is part of`);
        }
        await context.close();
      }
    }

    const context = await browser.newContext({ viewport: WIDTHS[0], timezoneId: "UTC" });
    const page = await context.newPage();
    var loudFailure = await auditLoudFailure(page, server.url);
    await context.close();
    if (!loudFailure.ok) {
      breaches.push(`rule 13: a missing fixture read did not fail loudly (${loudFailure.detail})`);
    }
  } finally {
    await browser.close();
    server.stop();
  }

  // Rule 7 / the operator's standing instruction, as a measurement rather than
  // an opinion: Home must not get taller because the deployment got bigger.
  const homeRow = (fixture, width) => findings
    .find((row) => row.fixture === fixture && row.view === "Home" && row.width === width) ?? null;
  const homeHeight = (fixture, width) => homeRow(fixture, width)?.document_height ?? null;
  const growth = WIDTHS.map(({ width }) => {
    const heights = Object.fromEntries(
      fixtures.map((fixture) => [fixture, homeHeight(fixture, width)]),
    );
    const others = fixtures
      .filter((fixture) => fixture !== "scale-200")
      .map((fixture) => heights[fixture])
      .filter((value) => value !== null);
    const cap = HOME_HEIGHT_CAP[width];
    const tallest = Math.max(...Object.values(heights).filter((value) => value !== null));
    return {
      width,
      cap,
      heights,
      tallest,
      scale_200: heights["scale-200"],
      under_cap: tallest <= cap,
      growth_tolerance_px: GROWTH_TOLERANCE_PX,
      scale_200_no_taller_than_the_others:
        heights["scale-200"] <= Math.max(...others) + GROWTH_TOLERANCE_PX,
    };
  });
  for (const row of growth) {
    if (!row.under_cap) {
      breaches.push(
        `Home@${row.width}: the tallest Home is ${row.tallest} px, over the `
        + `${row.cap} px cap. A block grew past its row cap.`,
      );
    }
    if (!row.scale_200_no_taller_than_the_others) {
      breaches.push(
        `Home@${row.width}: scale-200 renders ${row.scale_200} px, taller than every other `
        + "fixture. Something on Home grows with the size of the deployment.",
      );
    }
  }

  // The same blocks on every fixture, at each width. Set equality, not
  // containment: a block that should not be on this width is as much a breach
  // as one that is missing.
  const blockSets = WIDTHS.map(({ width }) => {
    const expected = HOME_BLOCKS[width];
    const known = new Set(Object.values(HOME_BLOCKS).flat());
    const perFixture = Object.fromEntries(fixtures.map((fixture) => [
      fixture,
      (homeRow(fixture, width)?.testids ?? []).filter((id) => known.has(id)).sort(),
    ]));
    let complete = true;
    for (const [fixture, present] of Object.entries(perFixture)) {
      const skip = HOME_BLOCKS_ABSENT[fixture] ?? [];
      const wanted = expected.filter((id) => !skip.includes(id));
      const absent = wanted.filter((id) => !present.includes(id));
      const extra = present.filter((id) => !wanted.includes(id));
      if (absent.length) {
        breaches.push(`Home@${width}: ${fixture} renders no ${absent.join(", ")}`);
        complete = false;
      }
      if (extra.length) {
        breaches.push(
          `Home@${width}: ${fixture} renders ${extra.join(", ")}, which this width does not show`,
        );
        complete = false;
      }
    }
    return {
      width, blocks: expected, absent_by_fixture: HOME_BLOCKS_ABSENT,
      per_fixture: perFixture, complete,
    };
  });

  mkdirSync(join(PACT_UI_DIR, "evidence"), { recursive: true });
  writeFileSync(
    join(PACT_UI_DIR, "evidence", "rules-audit.json"),
    `${JSON.stringify({
      min_font_px: MIN_FONT_PX,
      views: VIEWS,
      fixtures,
      widths: WIDTHS.map((size) => size.width),
      home_height_cap: HOME_HEIGHT_CAP,
      home_does_not_grow_with_scale: growth,
      home_renders_the_same_blocks: blockSets,
      updates_blocks: UPDATES_BLOCKS,
      updates_detail_blocks: UPDATES_DETAIL_BLOCKS,
      updates_detail: detailRows,
      sources_blocks: SOURCES_BLOCKS,
      sources_detail_blocks: SOURCES_DETAIL_BLOCKS,
      sources_detail: sourcesDetailRows,
      explore_blocks: EXPLORE_BLOCKS,
      explore_results_blocks: EXPLORE_RESULTS_BLOCKS,
      explore_results_full_blocks: EXPLORE_RESULTS_FULL_BLOCKS,
      explore_record_blocks: EXPLORE_RECORD_BLOCKS,
      explore_tab_blocks: EXPLORE_TAB_BLOCKS,
      explore_content_rendered_blocks: EXPLORE_CONTENT_RENDERED_BLOCKS,
      explore_frame_csp: FRAME_CSP,
      explore_pages: exploreRows,
      loud_failure: loudFailure,
      breaches,
      findings,
    }, null, 2)}\n`,
  );

  if (breaches.length) {
    for (const breach of breaches) console.error(`BREACH: ${breach}`);
    console.error(`${breaches.length} rule breach(es) across ${findings.length} rendered views`);
    return 1;
  }
  const exemptions = findings.reduce((total, row) => total + row.small_text_exempt.length, 0);
  console.log(
    `ok: ${findings.length} rendered views checked; no type under ${MIN_FONT_PX} px `
    + `(${exemptions} recorded exemption(s) for the brand lockup and bare glyphs), `
    + "no horizontal overflow at 390, every table has a count line, no colour-only status, "
    + "missing reads fail loudly",
  );
  return 0;
}

process.exit(await main());
