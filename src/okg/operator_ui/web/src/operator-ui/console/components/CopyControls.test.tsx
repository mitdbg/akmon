// The three copy controls, rendered: what they carry, what they name, what
// they copy. The suite has no DOM, so the click path is held by the audit
// (design/audit-rules.mjs, rule 15) and the state machine by `useCopy`'s
// contract asserted through the markup it produces at rest.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { CopyableId, CopyTextButton } from "@operator-ui/console/components/CopyControls";

const FULL = "okg:archiv3-small:branch:default:gen:20260913T090000000000Z:eea4e0d4e88d";

describe("CopyableId", () => {
  it("shows the text in monospace with a button named Copy <what> that carries data-copy", () => {
    const markup = renderToStaticMarkup(<CopyableId text={FULL} what="version id" />);
    // `title` carries the id whole: at 390 px the id WRAPS rather than
    // running the copy button off the right edge, and a reader who wants it
    // unbroken hovers for it.
    expect(markup).toContain(
      `<code class="okg-console__id" title="${FULL}">${FULL}</code>`,
    );
    expect(markup).toContain('data-copy="version id"');
    // The name carries the value, so ten copy icons in a list are not ten
    // identical "Copy version id" controls (audit B10). A version id is named
    // by its 12-hex tail, the form the page prints.
    expect(markup).toContain('aria-label="Copy version id eea4e0d4e88d"');
    expect(markup).toContain('type="button"');
    expect(markup).toContain('data-copied="false"');
    // At rest the icon is drawn and no confirmation text is.
    expect(markup).toContain("okg-console__copy-glyph");
    expect(markup).not.toContain("okg-console__copy-said");
    expect(markup).not.toContain(">copied<");
  });

  it("shows the children as given and still copies the full identifier", () => {
    const markup = renderToStaticMarkup(
      <CopyableId text={FULL} what="version id" reveal="row">
        <code className="okg-console__id">eea4e0d4e88d</code>
      </CopyableId>,
    );
    expect(markup).toContain(">eea4e0d4e88d</code>");
    expect(markup).not.toContain(`>${FULL}</code>`);
    // The full id is what the title names, so a hover tells the reader what
    // the click hands over.
    expect(markup).toContain(`title="Copy ${FULL}"`);
    expect(markup).toContain('data-reveal="row"');
  });

  it("names a short value whole and a long one by its start", () => {
    expect(renderToStaticMarkup(<CopyableId text="cmssw_releases" what="producer name" />))
      .toContain('aria-label="Copy producer name cmssw_releases"');
    const long = "r".repeat(30) + "s".repeat(50);
    expect(renderToStaticMarkup(<CopyableId text={long} what="record id" />))
      .toContain(`aria-label="Copy record id ${"r".repeat(24)}\u2026"`);
  });

  it("names the kind, not the value, in the title when the value is long", () => {
    const long = "x".repeat(120);
    const markup = renderToStaticMarkup(<CopyableId text={long} what="record id" />);
    expect(markup).toContain('title="Copy record id"');
  });

  it("adds the detail-id class to the mono element when asked", () => {
    const markup = renderToStaticMarkup(
      <CopyableId text="cmssw_releases" what="producer name" codeClassName="okg-console__detail-id" />,
    );
    expect(markup).toContain('class="okg-console__id okg-console__detail-id"');
  });
});

describe("CopyTextButton", () => {
  it("is a 13 px link-colour text control named Copy <what>", () => {
    const markup = renderToStaticMarkup(
      <CopyTextButton what="recorded error" text="ingest_source_incomplete: 412 of 2414 rows" />,
    );
    expect(markup).toContain("okg-console__copy--text");
    expect(markup).toContain('data-copy="recorded error"');
    expect(markup).toContain('aria-label="Copy recorded error"');
    expect(markup).toContain(">Copy</button>");
  });

  it("takes a label and a lazy text for the page control", () => {
    let built = 0;
    const markup = renderToStaticMarkup(
      <CopyTextButton
        className="okg-console__copy--page"
        what="Home as text"
        label="Copy as text"
        text={() => { built += 1; return "# x"; }}
      />,
    );
    expect(markup).toContain(">Copy as text</button>");
    expect(markup).toContain('aria-label="Copy Home as text"');
    // Not built until clicked: a page's Markdown is not assembled on render.
    expect(built).toBe(0);
  });
});
