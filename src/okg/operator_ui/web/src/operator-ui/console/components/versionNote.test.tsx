// What the console prints about a `?version=` it cannot honour, and what it
// refuses to print.
//
// `?version=` is not a value this console chose. It is whatever a reader, a
// bookmark, a pasted link or another tool put on the URL, and the sentence
// that answered it used to echo it verbatim whenever it did not contain
// `:gen:`. On 2026-09-22 that rendered a PostgreSQL DSN -- host, user and
// password -- into the console's own prose on Home, the Updates list and the
// attempt page at once.
//
// The rule now (PACT console-archiv3-view-fixes): the sentence names only
// the version the page SHOWS, and prints none of what the URL asked for. Not
// a prefix, not a truncated tail: a fragment of a secret is a secret.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { VersionNotice, versionNotReadHere } from "@operator-ui/console/components/VersionNote";
import { looksLikeVersionId, namedVersion } from "@operator-ui/console/model/shared/ids";
import { ignoredSortParam, ignoredVersionParam } from "@operator-ui/console/model/updates/filters";

const READING = "okg:lane-d-home-render:branch:default:gen:20260921T024809349784Z:5c5404d36118";
const ASKED = "okg:lane-d-home-render:branch:default:gen:20260101T000000000000Z:0000deadbeef";

/** The three shapes that are NOT a version id, and what each would have
 *  leaked. The DSN is the one that actually reached a rendered page. */
const NOT_A_VERSION = {
  "a DSN": "postgresql://okg_admin:hunter2@10.0.0.4:5432/okg_prod",
  "a path": "/var/lib/okg/deployments/lane-d/versions/current",
  "garbage": "why is this page empty",
} as const;

describe("the shape test for a generation id", () => {
  it("accepts the ids this console actually reads", () => {
    expect(looksLikeVersionId(READING)).toBe(true);
    expect(looksLikeVersionId(ASKED)).toBe(true);
    expect(namedVersion(ASKED)).toBe("0000deadbeef");
  });

  it("accepts nothing else, however it is shaped", () => {
    for (const [what, value] of Object.entries(NOT_A_VERSION)) {
      expect(looksLikeVersionId(value), what).toBe(false);
      expect(namedVersion(value), what).toBeNull();
    }
    // A bare 12-hex tail is the OLD attempt filter's value, not the picker's;
    // the ignored-parameter line answers it, not this sentence.
    expect(namedVersion("0000deadbeef")).toBeNull();
  });

  it("names a published id the versions read returned, whatever its shape", () => {
    // A future id that does not match the shape is still nameable when the
    // versions read returned it: the list is the authority, the shape is the
    // fallback for the ids past the end of a capped read.
    const odd = "v2/generation/00000000abcd";
    expect(namedVersion(odd)).toBeNull();
    expect(namedVersion(odd, [odd])).toBe("00000000abcd");
  });
});

describe("the sentence a page prints about a version it cannot read", () => {
  // One clause, saying what the page shows (Archiv3 review, 2026-09-22). It
  // used to name both versions and where the asked one went, above the title
  // on Updates; the picker says "latest" on these views now, so the note only
  // has to say the page always shows it.
  it("says the page always shows the latest version, and names that one", () => {
    const said = versionNotReadHere(READING, ASKED, [READING, ASKED]);
    expect(said).toBe("This page always shows the latest version, 5c5404d36118.");
    // The asked-for version is not this page's to name: the picker and the
    // links carry it to Explore.
    expect(said).not.toContain("0000deadbeef");
  });

  it("names the version it shows when that is not the newest published", () => {
    expect(versionNotReadHere(READING, ASKED, [ASKED, READING]))
      .toBe("This page always shows version 5c5404d36118, the one this server reads.");
  });

  it("withholds a value that is not a version id, whatever it is", () => {
    for (const [what, value] of Object.entries(NOT_A_VERSION)) {
      const said = versionNotReadHere(READING, value);
      expect(said, what).toBe("This page always shows the latest version, 5c5404d36118.");
      // Not the value, and not any run of it long enough to matter. The DSN's
      // password is six characters.
      expect(said, what).not.toContain(value);
      expect(said.toLowerCase(), what).not.toContain("hunter2");
    }
  });

  it("withholds it on a deployment that has published nothing either", () => {
    const said = versionNotReadHere(null, NOT_A_VERSION["a DSN"]);
    expect(said).toBe("This deployment has published no version for this page to show.");
    expect(said).not.toContain("hunter2");
  });
});

describe("the notice, as the page renders it", () => {
  it("puts no unrecognised value into the markup, by any route through it", () => {
    for (const [what, value] of Object.entries(NOT_A_VERSION)) {
      // Both branches of VersionNotice: with the table's ignored-parameter
      // line, and without it. Neither may carry the value.
      const withLine = renderToStaticMarkup(
        <VersionNotice
          reading={READING}
          asked={value}
          ignoredParam={ignoredVersionParam({ version: value })}
        />,
      );
      const bare = renderToStaticMarkup(
        <VersionNotice reading={READING} asked={value} />,
      );
      for (const markup of [withLine, bare]) {
        expect(markup, what).not.toContain(value);
        expect(markup, what).not.toContain("hunter2");
        expect(markup, what).not.toContain("10.0.0.4");
      }
    }
  });

  it("names the version the page shows, in one clause", () => {
    const markup = renderToStaticMarkup(
      <VersionNotice reading={READING} asked={ASKED} published={[READING, ASKED]} />,
    );
    expect(markup).toContain("This page always shows the latest version, 5c5404d36118.");
  });

  it("draws nothing at all when no version was asked for", () => {
    expect(renderToStaticMarkup(<VersionNotice reading={READING} asked={null} />))
      .toBe("");
  });
});

describe("the ignored-parameter line", () => {
  it("echoes only what passed the hex test, and says so once", () => {
    const hex = ignoredVersionParam({ version: "bd2648936849" }) ?? "";
    expect(hex).toContain("This link means attempt=bd2648936849.");
    // Exactly one. The line states the rename once; a second copy would read
    // as a second parameter the page is refusing.
    expect(hex.split("no longer filters this table")).toHaveLength(2);
    expect(hex.split("attempt=bd2648936849")).toHaveLength(2);
  });

  it("uses the console's em dash, not two hyphens", () => {
    const line = ignoredVersionParam({ version: "bd2648936849" }) ?? "";
    expect(line).toContain("—");
    expect(line).not.toContain("--");
  });

  it("prints no line, and no value, for anything carrying a colon", () => {
    expect(ignoredVersionParam({ version: ASKED })).toBeNull();
    expect(ignoredVersionParam({ version: NOT_A_VERSION["a DSN"] })).toBeNull();
    expect(ignoredVersionParam({})).toBeNull();
  });

  it("names no value it could not vouch for", () => {
    for (const value of [NOT_A_VERSION["a path"], NOT_A_VERSION.garbage]) {
      const line = ignoredVersionParam({ version: value });
      if (line !== null) expect(line).not.toContain(value);
    }
  });
});

describe("a `?sort=` on Updates", () => {
  it("is named on the page rather than dropped in silence", () => {
    // Updates draws its rows in the order the read returns them and takes no
    // ordering of its own, so `?sort=oldest` -- an old bookmark, or a link
    // written by hand -- changed nothing and said nothing. Explore already
    // names `sort=` and `since=` for the same reason; this is the console's
    // own line, on the other view that can be asked for an order it has not
    // got.
    const line = ignoredSortParam({ sort: "oldest" }) ?? "";
    expect(line).toContain("sort=oldest");
    expect(line).toContain("newest first");
    expect(line.split("sort=oldest")).toHaveLength(2);
  });

  it("prints nothing when the URL carries none", () => {
    expect(ignoredSortParam({})).toBeNull();
    expect(ignoredSortParam({ sort: "   " })).toBeNull();
  });
});
