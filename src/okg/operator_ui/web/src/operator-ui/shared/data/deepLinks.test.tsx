/**
 * Grammar parity: the console's half of the deep-link contract (PACT
 * chat-deep-links, task.chat-deep-links.grammar-parity).
 *
 * The fixture is written by `tests/substrate/chat/test_deep_links.py`. Its
 * `expect` fields are derived from the INPUT triple, never re-parsed from
 * the renderer's output — a fixture built from the output would assert the
 * parser against itself and could never fail.
 *
 * PACT operator-ui-remove-four-views deleted the Graph view that showed
 * records and the Trace view that showed sessions, producer runs and MCP
 * traces, so no declared kind has a view and Python emits no URL at all.
 * What this side still owns is the other half: a hash the app RETIRED must
 * open the console's Home, not a blank or broken view. Since PACT
 * console-default-remove-old-views that includes `#home` and `#health`, the
 * two pages the console replaced.
 *
 * The extension is load-bearing. `vitest.operator-ui.config.ts` collects
 * `src/**\/*.test.tsx` ONLY, so naming this file `.test.ts` would make it
 * silently never run.
 */
import { describe, expect, it } from "vitest";

import { CONSOLE_HOME_HASH, consoleHashFor } from "@operator-ui/app/HomeApp";
import { consoleViewFromHash } from "@operator-ui/routes/console/ConsoleRoute";
// data -> shared -> operator-ui -> src -> web -> operator_ui -> okg -> src -> root
import fixtureJson from "../../../../../../../../tests/substrate/chat/fixtures/deep_link_urls.json";

interface FixtureEntry {
  name: string;
  input: {
    kind: string | null;
    id: string;
    generation: string | null;
    producer_key: string | null;
  };
  expect: { refusal: string };
  url: string | null;
  rendered: string;
}

interface SubtypeCase {
  name: string;
  subtype: string | null;
  id: string | null;
  url: string | null;
  rendered: string | null;
  expect: { refusal?: string };
}

interface Fixture {
  generated_by: string;
  operator_ui_base: string;
  entries: FixtureEntry[];
  subtype_cases: SubtypeCase[];
  retired_hashes: string[];
}

const fixture = fixtureJson as unknown as Fixture;

describe("deep-link parity with the console's hash parser", () => {
  it("has a non-empty fixture to assert against", () => {
    // A fixture that silently emptied would make every case below vacuous.
    expect(fixture.entries.length).toBeGreaterThan(0);
    expect(fixture.generated_by).toBe("tests/substrate/chat/test_deep_links.py");
    expect(fixture.subtype_cases.length).toBe(5);
    expect(fixture.retired_hashes.length).toBeGreaterThan(0);
  });

  for (const entry of fixture.entries) {
    it(`emits no console link for ${entry.name}`, () => {
      // No view shows this kind, so there is no URL for the console to
      // parse. The identifier survives as plain text with a stated reason.
      expect(entry.url).toBeNull();
      expect(entry.expect.refusal).toBe("no-console-view");
      expect(entry.rendered).toBe(entry.input.id);
      expect(entry.rendered).toMatch(/\S/);
    });
  }

  for (const entry of fixture.subtype_cases) {
    it(`handles the MCP subtype case ${entry.name}`, () => {
      // Every subtype arm refuses now: the routable ones for want of a
      // view, the rest for the defect the Python side names.
      expect(entry.url).toBeNull();
      expect(entry.expect.refusal).toBeTruthy();
      expect(entry.rendered).toEqual(expect.any(String));
      expect(entry.rendered?.trim()).not.toBe("");
      expect(entry.rendered).toBe(entry.id);
    });
  }

  for (const hash of fixture.retired_hashes) {
    it(`opens the console's Home for the retired hash ${hash}`, () => {
      // Old chat answers and bookmarks still carry these. Home is the
      // honest landing: the console shows what it can rather than failing.
      // Exactly the console's Home, with nothing of the old hash carried over.
      expect(consoleHashFor(hash)).toBe(CONSOLE_HOME_HASH);
      expect(consoleViewFromHash(consoleHashFor(hash))).toBe("home");
    });
  }

  it("still resolves the views the console does serve", () => {
    // Without this the retired-hash cases above would pass just as well on
    // a parser that answered "Home" to absolutely everything.
    expect(consoleViewFromHash(consoleHashFor("#console/sources"))).toBe("sources");
    expect(consoleViewFromHash(consoleHashFor("#console/updates?outcome=blocked"))).toBe("updates");
    expect(consoleViewFromHash(consoleHashFor("#console/explore?q=x"))).toBe("explore");
  });
});
