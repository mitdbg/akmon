// The producer-state vocabulary, held to the states the three console reads
// can record.
//
// The file's own header promised this test and it did not exist, so the one
// claim the vocabulary makes -- that no state a real deployment records falls
// through -- was unchecked. The lists below are the reads' own vocabularies,
// written out rather than derived from `STATES`, so a state deleted from the
// module fails here instead of quietly passing.
import { describe, expect, it } from "vitest";

import {
  ATTENTION_STATES, PRODUCER_GLYPHS, STATES, UNDETERMINED_STATE,
  freshnessWords, producerGlyph, stateFromParam, stateOf, stateParam, stateRank, words,
} from "@operator-ui/console/model/shared/producerStates";
import { OKG_WORKSPACE_SOURCES_BEFORE } from "@operator-ui/console/__fixtures__/okgWorkspaceSourceHealth";

/** `console/source-diagnostics`: the source-sync convergence state. */
const SOURCE_STATES = [
  "fresh", "converging", "stuck", "blocked", "manual_required", "unsupported",
  // Not a state the convergence read writes: `stateOf` reads `blocked` with an
  // overdue reason as it (PACT console-archiv3-view-fixes), and the read
  // layer may serve it directly.
  "overdue",
];
/** `console/enricher-diagnostics`: the enricher health status. */
const ENRICHER_STATES = ["healthy", "failing", "withheld", "wired-but-silent", "unknown"];
/** `console/view-diagnostics`: the view status. */
const VIEW_STATES = ["healthy", "failing", "unknown"];
/** The design harness's own generated payloads, which predate the live reads. */
const FIXTURE_STATES = ["updated", "failed", "running", "never run", "excluded"];

const RECORDABLE = [
  ...new Set([
    ...SOURCE_STATES, ...ENRICHER_STATES, ...VIEW_STATES, ...FIXTURE_STATES,
    UNDETERMINED_STATE,
  ]),
];

describe("every state the three reads can record", () => {
  it.each(RECORDABLE)("maps %s to a word and a glyph of its own", (state) => {
    expect(STATES.includes(state as (typeof STATES)[number]) || state === UNDETERMINED_STATE)
      .toBe(true);
    const glyph = PRODUCER_GLYPHS[state];
    expect(glyph, state).toBeDefined();
    expect(glyph.word.length, state).toBeGreaterThan(0);
    expect(glyph.icon.length, state).toBeGreaterThan(0);
    expect(producerGlyph(state)).toBe(glyph);
  });

  it("reserves the absence glyph for the two states that ARE an absence", () => {
    const minus = RECORDABLE.filter((state) => PRODUCER_GLYPHS[state].icon === "minus");
    expect(minus.sort()).toEqual([UNDETERMINED_STATE, "never run", "unknown"].sort());
  });

  it("survives a round trip through a URL parameter", () => {
    for (const state of RECORDABLE) {
      const param = stateParam(state);
      expect(param, state).not.toMatch(/[ _]/);
      // No exemption. `not recorded` used to be skipped here, because it was
      // not in STATES and `stateFromParam` could not resolve it -- so the one
      // state every row of a runtime-less deployment is in was the one state
      // the URL could not name (review 4, finding 5).
      expect(stateFromParam(param), state).toBe(state);
    }
  });

  it("lists the absence among the states, last", () => {
    // The count strip, the state bar and the state filter are all built by
    // walking STATES. Leaving the absence out of it drew no strip and no bar
    // at all on a deployment whose reads record no state for any producer.
    expect(STATES).toContain(UNDETERMINED_STATE);
    expect(STATES[STATES.length - 1]).toBe(UNDETERMINED_STATE);
    expect(stateFromParam("not-recorded")).toBe(UNDETERMINED_STATE);
    expect(stateParam(UNDETERMINED_STATE)).toBe("not-recorded");
  });

  it("ranks every state it lists under Needs attention", () => {
    for (const state of ATTENTION_STATES) {
      expect(RECORDABLE, state).toContain(state);
      expect(stateRank(state), state).toBeLessThan(30);
    }
  });
});

describe("a state no read this console knows about", () => {
  it("is drawn as something to look at, not as an absence", () => {
    const glyph = producerGlyph("quarantined_by_policy");
    expect(glyph).toEqual({
      icon: "warning", word: "quarantined by policy", severity: "never-run",
    });
    // `minus` means "no read recorded a state" and must not become the
    // table's fallback: an unmapped state would then read as an absence.
    expect(glyph.icon).not.toBe("minus");
    expect(stateRank("quarantined_by_policy")).toBe(30);
    expect(words("quarantined_by_policy")).toBe("quarantined by policy");
  });

  it("reads a row that recorded no state at all as that, and not as never run", () => {
    expect(stateOf({ workflow: { state: null } })).toBe(UNDETERMINED_STATE);
    expect(stateOf({ workflow: {} })).toBe(UNDETERMINED_STATE);
    expect(stateOf({ workflow: { state: "fresh" } })).toBe("fresh");
    expect(producerGlyph(UNDETERMINED_STATE).severity).toBe("unrecorded");
  });
});

describe("the freshness status the readiness read records", () => {
  it.each([
    ["fresh", "fresh"],
    ["stale", "stale"],
    ["missing", "no watermark recorded"],
    ["not_evaluated", "not evaluated"],
    ["not_configured", "no schedule"],
    ["last_run_age_only", "no watermark read"],
    ["quarantined_by_policy", "quarantined by policy"],
  ])("reads %s as %s", (status, expected) => {
    expect(freshnessWords(status)).toBe(expected);
  });

  it("is blank only where the read carried no status", () => {
    expect(freshnessWords(null)).toBe("");
    expect(freshnessWords(undefined)).toBe("");
  });
});

describe("a good state the health read does not back (issue #2205)", () => {
  // The rows okg-workspace's producers read served before the change, with
  // `health.status` as the read serves it after: every source that was
  // `healthy` on no recorded outcome is `unknown` (the Python rule's own
  // test, `test_source_health_from_recorded_outcome.py`, pins the 24).
  const after = OKG_WORKSPACE_SOURCES_BEFORE.map((row) => ({
    ...row,
    health: { status: row.health.status === "healthy" ? "unknown" : row.health.status },
  }));

  it("reads a fresh convergence state with an unknown health as unknown to the read", () => {
    expect(stateOf({ workflow: { state: "fresh" }, health: { status: "unknown" } })).toBe("unknown");
    expect(producerGlyph("unknown").word).toBe("Unknown to the read");
  });

  it("keeps a good state the health read backs", () => {
    expect(stateOf({ workflow: { state: "fresh" }, health: { status: "healthy" } })).toBe("fresh");
    expect(stateOf({ workflow: { state: "fresh" } })).toBe("fresh");
  });

  it("never hides a recorded problem behind unknown", () => {
    expect(stateOf({ workflow: { state: "blocked" }, health: { status: "unknown" } })).toBe("blocked");
    expect(stateOf({ workflow: { state: "unsupported" }, health: { status: "unknown" } }))
      .toBe("unsupported");
    expect(stateOf({ workflow: { state: null }, health: { status: "unknown" } }))
      .toBe(UNDETERMINED_STATE);
    // An overdue source (blocked on a stale watermark) stays Overdue too.
    expect(stateOf({
      workflow: { state: "blocked", reason: "completed_scope_watermark_stale" },
      health: { status: "unknown" },
    })).toBe("overdue");
  });

  it("changes okg-workspace's sources from 25 Fresh to 22 unknown and 3 Fresh", () => {
    // Six of the capture's eight blocked sources are blocked on a stale
    // watermark, which `stateOf` reads as Overdue (OVERDUE_REASONS). That
    // split is the same before and after; only the good states change.
    const count = (rows: readonly {
      workflow: { state: string | null; reason?: string | null };
      health: { status: string };
    }[]) => {
      const counts: Record<string, number> = {};
      for (const row of rows) counts[stateOf(row)] = (counts[stateOf(row)] ?? 0) + 1;
      return counts;
    };
    expect(count(OKG_WORKSPACE_SOURCES_BEFORE))
      .toEqual({ fresh: 25, overdue: 6, blocked: 2, unsupported: 2 });
    expect(count(after)).toEqual({ unknown: 22, fresh: 3, overdue: 6, blocked: 2, unsupported: 2 });
  });
});
