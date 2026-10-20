// The count strip and the producers' state bar: counts by kind and by state,
// with the zeros dropped.
import { noun } from "@operator-ui/console/model/shared/format";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import {
  STATES, producerGlyph, stateOf, stateParam,
} from "@operator-ui/console/model/shared/producerStates";
import type { ProducerRecord } from "@operator-ui/console/model/sources/producers";
import type { ProducerKind } from "@operator-ui/console/model/shared/reads";
import type { IconName, Severity } from "@operator-ui/console/model/shared/status";

export interface CountLink {
  label: string;
  value: number;
  href: string;
  icon: IconName | null;
  severity: Severity;
}

/** One segment of the state bar: a state, how many, and its share of the
 *  width. Healthy is grey, not green: a bar that is 95 per cent green reads as
 *  a success meter rather than as a census. */
export interface StateSegment {
  state: string;
  count: number;
  share: number;
}

export interface ProducersModel {
  segments: StateSegment[];
  counts: CountLink[];
  /** How many producers the link leads to, so it can say so. */
  total: number;
  href: string;
  /** Printed INSTEAD of the rows and the counts when one good state covers
   *  every producer: "6 sources, all fresh" is the whole block. Null
   *  otherwise. */
  allGoodLine: string | null;
  /** What the link to the Sources view says. */
  linkLabel: string;
}

/**
 * The producers, counted by kind and then by the state each read RECORDED.
 *
 * The state list used to be written out here, in the design fixtures'
 * vocabulary -- updated, failed, running, never run, excluded, withheld. No
 * runtime emits any of those. On Archiv3 the six sources record `fresh`,
 * `blocked` and `manual_required`, so every state count was zero, every zero
 * was dropped, and the strip under "Sources" showed the three kind counts
 * and nothing else: a deployment with two blocked sources and one waiting on
 * a person looked exactly like one with nothing wrong.
 *
 * The counts are now taken from the states the rows actually carry, ordered
 * by `STATES` and with anything the list does not know about after them, so a
 * state a future runtime records is counted rather than silently dropped.
 */
export function producerCounts(producers: ProducerRecord[], link: LinkFn): CountLink[] {
  const byKind = (kind: ProducerKind) => producers.filter((row) => row.kind === kind).length;
  const kinds: CountLink[] = [
    {
      label: noun(byKind("source"), "source"), value: byKind("source"),
      href: link("sources", { kind: "source" }), icon: null, severity: "ok",
    },
    {
      label: noun(byKind("enricher"), "enricher"), value: byKind("enricher"),
      href: link("sources", { kind: "enricher" }), icon: null, severity: "ok",
    },
    {
      label: noun(byKind("view"), "view"), value: byKind("view"),
      href: link("sources", { kind: "view" }), icon: null, severity: "ok",
    },
  ];
  const states = countByState(producers).map(({ state, count }) => {
    const glyph = producerGlyph(state);
    return {
      // The glyph's word, lowercased: the strip is a sentence of counts, and
      // the word is the same one the state bar and the Sources table print,
      // so one state never reads as two things on one page.
      label: glyph.word.toLowerCase(),
      value: count,
      href: link("sources", { state: stateParam(state) }),
      // No icon on a state that is simply the good one: a tick beside every
      // count turns the strip into a scorecard.
      icon: glyph.severity === "ok" ? null : glyph.icon,
      severity: glyph.severity,
    };
  });
  return onlyNonZero([...kinds, ...states]);
}

/** Every state the rows carry, in `STATES` order, then any the list does not
 *  know about in the order they were met. Zeros are not produced at all. */
export function countByState(producers: ProducerRecord[]): { state: string; count: number }[] {
  const counts = new Map<string, number>();
  for (const row of producers) {
    // ONE accessor for all three kinds, because ONE read answers for all
    // three: `console/producers` folds an enricher's and a view's `status`
    // into a `workflow.state` of its own before it serves them
    // (`console_views/producers.py`). Home used to read the three diagnostics
    // routes directly, which do not agree on a shape, and needed a
    // per-kind accessor to survive them.
    const state = stateOf(row);
    counts.set(state, (counts.get(state) ?? 0) + 1);
  }
  const known = (STATES as readonly string[]).filter((state) => counts.has(state));
  const unknown = [...counts.keys()].filter(
    (state) => !(STATES as readonly string[]).includes(state),
  );
  return [...known, ...unknown].map((state) => ({ state, count: counts.get(state) ?? 0 }));
}

/**
 * Drop the zeros.
 *
 * `0 enrichers, 0 views, 0 failed, 0 running, 0 never run, 0 excluded, 0
 * withheld` is seven counts that say nothing, printed in the same weight as the
 * two that do. A state with nothing in it is not news; the Sources view lists
 * every state whether or not it is populated.
 */
function onlyNonZero(counts: CountLink[]): CountLink[] {
  return counts.filter((count) => count.value > 0);
}


export function stateSegments(producers: ProducerRecord[]): StateSegment[] {
  const total = producers.length;
  if (total === 0) return [];
  return countByState(producers)
    .map((row) => ({ ...row, share: (row.count / total) * 100 }));
}
