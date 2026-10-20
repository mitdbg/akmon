// Status vocabulary shared by every console view: a severity, an icon name and
// the glyph that pairs one with a word (rule 2: colour never carries status
// alone). Pure; no React.

/**
 * `never-run`, `excluded` and `withheld` are RECORDED facts about a producer
 * and are drawn in the warning colour, because each is something an operator
 * may need to act on. `unrecorded` is not a fact about the producer at all:
 * it means no read recorded a state, which happens on a deployment with no
 * runtime. Nothing is wrong with such a producer, so it is drawn in the muted
 * neutral style -- amber there would report a problem the reads never found.
 */
export type Severity =
  | "failed" | "blocked" | "running" | "never-run" | "ok" | "unrecorded";

/** Which icon the view draws. A name, not a component: this module is pure and
 *  has no React in it, and the mapping to Ant Design lives in ConsoleHome.
 *
 *  One icon means one thing, everywhere:
 *
 *    check    the read recorded a good state
 *    close    the read recorded a failure
 *    sync     work is under way
 *    warning  the read recorded a state an operator has to act on
 *    stop     the read recorded a state that holds this producer off
 *    minus    NO read recorded a state
 *
 *  `warning` exists because `minus` used to carry it. A producer whose
 *  runtime recorded `manual_required` or `stale` was drawn with the same
 *  "nothing was recorded" glyph as one whose runtime recorded nothing at all,
 *  in the same amber, so three live states and one absence were one mark on
 *  the page. */
export type IconName =
  | "check" | "close" | "sync" | "minus" | "stop" | "warning";

export interface Glyph {
  /** The icon. Rule 2: colour never carries status alone, and the word below
   *  is always beside it. */
  icon: IconName;
  /** The word beside it. */
  word: string;
  severity: Severity;
}

export const OUTCOME_GLYPHS: Record<string, Glyph> = {
  published: { icon: "check", word: "Published", severity: "ok" },
  blocked: { icon: "close", word: "Blocked", severity: "blocked" },
  running: { icon: "sync", word: "Running", severity: "running" },
};

export function outcomeGlyph(status: string): Glyph {
  return OUTCOME_GLYPHS[status] ?? { icon: "close", word: status, severity: "failed" };
}
