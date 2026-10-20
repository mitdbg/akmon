# Design — <change-id>

Carbon-RFC-shaped. The four sections that matter most for UI work and that
don't appear in backend RFCs — **Drawbacks (with teaching cost)**,
**Alternatives**, **Adoption strategy**, **How we teach this** — are not
optional. This file is the Stage-1 (design review) artifact; it lands and gets
sign-off *before* the typed contract (`ui/spec.md`) is written.

## Summary

One paragraph. What does this change introduce, and what does it unlock?

## Motivation

What use cases? What outcome do we expect? Frame it so that *if this design is
rejected, the motivation could still drive an alternative.* This is "why the
problem is worth solving", not "why I picked this design".

## Detailed design

### Behavior
States, events, the interaction loop. Don't restate the TS prop interface —
link to `ui/spec.md`. Do describe behaviors the type system can't express.

### Visual / tokens
Reference token *names*, not values (link `ui/visual.md`). Describe the visual
concept that drives the token choices.

### Accessibility
WCAG criteria, ARIA roles, focus management, keyboard, screen-reader
announcements. Required for every UI change.

### Responsive / density
Breakpoints, density modes, reflow — for components on >1 viewport.

### Internationalization
Translation-key namespace + RTL considerations, if it renders copy. See
`ui/content.md`.

## Drawbacks

Spell out the three UI drawback dimensions:
- **Implementation cost.** Integration with existing patterns; test surface.
- **Teaching cost.** What must operators/contributors learn, and where does
  that documentation live?
- **Maintenance cost.** Long-tail support; migration debt for breaking changes.

## Alternatives considered

Three or more (one of which is "do nothing"). For each: brief description +
the trade-off that ruled it out. Surface them here to short-circuit
re-litigation in review.

## Adoption strategy

How do existing consumers move? Codemod (name it) / manual migration (link the
guide + a grep pattern) / none (additive). State the rollout plan.

## How we teach this

Naming rationale; doc reorganization; changelog/migration-guide entry; a
Storybook story that exercises every variant; two real consumers once it ships.

## Unresolved questions

Explicit TBDs. Ship them with the design review; let reviewers resolve or punt.

## Migration plan

Required only when breaking or deprecating. Reference `ui/lifecycle.md` for the
stage transition + the rollback path.
