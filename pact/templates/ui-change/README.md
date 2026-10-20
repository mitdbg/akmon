# UI-change template (PACT)

A copy-paste scaffold for a PACT that touches the operator UI. The active
operator UI contract lives in
[`docs/develop/ui/operator-contract.md`](../../../docs/develop/ui/operator-contract.md).
The canonical agent workflow lives in
[`docs/runbooks/agents/ui.md`](../../../runbooks/agents/ui.md).
The PACT artifact conventions live in
[`pact/ui-change-guide.md`](../../ui-change-guide.md).

## When to use

Use it when the change introduces, modifies, deprecates, or removes a React
component, a design token, a user-facing flow, or editorial content. Use a
plain `okg pact new` (no `ui/`) for purely backend/tooling work that renders
nothing.

## How to use

1. Copy this directory:
   ```bash
   cp -r pact/templates/ui-change pact/changes/<your-change-id>
   ```
2. In `pact.yaml`, set `change.id` to `<your-change-id>`, replace every
   `<placeholder>`, and rename `req.ui.example` / the example component.
   Native PACT v4 does not accept top-level UI lane fields, so record the view
   boundary in `design.md` and `ui/spec.md` using Graph, Trace, or OKG Health
   from `docs/develop/ui/operator-contract.md`.
3. Pin the reference under `ui/references/` before implementation.
4. Fill only the contract files the slice needs. Use `ui/spec.md` for typed
   props and behavior, `ui/visual.md` for token bindings, and delete unused
   placeholders.
5. Build the static fixture render first; capture a browser screenshot; record
   reviewer approval under `ui/approvals/static/`.
6. For refinement passes, record keep/change/remove plus one `next-pass-focus`
   axis under `ui/feedback/` before editing.
7. Wire live data after static approval; capture live browser proof and reviewer
   sign-off under `ui/approvals/live/`.
8. Run the current single-app UI quality gate named by the PACT. The archived
   Playwright/supervisor-console gate is reference material only.
9. Attach evidence with `okg pact evidence attach`; then run
   `okg pact gate task-done <id> <task> --json`.

## Layout

```
pact/changes/<change-id>/
  pact.yaml          # change record (rationale, requirements, tasks, evidence)
  design.md          # brief rationale plus selected and forbidden UI lanes
  ui/
    references/      # screenshot, Figma export, URL, or current-state capture
    feedback/        # keep/change/remove + next-pass-focus for refinement
    approvals/       # static/live reviewer approvals
    spec.md          # typed contract: TS interface + tokens + a11y + scenarios
    visual.md  behavior.md  content.md  lifecycle.md
```
