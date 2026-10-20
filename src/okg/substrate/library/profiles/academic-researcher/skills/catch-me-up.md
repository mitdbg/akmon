---
name: catch-me-up
description: Start-of-day briefing. Combines what's due, what's open for the operator, what's new in the lab, and any deltas since last query. Triggers on "catch me up", "good morning", "what's new", "where are we", "anything I need to know".
---

# Catch-me-up

## Goal

Produce a 4-section morning briefing so the operator sees what's due, what's open, lab status, and what changed since last briefing.

## Required data

- **Deadlines** with `status='pending'` and `due ∈ [now, now+14d]`.
- **ActionItems** with `owner_ref=<operator>` and `status='open'`, ordered by due.
- **Active Advisees** of the operator, with their next milestone (status≠met) and last meeting date.
- **Deltas** since the operator's last briefing: new Papers (last 7d), confirmed candidates, status changes.

## Operator surface

Catch-me-up is read-only. Compose the default MCP read surface:
`inspect / search / expand / filter / map / aggregate / query`. `query`
is a read-only SQL escape hatch over pinned read-side views; do not use
write/admin tools such as `record` or `propose` for this briefing.

- Resolve operator Person:
  `search(method="alias", query=<orcid>, subtype_filter="person")`
- Pending deadlines in window:
  `search(subtype_filter="deadline", attr_filters={"status":"pending"})` → filter results client-side by `attrs.due`.
- Open action items:
  `search(subtype_filter="action_item", attr_filters={"status":"open"})` → filter on `owner_ref`.
- Active advisees:
  `expand(input={node_id:<operator>}, edge_type="advises", direction="out")` → filter `subtype="advisee"`.
- Per-advisee context: for each advisee `expand(input={node_id:<advisee>}, path=["has_milestone"])` and `expand(input={node_id:<advisee>}, path=["author_of"])`.
- Recent deltas:
  `inspect(target={kind:"generation_delta", from_generation:<last_seen>})`.

## Output shape

Four short sections, each 3–6 bullets:
1. **Due ≤14d** (overdue at top, then chronological)
2. **Open for you** (oldest first)
3. **Lab snapshot** (one line per advisee; flag anyone not met with in 14+d)
4. **Since last briefing** (skip if no prior)

Close with one suggested next action.

## Anti-patterns

- Don't dump full attribute payloads. Headlines only.
- Don't invent items; empty sections say so.
- Don't repeat yesterday's briefing unchanged — surface only deltas.
