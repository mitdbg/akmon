---
name: okg-record
description: Write a finding, observation, decision, or result back into the OKG — the agent write-back surface. Use when the user says "record this finding", "remember that", "log an observation/decision/hypothesis", "write this to the graph", "record a result". It goes through admission + publish, not a raw write.
---

# Recording findings into the OKG

`okg-query` reads; `record` writes back. The `record` MCP operator takes a
SEMANTIC payload (a kind + typed fields) and lowers it into the substrate's
admission shape — it does NOT write facts directly. The record flows through
admission and the normal publish cycle, then projects at the next published
generation.

## record — epistemic writes
```
record(kind="<kind>", payload={...}, authority="<role>", idempotency_key="<key>")
```
- `kind` is one of: `observation`, `finding`, `decision`, `hypothesis`,
  `experiment_run`, `evidence`, `verification`, `artifact`, `project`,
  `study`, `domain_entity`.
- `payload` carries the typed fields for that kind. Call `record` for a kind
  first to get its field template/spec, then fill it.
- `idempotency_key` makes re-runs replay-safe; the node id is kind-prefixed.
- `authority` names the role the write is made under.

Use it when you OBSERVE something worth keeping for downstream reasoning — a
finding, a decision, a result — instead of only emitting it to chat.

## propose — approval-gated changes
When a change needs sign-off before it lands (a schema narrowing, a contested
edge), use `propose` instead:
```
propose(action="submit", kind="<category>", payload={...}, authority="<role>")
```
A reviewer decides (`action="decide"`); the change lands only on approval.

## Confirm it landed
A record / propose write is NOT live until published — it projects at the next
publish cycle. Verify with the **okg-query** skill (search / trace for the new
node), reporting the `generation_id`.

Treat what you record as durable context, never as live code truth: current
files, tests and publish checks still decide.
