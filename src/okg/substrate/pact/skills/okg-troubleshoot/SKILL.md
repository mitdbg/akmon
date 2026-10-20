---
name: okg-troubleshoot
description: Diagnose a blocked or silent-failure publish — when a generation won't publish, or the numbers don't change when they should. Use when the user says "the publish is blocked", "the graph didn't update", "no rows/edges appeared", "generation blocked", "why is X missing", or "doctor/metrics looks wrong".
---

# Troubleshooting a publish

A default-value-as-fallback is a P0 bug, not a degradation. If something
"should work" but the numbers don't change, there is a silent failure —
investigate before moving on; never paper over it with a default.

## Start: what state is the generation in?
```bash
okg generation describe <g> --deployment <d> --json   # published | blocked + the invariant that blocked it
okg metrics                                           # projection lag, queue depth, DLQ, source watermarks
okg doctor --deployment <d> --check integrity --generation <g> --json
```

## The common silent-failure modes + the right diagnostic
| Symptom | Likely cause | Diagnose with |
|---|---|---|
| publish **blocked**, `narrowing_violation` | an edge whose `(parent_subtype, edge_type, child_subtype)` is not in the active catalog | `okg generation describe <g>` names the invariant; fix the catalog (okg-ontology-design) or the emitter |
| projector batch **committed zero rows** | cache-miss extraction or alias-resolution miss | `okg metrics` projection lag + the source's watermark |
| enricher emitted **zero derived edges** | AliasResolver miss, or the enricher's read query found nothing under the new generation | `okg trace node <node_id>` for one expected source node |
| **gate timeout** | a Phase B invariant exceeded its budget; the publish blocked | `okg generation describe <g>` + `docs/runbooks/troubleshooting/generation-blocked.md` |

## Trace one node end to end
```bash
okg trace node "<node_id>"   # facts -> live -> provenance: see exactly where it dropped
```
Find the root cause, fix it, and re-publish (via the **okg-provision** skill).
