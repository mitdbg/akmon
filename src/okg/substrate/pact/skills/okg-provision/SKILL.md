---
name: okg-provision
description: Stand up + verify an OKG graph from scratch, or refresh one — the one-command bootstrap plus the health gates. Use when the user says "provision the graph", "set up the okg database", "bootstrap a deployment", "publish a generation", "refresh the graph", "is the graph healthy", "run preflight/integrity".
---

# Provisioning + verifying a graph

`okg deployment up` is the current deploy/upgrade path: resolve the deployment
environment -> migrate -> catalog apply -> ingest + publish, with the target and
worker-restart step named in the report. `okg provision` remains the
empty-database bootstrap helper; prefer `okg deployment up` for refreshes,
worktree-isolated dev graphs, and release/upgrades.

## Stand up (or refresh)
```bash
okg deployment env <deployment> --json                  # inspect DSN authority
okg deployment up <deployment> --target dev            # isolated dev graph
okg deployment up <deployment> --target prod           # declared deployment DB
okg deployment up <deployment> --include pact           # scope to one source
okg provision --deployment <deployment>                 # first bootstrap helper
```
Output reports the `generation_id`, per-step results, selected target, and a
clean preflight (`preflight.ok: true`, `blocker_count: 0`). Re-run to pick up
new PACTs / commits / sessions. Do not start by inventing or exporting a DSN;
inspect `okg deployment env` first and use its resolved authority, including
worktree-main local-env routing when present.

## Preflight (name every missing prerequisite at once)
```bash
okg doctor --deployment <deployment> --check preflight --json
```
Reports missing extensions, an unapplied schema, or an unloaded catalog in one
report — instead of learning one missing object per blocked publish.

## Verify the published generation
```bash
okg doctor --deployment <deployment> --check integrity --generation <g> --json   # interval / duplicate checks
okg doctor --deployment <deployment> --check database --json                      # schema / migration state
okg metrics                                                                       # queue depth, DLQ, embed lag, watermarks
okg generation describe <g> --deployment <deployment> --json                      # published vs blocked + row deltas
```
A healthy publish: integrity `ok` (zero invalid intervals / duplicate nodes),
DLQ within bounds, the generation reports `published` (not `blocked`).

Pin a durable canonical dev graph by exporting `OKG_DSN` to a named database.
If the numbers don't change when they
should, that's a silent failure — use the **okg-troubleshoot** skill.
