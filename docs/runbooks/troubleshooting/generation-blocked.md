---
audience: operator
type: how-to
canonical_id: runbook-generation-blocked
---
# Runbook: generation repeatedly blocked on the same invariant


Start in **Home → Health** for the selected deployment. **Published graph**
identifies the version serving reads. **Latest recorded update** identifies the
latest publication attempt; **Recent update history** keeps older failures separate.
An older publication or an unchanged count alone does not prove an update failed.

## Diagnose

1. **Read the latest recorded outcome.** If it is blocked, preserve the published
   checkpoint, blocked attempt identifier and displayed reason. Copy its
   **Read-only diagnosis** command exactly, including deployment and branch:

   ```sh
   okg generation describe '<blocked-checkpoint-id>' --deployment '<deployment>' --branch '<branch>'
   ```

   Replace these placeholders with the values from Health. Use the full canonical
   checkpoint ID, not a guessed next ID or a compatibility display number.
   The output includes the recorded `blocked_reason` and available
   `invariant_summary` with sample violator rows. Health shows a bounded summary;
   this command gives the full recorded reason. Keep the normal credentials and
   access authority for the selected deployment.

   `okg generation list --deployment '<deployment>'` lists published versions
   only; it cannot identify an unlisted blocked attempt. If outcome, identifier
   or reason is unavailable, say so and inspect source/workflow diagnostics in
   Health. A refusal before a checkpoint was recorded may have no attempt here.
   Do not infer success, no work, or a block from absence of evidence.

   A blocked update leaves the previous published graph available. Publication
   checks do not prove that a source claim is factually true or false. See
   [How information becomes searchable](../../start/data-lifecycle.md).

2. **Is the invariant correct?**

   - Common mistake: a freshly-authored invariant is too strict
     for the real data.
   - Less common: a real correctness regression upstream (source
     bug, extractor bug) producing actual bad rows.

   Read the SQL body in `invariants.yaml` and compare against the
   sample rows. If the invariant is correct and the rows are
   wrong, the upstream is broken.

3. **Is one source causing it?**

   If the invariant is `scope=cross`, check the `source` column of
   each sample row; violators all from one source name the culprit.

---

## Respond

### Option A: the upstream is broken (fix the data)

1. Find the source producing bad rows.
2. Investigate the adapter/extractor. Patch.
3. Roll back the source's watermark in SQL (see
   `docs/runbooks/troubleshooting/source-poisoning.md` for the recipe), then run
   `okg ingest --deployment <deployment> --include <src> --mode reconcile --inline` to
   re-observe from before the bug. New facts supersede old; the
   next publish should pass the invariant.

### Option B: the invariant is too strict (fix the invariant)

1. Edit `invariants.yaml` to relax the rule (add a filter, reduce
   severity to `warn`).
2. `okg catalog load --deployment <dir>` (a bare load is a dry-run +
   summary) to preview the change and read the additive/restrictive
   classification (a lowered severity is additive; a tightened SQL
   body is restrictive).
3. `okg catalog load --deployment <dir> --apply` to apply it under a
   fresh `catalog_versions` row.
4. Next publish re-runs gates against the new invariant set. If the
   apply leaves a `catalog_versions` row stuck `pending`, follow
   [ontology-migration-rollback](ontology-rollback.md).

### Option C: the invariant is correct and the data is legitimately
new-shape (needs ontology change)

If the invariant is catching a case the ontology should actually
permit, this is an ontology edit:

1. Edit the module's `schema.yaml` (the `classes:` block or
   `okg_edge_narrowings`) under the deployment's ontology module
   directory to widen the narrowing so the legitimate rows are
   permitted.
2. `okg catalog load --deployment <dir>` to preview the recomposed
   catalog and its classification.
3. `okg catalog load --deployment <dir> --apply`.
4. Next publish carries the new `catalog_version_id`; the invariant
   re-evaluates under the new ontology. If the apply stalls on a
   `pending` `catalog_versions` row, follow
   [ontology-migration-rollback](ontology-rollback.md).

### Do not: bypass the invariant

Do not set `severity: warn` and move on until you
understand the violator rows and confirm they are
acceptable. The gate system exists to catch this class of
regression.

---

## Blocked with `reason: gate_timeout`

This is not an invariant violation — the gate ran out of its total budget. Read
`blocked_reason` carefully, because two of its fields measure different things:

| field | meaning |
|---|---|
| `budget_basis` | which quantity was compared against the budget. Currently `cpu_sum`. |
| `cpu_sum_ms` | the **sum** of per-invariant durations. This is what the budget bounds. |
| `elapsed_ms` | wall-clock for the phase. Informational — **not** what tripped the budget. |
| `total_timeout_ms` | the budget itself. |

Invariants within a pass evaluate concurrently (width from the
`gate.invariants` concurrency term, default 2), so wall-clock is now shorter
than the work done and no longer tracks the budget. If the budget were read as
wall-clock, a generation that used to block would start publishing merely
because dispatch changed. The CPU-sum basis keeps the same total work tripping
the same budget; only the latency to discover it improves.

So when you see `gate_timeout`:

1. Compare `cpu_sum_ms` against `total_timeout_ms`. If `elapsed_ms` is much
   smaller, that is expected and not the anomaly.
2. Find the expensive invariants: the per-invariant `duration_ms` values in the
   gate report sum to `cpu_sum_ms`. One slow invariant is a query-plan problem;
   many mildly slow ones are a corpus-growth problem.
3. Widening `gate.invariants` will **not** fix a `cpu_sum` timeout — it reduces
   wall-clock, not total work. Fix the slow invariant or raise the budget
   deliberately.

Note the width is deliberately small. Gate invariants borrow from the same
connection pool the publish cycle uses, and a `PoolTimeout` inside the gate is
not caught — it would surface as an unattributed pool error rather than a gate
verdict. `check_feasibility()` refuses a `gate.invariants` width that cannot fit
alongside the other borrowers.

---

## When to accept a blocked generation

- **Short-lived blocks** (one or two generations) while a manual
  fix is in flight are normal. The previous generation stays
  live.
- **Sustained blocks** (> 1 hour, > 10 consecutive blocked
  generations) indicate a persistent upstream bug or a wrong
  invariant. Don't wait for self-recovery; act.

---

## Verify recovery

After applying the relevant repair, refresh **Home → Health**. Confirm that
**Latest recorded update** is published and **Published graph** identifies the
new searchable checkpoint. Older blocked rows remain in **Recent update history**;
they are not the current outcome. Current source, queue and read failures still
need attention even when publication succeeded.

Run the read-only diagnosis command for the new checkpoint and a bounded query
through the reader's normal interface. Confirm the expected changed record is
visible. Zero net record changes do not establish that nothing was imported;
unavailable historical deltas remain unavailable.

```sh
okg generation describe '<published-checkpoint-id>' --deployment '<deployment>' --branch '<branch>'
```

---

## Escalation

If the blocked invariant is `severity=error, scope=cross` and the
violator pattern can't be explained by source data, page the
substrate team — this may be a projection bug.
