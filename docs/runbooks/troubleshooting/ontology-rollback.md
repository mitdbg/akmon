---
audience: operator
type: how-to
canonical_id: runbook-ontology-migration-rollback
---
# Runbook: catalog migration left pending after a crash


**Alert source.** `okg catalog history` or a direct query shows a
`catalog_versions` row stuck at `status='pending'` with no published
`graph_generations` row pointing at it. A new `okg catalog load
--deployment <dir> --apply` would compose and insert a *fresh* version
rather than completing the stranded one.

---

## What this means

`okg catalog load --deployment <dir> --apply` applies a catalog in one
pass (`apply_composed_catalog`):

1. Insert a `catalog_versions` row with `status='pending'`.
2. Apply the composed subtypes, narrowings, aliases, and identifier
   patterns.
3. Transition `catalog_versions.status` to `applied`.

If the process crashed between steps 1 and 3, the row stays `pending`.
The graph has not adopted the new catalog yet — no published generation
references it. There is no step-by-step resume: the recovery is to
mark the stranded row `rolled_back` and re-run a fresh apply.

**No data is lost.** Catalog application does not retract graph facts;
the next publish simply keeps using the last `applied` catalog version
until a new one is applied.

---

## Diagnose

```sql
SELECT catalog_version_id, status, applied_at, diff_summary
  FROM okg.catalog_versions
 WHERE status = 'pending'
 ORDER BY catalog_version_id DESC
 LIMIT 5;
```

Cross-reference against published generations to confirm none adopted
the pending version:

```sh
okg catalog history --json
okg generation list --deployment <name> --limit 5
```

`okg catalog history` reads the database from `--dsn` or `OKG_DSN`.
`okg generation list` takes the deployment's name (the last part of
`<dir>`), not its path.

---

## Respond

### Step 1: mark the stranded version rolled_back

There is no dedicated abandon command. Transition the row directly with SQL:

```sql
UPDATE okg.catalog_versions
   SET status = 'rolled_back'
 WHERE catalog_version_id = <id>
   AND status = 'pending';
```

This does not delete any rows the partial apply already wrote; the next
fresh apply recomposes the catalog from the deployment's modules.

### Step 2: re-apply cleanly

```sh
okg catalog load --deployment <dir>            # bare = dry-run + summary
okg catalog load --deployment <dir> --apply    # applies under a new version
```

The bare load shows the additive/restrictive classification before you
commit; `--apply` inserts a new `catalog_versions` row and drives it to
`applied`.

### Verify

```sh
okg catalog history --limit 3   # newest row is 'applied'
okg generation list --deployment <name> --limit 3   # next publish adopts it
```

---

## Prevent recurrence

- **Run a bare `okg catalog load` before `--apply`.** The dry-run
  summary shows the classification and diff; a surprise there is the
  signal to stop.
- **Do not start a second apply while one is pending.** Resolve the
  pending row first (Step 1), then apply.

---

## What NOT to do

- **Do NOT `DELETE FROM catalog_versions WHERE status='pending'`.**
  Mark the version `rolled_back` (Step 1) so the audit trail survives.
- **Do NOT manually insert `graph_generations` rows pointing at a
  pending catalog version.** The publisher's Phase C is the only writer
  of published generations.

---

## Escalation

If a fresh `okg catalog load --deployment <dir> --apply` also strands at
`pending`, page the substrate team — this indicates a bug in the catalog
applier itself.
