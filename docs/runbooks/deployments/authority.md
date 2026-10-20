---
audience: operator
type: how-to
canonical_id: runbook-deployment-authority
---
# Control deployment authority

Use this page to inspect what a producer may write, configure bounded automatic
acceptance, or repair catalog ownership after worktrees shared a writable
database. These controls are fail-closed: automatic acceptance never expands a
producer profile, and catalog repair never restores retired graph shapes.

## Inspect producer authority

Producer profiles bound a producer to allowed record kinds, node subtypes,
edge triples, identifier prefixes, and attribute predicates.

```sql
SELECT deployment_name, producer_id, status, profile_fingerprint,
       catalog_version_id, output_signature
  FROM okg.v_producer_profiles
 WHERE deployment_name = '<deployment>'
 ORDER BY producer_id, updated_at DESC;
```

`active` has an enforceable signature; `compat` is summary-only. Join a
profile to the admission that used its exact fingerprint:

```sql
SELECT ae.admission_id, ae.status, ae.producer_id,
       pp.status AS profile_status, pp.profile_fingerprint,
       ae.verdict->'producer_policy' AS policy_evidence
  FROM okg.v_admission_envelopes ae
  JOIN okg.v_producer_profiles pp
    ON pp.deployment_name = ae.deployment_name
   AND pp.producer_id = ae.producer_id
   AND pp.profile_fingerprint =
       ae.check_summary->'producer_policy'->>'profile_fingerprint'
 WHERE ae.deployment_name = '<deployment>'
 ORDER BY ae.created_at DESC LIMIT 20;
```

Accepted admissions retain the profile fingerprint and matched clause IDs.
Rejected admissions retain a stable `producer_*` code and bounded offending
records. For derived producers, run:

```bash
uv run okg doctor --check derived-admission \
  --deployment <deployment> --json
```

The profile fingerprint covers the authority signature, not
`catalog_version_id`; catalog evolution alone must not change authority.

## Configure bounded automatic admission

The runtime approver may accept small review-mode payloads only when exactly
one closed rule matches. Producer policy still validates and applies them.

```yaml
runtime:
  admission:
    approver:
      enabled: true
      actor: runtime.approver
      rules:
        - id: bounded.observations
          producer_ids: [agent:cooper]
          producer_kinds: [agent]
          intents: [record_observation]
          node_subtypes: [observation]
          edge_types: []
          max_nodes: 1
          max_edges: 1
```

The approver schema accepts only `enabled`, `actor`, and `rules`; each of 1–64
rules must name exact producer IDs, kinds, intents, allowed node and edge types,
and positive record bounds. Unknown keys, duplicate entries, invalid names,
unsupported producer kinds, or malformed bounds reject the manifest. Automatic
acceptance is limited to profiles with `admission_mode: review` and never
accepts retractions (`op: R`).

Submit with an idempotency key:

```bash
uv run okg admission submit \
  --deployment <deployment> \
  --producer-id agent:cooper --producer-kind agent \
  --intent record_observation \
  --idempotency-key cooper-observation-42 \
  --payload-json '{"node":{"node_id":"observation:42","subtype":"observation","attrs":{"summary":"bounded finding"}}}' \
  --authority-json '{"producer_policy":{"producer_id":"agent:cooper","producer_kind":"agent","admission_mode":"review"}}' \
  --json
```

Zero or multiple matches remain pending. Malformed payloads, unmatched
identity, excess counts, disallowed record types, and retractions each record a
specific `runtime_approver_*` reason. Replaying the idempotency key must return
the original status without another event or graph write.

## Repair catalog ownership

Use this branch when publication reports a narrowing violation and catalog
diagnosis reports `unclaimed_mixed_history`, `owner_mismatch`, or
`latest_incompatible`:

```bash
uv run okg catalog ownership diagnose \
  --deployment deployments/<deployment> --json
```

First distinguish ownership drift from a deliberately retired narrowing. Do
not re-add a retired edge shape to clear the error; remove old edges from the
replay or reopen the design decision through a reviewed change.

Prevent mixed history with one writable database per worktree. To transfer an
existing database deliberately:

```bash
uv run okg catalog ownership claim \
  --deployment deployments/<deployment> --force --json
```

A successful claim transfers write authority only. If its receipt says
`latest_compatible: false` or `resolved: false`, run the printed next command
to apply the current catalog, then publish and verify:

```bash
uv run okg catalog load --deployment deployments/<deployment> --apply --json
uv run okg run --once --deployment <deployment> --apply --inline --json
uv run okg catalog ownership diagnose \
  --deployment deployments/<deployment> --json
uv run okg sync health --deployment <deployment> --json
```

Recovery requires `ok: true` and a latest catalog row containing deployment,
catalog, schema, writer, and writer-provenance fingerprints. Do not reset the
database merely to repair ownership.
