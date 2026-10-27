---
audience: operator
type: how-to
canonical_id: runbook-nomos
---
# Nomos Runbook


Nomos is the AKMON policy layer for compiling graph-native policy into a
deterministic enforcement bundle, evaluating bounded decisions, and
emitting traces that refine the TLA+ model in `formal/nomos/`.

For institutional identity providers, verified claims, subject bindings, and
permission grants, see [Nomos institutional IAM](nomos-iam.md).

## Formal Gate

Install the pinned TLC jar if needed:

```sh
make formal-nomos-bootstrap
```

Run the normal model and the intentional counterexample gate:

```sh
make verify-nomos
```

Expected result:

- `make formal-nomos` passes.
- `make formal-nomos-counterexample` succeeds only because TLC writes a
  counterexample artifact.
- Artifacts land under `.scratch/formal/nomos` and
  `.scratch/formal/nomos-counterexample`.

## Local Demo

Run the end-to-end local demo:

```sh
uv run okg nomos demo --out .nomos-demo --overwrite --json
```

The demo writes:

- `policy.graph.json`: graph-native policy source.
- `compiled-policy.json`: deterministic enforcement bundle.
- `decision-inputs.json`: bounded decision inputs.
- `decisions.json`: durable decision evidence.
- `refinement-state.json`: trace artifact mapped to
  `formal/nomos/Nomos.tla`.

## Compile A Policy

```sh
uv run okg nomos compile \
  --policy .nomos-demo/policy.graph.json \
  --out .nomos-demo/compiled-policy.json \
  --json
```

The compiler rejects missing defaults, duplicate rule ids, unknown
effects, malformed match clauses, and non-object scopes. The policy
fingerprint is deterministic for the same policy graph.

## Evaluate A Decision

```sh
uv run okg nomos evaluate \
  --bundle .nomos-demo/compiled-policy.json \
  --input .nomos-demo/decision-inputs.json \
  --json
```

`nomos evaluate` expects one decision-input object. The demo file
contains a list, so use one object from it or create a small
input:

```json
{
  "subject": "okg:lab-a",
  "action": "poros.graph.submit",
  "classification": "public",
  "resource": {
    "graph_profile": "context",
    "root_subtypes": ["context_packet"]
  }
}
```

The command exits successfully for all valid decisions, including
`deny`; a denial is policy evidence, not a CLI error.

## Deployment-Managed MCP Record Authority

An enforce-mode deployment can authorize the MCP `record()` operator without a
hand-edited policy bundle. The rollout baseline derives one bounded
`mcp.record` allow when both posture declarations are present:

```yaml
nomos:
  runtime_enforcement: enforce
  deployment_posture:
    write_principals: [okg_service, mcp_client]
    mcp_write_default: review
```

The derived rule grants only `mcp.record`, only to subjects corresponding to
`write_principals`, and only for the manifest's deployment scope. A missing or
`deny` `mcp_write_default` emits no Record allow. The MCP server must still be
started with write mode enabled (`OKG_MCP_ALLOW_WRITES=1` or the equivalent
launch option); policy posture cannot turn write tools on by itself.

For the direct active-policy path, the presented writer identity comes from the
server-owned Nomos subject. Reads, Record, the source tools and the `run_sql`
audit record resolve it the same way, on every call: an explicit
`OKG_NOMOS_SUBJECT` / `OKG_MCP_NOMOS_SUBJECT` / `OKG_PRINCIPAL`; otherwise
`subject:mcp_client` when the active policy is the rollout baseline and grants
it the MCP tool boundary; otherwise the process login name. When none of these
yields a subject the call is refused with `mcp_principal_unresolved` and
nothing is written. When the active policy cannot be read during that lookup,
the call is refused with `authority_evaluation_failed` (or
`authority_unavailable` for a database failure); the subject never falls
through to the login name because the policy was unreadable. Caller-supplied `authority.subject`, `authority.actor`, and
`authority.producer_id` do not authorize the request and are replaced by the
server subject in admission provenance. The institutional-IAM path instead uses
the subject resolved from the verified identity claim and likewise replaces
caller-supplied identity fields.

The Nomos boundary requires the `audit` obligation only. An allow there does
not auto-accept a graph write: `mcp_write_default: review` leaves the existing
Record admission workflow in charge, so the new record remains `pending` until
an authorized reviewer accepts it and a later publish makes it visible.

Provisioning and `okg deployment up` reconcile posture changes only while the
active policy/config are the deployment-managed rollout baseline. Adding or
removing a writer, or changing `review` to `deny`, changes the baseline
fingerprint and revokes the old authorization on reseed. A separately activated
custom policy bundle and institutional-IAM subject bindings/permission grants
are not silently replaced by baseline reconciliation.

## Active Postgres State

Nomos runtime enforcement reads active policy/config rows from Postgres.
The active path is:

1. Compile graph-native policy/config records.
2. Upsert the compiled artifacts into `okg.nomos_policy_bundles` and
   `okg.nomos_config_sets`.
3. Activate one policy and one config set for the deployment scope.
4. Runtime code calls the active Postgres interface. Missing active
   policy/config fails closed for Nomos-owned scopes.

Minimal fixture:

```python
from okg.substrate.nomos import (
    activate_config_set,
    activate_policy_bundle,
    compile_config_graph,
    compile_policy_graph,
)

bundle = compile_policy_graph(policy_graph)
config = compile_config_graph(config_graph)
activate_policy_bundle(
    conn,
    deployment_name="example-commons",
    bundle=bundle,
    scope="deployment:example-commons",
    activated_by="operator:nomos-admin",
)
activate_config_set(
    conn,
    deployment_name="example-commons",
    config_set=config,
    scope="deployment:example-commons",
    activated_by="operator:nomos-admin",
)
```

Inspect active state:

```sh
uv run okg nomos inspect \
  --dsn "$OKG_TEST_DSN" \
  --deployment example-commons \
  --scope deployment:example-commons \
  --json
```

The output reports active policy/config fingerprints, subject-binding
and permission-grant counts, active identity providers, recent claim
fingerprints, and recent decision ids.

Check the active policy and config:

```sh
uv run okg --json doctor --check nomos --dsn "$OKG_TEST_DSN" \
  --deployment example-commons
```

The Nomos doctor check fails when a deployment-scoped run has no active policy
bundle or active config set. It also rejects plaintext-looking secret config
values and invalid config drift metadata.

Production-facing readiness requires Nomos to block actions, not only
observe them. For `ci`, `dogfood`, `prod`, `production`, and profiles
listed in `nomos.deployment_posture.readiness_profiles`, `off` and
`audit` modes fail the Nomos readiness slice unless the deployment
manifest explicitly allows an audit rollout exception for that profile:

```yaml
nomos:
  runtime_enforcement: audit
  rollout:
    audit_readiness_exception:
      allowed: true
      readiness_profiles:
        - dogfood
      reason: "named rollout window while policy owners verify deny impact"
      owner: "nomos-team"
      expires_at: "2999-01-01T00:00:00Z"
```

The exception must name a profile, reason, owner, and future `expires_at`;
expired or incomplete exceptions do not satisfy readiness. The `standard`
readiness profile is not in that fixed list: a `standard` deployment needs a
blocking mode only when its own `readiness_profiles` lists `standard`. Operator output
from `okg deployment ready`, `okg doctor --check nomos`, and Nomos rollout
audit commands reports the effective mode, where that mode came from, whether
it blocks actions, and the exception that allowed audit mode to pass.

### Setting the mode

`nomos.runtime_enforcement` in `deployment.yaml` is the deployment's declared
mode, and it is the one to change when the change should persist. The three
values are `off` (no evaluation), `audit` (evaluate, record the real decision,
allow the effect) and `enforce` (evaluate and block denied effects).

Every shipped profile and starter installs with `off`: a fresh install leaves
runtime checks off when the deployment declares no access rules. Audit and enforce are opt-in. The deployment posture and source
policies still apply under `off`, because `okg ingest` audits each source
against them in every mode.

At the source-admission boundary `audit` is log-and-allow. Admission gates on
the resolved mode, not on the effect recorded in the decision, so a recorded
`deny` is kept in the audit trail and the ingest still proceeds, so a
deployment that opts in to `audit` can take in data before it has a policy
that allows every source.

`okg search --json` reports what happened to that one call in four fields:

| Field | Meaning |
| --- | --- |
| `nomos_enforcement_mode` | The mode that decided the call: `enforce`, `audit` or `off`. |
| `nomos_effect` | What the call got: `allow` (or `redact`/`declassify`) when the policy allowed it; `audit` when the policy would refuse it but audit mode let it through; null under `off`, where nothing is evaluated. |
| `nomos_policy_effect` | The policy's own verdict, as recorded. It stays `deny` under audit, so the audit trail is visible without reading as a refusal. |
| `nomos_decision_id` | The recorded decision, or null under `off`. |

A refused search never carries these fields. It exits non-zero with the error
`nomos_runtime_denied` and returns no rows. The fields describe the call, not
the results: an empty result under `allow` or `audit` means nothing matched.
A null `nomos_effect` always means `off`: if an `enforce` or `audit`
evaluation ever produced no decision, the search refuses with
`nomos_search_decision_missing` instead of reporting null.

`OKG_NOMOS_RUNTIME_ENFORCEMENT` overrides that declaration for one process
only. It takes `audit`, or any of these boolean-like tokens, compared after
trimming and lower-casing:

| Token | Effective mode |
| --- | --- |
| `enforce`, `on`, `yes`, `true`, `1` | `enforce` |
| `off`, `no`, `false`, `disabled`, `0` | `off` |
| `audit` | `audit` |

An unrecognized value is refused rather than normalized, so a mistyped
override can never quietly disable the boundary. `okg deployment ready` and `okg doctor --check nomos`
name the variable as the mode's source when it is set, which is how to tell an
overridden process from a changed manifest. The readiness rule above applies
to the effective mode, so an override to `off` or `audit` fails readiness on
the named profiles exactly as a manifest declaration would.

The MCP `record` operator can use Nomos authority when the caller supplies a
gateway-verified claim envelope. The server always evaluates the
deployment's active Postgres bundle in the deployment scope. A client that
sends `authority.nomos_bundle_path` or `policy_bundle_path`, or a
`nomos_scope` or `nomos_resource` fact that differs from the server's, is
refused before evaluation with `nomos_client_policy_override_refused`.

Allowed requests persist a Nomos decision and continue to normal record
validation/admission. Denied requests return `unauthorized_write`, persist
the deny decision, and create no admission envelope.

`unauthorized_write` is emitted only for a rendered deny verdict. The same
rule applies to MCP graph reads: an identity-boundary or active-policy
evaluation failure is still fail-closed but carries a distinct code so an
infrastructure failure is never reported as a policy denial:

- `authority_unavailable` — the authority evaluation hit an infrastructure
  failure: a dead/severed database connection (e.g. `AdminShutdown`
  after a deployment database rebuild) or MCP pool acquisition failure
  (`mcp_pool_unavailable`). Postgres being reachable again is enough;
  the MCP pools validate connections at checkout and replace dead ones,
  so a retry succeeds.
- `authority_evaluation_failed` — the authority evaluation failed before
  rendering a verdict for a non-infrastructure reason (bad bundle path,
  invalid identity claim, internal evaluation error).

In both cases the `authority` evidence carries `evaluated: false` with
the underlying cause under the nested `nomos` object
(`nomos_authority_unavailable` / `nomos_authority_evaluation_failed`)
instead of a claimed `authorized: false` verdict. For read failures this
evidence is in `SubstrateError.data.authority` alongside `data.surface`; a
pool acquisition failure that happens before the authority boundary remains
`mcp_pool_unavailable`. No record-template example is attached to a
non-verdict write refusal.

A rendered read policy deny still returns `nomos_identity_denied`.

## Read And Export Enforcement

The Postgres interface includes runtime helpers for Nomos-gated reads
and exports:

- `read_nodes_with_active_policy(...)` evaluates each pinned node row
  through the active policy; public reads can receive redacted attrs
  while audit reads can receive the full row.
- `authorize_graph_export_with_active_policy(...)` gates Poros or graph
  export and records a decision before export code releases data.

Use `redact` or `declassify` effects with obligation evidence for
restricted/private public outputs. A plain `allow` on restricted public
export is treated as unsafe by the trace checker.

## Who sets the classification at the MCP boundary

The MCP record and graph-read boundaries evaluate the class the server
assigns to the surface: `internal` for `mcp.record`, `mcp.tool.call`, and
`graph.read.mcp`, and `restricted` for `graph.read.sql_admin` (the `query`
tool). A client's `authority.classification` can only raise that class,
never lower it. The ranking is `public` < `internal` < `confidential`,
`private`, `regulated_metadata`, `synthetic_phi` < `restricted` < `secret`.
A lower declared class is ignored. An unknown class, or a different class
of the same rank, is refused before evaluation with
`nomos_declared_classification_invalid` and writes no decision row.

## Decision identity and retention

`okg.nomos_decisions` records one row per distinct access decision.
Decision identity is **content-of-access**: the identity hash covers the
subject, action, classification, policy fingerprint, and the resource
(deployment, scope, row id, source lineage, access-context facts). The
publish coordinates — `generation_id`, `branch_name`, and
`catalog_version_id` — are not part of identity. They are recorded on the
audit-only `target` instead. Re-reading the same rows after a publish adds no
rows; a policy change creates new rows because the policy fingerprint is part
of the identity.

Whole-record text reads (`inspect(text)`, its continuation pages, and a
chunk's parent list) decide their rows once per **decision class** instead of
once per row. Rows whose policy facts agree on everything an active rule and
the identity check can read (classification, source set, each check's source
id and resource kind, the JSON-shape verdict) form one class. One
representative row per class is decided by the same evaluator; every other
row gets its answer, and a redacted row is masked with its own subtype's
classifier, exactly as the per-row filter would. Such a read records one
decision per class and target, under the representative's row id, with the
`target` tagged `read_class`, `class_member_count` and
`decided_by: "read_class"`. The other rows get no decision row of their own,
and member ids are not recorded. A record's `contains` edge to a chunk is
decided only when no visible `member_of` edge places that chunk, since the
read uses no other; the two edge directions are decided in two calls under
one loaded bundle, so a class that held both counts only the edges decided,
and with no active bundle such a read writes one decision per call. The trail
stays insert-once, so a repeat
read writes nothing and the tags describe the first read. A text read's first
page decides its classes; its continuation pages reuse the text that page
filtered (a per-process cache keyed by the reader's subject, read action and
surface, the enforcement mode, the active policy fingerprint, the pinned
generation and the record) and decide nothing again. Their decisions would
have been the same rows, so with an active bundle the trail does not change.
With no active bundle each filter call writes its own decision, so the about
three decisions of every uncached page become about three per read. A
session with an identity claim or an access context is never cached, and a
batch text read under audit or enforce never uses the cache. Consequences:
the 24-hour decision counts in `okg doctor` and the Nomos production-readiness
check go down (that check needs only one recent decision, so it still passes),
`recent_decisions` and the refinement/trace export show fewer rows, and a
lookup by `target->>'node_id'` finds only representatives for these reads.
A read goes row by row, as before, when the session carries an identity
claim or an access context, when the caller passes a subject or identity
claim, or when an active rule that can match the read names a fact outside
the class (a row id, a graph or source-record reference, the lineage, an
access-context fact, or any unknown key). If a representative was decided
under a different policy than the class was computed from, the read is
refused.

### The stored read class

Each graph row version carries its class in two nullable columns on
`okg.graph_nodes` and `okg.graph_edges`: `nomos_read_class` (a UUID holding
the first 16 bytes of the SHA-256 of the row's class facts) and
`nomos_read_class_version` (the class vocabulary version it was computed
under). The class facts are, for each check the per-row filter would make on
the row, the values of the facts a class-level rule can read (source set,
source id, resource kind) and whether the check's resource is JSON-shaped.
They come from the row version's own stored content: its lineage attribute,
its source columns and its id. Classification is not in the class: an
accepted attribute overlay can change it after the write, so a read keys rows
on (stored class, source set, effective classification).

- **Writers.** Every writer of a graph row version stamps the two columns from
  the row it writes: fact apply (every ingest, enricher and publish write,
  after publisher-derived lineage), the embedding write and the source-lineage
  repair. Branch restore, run undo and merge-back copy whole rows, class
  included. A row with no source lineage gets NULL. A test pins the writer
  list, so a new writer must be classified before it merges.
- **Reads.** Whole-record text reads select the columns when the deployment
  has them and key each row by its stored class when the class is present and
  its version is the running one. Any other row (NULL, written before the
  upgrade, or under an older vocabulary) is keyed by the read-time projection
  above, which gives the same answer more slowly. Every guard above applies to
  both.
- **Upgrade.** `okg migrate` adds the columns (nullable, no default, so no
  table rewrite). Rows already in the graph keep NULL until a write changes
  their content; there is no backfill command yet. A deployment that has not
  run `okg migrate` reads and writes as before.
- **Verification.** `okg doctor --check nomos` reports, for the latest
  published generation, the row counts with a current, NULL and older class,
  the number of distinct classes, and recomputes up to 200 random stamped rows
  per table. A stored class that disagrees with its row's content is reported
  as `nomos_read_class_mismatch`. Setting
  `OKG_NOMOS_READ_CLASS_VERIFY_SAMPLE=<n>` makes each class-decided read
  recompute `n` of its returned rows the same way and refuse the read on a
  mismatch (off by default).

### The reader's view: class decisions before the query

`nomos.reader_view.open` (and `open_many` for nodes and edges together)
decides every class in the catalog `okg.nomos_read_classes` once per call,
before the query runs, and gives the caller a SQL predicate that keeps only
rows the reader may see. Search and grep use it so a `LIMIT` is not spent on
hidden rows.

- **Catalog.** One row per class: the class id, the class facts it is the
  digest of, the vocabulary version and when it was first written. The
  stamping helper writes it, insert-once, in the same transaction as the
  first row stamped with that class, so every stamped class has its catalog
  row. The table is append-only and is not versioned per generation. `okg
  migrate` creates it (migration `0040_nomos_read_classes`); a clone purges
  it with the graph rows it describes. Rows stamped before the table existed
  have no catalog row and are read on the exact per-row path.
- **Decisions.** Each class is decided by the same evaluator, enforcement
  points, subject, action and runtime boundary as the per-row filter, from a
  resource rebuilt from its facts, under every classification that can behave
  differently: each value an active rule names, `internal`, one value no rule
  names and one blank value. These class decisions are not recorded:
  the trail is unchanged from before the reader's view. Rows the
  caller then gives to `finish` pass through the per-row filter itself, so
  every row given to `finish` gets exactly its own `_read` decisions, hidden
  ones included, as before. A row the predicate dropped leaves no decision.
- **The predicate keeps every row it cannot place.** A row with no class, a
  class from another vocabulary version, a class missing from the catalog, or
  a classification the SQL cannot map exactly (a non-string value, or a
  string with characters outside letters, digits, `_`, `.` and `-` that no
  rule names) passes the predicate, and `finish` decides it. The predicate
  never drops a row the per-row filter would serve. With an accepted
  classification overlay in scope, or on the unpublished working generation,
  the predicate keys on the class alone (visible under any classification).
- **`sql_exact` and what it reveals.** `sql_exact` says the predicate alone
  equals what `finish` serves, so a scanner may charge counts and bounds in
  SQL. It is decided without reading any row, and so it
  is true only when Nomos is off for the deployment. With Nomos on, the
  per-row filter can remove or mask a row whatever the policy says: it hides
  every row that has no source lineage, in audit and in enforce; in enforce
  it denies a row whose classification is blank; and a `redact` effect masks
  in audit as well as enforce. Ruling those out would take a read of the
  rows. **Disclosure:** the value and its `reason` reveal only whether Nomos
  is on and why a read went row by row (the session, the caller, the bundle
  or the schema); with Nomos on and the view in class mode the reason is
  always `nomos_on`. A finer cause would be read off the catalog, which
  holds the classes of hidden rows, so it is not given to readers.
- **`readable(alias, key)`** is a SQL boolean: true when a served row returns
  `key` as stored, false when the redact transform masks or drops it, NULL
  when the view cannot tell before `finish` (no usable class, an unmappable
  classification, or an overlay with disagreeing classifications). A clause
  on an unknown value matches nothing (D5). `text` also governs the
  searchable-text copy, which takes `text`'s class. Identifier attributes are
  masked, not pseudonymized, on reads, so `pseudonymized()` is empty.
- **Disclosed side channels.** The query planner and any index scan still
  read hidden rows before the predicate drops them (a BM25 window, for
  example), so timing and per-scan limits can depend on hidden rows.
- **Caller rows.** `finish` masks each row by its own subtype and
  classification, while `predicate` and `readable` read the stored row. So a
  caller passes rows as stored: the full `attrs` (at least the stored
  `classification`), or no `attrs` at all so the filter loads them, and the
  `subtype` column for nodes. A node row without `subtype` is refused, never
  guessed. `open` and `open_many` record nothing and do not commit, but
  their catalog and overlay SELECTs leave a transaction open on a
  non-autocommit connection until the caller commits; `finish` commits the
  connection after it records, as the per-row filter always has. A view
  opened with Nomos off re-checks the mode in `finish` and goes row by row if
  Nomos has been turned on since.
- **Shadow mode.** `force="shadow"` runs the class path and the per-row path
  on the same rows and refuses the read on any difference in rows, order,
  attrs, class decisions or `readable()` answers. `force="per_row"` takes the
  exact path.

Decision writes are batched per read-filter pass (one statement, not one
INSERT per row × source) and use `ON CONFLICT DO NOTHING`, so the recorded
`target` is deterministically the **first-observed** access for that decision
identity: the log records which distinct decisions exist, not every time each
was made. Poros exchange decisions, publish-gate decisions, and admission
administration may include a generation in the identity because the generation
or admission is the object being authorized.

Retention: the publisher prunes rows older than
`OKG_NOMOS_DECISION_RETENTION_DAYS` (default **90**; a positive integer —
anything else is a hard error, never a silent disable; effectively-keep-all
is a very large value) on every publish, and `okg nomos decisions-prune
[--days N] [--apply]` runs the same path by hand (it dry-runs by default;
pass `--apply` to delete). Pruning never touches
a row referenced by federation requests/pins/route events, Poros
incorporation events, external claims, or policy/config activations.
Because the write path is insert-once, a pruned decision that recurs is
recorded again on its next occurrence.

## MCP Read-Surface Visibility And Scaling Classification

Only `enforce` mode can hide a row: `off` applies no filter and `audit`
is log-and-allow. Every agent-visible MCP graph read surface falls into
one of four classes. Non-MCP server-side readers (publisher, enrichers, doctor,
consistency gates) run as the substrate itself and are out of scope for
per-subject visibility.

| Class | Meaning | Surfaces |
|---|---|---|
| Bounded row read, exact Nomos semantics | Fetch window is bounded; every row passes the per-row lineage filter, except whole-record text reads, which decide once per decision class with the same answer per row (see Decision identity and retention) | `inspect(node)`, `inspect(text)` (class-decided), `expand(neighbors)`, `expand(path)`, `search` (lexical/alias/semantic), `filter`, `map`, `aggregate(node)` (bounded by the anchor node's degree — a super-hub can still be expensive under enforce), sample nodes in `inspect(subtype)`, readiness sample |
| SQL aggregate, safe only in off/audit | Whole-graph/whole-scope SQL aggregate; exact because no row can hide; per-row audit decisions are not written for the aggregate read (the accepted `inspect(graph)` trade) | `inspect(graph)`, `inspect(subtype)` counts/frequencies/routes, `aggregate(edge)`, `preview_advance` id-diff, schema digest |
| Exact enforce-safe aggregate under a row budget | Under `enforce`, the per-row filter runs only when the input set fits the budget (~5k rows) | `inspect(graph)` (small graph), `inspect(subtype)` (nodes + incident edges), `aggregate(edge)` (matching edges), `expand(neighbors)` `total_count` (matching edges, when the page truncates), `preview_advance` (delta rows, filtered per pin) |
| Enforce-large omitted/refused | Above the budget, visibility-sensitive fields are omitted (`null`, `truncated=true`, a `note`) so hidden-source cardinality cannot leak | the four surfaces above, plus `expand(neighbors)` (`total_count: null` with `total_count_basis: omitted_over_budget_under_enforce`), the `okg://schema-digest` resource (subject-less, so always omitted under enforce) and `query` (graph-backed SQL refused under enforce; the implementation retains `run_sql` error-code and helper names) |

Failure behavior is fail-loud everywhere: a budget probe or SQL-aggregate
error raises; nothing falls back to unbounded materialization or to
unfiltered output.

Pagination semantics under `enforce`: bounded-fetch operators
(`filter`, `map`, `expand(neighbors)`, `expand(path)`, semantic search)
fetch `limit + 1` raw rows and compute `truncated` from the **pre-filter**
count, so a hidden row can never mislabel a page as complete; a page may
therefore under-fill (fewer than `limit` rows with `truncated=true`) —
re-call with a higher limit or narrower predicate. Offset pagination
iterates raw row positions, so iterating pages never skips a visible row.
`expand(neighbors)` keeps its page fill under `enforce` — a hidden row
inside the window lets the next raw row take its place — so a page can
consume more raw positions than it returns rows. Its `next_offset`
therefore advances by **raw positions consumed**, not by `limit`, because a
cursor advancing by `limit` would re-serve the row that was pulled forward.
With nothing hidden the two are the same number.
Lexical and alias search filter their full candidate set before
paginating and report exact-visible counts.

## Trace Check

```sh
uv run okg nomos trace \
  --decisions .nomos-demo/decisions.json \
  --bundle .nomos-demo/compiled-policy.json \
  --out .nomos-demo/refinement-state.json \
  --json
```

The trace check fails if a restricted/private public output lacks a
`redact` or `declassify` decision with matching obligation evidence.
