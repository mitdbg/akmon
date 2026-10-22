---
audience: data owner, operator, developer
type: reference
canonical_id: cli-surface
---
# AKMON command reference


The data-owner, operator, and developer reference for the hot-path
`uv run okg` commands. It is not exhaustive — run `uv run okg --help` (and
`uv run okg <verb> --help`) for the full command tree, the source of
truth. The default listing leaves out a few internal commands used by this
repository's own agents, CI and benchmarks, and names them in its closing
note; each still runs and has its own `--help`. It also leaves out, without
naming them, the cross-deployment exchange (federation) commands, which this
release does not support. The commands cover four
responsibilities: import information, store the graph, publish a version, and
search and use data.

For the software boundary behind these commands, see
[component responsibilities](../develop/architecture/overview.md). The
identifiers below are the public CLI names.

The machine-readable [error code registry](error_codes.yaml) is the authority
for stable error identifiers and recovery hints.

**Conventions:**

- `--json` (default under `OKG_AGENT=1`) — machine-readable envelope
  on stdout. Errors land on stderr as a single JSON object with
  `error.code` + `error.fix_hint`.
- Deployment-scoped commands resolve the database from the deployment
  manifest. They use `OKG_DSN` only when that manifest explicitly references
  `${OKG_DSN}`. Commands that do not name a deployment may accept `--dsn` or
  `OKG_DSN` as direct operator input.
- Write commands accept `--apply`; under `OKG_AGENT=1` they refuse
  without it.

**Renamed flags.** The bare `--profile` flag used to select seven unrelated
things depending on the command. Each of those flags now has a name that says
what it selects. The old spellings still work for one release: they are hidden
from `--help`, and each use prints one warning line on stderr (never stdout,
so `--json` output stays valid). `okg install --profile` and
`okg init --profile` keep their name: there it means the deployment profile.

| Old spelling | New spelling | Commands |
| --- | --- | --- |
| `--profile dev\|prod` | `--target` | `okg up`, `okg deployment up`, `okg deployment backup verify`, `okg deployment read-status` |
| `--profile general\|dogfood\|ci` | `--checks` | `okg deployment ready`, `okg deployment prove-live` |
| `--readiness-profile` | `--checks` | `okg up`, `okg deployment up`, `okg deployment nomos-source-lineage-rollout-audit` |
| `--profile deep\|off` | `--capture` | `okg run`, `okg ingest` |
| `--profile-collector` | `--capture-collector` | `okg run`, `okg ingest` |
| `okg profile` | `okg perf` | the capture inspection verb |
| `--profile` | `--permissions` | `okg agent-runtime-launch` |
| `--profile` | `--scope` | `okg search` |
| `--search-profile` | `--scope` | `okg chat setup` |
| `--tool-profile` | `--tools` | `okg mcp-serve` |
| `--tool-profile-ceiling` | `--tools-ceiling` | `okg mcp-serve` |
| `--profile` | `--provider` | `okg nomos provider fixture` |

Verbs are grouped by category, alphabetical within a category.
Sub-verbs follow their parent.

---

## Publisher lease failure

`Publisher.reserve()` raises `PublisherLeaseHeld` when another publisher or
source-sync publication owns the deployment's coordination guard.
`PublisherLockHeld` is a compatibility alias for the same Python exception;
structured errors use `publisher_lease_held`.

```json
{
  "error": {
    "code": "publisher_lease_held",
    "message": "publisher lock is held for deployment='<name>'",
    "fix_hint": "wait for the active publisher or source-sync publication to finish, then retry",
    "data": {
      "deployment_name": "<name>",
      "coordination_mode": "dbos",
      "advisory_key": [123, 456],
      "wait_seconds": 30.0
    }
  }
}
```

When source-sync publication has priority, `data` instead includes
`reason: "source_sync_active"` and `blocking_workflow_id`. The command fails
rather than running a concurrent publisher or reporting the held lease as
success. Wait for the owner or inspect the workflow the error names before
retrying.

## Operator

The daily-driver surface. Run a publish cycle, import sources,
inspect what was accepted, and trace a record through storage and derivation.

### `okg admission`

`okg admission {list, show, submit, decide}` — inspect admission
envelopes and lifecycle evidence.

Common shape across the four sub-verbs: `--deployment`, `--dsn`,
`--json`. Each sub-verb has its own required args.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_admission`)

#### `okg admission list`

`okg admission list [--status <s>] [--mode <m> | --held] [--source <s>] [--limit N]`
— list recent admission envelopes. `--held` keeps the review- and
quarantine-mode envelopes, the runs that wait for `okg admission decide`.

#### `okg admission show`

`okg admission show <admission_id>` — full envelope with events
and record refs.

#### `okg admission submit`

`okg admission submit --deployment <d> --producer-id <id> [--producer-kind agent] [--intent review_proposal] (--payload-json '<json>' | --payload-file <file>) [--authority-json '<json>'] [--check-summary-json '<json>'] [--idempotency-key <key>]`
— submit a review-mode node/edge payload through DBOS. Intent is a bounded
machine identifier; invalid values fail before enqueue. Deployments may opt
into the bounded, fail-closed runtime approver described in
[deployment authority guide](../runbooks/deployments/authority.md).

#### `okg admission decide`

`okg admission decide <admission_id> --decision {accept|reject|quarantine} [--reason ...]`
— accept, reject, or quarantine a review-mode admission. Also opens
lens-promotion envelopes with `--action promote --request-id <uuid>`.

### `okg alias`

`okg alias {add, collisions, list, remove, seed-from-subtype}` — manage the
alias dictionary (the manual operator path for `entity_mention`
resolution).
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_alias`)

#### `okg alias add`

`okg alias add <surface> <canonical> [--scope <s>]` — record one
alias.

#### `okg alias list`

`okg alias list [--scope <s>] [--limit N]` — read the dictionary.

#### `okg alias collisions`

`okg alias collisions [--limit N]` — list ambiguous alias values that
deterministic catalog seeding refused at the active catalog version.

#### `okg alias remove`

`okg alias remove <alias>` — drop one entry.

#### `okg alias seed-from-subtype`

`okg alias seed-from-subtype --subtype <name> --field <attr> [--apply]`
— bulk-import aliases from every node of a subtype's attribute value.

### `okg catalog`

`okg catalog {load, history, ownership, repair-restamp-ownership}` —
compose / install deployment catalog (modules + bridges).
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_catalog`)

#### `okg catalog load`

`okg catalog load --deployment <path-or-environment>
--writer-scope {deployment_root,environment} [--environment-id <id>]
[--apply]` — compose subtypes + narrowings + aliases + identifier patterns
from the deployment's modules + bridges. Dry-run by default; `--apply` writes
a fresh `catalog_versions` row. A deployment directory uses
`deployment_root`; a sealed environment uses `environment` plus its explicit
environment id. Ambient `OKG_ENVIRONMENT_ID` state does not select authority.

#### `okg catalog ownership`

`okg catalog ownership {diagnose,claim} --deployment <path-or-environment>
--writer-scope {deployment_root,environment} [--environment-id <id>]` — inspect
or claim the same explicit catalog-writer authority a publish will use.
Environment scope requires an environment id; deployment-root scope rejects
one. Recovery commands in the result repeat the selected authority flags.

#### `okg catalog history`

`okg catalog history [--limit N]` — list recent catalog versions
from `okg.catalog_versions`.

#### `okg catalog repair-restamp-ownership`

`okg catalog repair-restamp-ownership --deployment <name> [--dsn ...]
[--branch default] [--apply]` — return graph nodes that an earlier catalog
load took over to the source that wrote them. In earlier versions of okg, a
catalog load that changed a subtype's searchable-text fields rewrote every node of that subtype under the source `_catalog:restamp`, and
the original source could then never retract those nodes. The command reads
the row version each takeover replaced and reports the nodes per original
source.

- Without `--apply` it writes no graph rows, takes no branch lock, runs no
  DDL and rolls its transaction back. With `--apply` it takes the branch
  write lock before reading, so graph writers wait for it.
- A node whose source the branch history cannot name is reported under its
  reason (`no_earlier_version`, which also covers an earlier version
  overwritten in place, compacted away or left in a node table that schema
  branching has since replaced; `ambiguous_earlier_version`;
  `earlier_version_deleted`; `earlier_version_unattributed`;
  `earlier_version_no_lineage`) and left alone, and the command exits 1.
  With `--apply` the nodes it could resolve are still written and
  committed; the output says so.
- A node the source stopped emitting while `_catalog:restamp` owned it is
  not in the source's stored record set, so the source's next normal run
  does not retract it, even after the repair. The output lists those nodes
  per source and the follow-up that retracts them and nothing the source
  still emits: `okg ingest --deployment <name> --include <source>
  --no-predecessors --inline --reset-cursor`. Without `--no-predecessors`
  the source's predecessors would be reset too. If the source's current read
  emits no records, add `--allow-full-retraction <source>`; the
  full-retraction guard refuses that run otherwise. A source with no stored
  record set gets no command: a reset-cursor run would not retract its
  nodes, so the output marks it for inspection.
- Rerunning after a successful apply finds nothing.

### `okg doctor`

`okg doctor [--check <name>] [--deployment <d>] [--dsn ...]` — verify
Postgres and deployment health.

Built-in checks include: `extraction`, `extraction-wrap-invariant`,
`acceptance-criteria`, `extractors`, `integrity`, `admission`,
`derived-admission`, `agent-skills`, `agent-runtimes`, `mcp-surface-freshness`,
`external-live-authority`, `nomos`, `source-auth`, `database`,
`grant-registry-drift`, `pending-grants`,
`declared-attr-indexes`, `vector-sets`, `sources`. Run `okg doctor --check <unknown>` for the authoritative set.
Deployment checks come from `deployment.yaml`.
With `--deployment`, `vector_sets` reports per set: coverage at the pin
(sampled above 50,000 nodes with non-blank text: `method: sample`, `missing_upper_95`),
unpublished writes, index state, spend, fill age (flagged when never or
over 10 publishes old), worker fill errors and config drift, plus
old-column vectors no set owns, as warnings. `--check vector-sets` is
exact, untimed, and exits 1 on any finding. State is explicit: `clean`
proves readiness; `stale` found outdated state; `unavailable` could not prove
state; `command_failed` reports a failed helper. The last two block readiness.
With `--deployment`, doctor fails `latest_publish_blocked` when the newest
publish attempt was blocked, and names the fix commands.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_doctor`)
**Related:** [`runbooks/generation-blocked.md`](../runbooks/troubleshooting/generation-blocked.md)

### `okg generation`

`okg generation {list, describe}` — inspect published Chronos
checkpoint generations.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_generation`)

#### `okg generation list`

`okg generation list [--deployment <d>] [--limit N]` — recent
published generations from `okg.v_generations`.

#### `okg generation describe`

`okg generation describe <generation_id>` — checkpoint and graph counts.
New checkpoints report the publisher-selected `core_revision`,
`deployment_revision`, and full-repository `deployment_tree_sha` under
`repository_provenance` with `status: exact`. Legacy checkpoints report
`status: unavailable` and null values, which cannot prove an exact pair.
`ingest_completeness` says which sources were not current when the version
was published (see `okg ingest`); it is null for a version published before
that record existed.

### `okg ingest`

`okg ingest --deployment <d> [--include <list>] [--exclude <list>] [--no-predecessors] [--no-publish] [--force-enrichers] [--progress]`
— drive the selected sources in a deployment's `source_registry` in
topological order; publish. `--no-predecessors` keeps an explicit
`--include` exact instead of expanding through predecessor closure.

`--no-publish` leaves accepted changes in working state for inspection; it
does not make them searchable. The [maintained-knowledge
lifecycle](../start/data-lifecycle.md) defines that boundary, while
[Consistency architecture](../develop/architecture/consistency.md) defines
the evidence required before a derived result may be reused.

With no `--include`, ingest drives every source in the registry except those
setting `enabled_by_default: false` (an opt-out for sources like an
unconfigured `dblp` crawl). A source leaving the flag unset still runs, so a
hand-authored registry keeps its all-sources behavior. Predecessors of a
running source are pulled in even when opted out; name an opted-out source
with `--include` to run it.

`--force-enrichers` is the one-shot recovery lever for an ingest whose source
scan is unchanged but whose derived graph state must be recomputed. It
bypasses the ingest `no_source_work` skip and the publisher's unchanged-input
reuse, then runs enrichers, gates, and a new checkpoint. It passes through
inline and DBOS-queued ingest and is not persisted: the next unforced
invocation resumes normal skip behavior. Do not use it routinely instead of
fixing a source or catalog delta: it requests complete computation for one
cycle, and proves neither that the previous reuse decision was unsafe nor that
a resulting claim is true. See [Incremental maintenance and
consistency](../develop/architecture/consistency.md).

When some selected sources fail and others succeed, ingest still publishes
the successful sources' changes and exits 1. The failed sources' own writes
are undone first. The new version records which sources are not current
under `ingest_completeness` in its metadata (shown by `okg generation
describe`): `status` is `incomplete`, and `failed_sources` and
`dependency_blocked_sources` name each source by its registry name and
`ownership_id`. A source stays listed until a run of it succeeds, or until it
is removed from the source registry; later publishes keep the record. The
worker behaves the same way, and other sources keep syncing.

The record only comes from runs that publish. A run in which every selected
source fails publishes nothing, so a source that fails in runs of its own is
not listed there; its failed admissions are the record of that. Only when a
failed source's undo did not finish does the run still cut a `blocked`
checkpoint that demands an exact repair run (`repair_required`). That repair
run publishes like any other run once its failed sources' changes are undone,
even if they fail again, and the hold ends.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_ingest`)

### `okg deployment add-source`

`okg deployment source-types` lists the supported types and their settings.

`okg deployment add-source <deployment> <name> --type <type> [--setting k=v ...] [--credential ENV_NAME ...] [--registration <asset.json>] [--deployments-root <dir>] [--dsn <dsn>] [--apply] [--yes]`
— the guided flow of [Connect your data](../use/sources/index.md#connect-the-source):
preview without `--apply`; with `--apply` save, import, check, publish, and
print one searchable sample with its source; re-run the same command to retry
a failed step. `--registration` takes a packaged connector declaration instead
of `--type`; `--manifest <file>` takes a
[source manifest](source-manifest.md) instead of `--type`, `--setting` and
`--credential`.
**Implementation:** `src/okg/substrate/cli/source_add.py`

### `okg source draft`

`okg source draft <file> [<file> ...] [--type <name>]` or
`okg source draft '<url>' [--auth <mode>:<ENV> ...] [--type <name>]`
— read up to 200 sample records from local JSON, JSON Lines, CSV or YAML files,
or from one GET of a JSON API, and print a commented
[source manifest](source-manifest.md) to standard output: the record list
path, paging, field types, dates, nested types and links, a key, a title
and a text field. The `key:` and `deletion:` lines are marked `GUESSED`.
`--auth` names environment variables, never values: `bearer:ENV`,
`header:Name:ENV`, `basic:USER_ENV:PASSWORD_ENV` or
`client_certificate:CERT_ENV:KEY_ENV[:CA_DIR_ENV]`. It writes nothing and
never adds the source; preview the reviewed file with
`okg deployment add-source <deployment> <name> --manifest <file>`.
See [Bring your own data with a manifest](../use/sources/manifest.md#draft-a-manifest-from-your-data).
**Implementation:** `src/okg/substrate/cli/source_draft.py`

### `okg deployment remove-source`

`okg deployment remove-source <deployment> <source> [--deployments-root <dir>] [--dsn <dsn>] [--apply] [--yes]`
— the counterpart of `add-source`, described in [Remove a
source](../use/sources/index.md#remove-a-source). `<source>` is the registry
name or the ownership id. Without `--apply` it previews the records and
relationships to retract and the registry entry to remove; with `--apply` it
disables the source (stopping scheduled and default imports of it; a re-run
retracts records an explicit import wrote before the publish, and refuses
records that return after it, tracked in issue 3265), retracts its records
through the reviewed source retirement, publishes, removes the registry entry once the published version
records the retirement, re-claims the deployment identity, and updates the
access policy. A source that never reached the graph loses only its registry
entry. `--apply` prompts for confirmation; `--yes` is required when input is
not a terminal or with `--json`. Run the same command again to resume a
failed step. Refusals are `remove_source_refused`.
**Implementation:** `src/okg/substrate/cli/source_remove.py`

### `okg deployment conformance`

`okg deployment conformance schema --kind <request|receipt> [--protocol-version <v1|v2>]`
— print the public JSON Schema of one external-distribution conformance wire
document.

`okg deployment conformance validate --request <request.json> [--receipt <receipt.json>] [--trusted-external-review <review.json>] [--json]`
— check inert conformance JSON. Nothing is executed, no database is opened and
no network call is made, so a consumer can run it from outside any checkout.
Without `--receipt` only the request is checked; with one, the complete
exchange. Exit 0 is valid, 1 a refusal carrying a `diagnostic`, 2 a usage
refusal on stderr — an input the command could not read, or flags that do not
go together.

`--trusted-external-review` takes the acknowledgement record a reviewer
published out of band — the validator has no way to fetch it, so a receipt
claiming a `current` acknowledgement is otherwise always refused
`conformance_external_acknowledgement_pending`. The file is read as data by
the same rules as the request and the receipt: duplicate keys refused, unknown
fields refused, every failure a stable code. It requires `--receipt`, since
only a receipt carries an acknowledgement. Supplying it adds
`acknowledgement_bindings` to the `--json` result — the consumer id, source
revision, acknowledged schema digest and status the receipt asserted, and the
names of any fields on which the review disagreed. Only a `current`
acknowledgement is weighed against the review, so against a `historical` one
that list is always empty. Without the flag the result keys and bytes are
unchanged.
**Implementation:** `src/okg/substrate/cli/deployment.py` (`_cmd_deployment_conformance_validate`)

### `okg provision`

`okg provision --deployment <d> [--include <sources...>] [--readiness-sources] [--no-predecessors] [--publish-once]`
— bootstrap a deployment through extensions, schema migration, catalog apply,
source ingest, and publish. `--readiness-sources` selects the standardized
agent-context source set and publishes once after ingest so readiness checks do
not depend on broader registry closure.

| Exit | `--json` status | Meaning |
| --- | --- | --- |
| 0 | | Provisioned: the publish proof passed. |
| 8 | `capacity_warning` | Provisioned, and the publish committed, but it held the publisher lock longer than the capacity bar: the smaller of the running statement timeout and 900 s, divided by `publisher.lock_hold_headroom_factor` (default 2, so 450 s). `capacity_warning` and `publish_proof.capacity_warning` carry the hold, the bar and how the bar was derived. A caller that treats only failures as fatal proceeds; the Helm chart's bootstrap init container does. |
| 1 | | A step or the publish proof failed, for example a blocked generation, no row delta, dead-letter work, or an unmeasured lock hold. |
| 2 | | Usage or extension error, as for every `okg` command. |

**Implementation:** `src/okg/substrate/cli/provision.py` (`_cmd_provision`)

### `okg remediate entity-merge`

`okg remediate entity-merge --survivor <canonical-id> --retire <duplicate-id>
--deployment-name <d> [--branch <b>]` — plan a bounded duplicate-entity repair.
Dry-run is the default. The result identifies attribute and edge conflicts and pins
the published checkpoint, deployment catalog, complete incident-edge footprint,
and `plan_digest`; it does not write graph state.

After reviewing the plan, apply exactly that digest:

```bash
uv run okg remediate entity-merge \
  --survivor region:signal \
  --retire region:signal-batch-7 \
  --deployment-name cooper \
  --expected-plan-digest <digest-from-dry-run> \
  --accept-survivor-attrs \
  --actor <operator-id> \
  --apply --json
```

Every attribute that differs between the two nodes needs a disposition, or the
command refuses rather than guess:

| Disposition | Effect |
| --- | --- |
| `union` | Keep every entry from both lists. Both values must be lists. |
| `keep_survivor` | Take the surviving node's value. |
| `keep_retiree` | Take the retired node's value. |

A disposition comes from `--merge-attr <name>=<disposition>` (repeatable), or
from the schema, where an attribute can declare `annotations.merge_semantics`.
`--merge-attr` outranks the schema; `--accept-survivor-attrs` fills in
`keep_survivor` only for attributes neither covers, so it cannot undo a
declared `union`. It never applies to an attribute that is a list on both
sides: taking one side discards entries, so a differing list always needs its
own disposition. Use `union` for a citation list such as `grounded_in`.

Apply also refuses a changed published checkpoint, catalog, graph footprint,
unsafe narrowing, edge identity conflict, or self-loop. Two edges that assert
the same thing but were written by different extraction runs are recognised as
one edge and deduplicated; what the surviving edge does not carry over is
recorded in the plan under `provenance_variance`.

Different software producers are never treated as equivalent merely because they
reach the same `(source node, edge type, destination node)`. The deployment catalog
must declare `entity_merge_equivalence` on that exact edge narrowing, including
the oriented survivor/retiree producer and provenance pair, the producer-owned
audit attributes that may differ, and the supported confidence rule. The service
also requires equal semantic attributes, source records, normalized Nomos
lineage, and effective read-trust class, plus a survivor confidence at least as
strong as the retiree. There is no per-edge CLI
override: the CLI approves and digest-binds the destructive node repair, while
the versioned deployment catalog owns repeatable assertion semantics. Missing,
reversed, malformed, or nonmatching policy continues to block.

The declaration is an annotation on the deployment's `EdgeNarrowings`
attribute; catalog composition expands it onto the exact source subtype, edge
type, and destination subtype:

```yaml
annotations:
  edge_archetype: references
  src_subtypes: DocumentChunk
  dst_subtypes: ExperimentalUncertainty
  entity_merge_equivalence:
    schema_version: okg.entity_merge.edge_equivalence.v1
    survivor:
      producer: _enricher:mention_grounding
      provenance: derived_deterministic
    retiree:
      producer: _enricher:physics_extraction
      provenance: explicit
    audit_attrs: [evidence_basis, provenance_kind, src_kind]
    confidence_rule: survivor_gte
```

Governance and trust-marker attributes cannot be declared audit-only. This
capability does not silently enable any deployment: the measured Cooper repairs
remain blocked until Cooper publishes its reviewed rule in its own catalog.

Every safe edge deduplication includes versioned `assertion_variance` and
`equivalence_evidence` in the dry-run plan and durable receipt, retaining the
discarded assertion fields, exact catalog rule, and read-trust profile evidence
that justified keeping the already-live edge unchanged. A rule, profile, or
assertion change therefore changes `plan_digest` and makes a stale apply refuse,
and a merge is refused when rewriting the survivor's attributes would shrink its
recorded provenance. A successful apply returns a durable receipt in
`awaiting_publish`; the deployment's publish coordinator must cut and verify the
repair generation before treating the repair as live. `okg trace node
<duplicate-id>` reports the survivor and receipt after the retired node leaves
the live graph. Replaying the same apply requires the same survivor and digest
and does not mutate the graph again.

**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_remediate`)

### `okg library`

`okg library {list, show}` — AKMON runtime library refs (invariants,
linker routes, source modules, and deployment templates). Ontology definitions
remain available under `library/ontologies/` and are selected through a
deployment's `modules:` list rather than `okg.ontology.*` dotted refs.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_library`)

#### `okg library list`

`okg library list [--kind {invariants|linker_routes|modules|templates}]`
— enumerate library refs.

#### `okg library show`

`okg library show <ref>` — print the resolved YAML for one ref
(for example, `okg.modules.openspec`, a compatibility source module; it is
not the PACT authority for changes).

### `okg metrics`

`okg metrics {--llm | --qa-snapshot} [--since 1h] [--deployment <d>]`
— roll-up metrics. `--llm` returns token / cost spend; `--qa-snapshot`
returns a QA-gate snapshot for the deployment's latest published
generation.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_metrics`)

### `okg run`

`okg run --once --deployment <d> --apply [--verbose] [--force-enrichers] [--include-llm-invariants] [--llm-invariant <name>] [--json]`
— run one publish cycle. The runtime worker owns continuous and scheduled
work. The cycle publishes what `okg ingest` or source sync already staged; it
runs no source itself.

By default the command prints a short summary: whether it published, the
graph version, records and relationships added, updated and retracted, the
graph totals, and, with `--include-llm-invariants`, the LLM-invariant fire,
call-failure and parse-error counts. Source results come from `okg ingest`,
not from this command. Every warning the cycle reports (a blocked publish,
excluded sources, consistency violations, failed or degraded views, rejected
derived output, pending migrations, or any stage whose status is a failure or
a value the summary does not know) prints after it with a `fix:` line that
inspects the result, usually `okg generation describe <version>`. A refusal
prints its code and fix hint on stderr.
`--verbose` adds the full cycle report: stage timings, gates, views and
receipts. `--json` prints the full cycle report plus the same summary under
`run_summary`.

The exit code follows the publish verdict. Both paths classify the cycle
summary the same way, so the same cycle gives the same outcome and code.
`--json` carries the verdict as `run_summary.outcome` and
`run_summary.exit_code` on the in-process path, and as top-level `outcome`
and `exit_code` on the runtime-queue path (runtime enabled, no `--inline`),
which prints an `outcome=` line instead of the summary.

| Exit | Outcome | Meaning |
| --- | --- | --- |
| 0 | `published`, `no_change` | The cycle published a new graph version, or ran and found nothing new, so the current version stays. Warnings on a published cycle, such as excluded sources or rejected derived output, keep exit 0; they print with their `fix:` line. |
| 0 | `skipped` | The runtime did not run the cycle, for example while a source sync is active. Nothing was checked and nothing was published; readers still see the previous version. |
| 3 | `blocked` | A gate refused the graph version the cycle built. Readers still see the previous version. A block wins over a failure status reported with it. |
| 1 | `failed`, `unknown` | The cycle reported a failure, or a status nothing classifies, or no cycle summary. |
| 1 | `not_awaited` | Runtime queue only: the command stopped waiting before the cycle finished. |
| 1 | | A refusal or error with its code and fix hint. This includes a refusal before the cycle and an `--include-llm-invariants` step that fails after the cycle published. In that case the graph version is published even though the command exits 1. |
| 2 | | Usage or extension error, as for every `okg` command. |

`--force-enrichers` applies the same one-shot recovery semantics to the
publish cycle: bypass unchanged-input reuse, run enrichers and gates, and
cut a checkpoint. The invalidation receipt records `operator_forced`; the
next run is unforced unless the flag is supplied again. The [consistency
architecture](../develop/architecture/consistency.md) explains why missing
reuse evidence causes more computation and why a passed publication check is
not proof of factual truth.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_run`)

### `okg search`

`okg search --deployment <d> --query <text> [--method lexical|semantic|alias] [--limit N] [--offset N] [--generation-id <id>] [--select f1,f2] [--subtype <name>] [--scope <name>] [--source-family <name>] [--max-per-parent N] [--max-chars N] [--attr-filter key=value] [--embedder <spec>] [--allow-unverified-embedding-provenance] [--json]`
— search nodes at the pinned generation. The
[search guide](../use/search/index.md) explains how to keep the
`generation_id` pin across related reads.

The JSON shape matches the [MCP result envelope](mcp.md#result-envelope).
`--select` is comma-separated and repeatable; an unknown field fails with
`select_unknown_field` and lists `valid_fields`. `--attr-filter` is repeatable
and exact-match; a value is a JSON scalar when it parses as one (`true`,
`null`, numbers, quoted strings), otherwise a raw string.

Human output is the read commands' text form; `--select` adds a score.
Control characters print as spaces.

`--scope <name>` selects a deployment-declared search profile: the subtypes
it counts as content, with their refinements. `--scope all` is always
unscoped. A `search.default_profile` applies when you name neither `--scope`
nor `--subtype`; a note says so:

```
! search profile analyst applied: 10 subtypes
```

`--source-family` (repeatable), `--max-per-parent`, and `--max-chars` add the
authority, diversity, and preview controls; `--scope all` turns off every
profile-declared one. Their envelope fields, applicability, empty-page counts,
and refusals: [MCP search methods](mcp.md#search-methods).

#### `--method semantic`

The query embedder is the search vector set's backend
([`embeddings`](../operate/configure.md#embeddings)); `--embedder` and
`OKG_MCP_EMBEDDER` may repeat it (deprecated, a mismatch fails). A
standalone query needs `--embedder`, else `semantic_embedder_not_configured`.
`--embedder` with another method is an error.

Pages carry `embedder_spec`, `embedder_model_id`, `embedder_source`, the
plan fingerprint and `embedding_coverage`. They rank the set once its fill
completed by the pin ([semantic search](mcp-results.md#semantic-search)),
else the node columns, where `semantic_index_empty` refuses a scope with no
vector and `embedding_model_skew` another model.
`--allow-unverified-embedding-provenance` admits node-column rows of unknown
model as `embedding_provenance: unknown`; a set refuses it. With embeddings
disabled, semantic calls refuse.

Example:

```bash
okg --json search \
  --deployment my-graph \
  --query "context efficient" \
  --method lexical \
  --limit 10 \
  --offset 10 \
  --select node_id,subtype,excerpt
```

Default lexical rows are lean (`node_id`, `canonical_node_id`, `subtype`,
`method`, `match_kind`, `score`, `excerpt`, provenance). Narrowing `--select`
drops the match-context fields too, so `node_id` alone gets a minimal row.
Request `--select attrs` only when the excerpt is insufficient.
**Implementation:** `src/okg/substrate/cli/_read_side.py`

### `okg inspect`, `expand`, `filter`, `aggregate`, `map`, `query`

`okg <operator> [--deployment <d>] [--<argument> <value>] [--args <json>|@<file>|-] [--json]`
calls one [MCP read tool](mcp.md#tools); `OKG_DEPLOYMENT` names the default.

Each argument is a same-named flag; `--help` gives types and examples.
Values are JSON, else a string or comma list; `--args` takes the
object. `expand`, `inspect` and `query` take one argument bare. `okg guide`
shows pipe examples, then a read-only MCP client's connect text; `--json`
adds bytes and sha256.

`--where k=v` repeats (`!=`, `>`, `<`, `>=`, `<=`, `~` prefix); `--type`
names a subtype. `--jsonl` prints rows, `--ids` ids,
`--all` every page; status to stderr. `-` or `--stdin` reads ids;
`inspect texts -` their texts (`--lines 1-40`).

Output is text (over 20 rows: a pipe hint); `--json` prints one document;
`--all` joins pages (`all_pages`); several stdin calls print an array.
Exit 0 served, even cut or capped (the `--all` cap prints `resume:`);
2 refused, or a page with no next call (stderr; `retry: okg ...` when
fixable); 1 error. `OKG_READ_DEBUG=1` shows timings.

`okg search` runs the MCP tool for a tool-only flag or
`--method grep|attrs|identifier_mentions`; there `--all` means every page.

```bash
okg filter --type ticket --where attrs.status=Open --all --ids | okg inspect texts -
```

**Implementation:** `src/okg/substrate/cli/_read_operators.py`

### `okg chat-instance`

`okg chat-instance {up,status,supervise,down,front-door}` manages the
deployment's Open WebUI container. `up` requires a separate, reachable app
database (`OKG_CHAT_APP_DATABASE_URL`), a session-signing key
(`OKG_CHAT_WEBUI_SECRET_KEY`; `--admin-secret-env` renames it, and it is not
an admin API token), and a resolvable graph DSN to check database isolation.
`down` removes the container and record, preserving chat history.

Declare `chat.image` (pinned `:vX.Y.Z` or `@sha256:`) and
`chat.instance_port`, or supply `--image` / `--instance-port` to `up`.
The chat image is not published: build `ops/openwebui/Dockerfile` (Open WebUI
v0.11.4), set `chat.image`, and add its digest to `TESTED_IMAGES` mapped to
`"0.11.4"` (see `ops/openwebui/README.md`).
Conflicts and untested images refuse; only the patched Open WebUI `v0.11.4`
image is tested. `up` also starts the container with the update check and
easter eggs off (`ENABLE_VERSION_UPDATE_CHECK=false`,
`ENABLE_EASTER_EGGS=false`); for a container started without them `up`
refuses `instance_settings_changed` until `down` and `up` again. Records live
under `${XDG_DATA_HOME:-$HOME/.local/share}/okg/chat-instances/<name>/`;
legacy `deployments/<name>/` records migrate, and duplicates refuse
`instance_state_ambiguous`.

`status` derives instance state and exit status from `GET /api/version` and
observes the `chat.mcp.port` listener separately. `supervise` restarts the
chat container, never mcp-serve: it reports `unmanaged: ["mcp-serve"]` and
`mcp_endpoint_dead` when that listener fails. A listener does not prove MCP
tool discovery.

`front-door` returns `503` at `/console` until `--console-bundle-index` or
`--console-base-aware` proves base-aware routing; `--console-upstream` then
supplies the proxy target, never `chat.deep_links.operator_ui_base`.

### `okg chat plan`, `sync`, and `readback`

Every verb here refuses a deployment that declares no enabled `chat:` block.
No profile ships one; `okg chat setup` writes it from `--provider`,
`--model`, `--instance-port` and optional `--provider-key-env`,
`--mcp-port`, `--scope` and `--search-subtype`, recording the
provider credential as a variable name only. `--configure-only` writes or
checks the block and stops. A re-run changes nothing, and an answer that
disagrees with an existing block refuses `chat_block_answer_conflicts`.
[Set up chat](../operate/chat.md) walks through the answers.

`okg chat setup --deployment <d>` is the one guided entry point: it runs
`chat-instance up`, creates or signs in to the admin account, registers
`chat.provider`, checks `chat.models.default` is served, starts mcp-serve
when `chat.mcp.port` is silent, runs `sync`, then asks the preset one
question with `tool_ids`, in a new chat titled "okg chat setup: test
question" that stays in the admin's chat list. It reports *configuration
converged* apart from *conversation ready*, exits 0 only when that stored
answer records a graph tool call the graph answered without refusing
(`tool_use_unobserved` when it records none, `graph_tool_calls_refused` when
the graph refused every call, `graph_tool_calls_failed` when no call reached
the graph). A turn slower than the request's timeout is read back until it
finishes, for up to the same time again (`probe_turn_timeout` after that).
On failure it names the step, cause, and
variable to fix before re-running. `OKG_CHAT_ADMIN_PASSWORD`
(with `OKG_CHAT_ADMIN_EMAIL`) creates the first account on an empty instance
and signs in afterwards; `OKG_CHAT_ADMIN_TOKEN` wins when set.
`chat.provider` declares `base_url` as the container reaches it and
`api_key_env`, the variable holding the key. Secrets are never printed;
reapply preserves accounts.

The verbs below take `OKG_CHAT_INSTANCE_URL` and `OKG_CHAT_ADMIN_TOKEN`.
Declare the preset's base model in `chat.models.default` and
`chat.models.builtin`; no `default` hides the preset.

A selected bundle supplies branding defaults and instruction assets; the
deployment may replace branding but only narrow the bundle's connections and
tools. `--install-plan <dir>` composes the bundle's contribution; on
`chat-instance up` it sets the bundle's site name and must be passed on each
`up`. Packaged
playbooks and logos remain `undetermined`, not applied.

`okg chat plan --deployment <d> --out <new dir>` writes deterministic
`chat-plan.json` for seven configuration surfaces without contacting the
instance; it records credential references, never values, and never
overwrites. Apply with `okg chat sync --deployment <d> --plan <file>`;
manifest drift refuses `chat_plan_drift`, and sync records `plan_digest`.
`okg chat readback --deployment <d> --plan <file> --out <new dir>` compares
live settings read-only and exits nonzero on `chat_readback_deviation`.

Graph connections use the bearer named by `chat.mcp.auth_token_env`; an
undeclared credential means `auth_type: none`, and a declared but unset one
refuses `mcp_connection_credential_unresolved` before any change. The
instance's verify route dials with the credential in your environment, not
the one the site stored, so it proves your token works, not that chat turns
use it. Readback therefore also compares the stored credential with the
variable's value, by keyed digest inside one run, and reports
`mcp_connection_credential_stale` when they differ, for example after a token
rotation without `okg chat sync`. Readback also deviates when it cannot make
that comparison: `mcp_connection_credential_unmeasured` when the Open WebUI
version hides the stored key (0.11.4 does not), and
`mcp_connection_duplicate_id` when two connections carry the same id. A
rejected credential is a deviation; a missing verify route remains unmeasured.

`chat.mcp.live_tools` declares `names`, `url`, and `auth_token_env` for a
separate live-service connection that never inherits the graph bearer. Tools
must also be granted under `chat.mcp.audiences.<n>.live_tools`; no audience
means no connection. Names overlapping graph operators refuse
`chat_bundle_live_tool_conflict`; undeclared names or audiences with different
live-tool sets refuse `chat_live_tool_slot_refused`. Connection filters do not
replace backend authentication or Nomos authorization.

Sync sets the browser's initial model to the deployment preset, which
carries its tools and prompt; `chat.models.default` remains the base provider
model. `chat.models.ui_default`, a member of `builtin`, opens on a raw model
instead, without graph tools or the preset prompt. After applying all
surfaces, sync checks the vendor's first-entry rule and refuses a wrong
selection or missing model row.

Sync also switches off Open WebUI features the site does not offer, and
readback reports any that drift back on: the What's New dialog (site default
and the signed-in admin), the vendor's suggested prompts, follow-up chips,
tag, retrieval-query and autocomplete generation, community sharing, user
status, calendar, automations, the arena and channels (a manifest may turn
those two back on), voice mode, and the preset's built-in knowledge tools.
Notes stay on. Voice mode and `Workspace > Knowledge` are per-user
permissions, so admins keep them.

Successful sync/readback confirms measured settings; inspect `undetermined`,
`dropped`, and `no_consumer` entries for gaps. Sync alone does not prove a
model can answer or used graph tools; `chat setup` does. The pinned vendor's
`POST /api/chat/completions` attaches the preset's tools only when the caller
lists `tool_ids: ["server:mcp:okg-<d>"]`. The nightly real-container arm
is still missing.

`okg chat-instance lifecycle-receipt --kind {upgrade,rollback,retire} --out
<new dir>` writes a files-only record and performs no upgrade, rollback, or
retirement. Upgrade receipts require clean readback; rollback receipts reverse
a recorded upgrade and refuse a baseline outside the tested set; retirement
receipts require a live read showing OKG-owned surfaces removed, not that the
container is gone. Record the baseline first, verify container changes
separately, and keep the app database and graph intact.
**Implementation:** `src/okg/substrate/chat/instance_cli.py`

### `okg chat codex-shim`

`okg chat codex-shim --codex <path> --codex-home <dir> --mcp-url <url>
--mcp-token-env <VAR> --mcp-tools <list>` lets the chat site answer with the
operator's ChatGPT plan through the official Codex CLI. It serves
`GET /v1/models` and `POST /v1/chat/completions` (streaming and not) on
`127.0.0.1:8790`, and only there. Every call needs `Authorization: Bearer`
with the value of `--token-env` (default `OKG_CODEX_SHIM_TOKEN`, at least 32
characters); without it the shim does not start.

**Cost and terms.** Every person using the chat site spends the operator's
personal ChatGPT plan. Sharing one person's access this way may breach
OpenAI's terms, and that is the operator's decision. Each turn is a fresh
Codex run that replays the whole conversation: a two-message chat used about
39,000 prompt tokens, and one grounded question about 121,000.

**Isolation.** `--codex-home` names a Codex home used only by the shim. Sign
it in once with `CODEX_HOME=<dir> codex login`. The shim never reads or copies
credentials. It refuses a home that holds `AGENTS.md`, `config.toml` or
skills, and refuses the operator's own home. Codex runs with an empty `HOME`
and bundled skills off, because `--ignore-user-config` alone still loads the
operator's instructions and skills. At start the shim runs `codex debug
prompt-input` and refuses if that context appears. Runs use a read-only
sandbox in a scratch folder outside every repository, with the shell and
other built-in tools off. A run that executes a command, changes a file or
searches the web is stopped (`codex_containment_breach`). The shim refuses an
untested Codex version at start and stops answering if the CLI changes
underneath it.

**Graph tools.** When the site sends its tools, Codex calls the graph through
`--mcp-url`, and every call is approved. The shim checks that the server
exposes only `--mcp-tools`, at start and every few minutes. Start mcp-serve
with `OKG_MCP_TOOL_ALLOWLIST` set to the same list. A request without tools
runs with no graph at all. The site's per-tool switches cannot narrow the
set, so the first answer in a conversation names the tools Codex has.
Codex's progress notes arrive as reasoning, apart from the answer.

Only models that can call tools under these limits are listed (on codex-cli
0.155, `gpt-5.5`). Failures return a named error, such as
`codex_not_signed_in` or `codex_rate_limited`, and never an invented reply.

Point `chat.provider` at `http://host.docker.internal:8790/v1` with
`api_key_env: OKG_CODEX_SHIM_TOKEN` and `tools: provider`. The site records
no graph tool calls of its own. With `tools: provider`, `chat setup` reads
the shim's `okg_codex.tool_calls` record and reports it as
`provider_reported`. It is ready only when at least one call answered.
**Implementation:** `src/okg/substrate/chat/codex_shim.py`

### `okg status`

`okg status --deployment <d> [--dsn ...]` — deployment state
summary (publishing stage, sources, watermarks).
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_status`)

#### Deployment status result

`okg status --json` returns a diagnostic snapshot:

```json
{
  "deployment_name": "acme-ops",
  "stage": "running",
  "publishing": {
    "latest_published_generation": "<checkpoint-id>",
    "latest_published_target_generation": "<checkpoint-id>",
    "latest_published_status": "published",
    "latest_published_catalog_version": 5,
    "latest_published_node_count": 120,
    "latest_published_edge_count": 240,
    "latest_catalog_version": 5,
    "pending_generation": null,
    "latest_attempt": {
      "checkpoint_id": "<checkpoint-id>",
      "status": "published | blocked",
      "created_at": "<timestamp>",
      "blocked": false,
      "blocked_detail": "<reason; failed: <names>; hint: ...> | null"
    }
  },
  "sources": [
    {
      "source": "github_issues",
      "last_reconcile_at": "2026-04-23T10:00:00Z",
      "projection_cursor": "<cursor>",
      "n_node_facts": 120,
      "n_edge_facts": 240,
      "last_observed_at": null
    }
  ],
  "source_counts": {"generation": "<checkpoint>", "available": true, "reason": null},
  "live_subtypes": [{"subtype": "document", "count": 10}],
  "errors": [{"code": "status_probe_failed", "message": "..."}]
}
```

`n_node_facts` and `n_edge_facts` are the source's node and edge rows in the
latest published generation, read from the count rollup the publisher
maintains, so they change only when a generation publishes, not during an
ingest. They are `0` before the first publish. When the published
generation has no count rollup, or `OKG_IVM_CONSOLE_COUNT_ROLLUP=0` turns
the rollup off, they are `null`, the table shows `?`, and
`source_counts` reads `{"available": false, "reason": "..."}`. This does not
change the exit code. `last_observed_at` is always `null`; it came from the
retired fact log.

`stage` is `scaffolded`, `migrated`, `running`, or `unknown`. Unobserved
snapshot values are `null`. `publishing` starts empty, and `live_subtypes` may
be absent when a probe fails before those fields are collected. Consumers must
inspect `errors`; missing fields are unavailable evidence, not a healthy result.
`next_actions` values, when present, come from the closed enum in
[Error codes](error_codes.yaml).

`latest_attempt` is the newest publish attempt on the default branch,
published or not; `latest_published_*` is the newest one that published.
They differ when a gate refused the latest publish. It is `null` when the
deployment has never attempted a publish, and absent when `stage` is not
`running` or `migrated` or when reading it failed (then `errors` names
`latest_publish_attempt_unknown`). An attempt retired with `okg runtime
supersede-checkpoint` stays `blocked`, so status keeps exiting 3 until a newer
publish succeeds. A publish refused before it records an attempt, such as a
provenance refusal, does not appear here.

| Exit | Meaning |
| --- | --- |
| 0 | The status was read and the newest publish attempt published, or there has been none. |
| 3 | The newest publish attempt was blocked. Readers still see the previous version, or none. One `error: latest_publish_blocked:` line on stderr names the attempt and the reason. |
| 1 | A probe failed, or the newest attempt records a status that is neither `published` nor `blocked`. `errors` names it. |

`okg status --watch` shows a `BLOCKED latest publish attempt` line when the
newest attempt was blocked, keeps refreshing until interrupted, and exits 0.

When the latest checkpoint is blocked in `repair_required` (a source failed
after another source in its run succeeded), every publishing source-sync run
that does not repeat the checkpoint's exact selection, run mode and scope is
refused before it starts and records no admission, so the other sources stop
updating. Status then carries `source_sync_hold` (the checkpoint,
`required_selection`, `failed_sources`, `not_admitted_since_block` and the
dry-run `escape_hatch_command`), prints one `error: source_sync_held:` line
beside `latest_publish_blocked`, and `--watch` shows a `HELD` line. `okg doctor
--check sources` fails with a `source_sync_held` error, or warns
`source_sync_hold_unchecked` when it cannot read the database; a read that
fails is `source_sync_hold_unknown`.

### `okg start`

`okg start --deployment <d>` — start the deployment's Docker
stack. Reads `deployment.yaml.docker.{compose_path, project_name}`
and delegates to `docker compose start`. Idempotent (Docker
no-ops on an already-running stack).
**Implementation:** `src/okg/substrate/cli/lifecycle.py` (`cmd_start`)

### `okg stop`

`okg stop --deployment <d>` — stop the deployment's Docker
stack. Data persists (the volume is not removed).
**Implementation:** `src/okg/substrate/cli/lifecycle.py` (`cmd_stop`)

### `okg restart`

`okg restart --deployment <d>` — restart the deployment's Docker
stack (equivalent to `okg stop` then `okg start`, atomic via
`docker compose restart`).
**Implementation:** `src/okg/substrate/cli/lifecycle.py` (`cmd_restart`)

### `okg fetch-deployments`

`okg fetch-deployments --repo URL [--branch B] [--source-name N] [--list]`
— clone (or `git pull`) the `okg-deployments` repo into the XDG
cache (`${XDG_CACHE_HOME:-$HOME/.cache}/okg/deployments/<slug>/`).
After fetching, `okg install --profile <name>` resolves any cached
profile via the search-path. The reference-deployments repository is not
published, and this copy names no default: pass `--repo` or set
`OKG_DEPLOYMENTS_REPO`, or the command refuses with `repo_required`.
**Implementation:** `src/okg/substrate/cli/fetch.py` (`cmd_fetch_deployments`)

### `okg sync`

`okg sync {explain, health, poll, webhook}` — source-sync producer
control plane.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_sync`)
**Related:** [`runbooks/source-sync-not-firing.md`](../runbooks/troubleshooting/source-sync.md)

#### `okg sync explain`

`okg sync explain --deployment <d> [--source <s>] [--path <p>]`
— static source-sync policy (or trace one path event).

#### `okg sync health`

`okg sync health --deployment <d>` — producer health, queue depth,
DLQ state.

#### `okg sync poll`

`okg sync poll --deployment <d>` — run declared cheap probes once
and enqueue any changed sources.

#### `okg sync webhook`

`okg sync webhook --deployment <d> --source <s> [--payload-json <json> | --payload-file <file>]
[--signature <sig>] [--no-publish]` — map one webhook payload to a
source-sync workflow run (for replay / test). With neither payload flag the
payload is `{}`; when both are given, `--payload-json` wins.

### `okg trace`

`okg trace {node}` — historical fact-log and current Chronos-checkout inspection (one
sub-verb today).
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_trace`)

#### `okg trace node`

`okg trace node <node_id> [--generation-id <id>] [--dsn ...]` — dump the
legacy fact log and narrate the node from the selected Chronos checkout.
Chronos-native nodes may have no `okg.node_facts` rows. For a present graph
node, `derivation_story` orders origin and selected lineage before up to 25
adjacent edges, includes edge provenance, and reports total/cap/truncation
metadata. A requested generation pins both the node and edge reads. Successful
graph narration uses `diagnostic: fact_log_empty_graph_node_present` and does
not recommend raw SQL; missing or unavailable graph state remains explicit and
may include a diagnostic recovery command.

---

### `okg ui`

`okg ui --deployment <d> [--dsn <dsn>] [--generation <g>] [--host <h>] [--port <p>] [--auth-token-env <VAR>] [--dist <dir>]`
— serve the read-only [operator UI](../operate/operator-ui.md) for one
deployment: the read API and the built frontend on one port (default
`127.0.0.1:8765`). The DSN resolves from `--deployment` as for the other read
commands. The frontend must be built first (`npm run operator-ui:build`);
with no bundle the command refuses with `operator_ui_bundle_missing`. Only
`GET`, `HEAD` and `OPTIONS` are served (`HEAD` for the page and assets; the
read API's routes are `GET`-only); anything else gets HTTP `405` with
`operator_ui_read_only`. It binds loopback only (`localhost`, `127.0.0.0/8`,
`::1`), token or not; any other `--host`, or a port outside 1-65535, refuses
with `operator_ui_bind_refused`. Use an SSH tunnel for remote access. A
manifest with `nomos.runtime_enforcement` `audit` or `enforce` makes
`--auth-token-env` mandatory (`viz_read_auth_required`), whatever
`OKG_NOMOS_RUNTIME_ENFORCEMENT` says.
**Implementation:** `src/okg/viz/ui_server.py`; dispatch in
`src/okg/substrate/cli/main.py` (`_cmd_ui`)

## Admin

Runtime-internal control surfaces. Most are mocked in tests and
run against a real DBOS / Postgres cluster in production.

### `okg db`

`okg db {list, status, gc, dbos-gc, prune-blobs, current-runtime-dlq, acknowledge-runtime-dlq, remove-job-runtime-subsystem, drop, migrations, migrate-down, doctor}`
— inspect AKMON database inventory, status, and migration
drift.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_db`)

#### `okg db list`

`okg db list [--cluster] [--cluster-dsn ...]` — list deployment
databases.

#### `okg db status`

`okg db status [--deployment <d>] [--brief] [--top N]` — inspect
one database.

#### `okg db gc`

`okg db gc --cluster-dsn ... [--apply]` — sweep ephemeral test databases.
Dry-run by default (report-only, with per-database sizes); `--apply` drops
ONLY databases matching the exact allowlist prefixes `okg_test_`,
`okg_upgrade_verify_`, and `okg_golden`. Non-allowlisted databases always
survive and never abort the sweep.

#### `okg db current-runtime-dlq`

`okg db current-runtime-dlq --deployment <d>` — report raw, historical,
superseded, acknowledged, invalidly acknowledged, and still-blocking DBOS
terminal failures without rewriting workflow history.

#### `okg db acknowledge-runtime-dlq`

`okg db acknowledge-runtime-dlq --deployment <d> --request <json> [--apply --confirm-plan-id <sha256>]`
— dry-run and then atomically append exact, witnessed dispositions for retained
terminal failures. Apply requires the exact fresh plan ID and an operator/admin
database role.

#### `okg db dbos-gc`

`okg db dbos-gc [--stale-after-seconds N] [--status PENDING|ENQUEUED] [--confirm --apply]`
— dry-run or cancel stale DBOS PENDING/ENQUEUED workflows.

#### `okg db prune-blobs`

`okg db prune-blobs --kind image [--deployment <d>] [--grace-seconds N] [--confirm --apply]`
— delete rows of one kind from the blob store `okg.blobs` that nothing
references any more. Dry run by default; `--confirm --apply` deletes. A row is
deleted only when no retained generation references it and its `last_used_at`
is older than the grace window (default 86400 seconds). The grace window must be
at least 3600 seconds; a smaller value is refused with
`blob_prune_grace_too_short`, because an ingest that just reused a row
references it only once its run publishes. When the database has no registered
graph node storage, for example a fresh environment clone that kept `okg.blobs`
but not the graph, nothing reads as referenced: a dry run reports this in its
`note`, and `--apply` refuses with `blob_prune_no_graph` instead of deleting
every row past grace. For `image`, a
reference is the `png.id` of an image value in any retained row of the graph's
node storage, its substitute-declarer storage, or the attribute overlays.
History compaction deletes the rows of the generations it retires, so an image
only those rows used becomes deletable; overlay rows are not compacted.
The scan reads in bounded batches inside one snapshot; deletes run in bounded
batches under a lock timeout and re-check the grace window, so an ingest that
reuses a row while the command runs keeps it. The JSON report lists the rows
scanned per relation, the rows kept (`referenced`, `within_grace`,
`refreshed_since_scan`), and the rows and bytes deleted or, on a dry run, the
ones that would be.

#### `okg db remove-job-runtime-subsystem`

`okg db remove-job-runtime-subsystem [--deployment <d>] [--confirm --apply]
[--accept-retired-ledger-rows --retired-rows-dump-ref <path>]`
— irreversibly remove obsolete OKG job tables, views, functions, leases, and
cron entries after active-work safety checks. The retired subsystem has no
worker left to drain its rows, so `--accept-retired-ledger-rows` (which
requires a pre-drop dump reference, and records the destroyed row counts in the
migration ledger) is the audited way past a refusal that would otherwise be
permanent.

#### `okg db drop`

`okg db drop --name <db> --cluster-dsn ... [--confirm --apply]`
— drop one database safely.

#### `okg db migrations`

`okg db migrations [--deployment <d>]` — report schema migration
ledger status.

#### `okg db doctor`

`okg db doctor [--deployment <d>]` — run database health check.

#### `okg db role-passwords`

`okg db role-passwords [--deployment <d>] [--apply [--builtin-dev-passwords]]
[--json]` — report or replace the passwords of okg's database login roles.
Without `--apply` it lists which managed roles still accept a
built-in public password and whether the owner-only secrets file exists. With
`--apply` it gives `okg_mcp` and `app_rw` `LOGIN` and a random password, also
rotates `app_ro` and `catalog_admin` when they can log in, proves each login,
and writes the role connection strings to a mode-0600 file under
`$XDG_CONFIG_HOME/okg/role-secrets/` (override: `OKG_ROLE_SECRETS_FILE`). It
never prints a password and sends PostgreSQL only the SCRAM verifier. A role
whose override variable (`OKG_MCP_RO_DSN`, `OKG_MCP_RW_DSN`) is set is left
alone. `--builtin-dev-passwords` sets the public built-in passwords instead and
is refused unless `OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1`. Local databases only. See
[Security: database role passwords](security.md#database-role-passwords).

#### `okg db repair-effective-attribute-intervals`

`okg db repair-effective-attribute-intervals --deployment <d> --apply` — while
holding the deployment publisher lock, reconstruct and verify the latest
published generation's full effective-attribute snapshot and interval-backed
Nomos operator read model. The command fails on checkpoint, payload, count, or
generation drift; it is a zero-write no-op when parity already holds. A publish
that enables `OKG_IVM_INTERVAL_VIEW` on an existing deployment without this proof
refuses with `effective_interval_backfill_required` before changed-only view work.
`OKG_IVM_GENERATION_ORDINAL=0` disables the same read model, because it stops the
generation ordinals it is keyed by; publishes then write full snapshots, and
re-enabling either variable needs this repair first.

#### `okg db embed-backfill` / `embed-show` / `embed-index` / `semantic-index`

`embed-backfill --set <name> [--max-usd <usd>] [--dry-run]` links one
registered set's nodes to documents at the latest publish and embeds each
distinct document lacking a vector; no node rows; publish to read them. One
run per set; a paid set needs `--max-usd`, counted per set in
`okg.llm_calls`. `--dry-run` reports missing documents, tokens and dollars
per set and subtype from rates in `okg.llm_calls` (else dollars unknown),
without calls or writes. `embed-show --set <name> --node <id>` prints a
node's document, `doc_hash`, vector presence and `readable_at_pin`.
`embed-index --set <name> --apply` builds the registered index
concurrently, then drops the others. `semantic-index --apply` builds the
node-column HNSW index. Paid `openrouter:` calls: backfill (capped,
recorded), one per semantic query.

#### `okg db reclaim-index-space`

`okg db reclaim-index-space --deployment <d> [--apply] [--min-index-bytes N]
[--publish-lock-wait-seconds S] [--lock-timeout-seconds S] [--json]` — rebuild
the indexes of okg-schema tables that `okg doctor --check disk` reports as
`index_larger_than_table`. A failed or cancelled publish leaves the index pages
of the rows it rolled back; VACUUM empties the table but never shrinks an index. Without `--apply` the command lists the flagged tables, their table
and index bytes, and the `REINDEX TABLE CONCURRENTLY` statements it would run;
it exits 1 with `dry_run_blocked` when `--apply` would be refused now, naming
each blocker in `blocked_by`.
With `--apply` it holds the deployment's publisher lock for the whole run, so an
ingest's end-of-run publish during the run gets `PublisherLeaseHeld`. It
refuses (each with its own `data.reason`) when a publish holds that lock
(`publisher_lock_held`), when a source run of the deployment is still writing
(`source_run_in_progress`, the signal that also refuses a publish), and when a
target table has an invalid index (`invalid_index_present`,
for example a `*_ccnew` left by an interrupted rebuild; the refusal prints the
`DROP INDEX CONCURRENTLY` command). A writer that starts later makes the
rebuild's bounded wait fail (`reindex_failed`; the command then drops the
invalid `<index>_ccnew` / `_ccold` copies its own rebuild left, never another
index, and reports any it could not drop, including a TOAST `_ccnew`, with its
drop command) or the fingerprint differ (`fingerprint_changed`). It rebuilds one table at a time
with a bounded lock wait, and compares an authority fingerprint before and
after: index and constraint definitions, owners, grants, reloptions, comments
and row digests of the rebuilt tables, plus row digests of
`okg.attribute_versions`, the generation ordinals, the Chronos checkpoints, the
record-set and record-authority tables and the graph rows. Any difference fails
the command with both values and names the tables it already rebuilt (a rebuilt
table stays rebuilt). The fingerprint reads every row of those tables twice;
a source run that starts after the in-progress check makes it differ, so run
the command when no ingest is scheduled.

The publisher worker, `okg run --once`, `okg ingest` (once, after its last
layer; so also the source-sync workflow and provisioning) and merge-back
also rebuild bloated indexes on their own after a
successful publish; the MCP connector ingest and the exchange
receiver leave it to the next worker or run cycle. After the publish has
committed and released its locks, the rebuild compares each btree index of
`okg.attribute_effective_view_intervals`,
`okg.attribute_effective_view`, `okg.attribute_versions` and the two graph
tables with its estimated packed size (from `pg_class.reltuples` and
`pg_stats`, after an `ANALYZE` of the suspect tables). It rebuilds with
`REINDEX INDEX CONCURRENTLY` each index that is at least
`OKG_POST_PUBLISH_INDEX_REBUILD_RATIO` (default 1.5) times its estimate and at
least `OKG_POST_PUBLISH_INDEX_REBUILD_MIN_BYTES` (default 64 MiB) over it.
`OKG_POST_PUBLISH_INDEX_REBUILD=0` turns this off. Each `ANALYZE` and each
rebuild takes the publisher lock without waiting and releases it at once;
each lock wait lasts at most
`OKG_POST_PUBLISH_INDEX_REBUILD_LOCK_TIMEOUT_SECONDS` (default 5; one
concurrent rebuild can wait several times, plus its cleanup drop), and no
step starts after `OKG_POST_PUBLISH_INDEX_REBUILD_BUDGET_SECONDS`
(default 30). Before each step, the rebuild stops and leaves the rest to the
next successful publish when a publish is waiting for the lock or holds it, a
source run is writing, or another session's transaction with a snapshot is older than the lock timeout.
A publish that starts during the rebuild therefore waits for one step at
most. The rebuild first finishes any graph-index change a deferred `okg
migrate` left pending, and drops an invalid `_ccnew`/`_ccold` copy an earlier
rebuild of the same table left when no index build is in progress there. It
skips a table that has any other invalid index and reports it with its drop
command. It never fails the publish. Its report is in the publish summary
under `post_publish_index_rebuild`. That includes `status`, `rebuilt` (per
index: bytes before and after, estimate, ratio, seconds), `reclaimed_bytes`,
`seconds`, `max_publisher_lock_hold_seconds`, `cleaned_up_leftovers` and
`leftover_invalid_indexes`. It never judges an index named `okg_dattr_*`
(the declared-attribute indexes below), and it leaves a graph table for the
next run, listed in `deferred_graph_tables`, while another session holds the
graph-index DDL lock (a declared-attribute index build, a cold-history
rotation, a migrate).

#### `okg db attr-index`

`okg db attr-index reconcile [--deployment <d>] [--dsn <dsn>] [--apply]
[--retry-failed] [--lock-timeout-seconds S] [--json]` builds one partial
expression index per declared filterable attribute on the Chronos node
table, named `okg_dattr_n_<hash>` (MCP filter reads use them by
default, see `okg mcp-serve`). It builds none on the edge table, and drops an
`okg_dattr_e_` index an earlier version built (`last_reason`
`kind_not_indexed`; doctor lists it at INFO until then). Without `--apply` it
prints the plan (`would_build`, `would_drop`, failures stopped by the cap,
`okg_dattr_` indexes it does not own) and changes nothing. `--apply` needs
`--deployment` (refused, exit 1, without it): the publish-lock and
source-run checks below are per deployment, and the command assumes none;
`--dsn` may still name the database. `--apply` runs the
reconciler once and also opts the deployment in: from then on the worker's
`okg_declared_attr_index_maintenance` workflow, on the
`okg-<deployment>-index-maintenance` queue every five minutes, keeps the
indexes in step with the declarations, then runs the drift checks below. It
skips the drift checks when the reconcile was refused, preempted or failed,
and a run whose reconcile or drift check raised ends as a failed workflow
with the error in doctor's `last_run`. `OKG_DECLARED_ATTR_INDEXES=off` makes
that workflow do nothing. A run holds the graph-index DDL lock (refused,
exit 1, while another session holds it), drops its own failed and retired
indexes first, then builds each missing index with `CREATE INDEX
CONCURRENTLY` outside any transaction. It waits up to
`--lock-timeout-seconds` (default `OKG_DECLARED_ATTR_INDEX_LOCK_TIMEOUT_SECONDS`
or 600) for older transactions instead of failing. It builds nothing while a
publish holds the publisher lock, a source run is writing, or a transaction
is older than 60 seconds, and nothing on a table that has any invalid index,
whoever made it. Text indexes need a database collation that orders strings
by code point (libc `C`, `POSIX`, `C.UTF-8`, `C.utf8`; builtin `C` or
`C.UTF-8`). A failed build is retried after 1, 2, 4 and 8 runs (at most a day
apart); after five consecutive failures it stays `failed` until `--apply
--retry-failed`, and the command exits 1. `okg migrate` and catalog apply
cancel a build that holds the lock; the build is dropped on the next run and
not counted as a failure.

It also keeps one `okg_dstat_` statistics object per declared attribute.

`okg db attr-index verify --against-graph [--deployment <d> | --dsn <dsn>]
[--branch default] [--json]` compares, at every ordinal the
effective-attribute view retains, the view's declared keys with the graph
rows' declared keys; it records each check, sets an attribute's drift flag
on a difference and clears every flag after a clean run. It exits 1 and lists
`attrs_with_differences` (`<kind>:<attr>`) on a difference. It does not
report `clean` when it compared nothing: with no retained ordinal whose
publish synced the view (or no declared attribute), the status is
`nothing_compared`, with a `reason`, and the exit code is 3. Plain `verify`
(without `--against-graph`) compared the indexes with `okg.attr_index`,
which no longer exists; it is refused with exit 1, error code
`removed_check` and a fix hint naming `--against-graph`. After each run
the maintenance workflow checks the publishes since its last run: the
targets they changed, and one rotating slice of all targets per publish
(`OKG_DECLARED_ATTR_DRIFT_SLICES`, default 32), so a drifted target that no
publish changed is found within 32 publishes. `okg doctor --check
declared-attr-indexes` reports the lifecycle states, invalid indexes on the
graph tables, the failure cap, drift, the collation and long values, and
one `attr_not_indexed` finding per declared node attribute without a ready
index; it exits 1 on any ERROR finding: drift, a capped failure, a ready
index that is gone, or a declared attribute never enabled, refused by the
collation, or with maintenance off (`OKG_DECLARED_ATTR_INDEXES=off`, read
from doctor's own environment).

**Upgrading to the version that drops `attr_index`.** Migration `0043_attr_index_teardown`
drops `okg.attr_index`, its coverage markers, rebuild queue and derive front
(about 11.7 GB on one large deployment). Before deploying, on the old code: run
`okg db attr-index reconcile --deployment <d> --apply` and check that
`okg doctor --check declared-attr-indexes` reports OK. Then stop the worker
and the MCP servers (a step-3 reader holds locks on `okg.attr_index`, and the
migration waits at most 5 seconds for them), deploy, run `okg migrate
--apply`, and start them again. `okg db rebuild-attr-index` and `okg doctor
--check attr-index-coverage` no longer exist. There is no cheap rollback:
old code would recreate empty tables, so recovery is a forward fix.

### `okg agent`

`okg agent connect --deployment <d> [--agent claude-code|claude-desktop|cursor|codex ...]
[--writes] [--dry-run] [--replace] [--probe-timeout <s>] [--json]` registers
the deployment's MCP server with each agent and installs the bundled user skills
where it reads them ([connect an agent](../use/agents/index.md)).
Without `--agent` it connects every agent present and reports the rest as
`not found` (exit 0); a named agent that is absent is `not found` (exit 1).
Each agent's result is `connected`, `unchanged`, `not found` or `refused`
(an unparseable config file, or a duplicate entry for the deployment without
`--replace`); any `refused` exits 1. Before writing, it starts the entry the
way the agent will (cwd `/`) and refuses with the code `okg mcp-serve`
printed, writing nothing, when the server does not list its tools; it also
refuses `deployment_not_found`, `agent_connect_ephemeral_okg` (the running
okg is inside uv's cache), `agent_connect_writes_need_role_passwords`
(`--writes` while okg_mcp or app_rw still uses its built-in password),
`agent_connect_server_failed`, `agent_connect_server_timeout` and
`agent_connect_tools_missing`. The entry runs `okg agent-runtime-launch`
with `--workspace-root <deployment folder>`, `--deployments-dir`, and, when
they exist, `--local-env` and `--role-secrets-file`; the launcher refuses a
path that is not absolute, missing, a symlink, the wrong kind, another
user's, writable by group or others, or writable by the agent run (inside
the profile's write roots) with `agent_runtime_path_binding_invalid`, and
`--allow-builtin-role-passwords` for a profile that can write. A
write-enabled profile's write root is the deployment folder minus its
credentials file (`.okg/local.env`, and the role-password file if it sits
there): the launcher exports the excluded paths as
`OKG_AGENT_DENIED_WRITE_PATHS`, and `check_agent_write` in
`agent_runtimes/launcher.py` refuses a write to them with
`agent_runtime.write_denied`. Every distinct entry is started once before
anything is written; the Codex entry gets `startup_timeout_sec` (60, or three
times the measured start, at most 180). `okg agent disconnect
--deployment <d> [--agent ...] [--dry-run]` removes the entry and the skills
connect's sync manifest records; a skills folder another agent still reads
for a connect-written entry is kept. Both commands refuse Claude Desktop while
the app runs (it rewrites its config from memory), read each written file
back, and keep `<file>.okg-backup` as the latest version of the file with no
connect-written entry.
**Implementation:** `src/okg/substrate/library/agent_runtimes/connect.py`
(`run_connect`, `run_disconnect`)

### `okg mcp-serve`

`okg mcp-serve --deployment <d> [options]` — run the MCP server for one
deployment. It refuses to start until the deployment has a published
generation. [Connect an agent](../use/agents/index.md) shows how to use it;
[MCP graph reads](mcp.md) defines the tools.

| Option | Meaning |
| --- | --- |
| `--deployment <d>` | The deployment to serve; its manifest names the database (default `default`, a deployment of that name; `$OKG_DEPLOYMENT` is not read). |
| `--dsn <dsn>` | A database address to use directly, checked against the deployment's manifest when one exists. `$OKG_DSN` is not a fallback here. |
| `--branch <name>` | The graph branch to read (default `default`); one server reads one branch. |
| `--embedder <name>` | The semantic search backend for a `--dsn` launch: `hash` (tests) or `sentence-transformers:<model>`. |
| `--allow-writes` | Also serve the write and admin tools, over a separate write account. Off by default. |
| `--tools <name>` | Serve only a named tool profile: `discovery`, `reader`, `full`, or one the deployment declares. |
| `--tools-ceiling <name>` | Let a session widen its own tools up to this profile without a restart. |
| `--transport <t>` | `stdio` (default), for an agent that starts the server, or `streamable-http`, which serves `http://<host>:<port>/mcp`. |
| `--host <address>` | The HTTP bind address (default `127.0.0.1`). Without `--auth-token-env` or `--oidc-auth` the HTTP server has no login, and `--allow-writes` on another address is refused. |
| `--port <n>` | The HTTP port (default 8100). |
| `--allowed-host <host:port>` | Accept this `Host` header; repeat for each name clients use. |
| `--auth-token-env <VAR>` | Require a bearer token on HTTP, read from environment variable `VAR`. |
| `--multi-user-principals` | Name each request's user from a header a trusted proxy sets; needs `--auth-token-env`. See [Multi-user HTTP principals](mcp.md#multi-user-http-principals). |
| `--oidc-auth` | Verify each person's own token from the deployment's identity provider. HTTP only, and not with `--auth-token-env`, `--multi-user-principals` or writes; see [Multi-user HTTP principals](mcp.md#multi-user-http-principals). |
| `--auth-token` | Always refused: a token on the command line is visible to other processes. |

**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_mcp_serve`)

`OKG_ATTR_FILTER_INDEX` picks which index narrows an attribute-filtered
`filter`, `map` or `search` read before the exact predicate decides each
row; both values return the same rows in the same order:

- `expression`, or unset (the default): the declared node expression
  indexes `okg db attr-index reconcile` builds, at the branch head, for
  each attribute whose index is ready. Every other declared attribute the
  predicate narrows by does not narrow the read, and the response carries
  `{"pushdown": "not_covered", "attrs": [{"attr", "reason"}]}`, with reason
  `kind_not_indexed` (an edge attribute: no edge index is built),
  `index_missing`, `index_building`, `index_failed`,
  `declaration_changed`, `view_graph_drift` or `pinned_generation` (a read
  pinned to an older generation). The ready attributes still narrow.
  Prefix filters are not narrowed by any index, except a `search`
  `where` prefix on `path` (below).
- `off`: no index; the exact predicate scans the effective attributes.

`expression` also lets a query-less `search` with a top-level
`where` prefix on `path` or `attrs.path` narrow through the path-prefix
index, under Nomos `off` only. `path` must be a declared filterable `text`
attribute whose index is ready. Otherwise nothing narrows and the response
carries `{"pushdown": "not_covered", "attrs": [{"attr": "path",
"reason"}]}`. The reason is `not_declared`, a readiness reason above,
`pinned_generation`, `path_index_missing`, or `broad_prefix`: more than
10,000 visible candidates.

`attr_index` and `dual`, values from earlier versions, are refused:
an attribute-filtered read fails with `invalid_configuration` and
`data.reason` `removed_value`; other reads still work, and the MCP server
logs one ERROR at start. `OKG_ATTR_FILTER_DUAL_SAMPLE_EVERY` and
`OKG_ATTR_FILTER_DUAL_TIMEOUT_MS` have no effect; the server names them in
one WARNING at start. Any other value fails those reads with
`invalid_configuration`. A `ts` bound that the strict ISO-8601 reading
(`okg.attr_ts_v4`) refuses takes its attribute out of the narrowing
(reason `bound_not_strict_iso`; the other clauses still narrow); stored
`ts` values compare through that reading, and a float range bound
compares in double precision, so a stored number beyond double range no
longer fails the read with SQLSTATE 22003. `okg doctor --check
declared-attr-indexes` reports each declared node attribute without a
ready index (`attr_not_indexed`).

### `okg nomos`

`okg nomos {provider, binding, grant} {add, disable, list} [--dsn <dsn>]
--deployment <d> [--json]` — register identity providers, map users to
Nomos subjects, and grant subjects actions, without Python. `add` never
rewrites or re-enables an existing row; `disable` keeps the row and
records who, when and why; `list` includes disabled rows.
`okg nomos provider add --for-oidc-auth` fills the provider from the
manifest's `nomos.oidc_auth` block. Usage and refusal codes:
[Nomos IAM runbook](../runbooks/security/nomos-iam.md).
**Implementation:** `src/okg/substrate/cli/nomos.py` (`cmd_nomos`)

### `okg runtime`

`okg runtime {bootstrap, openshell-policy, worker, debug-workflow, cancel-workflow, smoke, drain, reset, escalation, measure-recovery, get-event, list-schedules, trigger-schedule, enqueue-fenced-recovery}`
— DBOS runtime layer.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_runtime`)
**Related:** [`runbooks/dbos-runtime.md`](../operate/deployments/runtime.md)

#### `okg runtime bootstrap`

`okg runtime bootstrap --deployment <d>` — install DBOS schema and role
split (idempotent).

#### `okg runtime openshell-policy`

`okg runtime openshell-policy --deployment <d> [--dsn <dsn>] [--output <path>] [--json]`
— render an OpenShell sandbox policy for the deployment's runtime worker from
`deployment.yaml` and `source_registry.yaml`: filesystem access limited to the
deployment root and Python venv, egress from declared source endpoints plus the
deployment Postgres DSN, and `landlock.compatibility` set to `hard_requirement`.

The command fails loudly when an endpoint cannot be derived; it does not
generate wildcard egress. `--dsn` takes precedence over ambient `OKG_DSN`,
including registry `${OKG_DSN}` placeholders. `--output` writes YAML to a
file and still prints the rendered policy to stdout unless `--json` is
selected. It only renders policy; a separate runtime proof is required
before claiming that an OpenShell-contained worker can run and publish
under hard Landlock plus proxy-mode egress.

#### `okg runtime worker`

`okg runtime worker --deployment <d>` — long-lived DBOS worker.

#### `okg runtime debug-workflow`

`okg runtime debug-workflow <workflow_id> --deployment <d>` — print a
structured debug report for one DBOS workflow.

#### `okg runtime cancel-workflow`

`okg runtime cancel-workflow <workflow_id> --deployment <d>` — cancel one
`ENQUEUED`/`PENDING`/`DELAYED` workflow wedging the queue or blocking
readiness; refuses anything else.

#### `okg runtime smoke`

`okg runtime smoke --deployment <d>` — LISTEN/NOTIFY round-trip test.

#### `okg runtime drain`

`okg runtime drain --deployment <d>` — wait for in-flight DBOS workflows on
the deployment's queue to finish (before runtime reset).

#### `okg runtime reset`

`okg runtime reset --deployment <d> --confirm` — reset DBOS state
(dev only; refused without the flag).

#### `okg runtime escalation`

`okg runtime escalation {respond, list, show}` — operator responses to
SDK escalations.

#### `okg runtime measure-recovery`

`okg runtime measure-recovery --deployment <d>` — measure DBOS
worker-recovery time (Decision 5).

#### `okg runtime get-event`

`okg runtime get-event <workflow_id> <key> --deployment <d>` — read a
workflow event by key.

#### `okg runtime list-schedules`

`okg runtime list-schedules --deployment <d>` — list DBOS schedules.

#### `okg runtime trigger-schedule`

`okg runtime trigger-schedule <s> --deployment <d>` —
trigger one DBOS schedule out-of-cycle.

#### `okg runtime enqueue-fenced-recovery`

`okg runtime enqueue-fenced-recovery <doctor_workflow|okg_ci_resource_monitor|okg_ci_git_history_reconcile> --deployment <d> --promotion-id <id>`
— enqueue exactly one allowlisted recovery-witness workflow under the active
promotion owner while recurring schedules stay removed. Refuses a missing,
released, or differently owned fence.

#### `okg runtime pause-schedule`

`okg runtime pause-schedule <s> --deployment <d>` — persistently pause one
schedule, verify that exact row is paused, and fence any running internal
scheduler wrapper before returning. A paused `okg_publisher_cycle` stays
ready: doctor warns `publisher_workflow_paused` (`_paused_long` after 24
hours); a failed, stuck or cancelled running cycle fails until a cycle
succeeds, including one cleared during an upgrade.

#### `okg runtime resume-schedule`

`okg runtime resume-schedule <s> --deployment <d>` — resume one
paused schedule and verify that its persisted state is active.

## Admin-maintenance

Rarely-run AKMON runtime maintenance.

### `okg consistency-rerun`

`okg consistency-rerun [--deployment <d>] [--generation <id>]` —
run deployment post-publish consistency checks for an existing
published generation.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_consistency_rerun`)

### `okg migrate`

`okg migrate [--apply] [--dsn ...]` — apply the v1 schema
idempotently. Dry-run by default. It first builds missing graph read
indexes concurrently and drops retired ones.

After the schema commit, migrate also retires, once, the repair requirement
of a `repair_required` checkpoint that an earlier release cut for a partially
failed ingest. It runs the same write as `okg runtime supersede-checkpoint
--apply`, with actor `okg migrate` and reason
`upgrade_3342_publish_incomplete`, both recorded on the checkpoint. Nothing is
deleted and the checkpoint stays `blocked`. A checkpoint this release blocks on
purpose, because a failed source's changes could not be undone, carries
`freeze_policy: post_3342_undo_unfinished` and is left alone. A bare `okg
migrate` covers every deployment in the database; `okg deployment up` passes
`--deployment`, so it covers that deployment only. Check
`incomplete_checkpoint_retirements` in the output: it lists each retirement
(empty when there was nothing to retire), and an entry with `skipped` means a
publish moved the checkpoint meanwhile, so the next migrate retries it. `okg
deployment up --skip-migrate` skips this step too. To see what would be retired first, run `okg runtime
supersede-checkpoint` without `--apply`.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_migrate`)

### `okg snapshot-load`

`okg snapshot-load --deployment <d> --snapshot <file> [--apply]`
— load a JSON snapshot as the deployment's first published
generation (renamed from `bootstrap`).
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_bootstrap`)

---

## Debug

Investigation surfaces. Not in the operator hot path; reach for
these when something is wrong.

### `okg branch`

`okg branch {list, show, create, abandon, delete}` — inspect AKMON
branches (`okg.branches`).
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_branch`)

#### `okg branch list`

`okg branch list --deployment <d>` — branches with role / status /
latest-gen.

#### `okg branch show`

`okg branch show --deployment <d> <name>` — one branch's metadata
+ generation-count summary.

#### `okg branch create`

`okg branch create --deployment <d> --role {experiment|quarantine} [--base-branch <b>] <name>`
— create an experiment or quarantine branch.

#### `okg branch abandon`

`okg branch abandon --deployment <d> <name>` — mark an experiment
or quarantine branch as abandoned (refuses the default branch).

A metadata tombstone only: the Chronos branch row survives and its
interval space stays allocated. Use `okg branch delete` to remove it.

#### `okg branch delete`

`okg branch delete --deployment <d> [--cascade] [--confirm] <name>` —
remove the Chronos branch row, decrementing the parent's child count and
requesting an interval GC pass. Refuses `main`, the default branch, and
any branch owned by a different deployment. Dry-runs without `--confirm`;
refuses a branch that still has children unless `--cascade` is passed.

Deleting (or abandoning) an orphaned fork branch unblocks history
compaction for its parent — a live child branch still reads the parent's
history, so compaction refuses while one exists.

### `okg perf`

`okg perf {list, show, compare}` — inspect and compare
performance captures. `okg profile` is the deprecated old name: it still
runs and prints a rename warning on stderr for one release.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_profile`)

#### `okg perf list`

`okg perf list [--deployment <d>]` — profiling captures.

#### `okg perf show`

`okg perf show <capture_id>` — one profiling capture.

#### `okg perf compare`

`okg perf compare <a> <b>` — diff two profiling captures.

---

## Dev-tooling

Runtime developer scaffolders + lints.

### `okg lint`

`okg lint deployments` — boundary / cross-cutting lints. Walks
every deployment manifest and checks
`runtime.workflows` Python modules for imports outside the
runtime's allowed surface.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_lint`)

#### `okg lint deployments`

(see parent — only sub-verb today)

### `okg new-module`

`okg new-module <name> [--in-tree | --deployment <d>]` — scaffold a new
ontology module skeleton (`module.yaml`, `schema.yaml`, `tests/`,
`README.md`) under `src/okg/substrate/library/ontologies/<name>/` (the
default, `--in-tree`) or under `modules/<name>/` in deployment `<d>`'s
folder, found as `okg deployment add-source` finds it (a name, folder or
`deployment.yaml` path). An unknown deployment is refused; nothing is
written.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_new_module`)

### `okg new-source`

Retired, and hidden from `okg --help`. It used to write a reader skeleton
that was not a working reader. Any invocation now exits
1 with the error `command_retired` and writes no file. To author a source,
copy the working connector in `examples/json-record-connector/` and add it
with `okg deployment add-source <deployment> <name> --registration
<dir>/connector.json`;
[Add a source adapter](../develop/index.md#add-a-source-adapter) walks
through it.
**Implementation:** `src/okg/substrate/cli/init.py` (`_cmd_new_source`)

### `okg schema`

`okg schema <yaml-path>` — emit JSON Schema for one authored file
(`deployment.yaml`, `source_registry.yaml`, `extractors.yaml`,
`module.yaml`, `schema.yaml`).
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_schema`)

---

## Scaffold

Bootstrap a new deployment or inspect one that exists.

### `okg install`

The retained packaged release entry supports `--update-source <existing-source-id>`
with the original installation arguments and `--verify-query`. It verifies the
original release and completed installation, then runs scoped inline ingest
including declared predecessors. It does not bootstrap or provision again.
See [packaged source updates](../operate/install-packaged.md#update-an-existing-packaged-source)
for the command, refusals and supported scope.

For authorized installed reads, the same retained entry accepts
`--search-query <text>` and optional `--search-generation <published-id>` with
the original installation selection. It invokes the existing lexical search
under the verified deployment's enforced Nomos policy, preserving normal
results and source metadata. Empty allowed results succeed. Search cannot be
combined with update or staging flags and does not ingest or publish.

`okg install [--profile <name>] [--deployment-name <n>] [--repo-root <r>] [--scaffold-only] [--no-publish] ...`
— the one documented way in, and the only verb classified
`newcomer_bring_up` (see
`src/okg/substrate/cli/entry_roles.py`).
The framework owns the verb: `--profile` selects what to build, and a profile
shipping its own installer contributes its flags to this parser. With no
`--profile` it installs the `minimal` starter: one small ontology module, no
sources, Nomos runtime enforcement off, migrated into the database
`--postgres-dsn` names and published as an empty first generation; grow it
with `okg deployment add-source`. `okg install --help` lists every
installable profile. `--profile academic-researcher` runs that profile's own
installer (Docker + Postgres + scaffold + agent-runtime MCP registration,
then a conversational wizard for source setup). Any other profile — a
library profile such as `workspace-graph`, or a reference deployment fetched
with `okg fetch-deployments` — is written out and brought up to a first
published generation. A flag one profile's installer contributes (such as
`--orcid`) is refused when another profile runs, rather than ignored. Profile `init_questions` become
`--<question-id>` flags. `--scaffold-only` skips a profile's own
installer; `--template <name>` scaffolds a legacy library template.

A successful install prints a short summary of about ten lines: the graph's
name, the deployment folder, the database, how many records and links the
graph now holds, one search to try, and how to connect an agent. It prints a
warning only when you must act on it, with the fix. `--verbose` prints the
full report instead: every file written, the module composition, the first
publish proof, and every warning, including `skipped_extends` (an optional
link to a module the profile does not load; it needs no action).
`--json` prints the same detail as one JSON document, with the same keys
with or without `--verbose`. A profile with its own installer
(`academic-researcher`) and `--template` installs ignore `--verbose`.

`--dry-run` prints what the install would do and exits without doing any of
it — no file or directory created, no database connection, no container
started, no schema migration or publish. That holds on every route `install`
and `init` take; the flag is framework-owned, so it is present whichever
profiles are installed.

What the plan *contains* depends on the route, so read `plan_complete` first
— it is on every plan, and `false` means the command could not describe the
run (a required answer is missing, say). The `status`
field is `dry_run_ok` or `dry_run_blocked`, and a blocked plan exits nonzero
so a wrapper script cannot read it as a clear one.

- **Profiles served by the generic scaffold** (`minimal`, `workspace-graph`, fetched
  reference deployments, `--scaffold-only`, and the legacy `--template`
  route) report `files_that_would_be_written`,
  `directories_that_would_be_created`, and whether the deployment directory
  would be created, refused, or overwritten — plus, under `--force --yes`,
  the `files_that_would_be_deleted` that overwrite would destroy.
- **Profiles shipping their own installer** (`academic-researcher`) report a plan of
  what that installer probes instead: Docker daemon and image presence,
  chosen ports, scaffold-directory collision, which sources will need setup
  approval, and agent-runtime config collisions.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_install`)

### `okg init`

An alias for the documented door: `okg init <args>` runs
`okg install <args> --scaffold-only`, and says so on stderr. It keeps older
scripts and runbooks working; new guidance should name `okg install`. Flag
surface, semantics, and end state are the door's — see
[`okg install`](#okg-install).
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_init_alias`)

### `okg deployment`

`okg deployment {show, new, list, fork-worktree, gc-forks, gc-scaffolds, env, isolated, release, up, apply, ready, plan, lint, profile, nomos-rollout-audit, nomos-source-lineage-rollout-audit, bug-report, bootstrap}` —
deployment authoring, host inventory, worktree forks and scaffold GC,
environment, isolated worktree database lifecycle, release contract,
deploy/upgrade, readiness, Nomos rollout audits, and bootstrap workflows.
**Implementation:** `src/okg/substrate/cli/main.py` (`_cmd_deployment`)

#### `okg deployment new`

`okg deployment new <name> [--deployments-root <dir>] [--force] [--yes] --apply`
— create a loader-valid starter deployment. `--apply` is required and writes its files.

#### `okg deployment show`

`okg deployment show <d>` — print the resolved deployment manifest
(with runtime block). `<d>` is a deployment name or a path to its
`deployment.yaml`.

#### `okg deployment env`

`okg deployment env <d>` — inspect or write the deployment local
environment profile used for manifest DSN interpolation.

`okg deployment env doctor <d>` — diagnose DSN authority without
writing local env. The report names manifest env refs, process env,
local env, worktree fallback, the selected authority source, target
database, conflicts, and recovery commands. Because `doctor` is the
nested diagnostic verb, it is reserved under `deployment env`.

#### `okg deployment plan`

`okg deployment plan <d>` — inspect resolved deployment wiring before
runtime. The plan shows source declarations, module defaults, enrichers,
extractors, alias resolver backends, declared job descriptors, checks,
views, and operator surfaces.

#### `okg deployment lint`

`okg deployment lint <d>` — run static authoring diagnostics over the
deployment manifest and related config. Findings include a code,
severity, stage, location, why it matters, and next action.

#### `okg deployment compat`

`okg deployment compat <d> [--commands FILE] [--against CONTRACT] [--json]`
— check, before an upgrade, whether a deployment works with the okg that is
running. It needs no database. It imports the deployment's own adapter
modules, so anything those modules do at import time happens. It runs:

1. `okg deployment lint`.
2. For every source-registry entry, with `code_repos` expanded: the module
   imports, the class exists (a missing one lists the classes the module does
   define), and the params fit the constructor. This imports the
   deployment's own adapter modules, as ingest would; it constructs nothing.
3. When the deployment runs Nomos (`nomos.runtime_enforcement` is `enforce`
   or `audit`), the Nomos deployment policy audit with `code_repos` expanded,
   as `okg provision` runs it.
4. With `--commands`: each `okg` command line in the file, one per line,
   still parses against this okg, after the same rewrites `okg` applies and
   with a profile's answer flags when the line names `--profile`. `#` lines
   are comments. `okg pact`, `--help` and `--version` lines are reported as
   not checked; nothing is executed.
5. With `--against`: the breaking changes between an older release contract
   and this okg's. Changes that touch a key, source class, module, verb or
   flag this deployment uses are listed first, and each is a blocker,
   `compat.breaking.touches_deployment`, whose fix points at the migration
   note in the release notes; breaking changes the deployment does not use
   are listed but do not block. Without `--against`, a
   deployment that names `release.tested_with: "<version>"` (beside
   `release.channel`; quote it, since an unquoted `0.10` is the number 0.1
   and is refused as unknown) is compared with that release's contract. The wheel
   attached to each GitHub release carries the contract of every earlier
   release, gzipped under `okg/_release_contracts/`. Two limits: comparisons
   start after v0.2.0, the first release with a contract; and any other build
   -- a dev wheel, an install from a checkout, a container image -- carries
   only the contracts present in its build directory when it was built,
   normally none. When the named release's contract is not carried, compat
   reports `compat.release.tested_with_not_bundled` as a warning (exit 0) and
   names the fix: pass `--against` with that release's
   `okg-contract-v<version>.json` asset. A carried contract that cannot be
   read makes the verdict unknown (exit 2).

| Exit | `verdict` | Meaning |
| --- | --- | --- |
| 0 | `compatible` | No blocker. Warnings may still print. |
| 1 | `incompatible` | At least one blocker, including a breaking change since the compared release that touches something this deployment uses. |
| 2 | `unknown` | The deployment was not found, or a check could not run, so compatibility is not known. It is never reported as compatible. |

A release contract (`okg.release_contract.v1`) is okg's public surface, read
from the installed package: CLI verbs, every spelling of each flag with how
many values it takes, positionals by position, exit
codes, `deployment.yaml` and source-registry key paths, reserved source
params, source classes, ontology modules and MCP tool names.
`scripts/release/build_contract.py`, given `--out FILE`, writes the running okg's
contract. Environment variables, MCP tool parameters, error codes and
profile-injected flags are not in it yet.

#### `okg deployment profile`

`okg deployment profile {list,explain}` — inspect built-in deployment
profiles and their defaults without reading secret values.

#### `okg deployment isolated`

`okg deployment isolated {list,gc} <d>` — list or garbage-collect
worktree-isolated deployment databases. `gc` is dry-run unless `--yes`
is supplied.

#### `okg deployment up`

`okg deployment up <d> [--target dev|prod] [--include <source> ...] [--skip-migrate]`
— idempotent deploy/upgrade path. It resolves the target database,
applies pending migrations when needed, applies the catalog, runs
scope-complete ingest/publish, and reports the runtime-worker restart as
the next action. `--target dev` uses the worktree-isolated
database; `--target prod` uses the declared deployment database.

For a deployment that runs from an installed okg package, `okg up` stops a
running worker, reloads it and starts it again through the deployment's own
worker service: on Linux the enabled instance `okg-runtime-worker@<d>.service`
whose shared template is the one OKG renders for `<d>`, on macOS the launchd
plist. `okg up` and `okg up --plan` refuse before changing anything
(`installed_upgrade_worker_unsupervised`) when a reload of a systemd system
service is needed and `okg up` is not running as root (run it with sudo), or,
on Linux with no service of its own, when the deployment has
`runtime.enabled: true`, a running worker, or a worker that ran before (install
the service as root first: `okg dogfood service install --deployment <d>
--adapter systemd --apply`), or when the reload check cannot be read. Per-user
systemd worker services are planned as the first follow-up after 2026-10-17.

#### `okg deployment backup restore`

`okg deployment backup restore <d> --archive <file> [--apply] [--json]` —
for a deployment that runs from an installed okg package: go back to the
backup `okg up` took before an upgrade. Without `--apply` it prints the plan
and changes nothing. It refuses (`installed_restore_refused`) any archive
other than the recorded `pre_upgrade_archive`, an archive without its
owner-only `<archive>.json` sidecar, one whose bytes changed, one taken from
another database, and a role that is not a superuser when the database uses
TimescaleDB (or, without it, lacks CREATEDB or membership in the database
owner's role). With `--apply` it stops the runtime worker,
restores the archive into a fresh database on the same server through the
verified restore path, replays the archive's owners, grants and default
privileges, checks data and access against the sidecar, copies the live
database's owner, grants and settings, and swaps it in by renaming; the live
database is kept as `<database>_pre_restore_<time>`. It refuses while another
session stays connected, ending only TimescaleDB background workers and
starting them again after the swap, and does not restart the runtime worker. Run
it with the okg that took the backup, then reinstall the older wheel. See
[Upgrade an installed deployment](../operate/lifecycle.md#upgrade-an-installed-deployment).

#### `okg deployment apply`

`okg deployment apply <d>` — read-only by default. It compares the selected
deployment with live catalog ownership/content, the active Nomos baseline,
source supervision, and the worker configuration, then prints a digest-bound
ordered plan. Execute that exact plan with
`okg deployment apply <d> --apply --confirm <plan-id>`.

Apply preserves each subsystem's fail-closed guards, records stage receipts,
and waits for the replacement worker to acknowledge the exact source and
schedule set. A second converged apply is write-free. `--dsn` works for
inspection, but explicit apply refuses a DSN override: the supervisor must
restart against the manifest/local-environment target. Use `deployment
up` for release work such as backups, migrations, dependency updates, and a
smoke publish; use `deployment apply` after declarative catalog, Nomos, source,
or runtime edits on an already provisioned deployment.

#### `okg deployment ready`

`okg deployment ready <d>` — report whether the deployment is ready for
agent graph reads. The report covers env resolution, migrations, latest
published generation, queue/DLQ, source freshness, doctor status, and MCP
read alignment. Required readiness dependency failures appear as
phase-specific blockers with `status: unavailable`,
`reason: readiness_dependency_failed`, the dependency name, the exception
text, and the shared state semantics. They must not be collapsed into
`ok: true`, empty data, or a generic clean/stale readiness result.

Under `--checks dogfood`, the `publisher.lock_headroom` check compares the
worst measured publish-lock hold with the capacity bar. A hold over the bar is
a warning, not a blocker: the check reports `status: warning`, its details
carry `status: capacity_warning`, the violations and
`next_action: reduce_publish_lock_hold`, and readiness can still be clean. An
unmeasured hold, or a disabled statement timeout, is reported as `unknown`.

#### `okg deployment bootstrap`

`okg deployment bootstrap {plan|inspect-source|draft|verify|apply} ...`
— plan, inspect, draft, verify, and apply a new deployment.

#### `okg deployment bug-report`

`okg deployment bug-report --deployment <d> [...]` — capture a
structured bug-report bundle for a deployment.
**Implementation:** `src/okg/substrate/support/bug_report.py`

### `okg environment`

`okg environment {show|plan|ready|service|promote}` operates a versioned,
long-lived staging or production environment descriptor. Pass an environment
id from `ops/environments/`, or an explicit `environment.yaml` path. Override
the descriptor root with `--environments-root` or `OKG_ENVIRONMENTS_DIR`.
**Implementation:** `src/okg/substrate/cli/environment.py`

#### `okg environment show`

`okg environment show <environment>` prints the resolved descriptor with
secrets redacted. Confirm the deployment, database, endpoints, release
paths, and service identities before an operation.

#### `okg environment plan`

`okg environment plan <environment>` validates the descriptor against the
current host topology without changing services, releases, or databases.

#### `okg environment ready`

`okg environment ready <environment> --adapter {launchd|systemd}` probes the
installed UI, webhook, MCP, generation, policy, database, and service state.
Use `--launcher-python`, `--systemd-scope`, and `--postgres-image` when those
installed values differ from the defaults.

```bash
uv run okg environment ready staging --adapter systemd --json
```

#### `okg environment service`

`okg environment service {render|install|start|stop|restart|status|logs}
<environment> --adapter {launchd|systemd}` owns the environment's database,
runtime worker, webhook, MCP, and UI services. `render` is read-only; the other
actions emit a plan unless `--apply` is supplied.

```bash
uv run okg environment service render staging --adapter systemd --json
uv run okg environment service restart staging --adapter systemd --apply --json
```

#### `okg environment promote`

`okg environment promote <staging> <production>` validates and then executes
an evidence-bound promotion of one sealed artifact. `--artifact` is the
artifact's `sha256:<64-hex-digest>` identity in the configured artifact store.
The command also requires `--promotion-id`, `--approved-by`,
`--supervision-evidence`, and `--adapter`; pass `--staging-evidence` for a new
approval. The default is a read-only approval plan. `--apply` performs the
serialized, fenced promotion or resumes recovery for the same promotion
identity.

```bash
uv run okg environment promote staging production \
  --artifact sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --promotion-id promote-2026-07-10 \
  --approved-by operator@example.org \
  --staging-evidence .scratch/staging-ready.json \
  --supervision-evidence .scratch/staging-services.json \
  --adapter systemd --json
```

---

## Profile-discovered

Verbs registered by an installed profile (each lives under
`library/profiles/<name>/cli/`). Today's only profile shipping CLI
verbs is `academic-researcher`; see
`src/okg/substrate/library/profiles/academic-researcher/INSTALL.md`
for the install flow.

### `okg add`

`okg add <source> --deployment <d>` — low-level recovery command that
adds one local, path or URL source to an existing Academic Researcher
deployment. `okg add --help` lists the accepted short names. To add a
local folder or GitHub repository to any deployment, use
`okg deployment add-source`.
**Implementation:** `src/okg/substrate/library/profiles/academic-researcher/cli/install.py` (`add`)

### `okg remove`

`okg remove <source> --deployment <d>` — remove one source from an
Academic Researcher deployment's source registry. Records already imported
from that source stay in the graph; later publishes add no new ones.
It does not revoke stored tokens. `okg remove --help` lists the
accepted short names.
**Implementation:** `src/okg/substrate/library/profiles/academic-researcher/cli/install.py` (`remove`)

### `okg reset`

`okg reset --deployment <d>` — remove one installed deployment, whatever its
profile: its directory and agent-runtime registrations. When the
academic-researcher installer started the deployment's Postgres in Docker
(recorded in the `docker:` block of `deployment.yaml`), reset also removes that
Docker project and its data volume. Any other database, such as the one
`--postgres-dsn` named, is not touched and keeps its published graph versions;
the result says `database: not_touched`. If `docker compose down -v` fails, or
`docker` is not on PATH for a recorded project, reset exits non-zero with
`docker_teardown: failed` and keeps the deployment directory so a retry still
knows the project. Confirms first unless `--yes`.
**Implementation:** `src/okg/substrate/library/profiles/academic-researcher/cli/install.py` (`reset`)
