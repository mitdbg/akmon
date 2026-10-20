---
audience: coding agent, developer
type: how-to
canonical_id: adopt-existing-deployment-bootstrap
---
# Adopt an Existing Deployment Into Bootstrap Evidence


Use this recipe when a deployment already has authored files
(`deployment.yaml`, `source_registry.yaml`, schemas, invariants, views,
and related config) but does not yet have a `bootstrap_plan.yaml` or a
recorded bootstrap verification.

This is intentionally a manual recipe, not a built-in import command.
Build an import command only after this exact sequence becomes repeated
mechanical work across multiple deployments.

## Goal

Create a `bootstrap_plan.yaml` that records enough of the existing
deployment surface to prove a bounded first publish slice:

- deployment identity and DSN reference
- representative source definitions
- bounded source samples
- selected ontology and narrowing plan
- extraction/linking/alias/identifier-pattern surface
- quality/view surface where already authored
- verification evidence from a real publish

Do not adopt every source or schema on the first pass; prove the setup
loop on one or two representative sources.

## Safety Rule

For an existing deployment, treat `bootstrap apply --apply` as a
promotion step that can overwrite authored runtime files. Work on a git
branch or a scratch copy, inspect `.okg_bootstrap/staged/`, and only
apply after the staged diff is intentional.

For early adoption work, prefer this loop:

```sh
okg deployment bootstrap draft --deployment <dir> --apply
git -C <dir> add -f -- .okg_bootstrap/staged
git -C <dir> add -- bootstrap_plan.yaml
git -C <dir> commit -m "Stage bootstrap deployment"
git -C <dir> status --short
okg deployment bootstrap verify --deployment <dir> --dsn "$DEV_DSN" --apply
okg deployment bootstrap status --deployment <dir>
```

Delay:

```sh
okg deployment bootstrap apply --deployment <dir> --apply
git -C <dir> add -A -- .
git -C <dir> commit -m "Apply verified bootstrap deployment"
```

until the staged files intentionally preserve or replace the existing
deployment files.

## Recipe

### 1. Snapshot Current Readiness

Run status before creating a plan. This captures the existing authored
surface and confirms the deployment is missing bootstrap evidence, not
runtime files.

```sh
export OKG_AGENT=1

okg deployment bootstrap status \
  --deployment <dir> \
  --json
```

Record:

- source count and source registry path
- schema and bridge-schema counts
- extraction artifacts present
- invariant and view counts
- the reported `stage`

The expected stage for an existing authored deployment without a plan is
`needs_bootstrap_plan`.

### 2. Pick a Proof Slice

Choose one or two sources that exercise the deployment’s real shape while
remaining bounded:

- one small source with stable local fixtures, if available
- one source connected to existing invariants/views, if the deployment
has a consistency story
- one source with identifiers that should exercise linker or alias config

Avoid live API/database sampling until a bounded sample fixture exists.
Bootstrap verification should prove behavior with fixtures first.

### 3. Create Or Update `bootstrap_plan.yaml`

Create the initial plan from a brief and representative sample paths:

```sh
okg deployment bootstrap plan \
  --deployment <dir> \
  --brief <brief.md> \
  --source records=fixtures/records.json \
  --apply
```

Then edit `bootstrap_plan.yaml` manually from the existing deployment
artifacts.

For each selected source, copy the production adapter surface from
`source_registry.yaml` into the matching `sources[]` entry:

```yaml
sources:
  - id: records
    kind: local_json
    sample_path: fixtures/records.json
    allow_live_sampling: false
    adapter:
      module: deployment_sources.records
      class: RecordsSource
    params:
      root: /data/records
    profile:
      source_class: discovery_crawl
      record_identity_kind: scoped_locator
      record_identity_fields: [source_id, record_id]
      source_revision_kind: content_hash
      deletion_semantics: missing_from_completed_scope
      publication_mode: published_generation
```

Do not add a per-source `schedule:` block. The source registry does not
accept one, and bootstrap refuses a plan that has one. A scheduled refresh is
the source's `sync.poll` block in `source_registry.yaml`, run when
`deployment.yaml` sets `runtime.enabled: true`.

The bootstrapper emits two registries:

- `source_registry.yaml`: intended production source config
- `source_registry.bootstrap.yaml`: fixture-backed proof config

### 4. Inspect Samples

Run bounded source inspection and let it populate `record_shapes`.

```sh
okg deployment bootstrap inspect-source \
  --deployment <dir> \
  --apply
```

If inspection fails, fix the sample path or sample format before
touching ontology/extraction plans. Source coverage is the first
blocking substrate stage.

### 5. Fill The Authoring Sections

Edit these sections in `bootstrap_plan.yaml`.

`ontology_plan`:

- copy deployment `modules:` from `deployment.yaml` into
  `library_modules`
- add only the subtypes required for the proof slice
- include searchable text and alias fields when known

`narrowing_plan`:

- add every edge triple the fixture slice will emit
- include any `Document` / `DocumentChunk` bridge narrowings needed by
  the generated fixture records

`extractor_plan`:

- copy relevant identifier patterns from `identifier_patterns.yaml`
- copy minimal linker routes from `linker_config.yaml`
- copy initial aliases from `aliases.json`

`quality_plan`:

- include only invariants that can run against the proof slice
- defer production-wide invariants that require sources outside the
  fixture slice

`view_plan`:

- include existing views only when they can evaluate against the proof
  slice
- otherwise leave a note in the plan evidence and keep views empty

### 6. Draft And Review Staged Files

Materialize staged files:

```sh
okg deployment bootstrap draft \
  --deployment <dir> \
  --apply
```

Review `.okg_bootstrap/staged/` against the existing deployment files.
Then commit the exact staged inputs that verification will publish. The `-f`
is required when the enclosing repository ignores `.okg_bootstrap/`:

```sh
git -C <dir> add -f -- .okg_bootstrap/staged
git -C <dir> add -- bootstrap_plan.yaml
git -C <dir> commit -m "Stage bootstrap deployment"
git -C <dir> status --short
```

Do not verify until `git status --short` is empty.
For existing deployments, this review is mandatory because `apply` will
promote staged runtime files.

Focus on:

- `deployment.yaml`
- `source_registry.yaml`
- `source_registry.bootstrap.yaml`
- generated schema and bridge schema
- `linker_config.yaml`
- `identifier_patterns.yaml`
- `aliases.json`
- `invariants.yaml`
- `views_registry.yaml`

Run status again and check the readiness scorecard:

```sh
okg deployment bootstrap status \
  --deployment <dir>
```

Expected stage after draft is `needs_verification`.

### 7. Verify With A Real Publish

Use a dev or throwaway DSN, not a production deployment DB:

```sh
okg deployment bootstrap verify \
  --deployment <dir> \
  --dsn "$DEV_DSN" \
  --apply
```

The publish records exact repository revisions. Commit the inspected staged
files and `bootstrap_plan.yaml` before verification; verification refuses a
dirty or uncommitted deployment repository.

Verification must record a published generation, catalog version, positive live
node and edge deltas, zero DLQ, and connectivity evidence. Fix the first named
substrate stage if it blocks.

### 8. Decide Whether To Apply

Apply only when both are true:

- verification status is `verified`
- staged runtime files intentionally preserve or replace the existing
  authored deployment files

```sh
okg deployment bootstrap apply \
  --deployment <dir> \
  --apply

git -C <dir> add -A -- .
git -C <dir> commit -m "Apply verified bootstrap deployment"
```

Apply also hands the catalog over. Verify published from
`.okg_bootstrap/staged/`, so that copy owned the database's catalog. After
copying the files, apply claims catalog ownership for `<dir>` and applies its
catalog, so later publishes from `<dir>` are accepted. Apply takes over only
from this deployment's own staged copy; any other owner is handled by the
ordinary ownership rules and still refuses a foreign writer. It uses the
database named by the staged `deployment.yaml` (`postgres.dsn`, with `${VAR}`
expanded); pass `--dsn` when that is not set. Before copying anything, apply
refuses when that database cannot be reached, has no `okg` schema, or is not
the database verify published to (`database_mismatch`; an explicit `--dsn`
overrides this check). The output reports the handover, including the target
host, port and database name, under `catalog_handover`. If the handover
fails, the plan stays `verified`, and the same command can be run again. A
second apply after a successful one is refused as already applied.

If the goal was only to gather evidence, stop after verification and
commit the `bootstrap_plan.yaml` plus any fixture samples. Do not promote
staged files until the deployment owner wants the generated files to
become the runtime source of truth. In a fresh checkout where ignored
staged files are absent, `status` should report `needs_staged_artifacts`;
that is expected for evidence-only adoption and protects `apply` from
using files not present locally.

Manual adoption is complete when status reports `ready_to_apply`, `applied`, or
the intentional evidence-only state `needs_staged_artifacts`; the plan records
the published proof; its live deltas are positive; and DLQ is zero.
