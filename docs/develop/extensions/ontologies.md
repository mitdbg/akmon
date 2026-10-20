---
audience: developer, operator
type: reference
canonical_id: profiles-and-ontologies
---
# Profiles and ontologies


Profiles and ontology modules turn the generic OKG data model into a
deployment-specific graph shape. A profile chooses a reusable
bundle of modules, source defaults, invariants, dashboards, and init
questions. Ontology modules define the graph vocabulary and cross-module
edge rules that a deployment composes into a versioned catalog.

For built-in sources and enrichers, read
[Built-in capabilities](../../reference/capabilities.md). For the data model,
read [The AKMON model](../../start/concepts.md). For the
publish path that applies catalogs and validates graph state, read
[Ingest and publish pipeline](../architecture/pipeline.md).

---

## Profile Contract

Reusable profiles live under
`src/okg/substrate/library/profiles/<name>/`. A profile may include:

| File or directory | Purpose |
|---|---|
| `profile.yaml` | Profile name, version, classification, description, init questions, and sibling file pointers |
| `modules.yaml` | Ordered ontology modules to compose for the deployment |
| `source-defaults/*.yaml` | Source registry snippets with `${var}` interpolation from init answers |
| `invariants.yaml` | Profile-level invariant bundle copied into the deployment |
| `dashboards/*.yaml` | Profile dashboards copied into the deployment |
| `deployment-defaults.yaml` | Optional deployment manifest defaults merged after scaffold-owned keys |
| profile-specific `cli/` or `skills/` | Optional operator tooling shipped by that profile |

`okg install --profile <name>` discovers built-in profiles, cwd-local
profiles under `./profiles/`, and profiles from `$OKG_PROFILES_DIR`.
`okg doctor` checks profile drift when a deployment carries a
`profile:` block: dropped modules or dropped profile sources become
diagnostic findings instead of silent drift.

## Built-In Profiles

| Profile | Intended use | Operating status | Setup path |
|---|---|---|---|
| `minimal` | The default `okg install`: an empty graph a team grows with its own sources and record types | Starting point for a team's own domain graph | [Choose a profile](../../use/profiles/index.md) |
| `workspace-graph` | A repository-centered graph for coding agents and active development work | Contributor and local dogfood use | [Workspace Graph setup](/runbooks/workspace-graph/setup/) |
| `academic-researcher` | A private workstation graph for an academic researcher, professor, or principal investigator: papers, code, projects, grants, and lab administration | Workstation evaluation | [Academic researcher installation](../../use/profiles/academic-researcher.md#install-on-a-workstation) |

No profile in this table implies a managed production service or a
production-readiness guarantee.
Before importing real data, review the profile's requirements, privacy
boundary, sources, and operating responsibilities. The `_test` profile
in the source tree is an internal loader fixture, not a user setup option.
The retired names `git-code`, `systems-research`, `academic-pi`, and
`cs-researcher` are refused with `profile_retired`; the error names
`workspace-graph` as the replacement for `git-code` and
`academic-researcher` as the replacement for `academic-pi` and
`cs-researcher`.

Detailed setup is available for the
[academic researcher](../../use/profiles/academic-researcher.md) profile.

## Composable Ontology Model

A deployment's effective ontology composes library modules and
deployment-local schemas. The catalog loader turns that into a versioned
catalog, and every published generation records the catalog context it
used.

| Piece | What it does | Where it lives |
|---|---|---|
| Module manifest | Names the module and declares provided schema/edge surfaces plus optional `extends` cross-link rule files. | `src/okg/substrate/library/ontologies/<module>/module.yaml` |
| Schema | Defines subtypes, attributes, edge types, and intra-module narrowings. LinkML classes become OKG subtypes only when annotated with an archetype. | `schema.yaml` beside the module manifest |
| Cross-links | Adds narrowings between modules without forcing every deployment to load every module. | `cross_links/*.yaml` |
| Profile module list | Selects module bundles for a deployment shape. | profile `modules.yaml` |
| Module owner | A bare `modules:` entry is an `okg` built-in; `owner:name` picks that owner's provider; one subtype has one owner. | `catalog/module_providers.py` |
| Deployment-local schemas | Adds deployment-owned types or bridges. | `deployments/<name>/schemas/` |
| Catalog compose/apply | Builds and applies the effective catalog. | `src/okg/substrate/catalog/modules_compose.py`, `modules_apply.py` |
| Catalog commands | Operator-facing compose/apply/history surface. | `okg catalog load`, `okg catalog history`, `okg catalog ownership` |

Cross-links keep modules composable: the composer admits a module's rule
about another module only when both endpoint subtypes exist in the active
catalog. `EdgeNarrowings` classes marked with `okg_edge_narrowings: true`
contribute the allowed edge triples. Strict compose checks catch missing bridge dependencies;
publish gates enforce the resulting narrowings on emitted edges.

## Deployment product states

The deployment-product contract keeps reusable distribution material,
instance configuration, deterministic resolution, executable release
material, and applied state separate. Its public package is
`okg.substrate.contracts.deployment_product`.

| Object | Owns | Does not contain |
| --- | --- | --- |
| `DistributionRelease` | Immutable distribution identity, bundles, asset references, and compatibility metadata | Instance values or runtime state |
| `ProductBundle` | Named, tested selection of distribution-owned assets | List-order precedence or copied asset payloads |
| `AuthoredDeploymentDefinition` | Selected release and bundles, instance values, typed overrides, local assets, and secret/connection references | Literal secrets or observed runtime state |
| `ResolvedLock` | Canonical inputs, artifact/package/environment digests, compatibility decisions, override provenance, and final composition digest | Secret values or versions, host state, generations, receipts, evidence, or last-known-good history |
| `SealedReleaseReference` | Reference to existing `okg.sealed_release/v1` executable authority | A second release builder or product artifact |
| `AppliedInstance` | References from an applied definition and lock to the sealed release and operation journal | Product-authoring input |

`validate_selection` handles embedded and external distributions.
`resolve_product_lock` emits the secret-free lock, and
`canonical_lock_bytes` defines its byte representation. `make_action_plan`
binds an expiring plan to the definition, lock, and cached artifact digests;
`require_plan_before_apply` must pass before mutation.

Bundle order and filesystem discovery do not create precedence. Equal identity
and digest deduplicate; conflicting digests, missing references, cycles, or
incompatible bundles fail. Overrides enter only through declared typed slots,
and the lock records owner change, value digest, and reason. Planning may
inspect extension metadata but cannot import executable extension code or
mutate provider or local state. The resolved lock feeds the existing sealed
release builder; unsupported arbitrary asset graphs or subtree merges are
refused rather than assigned invented precedence.

## Catalog reconciliation result

`ReconcilePlanRow` is the JSON shape stored in `plan.yaml` and copied into
`catalog_versions.reconcile_plan`. Its schema is
`schemas/plan.schema.json`. Each row uses one of these shapes:

```text
remove_narrowing  : {change, parent, edge, child, violators, action}
remove_subtype    : {change, subtype,           violators, action}
remove_edge_type  : {change, edge_type,         violators, action}
rename_subtype    : {change, from, to,           violators, action}
rename_edge_type  : {change, from, to,           violators, action}
tighten_attribute : {change, subtype, attribute, violators, action}
```

The allowed `action` values depend on `change` and are closed by the JSON
schema. Extend the schema and migrator together; unknown actions fail instead
of receiving a default reconciliation.

## Entity resolution escalation

`EntityResolutionEscalator` is the optional deployment-owned identity tier in
`src/okg/substrate/entity_resolution.py`. Resolution first tries curated
aliases and equality against live normalized names. It calls the escalator only
when those deterministic tiers decline, and mints a new identifier only when
the escalator explicitly returns `NOVEL`.

```python
class EntityResolutionEscalator(Protocol):
    def resolve(
        self,
        *,
        conn: psycopg.Connection,
        subtype: str,
        raw_name: str,
        normalized_key: str,
        graph_token: str,
        attrs: Mapping[str, Any],
        graph: Any,
    ) -> ResolutionDecision | None: ...
```

Configure it per subtype:

```yaml
entity_resolution:
  policies:
    measurement:
      escalation:
        class: my_deployment.identity.SlotKeyedEscalator
        config: {slots: [target_region, uses_method]}
```

The contract is fail closed:

- Query candidates through the pinned checkout with
  `graph.query(sql, params)`, never through `conn`. Chronos-managed graph base
  tables can return no rows on the bare connection without raising an error.
- Treat `attrs` and `graph` as read-only. `attrs` is a read-only deep copy;
  `graph` exposes mutation methods because it is the live checkout, but an
  escalator has no write authority.
- Return `MATCHED` with `canonical_node_id`, explicit `NOVEL`, `REJECTED`,
  `AMBIGUOUS`, or `UNAVAILABLE`. Returning `None`, raising, or omitting the
  canonical id on a match becomes `entity_resolution_unavailable` and never
  mints a node.
- `materialize=False` keeps the matched node's attributes; `True` emits the
  proposed attributes against the canonical id. The escalator must not use
  `True` for a node it does not own. An existing identity-claim row
  short-circuits the escalator for the same normalized name.
- Classes that cannot accept `attrs` and `graph` are refused when first used
  for a subtype. Changing a subtype's escalator changes the policy fingerprint,
  so claims made under the old policy no longer match.

Matching spans extraction runs. Nodes proposed earlier in the same enrichment
batch are not visible because its checkout predates the batch.

## Built-In Ontology Modules

AKMON currently ships 47 reusable ontology modules under the library
directory.

| Group | Modules | Purpose |
|---|---|---|
| Shared foundations | `concepts`, `person`, `dataset`, `document_starter`, `repo_starter`, `service`, `search_systems`, `incident`, `ticket`, `personas`, `external_authority`, `code_artifact` | Common hubs and starter shapes used by more specialized deployments. |
| Code and engineering | `git_graph`, `code_graph`, `code`, `openspec`, `forge`, `ci`, `quality_evidence`, `compute_env`, `external_repos` | Repositories, commits, files, symbols, package dependencies, legacy OpenSpec records, PACT-era change records, issues/PRs, CI, scanners, runtime facts, and external repo references. |
| Agent workflow | `agent_sessions`, `agent_memory`, `coordination`, `views`, `views_annotations` | Agent transcripts, tool-use traces, durable claims/decisions, supervisor/work-order state, materialized views, and PDF/viz annotations. |
| Research knowledge | `scholarly`, `reasoning`, `methodology`, `experimental`, `engram_runs`, `extraction` | Papers, authors, venues, claims, hypotheses, evidence, principles, experiments, Engram run artifacts, and extracted document claims. |
| Academic and collaboration | `academic_researcher`, `calendar`, `communications`, `bookkeeping` | Lab-management state, calendar events, email/chat surfaces, meetings, action items, projects, milestones, risks, and stakeholders. |
| Policy and governance | `nomos` | Policy bundles, identity claims, decisions, obligations, and audit records for authorization-sensitive effects. |

### Ontology module manifest inventory

The module names below come from the built-in `module.yaml` manifests.
Profile `modules.yaml` files select from this inventory; a module no
profile selects is still available for a deployment-local composition.

| Module | Version | Manifest |
|---|---|---|
| `academic_researcher` | `1.0.0` | `src/okg/substrate/library/ontologies/academic_researcher/module.yaml` |
| `agent_memory` | `1.0.0` | `src/okg/substrate/library/ontologies/agent_memory/module.yaml` |
| `agent_sessions` | `1.0.0` | `src/okg/substrate/library/ontologies/agent_sessions/module.yaml` |
| `bookkeeping` | `1.0.0` | `src/okg/substrate/library/ontologies/bookkeeping/module.yaml` |
| `calendar` | `1.0.0` | `src/okg/substrate/library/ontologies/calendar/module.yaml` |
| `chat_sessions` | `1.0.0` | `src/okg/substrate/library/ontologies/chat_sessions/module.yaml` |
| `ci` | `1.0.0` | `src/okg/substrate/library/ontologies/ci/module.yaml` |
| `code` | `1.0.0` | `src/okg/substrate/library/ontologies/code/module.yaml` |
| `code_artifact` | `1.0.0` | `src/okg/substrate/library/ontologies/code_artifact/module.yaml` |
| `code_graph` | `1.0.0` | `src/okg/substrate/library/ontologies/code_graph/module.yaml` |
| `communications` | `1.0.0` | `src/okg/substrate/library/ontologies/communications/module.yaml` |
| `compute_env` | `1.0.0` | `src/okg/substrate/library/ontologies/compute_env/module.yaml` |
| `concepts` | `1.0.0` | `src/okg/substrate/library/ontologies/concepts/module.yaml` |
| `consistency` | `1.0.0` | `src/okg/substrate/library/ontologies/consistency/module.yaml` |
| `coordination` | `1.0.0` | `src/okg/substrate/library/ontologies/coordination/module.yaml` |
| `cti` | `1.0.0` | `src/okg/substrate/library/ontologies/cti/module.yaml` |
| `dataset` | `1.0.0` | `src/okg/substrate/library/ontologies/dataset/module.yaml` |
| `doc_snapshot` | `1.0.0` | `src/okg/substrate/library/ontologies/doc_snapshot/module.yaml` |
| `document_starter` | `1.0.0` | `src/okg/substrate/library/ontologies/document_starter/module.yaml` |
| `engram_runs` | `1.0.0` | `src/okg/substrate/library/ontologies/engram_runs/module.yaml` |
| `experimental` | `1.0.0` | `src/okg/substrate/library/ontologies/experimental/module.yaml` |
| `external_authority` | `1.0.0` | `src/okg/substrate/library/ontologies/external_authority/module.yaml` |
| `external_repos` | `1.0.0` | `src/okg/substrate/library/ontologies/external_repos/module.yaml` |
| `extraction` | `1.0.0` | `src/okg/substrate/library/ontologies/extraction/module.yaml` |
| `forge` | `1.0.0` | `src/okg/substrate/library/ontologies/forge/module.yaml` |
| `git_graph` | `1.0.0` | `src/okg/substrate/library/ontologies/git_graph/module.yaml` |
| `incident` | `1.0.0` | `src/okg/substrate/library/ontologies/incident/module.yaml` |
| `kinship` | `1.0.0` | `src/okg/substrate/library/ontologies/kinship/module.yaml` |
| `legal` | `1.0.0` | `src/okg/substrate/library/ontologies/legal/module.yaml` |
| `literature` | `1.0.0` | `src/okg/substrate/library/ontologies/literature/module.yaml` |
| `methodology` | `1.0.0` | `src/okg/substrate/library/ontologies/methodology/module.yaml` |
| `nomos` | `1.0.0` | `src/okg/substrate/library/ontologies/nomos/module.yaml` |
| `openspec` | `1.0.0` | `src/okg/substrate/library/ontologies/openspec/module.yaml` |
| `ops` | `1.0.0` | `src/okg/substrate/library/ontologies/ops/module.yaml` |
| `person` | `1.0.0` | `src/okg/substrate/library/ontologies/person/module.yaml` |
| `personas` | `1.0.0` | `src/okg/substrate/library/ontologies/personas/module.yaml` |
| `quality_evidence` | `1.0.0` | `src/okg/substrate/library/ontologies/quality_evidence/module.yaml` |
| `reasoning` | `1.0.0` | `src/okg/substrate/library/ontologies/reasoning/module.yaml` |
| `repo_starter` | `1.0.0` | `src/okg/substrate/library/ontologies/repo_starter/module.yaml` |
| `scholarly` | `1.0.0` | `src/okg/substrate/library/ontologies/scholarly/module.yaml` |
| `search_systems` | `1.0.0` | `src/okg/substrate/library/ontologies/search_systems/module.yaml` |
| `service` | `1.0.0` | `src/okg/substrate/library/ontologies/service/module.yaml` |
| `stark_prime` | `1.0.0` | `src/okg/substrate/library/ontologies/stark_prime/module.yaml` |
| `ticket` | `1.0.0` | `src/okg/substrate/library/ontologies/ticket/module.yaml` |
| `views` | `1.0.0` | `src/okg/substrate/library/ontologies/views/module.yaml` |
| `views_annotations` | `1.0.0` | `src/okg/substrate/library/ontologies/views_annotations/module.yaml` |
| `xbrl` | `1.0.0` | `src/okg/substrate/library/ontologies/xbrl/module.yaml` |

The full module directory is
`src/okg/substrate/library/ontologies/`. Tests under
`tests/substrate/library/ontologies/` and `tests/substrate/catalog/`
prove module manifests, schema composition, abstract subtype
refinements, and cross-link behavior.

## Profile-To-Ontology Examples

| Profile | Defining ontology choices |
|---|---|
| `minimal` | `document_starter` only; `okg deployment add-source` adds the modules each new source needs |
| `workspace-graph` | `git_graph`, `openspec`, `agent_sessions`, `agent_memory`, `compute_env`, `code_graph`, `extraction`, `reasoning`, `ci`, `quality_evidence`, `views` |
| `academic-researcher` | Research core (extraction, concepts, person, views, reasoning, methodology, experimental, scholarly, coordination, CI, quality evidence, bookkeeping, agent sessions, compute env, agent memory, dataset, git graph, code graph, legacy OpenSpec archive support, external repos) plus `communications`, `calendar`, `academic_researcher`, PI invariants, and lab-management source defaults |

## How To Add A Profile

1. Create `src/okg/substrate/library/profiles/<name>/profile.yaml`.
2. Add `modules.yaml` with the smallest module set that composes cleanly.
3. Add source defaults only for sources that the profile can explain and
   test.
4. Add invariants and dashboards that express the profile's operating
   posture.
5. Add profile-loader or CLI tests under `tests/substrate/cli/`.
6. Document the profile in this page and, when it needs operator
   guidance, under `docs/use/profiles/`.

## How To Add Or Compose An Ontology Module

1. Add `module.yaml` and `schema.yaml` under
   `src/okg/substrate/library/ontologies/<module>/`.
2. Annotate every LinkML class that should become a catalog subtype
   with an OKG archetype.
3. Put intra-module narrowings in an `EdgeNarrowings` class annotated
   with `okg_edge_narrowings: true`.
4. Put cross-module narrowings in `cross_links/*.yaml`.
5. Add the module to a profile `modules.yaml` or a deployment's
   `modules:` list only after compose tests pass.
6. Run `okg catalog load --deployment <deployment>` before `--apply` to
   inspect subtype, narrowing, alias, identifier-pattern, and bridge
   counts.
7. Verify the change with compose tests, gate behavior, and at least one
   source or fixture that emits the new subtype/edge when the module is
   meant to be populated.

Do not put a deployment-specific subtype in the shared library unless it
is reusable outside that deployment. Deployment-local schemas belong
under `deployments/<name>/schemas/`, with bridge files under that
deployment when the relationship is not generally reusable.
