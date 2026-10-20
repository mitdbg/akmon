# Formal Model Design Contract

Formal models and production code must both answer to a design contract. The
contract is the intended behavior: it decides whether a mismatch is a code bug,
a model bug, missing formal coverage, a boundary/precondition, or a mapping bug.

## Decision Authority

When a formal model and production code disagree, reviewers decide against the
design contract, not against whichever artifact changed first.

Use this order:

1. Define the safety property and authority boundary.
2. List valid runtime paths, including intentional exceptional paths.
3. State what the model deliberately excludes.
4. Identify model preconditions.
5. Bind each precondition to upstream proof, runtime checks, or refinement tests.
6. Classify the mismatch before opening implementation work.

## Required README Section

Every direct formal model folder must include a `Design Contract` section with
these fields:

- `Safety property:` the intended correctness claim.
- `Authority boundary:` which runtime surface owns the decision.
- `Valid runtime paths:` production paths the model must include or explicitly
  exclude.
- `Out of scope:` behavior that remains covered by runtime tests or live
  substrate evidence.
- `Preconditions/refinement:` model assumptions and the tests or upstream proofs
  that keep those assumptions honest.

## Mismatch Taxonomy

Use one of these classifications before deciding a fix:

| Classification | Meaning |
| --- | --- |
| `code_bug` | Production behavior violates the intended design contract. |
| `model_bug` | The model excludes valid behavior or proves the wrong property. |
| `missing_formal_coverage` | Important production behavior has no formal property yet. |
| `boundary_precondition` | The model is valid only under an assumption that must be documented, proven upstream, or checked at runtime. |
| `mapping_bug` | README, checker, report, or test references point at the wrong evidence. |

## TLC Outcome Classification

Formal report consumers must distinguish checker execution status from model
semantics. A TLC timeout means the property was not freshly proven inside the
selected execution profile; it is not evidence of a violated invariant. Timeout
rows therefore use `status: fail`, `fresh: false`, `failure_class: timeout`,
and `proof_status: not_freshly_proven`. A real invariant violation uses
`failure_class: invariant_violation` and `proof_status: semantic_failure`.
Only rows with `status: pass`, `fresh: true`, and
`proof_status: freshly_proven` can satisfy strict PACT formal evidence.

## Preconditions And Refinement

A model precondition is acceptable only when it is visible and load-bearing. It
must be documented in the folder README and must name at least one of:

- an upstream formal model that proves it;
- a runtime check that rejects violations;
- a refinement or implementation test that fails when the assumption is false.

If none of those exists, classify the gap as `missing_formal_coverage` or
`boundary_precondition` in `formal/gap-inventory.toml` before changing code or
model behavior.

## Current Gap Inventory

`formal/gap-inventory.toml` records the current high-priority formal review
gaps. It is not the full issue tracker; it is the design-classification ledger
used to decide whether follow-up work changes the model, code, documentation,
checker evidence, or runtime tests.

## Boundary Inventory

`formal/boundary-inventory.toml` records production surfaces that are not
directly proven by a formal model, plus any audit examples that have since
become directly modeled. This keeps intentional runtime proof boundaries from
looking like accidental formal coverage.

Boundary rows use these fields:

- `id`: stable dotted identifier.
- `area`: owning assurance or production area.
- `title`: human-readable surface name.
- `classification`: one of `needs_formal_model`, `runtime_test_boundary`,
  `covered_indirectly`, or `obsolete_archive_only`.
- `owner_surface`: primary code, docs, or model owner.
- `rationale`: why the row has this classification.
- `evidence_refs`: runtime tests, docs, helpers, or source files that keep the
  boundary honest.
- `gap_id`: required only when `classification` is `needs_formal_model`.

Rows classified as `needs_formal_model` must have a matching entry in
`formal/gap-inventory.toml`. Other rows must not claim gap status; they must
name runtime evidence, indirect model coverage, or archive-only status instead.
Audit examples that are now directly modeled belong under `direct_models` with
`model_refs` and `evidence_refs`, not under runtime-only boundaries.

## Publish-eligibility decomposition

No single formal model proves end-to-end generation-publish eligibility. The
preconditions a real generation must satisfy before it becomes publish-visible
are owned by different models, each scoped to one surface. A reviewer must
compose them; treating any one model's "publish" action as the whole eligibility
contract is a `mapping_bug`.

| Precondition | Owning model | What the model proves |
| --- | --- | --- |
| Policy / bundle activation | `formal/nomos` (`Nomos.tla`) | A generation can only publish under an active policy bundle, the publish pins that bundle, and the pin is exactly the bundle active at publish time (never restamped by rotation; `GenerationPinMatchesPublishBundle`). This is deliberately policy-only: it does not check refs, narrowing, or Chronos status. **Precondition:** the row applies only when the deployment enables Nomos runtime enforcement -- `deployment_requires_nomos_runtime` false means `Publisher._nomos_generation_publish_blocker` returns without evaluation and the publish proceeds outside this model's guard (the nomos-off publish path, documented in `formal/nomos/README.md` Model Boundary). |
| Refs / projectability / idempotency | `formal/admission` (`Admission.tla`) | Source and derived facts become publish-visible only after admission policy, projectability, idempotency, and generation-feedback checks pass. |
| Narrowing / node-subtype scope | `formal/consistency` (`TransformGateDirtyScope.tla`, `TransformGateNodeSubtypeScope.tla`) | Candidate edges and dirty nodes must satisfy active-catalog narrowing and node-subtype validation (scoped or full fallback) before the gate passes. |
| Pin / blocked-status | `formal/chronos` (`ChronosReadPin.tla`) | Reads use an explicit session pin or preserved compatibility id; pin and blocked-status discipline governs which generation a session observes. |

Because these surfaces are proven independently, an end-to-end publish-eligibility
claim is a composition argument over the four models, not a single invariant. When
a publish behaves unexpectedly, classify the mismatch against the owning model for
the failing precondition rather than against whichever model names a "publish"
action.
