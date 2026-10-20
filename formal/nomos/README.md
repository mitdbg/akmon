# Nomos Formal Models

## System

The Nomos model covers OKG's policy layer: policy graph admission,
configuration graph admission, bundle compilation and activation,
configuration activation, request evaluation, Postgres-backed checks,
deployment and source posture, runtime actions, privacy/security obligations,
export visibility, external identity provider and claim handling, scoped
subject bindings, scoped permission grants, publication pins, and decision
traces.

## Design Contract

- Safety property: policy/config activation, request decisions, export
  visibility, deployment/source sensitivity, obligations, and external claims
  cannot broaden authority without an allowed Nomos decision.
- Source-lineage boundary: graph facts derived from one or more sources are
  visible only when every contributing source is authorized and policy
  fingerprints are fresh; partial, missing, malformed, unknown, or stale-policy
  lineage fails closed and cannot be made visible through redaction or
  declassification.
- Authority boundary: Nomos core, store, and identity code own policy decisions;
  deployment tests prove domain-specific public/private behavior.
- Valid runtime paths: bundle activation, config activation, allow/deny/review
  decisions, quarantine, redact/declassify decisions, external IAM claims,
  scoped grants, public export decisions, source/deployment mismatch handling,
  and no-export/secret obligations.
- IAM authority boundary: `src/okg/substrate/nomos/identity.py` is the source
  of truth for provider, claim, binding, grant, scope, action, classification,
  priority, effective-window, and resource-match semantics. The formal model
  covers the finite provider/claim/binding/grant/scope/action/classification
  abstraction: verify the claim/provider, resolve an active subject binding for
  the requested scope, then match an active allow grant for the resolved
  subject, scope, action, and classification. Runtime tests cover issuer,
  claim-value, priority, effective-window, and resource-match details.
- Activation evidence boundary: production policy and config activation rows
  always carry a recorded decision id. Normal enforced activations are evaluated
  under the active policy. The first policy activation may use the explicit
  bootstrap path, which records `nomos.bootstrap.first_policy_activation`.
  Deployments that have not enabled Nomos runtime enforcement still record an
  allow decision with `nomos.runtime.opt_out.activation`; they do not store a
  nullable activation decision. Upgraded databases backfill old nullable
  activation rows to `nomos.migration.activation_decision_backfill` decisions
  before enforcing the non-null table constraint.
- Out of scope: policy language completeness, database isolation, and deployment
  content semantics remain implementation and deployment-test concerns.
- Preconditions/refinement: implementation tests connect runtime IAM rows and
  decision contexts to the finite provider, claim, binding, grant, subject, and
  scope identifiers in `Nomos.tla`.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `Nomos.tla` | `Nomos.cfg` | Positive Nomos policy/configuration enforcement contract. |
| `Nomos.tla` | `NomosSourceLineage.cfg` | Focused positive source-lineage visibility contract for all-of-lineage graph reads. |
| `Nomos.tla` | `NomosFineGrainedAccess.cfg` | Focused PACT-bound fine-grained visibility contract for source-record, graph-object, as-of horizon, and unaddressable-lineage graph reads. |
| `Nomos.tla` | `NomosIamClaims.cfg` | Focused positive institutional IAM claim-isolation contract (`NomosIamClaimIsolationContract`): eight otherwise-grantable requests, one per claim check, plus a valid positive-control request. Kept OUT of `Nomos.cfg` so the binding/grant/claim combinatorics do not multiply the full control-plane request universe -- this is a decomposition for speed, not a coverage cut (each `*CannotGrant` invariant is proven non-vacuously here and holds trivially in `Nomos.cfg`). |
| `Nomos.tla` | `NomosPinRotation.cfg` | Focused positive publish-pin contract (`NomosPublishPinContract`) over a minimal request universe with the bundle universe >= 2 and `RotateBundle`, proving `GenerationPinMatchesPublishBundle` distinguishing. Kept out of `Nomos.cfg` so the pin/rotation state space does not multiply the control-plane config. |
| `Nomos.tla` | `NomosSecretPersistenceCounterexample.cfg` | Non-vacuity witness over the base model: sets `BuggySecretPersistence = TRUE` and names a real secret-bearing claim so `PersistSecretEvidenceBug` is reachable, making `persistedSecretEvidence` non-empty and `ClaimEvidenceDoesNotPersistSecret` falsifiable. |
| `Nomos.tla` | `NomosUntrustedProviderCounterexample.cfg`, `NomosExpiredClaimCounterexample.cfg`, `NomosAudienceMismatchCounterexample.cfg`, `NomosMissingAssuranceCounterexample.cfg`, `NomosAmbiguousBindingCounterexample.cfg`, `NomosRevokedProviderClaimCounterexample.cfg`, `NomosRevokedKerberosCounterexample.cfg`, `NomosUnverifiedClaimCounterexample.cfg` | Buggy-toggle witnesses over the focused `NomosIamClaims.cfg` universe: each config sets `BuggySkippedClaimChecks` to exactly one claim-check mode, letting that mode's isolated request through and refuting the REAL `*CannotGrant` invariant by name. Each institutional IAM failure mode has its own request, so the eight invariants are tested in isolation (one deny cannot mask another check's regression). `NomosUnverifiedClaimCounterexample.cfg` is the issue #1007 arm: it skips the `unverified` mode so a claim carrying NO verification evidence gets through, refuting `UnverifiedClaimCannotGrant` by name. |
| `Nomos.tla` | `NomosStalePinPublishCounterexample.cfg`, `NomosRotationRestampCounterexample.cfg` | Buggy-toggle witnesses over the focused `NomosPinRotation.cfg` universe: `BuggyPublishPinsStaleBundle` stamps a non-active bundle at publish, `BuggyRotationRestampsPins` rewrites the pins of already-published generations on rotation; both refute `GenerationPinMatchesPublishBundle` by name against the frozen `pinnedAtPublish` ghost. |
| `NomosCounterexample.tla` | `NomosCounterexample.cfg` | Aggregate expected-failing negative coverage for direct TLC exploration. |
| `NomosCounterexample.tla` | `NomosMissingPolicyCounterexample.cfg`, `NomosWeakDeploymentPostureCounterexample.cfg`, `NomosUnscopedIdentityCounterexample.cfg`, `NomosUnknownActionCounterexample.cfg`, `NomosPrivateExportCounterexample.cfg`, `NomosSourceDeploymentMismatchCounterexample.cfg` | Per-witness expected-failing negative coverage used by the strict checker. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `Nomos.tla` | Policy and config activation rows point at recorded allow decisions. Enforced activations are evaluated through the active policy, first-policy bootstrap records a bootstrap allow decision, runtime-enforcement opt-out records an explicit opt-out allow decision, and migration backfills legacy NULL rows before enforcing `NOT NULL`. | `src/okg/substrate/nomos/store.py`, `src/okg/substrate/nomos/refinement.py`, `src/okg/substrate/db/schema.sql` | `tests/substrate/nomos/test_nomos_active_store.py`, `tests/substrate/nomos/test_nomos_core.py` |
| `Nomos.tla` | Policy and configuration inputs compile into active bundles and config rows. | `src/okg/substrate/nomos/core.py`, `src/okg/substrate/nomos/store.py`, `src/okg/substrate/cli/nomos.py` | `tests/substrate/nomos/test_nomos_core.py` |
| `Nomos.tla` | Requests become allow, deny, quarantine, review, redact, declassify, or visible-output decisions under active policy. | `src/okg/substrate/nomos/core.py`, `src/okg/substrate/nomos/refinement.py` | `tests/substrate/nomos/test_nomos_core.py`, `test_wiki_gate.py` of a private deployment (not in this copy) |
| `Nomos.tla` | Runtime decision traces carry canonical action, classification, source sensitivity, deployment class, enforcement point, policy/config pins, and obligations. | `src/okg/substrate/nomos/core.py`, `src/okg/substrate/nomos/vocabulary.py`, `src/okg/substrate/nomos/refinement.py` | `tests/substrate/test_formal_nomos.py`, `tests/substrate/nomos/test_nomos_core.py` |
| `Nomos.tla` | External identity claims are normalized against provider trust, audience, assurance, freshness, MFA, and revocation checks before a subject binding can resolve. | `src/okg/substrate/nomos/identity.py`, `src/okg/substrate/nomos/core.py` | `tests/substrate/nomos/test_nomos_core.py`, `tests/substrate/nomos/test_nomos_institutional_iam.py` |
| `Nomos.tla` | Subject bindings and permission grants are represented by ids. A Postgres-backed external claim can use the active bundle only when an active binding matches provider/claim/subject/scope and an active allow grant matches subject/scope/action/classification. The refinement mapper excludes inactive binding/grant rows, matching the runtime loaders. | `src/okg/substrate/nomos/identity.py`, `src/okg/substrate/nomos/identity_store.py`, `src/okg/substrate/nomos/refinement.py`, `src/okg/substrate/nomos/store.py` | `tests/substrate/nomos/test_nomos_core.py`, `tests/substrate/nomos/test_nomos_identity_store.py`, `tests/substrate/nomos/test_nomos_institutional_iam_postgres.py` |
| `Nomos.tla` | Publishing a generation stamps the active policy bundle pin on the generation's checkpoint metadata; rotating to a new bundle resets per-scope config activation and must not restamp the pins of already-published generations. With runtime enforcement on, `generation.publish` is evaluated as a Nomos runtime boundary and a deny blocks the publish. | `src/okg/substrate/publisher/publisher.py`, `src/okg/substrate/nomos/store.py`, `src/okg/substrate/nomos/runtime_enforcement.py` | `tests/substrate/nomos/test_nomos_active_store.py`, `tests/substrate/nomos/test_nomos_runtime_enforcement.py` |
| `Nomos.tla` | Source-lineage graph requests require authorization for the full contributing source set before visible/public output. | `src/okg/substrate/nomos/source_lineage.py`, `src/okg/substrate/nomos/read_filter.py`, `src/okg/substrate/enrichers/derived_edges.py` | `tests/substrate/nomos/test_nomos_source_lineage_derived_closure.py`, `tests/substrate/nomos/test_nomos_institutional_iam_postgres.py` |
| `NomosCounterexample.tla` | Missing-policy, weak-deployment, unscoped-identity, unknown-action, private-export, and source/deployment mismatch failures are intentionally reachable. | `src/okg/substrate/nomos/core.py`, `src/okg/substrate/nomos/identity.py` | `tests/substrate/test_formal_nomos.py`, `tests/substrate/nomos/test_nomos_core.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `ConstantSetsOK`, `TypeOK` | Request, provider, role, policy, and config universes are well formed. | Policy fixture setup in `tests/substrate/nomos/test_nomos_core.py`. |
| `DefaultDeny`, `NoVisibleWithoutAllow`, `QuarantineNotVisible`, `NoPrivatePublicLeak`, `UnknownLicenseCannotPublicOutput`, `PrivateExportRequiresScopedClaim` | Visibility requires an explicit allow; quarantine/private/unknown-license cases cannot leak to public output. | Export and wiki gate tests in `test_wiki_gate.py` of a private deployment (not in this copy). |
| `MissingPolicyCannotAllow`, `UnknownActionCannotAllow`, `WeakDeploymentPostureCannotAllow`, `SourceDeploymentMismatchCannotAllow`, `NoExportObligationBlocksPublicOutput`, `DecisionObligationsComplete` | Missing policy, unknown actions, weak deployment posture, source/deployment sensitivity mismatch, no-export, and effect-specific obligation cases fail closed. | Formal wrapper/refinement tests in `tests/substrate/test_formal_nomos.py` plus runtime evaluator tests in `tests/substrate/nomos/test_nomos_core.py`. |
| `GenerationPolicyPinned`, `GenerationPinMatchesPublishBundle`, `DecisionReplayDeterministic`, `DecisionTraceComplete`, `DecisionTraceCitesProviderAndBinding` | Decisions cite the active policy/config inputs and replay deterministically with a complete trace. A published generation's policy pin is exactly the bundle active at publish time (frozen in the `pinnedAtPublish` ghost) and never moves afterwards -- distinguishable because the bundle universe is >= 2 and `RotateBundle` can change the active bundle between publishes; the stale-pin and rotation-restamp hazards each have a named witness. Allowed external IAM decisions must cite a provider, claim envelope, active subject binding, and active permission grant that match the request's subject, scope, action, and classification abstraction. | Core evaluator tests in `tests/substrate/nomos/test_nomos_core.py` and institutional IAM refinement tests in `tests/substrate/nomos/test_nomos_institutional_iam_postgres.py`. |
| `ConfigActivationAtomic`, `NoUnauthorizedConfigChange`, `PostgresEnforcementUsesActiveBundle` | Config changes activate only with an allowed recorded decision, unauthorized config changes fail before activation rows are written, and Postgres-backed checks use the active bundle/config pair only after matching active IAM binding and grant relations. | `tests/substrate/nomos/test_nomos_active_store.py` checks enforced, bootstrap, and runtime opt-out activation decisions. `tests/substrate/nomos/test_nomos_core.py` checks refinement rows reject missing activation decision evidence, map bindings by id, and filter inactive IAM rows. |
| `UntrustedProviderCannotGrant`, `ExpiredClaimCannotGrant`, `AudienceMismatchCannotGrant`, `MissingAssuranceCannotGrant`, `AmbiguousBindingCannotGrant`, `RevokedProviderCannotGrant`, `RevokedKerberosServicePrincipalCannotGrant`, `UnverifiedClaimCannotGrant` | External claims cannot grant access when provider trust, freshness, audience, assurance, binding, or revocation checks fail. Matching grants are considered only after subject binding resolution. Each of the eight checks has its OWN otherwise-grantable request in the focused `NomosIamClaims.cfg` (isolation: no other failure mode or authorization layer masks it) and a buggy-toggle witness config (`BuggySkippedClaimChecks`) that skips exactly that check and refutes the invariant by name. `NomosIamClaims.cfg` holds them out of `Nomos.cfg` so the claim combinatorics do not multiply the control-plane state space. | Institutional IAM tests in `tests/substrate/nomos/test_nomos_institutional_iam.py`, identity-store tests in `tests/substrate/nomos/test_nomos_identity_store.py`, and Postgres IAM tests in `tests/substrate/nomos/test_nomos_institutional_iam_postgres.py`. |
| `ExternalRolesAreScoped`, `ClaimEvidenceDoesNotPersistSecret`, `GraphProfileCannotBroaden`, `AgentCannotSelfExpand` | Imported roles stay scoped, secrets are not persisted, graph profile hints cannot broaden access, and agents cannot grant themselves more power. `ClaimEvidenceDoesNotPersistSecret` is now a reachable-state invariant with a non-vacuity witness: the base model carries the `persistedSecretEvidence` state variable and a `BuggySecretPersistence` CONSTANT, so `NomosSecretPersistenceCounterexample.cfg` enables the buggy claim-acceptance path and TLC refutes the invariant. On the safe `Nomos.cfg` the flag is `FALSE`, `PersistSecretEvidenceBug` is disabled, and the invariant holds over reachable state instead of vacuously over an empty set. | Identity and profile-hint tests in `tests/substrate/nomos/test_nomos_institutional_iam.py` and `tests/substrate/nomos/test_nomos_core.py`. `NomosSecretPersistenceCounterexample.cfg` must report `ClaimEvidenceDoesNotPersistSecret` violated. |
| `NomosSourceLineageVisibility` / `formal.nomos-source-lineage.visibility` | Mixed-source graph facts require authorization for the full lineage set; partial, missing, malformed, unknown, or stale-policy source lineage is hidden or quarantined and cannot be exposed via redaction/declassification. | `uv run python scripts/check_formal_nomos.py --profile smoke --out .scratch/formal/nomos-smoke --timeout-seconds 30` plus source-lineage and institutional IAM Postgres tests. |
| `NomosFineGrainedVisibility` / `formal.nomos-fgac.fine-grained-lineage` | Fine-grained graph facts require authorization for every addressable source-record or graph-object contributor; future-horizon, graph-denied, default-allow, and unaddressable context-sensitive rows fail closed. | `uv run python scripts/check_formal_nomos.py --profile smoke --config NomosFineGrainedAccess.cfg --property-id formal.nomos-fgac.fine-grained-lineage --out .scratch/nomos/fine-grained-access-control-formal --timeout-seconds 30` plus fine-grained access-control tests. |
| `NomosControlPlaneContract` | Combined control-plane contract over the positive safety invariants. | `uv run python scripts/check_formal_nomos.py --profile full --out .scratch/formal/nomos-control-plane --timeout-seconds 900` emits a strict formal report row. |
| `NomosIamClaimIsolationContract` / `formal.nomos-iam-claims.isolation` | Conjunction proven by the focused `NomosIamClaims.cfg`: the seven institutional claim checks each block their own isolated, otherwise-grantable request, an allowed external request cites a matching provider/binding/grant, and imported roles stay scoped. Decomposed out of the full config for speed; each conjunct has a named buggy-toggle witness. | `uv run python scripts/check_formal_nomos.py --profile iam --out .scratch/formal/nomos-iam` emits a strict formal report row; gated in the smoke suite as `nomos-iam-claim-isolation`. |
| `NomosPublishPinContract` / `formal.nomos-publish-pins.integrity` | Conjunction proven by the focused `NomosPinRotation.cfg` over a bundle universe >= 2 with `RotateBundle`: a published generation's policy pin is exactly the bundle active at publish time and is never restamped by a later rotation. | `uv run python scripts/check_formal_nomos.py --profile pins --out .scratch/formal/nomos-pins` emits a strict formal report row; gated in the smoke suite as `nomos-publish-pin-rotation`. |
| `NomosNonVacuityMissingPolicy`, `NomosNonVacuityWeakDeploymentPosture`, `NomosNonVacuityUnscopedIdentityClaim`, `NomosNonVacuityUnknownAction`, `NomosNonVacuityPrivateExportWithoutScopedClaim`, `NomosNonVacuitySourceDeploymentMismatch` | The negative model must find each expected unsafe trace independently, and the checker rejects the wrong invariant or unrelated TLC/tool error for any witness. | `make formal-nomos-counterexample` must produce a strict formal report row for every witness config. |
| `NomosControlPlaneNonVacuity`, `BrokenMissingPolicyCanAllow`, `BrokenWeakDeploymentPostureCanAllow`, `BrokenUnscopedIdentityClaimCanGrant`, `BrokenUnknownActionCanAllow`, `BrokenPrivateExportWithoutScopedClaimCanPublish`, `BrokenSourceDeploymentMismatchCanAllow` | Aggregate direct-TLC negative invariant and named broken predicates used for exploration and per-witness bindings. | `tests/substrate/test_formal_nomos.py` verifies the required witness configs exist and the strict checker uses per-witness rows. |

## Execution Profiles

| Profile | Config | Budget | Purpose | Command |
| --- | --- | --- | --- | --- |
| `smoke` | `NomosSourceLineage.cfg` | 30s | CI/operator freshness check for source-lineage visibility; not full control-plane release evidence. | `uv run python scripts/check_formal_nomos.py --profile smoke --out .scratch/formal/nomos-smoke --timeout-seconds 30` |
| `iam` | `NomosIamClaims.cfg` | 120s | Focused IAM claim-isolation evidence for `NomosIamClaimIsolationContract` (~50K states); the seven `*CannotGrant` invariants proven non-vacuously in isolation. | `uv run python scripts/check_formal_nomos.py --profile iam --out .scratch/formal/nomos-iam` |
| `pins` | `NomosPinRotation.cfg` | 60s | Focused publish-pin evidence for `NomosPublishPinContract` (~322 states, sub-second); the pin is distinguishing because the bundle universe is >= 2 with `RotateBundle`. | `uv run python scripts/check_formal_nomos.py --profile pins --out .scratch/formal/nomos-pins` |
| `full` | `Nomos.cfg` | 900s | Full-strength release/PACT evidence for `NomosControlPlaneContract`. The IAM claim family and publish-pin distinguishing proofs are decomposed into the `iam` and `pins` profiles so the full config keeps its original request universe and state space. | `uv run python scripts/check_formal_nomos.py --profile full --out .scratch/formal/nomos-control-plane --timeout-seconds 900` |

Use `uv run python scripts/check_formal_nomos.py --list-profiles --json` to
inspect the profile metadata. A timeout row is `timeout/not freshly proven`,
not a semantic invariant failure. Counterexample checks are separate
non-vacuity evidence and do not use smoke/full positive profiles.

## Verification Process

Run:

```sh
make formal-nomos-bootstrap
make formal-nomos
make formal-nomos-counterexample
make verify-nomos
```

The checker uses `.tools/tla2tools.jar` or `$TLA2TOOLS_JAR` and fails loudly
when Java/TLC is missing. Artifacts are written below
`.scratch/formal/nomos-control-plane`, `.scratch/formal/nomos-smoke`, or the
explicit `--out` directory. Counterexample verification runs every
per-witness config and succeeds only when TLC reports the expected broken
invariant for each witness. A missing invariant, different invariant, parser
error, setup failure, runtime exception, or any other unrelated TLC error is
rejected even if the expected invariant name appears in the output.

## Model Boundary

Implementation tests must connect model states to active policy/config rows,
request classifiers, external-claim validation, Postgres permissions, and
export visibility decisions. The model treats cryptography, identity-provider
assertions, SQL execution, and graph storage as trusted inputs whose validation
must be covered by implementation tests and runtime admission checks.

`PublishGeneration` in `Nomos.tla` is deliberately policy-only: it proves that a
generation can publish only under an active policy bundle and that the publish
pins that bundle (and, via `GenerationPinMatchesPublishBundle`, that the pin is
exactly the bundle active at publish time and never restamped by a later
rotation).

Boundary precondition -- the nomos-off publish path: the model's publish guard
only reflects deployments where Nomos runtime enforcement is enabled. In
production, `Publisher._nomos_generation_publish_blocker` returns `None` without
any policy evaluation when `deployment_requires_nomos_runtime` is false
(`src/okg/substrate/publisher/publisher.py`), so a nomos-off deployment
publishes outside this model's guard entirely (the activation ledger records an
explicit `nomos.runtime.opt_out.activation` allow decision instead). The
publish-eligibility decomposition table in `formal/design-contract.md` carries
this as a precondition of the Nomos row; claiming the model gates every publish
is a `mapping_bug` for nomos-off deployments.

It does not check refs/idempotency (owned by `formal/admission`),
narrowing or node-subtype scope (owned by `formal/consistency`), or session
pin/blocked-status discipline (owned by `formal/chronos`). End-to-end
publish-eligibility is a composition over those models, not a property of this
one; see the "Publish-eligibility decomposition" section in
`formal/design-contract.md`.
