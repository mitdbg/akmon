---
audience: developer, operator
type: explanation
canonical_id: nomos-control-plane
---
# Nomos Control Plane Architecture


Nomos records the policy decision for sensitive AKMON effects: graph
reads, source ingest, exports, Poros admission, policy activation, and
admin overrides. It combines deployment posture, source policy,
identity evidence, active policy/config state, and request context into
one auditable allow, deny, review, quarantine, redact, or declassify
decision.

The authority boundaries, decision shape, trace privacy, and fail-closed rules
below bind every Nomos caller.

## Control Plane Boundary

Nomos owns these surfaces:

- Vocabulary for deployment classes, principal kinds, resource kinds, actions,
  source sensitivity, data classifications, effects, obligations, trace fields,
  and reason codes.
- Policy bundle and config compilation, activation, and active-state lookup.
- Identity-provider metadata, verified claim envelopes, subject bindings, and
  scoped grants used by decisions.
- Deployment posture and source-policy audit terms used by readiness, doctor,
  publish, and export gates.
- Runtime decision evaluation and obligation return values.
- Privacy-safe decision traces used for audit, replay, and evidence.

Nomos does not replace hardened primitives that already protect concrete
resources. Runtime callers must compose Nomos with MCP write authorization,
egress/URL validation, file capability resolution, hardened parsers,
secret-reference enforcement, operator route authorization, and export/redaction
behavior. The export/redaction behavior is an existing hardening primitive, not
an optional downstream cleanup step. A Nomos integration that bypasses or
weakens those primitives is a bug.

## Authority Model

| Surface | Source of truth | Drift guard |
| --- | --- | --- |
| Domain vocabulary | `src/okg/substrate/library/ontologies/nomos/schema.yaml` (ontology), `src/okg/substrate/nomos/vocabulary.py` (canonical Python binding), and `formal/nomos/` (TLA+ constants) | Vocabulary drift tests compare ontology enum values across the Python and formal bindings; changing it requires one coordinated edit to all three. |
| Ontology/schema | `src/okg/substrate/library/ontologies/nomos/schema.yaml` and `module.yaml` | Ontology tests and catalog composition tests fail on missing or unknown terms. |
| Policy bundle definitions | Graph-native policy JSON/YAML admitted through Nomos policy APIs | Compiler validation, deterministic fingerprint tests, active-state tests, and formal replay pins. |
| Config set definitions | Graph-native config JSON/YAML admitted through Nomos config APIs | Secret literal rejection, deterministic fingerprint tests, and activation tests. |
| Active policy/config state | Postgres `okg.nomos_policy_activations` and config activation tables, mutated through the Activation APIs only | Store tests prove atomic supersession and missing or stale active state denies; direct writes outside the Activation APIs are not an authority path. |
| Identity providers and subject bindings | Nomos identity provider metadata plus verified claim envelopes from trusted identity systems | Identity tests reject expired, revoked, ambiguous, unscoped, low-assurance, or secret-bearing claims. |
| Deployment posture | Deployment/profile manifests using canonical `deployment_class` posture terms | Audit tests reject missing, malformed, unknown, or weaker-than-required posture. |
| Source policy | Source registry entries and producer/source metadata using canonical source-policy fields | Audit tests reject missing sensitivity, credential, retention, export, provenance, or live-call policy where required. |
| Decision traces | Nomos decision evaluator output after obligation enforcement | Trace privacy tests reject forbidden payload classes and require stable trace fields. |

Default values may exist only as explicit, tested compatibility migrations. A
missing policy, identity, deployment posture, source policy, or active state must
never silently broaden access.

## Deployment And Source Policy Matrix

Every kept deployment/profile must declare one of these deployment classes:

- `ci_authority`
- `dogfood_workspace`
- `personal_okg`
- `public_reference_demo`
- `synthetic_healthcare_demo`
- `governed_research_metadata`
- `org_operational_private`
- `private_product_engineering`

Every source whose properties can affect a decision must normalize to a
source-policy record with:

- `source_sensitivity`
- `data_classification`
- credential-reference policy
- live-call allowance
- retention/exportability policy
- provenance policy
- required privacy obligations

The deployment/source audit must fail readiness, doctor, publish, or export
claims when the source policy is stronger than the deployment posture can carry.
Examples include local private files in remote CI, PHI-shaped synthetic data in
a public demo without synthetic-healthcare posture, live external calls in an
offline profile, no-export data in an export path, or secret material without a
secret reference.

## Decision API

Runtime callers must reduce sensitive operations to one Nomos decision request.
The logical input schema is:

| Field | Required | Meaning |
| --- | --- | --- |
| `subject_binding_id` | Yes | Stable binding for the human, service, source adapter, agent, MCP client, or automation principal. |
| `subject_claim_refs` | When external identity is used | Redacted references to verified claim envelopes, not raw claims or credentials. |
| `principal_kind` | Yes | Canonical principal kind from the Nomos vocabulary. |
| `action` | Yes | Canonical action such as `graph.read.mcp`, `source.ingest`, `mcp.record`, `generation.publish`, or `policy.activate`. |
| `resource_kind` | Yes | Canonical resource kind such as `graph`, `source`, `generation`, `mcp_tool`, `policy_bundle`, or `secret`. |
| `resource_id` | Yes | Stable resource reference; local paths and private payloads must be redacted or hashed when persisted. |
| `data_classification` | Yes | Public, internal, private, restricted, confidential, secret, regulated metadata, or synthetic PHI. |
| `source_sensitivity` | When action touches a source or source-derived output | Normalized sensitivity such as public, local private, restricted, secret, credentialed external, or synthetic PHI. |
| `deployment_posture` | Yes | Active deployment/profile class and readiness profile. |
| `context` | Yes | Minimal structured context required for policy evaluation; no raw secrets, raw request payloads, or file contents. |
| `policy_generation_pin` | Yes | Active policy bundle id/version/fingerprint and source generation id. |
| `config_generation_pin` | When config affects the decision | Active config set id/version/fingerprint and source generation id. |
| `enforcement_point` | Yes | Stable caller boundary, for example `mcp.record`, `source.live_call`, or `graph.export`. |

The logical output schema is:

| Field | Required | Meaning |
| --- | --- | --- |
| `decision_id` | Yes | Stable id derived from canonical input, active policy/config pins, and evaluator version. |
| `effect` | Yes | One of `allow`, `deny`, `review`, `quarantine`, `redact`, or `declassify`. |
| `obligations` | Yes | Ordered obligation records the caller must enforce before effect. |
| `reason_code` | Yes | Stable code explaining the matched rule or fail-closed condition. |
| `matched_rule_ids` | Yes | Matched policy rules or default-deny marker. |
| `input_hash` | Yes | Hash of canonicalized, redacted decision input. |
| `trace_ref` | Yes | Pointer to the privacy-safe decision trace row. |
| `policy_bundle_id`, `policy_version`, `policy_fingerprint` | Yes | Active policy bundle pin used for evaluation. |
| `config_set_id`, `config_version`, `config_fingerprint` | When config affects the decision | Active config pin used for evaluation. |

Existing `DecisionInput` and `PolicyDecision` objects are compatibility
carriers for this API. Runtime enforcement work must extend or wrap them rather
than creating per-caller decision shapes.

## Fail-Closed Matrix

| Condition | Required result | Reason code class |
| --- | --- | --- |
| Missing policy bundle or inactive policy state | `deny` | `nomos.policy.missing` |
| Missing config when config is required | `deny` or `review` | `nomos.config.missing` |
| Stale policy/config generation pin | `deny` or `review` | `nomos.state.stale` |
| Missing identity or subject binding | `deny` | `nomos.identity.missing` |
| Expired, revoked, ambiguous, unscoped, or low-assurance identity claim | `deny` | `nomos.identity.invalid` |
| Unknown action, resource kind, principal kind, deployment class, source sensitivity, data classification, effect, or obligation | `deny` | `nomos.vocabulary.unknown` |
| Malformed context or non-canonical request | `deny` | `nomos.request.malformed` |
| Source/deployment posture mismatch | `deny`, `quarantine`, or `review` | `nomos.source.posture_mismatch` |
| Obligation returned but not enforced | `deny` or blocked effect | `nomos.obligation.unenforced` |
| Obligation enforced differently from decision trace | `deny` or blocked effect | `nomos.obligation.mismatch` |
| Caller attempts direct bypass of a Nomos boundary | `deny` and audit | `nomos.enforcement.bypass` |
| Break-glass/admin override without scoped authority | `review` or `deny` | `nomos.override.unscoped` |

No caller may convert one of these states to `allow` through a default posture,
empty policy, compatibility seed, missing field fallback, or exception handler.

## Obligation Semantics

Obligations are part of the decision contract. The caller must enforce them
before the requested effect, and the trace must record which obligations were
returned and which enforcement point handled them.

| Obligation | Caller responsibility |
| --- | --- |
| `audit` | Persist a privacy-safe trace and include the decision in relevant evidence/readiness views. |
| `redaction` | Remove or mask classified fields before read, export, or trace persistence. |
| `declassification` | Require a declassification rule and record the source, target classification, reviewer or policy authority, and generation pins. |
| `quarantine` | Withhold output from normal visibility and route it to the quarantine release path. |
| `review` | Block automatic effect until a scoped reviewer or policy authorizes continuation. |
| `no_export` | Prevent export-like reads and generated artifacts from leaving the allowed boundary. |
| `secret_ref_required` | Require a secret reference and reject literal secret or credential material. |
| `source_retention` | Apply retention, deletion, or no-persistence requirements from the source policy. |
| `break_glass_review` | Require scoped break-glass authority, additional trace detail, and follow-up review. |

Losing an obligation loses the decision. Runtime callers must fail closed when
obligation parsing, propagation, or enforcement fails.

## Trace Privacy

Decision traces must be useful evidence without becoming a new sensitive data
store.

Required trace fields:

- `decision_id`
- `policy_bundle_id`
- `policy_version`
- `policy_fingerprint`
- `config_set_id` when applicable
- `config_version` when applicable
- `config_fingerprint` when applicable
- `input_hash`
- `subject_binding_id`
- redacted `subject_ref`
- `principal_kind`
- `action`
- `resource_kind`
- redacted or hashed `resource_ref`
- `data_classification`
- `source_sensitivity` when applicable
- `deployment_posture`
- `effect`
- `obligations`
- `reason_code`
- `matched_rule_ids`
- `enforcement_point`
- `generation_pins`
- `created_at`

Forbidden trace payloads:

- raw secrets
- raw credential references that reveal secret backend location or material
- access tokens, refresh tokens, cookies, passwords, private keys, assertions, or
  tickets
- PHI-shaped fixture values
- private source payloads
- local file contents
- unredacted request parameters
- source-controlled sensitive metadata
- full external identity claims

Trace export and retention must follow the active deployment posture and source
policy. Public/demo traces may expose only redacted ids, reason codes, effects,
obligations, and generation pins. Private/operator traces may carry richer
redacted context, but never forbidden payloads.

## Runtime Enforcement Points

Runtime enforcement must make these boundaries mandatory Nomos callers:

- `graph.read.public_demo`
- `graph.read.local_operator`
- `graph.read.mcp`
- `graph.read.cli_search`
- `graph.read.sql_admin`
- `graph.read.export_like`
- `graph.export`
- `mcp.tool.call`
- `mcp.record`
- `source.ingest`
- `source.emit`
- `source.live_call`
- `secret.use`
- `generation.publish`
- `policy.activate`
- `config.activate`
- `declassify`
- `redact`
- `quarantine.release`
- `admin.override`

Each boundary needs tests for allow, deny, redact, declassify, quarantine,
review, missing policy, missing identity, stale state, unknown action, malformed
context, source/deployment mismatch, obligation mismatch, trace redaction, and
bypass attempts.

## Formal Model Contract

The formal model must cover the same terms and safety requirements as this
document:

- Deployment classes, source sensitivities, data classifications, actions,
  effects, obligations, policy/config activation, identity bindings, decision
  traces, and generation pins.
- Default deny, no unauthorized broadening, no private/public leak, no agent
  self-expansion, active policy/config pinning, and trace completeness.
- Non-vacuity counterexamples for missing policy, weak deployment posture,
  unscoped identity claim, unknown action, private export without a scoped
  claim, and source/deployment sensitivity mismatch.

The formal binding must either generate constants from the ontology
vocabulary or prove through tests that they have not drifted from the canonical
vocabulary.
