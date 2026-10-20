---
audience: developer
type: reference
canonical_id: consistency-claim-model-api
---
# Claim consistency extension API

A deployment developer needs a stable way to feed accepted claims and ordered
corrections into consistency checks without hiding disagreement inside an
extractor. The result must retain enough provenance for deployment policy and
publication checks to act on it.

## Inputs, outputs, and authority

The library receives derived `ClaimView` values, ordered `Correction` values,
deployment-supplied conflict rules, and precedence signals. It returns
deterministic effective claims, conflict findings, and canonical-or-contested
resolution records. A deployment may provide the data and pure callbacks; the
core owns the merge, identity, conflict classification, and resolution shape.

| Boundary | Contract |
|---|---|
| May decide | How supplied corrections merge; which supplied claims belong to a conflict bucket; which declared rule applies; and whether declared decisive signals produce a canonical or contested outcome. |
| May not decide | Whether a source claim is factually true, whether a deployment should publish, or whether a missing dependency can be treated as empty. The [consistency model](../architecture/consistency.md) owns those semantics and the publisher owns visibility. |
| Safe fallback | Missing or uncertain evidence is surfaced as an explicit error, uncertainty finding, or contested result. Callers must run the complete consistency path when reuse evidence is absent; this API does not invent a default claim or winner. |
| Failure and limits | Malformed corrections, typed semantics, and rule callbacks fail loudly; inputs are not mutated. The functions are pure and deterministic, but they do not perform storage, network, model calls, source canonicalization, or publication. |

[Incremental maintenance and claim
consistency](../architecture/consistency.md) defines safe reuse versus complete
computation, preserved disagreement, precedence, typed applicability, and the
current fixture-scoped runtime limit. The functions below implement the pure
claim-model call surface; they do not imply that every deployment runs the
optional reuse evaluator.

Every function here is **pure**: no database, no network, no LLM, no wall clock;
the same inputs always produce the same output, sorted deterministically. Inputs
are never mutated.

**Notation.** A claim (x, p, v, s) is a `ClaimView`
(x = `normalized_key`, p = `predicate`, v = `value`, s = `source_id`; the
logical id deliberately excludes v), Σ is the `ConflictRules` slice of the
deployment's constraint set, and ≻ is `resolve_bucket`'s signal order.

## Core-supplied vs deployment-supplied

The library supplies the *model* (the objects, the merge, the resolver, the
generic check engine). A deployment supplies its *data* (predicate schemas, the
incompatibility table, the scope vocabulary, subject canonicalization) — the
residence split of [Consistency model §8](../architecture/consistency.md). The
API below is the interface between the two columns.

| Core-supplied (this library) | Deployment-supplied (passed in as data) |
|---|---|
| `merge_effective_claims`, `Correction`, `MergeResult` — the C-updater | The derived `ClaimView` set (from the deployment's extractor) and the operator `Correction` decisions |
| `derive_conflict_findings` — the generic Σ engine | `ConflictRules`: `conflict_predicates`, `incompatible`, `constraint_for`, `canonical_key`, `unresolved_keys`, `CrossPredicatePolicy` rows |
| `resolve_bucket`, `Resolution` — the ≻ resolver | `ClaimSignals` per claim and operator `Precedent` decisions |
| `claim_logical_id`, `claim_key_slug` — identity discipline | The `(source_id, predicate, key, occurrence, anchor)` coordinates |
| The outcome / action / signal / provenance constants | — |

**Subject canonicalization is deployment data.** `ConflictRules.canonical_key`
is a per-claim pure function `(predicate, key) -> bucket_key`. A deployment that
clusters subject phrasings across its corpus precomputes that map and closes
over it in `canonical_key`. The library consumes the precomputed map; it does
not perform corpus-relative clustering itself.

## API-freeze and versioning contract

This surface is what deployment repos adopt against; it is **frozen**. The
freeze rules:

- **Additive-only within a version.** New optional parameters (with defaults),
  new dataclass fields (with defaults), and new module-level constants may be
  added without a version bump. Every public dataclass here already carries a
  `schema_version` string constant for this reason.
- **A breaking change requires a version bump.** Renaming or removing a public
  name, changing a function's required parameters or return shape, or altering
  the *meaning* of an existing outcome is a breaking change. It must bump the
  affected module's `schema_version` (e.g. `okg.claim_model.effective.v1`) and come
  with a matching edit to
  [Consistency model](../architecture/consistency.md) — a semantic change
  to `claim_model/` without a spec edit is a review defect (that page's
  versioning contract).
- **The adoption proof is a test.**
  `tests/substrate/consistency/test_claim_model_adoption_contract.py` exercises
  deployment call patterns against this API. A breaking change that this test
  catches requires updating deployment integrations together with the frozen
  contract.

## C — effective claims (`effective.py`)

The override-merge `C_eff = derived ⊕ operator source-corrections`.

### `merge_effective_claims(derived, corrections) -> MergeResult`

- **Inputs.** `derived`: an iterable of `ClaimView` (the extractor's claims).
  `corrections`: an iterable of `Correction` (operator source-corrections).
- **Output.** A `MergeResult`. Corrections apply in decision order
  (`(sequence, claim_id, action)`), last-writer-wins per `claim_id`.
- **Determinism.** Output `effective` is sorted by `node_id`; the result is
  invariant under input shuffling; derived claims are never mutated (overrides
  produce new values via `dataclasses.replace`).
- **Failure modes, kept distinct.** A correction whose target `claim_id` is
  absent is **expired** (recorded in `MergeResult.expired`, never applied, not an
  error). A **malformed** correction (unknown action, an override that changes
  nothing, an assert missing coordinates, a half-specified scope) raises
  `EffectiveClaimError` — never silently dropped.

### `Correction` (frozen dataclass)

One operator source-correction, targeting a claim by deterministic `claim_id`.
Fields: `claim_id`, `action` (one of the `ACTION_*` constants), `sequence`
(decision order), `decision_id` (optional provenance back-pointer), and the
override/assert payload `value`, `normalized_key`, `scope`, `scope_state`,
`source_id`, `predicate`, and `semantics`. For an override, non-`None` payload fields replace the
target's; `scope` and `scope_state` must be set together. For an assert, the full
coordinates `source_id` / `predicate` / `normalized_key` / `value` are required.

### `MergeResult` (frozen dataclass)

Fields: `effective` (the `tuple[ClaimView, ...]` result), `provenance`
(`claim_id -> PROV_*`), and the bookkeeping tuples `retracted`, `corrected`,
`asserted`, `expired`. Carries `schema_version`.

### Constants

- Actions: `ACTION_RETRACT`, `ACTION_OVERRIDE`, `ACTION_ASSERT`.
- Provenance: `PROV_DERIVED`, `PROV_CORRECTED`, `PROV_ASSERTED`.
- `EffectiveClaimError` — the loud-failure exception type.

## The shared claim shape (`views.py`)

### `ClaimView` (frozen dataclass)

The minimal view of one effective claim that checks and resolvers consume,
decoupled from any extractor: `node_id` (the deterministic logical id),
`source_id` (the document/record the claim came from — used only for
internal-vs-cross-document locality), `predicate`, `normalized_key`, `value`,
`scope` (a heading-path string), `scope_state` (`SCOPE_KNOWN` / `SCOPE_UNKNOWN`),
`element_ref` (`ElementRef | None`, default `None`), and `semantics`
(`ClaimSemantics | None`, default `None`).

`ClaimSemantics` carries `logical_subject`, `activity`, `value_type`,
`target_role`, `polarity`, `modality`, `conditions`, `applicability`, and
`event_order`. Its canonical attr form is `to_attr()`; malformed payloads fail
loudly through `claim_semantics_from_attr`. The logical subject and activity do
not derive from `element_ref` or `normalized_key`.

`element_ref` is the **optional typed handle** to the graph element the claim
describes. It is **additive — default `None`, no
`schema_version` bump** (per the freeze rules above; the `views` module stays
`v1`) — so `None` (a corpus-level claim with no graph referent) preserves the
prior shape exactly. Claim identity (`claim_logical_id`) excludes the handle.

`source_id` accepts the identifier of any source document or record.

### `ElementRef` (frozen dataclass)

The optional element handle a `ClaimView` may carry. `kind` is `"node"`
(`ELEMENT_KIND_NODE`, with a `node_id`) or `"edge"` (`ELEMENT_KIND_EDGE`, with
`src` / `edge_type` / `dst`); `bound` records whether the reference resolved to
a live element. **Node existence is the bound/unbound state, not a predicate**:
an unresolvable reference is recorded `bound=False` carrying the coordinates it
failed to resolve — never guessed (resolution is a later task). Construction
validates loudly (`ValueError` naming the field) on a bad `kind`, a node handle
without `node_id`, or an edge handle missing any of `src` / `edge_type` / `dst`.
`to_json_dict` / `from_json_dict` are an exact round-trip pair for persistence
in a node's attrs; `element_ref_from_attr` parses an attr payload
(object, JSON text, or `None`) to an `ElementRef | None`.

### `applicability_relation(a, b) -> str`

The single typed relation over `Applicability`: it combines `ScopeBounds`,
heading hierarchy, `TimeBounds`, logical subject, activity, and modality.
Returns `REL_OVERLAP`, `REL_DISJOINT`, or `REL_UNKNOWN`. `ScopeBounds` retains
separate lower and upper sets plus explicit `OMEGA`; `TimeBounds` uses half-open
ISO-date intervals.

### `scope_relation(a, b) -> str` (v1 adapter)

Adapts legacy heading-path fields into `Applicability.from_v1` and delegates to
`applicability_relation`; it is not an independent classifier. An unknown scope
is surfaced as `applicability_uncertain`, never as an actionable conflict.
Constants:
`SCOPE_KNOWN`, `SCOPE_UNKNOWN`, `REL_OVERLAP`, `REL_UNKNOWN`, `REL_DISJOINT`.

## Σ — the generic conflict engine (`conflicts.py`)

### `derive_conflict_findings(claims, rules, *, schema_version=...) -> list[ConflictFinding]`

- **Inputs.** `claims`: a flat iterable of effective `ClaimView`. `rules`: the
  deployment's `ConflictRules` (its Σ slice).
- **Output.** A sorted `list[ConflictFinding]`. The engine buckets claims by
  `(predicate, canonical key)`, emits a not-comparable finding for a multi-member
  bucket on an unresolved key, pairwise-checks the rest via `rules.incompatible`
  gated by `applicability_relation`, then applies each `CrossPredicatePolicy`.
- **Determinism.** Findings are sorted by `node_id`; a finding's id is a hash over
  its sorted member ids — order-independent.

### `ConflictRules` (frozen dataclass) — the deployment Σ

Fields: `conflict_predicates` (`frozenset[str]`), `incompatible`
(`(predicate, value_a, value_b) -> bool`), `constraint_for`
(`predicate -> constraint name`), `canonical_key` (optional
`(predicate, key) -> bucket_key`; the subject-canonicalization hook),
`unresolved_keys` (`frozenset[str]` whose members can never be bucketed — a
multi-member bucket on one is flagged not-comparable), and `cross_policies`
(a tuple of `CrossPredicatePolicy`). `typed_incompatible` is the optional
deployment callback for typed polarity/value-role/condition/event-order
semantics. A non-default callback-owned field without it fails loudly.
The maintained publish path registers that same callback with
`claim_projection_derivation_row(typed_incompatible=...)` (or
`register_claim_projection_derivation(..., typed_incompatible=...)`); the
transform residual, parity oracle, and repair-selection rebuild all use it.

### `CrossPredicatePolicy` (frozen dataclass)

One cross-predicate rule (e.g. "never recommend a deprecated target"): `constraint`,
`source_predicate`, `target_predicate`, `link_key` (`source claim -> target
normalized_key`), `violates` (`(source, target) -> bool`), `detail`
(`(source, target) -> str`). The engine owns the finding's `logical_key`, deriving it
uniformly as `{constraint}:{link}`; a deployment customizes the free-text `detail`, not
the label (a bespoke label prefix is normalized on adoption — see the adoption test's
cross-policy witness).

### `ConflictFinding` (frozen dataclass)

A proposal-only finding: `node_id`, `constraint`, `logical_key`,
`member_claim_ids`, `locality`, `detail`, and the additive typed contract:
`typed_category` (`conflict`, `alignment_uncertain`,
`applicability_uncertain`, `not_comparable`, or `no_conflict`),
`actionable_conflict`, and established/unknown dimension tuples for semantic
alignment (`subject`, `activity`, `value`) and applicability (`heading`,
`time`, `scope/conditions`). Consumers select actionable findings with
`typed_category == OUTCOME_CONFLICT` or `actionable_conflict`, never by parsing
`detail`.

The legacy `outcome` field remains an explicit adapter surface:
`conflict -> definite`, each uncertainty category -> `possible`,
`not_comparable -> not_comparable`, and `no_conflict -> possible` for callers
that require the old tri-state vocabulary. Persisted legacy values are not
silently rewritten; uncertainty rows carry `legacy_marker=legacy_possible`.
`CONSTRAINT_AMBIGUOUS` names the unresolved-key finding.

## Typed applicability (`applicability.py`)

`Applicability`, `ScopeBounds`, `TimeBounds`, `OMEGA`, and
`applicability_relation` form the active mechanism. The projection path uses
this typed model; a separate heading-only classifier is not public API.

## ≻ — precedence and resolution (`precedence.py`)

### `resolve_bucket(signals, precedents=()) -> Resolution`

- **Inputs.** `signals`: an iterable of `ClaimSignals` for the claims in one
  conflict bucket. `precedents`: an iterable of operator `Precedent`.
- **Output.** A `Resolution`. Claims are ranked ≻-descending under the fixed
  lexicographic signal order (operator precedent first, then supersession,
  effective date, source authority, independent support, document recency,
  maintenance). The top claim is `OUTCOME_CANONICAL` only when the first signal
  separating it from the runner-up is **decisive**; a full tie, or a lead only on
  the ranking-only signals (recency / maintenance), is `OUTCOME_CONTESTED`.
- **Determinism.** Order-independent; ties fall back to `claim_id`.

### `ClaimSignals` (frozen dataclass)

The ≻-relevant signals on one claim: `claim_id`, `source_class` (for precedent
matching), `scope` (for precedent scope-match), `is_superseded`, `effective_date`
(ISO string), `source_authority` (int), `independent_support` (int),
`document_recency` (ISO string, ranking-only), `maintenance_rank` (int,
ranking-only).

### `Precedent` (frozen dataclass)

An operator governance precedent: within `scope` (a heading path; `""` is global),
prefer `prefer_source`. Competing applicable precedents resolve last-writer-wins
by `sequence`. A precedent applies when its scope is global, equals, or subsumes
the claim's scope in either direction.

### `Resolution` (frozen dataclass) and constants

Fields: `outcome` (`OUTCOME_CANONICAL` / `OUTCOME_CONTESTED`),
`canonical_claim_id` (the winner, or `""` when contested), `ranked_claim_ids`
(all claims ≻-descending), `basis` (the deciding signal class, `"tie"`,
`"sole_claim"`, or `"empty"`), `schema_version`. The seven signal-class constants
`SIGNAL_PRECEDENT`, `SIGNAL_SUPERSESSION`, `SIGNAL_EFFECTIVE_DATE`,
`SIGNAL_SOURCE_AUTHORITY`, `SIGNAL_INDEPENDENT_SUPPORT`, `SIGNAL_DOCUMENT_RECENCY`,
`SIGNAL_MAINTENANCE` name each tier.

## Identity (`identity.py`)

### `claim_logical_id(source_id, predicate, key, occurrence, *, anchor="") -> str`

The deterministic logical id `claim:{predicate}:{slug}:{source}[:{anchor}]:{occurrence}`.
It **excludes the value and evidence span**: a re-extraction of the same
assertion yields the same id; a value change re-triggers review under a stable
identity. `occurrence` keeps two same-key claims in one document distinct; the
optional `anchor` (a structural anchor such as a heading-path hash) further
discriminates per-region extraction.

### `claim_key_slug(key) -> str`

Normalizes a logical key for the id: lowercase, non-alphanumerics to `-`, empty
maps to `_`.

## Adopting the API

A deployment adopts by supplying its data columns and calling the core:

1. Build `ClaimView` records from your extractor (any `source_id`).
2. Express your Σ as a `ConflictRules` (predicates, `incompatible`,
   `constraint_for`, optional `canonical_key`, `unresolved_keys`,
   `cross_policies`).
3. Merge operator decisions with `merge_effective_claims`, check with
   `derive_conflict_findings`, resolve buckets with `resolve_bucket`.

## Related pages

- [Consistency model](../architecture/consistency.md) — the normative
  semantics, exclusions, and witness index this API implements.
- [Ingest and publish pipeline](../architecture/pipeline.md) — where the
  consistency gate sits in a publish cycle.
- [Glossary](../../reference/glossary.md) — generation, narrowing, publish cycle.
