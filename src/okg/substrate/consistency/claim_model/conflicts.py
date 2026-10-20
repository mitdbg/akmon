"""Generic deterministic conflict detection over effective claims (Σ-parametric).

The "check" stage of the claim loop: bucket claims by ``(predicate, key)``,
flag incompatible values whose scopes overlap, and classify each finding with
the typed outcome contract (``conflict``, ``alignment_uncertain``,
``applicability_uncertain``, ``not_comparable``, or ``no_conflict``). A bucket whose claims come from ONE
document is an INTERNAL contradiction; one spanning several is a CROSS-DOC
conflict — the same check, distinguished only by document count.

The engine is Σ-PARAMETRIC: the deployment supplies its constraint table as a
:class:`ConflictRules` value (which predicates conflict, what incompatible
means per predicate, key canonicalization, cross-predicate policies) — the
mechanics live here, the semantics stay deployment data (v1-currency-spec §10:
deployments own predicate schemas and incompatibility tables). Hoisted and
generalized from ``cern_twiki.currency_check`` (PACT
``consistency-claim-model-core``).

Pure: DB-free, no LLM, deterministic ids, sorted output. Unknown semantic
alignment is never actionable; unknown applicability is a separate
non-actionable category. The legacy definite/possible values are available
only through the explicit adapter; an unresolved key yields NOT-COMPARABLE
rather than a forced verdict.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Iterable, Mapping

from okg.substrate.consistency.claim_model.applicability import (
    REL_DISJOINT,
    REL_OVERLAP,
    REL_UNKNOWN,
    applicability_relation,
)
from okg.substrate.consistency.claim_model.views import (
    ClaimView,
)

CONFLICTS_SCHEMA_VERSION = "okg.claim_model.conflicts.v1"

# Typed outcome vocabulary (issue #746).  The v1 constants below intentionally
# remain byte-compatible with existing callers; ``typed_category`` is the
# additive, machine-selectable contract and ``legacy_outcome`` is the only
# adapter back to those values.
OUTCOME_CONFLICT = "conflict"
OUTCOME_ALIGNMENT_UNCERTAIN = "alignment_uncertain"
OUTCOME_APPLICABILITY_UNCERTAIN = "applicability_uncertain"
OUTCOME_NO_CONFLICT = "no_conflict"
ALIGNMENT_DIMENSIONS = ("subject", "activity", "value")
APPLICABILITY_DIMENSIONS = ("heading", "time", "scope/conditions")
LEGACY_POSSIBLE_MARKER = "legacy_possible"

OUTCOME_DEFINITE = "definite"
OUTCOME_POSSIBLE = "possible"
OUTCOME_NOT_COMPARABLE = "not_comparable"

OUTCOME_CATEGORIES = frozenset({
    OUTCOME_CONFLICT,
    OUTCOME_ALIGNMENT_UNCERTAIN,
    OUTCOME_APPLICABILITY_UNCERTAIN,
    OUTCOME_NOT_COMPARABLE,
    OUTCOME_NO_CONFLICT,
})

LOCALITY_INTERNAL = "internal"
LOCALITY_CROSS_DOC = "cross_doc"

CONSTRAINT_AMBIGUOUS = "ambiguous_key_not_comparable"


@dataclass(frozen=True)
class CrossPredicatePolicy:
    """One cross-predicate policy rule (e.g. "never recommend a deprecated target").

    For every claim of ``source_predicate``, ``link_key(claim)`` names the
    ``normalized_key`` of ``target_predicate`` claims it is checked against;
    ``violates(source, target)`` decides the violation; scope gating and
    outcome classification are the engine's.
    """

    constraint: str
    source_predicate: str
    target_predicate: str
    link_key: Callable[[ClaimView], str]
    violates: Callable[[ClaimView, ClaimView], bool]
    detail: Callable[[ClaimView, ClaimView], str]


@dataclass(frozen=True)
class ConflictRules:
    """The deployment-supplied Σ slice the generic engine consumes.

    ``incompatible(predicate, value_a, value_b)`` decides pairwise conflict for
    the predicates in ``conflict_predicates``; ``constraint_for(predicate)``
    names the violated constraint; ``canonical_key`` (optional) maps a claim's
    ``normalized_key`` to its bucket key (subject canonicalization);
    ``unresolved_keys`` are keys that cannot be bucketed (claims sharing one
    are flagged NOT-COMPARABLE, never force-verdicted).
    """

    conflict_predicates: frozenset[str]
    incompatible: Callable[[str, str, str], bool]
    constraint_for: Callable[[str], str]
    canonical_key: Callable[[str, str], str] | None = None
    unresolved_keys: frozenset[str] = frozenset({""})
    cross_policies: tuple[CrossPredicatePolicy, ...] = ()
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None


@dataclass(frozen=True)
class ConflictFinding:
    """A proposal-only deterministic conflict finding."""

    node_id: str
    constraint: str
    logical_key: str
    member_claim_ids: tuple[str, ...]
    outcome: str
    locality: str
    detail: str
    schema_version: str
    candidate_status: str = "proposal"
    canonical_admitted: bool = False
    typed_category: str = ""
    actionable_conflict: bool = False
    alignment_established: tuple[str, ...] = ()
    alignment_unknown: tuple[str, ...] = ()
    applicability_established: tuple[str, ...] = ()
    applicability_unknown: tuple[str, ...] = ()
    legacy_marker: str = ""

    def __post_init__(self) -> None:
        category = self.typed_category or _legacy_to_typed(self.outcome)
        if category not in {
            OUTCOME_CONFLICT, OUTCOME_ALIGNMENT_UNCERTAIN,
            OUTCOME_APPLICABILITY_UNCERTAIN, OUTCOME_NOT_COMPARABLE,
            OUTCOME_NO_CONFLICT,
        }:
            raise ValueError(f"unknown typed outcome category {category!r}")
        if self.outcome != legacy_outcome(category):
            raise ValueError(
                f"legacy outcome {self.outcome!r} disagrees with typed category "
                f"{category!r}; use legacy_outcome() explicitly"
            )
        object.__setattr__(self, "typed_category", category)
        object.__setattr__(self, "actionable_conflict", category == OUTCOME_CONFLICT)
        if category in {OUTCOME_ALIGNMENT_UNCERTAIN, OUTCOME_APPLICABILITY_UNCERTAIN}:
            object.__setattr__(self, "legacy_marker", self.legacy_marker or LEGACY_POSSIBLE_MARKER)


@dataclass(frozen=True)
class TypedOutcome:
    """A typed classification plus the evidence dimensions that led to it."""

    category: str
    alignment_established: tuple[str, ...] = ()
    alignment_unknown: tuple[str, ...] = ()
    applicability_established: tuple[str, ...] = ()
    applicability_unknown: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.category not in {
            OUTCOME_CONFLICT, OUTCOME_ALIGNMENT_UNCERTAIN,
            OUTCOME_APPLICABILITY_UNCERTAIN, OUTCOME_NOT_COMPARABLE,
            OUTCOME_NO_CONFLICT,
        }:
            raise ValueError(f"unknown typed outcome category {self.category!r}")
        for name, values, allowed in (
            ("alignment_established", self.alignment_established, ALIGNMENT_DIMENSIONS),
            ("alignment_unknown", self.alignment_unknown, ALIGNMENT_DIMENSIONS),
            ("applicability_established", self.applicability_established, APPLICABILITY_DIMENSIONS),
            ("applicability_unknown", self.applicability_unknown, APPLICABILITY_DIMENSIONS),
        ):
            if not isinstance(values, tuple) or not all(v in allowed for v in values):
                raise ValueError(f"{name} contains an unknown dimension")
        if set(self.alignment_established) & set(self.alignment_unknown):
            raise ValueError("alignment dimension cannot be both established and unknown")
        if set(self.applicability_established) & set(self.applicability_unknown):
            raise ValueError("applicability dimension cannot be both established and unknown")

    @property
    def actionable_conflict(self) -> bool:
        return self.category == OUTCOME_CONFLICT

    @property
    def legacy_outcome(self) -> str:
        return legacy_outcome(self.category)


def legacy_outcome(category: str) -> str:
    """Explicit v1 adapter; uncertainty is marked, never rewritten as conflict."""
    if category not in OUTCOME_CATEGORIES:
        raise ValueError(f"unknown typed outcome category {category!r}")
    if category == OUTCOME_CONFLICT:
        return OUTCOME_DEFINITE
    if category == OUTCOME_NOT_COMPARABLE:
        return OUTCOME_NOT_COMPARABLE
    if category in {OUTCOME_ALIGNMENT_UNCERTAIN, OUTCOME_APPLICABILITY_UNCERTAIN}:
        return OUTCOME_POSSIBLE
    return OUTCOME_POSSIBLE


def _legacy_to_typed(outcome: str) -> str:
    if outcome == OUTCOME_DEFINITE:
        return OUTCOME_CONFLICT
    if outcome == OUTCOME_NOT_COMPARABLE:
        return OUTCOME_NOT_COMPARABLE
    if outcome == OUTCOME_POSSIBLE:
        return OUTCOME_APPLICABILITY_UNCERTAIN
    raise ValueError(f"unknown legacy outcome {outcome!r}")


def _locality(members: Iterable[ClaimView]) -> str:
    sources = {m.source_id for m in members}
    return LOCALITY_INTERNAL if len(sources) <= 1 else LOCALITY_CROSS_DOC


def _outcome_for(rel: str) -> str:
    return OUTCOME_DEFINITE if rel == REL_OVERLAP else OUTCOME_POSSIBLE


def _dimension_evidence(
    a: ClaimView, b: ClaimView,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Return established/unknown semantic and applicability dimensions.

    ``unknown`` is explicit only; an absent legacy axis means unconstrained and
    is therefore established.  The sentinel ``unknown`` is accepted for
    deployment payloads that cannot recover a semantic coordinate.
    """
    alignment_unknown: set[str] = set()
    for dim, values in (
        ("subject", (None if a.semantics is None else a.semantics.logical_subject,
                      None if b.semantics is None else b.semantics.logical_subject)),
        ("activity", (None if a.semantics is None else a.semantics.activity,
                       None if b.semantics is None else b.semantics.activity)),
        # Different values are the proposition being checked, not unknown
        # value alignment. Only an explicit unknown sentinel is uncertain.
        ("value", (a.value, b.value)),
    ):
        if any(value in {"", "unknown", "__unknown__"} for value in values if value is not None):
            alignment_unknown.add(dim)
    alignment_established = tuple(dim for dim in ALIGNMENT_DIMENSIONS if dim not in alignment_unknown)

    applicability_unknown: set[str] = set()
    for view in (a, b):
        app = view.resolved_applicability()
        if app.heading_state == "unknown":
            applicability_unknown.add("heading")
        if app.valid_time.state == "unknown":
            applicability_unknown.add("time")
        if app.scope_bounds is not None:
            if not app.scope_bounds.dimensions():
                applicability_unknown.add("scope/conditions")
            for dim in app.scope_bounds.dimensions():
                lo, hi = app.scope_bounds.lo_bound(dim), app.scope_bounds.hi_bound(dim)
                if lo != hi:
                    applicability_unknown.add("scope/conditions")
        if view.semantics is not None and any(
            condition in {"", "unknown", "__unknown__"}
            for condition in view.semantics.conditions
        ):
            applicability_unknown.add("scope/conditions")
    applicability_established = tuple(
        dim for dim in APPLICABILITY_DIMENSIONS if dim not in applicability_unknown
    )
    return (
        alignment_established,
        tuple(dim for dim in ALIGNMENT_DIMENSIONS if dim in alignment_unknown),
        applicability_established,
        tuple(dim for dim in APPLICABILITY_DIMENSIONS if dim in applicability_unknown),
    )


def classify_outcome(
    a: ClaimView,
    b: ClaimView,
    *,
    incompatible: bool,
    applicability: str,
    unresolved_key: bool = False,
) -> TypedOutcome:
    """Classify a pair using the issue-746 precedence rules."""
    ae, au, pe, pu = _dimension_evidence(a, b)
    if unresolved_key:
        # The key is the subject-alignment witness; retain that uncertainty in
        # the structured evidence even though the category is its own axis.
        return TypedOutcome(
            OUTCOME_NOT_COMPARABLE,
            tuple(dim for dim in ae if dim != "subject"),
            tuple(dict.fromkeys((*au, "subject"))),
            pe,
            pu,
        )
    if au:
        return TypedOutcome(OUTCOME_ALIGNMENT_UNCERTAIN, ae, au, pe, pu)
    if not incompatible or applicability == REL_DISJOINT:
        return TypedOutcome(OUTCOME_NO_CONFLICT, ae, au, pe, pu)
    if applicability == REL_UNKNOWN and not pu:
        pu = ("scope/conditions",)
        pe = tuple(dim for dim in APPLICABILITY_DIMENSIONS if dim != "scope/conditions")
    if pu or applicability != REL_OVERLAP:
        return TypedOutcome(OUTCOME_APPLICABILITY_UNCERTAIN, ae, au, pe, pu)
    return TypedOutcome(OUTCOME_CONFLICT, ae, au, pe, pu)


def claims_incompatible(
    rules: ConflictRules,
    predicate: str,
    a: ClaimView,
    b: ClaimView,
    *,
    legacy_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> bool:
    """Apply the deployment's one typed-or-v1 incompatibility decision.

    Maintained projections call this same helper after their relational stage
    emits candidate pairs, so callback-owned semantics cannot drift between
    the pure library and publish-time evaluation.
    """
    typed = a.semantics is not None or b.semantics is not None
    requires_typed = any(
        claim.semantics is not None and claim.semantics.requires_typed_rules()
        for claim in (a, b)
    )
    if rules.typed_incompatible is not None and typed:
        return rules.typed_incompatible(a, b)
    if requires_typed:
        raise ValueError(
            "typed claim semantics require ConflictRules.typed_incompatible: "
            f"claims {a.node_id!r}, {b.node_id!r}"
        )
    if legacy_incompatible is not None:
        return legacy_incompatible(a, b)
    return rules.incompatible(predicate, a.value, b.value)


def _finding(
    constraint: str,
    logical_key: str,
    members: tuple[ClaimView, ...],
    outcome: str,
    detail: str,
    schema_version: str,
    typed: TypedOutcome | None = None,
) -> ConflictFinding:
    member_ids = tuple(sorted(m.node_id for m in members))
    digest = hashlib.sha256("|".join(member_ids).encode("utf-8")).hexdigest()[:12]
    typed = typed or TypedOutcome(_legacy_to_typed(outcome))
    return ConflictFinding(
        node_id=f"conflict:{constraint}:{digest}",
        constraint=constraint,
        logical_key=logical_key,
        member_claim_ids=member_ids,
        outcome=outcome,
        locality=_locality(members),
        detail=detail,
        schema_version=schema_version,
        typed_category=typed.category,
        actionable_conflict=typed.actionable_conflict,
        alignment_established=typed.alignment_established,
        alignment_unknown=typed.alignment_unknown,
        applicability_established=typed.applicability_established,
        applicability_unknown=typed.applicability_unknown,
        legacy_marker=(LEGACY_POSSIBLE_MARKER if typed.category in {
            OUTCOME_ALIGNMENT_UNCERTAIN, OUTCOME_APPLICABILITY_UNCERTAIN
        } else ""),
    )


def derive_conflict_findings(
    claims: Iterable[ClaimView],
    rules: ConflictRules,
    *,
    schema_version: str = CONFLICTS_SCHEMA_VERSION,
) -> list[ConflictFinding]:
    """Derive conflict findings from a flat list of effective claims. Pure.

    Buckets ``rules.conflict_predicates`` claims by ``(predicate, bucket_key)``
    (via ``rules.canonical_key`` when supplied), emits a NOT-COMPARABLE finding
    for multi-member buckets on an unresolved key, pairwise-checks the rest via
    the shared typed-or-v1 incompatibility decision gated by
    :func:`applicability_relation`, then applies each
    :class:`CrossPredicatePolicy` through the same typed decision.
    """
    claims = list(claims)
    findings: list[ConflictFinding] = []

    def bucket_key(c: ClaimView) -> str:
        if rules.canonical_key is None:
            return c.normalized_key
        return rules.canonical_key(c.predicate, c.normalized_key)

    buckets: dict[tuple[str, str], list[ClaimView]] = defaultdict(list)
    for c in claims:
        if c.predicate not in rules.conflict_predicates:
            continue
        buckets[(c.predicate, bucket_key(c))].append(c)

    for (predicate, key), members in sorted(buckets.items()):
        logical_key = f"{predicate}:{key}"
        if key in rules.unresolved_keys and len(members) >= 2:
            # Can't reliably bucket an unresolved key -> flag, never force a verdict.
            evidence = [_dimension_evidence(members[0], member) for member in members[1:]]
            ae = tuple(dim for dim in ALIGNMENT_DIMENSIONS
                       if all(dim in item[0] for item in evidence))
            au = tuple(dim for dim in ALIGNMENT_DIMENSIONS if dim not in ae)
            pe = tuple(dim for dim in APPLICABILITY_DIMENSIONS
                       if all(dim in item[2] for item in evidence))
            pu = tuple(dim for dim in APPLICABILITY_DIMENSIONS if dim not in pe)
            findings.append(_finding(
                CONSTRAINT_AMBIGUOUS, logical_key, tuple(members),
                OUTCOME_NOT_COMPARABLE,
                "claims share an unresolved (unknown) key; alignment unconfirmed",
                schema_version,
                typed=TypedOutcome(
                    OUTCOME_NOT_COMPARABLE,
                    tuple(dim for dim in ae if dim != "subject"),
                    tuple(dict.fromkeys((*au, "subject"))),
                    pe,
                    pu,
                ),
            ))
            continue
        if key in rules.unresolved_keys:
            continue
        constraint = rules.constraint_for(predicate)
        ordered = sorted(members, key=lambda m: m.node_id)
        for i in range(len(ordered)):
            for j in range(i + 1, len(ordered)):
                a, b = ordered[i], ordered[j]
                if not claims_incompatible(rules, predicate, a, b):
                    continue
                rel = applicability_relation(
                    a.resolved_applicability(), b.resolved_applicability()
                )
                typed = classify_outcome(
                    a, b, incompatible=True, applicability=rel,
                )
                # A disjoint relation caused by an unknown semantic axis is
                # not proof of non-conflict: retain the alignment uncertainty
                # as a non-actionable finding. Known disjoint axes stay sparse.
                if rel == REL_DISJOINT and not typed.alignment_unknown:
                    continue
                findings.append(_finding(
                    constraint, logical_key, (a, b), typed.legacy_outcome,
                    f"incompatible {predicate} values {a.value!r} vs {b.value!r}",
                    schema_version,
                    typed=typed,
                ))

    for policy in rules.cross_policies:
        targets_by_key: Mapping[str, list[ClaimView]] = _targets_by_key(
            claims, policy.target_predicate
        )
        for c in sorted(claims, key=lambda m: m.node_id):
            if c.predicate != policy.source_predicate:
                continue
            link = policy.link_key(c)
            for t in sorted(targets_by_key.get(link, ()), key=lambda m: m.node_id):
                if not claims_incompatible(
                    rules,
                    policy.source_predicate,
                    c,
                    t,
                    legacy_incompatible=policy.violates,
                ):
                    continue
                rel = applicability_relation(
                    c.resolved_applicability(), t.resolved_applicability()
                )
                typed = classify_outcome(
                    c, t, incompatible=True, applicability=rel,
                )
                if rel == REL_DISJOINT and not typed.alignment_unknown:
                    continue
                findings.append(_finding(
                    policy.constraint, f"{policy.constraint}:{link}",
                    (c, t), typed.legacy_outcome,
                    policy.detail(c, t),
                    schema_version,
                    typed=typed,
                ))

    findings.sort(key=lambda f: f.node_id)
    return findings


def _targets_by_key(
    claims: list[ClaimView], predicate: str,
) -> dict[str, list[ClaimView]]:
    out: dict[str, list[ClaimView]] = defaultdict(list)
    for c in claims:
        if c.predicate == predicate:
            out[c.normalized_key].append(c)
    return out
