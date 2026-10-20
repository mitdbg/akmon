"""Typed core of the injection framework.

The contracts these types pin are normative in
pact/changes/consistency-injection-benchmark/designs/design.md (§5–§7):
spans are exact character offsets into the decoded document text; every
mutation carries the issue-#749 facet vector, a hardness tier, an
era-conditional expected outcome, and an exact inverse; and every
gold-integrity failure is a recorded rejection, never a silent drop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

HARDNESS_TIERS = (
    "t1_literal",
    "t2_reexpression",
    "t3_paraphrase",
    "t4_cross_predicate",
)

REJECTION_REASONS = (
    "ambiguous_match",
    "not_found",
    "preimage_mismatch",
    "identity_unstable",
    "b0_overlap",
    "span_overlap",
    "bucket_collision",
)

SITE_MODES = ("graph", "independent")


class InjectionError(RuntimeError):
    """Loud failure in the injection framework — never a silent fallback."""


@dataclass(frozen=True)
class Span:
    """Half-open character span ``[start, end)`` in decoded document text.

    ``start == end`` is a zero-width insertion point.
    """

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < self.start:
            raise InjectionError(f"malformed span [{self.start}, {self.end})")

    def overlaps(self, other: "Span") -> bool:
        """Overlap test; zero-width spans occupy one position for this test."""
        a_end = max(self.end, self.start + 1)
        b_end = max(other.end, other.start + 1)
        return self.start < b_end and other.start < a_end

    def to_json(self) -> dict[str, int]:
        return {"start": self.start, "end": self.end}


@dataclass(frozen=True)
class ClaimRow:
    """One effective claim from the pinned clean generation.

    Mirrors the ``okg.claim_effective`` targeting columns (predicate,
    normalized_key, value, scope, scope_state, claim_semantics, source_id);
    ``source_doc``
    is the corpus-relative path the snapshot builder resolved the claim's
    source to, and ``claim_id`` is its ``claim_logical_id`` (value- and
    span-excluding, so an in-place value rewrite preserves it).
    """

    claim_id: str
    predicate: str
    normalized_key: str
    value: str
    scope: str
    scope_state: str
    source_doc: str
    claim_semantics: str | None = None

    def __post_init__(self) -> None:
        if not self.claim_id:
            raise InjectionError("ClaimRow.claim_id must be non-empty")
        if not self.source_doc:
            raise InjectionError(f"claim {self.claim_id!r} has no source_doc")
        if self.claim_semantics is not None and not isinstance(
            self.claim_semantics, str
        ):
            raise InjectionError(
                f"claim {self.claim_id!r} has non-string claim_semantics"
            )


@dataclass(frozen=True)
class Facets:
    """The issue-#749 facet vector, recorded verbatim on every mutation."""

    relation: str
    object: str
    scope: str
    semantic_control: str
    consequence: str

    def to_json(self) -> dict[str, str]:
        return {
            "relation": self.relation,
            "object": self.object,
            "scope": self.scope,
            "semantic_control": self.semantic_control,
            "consequence": self.consequence,
        }


@dataclass(frozen=True)
class ExpectedOutcome:
    """Engine-vocabulary expectation for one mutation (era-conditional).

    ``channel_today`` states the expectation under today's vocabulary;
    ``channel_post_siblings`` the expectation once #744/#746 land. Hard
    negatives name the suppressing ``mechanism`` and, where that mechanism
    does not gate findings yet, carry ``flagged_today=True``
    (expected-flagged-today, design §5).
    """

    kind: str
    claim_logical_id: str | None = None
    counterpart_claim_id: str | None = None
    bucket_key: str | None = None
    channel_today: str | None = None
    channel_post_siblings: str | None = None
    locality: str | None = None
    severity: str | None = None
    mechanism: str | None = None
    flagged_today: bool | None = None

    def __post_init__(self) -> None:
        if not self.kind:
            raise InjectionError("ExpectedOutcome.kind must be non-empty")

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"kind": self.kind}
        for key in (
            "claim_logical_id",
            "counterpart_claim_id",
            "bucket_key",
            "channel_today",
            "channel_post_siblings",
            "locality",
            "severity",
            "mechanism",
            "flagged_today",
        ):
            value = getattr(self, key)
            if value is not None:
                out[key] = value
        return out


@dataclass(frozen=True)
class Site:
    """One candidate injection site.

    Graph-conditioned sites carry the claim (and optionally a counterpart
    claim for cross-document pairs) and are localized by the injector;
    independent sites carry a pre-resolved span and no claim.
    """

    site_id: str
    mode: str
    doc: str
    claim: ClaimRow | None = None
    counterpart: ClaimRow | None = None
    span: Span | None = None

    def __post_init__(self) -> None:
        if not self.site_id:
            raise InjectionError("Site.site_id must be non-empty")
        if self.mode not in SITE_MODES:
            raise InjectionError(f"Site.mode {self.mode!r} not in {SITE_MODES}")
        if self.mode == "graph" and self.claim is None:
            raise InjectionError(f"graph site {self.site_id!r} carries no claim")
        if self.mode == "independent" and self.span is None:
            raise InjectionError(f"independent site {self.site_id!r} carries no span")


def claim_site_id(claim: ClaimRow) -> str:
    """Deterministic site id for a single-claim site."""
    return f"claim:{claim.claim_id}"


def pair_site_id(claim: ClaimRow, counterpart: ClaimRow) -> str:
    """Deterministic, order-canonical site id for a cross-document pair site."""
    a, b = sorted((claim.claim_id, counterpart.claim_id))
    return f"pair:{a}:{b}"


def raw_site_id(doc: str, span: Span, kind: str) -> str:
    """Deterministic site id for a pipeline-independent raw-span site."""
    return f"raw:{doc}:{span.start}-{span.end}:{kind}"


@dataclass(frozen=True)
class MutationPlan:
    """One operator's planned rewrite of a site.

    ``span`` / ``pre_text`` address the ORIGINAL document text (the injector
    re-checks the byte pre-image before applying); a zero-width span with
    ``pre_text == ""`` is an insertion. Operators that add or remove
    same-(predicate, key) assertions must set ``changes_assertion_count`` so
    the identity-stability gate runs (design §5).
    """

    site: Site
    span: Span
    pre_text: str
    post_text: str
    params: Mapping[str, Any]
    facets: Facets
    hardness_tier: str
    expected: ExpectedOutcome
    family_key: str | None = None
    changes_assertion_count: bool = False

    def __post_init__(self) -> None:
        if self.hardness_tier not in HARDNESS_TIERS:
            raise InjectionError(
                f"hardness_tier {self.hardness_tier!r} not in {HARDNESS_TIERS}"
            )
        if (self.span.end - self.span.start) != len(self.pre_text):
            raise InjectionError(
                f"plan span length {self.span.end - self.span.start} != "
                f"len(pre_text) {len(self.pre_text)} at {self.site.site_id!r}"
            )
        if self.pre_text == self.post_text:
            raise InjectionError(f"no-op plan at {self.site.site_id!r}")


@dataclass(frozen=True)
class RejectedSite:
    """A recorded gold-integrity rejection (never silent)."""

    site_id: str
    operator_id: str
    operator_version: str
    doc: str
    reason: str
    detail: str = ""

    def __post_init__(self) -> None:
        if self.reason not in REJECTION_REASONS:
            raise InjectionError(f"rejection reason {self.reason!r} not in {REJECTION_REASONS}")

    def to_json(self) -> dict[str, Any]:
        return {
            "site_id": self.site_id,
            "operator": {"id": self.operator_id, "version": self.operator_version},
            "doc": self.doc,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class MutationRecord:
    """One applied mutation — a ``mutations.jsonl`` row (the answer key)."""

    mutation_id: str
    family_id: str
    mode: str
    doc: str
    span: Span
    pre_text: str
    post_text: str
    operator_id: str
    operator_version: str
    params: Mapping[str, Any]
    facets: Facets
    hardness_tier: str
    expected: ExpectedOutcome
    inverse_span: Span
    inverse_text: str

    def to_json(self) -> dict[str, Any]:
        return {
            "mutation_id": self.mutation_id,
            "family_id": self.family_id,
            "mode": self.mode,
            "doc": self.doc,
            "span": self.span.to_json(),
            "pre_text": self.pre_text,
            "post_text": self.post_text,
            "operator": {"id": self.operator_id, "version": self.operator_version},
            "params": dict(self.params),
            "facets": self.facets.to_json(),
            "hardness_tier": self.hardness_tier,
            "expected": self.expected.to_json(),
            "inverse": {"span": self.inverse_span.to_json(), "text": self.inverse_text},
        }


@dataclass(frozen=True)
class RunConfig:
    """Pinned injection-run configuration (part of the determinism contract)."""

    seed: int
    out_dir: Path
    selection_rate: float = 1.0
    b0_claim_ids: frozenset[str] = frozenset()
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not (0.0 <= self.selection_rate <= 1.0):
            raise InjectionError(f"selection_rate {self.selection_rate} outside [0, 1]")
