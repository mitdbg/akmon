"""The v1 built-in operator library (issue-#749 families + hard-negative twins).

Positives perturb exactly one claim coordinate (design §5) and state their
expected outcome in engine vocabulary. Hard-negative twins carry
ERA-CONDITIONAL gold: twins whose suppressing mechanism gates finding
derivation today (bucketing, scope relation, value compatibility,
canonicalization) declare ``channel_today="no_conflict"``; twins whose
mechanism arrives with a sibling (succession → precedence is
resolution-layer only; plan-vs-observation → modality lands with #744) are
``flagged_today=True`` and declare their exact today-vocabulary channel —
they measure the sibling gap rather than certifying suppression.

Operators are frozen dataclasses so the YAML registry can override their
targeting fields (``predicates`` / ``keys`` / ``source_docs`` / …) as
constructor params without subclassing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from okg.bench.injection.prf import KeyedPRF
from okg.bench.injection.snapshot import ClaimSnapshot, CorpusSnapshot
from okg.bench.injection.types import (
    ClaimRow,
    ExpectedOutcome,
    Facets,
    InjectionError,
    MutationPlan,
    Site,
    Span,
    claim_site_id,
)


@dataclass(frozen=True)
class ClaimValueOperator:
    """Base: in-place rewrite of a claim's value at its localized span.

    The base declares the NEUTRAL targeting fields (empty tuples — a
    default-constructed operator targets nothing); deployments supply
    targeting via registry params (PACT ship-okg-bench). Subclasses
    implement ``rewrite`` / ``facets`` / ``expected``. The base yields one
    graph-conditioned site per matching claim, resolving a bucket
    counterpart from ``counterpart_docs`` when declared (no counterpart
    there → no site; the fixture suite's per-operator coverage guard
    catches an unexpectedly empty family loudly).
    """

    operator_id: str = ""
    version: str = "1"
    predicates: tuple[str, ...] = ()
    keys: tuple[str, ...] = ()
    source_docs: tuple[str, ...] = ()
    counterpart_docs: tuple[str, ...] = ()
    require_singleton_bucket: bool = False
    hardness_tier: str = "t1_literal"
    changes_assertion_count: bool = False

    def sites(self, corpus: CorpusSnapshot, claims: ClaimSnapshot) -> Iterable[Site]:
        # NEUTRAL contract: predicates is the targeting opt-in. A
        # default-constructed operator (no registry params) enumerates zero
        # sites — the per-operator coverage guard makes that loud, never a
        # silent match-everything.
        if not self.predicates:
            return
        buckets = claims.by_bucket()
        for claim in claims.claims:
            if claim.predicate not in self.predicates:
                continue
            if self.keys and claim.normalized_key not in self.keys:
                continue
            if self.source_docs and claim.source_doc not in self.source_docs:
                continue
            if not self.matches(claim):
                continue
            siblings = [
                sibling
                for sibling in buckets[(claim.predicate, claim.normalized_key)]
                if sibling.claim_id != claim.claim_id
            ]
            if self.require_singleton_bucket and siblings:
                continue
            counterpart: ClaimRow | None = None
            if self.counterpart_docs:
                candidates = sorted(
                    (s for s in siblings if s.source_doc in self.counterpart_docs),
                    key=lambda row: row.claim_id,
                )
                if not candidates:
                    continue
                counterpart = candidates[0]
            yield Site(
                site_id=claim_site_id(claim),
                mode="graph",
                doc=claim.source_doc,
                claim=claim,
                counterpart=counterpart,
            )

    def matches(self, claim: ClaimRow) -> bool:
        return True

    def plan(self, site: Site, doc_text: str, span: Span, prf: KeyedPRF) -> MutationPlan:
        claim = site.claim
        assert claim is not None
        tag = f"{self.operator_id}:{site.site_id}"
        post_text, params = self.rewrite(claim, prf, tag)
        if post_text == claim.value:
            raise InjectionError(
                f"operator {self.operator_id!r} produced a no-op rewrite at {site.site_id!r}"
            )
        return MutationPlan(
            site=site,
            span=span,
            pre_text=claim.value,
            post_text=post_text,
            params=params,
            facets=self.facets(claim),
            hardness_tier=self.hardness_tier,
            expected=self.expected(claim, site.counterpart),
            family_key=self.family_key(claim),
            changes_assertion_count=self.changes_assertion_count,
        )

    def family_key(self, claim: ClaimRow) -> str | None:
        return None

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        raise NotImplementedError(f"{type(self).__name__}.rewrite")

    def facets(self, claim: ClaimRow) -> Facets:
        raise NotImplementedError(f"{type(self).__name__}.facets")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        raise NotImplementedError(f"{type(self).__name__}.expected")

    # Shared expected-outcome builders -------------------------------------

    def _conflict(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return ExpectedOutcome(
            kind="conflict",
            claim_logical_id=claim.claim_id,
            counterpart_claim_id=counterpart.claim_id if counterpart else None,
            bucket_key=f"{claim.predicate}:{claim.normalized_key}",
            channel_today="definite",
            locality="cross_document" if counterpart else "internal",
        )

    def _hard_negative(
        self,
        claim: ClaimRow,
        counterpart: ClaimRow | None,
        *,
        mechanism: str,
        flagged_today: bool,
        channel_today: str,
        channel_post_siblings: str,
    ) -> ExpectedOutcome:
        return ExpectedOutcome(
            kind="hard_negative",
            claim_logical_id=claim.claim_id,
            counterpart_claim_id=counterpart.claim_id if counterpart else None,
            bucket_key=f"{claim.predicate}:{claim.normalized_key}",
            channel_today=channel_today,
            channel_post_siblings=channel_post_siblings,
            mechanism=mechanism,
            flagged_today=flagged_today,
        )


_NUMBER_UNIT_RE = re.compile(r"^(\d+(?:\.\d+)?)(\s*)(MB|GB|TB|ms|s)$")
_ISO_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_ALIASABLE_RE = re.compile(r"[a-z]+0\d*")

_UNIT_SWAP = {"MB": "GB", "GB": "TB", "TB": "MB", "ms": "s", "s": "ms"}
_STATUS_ANTONYMS = {
    "active": "retired",
    "retired": "active",
    "enabled": "disabled",
    "disabled": "enabled",
    "supported": "unsupported",
    "unsupported": "supported",
    "required": "optional",
    "optional": "required",
}


@dataclass(frozen=True)
class NumericValueOperator(ClaimValueOperator):
    """Family: numeric value and unit changes."""

    operator_id: str = "injection.numeric_value"
    version: str = "1"

    def matches(self, claim: ClaimRow) -> bool:
        return _NUMBER_UNIT_RE.match(claim.value) is not None

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        match = _NUMBER_UNIT_RE.match(claim.value)
        assert match is not None
        number, gap, unit = match.groups()
        kind = prf.choice(f"{tag}:kind", ["number", "unit"])
        if kind == "number":
            candidates = [c for c in ("128", "256", "768", "1024") if c != number]
            replacement = prf.choice(f"{tag}:number", candidates)
            return f"{replacement}{gap}{unit}", {"kind": "number", "replacement": replacement}
        return f"{number}{gap}{_UNIT_SWAP[unit]}", {"kind": "unit", "replacement": _UNIT_SWAP[unit]}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "number", "inter_document", "none", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._conflict(claim, counterpart)


@dataclass(frozen=True)
class DateTransposeOperator(ClaimValueOperator):
    """Family: date/day/year transpositions."""

    operator_id: str = "injection.date_transpose"
    version: str = "1"

    def matches(self, claim: ClaimRow) -> bool:
        return _ISO_DATE_RE.match(claim.value) is not None

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        match = _ISO_DATE_RE.match(claim.value)
        assert match is not None
        year, month, day = match.groups()
        if 1 <= int(day) <= 12 and day != month:
            return f"{year}-{day}-{month}", {"kind": "day_month_swap"}
        shift = prf.randint(f"{tag}:year", 1, 3)
        return f"{int(year) + shift}-{month}-{day}", {"kind": "year_shift", "shift": shift}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "date", "intra_document", "time", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._conflict(claim, counterpart)


@dataclass(frozen=True)
class IdentifierSubstOperator(ClaimValueOperator):
    """Family: identifier and path substitutions."""

    operator_id: str = "injection.identifier_subst"
    version: str = "1"
    alternatives: tuple[str, ...] = ()

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        candidates = [alt for alt in self.alternatives if alt != claim.value]
        if not candidates:
            raise InjectionError(f"{self.operator_id}: no substitute differs from {claim.value!r}")
        replacement = prf.choice(f"{tag}:ident", candidates)
        return replacement, {"replacement": replacement}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("mismatch", "identifier", "inter_document", "entity", "wrong_resource")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._conflict(claim, counterpart)


@dataclass(frozen=True)
class UrlSwapOperator(ClaimValueOperator):
    """Family: URL/attachment target swaps."""

    operator_id: str = "injection.url_swap"
    version: str = "1"

    def matches(self, claim: ClaimRow) -> bool:
        return claim.value.startswith("https://") and "/" in claim.value[len("https://") :]

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        base, _, last = claim.value.rpartition("/")
        candidates = [s for s in ("legacy", "v0", "internal") if s != last]
        segment = prf.choice(f"{tag}:segment", candidates)
        return f"{base}/{segment}", {"segment": segment}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("broken_reference", "url", "intra_document", "none", "broken_navigation")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return ExpectedOutcome(
            kind="broken_reference",
            claim_logical_id=claim.claim_id,
            bucket_key=f"{claim.predicate}:{claim.normalized_key}",
            locality="internal",
        )


@dataclass(frozen=True)
class ConfigSwapOperator(ClaimValueOperator):
    """Family: configuration key/value swaps (value takes another key's value)."""

    operator_id: str = "injection.config_swap"
    version: str = "1"
    donor_key: str = ""

    def sites(self, corpus: CorpusSnapshot, claims: ClaimSnapshot) -> Iterable[Site]:
        donors = {
            claim.normalized_key: claim.value
            for claim in claims.claims
            if claim.predicate in self.predicates
        }
        donor_value = donors.get(self.donor_key)
        if donor_value is None:
            return
        object.__setattr__(self, "_donor_value", donor_value)
        yield from super().sites(corpus, claims)

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        donor_value = getattr(self, "_donor_value", None)
        if donor_value is None or donor_value == claim.value:
            raise InjectionError(f"{self.operator_id}: donor key {self.donor_key!r} unusable")
        return donor_value, {"donor_key": self.donor_key, "donor_value": donor_value}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("mismatch", "config_value", "inter_document", "component", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._conflict(claim, counterpart)


@dataclass(frozen=True)
class StatusInversionOperator(ClaimValueOperator):
    """Family: explicit negation / status inversion."""

    operator_id: str = "injection.status_inversion"
    version: str = "1"

    def matches(self, claim: ClaimRow) -> bool:
        return claim.value in _STATUS_ANTONYMS

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        return _STATUS_ANTONYMS[claim.value], {"from": claim.value}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "status", "inter_document", "none", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._conflict(claim, counterpart)


@dataclass(frozen=True)
class StaleCopiedMetadataOperator(ClaimValueOperator):
    """Family: stale copied metadata — update the origin, strand the copies.

    Emits a coordinated family (shared ``family_key``): the origin document's
    value and review date move together; every copy in ``counterpart_docs``
    goes stale, one expected cross-document conflict per mutated claim.
    """

    operator_id: str = "injection.stale_copied_metadata"
    version: str = "1"

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        if claim.predicate == "reviewed":
            match = _ISO_DATE_RE.match(claim.value)
            if match is None:
                raise InjectionError(f"{self.operator_id}: reviewed value is not an ISO date")
            year, month, day = match.groups()
            return f"{int(year) + 1}-{month}-{day}", {"kind": "review_date_bump"}
        replacement = prf.choice(f"{tag}:version", ["4.7", "5.0"])
        return replacement, {"kind": "version_bump", "replacement": replacement}

    def family_key(self, claim: ClaimRow) -> str:
        return f"stale-copy:{claim.normalized_key.split('.')[0]}:{claim.source_doc}"

    def facets(self, claim: ClaimRow) -> Facets:
        object_kind = "date" if claim.predicate == "reviewed" else "number"
        return Facets("contradiction", object_kind, "inter_document", "time", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._conflict(claim, counterpart)


@dataclass(frozen=True)
class CrossDocUpdateOperator(ClaimValueOperator):
    """Family: cross-document conflicting updates."""

    operator_id: str = "injection.cross_doc_update"
    version: str = "1"

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        candidates = [team for team in ("beta-team", "core-team") if team != claim.value]
        replacement = prf.choice(f"{tag}:owner", candidates)
        return replacement, {"replacement": replacement}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "prose_claim", "inter_document", "entity", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._conflict(claim, counterpart)


@dataclass(frozen=True)
class TemporalMutationOperator(ClaimValueOperator):
    """Family: scope/condition/modality/temporal mutations — temporal positive."""

    operator_id: str = "injection.temporal_mutation"
    version: str = "1"

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        match = _ISO_DATE_RE.match(claim.value)
        if match is None:
            raise InjectionError(f"{self.operator_id}: certified value is not an ISO date")
        year, month, day = match.groups()
        shift = prf.randint(f"{tag}:shift", 1, 4)
        return f"{year}-{int(month) + shift:02d}-{day}", {"shift_months": shift}

    def matches(self, claim: ClaimRow) -> bool:
        match = _ISO_DATE_RE.match(claim.value)
        return match is not None and int(match.group(2)) <= 8

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "date", "inter_document", "time", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._conflict(claim, counterpart)


@dataclass(frozen=True)
class MalformedValueOperator(ClaimValueOperator):
    """Family: malformed command/query/value injection."""

    operator_id: str = "injection.malformed_value"
    version: str = "1"

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        garbage = f"{claim.value};;{prf.randint(f'{tag}:hex', 0, 0xFFFFFF):06x}###"
        return garbage, {"kind": "delimiter_injection"}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets(
            "malformed_value", "config_value", "intra_document", "none", "unusable_procedure"
        )

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return ExpectedOutcome(
            kind="malformed_value",
            claim_logical_id=claim.claim_id,
            bucket_key=f"{claim.predicate}:{claim.normalized_key}",
            locality="internal",
        )


class _StepRemovalMixin(ClaimValueOperator):
    """Shared line-removal plan for procedure-step operators."""

    def plan(self, site: Site, doc_text: str, span: Span, prf: KeyedPRF) -> MutationPlan:
        claim = site.claim
        assert claim is not None
        line_start = doc_text.rfind("\n", 0, span.start) + 1
        newline = doc_text.find("\n", span.end)
        line_end = len(doc_text) if newline < 0 else newline + 1
        pre_text = doc_text[line_start:line_end]
        return MutationPlan(
            site=site,
            span=Span(start=line_start, end=line_end),
            pre_text=pre_text,
            post_text="",
            params={"removed_step": claim.normalized_key},
            facets=self.facets(claim),
            hardness_tier=self.hardness_tier,
            expected=self.expected(claim, site.counterpart),
            changes_assertion_count=True,
        )


@dataclass(frozen=True)
class OmittedStepOperator(_StepRemovalMixin):
    """Family: omitted required step."""

    operator_id: str = "injection.omitted_step"
    version: str = "1"

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets(
            "omitted_required_step", "command", "intra_document", "none", "unusable_procedure"
        )

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return ExpectedOutcome(
            kind="claim_absent",
            claim_logical_id=claim.claim_id,
            bucket_key=f"{claim.predicate}:{claim.normalized_key}",
            locality="internal",
        )


# Hard-negative twins ------------------------------------------------------


@dataclass(frozen=True)
class HnRoundingOperator(ClaimValueOperator):
    """Twin: compatible rounded values → value compatibility (suppressed today)."""

    operator_id: str = "injection.hn_rounding"
    version: str = "1"
    hardness_tier: str = "t2_reexpression"

    def matches(self, claim: ClaimRow) -> bool:
        # A trailing zero is value-preserving only after a decimal point:
        # "99.9" -> "99.90" is equivalent, "99" -> "990" would be a real
        # contradiction mislabeled as a hard negative.
        return "." in claim.value and claim.value.replace(".", "", 1).isdigit()

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        return f"{claim.value}0", {"kind": "trailing_zero"}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "number", "inter_document", "none", "cosmetic_metadata")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._hard_negative(
            claim,
            counterpart,
            mechanism="value_compatibility",
            flagged_today=False,
            channel_today="no_conflict",
            channel_post_siblings="no_conflict",
        )


@dataclass(frozen=True)
class HnAliasOperator(ClaimValueOperator):
    """Twin: equivalent formatting/aliases → canonicalization (suppressed today)."""

    operator_id: str = "injection.hn_alias"
    version: str = "1"
    hardness_tier: str = "t2_reexpression"

    def matches(self, claim: ClaimRow) -> bool:
        # The alias transform (uppercase + dash before the digit block)
        # is equivalence-preserving only for lowercase alnum identifiers
        # of the word+digits shape ("cache01" -> "CACHE-01").
        return _ALIASABLE_RE.fullmatch(claim.value) is not None

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        return claim.value.upper().replace("0", "-0", 1), {"kind": "alias_form"}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("mismatch", "identifier", "inter_document", "entity", "cosmetic_metadata")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._hard_negative(
            claim,
            counterpart,
            mechanism="canonicalization",
            flagged_today=False,
            channel_today="no_conflict",
            channel_post_siblings="no_conflict",
        )


@dataclass(frozen=True)
class HnDifferentEntityOperator(ClaimValueOperator):
    """Twin: different entities → bucketing (suppressed today)."""

    operator_id: str = "injection.hn_different_entity"
    version: str = "1"
    require_singleton_bucket: bool = True

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        candidates = [v for v in ("8.0", "7.5") if v != claim.value]
        replacement = prf.choice(f"{tag}:version", candidates)
        return replacement, {"replacement": replacement}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "number", "inter_document", "entity", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._hard_negative(
            claim,
            counterpart,
            mechanism="bucketing",
            flagged_today=False,
            channel_today="no_conflict",
            channel_post_siblings="no_conflict",
        )


@dataclass(frozen=True)
class HnConditionalOperator(ClaimValueOperator):
    """Twin: conditional alternatives → scope relation (suppressed today)."""

    operator_id: str = "injection.hn_conditional"
    version: str = "1"

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        replacement = str(prf.randint(f"{tag}:timeout", 40, 90))
        return replacement, {"replacement": replacement}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "config_value", "intra_document", "condition", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._hard_negative(
            claim,
            counterpart,
            mechanism="scope_relation",
            flagged_today=False,
            channel_today="no_conflict",
            channel_post_siblings="no_conflict",
        )


@dataclass(frozen=True)
class HnSuccessionOperator(ClaimValueOperator):
    """Twin: legitimate succession → precedence (expected-flagged-today).

    Precedence is resolution-layer only, so with equal known scopes and
    incompatible values today's classifier emits a DEFINITE finding — the
    twin's era-conditional gold records exactly that, and the finding must
    leave the actionable channel once #746 separates channels and
    supersession settles the bucket.
    """

    operator_id: str = "injection.hn_succession"
    version: str = "1"
    hardness_tier: str = "t2_reexpression"

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        return f"RB-9 (supersedes {claim.value})", {"successor": "RB-9"}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("contradiction", "identifier", "inter_document", "time", "wrong_fact")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._hard_negative(
            claim,
            counterpart,
            mechanism="precedence",
            flagged_today=True,
            channel_today="definite",
            channel_post_siblings="resolved_canonical",
        )


@dataclass(frozen=True)
class HnPlanVsObservationOperator(ClaimValueOperator):
    """Twin: plan versus observation → modality control (expected-flagged-today)."""

    operator_id: str = "injection.hn_plan_vs_observation"
    version: str = "1"
    hardness_tier: str = "t3_paraphrase"

    def rewrite(self, claim: ClaimRow, prf: KeyedPRF, tag: str) -> tuple[str, dict]:
        return "completed", {"from_modality": claim.value}

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets(
            "contradiction", "prose_claim", "inter_document", "plan_vs_observation", "wrong_fact"
        )

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._hard_negative(
            claim,
            counterpart,
            mechanism="modality_control",
            flagged_today=True,
            channel_today="definite",
            channel_post_siblings="no_conflict",
        )


@dataclass(frozen=True)
class HnOptionalStepRemovedOperator(_StepRemovalMixin):
    """Twin: legitimately removable optional step → conditional requirement."""

    operator_id: str = "injection.hn_optional_step_removed"
    version: str = "1"

    def facets(self, claim: ClaimRow) -> Facets:
        return Facets("omitted_required_step", "command", "intra_document", "condition", "cosmetic_metadata")

    def expected(self, claim: ClaimRow, counterpart: ClaimRow | None) -> ExpectedOutcome:
        return self._hard_negative(
            claim,
            counterpart,
            mechanism="scope_relation",
            flagged_today=False,
            channel_today="no_conflict",
            channel_post_siblings="no_conflict",
        )


DEFAULT_V1_OPERATOR_TYPES: tuple[type, ...] = (
    NumericValueOperator,
    DateTransposeOperator,
    IdentifierSubstOperator,
    UrlSwapOperator,
    ConfigSwapOperator,
    StatusInversionOperator,
    StaleCopiedMetadataOperator,
    CrossDocUpdateOperator,
    TemporalMutationOperator,
    MalformedValueOperator,
    OmittedStepOperator,
    HnRoundingOperator,
    HnAliasOperator,
    HnDifferentEntityOperator,
    HnConditionalOperator,
    HnSuccessionOperator,
    HnPlanVsObservationOperator,
    HnOptionalStepRemovedOperator,
)
# No default_v1_operators() factory: core operators ship NEUTRAL (empty
# targeting — a default-constructed operator yields zero sites). Targeting
# is deployment-supplied registry params; see
# deployments/consistency-injection-fixture/operators.yaml for the ring-0
# tuned set (PACT ship-okg-bench, req.ship.core-operators-neutral).
