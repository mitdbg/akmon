"""Scoped precedents — the ≻-updater of the (C, Σ, ≻) claim-consistency model.

``≻`` is the scoped priority relation. Given a conflict bucket's effective
claims, :func:`resolve_bucket` picks the CANONICAL claim by a fixed
lexicographic order over signal classes (strongest first) — explicit
supersession, claim-local effective date, declared source authority,
dependency-adjusted independent support, document recency, maintenance
heuristic — optionally overridden by operator governance PRECEDENTS scoped to
a heading path.

A claim is canonical only when it is the unique ≻-maximum AND the first signal
that separates it from the runner-up is DECISIVE (a precedent, or one of the
top four classes). A full tie, or a lead decided only by the ranking-only
signals (document recency / maintenance), is CONTESTED — recency may RANK a
claim but never SETTLES one on its own (a page edited yesterday might just
have a typo fixed in a fifteen-year-old sentence).

An operator precedent updates exactly ``≻`` — the one object a governance
decision touches (source corrections update C via :mod:`.effective`; rule
corrections update Σ). Hoisted from ``cern_twiki.scoped_precedents`` (PACT
``consistency-claim-model-core``). Pure, deterministic, DB-free. v1 is
component-wise selection per bucket — no denial-constraint engine, no repair
enumeration.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

SCOPED_PRECEDENTS_SCHEMA_VERSION = "okg.claim_model.precedence.v1"

# Signal classes, strongest first.
SIGNAL_PRECEDENT = "operator_precedent"
SIGNAL_SUPERSESSION = "explicit_supersession"
SIGNAL_EFFECTIVE_DATE = "claim_local_effective_date"
SIGNAL_SOURCE_AUTHORITY = "declared_source_authority"
SIGNAL_INDEPENDENT_SUPPORT = "dependency_adjusted_support"
SIGNAL_DOCUMENT_RECENCY = "document_recency"
SIGNAL_MAINTENANCE = "maintenance_heuristic"

# Signals that can SETTLE a winner. Recency + maintenance only RANK (never settle).
_DECISIVE = frozenset({
    SIGNAL_PRECEDENT, SIGNAL_SUPERSESSION, SIGNAL_EFFECTIVE_DATE,
    SIGNAL_SOURCE_AUTHORITY, SIGNAL_INDEPENDENT_SUPPORT,
})
# The ``≻`` sort key positions, in order, mapped to their signal class.
_KEY_SIGNAL = (
    SIGNAL_PRECEDENT, SIGNAL_SUPERSESSION, SIGNAL_EFFECTIVE_DATE,
    SIGNAL_SOURCE_AUTHORITY, SIGNAL_INDEPENDENT_SUPPORT,
    SIGNAL_DOCUMENT_RECENCY, SIGNAL_MAINTENANCE,
)

OUTCOME_CANONICAL = "canonical"
OUTCOME_CONTESTED = "contested"

# The heading-path separator precedent scopes use (matches views.SCOPE_PATH_SEP).
_SCOPE_PATH_SEP = " > "


@dataclass(frozen=True)
class ClaimSignals:
    """The ≻-relevant signals on one effective claim in a conflict bucket."""

    claim_id: str
    source_class: str = ""        # for precedent matching (source family / class / authority id)
    scope: str = ""               # heading-path scope (for precedent scope-match)
    is_superseded: bool = False   # explicitly superseded by a peer (loses the supersession signal)
    effective_date: str = ""      # claim-local effective date (ISO), "" = none
    source_authority: int = 0     # declared source-authority rank (higher = stronger)
    # NAIVE corroboration: distinct source_ids asserting this claim's value in
    # its bucket, computed by the ENGINE (no producer can see the bucket). The
    # persisted basis token still reads "dependency_adjusted_support", but
    # copies do NOT count once — no dependency relation exists to adjust by, so
    # N mirrored documents outrank one correct original.
    independent_support: int = 0
    document_recency: str = ""    # document last-modified (ISO) — ranking-only
    maintenance_rank: int = 0     # maintenance heuristic — ranking-only
    # The asserted value. Claims sharing it RESTATE each other rather than
    # competing, so the basis comparison must skip past them to the first
    # rival value. ``None`` means "not supplied" and preserves the historical
    # claim-adjacent comparison; it is NOT a default value standing in for a
    # real one — see resolve_bucket, which rejects a partially-supplied bucket
    # rather than guessing.
    value: str | None = None
    # True when ``value`` is a STAND-IN identity rather than the value the
    # source asserts. A typed claim is its own peer group — a registered
    # callback, not the data, decides its incompatibility — so the derivation
    # puts the claim's node_id in ``value`` to keep every typed claim a rival of
    # every other. That surrogate ranks correctly but MEANS nothing: writing it
    # into a claim would store a node id where a value belongs. A repair may
    # RANK on a surrogate; it must never WRITE one (see
    # ``selection._propose_surviving_value_overrides``, which refuses).
    value_is_surrogate: bool = False


@dataclass(frozen=True)
class Precedent:
    """An operator governance precedent: within ``scope``, prefer ``prefer_source``.

    Updates ``≻``. ``scope`` "" is global. Competing applicable precedents
    resolve last-writer-wins by ``sequence`` (decision order).
    """

    prefer_source: str
    scope: str = ""
    sequence: int = 0


@dataclass(frozen=True)
class Resolution:
    """The ≻ resolution of one bucket."""

    outcome: str                        # OUTCOME_CANONICAL | OUTCOME_CONTESTED
    canonical_claim_id: str             # the winner, or "" when contested
    ranked_claim_ids: tuple[str, ...]   # all claims, ≻-descending
    basis: str                          # the deciding signal class, or "tie"
    # The claims the winner actually BEAT: those asserting a different value.
    # Not "everything but rank 1" — claims sharing the winner's value corroborate
    # it, and a bucket settled BY that corroboration must not then declare its
    # own corroborators superseded (which would queue a retraction of the very
    # evidence that produced the win, and drop support below the rival on the
    # next cycle). Empty when contested. Falls back to everything-but-rank-1
    # only when no values were supplied, preserving legacy callers.
    superseded_claim_ids: tuple[str, ...] = ()
    schema_version: str = SCOPED_PRECEDENTS_SCHEMA_VERSION


def _scope_applies(precedent_scope: str, claim_scope: str) -> bool:
    """A precedent applies when its scope is global ("") or overlaps the claim's
    heading-path scope (equality, or subsumption either way)."""
    if precedent_scope == "":
        return True
    if not claim_scope:
        return False
    if precedent_scope == claim_scope:
        return True
    return (claim_scope.startswith(precedent_scope + _SCOPE_PATH_SEP)
            or precedent_scope.startswith(claim_scope + _SCOPE_PATH_SEP))


def _precedent_rank(c: ClaimSignals, precedents: list[Precedent]) -> int:
    """1 if the LATEST applicable precedent prefers this claim's source_class, else 0."""
    applicable = [p for p in precedents if _scope_applies(p.scope, c.scope)]
    if not applicable:
        return 0
    latest = max(applicable, key=lambda p: p.sequence)
    return 1 if c.source_class and c.source_class == latest.prefer_source else 0


def _key(c: ClaimSignals, precedents: list[Precedent]) -> tuple:
    """The ≻ sort key (descending). Position 0 is the precedent tier; 1..6 are the
    fixed signal classes. Dates compare as ISO strings ("" sorts lowest)."""
    return (
        _precedent_rank(c, precedents),     # SIGNAL_PRECEDENT
        0 if c.is_superseded else 1,        # SIGNAL_SUPERSESSION (not-superseded wins)
        c.effective_date,                   # SIGNAL_EFFECTIVE_DATE
        c.source_authority,                 # SIGNAL_SOURCE_AUTHORITY
        c.independent_support,              # SIGNAL_INDEPENDENT_SUPPORT
        c.document_recency,                 # SIGNAL_DOCUMENT_RECENCY (ranking-only)
        c.maintenance_rank,                 # SIGNAL_MAINTENANCE (ranking-only)
    )


def resolve_bucket(
    signals: Iterable[ClaimSignals], precedents: Iterable[Precedent] = (),
) -> Resolution:
    """Resolve a conflict bucket to a canonical winner or contested. Pure, deterministic.

    Ranks the claims ≻-descending; the top is CANONICAL only when the first signal
    that separates it from the runner-up is DECISIVE. A full tie, or a lead only on
    the ranking-only signals (recency / maintenance), is CONTESTED.

    The runner-up is the best claim asserting a DIFFERENT ``value``, not simply
    the second-ranked claim. Claims sharing the leader's value restate it, so
    comparing against one reports a tie that answers no question the bucket
    asks — and it is precisely what made ``independent_support`` unable to
    settle anything, since a value corroborated by *s* sources is carried by
    *s* mutually-tied claims that occupy both top slots. When no claim carries
    a different value the bucket is unanimous, hence canonical.

    ``value`` is optional. If NO claim supplies one the historical
    claim-adjacent comparison is used unchanged; if only SOME do, the bucket is
    rejected rather than silently mixing the two readings.
    """
    precedents = list(precedents)
    items = list(signals)
    if not items:
        return Resolution(OUTCOME_CONTESTED, "", (), "empty")

    supplied = [c.value is not None for c in items]
    if any(supplied) and not all(supplied):
        raise ValueError(
            "resolve_bucket: value supplied on some claims but not all "
            f"({sum(supplied)}/{len(items)}); a partially-valued bucket has no "
            "well-defined peer grouping, and defaulting the rest to a shared "
            "empty value would silently merge distinct claims into one peer"
        )
    value_aware = all(supplied)

    ranked = sorted(items, key=lambda c: (_key(c, precedents), c.claim_id), reverse=True)
    ranked_ids = tuple(c.claim_id for c in ranked)

    def _beaten() -> tuple[str, ...]:
        """The claims the winner beat. Value-aware: claims restating the
        winner's value corroborate it and were not beaten by it."""
        if not value_aware:
            return ranked_ids[1:]
        return tuple(
            c.claim_id for c in ranked[1:] if c.value != ranked[0].value)

    if len(ranked) == 1:
        return Resolution(
            OUTCOME_CANONICAL, ranked[0].claim_id, ranked_ids, "sole_claim", ())

    if value_aware:
        rival = next(
            (c for c in ranked[1:] if c.value != ranked[0].value), None)
        if rival is None:
            # Every claim asserts one value: unanimous, so nothing contests it
            # and — critically — nothing was superseded.
            return Resolution(
                OUTCOME_CANONICAL, ranked[0].claim_id, ranked_ids,
                "sole_claim", ())
    else:
        rival = ranked[1]

    top, runner = _key(ranked[0], precedents), _key(rival, precedents)
    for i, (t, r) in enumerate(zip(top, runner)):
        if t != r:  # first position where the leader strictly beats the runner-up
            basis = _KEY_SIGNAL[i]
            if basis in _DECISIVE:
                return Resolution(
                    OUTCOME_CANONICAL, ranked[0].claim_id, ranked_ids, basis,
                    _beaten())
            # ranking-only lead: contested, so nothing is superseded
            return Resolution(OUTCOME_CONTESTED, "", ranked_ids, basis, ())
    return Resolution(OUTCOME_CONTESTED, "", ranked_ids, "tie")  # fully tied
