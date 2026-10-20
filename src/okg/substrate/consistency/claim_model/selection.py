"""Repair selection — the reified ``r*`` layer of the (C, Σ, ≻) claim model.

Detection (``conflicts.py``) and precedence (``precedence.py``) label a bucket's
claims canonical / contested / superseded but **execute nothing**: there is no
first-class "selected repair" to record, protect with a certificate, or check
for equality. This module reifies it (adjudication #7): a repair is a set of
corrections with pairwise-disjoint footprints, chosen from **declared, versioned
candidate generators** (adjudication #8), and the choice is recorded as a
:class:`SelectedRepair`.

What v1 is — exactly as the 2026-07-13 formalism adjudication ratified, no more:

- **Declared generators (adj. #8, verbatim).** Every repair the engine can
  select comes from a :class:`RepairGenerator` with an id, version, and output
  form (the correction actions it may emit). Two candidate-producing generators
  ship, in this declared order:

  1. :data:`PRECEDENCE_RETRACTION_GENERATOR` — for a bucket resolved with a
     *decisive* winner it proposes retracting the precedence-losers; for a
     contested / tie bucket it proposes nothing.
  2. :data:`OVERRIDE_SURVIVING_VALUE_GENERATOR` — for the same decisive bucket
     it proposes REWRITING each loser's value to the winner's, so the conflict
     closes without deleting the losing claims. It refuses (proposes nothing)
     whenever the evidence names no value to copy: a contested bucket, a bucket
     carrying no values at all, or a bucket whose ``value`` is the typed
     surrogate node_id rather than an asserted value. It never synthesizes a
     value — every value it writes is copied verbatim from a claim that
     SURVIVES the repair, named by ``source_claim_id`` on the correction.

  The **empty repair is always a candidate by construction**
  (:func:`enumerate_candidates` always includes it). A generator whose
  per-evaluation candidate set cannot be finitely enumerated is declarable only
  as ``run_always`` — mirroring the write-envelope unbounded marker.
- **Reified, deterministic selection (adj. #9 ADAPT/DEFER).** Selection is the
  **precedence full-repair limit**: apply every decisive-bucket retraction,
  select the empty repair for every conflict with no decisive winner. The scalar
  objective (error cost + λ·repair cost under a budget) is **deferred** — no
  arbitration, no costs, no λ. Building that machinery here would be a review
  defect against ``req.repair.objective-as-adjudicated``.

  With two generators able to propose for the same bucket, the tie-break is
  **declaration order** (:func:`select_repairs`): the bucket takes the candidate
  of the EARLIEST-declared generator in :data:`DEFAULT_GENERATORS` that proposed
  one, and the empty repair when none did. That rule is ordering-stable — it is
  fixed by the declaration, not read off the inputs or off the candidates'
  content — so it is invariant under input shuffling and involves no comparison
  of repairs. Retraction is declared first, so the ratified precedence
  full-repair limit is preserved EXACTLY. Preferring an override on its merits
  is precisely the arbitration adjudication #9 deferred; until that lands, the
  enlarged candidate set is a recorded MEASUREMENT SURFACE (every repair the
  engine could have taken is enumerated and persisted), not a new decision.
- **Abstention preserved exactly (adj. #10, KEEP-ENGINE).** A bucket with no
  decisive winner selects the empty repair *for that conflict* and its violation
  is retained as :class:`UnresolvedConflict` with reason ``contested``. The
  engine's ranking-only-never-settles semantics is inherited unchanged: selection
  consults :func:`resolve_bucket` and never re-derives — recency *ranks* a repair,
  it never *authorizes* one.

Notation (``consistency-differential-spec/designs/notation-map.md``): ``r`` =
:class:`SelectedRepair`; ``R(G)`` = :class:`CandidateSet` over
:class:`RepairGenerator`; ``r* selection`` = :func:`select_repairs`; ``dist(r)``
= ``distrusted_claim_ids`` and ``r(G)∖G`` = ``asserted_claim_ids`` on the
selection record. ``distrusted`` coincides with ``superseded`` (the ≻-dominated
status from ``bucket_resolution``) only in this full-repair precedence limit;
``asserted`` is empty while the SELECTED repair stays deletion-shaped — the
override generator populates the candidate set, and declaration order keeps
retraction selected, so nothing is asserted yet.

Same discipline as its siblings: **pure** — DB-free, no LLM, no wall clock,
deterministic sorted output, inputs never mutated. Persistence (the pinned store
``okg.repair_selections``) and materialization of the resolved projection are
**later tasks**; this module is the pure selection layer only.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Mapping

from okg.substrate.consistency.claim_model.effective import (
    ACTION_ASSERT,
    ACTION_OVERRIDE,
    ACTION_RETRACT,
)
from okg.substrate.consistency.claim_model.precedence import (
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    ClaimSignals,
    Precedent,
    resolve_bucket,
)

# The selection's own schema version. It is stamped into the digested
# ``SelectedRepair`` JSON *and* onto every persisted ``okg.repair_selections``
# row, which makes it the UPGRADE FENCE for the differential carry: a
# generation's row may be copied forward only when it was written under THIS
# version. Bump it whenever the recorded selection content changes shape — v2
# added the override candidate to the candidate set (PACT
# concord-second-repair-generator), so a v1 row's `override_claim_ids` is
# absent-not-empty and carrying it forward would silently produce a
# candidate set missing the override candidate and a whole-generation digest
# that no longer matches a from-scratch recompute.
SELECTION_SCHEMA_VERSION = "okg.claim_model.selection.v2"

# The version a row written before the override candidate existed carries. It
# is the DDL default of ``okg.repair_selections.selection_schema_version``, so
# a legacy row backfilled by the upgrade reads as v1 and fails the fence.
SELECTION_SCHEMA_VERSION_LEGACY_V1 = "okg.claim_model.selection.v1"

# The repair-correction vocabulary is the SAME three actions as operator
# source-corrections (adjudication #7: one vocabulary, two provenances —
# operator corrections edit C, repair corrections are generator output).
_KNOWN_ACTIONS = frozenset({ACTION_RETRACT, ACTION_OVERRIDE, ACTION_ASSERT})

# Declared generator identities. Two candidate-producing generators ship (a
# deletion-shaped one and a rewrite-shaped one); the empty repair carries a
# reserved identity so every candidate — including "do nothing" — names a
# declared provenance.
PRECEDENCE_RETRACTION_GENERATOR_ID = "precedence-retraction"
OVERRIDE_SURVIVING_VALUE_GENERATOR_ID = "override-surviving-value"
EMPTY_REPAIR_GENERATOR_ID = "empty-repair"
GENERATOR_VERSION_V1 = "v1"

# The empty repair is bucket-agnostic ("do nothing" applies to no single
# conflict); this reserved bucket key marks it in the candidate set.
EMPTY_REPAIR_BUCKET_KEY = ""


class RepairSelectionError(ValueError):
    """A generator declaration or selection is malformed; fail loud, never guess."""


# --------------------------------------------------------------------------- #
# Corrections a repair applies                                                 #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RepairCorrection:
    """One correction a repair applies, targeting a claim by deterministic id.

    ``action`` is one of the shared ``ACTION_*`` constants; its provenance is a
    generator, not an operator decision (adjudication #7). ``ACTION_RETRACT``
    drops a superseded claim and carries no payload.

    ``ACTION_OVERRIDE`` rewrites ``claim_id``'s value, and its payload is
    ``source_claim_id`` — a REFERENCE to the surviving claim whose value is
    copied, never a literal the generator chose. That is deliberate: a
    correction that cannot name a value cannot invent one, so "fabricate a
    plausible value to close the conflict" is not expressible in this
    vocabulary. Applying the correction reads the value off
    ``source_claim_id``; the claim must survive the same repair, or the
    correction would name a value the repaired state no longer contains.

    The two payload shapes are mutually exclusive and enforced: a RETRACT with
    a ``source_claim_id``, or an OVERRIDE without one, fails loud. The assert
    payload is added additively when an assert generator lands (freeze rules).
    """

    action: str
    claim_id: str
    source_claim_id: str | None = None

    def __post_init__(self) -> None:
        if self.action == ACTION_OVERRIDE and not self.source_claim_id:
            raise RepairSelectionError(
                f"override correction on {self.claim_id!r} names no "
                f"source_claim_id; an override must COPY a surviving claim's "
                f"value, never supply one of its own"
            )
        if self.action != ACTION_OVERRIDE and self.source_claim_id is not None:
            raise RepairSelectionError(
                f"{self.action!r} correction on {self.claim_id!r} carries a "
                f"source_claim_id; only an override reads a value from another "
                f"claim"
            )
        if self.source_claim_id is not None and self.source_claim_id == self.claim_id:
            raise RepairSelectionError(
                f"override correction on {self.claim_id!r} sources its value "
                f"from itself; that changes nothing"
            )

    def to_json_dict(self) -> dict:
        d = {"action": self.action, "claim_id": self.claim_id}
        # Omitted (not null) when absent, so a retraction's persisted shape is
        # byte-identical to the one this layer emitted before overrides existed.
        if self.source_claim_id is not None:
            d["source_claim_id"] = self.source_claim_id
        return d

    @classmethod
    def from_json_dict(cls, d: Mapping) -> RepairCorrection:
        return cls(
            action=d["action"],
            claim_id=d["claim_id"],
            source_claim_id=d.get("source_claim_id"),
        )


# --------------------------------------------------------------------------- #
# Declared, versioned candidate generators (R(G))                             #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RepairGenerator:
    """A declared, versioned repair-candidate generator (adjudication #8).

    Pure declaration data that round-trips (:meth:`to_json_dict` /
    :meth:`from_json_dict`): ``generator_id`` and ``version`` are its identity;
    ``output_form`` is the set of correction actions it may emit (a subset of the
    shared ``ACTION_*`` vocabulary). ``unbounded`` marks a generator whose
    per-evaluation candidate set cannot be finitely enumerated ahead of time —
    such a generator is declarable **only** as ``run_always`` (it can never be
    skipped), mirroring the write-envelope unbounded marker; declaring it
    otherwise fails loud.

    The ``propose`` contract dispatches to the pure implementation registered for
    ``generator_id`` and enforces that emitted corrections stay within
    ``output_form``.
    """

    generator_id: str
    version: str
    output_form: frozenset[str]
    unbounded: bool = False
    run_always: bool = False

    def __post_init__(self) -> None:
        unknown = set(self.output_form) - _KNOWN_ACTIONS
        if unknown:
            raise RepairSelectionError(
                f"generator {self.generator_id!r} declares unknown correction "
                f"actions {sorted(unknown)!r}; allowed: {sorted(_KNOWN_ACTIONS)!r}"
            )
        if self.unbounded and not self.run_always:
            raise RepairSelectionError(
                f"generator {self.generator_id!r} is unbounded and must be "
                f"declared run_always (an unbounded generator can never be "
                f"skipped)"
            )

    def propose(
        self,
        buckets: Iterable[ConflictBucket],
        precedents: Iterable[Precedent] = (),
    ) -> tuple[Candidate, ...]:
        """Enumerate this generator's candidates over the conflict buckets. Pure.

        Dispatches to the implementation registered for ``generator_id``; raises
        if none is registered, or if the implementation emits a correction whose
        action is outside this generator's declared ``output_form``.
        """
        impl = _PROPOSE_IMPLS.get(self.generator_id)
        if impl is None:
            raise RepairSelectionError(
                f"no propose implementation registered for generator "
                f"{self.generator_id!r}"
            )
        candidates = impl(self, list(buckets), list(precedents))
        seen_buckets: set[str] = set()
        for candidate in candidates:
            outside = {c.action for c in candidate.corrections} - self.output_form
            if outside:
                raise RepairSelectionError(
                    f"generator {self.generator_id!r} emitted corrections "
                    f"{sorted(outside)!r} outside its declared output_form "
                    f"{sorted(self.output_form)!r}"
                )
            # One candidate per bucket per generator. Selection's tie-break is
            # declaration ORDER (see select_repairs); two candidates from one
            # generator for one bucket would leave that rule with nothing to
            # separate them, so it is refused rather than resolved by guess.
            if candidate.bucket_key in seen_buckets:
                raise RepairSelectionError(
                    f"generator {self.generator_id!r} proposed more than one "
                    f"candidate for bucket {candidate.bucket_key!r}; declaration "
                    f"order cannot separate them"
                )
            seen_buckets.add(candidate.bucket_key)
        return tuple(candidates)

    def to_json_dict(self) -> dict:
        return {
            "generator_id": self.generator_id,
            "version": self.version,
            "output_form": sorted(self.output_form),
            "unbounded": self.unbounded,
            "run_always": self.run_always,
        }

    @classmethod
    def from_json_dict(cls, d: Mapping) -> RepairGenerator:
        return cls(
            generator_id=d["generator_id"],
            version=d["version"],
            output_form=frozenset(d["output_form"]),
            unbounded=bool(d.get("unbounded", False)),
            run_always=bool(d.get("run_always", False)),
        )


# --------------------------------------------------------------------------- #
# The conflict bucket presented to selection                                  #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ConflictBucket:
    """One conflict presented to repair selection: a key plus its competing claims.

    ``bucket_key`` names the conflict (e.g. the ``ConflictFinding`` logical key,
    or ``predicate:canonical_key``); selection keys every per-conflict outcome
    and unresolved reason on it. ``signals`` are the ≻-relevant
    :class:`ClaimSignals` of the claims in the bucket — resolution consumes them
    via :func:`resolve_bucket` (order-independent; claim-id tiebreaks live inside
    that existing semantics).
    """

    bucket_key: str
    signals: tuple[ClaimSignals, ...]


# --------------------------------------------------------------------------- #
# The candidate set (recorded per evaluation)                                 #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Candidate:
    """One repair candidate: the corrections it applies, tagged by generator.

    A candidate is per-bucket (``bucket_key``); the empty repair uses
    :data:`EMPTY_REPAIR_BUCKET_KEY` and empty ``corrections``. Carries the
    generator identity + version so every candidate names a declared provenance.
    """

    bucket_key: str
    generator_id: str
    generator_version: str
    corrections: tuple[RepairCorrection, ...] = ()

    def to_json_dict(self) -> dict:
        return {
            "bucket_key": self.bucket_key,
            "generator_id": self.generator_id,
            "generator_version": self.generator_version,
            "corrections": [c.to_json_dict() for c in self.corrections],
        }


@dataclass(frozen=True)
class CandidateSet:
    """The finite, enumerable, recorded set of candidates for one evaluation.

    ``candidates`` is deterministically ordered; :meth:`to_json_dict` renders it
    for later persistence (the pinned ``okg.repair_selections`` store is a later
    task — this layer stays pure).
    """

    candidates: tuple[Candidate, ...] = ()
    schema_version: str = SELECTION_SCHEMA_VERSION

    def to_json_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "candidates": [c.to_json_dict() for c in self.candidates],
        }


# --------------------------------------------------------------------------- #
# The selection record (r*)                                                    #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SelectedCandidate:
    """The candidate selected for one conflict, with the deciding basis.

    ``basis`` is the deciding signal class from :class:`Resolution` (a decisive
    signal for a canonical bucket, ``"sole_claim"`` for a lone claim, or the
    ranking-only / ``"tie"`` basis for a contested bucket). ``outcome`` is the
    bucket's ``resolve_bucket`` outcome.
    """

    bucket_key: str
    generator_id: str
    generator_version: str
    basis: str
    outcome: str
    corrections: tuple[RepairCorrection, ...] = ()

    def to_json_dict(self) -> dict:
        return {
            "bucket_key": self.bucket_key,
            "generator_id": self.generator_id,
            "generator_version": self.generator_version,
            "basis": self.basis,
            "outcome": self.outcome,
            "corrections": [c.to_json_dict() for c in self.corrections],
        }


@dataclass(frozen=True)
class UnresolvedConflict:
    """A conflict the selection abstained on: the violation is retained.

    ``reason`` is ``contested`` (adjudication #10 encoding); ``basis`` carries the
    specific deciding signal (``"tie"``, ``document_recency``, ``maintenance_*``)
    so an operator sees *why* it is contested. Recency never authorizes a repair.
    """

    bucket_key: str
    reason: str
    basis: str

    def to_json_dict(self) -> dict:
        return {
            "bucket_key": self.bucket_key,
            "reason": self.reason,
            "basis": self.basis,
        }


@dataclass(frozen=True)
class SelectedRepair:
    """The recorded selection over the candidate set — the reified ``r*``.

    ``selected`` carries one :class:`SelectedCandidate` per conflict (its
    generator id + version + basis); ``distrusted_claim_ids`` (``dist(r)``) are
    the retraction targets across decisive buckets; ``asserted_claim_ids``
    (``r(G)∖G``) is empty under the v1 deletion-shaped generator (the field
    exists per the pinned notation); ``unresolved`` retains each abstained
    conflict. Deterministic and pure. :meth:`to_json_dict` is the persistence
    shape (byte-stable under input shuffling).
    """

    selected: tuple[SelectedCandidate, ...]
    distrusted_claim_ids: tuple[str, ...]
    asserted_claim_ids: tuple[str, ...]
    unresolved: tuple[UnresolvedConflict, ...]
    candidate_set: CandidateSet
    schema_version: str = SELECTION_SCHEMA_VERSION

    def to_json_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "selected": [s.to_json_dict() for s in self.selected],
            "distrusted_claim_ids": list(self.distrusted_claim_ids),
            "asserted_claim_ids": list(self.asserted_claim_ids),
            "unresolved": [u.to_json_dict() for u in self.unresolved],
            "candidate_set": self.candidate_set.to_json_dict(),
        }


# --------------------------------------------------------------------------- #
# The v1 generator: precedence retraction                                     #
# --------------------------------------------------------------------------- #
def _losers(resolution) -> tuple[str, ...]:
    """The precedence-losers of a bucket: the claims the winner actually BEAT.

    NOT "every ranked claim but the canonical". Claims asserting the winner's
    value corroborate it rather than lose to it, and retracting them would
    delete the evidence that settled the bucket — dropping support below the
    rival value, so the next cycle re-opens the conflict this repair just
    closed. ``resolve_bucket`` computes the set where the values live.
    """
    return tuple(
        cid for cid in resolution.superseded_claim_ids
        if cid != resolution.canonical_claim_id
    )


def _propose_precedence_retractions(
    generator: RepairGenerator,
    buckets: list[ConflictBucket],
    precedents: list[Precedent],
) -> tuple[Candidate, ...]:
    """Propose one retraction candidate per *decisively-resolved* bucket.

    For a bucket whose :func:`resolve_bucket` outcome is canonical with at least
    one loser, retract the losers; a contested / tie bucket, or a sole-claim
    bucket with no loser, proposes nothing (the always-present empty repair
    covers those). Pure and deterministic.
    """
    candidates: list[Candidate] = []
    for bucket in sorted(buckets, key=lambda b: b.bucket_key):
        resolution = resolve_bucket(bucket.signals, precedents)
        if resolution.outcome != OUTCOME_CANONICAL:
            continue
        losers = _losers(resolution)
        if not losers:
            continue
        corrections = tuple(
            RepairCorrection(action=ACTION_RETRACT, claim_id=cid)
            for cid in sorted(losers)
        )
        candidates.append(Candidate(
            bucket_key=bucket.bucket_key,
            generator_id=generator.generator_id,
            generator_version=generator.version,
            corrections=corrections,
        ))
    return tuple(candidates)


PRECEDENCE_RETRACTION_GENERATOR = RepairGenerator(
    generator_id=PRECEDENCE_RETRACTION_GENERATOR_ID,
    version=GENERATOR_VERSION_V1,
    output_form=frozenset({ACTION_RETRACT}),
)


# --------------------------------------------------------------------------- #
# The second generator: override with a surviving value                       #
# --------------------------------------------------------------------------- #
def _override_source(resolution, bucket: ConflictBucket) -> ClaimSignals | None:
    """The surviving claim whose value an override may copy, or ``None``.

    Returns the canonical winner when — and ONLY when — it actually asserts a
    value the repaired state would keep. Three ways that fails, each of which
    means "the evidence names no replacement", and in each of which the caller
    must propose NOTHING rather than write something:

    * the bucket has no decisive winner (contested / tie): the precedence rules
      REFUSED to settle it, and an override that picked a side would settle by
      the back door what abstention deliberately left open;
    * the winner carries no ``value`` at all (``None`` — ``resolve_bucket``
      rejects a partially-valued bucket, so this means the whole bucket is
      value-less): there is nothing to copy;
    * the winner's ``value`` is the typed SURROGATE node_id
      (``value_is_surrogate``): it ranks the claim but is not a value any source
      asserted, and writing it would store an identifier where a value belongs.

    Note what is NOT here: any construction of a value. The return is a claim
    already in the bucket, so the only values reachable are ones a source
    already asserted.
    """
    if resolution.outcome != OUTCOME_CANONICAL:
        return None
    winner = next(
        (s for s in bucket.signals if s.claim_id == resolution.canonical_claim_id),
        None,
    )
    if winner is None or winner.value is None or winner.value_is_surrogate:
        return None
    return winner


def _propose_surviving_value_overrides(
    generator: RepairGenerator,
    buckets: list[ConflictBucket],
    precedents: list[Precedent],
) -> tuple[Candidate, ...]:
    """Propose one override candidate per decisive bucket that names a value.

    For a bucket whose :func:`resolve_bucket` outcome is canonical with at least
    one loser AND whose winner asserts a real (non-surrogate) value, propose
    rewriting every loser's value to the winner's — referenced by
    ``source_claim_id``, so the value travels as a pointer at a surviving claim
    and is never authored here. Every other bucket proposes nothing: see
    :func:`_override_source` for the three refusals. Pure and deterministic.
    """
    candidates: list[Candidate] = []
    for bucket in sorted(buckets, key=lambda b: b.bucket_key):
        resolution = resolve_bucket(bucket.signals, precedents)
        source = _override_source(resolution, bucket)
        if source is None:
            continue
        losers = _losers(resolution)
        if not losers:
            continue
        corrections = tuple(
            RepairCorrection(
                action=ACTION_OVERRIDE,
                claim_id=cid,
                source_claim_id=source.claim_id,
            )
            for cid in sorted(losers)
        )
        candidates.append(Candidate(
            bucket_key=bucket.bucket_key,
            generator_id=generator.generator_id,
            generator_version=generator.version,
            corrections=corrections,
        ))
    return tuple(candidates)


OVERRIDE_SURVIVING_VALUE_GENERATOR = RepairGenerator(
    generator_id=OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
    version=GENERATOR_VERSION_V1,
    output_form=frozenset({ACTION_OVERRIDE}),
)

# The declared generator set, IN PRECEDENCE ORDER — the order is load-bearing:
# :func:`select_repairs` breaks a per-bucket tie by taking the earliest-declared
# generator that proposed. Retraction first preserves the ratified precedence
# full-repair limit exactly; the override generator enlarges what is enumerated
# and recorded without changing what is selected. The empty repair is added
# structurally by :func:`enumerate_candidates`, so it is always a candidate
# regardless of the generator set.
DEFAULT_GENERATORS: tuple[RepairGenerator, ...] = (
    PRECEDENCE_RETRACTION_GENERATOR,
    OVERRIDE_SURVIVING_VALUE_GENERATOR,
)

# generator_id -> pure propose implementation. Populated once at import and
# treated as immutable; extension (an assert generator) registers here.
_PROPOSE_IMPLS: dict[str, Callable[
    [RepairGenerator, list["ConflictBucket"], list[Precedent]],
    tuple["Candidate", ...],
]] = {
    PRECEDENCE_RETRACTION_GENERATOR_ID: _propose_precedence_retractions,
    OVERRIDE_SURVIVING_VALUE_GENERATOR_ID: _propose_surviving_value_overrides,
}


# --------------------------------------------------------------------------- #
# Enumeration + selection                                                      #
# --------------------------------------------------------------------------- #
def _empty_candidate() -> Candidate:
    """The always-present empty repair (adjudication #8, "empty repair present")."""
    return Candidate(
        bucket_key=EMPTY_REPAIR_BUCKET_KEY,
        generator_id=EMPTY_REPAIR_GENERATOR_ID,
        generator_version=GENERATOR_VERSION_V1,
        corrections=(),
    )


def _candidate_sort_key(c: Candidate) -> tuple:
    return (
        c.bucket_key,
        c.generator_id,
        c.generator_version,
        tuple(
            (rc.action, rc.claim_id, rc.source_claim_id or "")
            for rc in c.corrections
        ),
    )


def _proposals_in_declaration_order(
    buckets: list[ConflictBucket],
    generators: Iterable[RepairGenerator],
    precedents: list[Precedent],
) -> list[tuple[Candidate, ...]]:
    """Each declared generator's proposals, IN DECLARATION ORDER. Pure.

    The one place the generator sequence is walked, so enumeration and selection
    cannot disagree about which generator proposed what.
    """
    return [g.propose(buckets, precedents) for g in generators]


def enumerate_candidates(
    buckets: Iterable[ConflictBucket],
    generators: Iterable[RepairGenerator] = DEFAULT_GENERATORS,
    precedents: Iterable[Precedent] = (),
) -> CandidateSet:
    """Enumerate the finite candidate set for one evaluation. Pure.

    The set is the always-present empty repair plus every declared generator's
    proposals, deduplicated and deterministically ordered. This is the recorded
    ``R(G)`` for the evaluation: a decisive bucket carrying a real value now has
    TWO non-empty candidates (retract the losers, or rewrite them to the
    winner's value), so the recorded set states every repair the engine could
    have taken and not only the one it did.
    """
    buckets = list(buckets)
    precedents = list(precedents)
    candidates: list[Candidate] = [_empty_candidate()]
    for proposed in _proposals_in_declaration_order(buckets, generators, precedents):
        candidates.extend(proposed)
    ordered = sorted(set(candidates), key=_candidate_sort_key)
    return CandidateSet(candidates=tuple(ordered))


def select_repairs(
    buckets: Iterable[ConflictBucket],
    precedents: Iterable[Precedent] = (),
) -> SelectedRepair:
    """Select repairs — the precedence full-repair limit (adjudication #9/#10). Pure.

    Per bucket, the tie-break over the enlarged candidate set is **declaration
    order**: take the candidate of the earliest generator in
    :data:`DEFAULT_GENERATORS` that proposed for this bucket, else the empty
    repair. Nothing is compared — no cost, no score, no preference read off the
    candidates — so the rule is ordering-stable and invariant under input
    shuffling, and adjudication #9's deferred scalar objective stays deferred.
    Retraction is declared first, so the observable selection is unchanged: a
    decisive winner with losers selects the retraction (its losers become
    ``distrusted``); a decisive sole claim selects the empty repair (nothing to
    retract); a contested / tie bucket selects the empty repair *and* retains
    its violation as :class:`UnresolvedConflict` with reason ``contested``.

    ``distrusted`` is accumulated from the SELECTED corrections' retraction
    targets — the claims the chosen repair drops — so it stays a property of the
    selection rather than of the (now larger) candidate set.

    Selection consults :func:`resolve_bucket` and never re-derives, so the
    engine's ranking-only-never-settles abstention is preserved exactly.
    """
    buckets = list(buckets)
    precedents = list(precedents)
    per_generator = _proposals_in_declaration_order(
        buckets, DEFAULT_GENERATORS, precedents
    )
    candidate_set = enumerate_candidates(buckets, DEFAULT_GENERATORS, precedents)
    # bucket_key -> the earliest-declared generator's candidate for it.
    first_by_bucket: dict[str, Candidate] = {}
    for proposed in per_generator:
        for candidate in proposed:
            first_by_bucket.setdefault(candidate.bucket_key, candidate)

    selected: list[SelectedCandidate] = []
    unresolved: list[UnresolvedConflict] = []
    distrusted: set[str] = set()

    for bucket in sorted(buckets, key=lambda b: b.bucket_key):
        resolution = resolve_bucket(bucket.signals, precedents)
        chosen = first_by_bucket.get(bucket.bucket_key)
        if chosen is None:
            # No generator proposed: decisive-but-nothing-to-retract (sole
            # claim), or contested/tie where every generator abstained. The
            # always-present empty repair is selected for this conflict.
            chosen = Candidate(
                bucket_key=bucket.bucket_key,
                generator_id=EMPTY_REPAIR_GENERATOR_ID,
                generator_version=GENERATOR_VERSION_V1,
                corrections=(),
            )
        selected.append(SelectedCandidate(
            bucket_key=bucket.bucket_key,
            generator_id=chosen.generator_id,
            generator_version=chosen.generator_version,
            basis=resolution.basis,
            outcome=resolution.outcome,
            corrections=chosen.corrections,
        ))
        distrusted.update(
            c.claim_id for c in chosen.corrections if c.action == ACTION_RETRACT
        )
        if resolution.outcome != OUTCOME_CANONICAL:
            # ranking-only lead or full tie -> violation retained, never settled.
            # Every generator abstains on such a bucket, so the empty repair is
            # what was chosen; assert that rather than assume it.
            if chosen.corrections:
                raise RepairSelectionError(
                    f"generator {chosen.generator_id!r} proposed a repair for "
                    f"contested bucket {bucket.bucket_key!r}; abstention is not "
                    f"a generator's to overrule"
                )
            unresolved.append(UnresolvedConflict(
                bucket_key=bucket.bucket_key,
                reason=OUTCOME_CONTESTED,
                basis=resolution.basis,
            ))

    selected.sort(key=lambda s: s.bucket_key)
    unresolved.sort(key=lambda u: u.bucket_key)
    return SelectedRepair(
        selected=tuple(selected),
        distrusted_claim_ids=tuple(sorted(distrusted)),
        asserted_claim_ids=(),
        unresolved=tuple(unresolved),
        candidate_set=candidate_set,
    )
