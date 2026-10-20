"""The precedence-arm PLAN: which extracted claims get which decisive-signal
stamps, and the EXPECTED winner per targeted bucket (synth-precedence-arms).

CORE module (substrate-free): it computes a pure, deterministic plan over the
sealed manifest surfaces (trace + patients) and the arm declaration, so the plan
is sealed into the manifest and every witness checks engine output against SEALED
gold rather than re-deriving the ranking logic. The RUNNER (runner.py) reads the
plan back and applies the stamps to the extracted claim facts at ingest, and
mints the operator PRECEDENT nodes — those touch substrate ``NodeFact`` objects
and so cannot live here.

The plan populates each of the five DECISIVE precedence bases from a controlled
bucket:

* ``isolate:<signal>`` — one bucket where that signal is the ONLY discriminator
  (a 1-vs-1 conflict with equal corroboration, so withholding the signal leaves
  it CONTESTED — the negative-arm control);
* ``corroborate`` — an UNSTAMPED majority bucket, settled by
  ``dependency_adjusted_support`` alone (the fifth basis, reusing the majority
  machinery);
* ``disagree:<hi>_over_<lo>`` — one bucket per ADJACENT decisive pair where the
  two signals favour DIFFERENT values; the run must show the higher-priority
  signal winning, and a transposition of the two must break it.

Every v1 arm stamps at ingest, so the rendered corpus is byte-identical between
an arm and its signal-withheld negative — the documents never carry the signals,
only the ingested claims do.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from okg.bench.synth.config import (
    SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE,
    SIGNAL_DECLARED_SOURCE_AUTHORITY,
    SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT,
    SIGNAL_EXPLICIT_SUPERSESSION,
    SIGNAL_OPERATOR_PRECEDENT,
)
from okg.bench.synth.prf import SynthPrf

ARM_PLAN_SCHEMA = "okg.synth.precedence-arm-plan.v1"

# Ingest-stamp values, fixed so the plan is deterministic and legible. ISO dates
# compare as strings (the engine's rule); authority is an int cast; is_superseded
# is the string 'true' (the engine's ``attrs->>'is_superseded' = 'true'`` test);
# the precedent source_class varies from the deterministic lane's default.
_DATE_EARLY = "2000-01-01"
_DATE_LATE = "2030-01-01"
_DATE_SINGLE = "2020-01-01"
_AUTHORITY_HIGH = "9"
_AUTHORITY_LOW = "1"
_AUTHORITY_SINGLE = "5"
_SUPERSEDED = "true"
_PRECEDENT_SOURCE_CLASS = "specialist_note"
DEFAULT_SOURCE_CLASS = "wiki"  # ClaimCorpusSource.source_class default

# The four adjacent DECISIVE pairs, highest-priority first, in _KEY_SIGNAL order.
ADJACENT_DECISIVE_PAIRS = (
    (SIGNAL_OPERATOR_PRECEDENT, SIGNAL_EXPLICIT_SUPERSESSION),
    (SIGNAL_EXPLICIT_SUPERSESSION, SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE),
    (SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE, SIGNAL_DECLARED_SOURCE_AUTHORITY),
    (SIGNAL_DECLARED_SOURCE_AUTHORITY, SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT),
)

# Attr-backed decisive signals, in _KEY_SIGNAL priority order (for isolation).
_ISOLATE_ATTR_SIGNALS = (
    SIGNAL_OPERATOR_PRECEDENT,
    SIGNAL_EXPLICIT_SUPERSESSION,
    SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE,
    SIGNAL_DECLARED_SOURCE_AUTHORITY,
)


class ArmPlanError(ValueError):
    """The requested arm cannot be realized against this corpus (fail-closed)."""


def _slug(text: str) -> str:
    """The one slug rule (equals claim_corpus_source._slug / synth_slug)."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _bucket_claims(
    trace: Sequence[Mapping[str, Any]], patients: Mapping[str, Mapping[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    """bucket_key -> [{claim_id, doc, value, corrupted, fact_id}], one claim per
    rendering.

    A claim id is ``claim:{doc}:{pred}:{entity}:{occurrence}`` (the grammar the
    deterministic parser mints). Occurrence is assigned in trace order within
    ``(doc, pred, entity)`` — matching the parser's line-order counting — so a
    fact rendered twice inside one document keys distinctly. ``fact_id`` rides
    along so the provenance plan can group a corruption's corrupted renderings by
    their shared target fact without re-deriving the occurrence keying.
    """
    occ: dict[tuple[str, str, str], int] = {}
    out: dict[str, list[dict[str, Any]]] = {}
    for row in trace:
        pred = _slug(row["predicate_label"])
        entity = patients[row["fact_id"].split(":")[1]]["entity"]
        doc = row["doc"]
        key = (doc, pred, entity)
        occurrence = occ.get(key, 0)
        occ[key] = occurrence + 1
        bucket = f"{pred}:{entity}"
        out.setdefault(bucket, []).append({
            "claim_id": f"claim:{doc}:{pred}:{entity}:{occurrence}",
            "doc": doc,
            "value": row["surface_value"],
            "corrupted": bool(row["corrupted"]),
            "fact_id": row["fact_id"],
        })
    return out


def _classify(claims: Sequence[Mapping[str, Any]]) -> str | None:
    """"one_vs_one" | "majority" | None. Requires exactly two distinct values,
    every claim in a distinct document (occurrence 0), and — for one_vs_one —
    equal support so withholding a stamp leaves the bucket a genuine tie."""
    docs = [c["doc"] for c in claims]
    if len(set(docs)) != len(docs):
        return None  # a repeated occurrence in one doc — skip (keeps ids simple)
    counts: dict[str, int] = {}
    for c in claims:
        counts[c["value"]] = counts.get(c["value"], 0) + 1
    if len(counts) != 2:
        return None
    lo, hi = sorted(counts.values())
    if lo == hi:
        return "one_vs_one"
    return "majority"


def _majority_minority(
    claims: Sequence[Mapping[str, Any]],
) -> tuple[str, list[dict], dict]:
    """(majority_value, majority_claims, minority_claim) for a majority bucket."""
    counts: dict[str, int] = {}
    for c in claims:
        counts[c["value"]] = counts.get(c["value"], 0) + 1
    majority_value = max(counts, key=lambda v: (counts[v], v))
    majority = [c for c in claims if c["value"] == majority_value]
    minority = next(c for c in claims if c["value"] != majority_value)
    return majority_value, majority, minority


def _isolate_target(signal: str, bucket: str, claims: Sequence[Mapping[str, Any]]):
    """A 1-vs-1 bucket where ``signal`` is the ONLY discriminator, winner = the
    lower-claim-id side A. The negative arm withholds the stamp → CONTESTED."""
    a, b = sorted(claims, key=lambda c: c["claim_id"])[:2]
    stamps: dict[str, dict[str, str]] = {}
    precedents: list[dict[str, str]] = []
    if signal == SIGNAL_OPERATOR_PRECEDENT:
        stamps[a["claim_id"]] = {"source_class": _PRECEDENT_SOURCE_CLASS}
        precedents.append({
            "node_id": f"precedent:isolate:{bucket}",
            "prefer_source": _PRECEDENT_SOURCE_CLASS,
            "scope": "",
            "sequence": "0",
        })
    elif signal == SIGNAL_EXPLICIT_SUPERSESSION:
        stamps[b["claim_id"]] = {"is_superseded": _SUPERSEDED}  # B loses → A wins
    elif signal == SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE:
        stamps[a["claim_id"]] = {"effective_date": _DATE_SINGLE}
    elif signal == SIGNAL_DECLARED_SOURCE_AUTHORITY:
        stamps[a["claim_id"]] = {"source_authority": _AUTHORITY_SINGLE}
    else:  # pragma: no cover - guarded by caller
        raise ArmPlanError(f"no ingest-stamp isolation for signal {signal!r}")
    return {
        "role": f"isolate:{signal}",
        "bucket": bucket,
        "shape": "one_vs_one",
        "stamps": stamps,
        "precedents": precedents,
        "expected_outcome": "canonical",
        "expected_basis": signal,
        "expected_winner_value": a["value"],
        "winner_claim_id": a["claim_id"],
    }


def _corroborate_target(bucket: str, claims: Sequence[Mapping[str, Any]]):
    """An UNSTAMPED majority bucket — settled by dependency_adjusted_support."""
    majority_value, majority, _minority = _majority_minority(claims)
    winner = max(majority, key=lambda c: c["claim_id"])  # RANK tiebreak: id DESC
    return {
        "role": "corroborate",
        "bucket": bucket,
        "shape": "majority",
        "stamps": {},
        "precedents": [],
        "expected_outcome": "canonical",
        "expected_basis": SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT,
        "expected_winner_value": majority_value,
        "winner_claim_id": winner["claim_id"],
    }


def _disagree_target(hi: str, lo: str, bucket: str, claims: Sequence[Mapping[str, Any]]):
    """A bucket where ``hi`` and ``lo`` favour DIFFERENT values; ``hi`` must win.

    The higher-priority signal is stamped to favour winner A; the lower-priority
    signal is stamped to favour the rival B, so a transposition of the two in the
    precedence key flips the outcome to B (the mutation non-vacuity proof).
    """
    stamps: dict[str, dict[str, str]] = {}
    precedents: list[dict[str, str]] = []

    if lo == SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT:
        # source_authority (hi) vs corroboration (lo): a MAJORITY bucket. The
        # minority (corrupted) claim gets high authority and must beat the
        # majority value that corroboration favours.
        majority_value, _majority, minority = _majority_minority(claims)
        stamps[minority["claim_id"]] = {"source_authority": _AUTHORITY_HIGH}
        return {
            "role": f"disagree:{hi}_over_{lo}",
            "bucket": bucket,
            "shape": "majority",
            "stamps": stamps,
            "precedents": precedents,
            "expected_outcome": "canonical",
            "expected_basis": hi,
            "expected_winner_value": minority["value"],
            "winner_claim_id": minority["claim_id"],
            "lower_would_prefer_value": majority_value,
        }

    a, b = sorted(claims, key=lambda c: c["claim_id"])[:2]  # A wins on hi
    a_stamp: dict[str, str] = {}
    b_stamp: dict[str, str] = {}
    if hi == SIGNAL_OPERATOR_PRECEDENT:
        a_stamp["source_class"] = _PRECEDENT_SOURCE_CLASS
        precedents.append({
            "node_id": f"precedent:disagree:{bucket}",
            "prefer_source": _PRECEDENT_SOURCE_CLASS,
            "scope": "",
            "sequence": "0",
        })
    elif hi == SIGNAL_EXPLICIT_SUPERSESSION:
        b_stamp["is_superseded"] = _SUPERSEDED  # B superseded → A wins on hi
    elif hi == SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE:
        a_stamp["effective_date"] = _DATE_LATE
        b_stamp["effective_date"] = _DATE_EARLY
    else:  # pragma: no cover
        raise ArmPlanError(f"no disagreement stamp for hi signal {hi!r}")

    if lo == SIGNAL_EXPLICIT_SUPERSESSION:
        a_stamp["is_superseded"] = _SUPERSEDED  # A superseded → B wins on lo
    elif lo == SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE:
        b_stamp["effective_date"] = _DATE_LATE  # B later → B wins on lo
        a_stamp.setdefault("effective_date", _DATE_EARLY)
    elif lo == SIGNAL_DECLARED_SOURCE_AUTHORITY:
        b_stamp["source_authority"] = _AUTHORITY_HIGH  # B higher → B wins on lo
        a_stamp["source_authority"] = _AUTHORITY_LOW
    else:  # pragma: no cover
        raise ArmPlanError(f"no disagreement stamp for lo signal {lo!r}")

    if a_stamp:
        stamps[a["claim_id"]] = a_stamp
    if b_stamp:
        stamps[b["claim_id"]] = b_stamp
    return {
        "role": f"disagree:{hi}_over_{lo}",
        "bucket": bucket,
        "shape": "one_vs_one",
        "stamps": stamps,
        "precedents": precedents,
        "expected_outcome": "canonical",
        "expected_basis": hi,
        "expected_winner_value": a["value"],
        "winner_claim_id": a["claim_id"],
        "lower_would_prefer_value": b["value"],
    }


# -- the poisoning selection (synth-authority-poisoning) --------------------


def _poison_eligibility(
    claims: Sequence[Mapping[str, Any]],
) -> tuple[str, str] | None:
    """``(corrupted_value, clean_value)`` if this bucket is poisonable, else None.

    A bucket is poisonable by a wrong-authority stamp iff it carries >= 1 corrupted
    claim and >= 1 clean claim, the corrupted claims all share ONE value distinct
    from the clean value, and that corrupted value is NOT already a strict majority
    (``len(corrupted) <= len(clean)``). The last condition is what makes the flip
    the POISON and not a coincidence: absent the stamp a 1-vs-1 stays a CONTESTED
    tie and a corrupted-minority majority settles CANONICALLY on the CLEAN value, so
    only the authority stamp can carry the corrupted value to a canonical win. A
    corrupted-majority bucket already mis-repairs on corroboration, so poisoning it
    would credit the poison for a flip it did not cause — excluded here.
    """
    corrupted = [c for c in claims if c["corrupted"]]
    clean = [c for c in claims if not c["corrupted"]]
    if not corrupted or not clean:
        return None
    corrupted_values = {c["value"] for c in corrupted}
    clean_values = {c["value"] for c in clean}
    if len(corrupted_values) != 1 or len(clean_values) != 1:
        return None
    (corrupted_value,) = corrupted_values
    (clean_value,) = clean_values
    if corrupted_value == clean_value:
        return None  # the corruption did not change the value — no conflict
    if len(corrupted) > len(clean):
        return None  # corrupted is the strict majority — not a poison-caused flip
    return corrupted_value, clean_value


def _poison_target(
    bucket: str,
    claims: Sequence[Mapping[str, Any]],
    corrupted_value: str,
    clean_value: str,
) -> dict[str, Any]:
    """Stamp ``declared_source_authority`` HIGH on the CORRUPTED claim(s), so the
    engine settles the bucket canonically on the corrupted value — the FLIP. The
    expected outcome is a mis-repair (winner ≢ clean) on basis
    ``declared_source_authority``; ``lower_would_prefer_value`` records the clean
    value the bucket would settle on absent the poison (the by-basis witness pins
    that a flip on any OTHER basis is not the poison)."""
    corrupted = [c for c in claims if c["corrupted"]]
    stamps = {c["claim_id"]: {"source_authority": _AUTHORITY_HIGH} for c in corrupted}
    shape = "one_vs_one" if len(corrupted) == len(claims) - len(corrupted) else "majority"
    return {
        "role": f"poison:{bucket}",
        "bucket": bucket,
        "shape": shape,
        "stamps": stamps,
        "precedents": [],
        "poison": True,
        "expected_outcome": "canonical",
        "expected_basis": SIGNAL_DECLARED_SOURCE_AUTHORITY,
        "expected_winner_value": corrupted_value,
        "lower_would_prefer_value": clean_value,
    }


# -- the rescue selection (concord-system-stress, req.stress.precedence) -----


def _rescue_eligibility(
    claims: Sequence[Mapping[str, Any]],
) -> tuple[str, str] | None:
    """``(corrupted_value, clean_value)`` if this bucket is RESCUABLE, else None.

    The exact COMPLEMENT of :func:`_poison_eligibility`. A bucket is rescuable
    iff it carries >= 1 corrupted and >= 1 clean claim, each side shares one
    value, the values differ, and the corrupted value IS a strict majority
    (``len(corrupted) > len(clean)``) — the condition poisoning excludes.

    That exclusion is why this arm has to exist. Poisoning skips these buckets
    because they already mis-repair on corroboration, so a wrong-authority
    stamp there would take credit for a flip it did not cause. But those are
    precisely the failure this program is chasing: the corrupted-majority case
    measured as settled wrongly 20 times out of 20. Absent a stamp the engine
    settles them canonically on the CORRUPTED value, so only a correct
    authority stamp can carry the CLEAN value to a canonical win — which makes
    the rescue attributable to authority in the same way the flip is
    attributable to the poison.
    """
    corrupted = [c for c in claims if c["corrupted"]]
    clean = [c for c in claims if not c["corrupted"]]
    if not corrupted or not clean:
        return None
    corrupted_values = {c["value"] for c in corrupted}
    clean_values = {c["value"] for c in clean}
    if len(corrupted_values) != 1 or len(clean_values) != 1:
        return None
    (corrupted_value,) = corrupted_values
    (clean_value,) = clean_values
    if corrupted_value == clean_value:
        return None  # the corruption did not change the value — no conflict
    if len(corrupted) <= len(clean):
        return None  # not a corrupted majority — that is the poison arm's pool
    return corrupted_value, clean_value


def _rescue_target(
    bucket: str,
    claims: Sequence[Mapping[str, Any]],
    corrupted_value: str,
    clean_value: str,
) -> dict[str, Any]:
    """Stamp ``declared_source_authority`` HIGH on the CLEAN claim(s), so the
    engine settles canonically on the clean value DESPITE being outvoted — the
    RESCUE. Mirrors :func:`_poison_target` field for field.

    ``lower_would_prefer_value`` records the corrupted value the bucket would
    settle on absent the stamp, which is what makes the by-basis witness able
    to refuse a rescue credited to a coincidence: a bucket that settles on the
    clean value for any reason OTHER than authority is not a rescue.
    """
    clean = [c for c in claims if not c["corrupted"]]
    stamps = {c["claim_id"]: {"source_authority": _AUTHORITY_HIGH} for c in clean}
    return {
        "role": f"rescue:{bucket}",
        "bucket": bucket,
        "shape": "majority",
        "stamps": stamps,
        "precedents": [],
        "rescue": True,
        "expected_outcome": "canonical",
        "expected_basis": SIGNAL_DECLARED_SOURCE_AUTHORITY,
        "expected_winner_value": clean_value,
        "lower_would_prefer_value": corrupted_value,
    }


def build_arm_plan(
    *,
    admits: Sequence[str],
    surface: Mapping[str, str],
    trace: Sequence[Mapping[str, Any]],
    patients: Mapping[str, Mapping[str, Any]],
    seed: str,
    exclude_buckets: frozenset[str] = frozenset(),
    role_redundancy: int = 1,
    poison_rate: float = 0.0,
    rescue_rate: float = 0.0,
) -> dict[str, Any]:
    """The sealed arm plan. Deterministic over the sorted conflicted buckets; a
    stable seed provenance is recorded. Fail-closed when the corpus lacks enough
    conflicted buckets of the shape a role needs — never a silent under-plan.

    Returns ``{schema, admits, surface, seed, targets: [...]}``; ``targets`` is
    empty for the default (admits-empty) arm, so the manifest of a default run is
    byte-identical except for this small block.

    ``exclude_buckets`` are conflicted buckets the arm must NOT target — the
    composition seam (task.complete.capstone). A relational Σ-kind corruption
    (incompatible_pair / linked_pair / unresolved_key) reads its detection off a
    finding CHANNEL that an ingest stamp would flip (a stamp forces a canonical
    verdict, so an unresolved_key would stop reading not_comparable and its recall
    would silently zero), and a majority-visibility corruption is the run's
    realized-majority mis-repair evidence, which must be settled by corroboration
    ALONE, never arm-forced. The default (empty) leaves the single-axis arm
    manifests byte-identical, so no existing golden moves.

    ``poison_rate`` r (synth-authority-poisoning) is the ADVERSARIAL axis: over the
    ELIGIBLE conflicted buckets LEFT after the roles are seated (poisonable per
    :func:`_poison_eligibility`), a PRF-chosen fraction r gets
    ``declared_source_authority`` stamped on the CORRUPTED claim — a wrong-authority
    stamp that flips the bucket to the corrupted value. The eligible pool is shuffled
    ONCE (seeded by the run seed, INDEPENDENT of r), and the first
    ``round(r * len(eligible))`` are poisoned, so a lower r poisons a SUBSET of a
    higher r's buckets and the flip curve is monotone by construction. The plan
    records ``poison_rate``, the sorted ``poison_eligible`` pool, and the sorted
    ``poisoned_buckets`` for the sealed flip report. r = 0 poisons nothing.
    """
    admits = tuple(admits)
    admitted = set(admits)
    bucket_claims = _bucket_claims(trace, patients)

    one_vs_one: list[str] = []
    majority: list[str] = []
    for bucket in sorted(bucket_claims):
        if bucket in exclude_buckets:
            continue
        shape = _classify(bucket_claims[bucket])
        if shape == "one_vs_one":
            one_vs_one.append(bucket)
        elif shape == "majority":
            majority.append(bucket)

    def _take(pool: list[str], role: str) -> str:
        if not pool:
            raise ArmPlanError(
                f"precedence arm {sorted(admitted)}: no unused conflicted bucket "
                f"left for role {role!r}; the corpus has too few conflicts of the "
                "needed shape. Raise the corruption count or redundancy."
            )
        return pool.pop(0)

    reps = max(1, int(role_redundancy))
    targets: list[dict[str, Any]] = []
    # Isolation of each admitted attr-backed signal (1-vs-1), ``reps`` buckets each
    # so a basis proven on several buckets survives extraction attrition.
    for signal in _ISOLATE_ATTR_SIGNALS:
        if signal in admitted:
            for _ in range(reps):
                bucket = _take(one_vs_one, f"isolate:{signal}")
                targets.append(_isolate_target(signal, bucket, bucket_claims[bucket]))
    # Isolation of corroboration (unstamped majority).
    if SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT in admitted:
        for _ in range(reps):
            bucket = _take(majority, "corroborate")
            targets.append(_corroborate_target(bucket, bucket_claims[bucket]))
    # ``reps`` disagreement buckets per adjacent decisive pair BOTH signals admit.
    for hi, lo in ADJACENT_DECISIVE_PAIRS:
        if hi in admitted and lo in admitted:
            pool = majority if lo == SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT else one_vs_one
            for _ in range(reps):
                bucket = _take(pool, f"disagree:{hi}_over_{lo}")
                targets.append(_disagree_target(hi, lo, bucket, bucket_claims[bucket]))

    # The poisoning selection: over the conflicted buckets LEFT after the roles are
    # seated, the poisonable ones (a corrupted claim that is not a strict majority)
    # are the eligible pool; a PRF-chosen fraction r of them gets a wrong-authority
    # stamp on the corrupted claim. Shuffled once (seed-keyed, r-independent) so the
    # poisoned set nests as r grows and the flip curve is monotone by construction.
    rate = max(0.0, min(1.0, float(poison_rate)))
    eligible: list[str] = []
    poison_values: dict[str, tuple[str, str]] = {}
    for bucket in sorted(set(one_vs_one) | set(majority)):
        elig = _poison_eligibility(bucket_claims[bucket])
        if elig is not None:
            eligible.append(bucket)
            poison_values[bucket] = elig
    eligible.sort()
    n_poison = round(rate * len(eligible))
    poisoned = sorted(SynthPrf(seed).shuffled(eligible, "poison")[:n_poison])
    for bucket in poisoned:
        corrupted_value, clean_value = poison_values[bucket]
        targets.append(
            _poison_target(bucket, bucket_claims[bucket], corrupted_value, clean_value)
        )

    # The RESCUE selection: the poison arm's complement. Over the conflicted
    # buckets left after the roles are seated, the rescuable ones (a corrupted
    # STRICT MAJORITY — exactly what poisoning excludes) are the eligible pool;
    # a PRF-chosen fraction gets a CORRECT authority stamp on the clean claim.
    # Shuffled once under its own key, rate-independent, so the rescued set
    # nests as the rate grows and the rescue curve is monotone by construction,
    # mirroring the poison curve it is read against.
    rescue = max(0.0, min(1.0, float(rescue_rate)))
    rescue_eligible: list[str] = []
    rescue_values: dict[str, tuple[str, str]] = {}
    already = {t["bucket"] for t in targets}
    for bucket in sorted(set(one_vs_one) | set(majority)):
        if bucket in already:
            continue
        elig = _rescue_eligibility(bucket_claims[bucket])
        if elig is not None:
            rescue_eligible.append(bucket)
            rescue_values[bucket] = elig
    rescue_eligible.sort()
    n_rescue = round(rescue * len(rescue_eligible))
    rescued = sorted(SynthPrf(seed).shuffled(rescue_eligible, "rescue")[:n_rescue])
    for bucket in rescued:
        corrupted_value, clean_value = rescue_values[bucket]
        targets.append(
            _rescue_target(bucket, bucket_claims[bucket], corrupted_value, clean_value)
        )

    plan: dict[str, Any] = {
        "schema": ARM_PLAN_SCHEMA,
        "admits": list(admits),
        "surface": {k: surface[k] for k in sorted(surface)},
        "seed": seed,
        "poison_rate": rate,
        "poison_eligible": eligible,
        "poisoned_buckets": poisoned,
        "targets": targets,
    }
    # The rescue axis is emitted ONLY when it is on. The plan is sealed into the
    # manifest and the manifest is digested, so emitting an off axis would move
    # every manifest digest in the repository and invalidate every sealed row
    # and golden that pins one — breaking this function's own guarantee that a
    # default (admits-empty) arm leaves a manifest byte-identical. A run that
    # USES the axis carries it, and digests differently, which is correct.
    if rescue > 0.0 or rescued:
        plan["rescue_rate"] = rescue
        plan["rescue_eligible"] = rescue_eligible
        plan["rescued_buckets"] = rescued
    return plan
