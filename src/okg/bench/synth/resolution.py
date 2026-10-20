"""Resolution-row loader and the repair-reachability report.

The existing pipeline-state loader (``okg.bench.injection.load_pipeline_state``)
reads only ``row_kind='finding'`` rows, so the REPAIR predicate — did the
engine pick the right value? — is undefined without this module.

This is a RUNNER-layer module: it reads a Chronos checkout and therefore
imports substrate code by design (the synth CORE stays substrate-free;
see the purity boundary in the kg-first-synthetic-corpus design §3).

Verified engine behaviour this module encodes (real publish, 40 patients /
28 corruptions): ``bucket_resolution`` rows are emitted ONLY for conflicting
buckets; 22 of 824 effective buckets produced one, and all 22 resolved
``contested`` with ``basis='tie'``. A corruption rendered exactly once
produces NO resolution row at all — it is UN-ADJUDICATED, neither detected
nor repaired. ``member_claim_ids`` is NULL on these rows; the members live
in ``ranked_claim_ids``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from okg.bench.synth.corruption import (
    CLASS_BACKUP_CHECK,
    CONSTRAINT_KINDS,
    KIND_LINKED_PAIR,
    KIND_UNRESOLVED_KEY,
    PARTITION_CROSS_DOC,
    PARTITION_INTERNAL_ONLY,
    constraint_kind_for,
)
from okg.bench.synth.provenance import (
    FLAVOR_COPY,
    FLAVOR_INDEPENDENT,
    FLAVORS,
    PROVENANCE_GROUP_ATTR,
)

ROW_KIND_RESOLUTION = "bucket_resolution"
OUTCOME_CANONICAL = "canonical"
OUTCOME_CONTESTED = "contested"
BASIS_TIE = "tie"

# Per-corruption adjudication states (mutually exclusive, exhaustive).
STATE_REPAIRED = "repaired"            # canonical AND winner ≡ clean
STATE_MIS_REPAIRED = "mis_repaired"    # canonical AND winner ≢ clean
STATE_DETECTED_ONLY = "detected_only"  # contested (noticed, not resolved)
STATE_UNADJUDICATED = "unadjudicated"  # no resolution row at all

# --- terminal funnel states (req.attrib.states) ---------------------------
#
# adjudicate() answers "did the ENGINE settle this bucket?" but folds four
# distinct upstream fates into one ``unadjudicated`` label: the injection was
# lost at extraction, laundered at extraction, its counterpart was lost, or it
# survived whole and the engine simply produced no finding. The funnel splits
# those apart so a loss is charged to the STAGE that caused it — extraction A,
# detection B, or repair C — rather than always debiting the engine. The three
# adjudicated states below are re-used verbatim; the funnel only refines the
# ``unadjudicated`` residue.
STATE_MISSED_AT_EXTRACTION = "missed_at_extraction"        # no corrupted claim in gold bucket
STATE_LAUNDERED_AT_EXTRACTION = "laundered_at_extraction"  # claim present, corrupted value gone
# Decided from gold + extraction ALONE, ahead of any engine output, so it is
# reachable even when the engine emitted something on the bucket (req.remediate.
# denominator). See _terminal_state's ordering contract.
STATE_COUNTERPART_LOST = "counterpart_lost"                # survived, detection material lost
STATE_NOT_DETECTED = "not_detected"                        # survived+expressible, engine silent
# RESERVED for synth-constraint-coverage's corrections (retract that pulls the
# claim out of C_eff, OR override that rewrites the corrupted value): filing an
# override as not_detected would debit the engine for an operator action. The
# vocabulary and the exhaustiveness check admit it now so the sibling child
# lands without editing this contract; its reachability witnesses land there.
STATE_REMOVED_FROM_SCOPE = "removed_from_scope"

# Injection order: inject → extract → detect → repair, then the reserved
# retract/override state last (always zero until corrections exist). The golden
# reads top-to-bottom as the funnel.
FUNNEL_STATES = (
    STATE_MISSED_AT_EXTRACTION,
    STATE_LAUNDERED_AT_EXTRACTION,
    STATE_COUNTERPART_LOST,
    STATE_NOT_DETECTED,
    STATE_DETECTED_ONLY,
    STATE_MIS_REPAIRED,
    STATE_REPAIRED,
    STATE_REMOVED_FROM_SCOPE,
)

# The engine produced a finding for these (the conflict was noticed).
_DETECTED_STATES = frozenset(
    {STATE_DETECTED_ONLY, STATE_MIS_REPAIRED, STATE_REPAIRED}
)
# The conflict SURVIVED extraction as an engine-answerable question: the
# corrupted value AND the material its detection needs both landed. missed and
# laundered lost the corrupted value; counterpart_lost lost the material; the
# rest are a fair charge against detection.
_SURVIVED_CONFLICT_STATES = _DETECTED_STATES | {STATE_NOT_DETECTED}
# Extraction did NOT carry the corrupted surface into the gold bucket in these
# two states: missed lost the claim, laundered kept the claim but dropped the
# injected value. INJECTION SURVIVAL is the complement within extraction, named
# once here so the per-class survival block and the funnel can never disagree on
# what "survived" means (req.attrib.survival — one implementation, no parallel
# join). counterpart_lost is NOT here: the corrupted value itself survived, only
# its detection material did not.
_EXTRACTION_LOSS_STATES = frozenset(
    {STATE_MISSED_AT_EXTRACTION, STATE_LAUNDERED_AT_EXTRACTION}
)
# Only these partitions have an ENGINE-visible conflict by construction, so
# conditional detection recall draws its denominator from them alone.
# world_knowledge keeps its own denominator (a domain judge, not the engine);
# undetectable_by_construction has no detector lane in v1.
_CONDITIONAL_PARTITIONS = frozenset({PARTITION_CROSS_DOC, PARTITION_INTERNAL_ONLY})

# The falsification's scope condition has two halves.
#
# (1) These six ranking signals must be ABSENT. Clinical notes carry dates and
#     `effective_date` is DECISIVE, so an extractor that helpfully stamps a
#     note date would resolve buckets canonically and silently destroy the
#     result while looking like realism.
PRECEDENCE_SIGNAL_ATTRS = frozenset({
    "is_superseded", "effective_date", "source_authority",
    "independent_support", "document_recency", "maintenance_rank",
})
# (2) `source_class` is NOT a ranking signal — it is the key an operator
#     PRECEDENT matches on. It may be present, but it must be UNIFORM across
#     claims: a precedent can only discriminate if claims differ on it.
SOURCE_CLASS_ATTR = "source_class"

# --- per-arm scope condition (req.arms.declared) --------------------------
#
# The falsification's GLOBAL "all six absent" invariant (PRECEDENCE_SIGNAL_ATTRS
# above) becomes a function of the ARM declaration: an arm ADMITS a set of
# decisive signals, and the scope condition asserts exactly the admitted signals'
# attrs MAY be present and the complement is ABSENT — so the corroboration-only
# arm keeps today's isolation (and today's witnesses pass unchanged) while an
# effective-date arm can admit dates without the condition firing. The condition
# stays LOUD: a signal attr appearing in an arm that did not declare it is a
# FAILURE naming the signal, because that is precisely the silent-realism failure
# the original suppression was written to prevent.

from okg.bench.synth.config import (  # noqa: E402
    SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE,
    SIGNAL_DECLARED_SOURCE_AUTHORITY,
    SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT,
    SIGNAL_EXPLICIT_SUPERSESSION,
    SIGNAL_OPERATOR_PRECEDENT,
)

# Each decisive signal that has a CLAIM-ATTR ingest surface, mapped to that attr.
# operator_precedent enters via source_class variation + operator PRECEDENT nodes
# (source_class is SOURCE_CLASS_ATTR, governed by the uniformity rule above, not a
# ranking attr), and dependency_adjusted_support is COMPUTED by the engine from
# the bucket shape (its independent_support attr is IGNORED, so stamping it is
# silent theatre) — neither has a stampable ranking attr, so neither appears here.
SIGNAL_INGEST_ATTR = {
    SIGNAL_EXPLICIT_SUPERSESSION: "is_superseded",
    SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE: "effective_date",
    SIGNAL_DECLARED_SOURCE_AUTHORITY: "source_authority",
}
# Reverse: every ranking attr → the signal name it belongs to, for LOUD messages.
# document_recency / maintenance_rank are ranking-only (never decisive, never
# admittable); independent_support is engine-computed (its attr is never a valid
# stamp). All three are therefore always in the forbidden complement.
_ATTR_SIGNAL = {
    "is_superseded": SIGNAL_EXPLICIT_SUPERSESSION,
    "effective_date": SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE,
    "source_authority": SIGNAL_DECLARED_SOURCE_AUTHORITY,
    "independent_support": SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT,
    "document_recency": "document_recency",
    "maintenance_rank": "maintenance_heuristic",
}


class ArmScopeError(ValueError):
    """An arm carried a precedence signal it did not declare (loud, named)."""


def admitted_signal_attrs(admits: Sequence[str]) -> frozenset[str]:
    """The ranking attrs an arm's admitted signals MAY carry at ingest."""
    return frozenset(
        SIGNAL_INGEST_ATTR[signal] for signal in admits if signal in SIGNAL_INGEST_ATTR
    )


def assert_arm_signal_scope(
    claim_facts: Any,
    admits: Sequence[str],
    *,
    provenance_admitted: bool = False,
    poisoned_claim_ids: frozenset[str] = frozenset(),
) -> None:
    """Per-arm scope condition. Assert each claim carries ONLY admitted signal
    attrs; an undeclared signal attr fails LOUDLY naming the signal.

    ``claim_facts`` is an iterable of ``consistency_claim`` facts (``.node_id`` +
    ``.attrs``). ``admits`` is the arm's admitted-signal names. When
    operator_precedent is NOT admitted, ``source_class`` must additionally be
    UNIFORM (the SOURCE_CLASS_ATTR rule) — a varying source_class lets an operator
    precedent settle a bucket the arm did not declare it studies.

    ``provenance_admitted`` (synth-provenance-diversity) declares the run stamps
    the DECLARED provenance axis (``provenance_group``). It is a first-class axis,
    NOT a ranking signal, so it is checked separately from PRECEDENCE_SIGNAL_ATTRS
    and never affects the ranking-signal logic: an undeclared ranking-signal stamp
    still fails loudly exactly as before. A ``provenance_group`` attr on a claim
    when the run did NOT declare the provenance axis is itself a LOUD failure — the
    same silent-realism guard, applied to the provenance axis: an undeclared
    provenance stamp could feed a future independence-weighting engine a signal the
    run never sanctioned.

    ``poisoned_claim_ids`` (synth-authority-poisoning) are the claims the sealed
    poison plan stamps a wrong ``source_authority`` on. The guard admits that attr
    on EXACTLY those claims and NOWHERE else: a poisoned claim may carry authority
    (that is the declared adversarial axis), but a ``source_authority`` on any
    non-poisoned claim of an arm that did not admit declared_source_authority still
    fails loudly. Precise by claim id, never a global relaxation, so the guard keeps
    catching a stray authority stamp even while a poison run is in flight.
    """
    admits = tuple(admits)
    allowed = admitted_signal_attrs(admits)
    forbidden_base = PRECEDENCE_SIGNAL_ATTRS - allowed
    poisoned_claim_ids = frozenset(poisoned_claim_ids)
    facts = list(claim_facts)
    for fact in facts:
        forbidden = forbidden_base
        if fact.node_id in poisoned_claim_ids:
            # the poison declares authority on THIS claim; it stays forbidden on
            # every other claim the arm did not admit it for.
            forbidden = forbidden - {SIGNAL_INGEST_ATTR[SIGNAL_DECLARED_SOURCE_AUTHORITY]}
        leaked = set(fact.attrs) & forbidden
        if leaked:
            signals = sorted(_ATTR_SIGNAL[attr] for attr in leaked)
            raise ArmScopeError(
                f"{fact.node_id}: carries precedence signal attr(s) "
                f"{sorted(leaked)} = signal(s) {signals} NOT admitted by this arm "
                f"(admits={sorted(admits)}). A stamped signal an arm did not "
                "declare settles buckets while looking like added realism — the "
                "exact silent-realism failure this scope condition guards."
            )
        if PROVENANCE_GROUP_ATTR in fact.attrs and not provenance_admitted:
            raise ArmScopeError(
                f"{fact.node_id}: carries the declared-axis attr "
                f"{PROVENANCE_GROUP_ATTR!r} but the run did not admit the "
                "provenance axis (provenance_admitted=False). The provenance group "
                "is a DECLARED axis, not a ranking signal, and an undeclared "
                "provenance stamp could feed a future independence-weighting engine "
                "a signal this run never sanctioned — the same silent-realism "
                "failure the scope condition guards."
            )
    if SIGNAL_OPERATOR_PRECEDENT not in admits and facts:
        classes = {fact.attrs.get(SOURCE_CLASS_ATTR) for fact in facts}
        if len(classes) > 1:
            raise ArmScopeError(
                f"source_class is not UNIFORM ({sorted(map(str, classes))}) but "
                f"this arm does not admit {SIGNAL_OPERATOR_PRECEDENT!r}: an "
                "operator precedent could settle a bucket the arm did not declare."
            )


def synth_slug(text: str) -> str:
    """The one slug rule (must equal claim_corpus_source._slug).

    Defined here so the scorer, the resolution adjudicator, and the witnesses
    all key buckets identically — divergent copies silently zero every lookup.
    """
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def corruption_bucket_map(manifest: Mapping[str, Any]) -> dict[str, str]:
    """corruption_id -> bucket key ("predicate:entity"), from sealed gold."""
    trace = manifest["trace"]
    patients = manifest["patients"]
    out: dict[str, str] = {}
    for site in manifest["gold"]["corruptions"]:
        entity = patients[site["fact_id"].split(":")[1]]["entity"]
        predicate = next(
            synth_slug(r["predicate_label"])
            for r in trace
            if r["fact_id"] == site["fact_id"] and r["doc"] == site["doc"]
        )
        out[site["corruption_id"]] = f"{predicate}:{entity}"
    return out


# --- THE detection predicate, stated once (req.honesty.channel-credit) -----
#
# The four Σ constraint kinds surface findings on DIFFERENT channels: functional,
# incompatible_pair and linked_pair conflicts are definite/possible; an
# unresolved key is NOT-COMPARABLE (the engine refuses to force a verdict). So a
# corruption is DETECTED iff the engine emitted a finding on ITS bucket on the
# channel set its GOLD constraint kind declares — never pooled, and never
# any-channel: a conflict finding on an unresolved_key bucket does NOT count. The
# funnel and per_kind_detection BOTH derive from this one predicate, so the two
# can never again disagree on whether the engine noticed a corruption.
_CONFLICT_CHANNELS = frozenset({"definite", "possible"})
_NOT_COMPARABLE_CHANNEL = "not_comparable"


def _kind_channels(kind: str) -> frozenset[str]:
    """The finding channel(s) a corruption of ``kind`` is detected on."""
    if kind == KIND_UNRESOLVED_KEY:
        return frozenset({_NOT_COMPARABLE_CHANNEL})
    return _CONFLICT_CHANNELS


def _channels_by_bucket(findings: Sequence[Any]) -> dict[str, frozenset[str]]:
    """bucket_key -> the set of channels the engine emitted a finding on there.

    The one join the detection predicate needs, built once from the finding rows
    so the funnel and per_kind_detection read the same engine surface.
    """
    out: dict[str, set[str]] = {}
    for finding in findings:
        out.setdefault(finding.bucket_key, set()).add(finding.channel)
    return {bucket: frozenset(channels) for bucket, channels in out.items()}


def bucket_detected_for_kind(
    channels_by_bucket: Mapping[str, frozenset[str]], bucket: str, gold_kind: str
) -> bool:
    """THE detection predicate: is a corruption of ``gold_kind`` on ``bucket``
    detected? True iff a finding sits on that bucket on a channel the GOLD kind
    declares — per-declared-channel, not any-channel."""
    return bool(channels_by_bucket.get(bucket, frozenset()) & _kind_channels(gold_kind))


def gold_constraint_kind(site: Mapping[str, Any]) -> str:
    """The Σ kind a sealed gold corruption targets (functional unless declared)."""
    return dict(site.get("params", {})).get("constraint_kind") or constraint_kind_for(
        site["corruption_class"]
    )


def _claim_bucket(claim_id: str) -> str | None:
    parsed = _claim_bucket_and_doc(claim_id)
    return parsed[0] if parsed is not None else None


def _empty_kind_cell() -> dict[str, dict[str, int]]:
    return {
        "end_to_end": {"denominator": 0, "detected": 0},
        "conditional": {"denominator": 0, "detected": 0},
    }


def per_kind_detection(
    manifest: Mapping[str, Any],
    stages: Mapping[str, Any],
    findings: Sequence[Any],
    finding_kinds: Mapping[str, str],
) -> dict[str, Any]:
    """Detection recall PER constraint kind, in the SAME two forms the aggregate
    funnel reports, plus the linked_pair cross-bucket witness.

    Derived entirely from the funnel's per-corruption states (``stages``), so the
    per-kind rows and the aggregate funnel can never disagree — they are the one
    detection predicate, sliced by kind. Each kind gets both denominators the
    funnel has:

    * ``end_to_end`` — every gold site of the kind (all partitions), detected =
      the sites whose terminal state is a detected state. This is the honest
      denominator an engine author reads for a per-constraint diagnosis; it no
      longer pools extraction loss with detection miss the way a raw
      finding-on-bucket count did.
    * ``conditional`` — only the sites whose conflict SURVIVED extraction
      (``_SURVIVED_CONFLICT_STATES``), so its misses are STRICTLY detection
      losses: the per-kind conditional misses sum to the funnel's not_detected
      count, because a survived-conflict site that is not detected is exactly a
      not_detected site.

    Every one of the four kinds appears in ``by_kind`` (both denominators 0 when
    the corpus declares no corruption of it) so a kind that never fires is a
    visible zero, never an omitted row. ``stages`` is the :func:`funnel` output;
    ``findings`` / ``finding_kinds`` are used ONLY for the cross-bucket witness.

    ``linked_cross_bucket`` records, per linked_pair finding, the (predicate,
    entity) buckets its two member claims occupy: a linked_pair finding is the
    only cross-bucket constraint, so its members MUST span two buckets, and this
    is what the witness asserts.
    """
    per_corruption = stages["per_corruption"]
    gold_kind: dict[str, str] = {
        site["corruption_id"]: gold_constraint_kind(site)
        for site in manifest["gold"]["corruptions"]
    }

    by_kind: dict[str, dict[str, dict[str, int]]] = {
        kind: _empty_kind_cell() for kind in CONSTRAINT_KINDS
    }
    for cid, state in per_corruption.items():
        kind = gold_kind[cid]
        cell = by_kind.setdefault(kind, _empty_kind_cell())
        detected = state in _DETECTED_STATES
        cell["end_to_end"]["denominator"] += 1
        if detected:
            cell["end_to_end"]["detected"] += 1
        if state in _SURVIVED_CONFLICT_STATES:
            cell["conditional"]["denominator"] += 1
            if detected:
                cell["conditional"]["detected"] += 1

    linked_cross: list[dict[str, Any]] = []
    for finding in findings:
        if finding_kinds.get(finding.finding_id) != KIND_LINKED_PAIR:
            continue
        member_buckets = sorted(
            {
                b
                for cid in finding.claim_ids
                for b in (_claim_bucket(cid),)
                if b is not None
            }
        )
        linked_cross.append(
            {"finding_id": finding.finding_id, "member_buckets": member_buckets}
        )
    linked_cross.sort(key=lambda row: row["finding_id"])

    return {
        "by_kind": {kind: by_kind[kind] for kind in sorted(by_kind)},
        "linked_cross_bucket": linked_cross,
    }


def corruption_renderings(manifest: Mapping[str, Any]) -> dict[str, int]:
    """corruption_id -> how many times its fact is rendered (effective k).

    A corruption on a fact rendered exactly once cannot conflict with
    anything, so the engine emits no resolution row for it.
    """
    effective_k = manifest.get("effective_k", {})
    return {
        site["corruption_id"]: int(effective_k.get(site["fact_id"], 0))
        for site in manifest["gold"]["corruptions"]
    }


@dataclass(frozen=True)
class BucketResolution:
    """One ``bucket_resolution`` row, with the winner's value joined in."""

    predicate: str
    bucket_key: str
    outcome: str
    basis: str
    winner_claim_id: str
    ranked_claim_ids: tuple[str, ...]
    winner_value: str | None

    @property
    def bucket(self) -> str:
        return f"{self.predicate}:{self.bucket_key}"

    @property
    def is_canonical(self) -> bool:
        return self.outcome == OUTCOME_CANONICAL


def load_resolutions(
    checkout: Any, claim_values: Mapping[str, str]
) -> tuple[BucketResolution, ...]:
    """Read every ``bucket_resolution`` row, joining the winner to its value.

    ``claim_values`` maps claim_id -> value (``PipelineState.claims``).
    """
    rows = checkout.query(
        "SELECT predicate, bucket_key, outcome, basis, winner_claim_id, "
        "ranked_claim_ids FROM okg.claim_projection WHERE row_kind = :kind "
        "ORDER BY predicate, bucket_key",
        {"kind": ROW_KIND_RESOLUTION},
    )
    out: list[BucketResolution] = []
    for row in rows:
        ranked_raw = row.get("ranked_claim_ids")
        ranked = tuple(json.loads(ranked_raw)) if ranked_raw else ()
        winner = str(row.get("winner_claim_id") or "")
        out.append(
            BucketResolution(
                predicate=str(row["predicate"]),
                bucket_key=str(row["bucket_key"]),
                outcome=str(row["outcome"]),
                basis=str(row.get("basis") or ""),
                winner_claim_id=winner,
                ranked_claim_ids=ranked,
                winner_value=claim_values.get(winner) if winner else None,
            )
        )
    return tuple(out)


def falsification_report(resolutions: Sequence[BucketResolution]) -> dict[str, Any]:
    """The repair-reachability result, computed from engine rows only.

    ``repair_reachable`` is False when the engine never resolves a bucket
    canonically — i.e. no decisive precedence signal separates the claims,
    so no amount of corroboration can settle one. That is a property of the
    ENGINE under these inputs, and the scorecard must present it as
    UNREACHABLE rather than as a measured zero.
    """
    if not resolutions:
        # "every row ties" and "zero canonical" are both VACUOUSLY true on an
        # empty set, so a run whose publish or alias table silently produced no
        # conflicting bucket would otherwise report UNREACHABLE. Refuse.
        raise ValueError(
            "cannot judge repair reachability from ZERO resolution rows: an "
            "empty set satisfies the falsification vacuously. Check that the "
            "corpus actually produced conflicting buckets (publish succeeded, "
            "alias/bucket keys match, corruptions have counterparts)."
        )
    by_pair: dict[str, int] = {}
    for res in resolutions:
        by_pair[f"{res.outcome}/{res.basis}"] = by_pair.get(f"{res.outcome}/{res.basis}", 0) + 1
    canonical = [r for r in resolutions if r.is_canonical]
    return {
        "resolution_rows": len(resolutions),
        "outcome_basis_distribution": dict(sorted(by_pair.items())),
        "canonical_resolutions": len(canonical),
        "repair_reachable": bool(canonical),
        "note": (
            "repair UNREACHABLE on this corpus: no bucket resolved "
            "canonically. The engine computes independent_support itself (a bucket-level count over "
            "C_eff) and settles a bucket when one value holds a strict "
            "support majority. Unreachability here is therefore a property "
            "of the CORPUS, not of the engine: every conflicted bucket is a "
            "1-vs-1 split with no majority to find. Report it as such, never "
            "as a measured repair rate"
        )
        if not canonical
        else "repair reachable: at least one bucket resolved canonically",
    }


def adjudicate(
    *,
    corruption_buckets: Mapping[str, str],
    clean_values: Mapping[str, str],
    resolutions: Sequence[BucketResolution],
    detected_buckets: frozenset[str],
    equivalent,
    exclude: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Per-corruption adjudication state + the repair tallies.

    ``corruption_buckets`` maps corruption_id -> bucket key; ``clean_values``
    maps corruption_id -> the CLEAN ground-truth value; ``equivalent(a, b)``
    is the DECLARED value equivalence (never an implicit rule).

    DETECTED and REPAIRED are independent: a contested bucket is detected and
    NOT repaired. A corruption whose bucket has no resolution row at all is
    UN-ADJUDICATED — invisible to the resolution surface — and is counted
    separately so it is never silently folded into "not repaired".

    ``exclude`` (synth-authority-poisoning) are corruption_ids kept OUT of this
    repair reading entirely — the POISONED buckets, whose adversarial wrong-authority
    stamp would otherwise inflate the UNPOISONED mis-repair tally. The poison flip is
    reported in its own block against its own denominator; excluding it here is what
    keeps the two axes orthogonal (the capstone's armed mis-repairs had to be
    separated by hand — this makes the separation structural). Default empty.
    """
    by_bucket = {r.bucket: r for r in resolutions}
    states: dict[str, str] = {}
    corruption_buckets = {
        cid: bucket
        for cid, bucket in corruption_buckets.items()
        if cid not in exclude
    }
    for corruption_id, bucket in sorted(corruption_buckets.items()):
        res = by_bucket.get(bucket)
        if res is None:
            states[corruption_id] = STATE_UNADJUDICATED
        elif not res.is_canonical:
            states[corruption_id] = STATE_DETECTED_ONLY
        elif res.winner_value is not None and equivalent(
            res.winner_value, clean_values[corruption_id]
        ):
            states[corruption_id] = STATE_REPAIRED
        else:
            states[corruption_id] = STATE_MIS_REPAIRED
    tally = {
        state: sum(1 for s in states.values() if s == state)
        for state in (
            STATE_REPAIRED, STATE_MIS_REPAIRED, STATE_DETECTED_ONLY, STATE_UNADJUDICATED
        )
    }
    return {
        "per_corruption": states,
        "tally": tally,
        "detected": sum(
            1 for cid, b in corruption_buckets.items() if b in detected_buckets
        ),
        "total": len(corruption_buckets),
    }


# -- the terminal-state funnel (req.attrib.states) -------------------------


def _claim_bucket_and_doc(claim_id: str) -> tuple[str, str] | None:
    """(bucket, doc) for a claim id, or None when it is not the one grammar.

    Claim ids are ``claim:{doc}:{predicate}:{entity}:{occurrence}`` — the shape
    claim_corpus_source mints and every scorer buckets on. Parsing here (not by
    guessing occurrences) is what keeps the funnel independent of how many times
    a fact was rendered.
    """
    parts = claim_id.split(":")
    if len(parts) != 5 or parts[0] != "claim":
        return None
    _claim, doc, predicate, entity, _occ = parts
    return f"{predicate}:{entity}", doc


def _fd_input_buckets(
    params: Mapping[str, str],
    predicate_by_fact: Mapping[str, str],
    patients: Mapping[str, Mapping[str, str]],
) -> tuple[frozenset[str], bool]:
    """Buckets the declared FD inputs would occupy, and whether all resolved.

    Returns ``(buckets, resolvable)``. ``resolvable`` is False when a declared
    input has no trace row (never rendered) or no known patient — either way its
    claim cannot be present, so the FD check cannot fire and the site's conflict
    is inexpressible. A derived_value corruption with no declared inputs is
    inexpressible too (there is nothing to recompute against), never silently
    treated as satisfied.
    """
    raw = params.get("input_fact_ids", "")
    input_ids = [i for i in raw.split(",") if i]
    if not input_ids:
        return frozenset(), False
    buckets: set[str] = set()
    for input_id in input_ids:
        predicate = predicate_by_fact.get(input_id)
        pid = input_id.split(":")[1] if input_id.count(":") >= 1 else ""
        entity = patients.get(pid, {}).get("entity")
        if predicate is None or entity is None:
            return frozenset(), False
        buckets.add(f"{predicate}:{entity}")
    return frozenset(buckets), True


def _detection_material_survived(
    *,
    corruption_class: str,
    partition: str,
    clean_value: str,
    corrupted_docs: frozenset[str],
    bucket_docs: Sequence[tuple[str, str]],
    fd_input_buckets: frozenset[str],
    fd_resolvable: bool,
    present_buckets: frozenset[str],
    equivalent,
) -> bool:
    """Is this corruption's conflict EXPRESSIBLE after extraction?

    A pure function of SEALED GOLD (class, partition, clean value, which docs the
    injection touched, the declared FD inputs) and the POST-EXTRACTION claims —
    it never reads engine output. That is the whole point: the answer decides
    whether the site belongs in the conditional detection denominator, and a
    denominator that moved with the engine's own output could not measure the
    engine (req.remediate.denominator).

    Class- and partition-aware, never global — a global predicate mislabels
    exactly the classes the partition system exists to separate.
    """
    clean_counterpart_survived = any(
        doc not in corrupted_docs and equivalent(v, clean_value)
        for doc, v in bucket_docs
    )
    if clean_counterpart_survived:
        return True
    if corruption_class == "derived_value":
        # The FD check needs its declared inputs, not a counterpart.
        return fd_resolvable and all(b in present_buckets for b in fd_input_buckets)
    if CLASS_BACKUP_CHECK.get(corruption_class, False):
        # A functional-predicate backup check fires on the own claim alone, which
        # already survived — so a miss here is a genuine engine miss, not a loss.
        return True
    if partition == PARTITION_CROSS_DOC:
        # A clean counterpart WAS rendered (that is what cross_doc means) and did
        # not survive extraction: the engine never had the conflict to find.
        return False
    # undetectable_by_construction or world_knowledge in isolation: no counterpart
    # was ever rendered, so nothing was LOST. The partition cross-tab keeps these
    # out of the detection-recall denominator.
    return True


def _terminal_state(
    *,
    corruption_class: str,
    partition: str,
    corrupted_value: str,
    clean_value: str,
    corrupted_docs: frozenset[str],
    bucket_docs: Sequence[tuple[str, str]],
    res: BucketResolution | None,
    detected: bool,
    fd_input_buckets: frozenset[str],
    fd_resolvable: bool,
    present_buckets: frozenset[str],
    equivalent,
) -> str:
    """The one funnel state for one corruption (design: total, fail-closed).

    ``bucket_docs`` are the ``(doc, value)`` pairs of the extracted claims that
    landed in this corruption's GOLD bucket. Bucket scoping is deliberate: a
    claim mis-keyed to a wrong in-vocabulary attribute, scope, or entity slug
    never reaches the gold bucket, so it registers here as an extraction loss —
    never as a detection miss the engine gets debited for.

    ORDER IS THE CONTRACT (req.remediate.denominator). Both upstream questions —
    did the injection survive extraction, and did the material its detection
    needs survive — are settled BEFORE any engine output is read. So the three
    upstream states, and with them membership in the conditional detection
    denominator, are a function of gold and extraction ALONE; engine output only
    selects among the states that remain. Deciding expressibility after the
    engine (as this did before) made the denominator shrink only on engine
    MISSES, which is the flattering direction and cannot measure the engine.
    """
    # (1) Did the corrupted value survive extraction into the gold bucket?
    from_corrupted = [v for doc, v in bucket_docs if doc in corrupted_docs]
    if not from_corrupted:
        # No claim from any corrupted rendering reached the gold bucket. This is
        # the mis-keying case too: the claim exists elsewhere, not here.
        return STATE_MISSED_AT_EXTRACTION
    if not any(equivalent(v, corrupted_value) for v in from_corrupted):
        # A claim arrived but the injection did not: the extractor normalised or
        # garbled the corrupted value. A different defect from absence.
        return STATE_LAUNDERED_AT_EXTRACTION

    # (2) The corrupted value survived. Was its conflict still EXPRESSIBLE? Gold
    #     and extraction only — the engine has not been consulted yet.
    if not _detection_material_survived(
        corruption_class=corruption_class,
        partition=partition,
        clean_value=clean_value,
        corrupted_docs=corrupted_docs,
        bucket_docs=bucket_docs,
        fd_input_buckets=fd_input_buckets,
        fd_resolvable=fd_resolvable,
        present_buckets=present_buckets,
        equivalent=equivalent,
    ):
        # The engine never had THIS conflict to find, so nothing it emits on this
        # bucket can be credit for finding it. It may still have emitted: a third,
        # non-gold value left in the bucket is a real conflict the engine can
        # resolve. That is a finding about a DIFFERENT question, and the funnel and
        # the repair tally therefore disagree here BY DESIGN — exactly as they
        # already do on missed_at_extraction and laundered_at_extraction, which
        # have always been decided ahead of the engine. The agreement property the
        # step-(3) short-circuit protects is scoped to the sites the funnel admits
        # as engine-answerable, and it survives verbatim below.
        return STATE_COUNTERPART_LOST

    # (3) Survived AND expressible: the engine had a fair question. Re-use
    #     adjudicate()'s exact reading so the funnel never disagrees with the
    #     repair tally on a bucket both can see.
    if res is not None:
        if not res.is_canonical:
            return STATE_DETECTED_ONLY
        if res.winner_value is not None and equivalent(res.winner_value, clean_value):
            return STATE_REPAIRED
        return STATE_MIS_REPAIRED
    if detected:
        # A finding without a resolution row is still noticed, not resolved.
        return STATE_DETECTED_ONLY
    return STATE_NOT_DETECTED


def funnel(
    *,
    manifest: Mapping[str, Any],
    extracted_claims: Mapping[str, str],
    resolutions: Sequence[BucketResolution],
    findings: Sequence[Any],
    equivalent,
    removed_corruptions: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Every corruption's terminal state, cross-tabbed with its partition.

    ``removed_corruptions`` are corruption_ids whose conflict an OPERATOR
    correction removed from the engine's view — its claim RETRACTED out of
    C_eff, or its corrupted value OVERRIDDEN. They terminate in
    ``removed_from_scope`` (the reserved state synth-stage-attribution left
    unreachable), never debited to the engine as a detection miss: a retract
    drops the claim (so the corrupted value is absent from C_eff and would
    otherwise read as missed_at_extraction) and an override rewrites it (so the
    conflict is gone with the claim still present). Default empty, so every
    correction-free run reports removed_from_scope = 0 exactly as before.

    Pure over sealed gold + the post-extraction facts: ``manifest`` (trace,
    patients, gold corruptions), ``extracted_claims`` (claim_id -> value at the
    corrupted generation), the engine's ``resolutions`` and ``findings`` (the
    finding rows, from which detection is judged by THE predicate —
    :func:`bucket_detected_for_kind` — per corruption's GOLD kind and its bucket,
    so an unresolved_key site flagged on not_comparable lands detected_only, not
    not_detected), and the DECLARED value ``equivalent`` (canonicalize ∘
    synth_value_equivalent, shared with the repair adjudication).
    Scorecard-independent by design — the scorecard calls it and derives the rates.

    Returns per-corruption rows (each carrying the REALIZED post-extraction
    vote), the exhaustive state tally, the partition × state cross-tab, the
    conditional/end-to-end detection and repair rates, and per-class injection
    survival (the fraction of each class's sites not lost at extraction, with
    the non-surviving sites named). Extraction survival is DERIVED from which
    renderings the trace marks corrupted joined against the claims that actually
    landed — the deterministic lane is a lossless control, not an assumption.
    """
    trace = manifest["trace"]
    patients = manifest["patients"]
    gold = manifest["gold"]["corruptions"]

    buckets = corruption_bucket_map(manifest)

    # THE detection predicate's one join: which channels the engine emitted a
    # finding on, per bucket. Detection is then per corruption's GOLD kind.
    channels_by_bucket = _channels_by_bucket(findings)

    predicate_by_fact: dict[str, str] = {}
    for row in trace:
        predicate_by_fact.setdefault(row["fact_id"], synth_slug(row["predicate_label"]))

    site_info: dict[str, dict[str, Any]] = {}
    for site in gold:
        cid = site["corruption_id"]
        entry = site_info.setdefault(
            cid,
            {
                "class": site["corruption_class"],
                "partition": site["partition"],
                "clean_value": site["pre_text"],
                "corrupted_value": site["post_text"],
                "params": dict(site.get("params", {})),
                "kind": gold_constraint_kind(site),
                "corrupted_docs": set(),
            },
        )
        entry["corrupted_docs"].add(site["doc"])

    # Index the post-extraction claims by gold bucket once.
    docs_by_bucket: dict[str, list[tuple[str, str]]] = {}
    present_buckets: set[str] = set()
    for claim_id, value in extracted_claims.items():
        parsed = _claim_bucket_and_doc(claim_id)
        if parsed is None:
            continue
        bucket, doc = parsed
        docs_by_bucket.setdefault(bucket, []).append((doc, value))
        present_buckets.add(bucket)
    present = frozenset(present_buckets)

    by_bucket_res = {r.bucket: r for r in resolutions}

    rows: list[dict[str, Any]] = []
    per_corruption: dict[str, str] = {}
    for cid in sorted(buckets):
        bucket = buckets[cid]
        entry = site_info[cid]
        corrupted_value = entry["corrupted_value"]
        clean_value = entry["clean_value"]
        corrupted_docs = frozenset(entry["corrupted_docs"])
        bucket_docs = docs_by_bucket.get(bucket, [])

        fd_buckets, fd_resolvable = _fd_input_buckets(
            entry["params"], predicate_by_fact, patients
        )
        if cid in removed_corruptions:
            # An operator correction removed this corruption's conflict from
            # C_eff (retract) or rewrote its value (override) — an operator
            # action, not a detection miss. Attributed BEFORE the extraction
            # check, which would otherwise read the retracted claim's absence as
            # missed_at_extraction.
            state = STATE_REMOVED_FROM_SCOPE
        else:
            state = _terminal_state(
                corruption_class=entry["class"],
                partition=entry["partition"],
                corrupted_value=corrupted_value,
                clean_value=clean_value,
                corrupted_docs=corrupted_docs,
                bucket_docs=bucket_docs,
                res=by_bucket_res.get(bucket),
                detected=bucket_detected_for_kind(
                    channels_by_bucket, bucket, entry["kind"]
                ),
                fd_input_buckets=fd_buckets,
                fd_resolvable=fd_resolvable,
                present_buckets=present,
                equivalent=equivalent,
            )
        per_corruption[cid] = state

        # The realized post-extraction vote over the whole bucket. Extraction can
        # invert a planned majority (lose a corrupted rendering: 2-vs-1 -> 1-vs-1;
        # lose the clean counterpart: the lie goes unanimous), so majority-arm
        # conclusions are drawn only over sites whose realized vote kept shape.
        corrupted_votes = sum(
            1 for _doc, v in bucket_docs if equivalent(v, corrupted_value)
        )
        clean_votes = sum(1 for _doc, v in bucket_docs if equivalent(v, clean_value))
        # WHICH SIGNAL SETTLED IT (concord-system-stress, req.stress.precedence).
        # Read from the engine's own recorded resolution, never re-derived here:
        # a harness that recomputed the basis could disagree with the engine it
        # is grading, which is the one thing this row must not do. Absent when
        # the bucket produced no resolution row at all.
        _res = by_bucket_res.get(bucket)
        rows.append(
            {
                "corruption_id": cid,
                "state": state,
                "resolution_outcome": getattr(_res, "outcome", "") if _res else "",
                "resolution_basis": getattr(_res, "basis", "") if _res else "",
                "partition": entry["partition"],
                "corruption_class": entry["class"],
                "bucket": bucket,
                "realized_vote": {
                    "corrupted": corrupted_votes,
                    "clean": clean_votes,
                    "other": len(bucket_docs) - corrupted_votes - clean_votes,
                },
            }
        )

    states = {s: 0 for s in FUNNEL_STATES}
    for state in per_corruption.values():
        if state not in states:  # pragma: no cover - defensive
            raise ValueError(f"funnel produced an unknown terminal state {state!r}")
        states[state] += 1

    total = len(buckets)
    if sum(states.values()) != total:  # pragma: no cover - guarded by construction
        raise ValueError(
            f"funnel is not total: {states} sums to {sum(states.values())}, "
            f"expected exactly one state per corruption ({total})"
        )

    partition_state: dict[str, dict[str, int]] = {}
    for row in rows:
        cell = partition_state.setdefault(row["partition"], {})
        cell[row["state"]] = cell.get(row["state"], 0) + 1
    partition_state = {
        p: {s: partition_state[p][s] for s in sorted(partition_state[p])}
        for p in sorted(partition_state)
    }

    cond_denom = cond_detected = e2e_detected = 0
    for row in rows:
        st = row["state"]
        if st in _DETECTED_STATES:
            e2e_detected += 1
        if row["partition"] in _CONDITIONAL_PARTITIONS and st in _SURVIVED_CONFLICT_STATES:
            cond_denom += 1
            if st in _DETECTED_STATES:
                cond_detected += 1
    repaired = states[STATE_REPAIRED]

    # Per-class injection survival (req.attrib.survival), DERIVED from the same
    # rows the funnel already produced: a site survived extraction iff its
    # terminal state is not one of the two extraction-loss states. No parallel
    # join, so survival and the funnel can never diverge; the non-surviving
    # sites are named for the report.
    survival_by_class: dict[str, dict[str, int]] = {}
    non_surviving: list[str] = []
    for row in rows:
        cell = survival_by_class.setdefault(
            row["corruption_class"], {"sites": 0, "survived": 0}
        )
        cell["sites"] += 1
        if row["state"] in _EXTRACTION_LOSS_STATES:
            non_surviving.append(row["corruption_id"])
        else:
            cell["survived"] += 1
    per_class_survival = {
        "by_class": {k: survival_by_class[k] for k in sorted(survival_by_class)},
        "non_surviving_sites": sorted(non_surviving),
    }

    return {
        "rows": rows,
        "per_corruption": per_corruption,
        "total": total,
        "states": states,
        "partition_state": partition_state,
        "per_class_survival": per_class_survival,
        # Conditional recall hides upstream decay; end-to-end alone misattributes
        # it. The funnel derives both from the same states — never from config.
        "detection_recall": {
            "conditional": {"denominator": cond_denom, "detected": cond_detected},
            "end_to_end": {"denominator": total, "detected": e2e_detected},
        },
        "repair_rate": {
            "conditional": {"denominator": e2e_detected, "repaired": repaired},
            "end_to_end": {"denominator": total, "repaired": repaired},
        },
    }


# -- per-flavor majority mis-repair (req.prov.flavors) ---------------------


def per_flavor_mis_repair(
    manifest: Mapping[str, Any],
    stages: Mapping[str, Any],
    poisoned_corruptions: frozenset[str] = frozenset(),
) -> dict[str, dict[str, int]]:
    """Mis-repair over PRESERVED majorities, split by provenance flavor.

    The gradability surface for the coming independence-weighting fix. Each
    majority site the sealed provenance plan labelled COPY or INDEPENDENT is
    counted against its OWN denominator of REALIZED-preserved majorities (the
    site's post-extraction vote still holds a strict corrupted majority AND a
    clean counterpart survived to be wrong about — the same preservation predicate
    the capstone axis h uses), so a flavor's mis_repaired is meaningful only over a
    non-zero preserved denominator.

    Derived from the funnel's per-corruption ROWS (``stages['rows']``: each carries
    the realized vote and terminal state) joined to the sealed
    ``provenance.flavor_by_corruption`` — never a re-derivation of the ranking
    logic. On today's engine both flavors mis-repair every preserved site; the day
    independence weighting lands, COPY must fall to zero while INDEPENDENT keeps
    settling on the genuinely-better-evidenced majority.

    ``poisoned_corruptions`` (synth-authority-poisoning) are excluded from the
    per-flavor denominators: a poisoned bucket's mis-repair is a wrong-authority flip,
    NOT a corroboration mis-repair, so counting it here would contaminate the
    majority-flavor reading the independence fix is graded on. The poison axis owns
    those buckets; keeping them out here keeps the two axes orthogonal.
    """
    prov = manifest.get("provenance") or {}
    flavor_by_cid: Mapping[str, str] = prov.get("flavor_by_corruption") or {}
    rows_by_cid = {row["corruption_id"]: row for row in stages["rows"]}

    cells: dict[str, dict[str, int]] = {
        flavor: {"preserved": 0, "mis_repaired": 0} for flavor in FLAVORS
    }
    for cid, flavor in sorted(flavor_by_cid.items()):
        row = rows_by_cid.get(cid)
        if row is None or flavor not in cells or cid in poisoned_corruptions:
            continue
        vote = row["realized_vote"]
        # A genuine realized majority-wrong conflict: corrupted still holds the
        # strict majority AND a clean counterpart survived (clean == 0 is unanimous,
        # not a conflict — no mis-repair is possible), matching capstone axis h.
        if vote["corrupted"] > vote["clean"] and vote["clean"] >= 1:
            cells[flavor]["preserved"] += 1
            if row["state"] == STATE_MIS_REPAIRED:
                cells[flavor]["mis_repaired"] += 1
    return {FLAVOR_COPY: cells[FLAVOR_COPY], FLAVOR_INDEPENDENT: cells[FLAVOR_INDEPENDENT]}


# -- the poisoning flip report (synth-authority-poisoning) ------------------


def poisoned_buckets_of(manifest: Mapping[str, Any]) -> frozenset[str]:
    """The sealed poisoned bucket keys ("predicate:entity") of a run's arm plan."""
    plan = (manifest.get("precedence_arm") or {}).get("plan") or {}
    return frozenset(plan.get("poisoned_buckets") or ())


def poisoned_corruptions_of(manifest: Mapping[str, Any]) -> frozenset[str]:
    """The corruption_ids whose gold bucket the run POISONED — the set to exclude
    from every unpoisoned repair denominator (repair tally, majority-flavor,
    capstone axis h). Derived from the sealed poison plan joined to the sealed
    corruption→bucket map, so it never re-derives the selection.
    """
    poisoned = poisoned_buckets_of(manifest)
    if not poisoned:
        return frozenset()
    return frozenset(
        cid for cid, bucket in corruption_bucket_map(manifest).items()
        if bucket in poisoned
    )


def poison_flip_report(
    manifest: Mapping[str, Any],
    resolutions: Sequence[BucketResolution],
    stages: Mapping[str, Any],
) -> dict[str, Any]:
    """The FLIP report over the sealed poisoned buckets (req.poison.arm).

    A poisoned bucket FLIPPED iff the engine settled it canonically on the wrong
    value — its terminal funnel state is ``mis_repaired``. The flip rate is
    flips / poisoned (0 when nothing was poisoned — the r=0 negative control reads a
    poisoned count of 0 and a flip rate of 0). ``by_basis`` records the resolution
    BASIS of every flipped bucket, so the by-basis witness can fail a poisoned bucket
    that flipped on any basis OTHER than ``declared_source_authority`` (the flip must
    be the poison, not a coincidence). ``all_flips_on_authority`` is the crisp
    predicate that witness reads; ``per_bucket`` carries the auditable detail.

    Read entirely from the sealed poison plan joined to the published resolution rows
    and the funnel's per-corruption states — never re-derived ranking logic.
    """
    from collections import Counter

    plan = (manifest.get("precedence_arm") or {}).get("plan") or {}
    poison_rate = float(plan.get("poison_rate", 0.0) or 0.0)
    eligible = list(plan.get("poison_eligible") or ())
    poisoned = sorted(plan.get("poisoned_buckets") or ())

    res_by_bucket = {r.bucket: r for r in resolutions}
    state_by_cid = stages.get("per_corruption", {})
    cid_by_bucket: dict[str, str] = {}
    for cid, bucket in corruption_bucket_map(manifest).items():
        cid_by_bucket.setdefault(bucket, cid)

    per_bucket: list[dict[str, Any]] = []
    flipped = 0
    by_basis: Counter[str] = Counter()
    for bucket in poisoned:
        cid = cid_by_bucket.get(bucket)
        res = res_by_bucket.get(bucket)
        state = state_by_cid.get(cid) if cid is not None else None
        is_flip = state == STATE_MIS_REPAIRED
        basis = res.basis if res is not None else None
        if is_flip:
            flipped += 1
            by_basis[basis or ""] += 1
        per_bucket.append({
            "bucket": bucket,
            "corruption_id": cid,
            "outcome": res.outcome if res is not None else None,
            "basis": basis,
            "winner_value": res.winner_value if res is not None else None,
            "state": state,
            "flipped": is_flip,
        })

    flip_rate = (flipped / len(poisoned)) if poisoned else 0.0
    all_flips_on_authority = set(by_basis) <= {SIGNAL_DECLARED_SOURCE_AUTHORITY}
    return {
        "enabled": poison_rate > 0.0,
        "poison_rate": poison_rate,
        "eligible": len(eligible),
        "poisoned": len(poisoned),
        "flipped": flipped,
        "flip_rate": flip_rate,
        "expected_basis": SIGNAL_DECLARED_SOURCE_AUTHORITY,
        "by_basis": dict(sorted(by_basis.items())),
        "all_flips_on_authority": all_flips_on_authority,
        "per_bucket": per_bucket,
    }


class PoisonCurveError(ValueError):
    """A non-monotone flip curve — an INSTRUMENT defect, never an engine finding."""


def assert_monotone_flip_curve(
    points: Sequence[tuple[float, int]],
) -> list[tuple[float, int]]:
    """The pact's monotonicity clause (req.poison.curve): the flip COUNT must be
    non-decreasing as the poisoning rate r rises.

    The poisoned set nests by construction (the eligible pool is shuffled once,
    r-independent, and each rate takes a prefix), so a HIGHER r poisons a SUPERSET
    of a lower r's buckets and — in the lossless deterministic lane — flips at least
    as many. A drop is therefore an INSTRUMENT defect (a broken selection or scorer),
    raised LOUDLY here so it can never be mistaken for an engine finding. Returns the
    rate-sorted points on success.
    """
    ordered = sorted(points, key=lambda p: p[0])
    for (r_lo, f_lo), (r_hi, f_hi) in zip(ordered, ordered[1:]):
        if f_hi < f_lo:
            raise PoisonCurveError(
                f"non-monotone flip curve: r={r_lo} flipped {f_lo} but r={r_hi} "
                f"flipped {f_hi} (FEWER). The poisoned set nests by construction, so "
                f"a drop is an INSTRUMENT defect (selection or scorer), never an "
                f"engine finding — the curve is not reportable."
            )
    return ordered
