"""task.attrib.states witnesses: every terminal state is REACHED.

The funnel splits adjudicate()'s one ``unadjudicated`` label into the four
upstream fates it hid — the injection lost at extraction, laundered at
extraction, its counterpart lost, or survived-whole-but-unfound — so a loss is
charged to the stage that caused it, not always to the engine. These witnesses
are constructed by hand (no DB): each builds the smallest post-extraction world
that forces exactly one state, so a regression that folds two fates back
together fails here rather than silently mis-attributing a loss in a real run.

Non-vacuity is per STATE, not per metric: a scorecard can read a rate of 0.0
whether the state is genuinely unreachable or merely never exercised, and only a
reaching witness tells the two apart.
"""

from __future__ import annotations

import pytest

from okg.bench.synth import resolution as resolution_mod
from okg.bench.synth.resolution import (
    BucketResolution,
    STATE_COUNTERPART_LOST,
    STATE_DETECTED_ONLY,
    STATE_LAUNDERED_AT_EXTRACTION,
    STATE_MIS_REPAIRED,
    STATE_MISSED_AT_EXTRACTION,
    STATE_NOT_DETECTED,
    STATE_REPAIRED,
    adjudicate,
    funnel,
)
from okg.bench.synth.runner import synth_survival_equivalent
from okg.bench.injection.scoring import FindingView


def _finding(bucket: str, channel: str = "definite") -> FindingView:
    """A finding on ``bucket`` on ``channel`` — the funnel's detected-side input.

    The detected flag is now judged by THE predicate (a finding on the corruption's
    bucket on the channel its GOLD kind declares), so a witness expresses "the
    engine noticed this bucket" by handing the funnel a finding, not a pre-flattened
    bucket set. Default ``definite`` is the conflict channel functional /
    incompatible / linked kinds are detected on."""
    return FindingView(finding_id=f"f:{bucket}", channel=channel, bucket_key=bucket)

# One patient, one entity slug — the bucket key every claim id carries.
PID = "p1"
ENTITY = "patient_alice"
WEIGHT = f"fact:{PID}:observation:body_weight:body_weight"
HEIGHT = f"fact:{PID}:observation:body_height:body_height"
BMI = f"fact:{PID}:observation:body_mass_index:body_mass_index"
WEIGHT_LABEL, WEIGHT_SLUG = "Body Weight", "body_weight"
HEIGHT_LABEL, HEIGHT_SLUG = "Body Height", "body_height"
BMI_LABEL, BMI_SLUG = "Body Mass Index", "body_mass_index"
WEIGHT_BUCKET = f"{WEIGHT_SLUG}:{ENTITY}"
BMI_BUCKET = f"{BMI_SLUG}:{ENTITY}"

EQUIV = synth_survival_equivalent


def _trace(fact_id: str, doc: str, label: str, corrupted: bool) -> dict:
    return {
        "fact_id": fact_id,
        "doc": doc,
        "predicate_label": label,
        "surface_value": "",
        "clean_start": 0,
        "clean_end": 0,
        "corrupted_start": 0,
        "corrupted_end": 0,
        "corrupted": corrupted,
        "variation": "",
    }


def _corruption(
    cid: str,
    cls: str,
    partition: str,
    fact_id: str,
    doc: str,
    pre: str,
    post: str,
    params: dict | None = None,
) -> dict:
    return {
        "corruption_id": cid,
        "corruption_class": cls,
        "layer": "kg_fork",
        "partition": partition,
        "fact_id": fact_id,
        "doc": doc,
        "clean_span": [0, 0],
        "corrupted_span": [0, 0],
        "pre_text": pre,
        "post_text": post,
        "params": params or {},
    }


def _manifest(trace: list[dict], corruptions: list[dict]) -> dict:
    return {
        "trace": trace,
        "patients": {PID: {"entity": ENTITY}},
        "gold": {"corruptions": corruptions},
    }


def _claim(doc: str, predicate_slug: str, value: str) -> tuple[str, str]:
    return (f"claim:{doc}:{predicate_slug}:{ENTITY}:0", value)


def _contested(bucket: str) -> BucketResolution:
    predicate, key = bucket.split(":", 1)
    return BucketResolution(
        predicate=predicate,
        bucket_key=key,
        outcome="contested",
        basis="tie",
        winner_claim_id="",
        ranked_claim_ids=("a", "b"),
        winner_value=None,
    )


def _canonical(bucket: str, winner_value: str) -> BucketResolution:
    predicate, key = bucket.split(":", 1)
    return BucketResolution(
        predicate=predicate,
        bucket_key=key,
        outcome="canonical",
        basis="independent_support",
        winner_claim_id="w",
        ranked_claim_ids=("w", "l"),
        winner_value=winner_value,
    )


def _state_of(manifest, claims, resolutions=(), detected=frozenset(), findings=()) -> str:
    # A witness expresses "the engine noticed this bucket" either as a conflict
    # finding per detected bucket (``detected``, the common functional case) or by
    # handing explicit finding rows (``findings``, for a specific channel).
    finding_rows = tuple(findings) + tuple(_finding(b) for b in sorted(detected))
    out = funnel(
        manifest=manifest,
        extracted_claims=dict(claims),
        resolutions=tuple(resolutions),
        findings=finding_rows,
        equivalent=EQUIV,
    )
    (cid,) = out["per_corruption"]
    return out["per_corruption"][cid]


# -- one witness per terminal state ---------------------------------------


def test_missed_at_extraction() -> None:
    """No claim from the corrupted rendering reached the gold bucket, even
    though a clean counterpart did. The corruption never entered the engine's
    view; charge it to extraction, not detection."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    claims = [_claim("d2.md", WEIGHT_SLUG, "82.0 kg")]  # only the clean counterpart
    assert _state_of(_manifest(trace, [corr]), claims) == STATE_MISSED_AT_EXTRACTION


def test_mis_keyed_in_vocab_claim_is_an_extraction_loss_not_not_detected() -> None:
    """The corrupted rendering DID extract, but under a wrong in-vocabulary
    predicate, so it never reached the gold bucket. Bucket scoping files that as
    an extraction loss against the gold bucket — never as not_detected, which
    would debit the engine for a claim it never saw in the right place."""
    trace = [_trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True)]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    # the injected value landed in a DIFFERENT in-vocabulary bucket
    claims = [_claim("d1.md", "abdominal_girth", "180.0 kg")]
    assert _state_of(_manifest(trace, [corr]), claims) == STATE_MISSED_AT_EXTRACTION


def test_laundered_at_extraction() -> None:
    """A claim from the corrupted rendering reached the gold bucket, but the
    injected value did not — the extractor silently repaired it back to clean.
    A different defect from absence, and it must not read as a detection miss."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "82.0 kg"),  # corrupted rendering read as CLEAN
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    assert _state_of(_manifest(trace, [corr]), claims) == STATE_LAUNDERED_AT_EXTRACTION


def test_formatting_drift_is_not_laundering() -> None:
    """The declared equivalence composes canonicalize_value, so unit drift
    ('82000 g' rendered, '82.0 kg' after ingest canonicalization) is the SAME
    value — survived, never mis-scored as the extractor laundering the injection.
    Otherwise every benign twin would read as a launder."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180000 g"
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),  # canonicalized equivalent survived
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    # survived + expressible (clean counterpart) + engine silent => not_detected,
    # NOT laundered.
    assert _state_of(_manifest(trace, [corr]), claims) == STATE_NOT_DETECTED


def test_counterpart_lost_via_lost_cross_doc_counterpart() -> None:
    """The corrupted claim survived but its clean counterpart did not. A
    cross_doc conflict needs both sides; the engine never had the conflict to
    find, so it is an extraction loss — not a detection miss."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    claims = [_claim("d1.md", WEIGHT_SLUG, "180.0 kg")]  # counterpart d2 lost
    assert _state_of(_manifest(trace, [corr]), claims) == STATE_COUNTERPART_LOST


def test_counterpart_lost_via_missing_fd_input() -> None:
    """A derived_value break survived, but its declared functional-dependency
    input was lost at extraction, so the FD check can never fire. The material
    the conflict needs — an input claim, not a counterpart — is gone: an
    extraction loss."""
    trace = [
        _trace(BMI, "d1.md", BMI_LABEL, corrupted=True),
        _trace(HEIGHT, "d1.md", HEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c",
        "derived_value",
        "internal_only",
        BMI,
        "d1.md",
        "24.0",
        "31.0",
        params={"dependency": "bmi", "input_fact_ids": f"{WEIGHT},{HEIGHT}"},
    )
    # the derived claim survived; NEITHER FD input bucket is populated
    claims = [_claim("d1.md", BMI_SLUG, "31.0")]
    assert _state_of(_manifest(trace, [corr]), claims) == STATE_COUNTERPART_LOST


def test_not_detected() -> None:
    """The conflict was fully expressible post-extraction — corrupted value and
    clean counterpart both survived — and the engine produced no finding. This
    is the only state that debits the engine for a real miss."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),  # counterpart survived
    ]
    assert _state_of(_manifest(trace, [corr]), claims) == STATE_NOT_DETECTED


def test_detected_only() -> None:
    """The engine noticed the conflict (a contested resolution row) but could
    not settle it. Detected, not repaired — the two are independent questions."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    state = _state_of(
        _manifest(trace, [corr]),
        claims,
        resolutions=[_contested(WEIGHT_BUCKET)],
        detected={WEIGHT_BUCKET},
    )
    assert state == STATE_DETECTED_ONLY


def test_mis_repaired() -> None:
    """The engine settled the bucket canonically — on the corrupted value. It
    repaired to the lie."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    state = _state_of(
        _manifest(trace, [corr]),
        claims,
        resolutions=[_canonical(WEIGHT_BUCKET, "180.0 kg")],
        detected={WEIGHT_BUCKET},
    )
    assert state == STATE_MIS_REPAIRED


def test_repaired() -> None:
    """The engine settled the bucket canonically on the CLEAN value."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    state = _state_of(
        _manifest(trace, [corr]),
        claims,
        resolutions=[_canonical(WEIGHT_BUCKET, "82.0 kg")],
        detected={WEIGHT_BUCKET},
    )
    assert state == STATE_REPAIRED


# -- the ONE detection predicate is per-declared-channel -------------------


def _unresolved_corruption() -> dict:
    """A survived unresolved_key site: corrupted value present in d1, clean
    counterpart in d2, so only the detection channel decides its state."""
    return _corruption(
        "c",
        "ambiguous_subject",
        "cross_doc",
        WEIGHT,
        "d1.md",
        "82.0 kg",
        "180.0 kg",
        params={"constraint_kind": "unresolved_key"},
    )


def _unresolved_trace_and_claims():
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    return trace, claims


def test_unresolved_key_flagged_on_not_comparable_is_detected_only() -> None:
    """The engine flags an unresolved key on its NOT-COMPARABLE channel (it
    refuses a verdict). Its GOLD kind declares that channel, so the site is
    DETECTED — it lands detected_only (its bucket is never canonically resolved by
    construction), never not_detected. This is the funnel side of the one
    predicate agreeing with per_kind_detection."""
    from okg.bench.synth.resolution import bucket_detected_for_kind

    trace, claims = _unresolved_trace_and_claims()
    corr = _unresolved_corruption()
    assert bucket_detected_for_kind(
        {WEIGHT_BUCKET: frozenset({"not_comparable"})}, WEIGHT_BUCKET, "unresolved_key"
    )
    state = _state_of(
        _manifest(trace, [corr]),
        claims,
        findings=(_finding(WEIGHT_BUCKET, channel="not_comparable"),),
    )
    assert state == STATE_DETECTED_ONLY


def test_conflict_finding_on_unresolved_key_bucket_does_not_count() -> None:
    """The mismatch witness: a CONFLICT-channel (definite) finding on an
    unresolved_key bucket does NOT credit it — the credit is per-declared-channel,
    not any-channel. An unresolved key declares not_comparable, so a definite
    finding leaves it undetected (not_detected here, with a surviving counterpart),
    exactly as it would read with NO finding at all."""
    from okg.bench.synth.resolution import bucket_detected_for_kind

    trace, claims = _unresolved_trace_and_claims()
    corr = _unresolved_corruption()
    # the predicate itself: a definite channel does not satisfy unresolved_key
    assert not bucket_detected_for_kind(
        {WEIGHT_BUCKET: frozenset({"definite"})}, WEIGHT_BUCKET, "unresolved_key"
    )
    mismatch = _state_of(
        _manifest(trace, [corr]),
        claims,
        findings=(_finding(WEIGHT_BUCKET, channel="definite"),),
    )
    baseline = _state_of(_manifest(trace, [corr]), claims)  # no finding at all
    assert mismatch == STATE_NOT_DETECTED
    assert mismatch == baseline, (
        "a conflict finding on an unresolved_key bucket must read exactly like no "
        "finding — it does not count"
    )


# -- totality: no partition is exempt --------------------------------------


def test_world_knowledge_with_surviving_counterpart_agrees_with_repair_tally() -> None:
    """The funnel is TOTAL. A world_knowledge site (implausible in isolation) that
    happens to keep a surviving counterpart still produces an engine-visible
    conflict, and it walks the SAME machine: no partition pass-through. The state
    it lands in must agree with what adjudicate() — the landed repair tally —
    already says for that bucket, or the two would double-state it."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c",
        "value_perturbation",
        "world_knowledge",  # implausible-in-isolation, yet a counterpart survives
        WEIGHT,
        "d1.md",
        "82.0 kg",
        "980.0 kg",
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "980.0 kg"),
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    manifest = _manifest(trace, [corr])
    resolutions = (_canonical(WEIGHT_BUCKET, "82.0 kg"),)
    out = funnel(
        manifest=manifest,
        extracted_claims=dict(claims),
        resolutions=resolutions,
        findings=(_finding(WEIGHT_BUCKET),),
        equivalent=EQUIV,
    )
    (cid,) = out["per_corruption"]
    funnel_state = out["per_corruption"][cid]

    repair = adjudicate(
        corruption_buckets={cid: WEIGHT_BUCKET},
        clean_values={cid: "82.0 kg"},
        resolutions=resolutions,
        detected_buckets=frozenset({WEIGHT_BUCKET}),
        equivalent=EQUIV,
    )
    assert funnel_state == STATE_REPAIRED
    assert repair["per_corruption"][cid] == funnel_state, (
        "a world_knowledge site with a surviving counterpart must land in the "
        "same state the repair tally already gives it — the funnel is total"
    )
    # and it is counted, in its own partition cell, not exempted
    assert out["partition_state"]["world_knowledge"][STATE_REPAIRED] == 1


# -- the realized post-extraction vote -------------------------------------


def test_realized_vote_records_the_planned_majority() -> None:
    """A majority-arm site (corrupted in 2 of 3 renderings) records its realized
    vote so a downstream reader can tell the planned shape was preserved."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d3.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    # a corruption spanning two docs is two gold rows sharing one id
    corr2 = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d2.md", "82.0 kg", "180.0 kg"
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),
        _claim("d2.md", WEIGHT_SLUG, "180.0 kg"),
        _claim("d3.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    out = funnel(
        manifest=_manifest(trace, [corr, corr2]),
        extracted_claims=dict(claims),
        resolutions=(),
        findings=(),
        equivalent=EQUIV,
    )
    (row,) = out["rows"]
    assert row["realized_vote"] == {"corrupted": 2, "clean": 1, "other": 0}


def test_realized_vote_catches_extraction_inverting_a_majority() -> None:
    """Extraction can silently demote a planned 2-vs-1 to 1-vs-1 by losing a
    corrupted rendering. The realized vote says so, which is why majority-arm
    conclusions are drawn only over sites whose vote kept shape."""
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d3.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    corr2 = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d2.md", "82.0 kg", "180.0 kg"
    )
    claims = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),  # d2's corrupted claim lost
        _claim("d3.md", WEIGHT_SLUG, "82.0 kg"),
    ]
    out = funnel(
        manifest=_manifest(trace, [corr, corr2]),
        extracted_claims=dict(claims),
        resolutions=(),
        findings=(),
        equivalent=EQUIV,
    )
    (row,) = out["rows"]
    assert row["realized_vote"] == {"corrupted": 1, "clean": 1, "other": 0}


# -- fail-closed exhaustiveness --------------------------------------------


def test_funnel_fails_closed_on_a_stateless_corruption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exhaustiveness guard must have teeth. If a corruption ever falls out
    of the state machine (an unknown or absent state), the funnel refuses rather
    than reporting a tally that silently under-counts the corruption set."""
    monkeypatch.setattr(resolution_mod, "_terminal_state", lambda **_: "limbo")
    trace = [_trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True)]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    with pytest.raises(ValueError, match="unknown terminal state"):
        funnel(
            manifest=_manifest(trace, [corr]),
            extracted_claims={},
            resolutions=(),
            findings=(),
            equivalent=EQUIV,
        )


def test_conditional_zero_denominator_is_reported_not_raised() -> None:
    """req.honesty.perkind-conditional: a kind whose every site is an extraction
    loss has a CONDITIONAL denominator of zero (extraction lost every survivable
    conflict). per_kind_detection REPORTS that zero — end-to-end still counts the
    sites — and never raises: a conditional zero is data, not a run failure."""
    from okg.bench.synth.resolution import per_kind_detection

    trace = [_trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True)]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    manifest = _manifest(trace, [corr])
    # no claim from the corrupted rendering reaches the bucket -> missed_at_extraction
    stages = funnel(
        manifest=manifest, extracted_claims={}, resolutions=(), findings=(),
        equivalent=EQUIV,
    )
    assert stages["states"][STATE_MISSED_AT_EXTRACTION] == 1
    pk = per_kind_detection(manifest, stages, (), {})
    cell = pk["by_kind"]["functional"]
    assert cell["end_to_end"] == {"denominator": 1, "detected": 0}
    # the conditional denominator is a REPORTED zero, no exception raised
    assert cell["conditional"] == {"denominator": 0, "detected": 0}


def test_reserved_removed_from_scope_is_admitted_but_zero() -> None:
    """removed_from_scope is reserved in the vocabulary and admitted by the tally
    (so synth-constraint-coverage lands without editing this contract), yet it is
    never reachable HERE — no correction machinery exists in this change."""
    from okg.bench.synth.resolution import FUNNEL_STATES, STATE_REMOVED_FROM_SCOPE

    assert STATE_REMOVED_FROM_SCOPE in FUNNEL_STATES
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    out = funnel(
        manifest=_manifest(trace, [corr]),
        extracted_claims=dict([_claim("d1.md", WEIGHT_SLUG, "180.0 kg")]),
        resolutions=(),
        findings=(),
        equivalent=EQUIV,
    )
    assert out["states"][STATE_REMOVED_FROM_SCOPE] == 0
    assert set(out["states"]) == set(FUNNEL_STATES)


# -- the conditional denominator is a function of gold + extraction ONLY ----
#
# req.remediate.denominator. Expressibility used to be decided AFTER the engine
# short-circuit, so counterpart_lost — the state that removes a site from the
# conditional denominator — was unreachable whenever the engine emitted anything
# on the bucket. The denominator could then shrink only on engine MISSES (the
# flattering direction), and a false positive on a gold bucket whose material was
# lost raised conditional recall. These witnesses hold EXTRACTION FIXED and vary
# only the engine's output; against the pre-fix ordering they fail.

HEIGHT_BUCKET = f"{HEIGHT_SLUG}:{ENTITY}"

# The one extracted world both witnesses below reuse. Two cross_doc sites:
#   weight — corrupted rendering AND clean counterpart both landed  -> expressible
#   height — corrupted rendering landed, clean counterpart did NOT  -> material lost
_FIXED_TRACE = [
    _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
    _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    _trace(HEIGHT, "d1.md", HEIGHT_LABEL, corrupted=True),
    _trace(HEIGHT, "d2.md", HEIGHT_LABEL, corrupted=False),
]
_FIXED_CORRUPTIONS = [
    _corruption(
        "c_expressible", "value_perturbation", "cross_doc",
        WEIGHT, "d1.md", "82.0 kg", "180.0 kg",
    ),
    _corruption(
        "c_material_lost", "value_perturbation", "cross_doc",
        HEIGHT, "d1.md", "1.70 m", "2.60 m",
    ),
]
_FIXED_CLAIMS = dict([
    _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),
    _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),   # counterpart SURVIVED
    _claim("d1.md", HEIGHT_SLUG, "2.60 m"),
    # d2's clean height claim was LOST at extraction -> no counterpart
])

# Engine outputs, applied identically to BOTH buckets so the only thing varying
# across cases is what the engine emitted.
_ENGINE_CASES = {
    "silent": ((), ()),
    "finding_only": ((), (WEIGHT_BUCKET, HEIGHT_BUCKET)),
    "finding_and_contested": (
        (_contested(WEIGHT_BUCKET), _contested(HEIGHT_BUCKET)),
        (WEIGHT_BUCKET, HEIGHT_BUCKET),
    ),
    "finding_and_canonical": (
        (_canonical(WEIGHT_BUCKET, "82.0 kg"), _canonical(HEIGHT_BUCKET, "1.70 m")),
        (WEIGHT_BUCKET, HEIGHT_BUCKET),
    ),
}


def _fixed_extraction_funnel(case: str) -> dict:
    resolutions, detected = _ENGINE_CASES[case]
    return funnel(
        manifest=_manifest(_FIXED_TRACE, _FIXED_CORRUPTIONS),
        extracted_claims=dict(_FIXED_CLAIMS),
        resolutions=resolutions,
        findings=tuple(_finding(b) for b in detected),
        equivalent=EQUIV,
    )


def test_conditional_denominator_is_invariant_under_engine_output() -> None:
    """THE witness the metric never had (req.remediate.denominator).

    Extraction is held FIXED — the same four trace rows and the same three
    extracted claims in every case — and only the engine's output varies: silent,
    a finding with no resolution, a finding with a contested resolution, and a
    finding with a canonical resolution. Denominator membership is decided by
    gold and extraction alone, so the conditional denominator must not move.

    Against the pre-fix ordering it moves from 1 (silent: the height site reads
    counterpart_lost) to 2 (any emission: the height site is admitted as
    detected/repaired), and conditional recall rises with it. That is the defect:
    the engine could enlarge its own denominator by flagging a bucket whose gold
    conflict extraction had already destroyed."""
    denominators = {}
    counterpart_lost = {}
    detected = {}
    for case in _ENGINE_CASES:
        out = _fixed_extraction_funnel(case)
        cond = out["detection_recall"]["conditional"]
        denominators[case] = cond["denominator"]
        detected[case] = cond["detected"]
        counterpart_lost[case] = out["states"][STATE_COUNTERPART_LOST]
        # the site whose material died is counterpart_lost NO MATTER what the
        # engine said about its bucket
        assert out["per_corruption"]["c_material_lost"] == STATE_COUNTERPART_LOST, (
            f"case {case!r}: a site whose detection material was lost must be "
            f"counterpart_lost regardless of engine output, got "
            f"{out['per_corruption']['c_material_lost']!r}"
        )

    assert set(denominators.values()) == {1}, (
        f"the conditional denominator MOVED with engine output: {denominators}. "
        "It must be a function of gold and extraction only."
    )
    assert set(counterpart_lost.values()) == {1}, counterpart_lost
    # non-vacuity: the engine's output DOES move the numerator (otherwise this
    # test would pass on a funnel that ignored the engine entirely)
    assert detected["silent"] == 0
    assert detected["finding_only"] == 1
    assert set(detected.values()) == {0, 1}


def test_funnel_still_agrees_with_the_repair_tally_on_expressible_sites() -> None:
    """The property the engine short-circuit was protecting SURVIVES the reorder.

    On every site the funnel admits as engine-answerable — the corrupted value
    survived AND its detection material survived — the funnel state equals what
    adjudicate() gives the same bucket, in every engine case. The short-circuit
    is unchanged; it simply now runs after expressibility instead of before."""
    for case in _ENGINE_CASES:
        resolutions, _detected = _ENGINE_CASES[case]
        out = _fixed_extraction_funnel(case)
        repair = adjudicate(
            corruption_buckets={"c_expressible": WEIGHT_BUCKET},
            clean_values={"c_expressible": "82.0 kg"},
            resolutions=resolutions,
            detected_buckets=frozenset({WEIGHT_BUCKET}),
            equivalent=EQUIV,
        )
        funnel_state = out["per_corruption"]["c_expressible"]
        tally_state = repair["per_corruption"]["c_expressible"]
        if tally_state == "unadjudicated":
            # adjudicate() has no finding channel, so a bucket with no resolution
            # row is unadjudicated to it; the funnel refines that residue. Agreement
            # is asserted where BOTH can see the bucket — i.e. a resolution row.
            continue
        assert funnel_state == tally_state, (
            f"case {case!r}: funnel says {funnel_state!r} but the repair tally "
            f"says {tally_state!r} on a bucket both can see"
        )
    # and the funnel stays exhaustive in every case
    for case in _ENGINE_CASES:
        out = _fixed_extraction_funnel(case)
        assert sum(out["states"].values()) == out["total"] == 2


def test_counterpart_lost_disagreement_has_the_same_shape_as_missed_at_extraction() -> (
    None
):
    """The disagreement introduced by deciding expressibility first is NOT new in
    kind — it is the one the funnel has always had on extraction losses.

    A missed_at_extraction site whose bucket carries a resolution row (some other
    claim in the bucket conflicts) has always read missed_at_extraction in the
    funnel while adjudicate() calls it repaired. counterpart_lost now behaves the
    same way, for the same reason: the funnel charges the loss to the stage that
    caused it, and adjudicate() answers a narrower question ("did the engine
    settle this bucket?") that is blind to whether the GOLD conflict was still
    there to settle. This witness pins both, so a future reader can see the fix
    extended an existing precedent rather than weakening the agreement property."""
    # (a) the PRE-EXISTING case: nothing from the corrupted rendering landed, yet
    # the bucket resolves because two other claims disagree.
    trace = [
        _trace(WEIGHT, "d1.md", WEIGHT_LABEL, corrupted=True),
        _trace(WEIGHT, "d2.md", WEIGHT_LABEL, corrupted=False),
    ]
    corr = _corruption(
        "c", "value_perturbation", "cross_doc", WEIGHT, "d1.md", "82.0 kg", "180.0 kg"
    )
    claims = [
        _claim("d2.md", WEIGHT_SLUG, "82.0 kg"),
        _claim("d3.md", WEIGHT_SLUG, "77.0 kg"),  # an unrelated third value
    ]
    resolutions = (_canonical(WEIGHT_BUCKET, "82.0 kg"),)
    out = funnel(
        manifest=_manifest(trace, [corr]),
        extracted_claims=dict(claims),
        resolutions=resolutions,
        findings=(_finding(WEIGHT_BUCKET),),
        equivalent=EQUIV,
    )
    repair = adjudicate(
        corruption_buckets={"c": WEIGHT_BUCKET},
        clean_values={"c": "82.0 kg"},
        resolutions=resolutions,
        detected_buckets=frozenset({WEIGHT_BUCKET}),
        equivalent=EQUIV,
    )
    assert out["per_corruption"]["c"] == STATE_MISSED_AT_EXTRACTION
    assert repair["per_corruption"]["c"] == STATE_REPAIRED

    # (b) the case this change adds: the corrupted value survived, its clean
    # counterpart did not, and a third value keeps the bucket conflicting. Same
    # shape — the funnel charges extraction, the tally reports what the engine did.
    claims_b = [
        _claim("d1.md", WEIGHT_SLUG, "180.0 kg"),  # injection survived
        _claim("d3.md", WEIGHT_SLUG, "77.0 kg"),   # counterpart lost; other value
    ]
    out_b = funnel(
        manifest=_manifest(trace, [corr]),
        extracted_claims=dict(claims_b),
        resolutions=resolutions,
        findings=(_finding(WEIGHT_BUCKET),),
        equivalent=EQUIV,
    )
    assert out_b["per_corruption"]["c"] == STATE_COUNTERPART_LOST
    assert repair["per_corruption"]["c"] == STATE_REPAIRED
    # and it is OUT of the conditional denominator, which is the point
    assert out_b["detection_recall"]["conditional"]["denominator"] == 0
