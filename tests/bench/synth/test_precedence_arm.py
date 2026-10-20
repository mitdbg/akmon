"""task.arms.declare witnesses (no DB): the precedence arm's declaration.

The scope condition is now PER-ARM: an arm ADMITS a set of decisive signals, the
condition asserts exactly that set may be present and the complement is ABSENT,
and it stays LOUD — an undeclared signal fails naming it. The corroboration-only
arm keeps today's isolation (its existing witnesses in test_resolution pass
unchanged); the declaration + per-signal surface + sealed plan are recorded in
the manifest; and — because every v1 arm stamps at ingest — the rendered corpus
is byte-identical between an arm and its signal-withheld negative.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from okg.bench.synth.config import (
    ADMITTABLE_SIGNALS,
    SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE,
    SIGNAL_DECLARED_SOURCE_AUTHORITY,
    SIGNAL_OPERATOR_PRECEDENT,
    SURFACE_INGEST_STAMP,
    ConfigError,
    PrecedenceArm,
    load_config,
)
from okg.bench.synth.resolution import (
    ArmScopeError,
    admitted_signal_attrs,
    assert_arm_signal_scope,
)
from tests.bench.synth._precedence_arm_support import (
    all_signals_config,
    arm_config_dict,
    build_arm_manifest,
    default_arm_config,
)


@dataclass
class _Claim:
    """A stand-in consistency_claim fact (.node_id + .attrs) for the scope check."""

    node_id: str
    attrs: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# the closed vocabulary is the engine's decisive set                          #
# --------------------------------------------------------------------------- #
def test_admittable_vocabulary_is_the_engine_decisive_set() -> None:
    """The arm vocabulary is defined as literals in the CORE config (which may
    not import substrate); this pins it to the engine's _DECISIVE so the two can
    never drift — a signal the engine treats as decisive but the arm cannot admit
    (or vice versa) would silently make an arm unstudiable or a stamp inert."""
    from okg.substrate.consistency.claim_model.precedence import _DECISIVE

    assert ADMITTABLE_SIGNALS == _DECISIVE
    assert len(ADMITTABLE_SIGNALS) == 5  # non-vacuity: the set is not empty


# --------------------------------------------------------------------------- #
# per-arm scope condition: admitted OK, undeclared LOUD                        #
# --------------------------------------------------------------------------- #
def test_arm_admitting_effective_date_does_not_fire() -> None:
    """An arm that ADMITS effective_date may carry it — the condition is silent,
    where the old global invariant would have fired."""
    claims = [
        _Claim("claim:a", {"value": "1", "effective_date": "2020-01-01"}),
        _Claim("claim:b", {"value": "2"}),
    ]
    # no raise
    assert_arm_signal_scope(claims, admits=[SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE])
    assert admitted_signal_attrs([SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE]) == {
        "effective_date"
    }


def test_undeclared_signal_fails_loudly_naming_it() -> None:
    """The SAME arm (admits effective_date only) with a source_authority stamp
    fails loudly, and the message names the leaked signal — the silent-realism
    failure the suppression exists to prevent."""
    claims = [
        _Claim("claim:a", {"value": "1", "effective_date": "2020-01-01"}),
        _Claim("claim:b", {"value": "2", "source_authority": "9"}),
    ]
    with pytest.raises(ArmScopeError) as exc:
        assert_arm_signal_scope(claims, admits=[SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE])
    assert SIGNAL_DECLARED_SOURCE_AUTHORITY in str(exc.value)
    assert "claim:b" in str(exc.value)


def test_default_arm_admits_nothing_and_still_guards() -> None:
    """The default (empty) arm admits no signal: unsigned claims pass (today's
    corroboration-only lane), but any ranking attr fails loudly — this is the
    exact guard the existing test_resolution witness relies on."""
    clean = [_Claim("claim:a", {"value": "1", "source_class": "wiki"})]
    assert_arm_signal_scope(clean, admits=[])  # no raise

    leaked = [_Claim("claim:a", {"value": "1", "is_superseded": "true"})]
    with pytest.raises(ArmScopeError, match="explicit_supersession"):
        assert_arm_signal_scope(leaked, admits=[])


def test_source_class_uniformity_unless_precedent_admitted() -> None:
    """source_class may vary ONLY when operator_precedent is admitted (else a
    precedent could settle a bucket the arm did not declare)."""
    varied = [
        _Claim("claim:a", {"value": "1", "source_class": "wiki"}),
        _Claim("claim:b", {"value": "2", "source_class": "specialist_note"}),
    ]
    with pytest.raises(ArmScopeError, match="source_class is not UNIFORM"):
        assert_arm_signal_scope(varied, admits=[])
    # admitting operator_precedent lifts the uniformity requirement
    assert_arm_signal_scope(varied, admits=[SIGNAL_OPERATOR_PRECEDENT])


# --------------------------------------------------------------------------- #
# config validation: closed vocabulary + surface deferral                      #
# --------------------------------------------------------------------------- #
def test_config_rejects_unknown_signal_naming_the_vocabulary() -> None:
    with pytest.raises(ConfigError, match="unknown signal 'nonsense'"):
        load_config(arm_config_dict(["nonsense"]))


def test_config_rejects_duplicate_admit() -> None:
    with pytest.raises(ConfigError, match="duplicate signal"):
        load_config({
            **arm_config_dict([]),
            "precedence_arm": {"admits": [
                SIGNAL_OPERATOR_PRECEDENT, SIGNAL_OPERATOR_PRECEDENT,
            ]},
        })


def test_config_rejects_document_surface_as_a_declared_deferral() -> None:
    with pytest.raises(ConfigError, match="DECLARED DEFERRAL"):
        load_config({
            **arm_config_dict([SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE]),
            "precedence_arm": {
                "admits": [SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE],
                "surface": {SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE: "document"},
            },
        })


def test_config_rejects_surface_for_unadmitted_signal() -> None:
    with pytest.raises(ConfigError, match="does not admit"):
        load_config({
            **arm_config_dict([SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE]),
            "precedence_arm": {
                "admits": [SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE],
                "surface": {SIGNAL_DECLARED_SOURCE_AUTHORITY: SURFACE_INGEST_STAMP},
            },
        })


def test_resolved_surface_defaults_to_ingest_stamp() -> None:
    arm = PrecedenceArm(admits=(
        SIGNAL_OPERATOR_PRECEDENT, SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE,
    ))
    assert arm.resolved_surface() == {
        SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE: SURFACE_INGEST_STAMP,
        SIGNAL_OPERATOR_PRECEDENT: SURFACE_INGEST_STAMP,
    }
    assert PrecedenceArm().resolved_surface() == {}  # default arm


# --------------------------------------------------------------------------- #
# the manifest records the declaration + per-signal surface + sealed plan       #
# --------------------------------------------------------------------------- #
def test_manifest_records_admitted_set_surface_and_plan() -> None:
    manifest = build_arm_manifest(all_signals_config())
    arm = manifest["precedence_arm"]
    assert set(arm["admits"]) == ADMITTABLE_SIGNALS
    # per-signal surface: every admitted signal on ingest_stamp
    assert arm["surface"] == {s: SURFACE_INGEST_STAMP for s in ADMITTABLE_SIGNALS}
    plan = arm["plan"]
    assert plan["schema"] == "okg.synth.precedence-arm-plan.v1"
    # every decisive basis is represented among the sealed targets
    bases = {t["expected_basis"] for t in plan["targets"]}
    assert bases == ADMITTABLE_SIGNALS, (
        f"the sealed plan must target all five decisive bases: missing "
        f"{ADMITTABLE_SIGNALS - bases}"
    )
    # non-vacuity: the default arm seals an EMPTY plan (no stamps at all)
    default_manifest = build_arm_manifest(default_arm_config())
    assert default_manifest["precedence_arm"]["admits"] == []
    assert default_manifest["precedence_arm"]["plan"]["targets"] == []


# --------------------------------------------------------------------------- #
# byte-identity: the ingest surface leaves the documents untouched              #
# --------------------------------------------------------------------------- #
def test_rendered_corpus_byte_identical_arm_vs_negative() -> None:
    """Same world, same seed: an ingest-stamp arm and its signal-withheld
    negative render byte-identical corpora — the documents never carry the
    signals, only the ingested claims do. The config_digest DOES differ (the arm
    is part of the canonical dump), which is what forces the golden regen."""
    arm = build_arm_manifest(all_signals_config())
    negative = build_arm_manifest(default_arm_config())
    assert arm["clean_corpus"] == negative["clean_corpus"]
    assert arm["corrupted_corpus"] == negative["corrupted_corpus"]
    # and the change is real: the config digest moved, so the two are not the
    # same run trivially
    assert arm["config_digest"] != negative["config_digest"]
