"""No-change gating for the source optimization observability assessment.

PACT okg-workspace-recovery-tail / req.expected-delta-budget. A source that
DECLARES an explicit all-zero single-delta is "no-change-dominant": a
representative single change to the deployment does not touch it, so producing 0
observed delta is correct (no work-amplification is possible) — not missing
evidence. `_microbench_observability_terms` must treat that 0/0 as a pass and
grade only latency, while still flagging a truly-missing declaration.

NOTE: clearing these observability gaps does NOT by itself pass the readiness
optimization_budget gate — the dominant blocker is full-pipeline publish latency
(the derived-fact re-load). This guards the gate-semantics half only.
"""

from okg.substrate.source_sync_microbench import (
    EXPECTED_DELTA_SOURCE_HARNESS_DECLARED,
    _microbench_observability_terms,
)


def _terms(expected_delta, expected_source, observed, *, latency=50.0, work=0):
    return _microbench_observability_terms(
        expected_delta=expected_delta,
        expected_delta_source=expected_source,
        publish_scope={"publish_delta_totals": observed},
        latency_p95_ms=latency,
        sample_count=1,
        source_work_units=work,
    )


ZERO = {"nodes_updated": 0, "edges_updated": 0}
OBS0 = {"nodes_updated": 0, "edges_updated": 0}


def test_declared_zero_is_no_change_dominant_pass():
    t = _terms(ZERO, EXPECTED_DELTA_SOURCE_HARNESS_DECLARED, OBS0)
    assert t["no_change_dominant"] is True
    assert t["observability_status"] == "pass"
    assert t["observability_gaps"] == []


def test_negative_expected_component_is_not_no_change_dominant():
    t = _terms(
        {"nodes_updated": -1, "edges_updated": 1},
        EXPECTED_DELTA_SOURCE_HARNESS_DECLARED,
        OBS0,
    )
    assert t["no_change_dominant"] is False
    assert "missing_expected_delta" in t["observability_gaps"]


def test_no_declaration_still_flags_missing():
    # Empty / absent expected_delta is genuinely missing, not no-change.
    t = _terms({}, EXPECTED_DELTA_SOURCE_HARNESS_DECLARED, OBS0)
    assert t["no_change_dominant"] is False
    assert "missing_expected_delta" in t["observability_gaps"]
    assert "missing_observed_delta" in t["observability_gaps"]


def test_unaccepted_source_zero_is_not_no_change():
    t = _terms(ZERO, "missing_or_empty", OBS0)
    assert t["no_change_dominant"] is False
    assert "missing_expected_delta" in t["observability_gaps"]


def test_real_single_delta_matching_passes():
    exp = {"nodes_updated": 120, "edges_updated": 120}
    obs = {"nodes_updated": 120, "edges_updated": 120}
    t = _terms(exp, EXPECTED_DELTA_SOURCE_HARNESS_DECLARED, obs, work=240)
    assert t["no_change_dominant"] is False
    assert t["observability_status"] == "pass"


def test_real_single_delta_mismatch_flags():
    exp = {"nodes_updated": 100, "edges_updated": 100}
    obs = {"nodes_updated": 120, "edges_updated": 120}
    t = _terms(exp, EXPECTED_DELTA_SOURCE_HARNESS_DECLARED, obs, work=240)
    assert "expected_delta_mismatch" in t["observability_gaps"]


def test_no_change_declared_but_observed_work_is_not_silently_passed():
    # If a "no-change" source unexpectedly emits a large delta, it is NOT
    # no-change-dominant (observed > 0) and runaway amplification still flags.
    obs = {"nodes_updated": 5000, "edges_updated": 5000}
    t = _terms(ZERO, EXPECTED_DELTA_SOURCE_HARNESS_DECLARED, obs, work=100000)
    assert t["no_change_dominant"] is False
