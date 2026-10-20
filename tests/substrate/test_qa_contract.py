"""Unit tests for the stability-contract validator.

The contract YAML loader, the ContractResult shape, and the per-
field pass/fail computation are all exercised against synthetic
metrics + catalog state; no DB I/O.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.qa import (
    CatalogState,
    QAGateMetrics,
    StabilityContract,
    check_contract,
)


def _baseline_metrics(**overrides) -> QAGateMetrics:
    base = dict(
        deployment_name="af_pubs",
        generation_id="okg:af_pubs:branch:default:gen:sample",
        nodes_live_delta=1000,
        edges_live_delta=2000,
        queue_depth={
            "ingest": 0, "link": 0, "enrich": 0,
            "publish": 0, "embed": 0,
        },
        dlq_count=0,
        cross_source_invariants_total=5,
        cross_source_invariants_failed=0,
        cross_source_invariants_timed_out=0,
        source_local_invariants_total=3,
        source_local_invariants_failed=0,
        substrate_integrity_checks={},
        excluded_sources=[],
        total_invariant_duration_ms=300,
        total_invariant_budget_exceeded=False,
        generation_blocked_count=0,
        doctor_exit_code=0,
        publish_wallclock_seconds=10.0,
    )
    base.update(overrides)
    return QAGateMetrics(**base)


def _baseline_contract(**overrides) -> StabilityContract:
    base = dict(
        deployment_name="af_pubs",
        expected_sources=["af_pubs.catalog", "af_pubs.pdf_text"],
        expected_subtypes=["publication", "pdf_section"],
        expected_edge_types=["references", "supersedes"],
        min_nodes_live=500,
        min_edges_live=500,
        max_publish_wallclock_seconds=60.0,
        required_views=["af_pubs.publication_overview"],
        required_invariants=["body_contradicts__chaplain_services"],
        viz_smoke_pages=["/viz/", "/viz/api/health"],
    )
    base.update(overrides)
    return StabilityContract(**base)


def _baseline_catalog(**overrides) -> CatalogState:
    base = dict(
        active_subtypes={"publication", "pdf_section", "section"},
        active_edge_types={"references", "supersedes", "contains"},
        registered_sources={
            "af_pubs.catalog", "af_pubs.pdf_text", "af_pubs.obsolete",
        },
        registered_views={
            "af_pubs.publication_overview",
            "af_pubs.recent_changes",
        },
        invariant_status={
            "body_contradicts__chaplain_services": "passed",
            "stale_without_review": "passed",
        },
    )
    base.update(overrides)
    return CatalogState(**base)


def test_contract_yaml_round_trip(tmp_path: Path) -> None:
    yaml_text = """
deployment_name: af_pubs
expected_sources: [af_pubs.catalog, af_pubs.pdf_text]
expected_subtypes: [publication, pdf_section]
expected_edge_types: [references, supersedes]
min_nodes_live: 500
min_edges_live: 500
max_publish_wallclock_seconds: 60.0
required_views: [af_pubs.publication_overview]
required_invariants: [body_contradicts__chaplain_services]
viz_smoke_pages: ["/viz/", "/viz/api/health"]
"""
    p = tmp_path / "stability_contract.yaml"
    p.write_text(yaml_text)
    c = StabilityContract.from_yaml(p)
    assert c.deployment_name == "af_pubs"
    assert c.min_nodes_live == 500
    assert c.required_invariants == ["body_contradicts__chaplain_services"]


def test_contract_yaml_rejects_missing_keys(tmp_path: Path) -> None:
    p = tmp_path / "stability_contract.yaml"
    p.write_text("deployment_name: af_pubs\n")
    with pytest.raises(ValueError, match="missing required keys"):
        StabilityContract.from_yaml(p)


def test_pass_when_everything_is_within_bounds() -> None:
    result = check_contract(
        _baseline_contract(),
        _baseline_metrics(),
        _baseline_catalog(),
    )
    assert result.passed
    assert result.deployment_name == "af_pubs"
    assert result.generation_id == "okg:af_pubs:branch:default:gen:sample"
    # No failures.
    assert result.failures() == []


def test_fail_when_min_nodes_live_not_met() -> None:
    metrics = _baseline_metrics(nodes_live_delta=10)
    result = check_contract(
        _baseline_contract(),
        metrics,
        _baseline_catalog(),
    )
    assert not result.passed
    fails = result.failures()
    assert any(r.field == "min_nodes_live" for r in fails)


def test_fail_when_required_invariant_failed() -> None:
    catalog = _baseline_catalog(
        invariant_status={
            "body_contradicts__chaplain_services": "failed",
            "stale_without_review": "passed",
        },
    )
    result = check_contract(
        _baseline_contract(),
        _baseline_metrics(),
        catalog,
    )
    assert not result.passed
    inv_fails = [
        r for r in result.failures()
        if r.field.startswith("invariant:")
    ]
    assert len(inv_fails) == 1
    assert "body_contradicts__chaplain_services" in inv_fails[0].field
    assert "failed" in inv_fails[0].message


def test_fail_when_required_invariant_missing() -> None:
    catalog = _baseline_catalog(invariant_status={})
    result = check_contract(
        _baseline_contract(),
        _baseline_metrics(),
        catalog,
    )
    assert not result.passed
    inv_fails = [
        r for r in result.failures()
        if r.field.startswith("invariant:")
    ]
    assert len(inv_fails) == 1
    assert inv_fails[0].actual == "missing"


def test_fail_when_expected_subtype_not_in_catalog() -> None:
    catalog = _baseline_catalog(
        active_subtypes={"publication"},  # missing pdf_section
    )
    result = check_contract(
        _baseline_contract(),
        _baseline_metrics(),
        catalog,
    )
    assert not result.passed
    fails = [r for r in result.failures() if r.field == "expected_subtypes"]
    assert fails
    assert "pdf_section" in fails[0].message


def test_fail_when_publish_wallclock_exceeds_max() -> None:
    metrics = _baseline_metrics(publish_wallclock_seconds=120.0)
    result = check_contract(
        _baseline_contract(max_publish_wallclock_seconds=60.0),
        metrics,
        _baseline_catalog(),
    )
    assert not result.passed
    fails = [
        r for r in result.failures()
        if r.field == "max_publish_wallclock_seconds"
    ]
    assert fails


def test_fail_when_phase_b_excluded_a_source() -> None:
    metrics = _baseline_metrics(
        excluded_sources=["af_pubs.pdf_text"],
    )
    result = check_contract(
        _baseline_contract(),
        metrics,
        _baseline_catalog(),
    )
    assert not result.passed
    fails = [
        r for r in result.failures()
        if r.field == "excluded_sources"
    ]
    assert fails
    assert "af_pubs.pdf_text" in fails[0].message


def test_fail_when_doctor_exit_nonzero() -> None:
    metrics = _baseline_metrics(doctor_exit_code=1)
    result = check_contract(
        _baseline_contract(),
        metrics,
        _baseline_catalog(),
    )
    assert not result.passed
    fails = [
        r for r in result.failures()
        if r.field == "doctor_exit_code"
    ]
    assert fails


def test_deployment_mismatch_raises() -> None:
    contract = _baseline_contract(deployment_name="af_pubs")
    metrics = _baseline_metrics(deployment_name="okg-workspace")
    with pytest.raises(ValueError, match="contract is for"):
        check_contract(contract, metrics, _baseline_catalog())


def test_summary_shape() -> None:
    metrics = _baseline_metrics(nodes_live_delta=5)  # < min 500
    result = check_contract(
        _baseline_contract(),
        metrics,
        _baseline_catalog(),
    )
    summary = result.summary()
    assert "FAIL" in summary
    assert "af_pubs" in summary
    assert "min_nodes_live" in summary
