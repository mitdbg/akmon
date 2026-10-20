"""The library coherence probe runs against the real trees and reports counts.

These tests pin the probe's CONTRACT (it executes, resolves endpoints the way
the audit claims, and reports an unavailable deployments tree honestly), not
the current defect counts. Pinning the counts would turn every legitimate
ontology edit into a test failure; the probe emits a machine-readable baseline
for CI rather than relying on a point-in-time audit page.
"""
from __future__ import annotations

import pathlib

import pytest

from scripts.audit_library_coherence import (
    build_report,
    scan_ontology,
    scan_profiles,
    scan_sources,
)


@pytest.fixture(scope="module")
def ontology() -> dict:
    return scan_ontology()


def test_ontology_scan_reads_the_real_module_tree(ontology: dict) -> None:
    assert ontology["module_count"] > 30
    assert ontology["class_count"] > 200
    assert ontology["narrowing_count"] > 300


def test_every_narrowing_endpoint_resolves_to_a_defined_class(
    ontology: dict,
) -> None:
    """A dangling endpoint would mean a narrowing can never be satisfied.

    The audit reports zero; if this regresses, a narrowing references a class
    no module defines and the edge is silently unbuildable.
    """
    assert ontology["unresolved_narrowing_endpoint_count"] == 0, (
        f"dangling endpoints: {ontology['unresolved_narrowing_endpoints']}"
    )


def test_locally_defined_endpoints_are_not_counted_as_cross_module_deps(
    ontology: dict,
) -> None:
    """Resolution must be per-class and local-first.

    9 class names are defined by two modules each. Resolving an endpoint via a
    single global owner map would report a module as depending on a twin that
    merely shares a name, inflating the dependency and closure counts.
    """
    for module, deps in ontology["undeclared_cross_module_deps"].items():
        assert module not in deps, (
            f"{module} reported as depending on itself"
        )


def test_profile_closure_resolves_duplicated_classes_by_any_holder(
    ontology: dict,
) -> None:
    """A duplicated endpoint class is satisfied by any one of its holders."""
    profiles = scan_profiles(ontology)
    holders = ontology["_class_holders"]
    for prof, gaps in profiles["closure_gaps"].items():
        members = set(profiles["module_sets"][prof])
        for cls in gaps:
            assert not (set(holders[cls]) & members), (
                f"{prof}: {cls} reported as a gap but a holder is present"
            )


def test_source_and_enricher_inventory_is_non_empty() -> None:
    src = scan_sources()
    assert src["source_count"] > 50
    assert src["enricher_count"] >= 1
    assert src["registry_source_count"] > 0


def test_missing_deployments_tree_is_reported_not_defaulted(
    tmp_path: pathlib.Path,
) -> None:
    """An absent deployments tree must surface, never silently read as clean.

    A default-value fallback here would report the deployment dimension as
    audited-and-clean when it was never read at all.
    """
    report = build_report(tmp_path / "does-not-exist")
    assert report["deployments"]["status"] == "unavailable"
    assert "not found" in report["deployments"]["reason"]
