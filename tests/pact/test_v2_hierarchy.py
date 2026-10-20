"""Hierarchy invariant: parent PACT cannot duplicate child evidence.

Per `simplify-pact-surface` task 1, the old test asserted multi-sentence
prose about program/child PACT roles in the development-contract runbook.
That prose moved to `pact/AGENTS.md` as a single bullet; the structural
invariant lives in `validation.py::_hierarchy_issues`. This test asserts
the invariant directly rather than greping the docs.
"""
from __future__ import annotations

from okg.substrate.pact.models import (
    Change,
    PactHierarchy,
    PactManifest,
)
from okg.substrate.pact.validation import validate_manifest


def test_self_parent_rejected() -> None:
    manifest = PactManifest(
        change=Change(id="alpha", title="Alpha"),
        hierarchy=PactHierarchy(parent="alpha"),
    )
    report = validate_manifest(manifest)
    codes = {i.code for i in report.issues if i.severity == "error"}
    assert "invalid_hierarchy_self_parent" in codes


def test_self_child_rejected() -> None:
    manifest = PactManifest(
        change=Change(id="alpha", title="Alpha"),
        hierarchy=PactHierarchy(parent=None, children=["alpha"]),
    )
    report = validate_manifest(manifest)
    codes = {i.code for i in report.issues if i.severity == "error"}
    assert "invalid_hierarchy_self_child" in codes
