"""PACT v5 schema: unknown keys refused; intent digest covers reviewed intent only."""

from __future__ import annotations

import pytest
import yaml

from pact.digest import intent_digest
from pact.schema import Decision, Evidence, Manifest
from pact.store import PactError, load

BASE = {
    "pact_version": 5,
    "id": "demo-change",
    "title": "Demo",
    "tier": 1,
    "why": "Because.",
    "affected_code": ["src/x/"],
    "requirements": [{"id": "req.demo", "statement": "It works."}],
    "tasks": [{"id": "task.demo", "requirement": "req.demo", "verification": "pytest passes."}],
}


def _manifest(**overrides) -> Manifest:
    data = {**BASE, **overrides}
    return Manifest.model_validate(data)


def test_minimal_manifest_validates() -> None:
    manifest = _manifest()
    assert manifest.structural_errors() == []


@pytest.mark.parametrize("key", ["prose", "edits", "waivers", "formal", "method_metadata", "change", "test_plan"])
def test_unknown_top_level_keys_are_refused(key: str) -> None:
    with pytest.raises(Exception):
        _manifest(**{key: []})


def test_unknown_nested_keys_are_refused() -> None:
    with pytest.raises(Exception):
        _manifest(tasks=[{**BASE["tasks"][0], "status": "done"}])


def test_pact_version_must_be_five() -> None:
    with pytest.raises(Exception):
        _manifest(pact_version=4)


def test_structural_rules() -> None:
    m = _manifest(tasks=[{"id": "task.demo", "requirement": "req.other", "verification": "x"}])
    errors = m.structural_errors()
    assert "task_requirement_unknown:task.demo" in errors
    assert "requirement_without_task:req.demo" in errors


def test_evidence_artifact_needs_digest() -> None:
    m = _manifest(evidence=[{"kind": "executed_test", "requirement": "req.demo", "artifact": "junit.xml"}])
    assert "evidence_artifact_without_digest:0" in m.structural_errors()


def test_requirement_edit_changes_digest_and_evidence_does_not() -> None:
    base = _manifest()
    d0 = intent_digest(base)
    with_evidence = _manifest(evidence=[{"kind": "manual", "requirement": "req.demo", "head": "a" * 40}])
    with_decision = _manifest(decisions=[{"digest": d0, "reviewer": "riley"}])
    assert intent_digest(with_evidence) == d0
    assert intent_digest(with_decision) == d0
    edited = _manifest(requirements=[{"id": "req.demo", "statement": "It works better."}])
    assert intent_digest(edited) != d0
    retiered = _manifest(tier=2)
    assert intent_digest(retiered) != d0


def test_digest_is_stable_across_key_order() -> None:
    a = Manifest.model_validate(BASE)
    reordered = dict(reversed(list(BASE.items())))
    b = Manifest.model_validate(reordered)
    assert intent_digest(a) == intent_digest(b)


def test_load_reports_invalid_manifest_token(tmp_path) -> None:
    path = tmp_path / "pact.yaml"
    path.write_text(yaml.safe_dump({**BASE, "bogus": 1}), encoding="utf-8")
    with pytest.raises(PactError) as info:
        load(path)
    assert info.value.token == "invalid_manifest"


def test_evidence_and_decision_field_validation() -> None:
    with pytest.raises(Exception):
        Evidence(kind="executed_test", requirement="req.demo", sha256="nothex")
    with pytest.raises(Exception):
        Evidence(kind="junit", requirement="req.demo")
    with pytest.raises(Exception):
        Decision(digest="md5:abc", reviewer="x")
