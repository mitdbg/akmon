"""okg reads v5 manifests through a read-only adapted v4 view, so the graph
projection source, corpus hygiene, and the review views keep working while
both shapes live under pact/changes."""

from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.library.sources.pact import _cached_manifest_inventory
from okg.substrate.pact.store import load_manifest_bytes
from okg.substrate.pact.v5_adapter import to_v4
from okg.substrate.pact.validation import validate_manifest

V5 = {
    "pact_version": 5,
    "id": "v5-demo",
    "title": "Demo",
    "tier": 1,
    "why": "Reason.\n\nCause.",
    "affected_code": ["src/x/"],
    "requirements": [{"id": "req.a", "statement": "A holds."}, {"id": "req.b", "statement": "B holds."}],
    "tasks": [{"id": "task.a", "requirement": "req.a", "verification": "pytest a"}, {"id": "task.b", "requirement": "req.b", "verification": "pytest b"}],
    "evidence": [
        {"kind": "executed_test", "requirement": "req.a", "status": "pass", "head": "a" * 40, "artifact": "junit.xml", "sha256": "0" * 64},
        {"kind": "manual", "requirement": "req.b", "status": "pass"},
    ],
    "decisions": [{"digest": "sha256:" + "b" * 64, "reviewer": "riley"}],
}


def test_v4_loader_accepts_a_v5_manifest_and_validates_clean(tmp_path: Path) -> None:
    payload = yaml.safe_dump(V5).encode("utf-8")
    manifest = load_manifest_bytes(payload, source_path=tmp_path / "pact.yaml")
    assert manifest.change.id == "v5-demo" and manifest.change.tier == 1
    assert manifest.change.rationale == "Reason." and manifest.change.root_cause == "Cause."
    assert [t.status for t in manifest.tasks] == ["done", "todo"], "only head-bound passing rows count"
    assert manifest.change.status == "implementation"
    report = validate_manifest(manifest, repo_root=tmp_path)
    assert [i for i in report.issues if i.severity == "error"] == []


def test_adapter_is_conservative_about_proof() -> None:
    view = to_v4(V5)
    statuses = {row["id"]: row["status"] for row in view["evidence"]}
    assert statuses == {"ev.v5.0": "pass", "ev.v5.1": "fail"}
    assert view["change"]["method_metadata"]["pact_version"] == 5
    assert view["change"]["method_metadata"]["decisions"][0]["reviewer"] == "riley"
    fully = dict(V5, evidence=[dict(V5["evidence"][0]), {"kind": "manual", "requirement": "req.b", "head": "c" * 40}])
    assert to_v4(fully)["change"]["status"] == "verification"


def test_projection_source_inventory_reads_v5_bytes(tmp_path: Path) -> None:
    payload = yaml.safe_dump(V5).encode("utf-8")
    entry = _cached_manifest_inventory(payload, str(tmp_path / "pact.yaml"))
    assert entry.manifest.change.id == "v5-demo"
    assert entry.affected_code == ("src/x/",)
