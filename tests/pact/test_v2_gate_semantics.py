from __future__ import annotations

from pathlib import Path

from okg.substrate.pact.cli import _status
from okg.substrate.pact.markdown_view import write_markdown_view
from okg.substrate.pact.models import Change, Evidence, PactManifest
from okg.substrate.pact.store import save_manifest
from okg.substrate.pact.validation import validate_manifest


def _manifest() -> PactManifest:
    return PactManifest(
        pact_version=0,
        target_pact_version=2,
        change=Change(id="demo", title="Demo", status="proposal"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "PACT V2 must separate planned proof, design context, and executed evidence.",
                "evidence_policy": {"required": ["planned_test"]},
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "planned test is traceable",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo task",
                "status": "done",
                "verification": "pytest tests/pact/test_v2_gate_semantics.py -q",
                "requirements": ["req.demo"],
                "evidence": ["ev.pytest.req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v2_gate_semantics.py::test_design_record_does_not_satisfy_evidence_gate",
            }
        ],
    )


def test_design_record_does_not_satisfy_evidence_gate() -> None:
    manifest = _manifest()
    manifest.evidence.append(
        Evidence(
            id="ev.design.req.demo",
            kind="design",
            status="pass",
            refs=["req.demo"],
            artifact="pact/changes/demo/designs/decision.md",
        )
    )

    report = validate_manifest(manifest, lifecycle_gate="verification")
    state = report.alignment["requirements"]["req.demo"]

    assert state["traceability_status"] == "traceable"
    assert state["satisfaction_status"] == "unsatisfied"
    assert state["satisfaction_missing"] == ["executed_test"]
    assert any(issue.code == "completion_gate_unsatisfied" for issue in report.issues)


def test_local_completion_and_graph_projection_are_reported_separately(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # The graph surfaces read the repository's configured PACT graph
    # deployment; this test configures the one it exercises (#2823).
    monkeypatch.setenv("OKG_PACT_GRAPH_DEPLOYMENT", "okg-workspace")
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = tmp_path
    manifest = _manifest()
    manifest.evidence.append(
        Evidence(
            id="ev.pytest.req.demo",
            kind="pytest",
            status="pass",
            refs=["req.demo", "task.demo", "test.demo"],
            artifact=".tmp/pact/demo/pytest.xml",
            data={
                "junit_test_ids": [
                    "tests.pact.test_v2_gate_semantics::test_design_record_does_not_satisfy_evidence_gate"
                ]
            },
        )
    )
    save_manifest(root, manifest)
    write_markdown_view(root, manifest.change.id)

    payload = _status(root, manifest.change.id, include_graph=True)

    assert payload["lifecycle"]["verification"] is True
    assert payload["evidence_gaps"] == []
    assert payload["graph_projection"]["status"] == "unavailable"
    assert payload["graph_projection"]["reason"] == "OKG_DSN is not set"
