from __future__ import annotations

from pathlib import Path

from okg.substrate.pact.cli import _status
from okg.substrate.pact.models import Change, PactHierarchy, PactManifest
from okg.substrate.pact.store import save_manifest


def test_parent_child_rollup_preserves_child_proof_ownership(
    tmp_path: Path,
) -> None:
    parent = PactManifest(
        target_pact_version=3,
        change=Change(id="parent", title="Parent"),
        hierarchy=PactHierarchy(
            role="program",
            children=["child"],
            integration_acceptance=["parent accepts child once child verifies"],
        ),
    )
    child = PactManifest(
        target_pact_version=3,
        change=Change(id="child", title="Child"),
        hierarchy=PactHierarchy(role="child", parent="parent"),
        requirements=[
            {
                "id": "req.child",
                "statement": "Child owns concrete proof.",
                "evidence_policy": {"required": ["executed_test"]},
                "scenarios": [
                    {
                        "id": "scn.child",
                        "title": "child proof",
                        "when": ["status rolls up"],
                        "then": ["proof remains owned by the child"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.child",
                        "description": "child proof exists",
                        "evidence": ["executed_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.child",
                "title": "Child",
                "verification": "pytest tests/pact/test_v3_hierarchy.py -q",
                "requirements": ["req.child"],
            }
        ],
    )
    save_manifest(tmp_path, parent)
    save_manifest(tmp_path, child)

    payload = _status(tmp_path, "parent", include_graph=False)
    rollup = payload["hierarchy"]["child_rollup"][0]

    assert payload["evidence_gaps"] == []
    assert rollup["change"] == "child"
    assert rollup["proof_owner"] == "child"
    assert rollup["proof_gaps"] == [
        {"id": "req.child", "missing": ["executed_test", "acceptance:acc.child"]}
    ]
    assert "child requirements" in payload["hierarchy"]["proof_ownership"]
