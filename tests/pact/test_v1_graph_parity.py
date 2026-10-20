from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.pact import PACTSource
from okg.substrate.pact.models import Change, Evidence, PactManifest
from okg.substrate.pact.store import save_manifest


def test_pact_source_status_matches_file_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo", status="implementation"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must project status.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "projects",
                        "when": ["PACTSource runs"],
                        "then": ["graph attrs match file status"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "graph parity",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Do demo",
                "verification": "pytest tests/pact/test_v1_graph_parity.py -q",
                "requirements": ["req.demo"],
                "evidence": ["ev.pytest.req.demo"],
                "environment": [
                    {
                        "id": "env.OKG_DSN",
                        "kind": "env_var",
                        "env": "OKG_DSN",
                    }
                ],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_graph_parity.py::test_pact_source_status_matches_file_status",
            }
        ],
        evidence=[
            Evidence(
                id="ev.pytest.req.demo",
                kind="pytest",
                status="fail",
                refs=["req.demo", "test.demo", "task.demo"],
            )
        ],
    )
    save_manifest(tmp_path, manifest)

    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    nodes = {fact.node_id: fact for fact in facts if isinstance(fact, NodeFact)}

    change = nodes["pact-change:demo"]
    task = nodes["pact-task:demo:task.demo"]
    planned_test = nodes["pact-verification:demo:test:test.demo"]
    evidence = nodes["pact-verification:demo:evidence:ev.pytest.req.demo"]
    env_check = nodes["pact-verification:demo:env:task.demo:env.OKG_DSN"]

    assert change.subtype == "pact_change"
    assert task.subtype == "pact_task"
    # The fixture's row is `status: fail`, so the requirement now reports two
    # different things about it and the projection must carry both: the
    # unfilled proof slot (`executed_test`), and the recorded outcome that
    # REFUTES the requirement regardless of the kind carrying it
    # (`pact-evidence-policy-blindness`). Before that change only the first
    # was reported here, while `evidence_issues` below already named the same
    # row as failing at TASK level — the two surfaces now agree.
    assert change.attrs["evidence_gaps"] == [
        {
            "id": "req.demo",
            "missing": [
                "executed_test",
                "refuted_by:ev.pytest.req.demo:pytest:fail",
            ],
        }
    ]
    assert change.attrs["environment_blockers"][0]["name"] == "OKG_DSN"
    assert task.attrs["alignment_status"] == "blocked"
    assert task.attrs["environment_blockers"][0]["id"] == "env.OKG_DSN"
    assert task.attrs["evidence_issues"] == [
        "failing_evidence:ev.pytest.req.demo:fail"
    ]
    assert planned_test.attrs["step_kind"] == "planned_test"
    assert planned_test.attrs["satisfaction_status"] == "unsatisfied"
    assert evidence.attrs["step_kind"] == "evidence"
    assert evidence.attrs["evidence_status"] == "fail"
    assert env_check.attrs["step_kind"] == "environment_check"
    assert env_check.attrs["blocked"] is True
