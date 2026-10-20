from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact.models import Change, Evidence, PactManifest
from okg.substrate.pact.store import save_manifest
from tests.pact.test_cli import _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_acceptance_requires_declared_semantic_evidence_mapping(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    manifest = PactManifest(
        target_pact_version=3,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "A specific acceptance test must prove the behavior.",
                "evidence_policy": {"required": ["executed_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "specific proof",
                        "when": ["evidence status runs"],
                        "then": ["the acceptance criterion is mapped"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "specific test evidence is present",
                        "evidence": ["test.demo"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo",
                "verification": "pytest tests/pact/test_v3_proof_semantics.py -q",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v3_proof_semantics.py::test_acceptance_requires_declared_semantic_evidence_mapping",
            }
        ],
        evidence=[
            Evidence(
                id="ev.pytest.broad",
                kind="pytest",
                status="pass",
                refs=["req.demo"],
                artifact="junit.xml",
                data={
                    "junit_test_ids": [
                        "tests.pact.test_v3_proof_semantics::test_acceptance_requires_declared_semantic_evidence_mapping"
                    ]
                },
            )
        ],
    )
    save_manifest(root, manifest)

    # a requirement is unsatisfied, so `evidence status` refuses (exit 1)
    # rather than reporting the gap under a success code
    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "evidence",
            "status",
            "demo",
            "--json",
        ]
    )
    status = _json_out(capsys)
    assert rc == 1
    assert status["verdict"] == "refused"
    acceptance = {row["id"]: row for row in status["acceptance"]}
    assert acceptance["acc.demo"]["status"] == "unsatisfied"
    assert acceptance["acc.demo"]["missing"] == ["test.demo"]

    manifest.evidence[0].refs = ["test.demo"]
    save_manifest(root, manifest)
    # the acceptance criterion is now mapped, but the row was moved OFF
    # req.demo, so the requirement's own executed_test proof is gone and
    # the read still refuses -- naming req.demo, not the acceptance
    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "evidence",
            "status",
            "demo",
            "--json",
        ]
    )
    status = _json_out(capsys)
    assert rc == 1
    assert status["verdict"] == "refused"
    assert status["blocked_by"] == ["unsatisfied_requirement:req.demo"]
    acceptance = {row["id"]: row for row in status["acceptance"]}
    assert acceptance["acc.demo"]["status"] == "satisfied"
