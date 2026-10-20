from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import save_manifest
from tests.pact.test_cli import _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_status_reports_traceability_satisfaction_and_next_action(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must expose status.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "status",
                        "when": ["status runs"],
                        "then": ["evidence gaps are visible"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "status output",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Do demo",
                "verification": "uv run --extra dev pytest tests/pact/test_v1_status_surface.py -q",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_status_surface.py::test_status_reports_traceability_satisfaction_and_next_action",
            }
        ],
    )
    save_manifest(root, manifest)

    rc = main(["pact", "--root", str(root), "view", "demo", "--format=status", "--json"])
    payload = _json_out(capsys)

    assert rc == 0
    assert payload["evidence_gaps"] == [
        {"id": "req.demo", "missing": ["executed_test"]}
    ]
    assert payload["missing_evidence"] == payload["evidence_gaps"]
    assert payload["next"][0]["id"] == "task.demo"
    assert payload["next"][0]["verification"] == "uv run --extra dev pytest tests/pact/test_v1_status_surface.py -q"
    assert "executed_test" in payload["next"][0]["evidence_gaps"][0]

    rc = main(["pact", "--root", str(root), "view", "demo", "--for-agent", "--json"])
    show = _json_out(capsys)

    assert rc == 0
    assert show["evidence_gaps"][0]["missing"] == ["executed_test"]
    assert show["requirements"][0]["alignment"]["traceability_status"] == "traceable"
    assert show["requirements"][0]["alignment"]["satisfaction_status"] == "unsatisfied"
