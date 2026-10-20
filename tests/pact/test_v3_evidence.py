from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.models import TestPlanEntry as PactTestPlanEntry
from okg.substrate.pact.store import load_manifest, save_manifest
from tests.pact.test_cli import _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_evidence_helper_maps_executed_reports_and_flags_stale_proof(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    assert main([
        "pact", "--root", str(root), "new-change", "demo",
        "--template", "protocol", "--json",
    ]) == 0
    _json_out(capsys)
    for req_id in ("req.good", "req.missing"):
        pact_operations.add_requirement(
            root, "demo", req_id=req_id, title=req_id,
            statement=f"{req_id} must have executed evidence.",
            evidence=["executed_test"],
        )
    test_id = "tests/pact/test_v3_evidence.py::test_good"
    pact_operations.require_test(root, "demo", "req.good", test_id)
    pact_operations.require_test(
        root, "demo", "req.missing",
        "tests/pact/test_v3_evidence.py::test_missing",
    )

    report = tmp_path / "junit.xml"
    report.write_text(
        """
        <testsuite>
          <testcase classname="tests.pact.test_v3_evidence" name="test_good"/>
        </testsuite>
        """,
        encoding="utf-8",
    )
    # ingest lands the rows locally and queues the ledger write -> deferred
    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--pytest-report", str(report), "--json",
    ])
    assert rc == 3
    assert _json_out(capsys)["verdict"] == "deferred"

    manifest = load_manifest(root, "demo")
    manifest.test_plan.append(
        PactTestPlanEntry(
            id="test.stale",
            requirement="req.deleted",
            test_id="tests/pact/test_v3_evidence.py::test_deleted",
        )
    )
    save_manifest(root, manifest)

    # req.missing has no proof, so the status read refuses (exit 1); the
    # per-requirement rows below are the diagnostic it still carries
    rc = main([
        "pact", "--root", str(root), "evidence", "status", "demo",
        "--json",
    ])
    status = _json_out(capsys)
    assert rc == 1
    assert status["verdict"] == "refused"

    requirements = {row["id"]: row for row in status["requirements"]}
    assert requirements["req.good"]["status"] == "satisfied"
    assert requirements["req.good"]["evidence"]
    assert requirements["req.missing"]["status"] == "unsatisfied"
    assert requirements["req.missing"]["missing"] == [
        "executed_test",
        "acceptance:acc.req-missing",
    ]
    assert status["missing_requirements"] == [
        {
            "id": "req.missing",
            "missing": ["executed_test", "acceptance:acc.req-missing"],
        }
    ]
    assert status["stale_planned_tests"] == [
        {"id": "test.stale", "requirement": "req.deleted"}
    ]
    stale_plan = [
        row for row in status["planned_tests"] if row["id"] == "test.stale"
    ][0]
    assert stale_plan["stale"] is True
