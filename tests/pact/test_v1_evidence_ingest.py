from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import load_manifest, save_manifest
from okg.substrate.pact.validation import validate_manifest
from tests.pact.test_cli import _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _manifest() -> PactManifest:
    return PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must ingest evidence.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "ingests",
                        "when": ["JUnit is ingested"],
                        "then": ["evidence maps to the planned test"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "JUnit evidence",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Do demo",
                "verification": "uv run --extra dev pytest tests/pact/test_v1_evidence_ingest.py -q",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_evidence_ingest.py::test_pytest_junit_ingest_maps_to_planned_tests",
                "command": "uv run --extra dev pytest tests/pact/test_v1_evidence_ingest.py -q",
            }
        ],
    )


def test_pytest_junit_ingest_maps_to_planned_tests(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    save_manifest(root, _manifest())
    report = tmp_path / "junit.xml"
    report.write_text(
        """
        <testsuite>
          <testcase classname="tests.pact.test_v1_evidence_ingest" name="test_pytest_junit_ingest_maps_to_planned_tests" time="0.01"/>
        </testsuite>
        """,
        encoding="utf-8",
    )

    rc = main([
        "pact",
        "--root",
        str(root),
        "evidence",
        "ingest",
        "demo",
        "--pytest-report",
        str(report),
        "--json",
    ])
    payload = _json_out(capsys)

    # the row lands locally; the ledger write is only queued -> deferred
    assert rc == 3
    assert payload["verdict"] == "deferred"
    assert payload["evidence"] == 1

    data = yaml.safe_load((root / "pact" / "changes" / "demo" / "pact.yaml").read_text())
    evidence = data["evidence"][0]
    assert evidence["refs"] == ["req.demo", "test.demo", "task.demo"]
    assert evidence["command"] == "uv run --extra dev pytest tests/pact/test_v1_evidence_ingest.py -q"
    assert evidence["data"]["planned_tests"] == ["test.demo"]
    assert evidence["data"]["tasks"] == ["task.demo"]

    assert main([
        "pact", "--root", str(root), "gate", "task-done", "demo",
        "task.demo", "--json",
    ]) == 0
    _json_out(capsys)

    report = validate_manifest(
        load_manifest(root, "demo"), lifecycle_gate="verification",
    )
    assert report.alignment["requirements"]["req.demo"]["satisfaction_status"] == "satisfied"


def test_graph_report_ingest_records_generation_evidence(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    manifest = _manifest()
    manifest.requirements[0].evidence_policy.required = ["published_generation"]
    save_manifest(root, manifest)
    report = tmp_path / "graph-evidence.yaml"
    report.write_text(
        "kind: published_generation\n"
        "status: published\n"
        "refs: [req.demo]\n"
        "generation_id: 7\n"
        "command: okg generation describe 7 --json\n"
        "nodes_delta: 3\n"
        "edges_delta: 4\n",
        encoding="utf-8",
    )

    rc = main([
        "pact",
        "--root",
        str(root),
        "evidence",
        "ingest",
        "demo",
        "--graph-report",
        str(report),
        "--json",
    ])
    payload = _json_out(capsys)

    # deferred: the graph row landed, the ledger write is queued
    assert rc == 3
    assert payload["verdict"] == "deferred"
    assert payload["evidence"] == 1
    data = yaml.safe_load((root / "pact" / "changes" / "demo" / "pact.yaml").read_text())
    evidence = data["evidence"][0]
    assert evidence["kind"] == "published_generation"
    assert evidence["status"] == "pass"
    assert evidence["generation_id"] == 7
    assert evidence["data"]["nodes_delta"] == 3


def test_graph_report_ingest_preserves_string_generation_ref(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    manifest = _manifest()
    manifest.requirements[0].evidence_policy.required = ["graph_report"]
    save_manifest(root, manifest)
    report = tmp_path / "graph-evidence.yaml"
    report.write_text(
        "kind: graph_report\n"
        "status: pass\n"
        "refs: [req.demo]\n"
        "generation_ref: okg:okg-workspace:branch:default:gen:test\n"
        "command: okg dogfood status --deployment okg-workspace --json\n"
        "doctor_ok: true\n",
        encoding="utf-8",
    )

    rc = main([
        "pact",
        "--root",
        str(root),
        "evidence",
        "ingest",
        "demo",
        "--graph-report",
        str(report),
        "--json",
    ])
    payload = _json_out(capsys)

    # deferred: the graph row landed, the ledger write is queued
    assert rc == 3
    assert payload["verdict"] == "deferred"
    assert payload["evidence"] == 1
    data = yaml.safe_load((root / "pact" / "changes" / "demo" / "pact.yaml").read_text())
    evidence = data["evidence"][0]
    assert "generation_id" not in evidence
    assert evidence["data"]["generation_ref"] == "okg:okg-workspace:branch:default:gen:test"


def test_graph_report_satisfies_manual_test_plan_entry(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    manifest = _manifest()
    manifest.requirements[0].evidence_policy.required = ["graph_report"]
    manifest.requirements[0].acceptance[0].evidence = ["graph_report"]
    manifest.test_plan[0].test_id = "manual::graph_health"
    manifest.test_plan[0].command = "okg doctor --check integrity --json"
    manifest.test_plan[0].evidence = ["graph_report"]
    save_manifest(root, manifest)
    report = tmp_path / "graph-evidence.yaml"
    report.write_text(
        "kind: graph_report\n"
        "status: pass\n"
        "refs: [req.demo, test.demo, task.demo]\n"
        "command: okg doctor --check integrity --json\n"
        "doctor_ok: true\n"
        "dlq_count: 0\n",
        encoding="utf-8",
    )

    rc = main([
        "pact",
        "--root",
        str(root),
        "evidence",
        "ingest",
        "demo",
        "--graph-report",
        str(report),
        "--json",
    ])
    # deferred: the graph row landed, the ledger write is queued
    assert rc == 3
    assert _json_out(capsys)["verdict"] == "deferred"

    report = validate_manifest(load_manifest(root, "demo"))
    assert report.alignment["requirements"]["req.demo"]["satisfaction_status"] == "satisfied"
    assert report.alignment["test_plan"]["test.demo"]["satisfaction_status"] == "satisfied"
