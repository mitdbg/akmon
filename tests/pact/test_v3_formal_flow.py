from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.models import (
    AcceptanceCriterion,
    Change,
    PactManifest,
    Task,
    Waiver,
)
from okg.substrate.pact.store import file_digest, load_manifest, save_manifest
from okg.substrate.pact.validation import validate_manifest
from tests.pact.test_cli import _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _seed_formal_manifest(
    root: Path,
    *,
    property_ids: tuple[str, ...] = ("DemoInvariant",),
    checker_command: str | None = None,
) -> tuple[str, str]:
    checker_command = (
        checker_command
        or "tlc2.TLC -config formal/demo/Demo.cfg formal/demo/Demo.tla"
    )
    refinement_ref = (
        "tests/pact/test_v3_formal_flow.py::"
        "test_formal_binding_requires_checker_refinement_and_freshness"
    )
    (root / "formal" / "demo").mkdir(parents=True)
    (root / "formal" / "demo" / "Demo.tla").write_text(
        "---- MODULE Demo ----\n====\n",
        encoding="utf-8",
    )
    (root / "formal" / "demo" / "Demo.cfg").write_text(
        "SPECIFICATION Spec\n",
        encoding="utf-8",
    )
    (root / "tests" / "pact").mkdir(parents=True)
    (root / "tests" / "pact" / "test_v3_formal_flow.py").write_text(
        "def test_formal_binding_requires_checker_refinement_and_freshness():\n"
        "    pass\n",
        encoding="utf-8",
    )
    manifest = PactManifest(
        target_pact_version=3,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Formal proof must be fresh.",
                "evidence_policy": {"required": ["formal_check"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "fresh formal proof",
                        "when": ["formal evidence is ingested"],
                        "then": ["the binding is satisfied"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "formal checker evidence is fresh",
                        "evidence": ["formal.demo"],
                    }
                ],
            }
        ],
        formal=[
            {
                "id": "formal.demo" if index == 0 else f"formal.demo{index}",
                "requirement": "req.demo",
                "property_id": property_id,
                "module": "formal/demo/Demo.tla",
                "config": "formal/demo/Demo.cfg",
                "checker_command": checker_command,
                "refinement_tests": [refinement_ref],
            }
            for index, property_id in enumerate(property_ids)
        ],
    )
    save_manifest(root, manifest)
    return checker_command, refinement_ref


def _formal_report_result(
    root: Path,
    tmp_path: Path,
    *,
    property_id: str = "DemoInvariant",
    checker_command: str,
    refinement_ref: str,
) -> dict:
    artifact = tmp_path / f"{property_id}.stdout.log"
    artifact.write_text("TLC finished without invariant violations.\n", encoding="utf-8")
    return {
        "property_id": property_id,
        "kind": "formal_check",
        "status": "pass",
        "fresh": True,
        "checker_command": checker_command,
        "command": (
            "java -cp .tools/tla2tools.jar tlc2.TLC "
            "-config formal/demo/Demo.cfg formal/demo/Demo.tla"
        ),
        "artifact": str(artifact),
        "refinement_tests": [refinement_ref],
        "module_digest": file_digest(root / "formal" / "demo" / "Demo.tla"),
        "config_digest": file_digest(root / "formal" / "demo" / "Demo.cfg"),
    }


def _ingest_formal_result(
    root: Path,
    report_path: Path,
    result: dict,
    capsys,
) -> str:
    report_path.write_text(
        yaml.safe_dump({"results": [result]}),
        encoding="utf-8",
    )
    # the formal row lands in pact.yaml; only the ledger write is queued,
    # so ingest reports a deferral (exit 3) rather than a recorded closure
    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--formal-report", str(report_path), "--json",
    ])
    assert rc == 3
    assert _json_out(capsys)["verdict"] == "deferred"
    evidence = load_manifest(root, "demo").evidence
    return evidence[-1].id


def _add_completed_formal_task(root: Path, evidence_id: str) -> PactManifest:
    manifest = load_manifest(root, "demo")
    manifest.tasks.append(
        Task(
            id="task.demo",
            title="Demo",
            status="done",
            verification="run TLC",
            requirements=["req.demo"],
            evidence=[evidence_id],
        )
    )
    manifest.waivers.append(
        Waiver(
            id="waiver.lifecycle",
            scope="lifecycle.bypass",
            reason="test isolates formal evidence verification behavior",
            owner="owner",
        )
    )
    save_manifest(root, manifest)
    return load_manifest(root, "demo")


def test_formal_binding_requires_checker_refinement_and_freshness(
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
                "statement": "Formal proof must be fresh.",
                "evidence_policy": {"required": ["formal_check"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "fresh formal proof",
                        "when": ["formal evidence is ingested"],
                        "then": ["the binding is satisfied"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "formal checker evidence is fresh",
                        "evidence": ["formal.demo"],
                    }
                ],
            }
        ],
        formal=[
            {
                "id": "formal.demo",
                "requirement": "req.demo",
                "property_id": "DemoInvariant",
                "module": "formal/demo/Demo.tla",
                "refinement_tests": [
                    "tests/pact/test_v3_formal_flow.py::test_formal_binding_requires_checker_refinement_and_freshness"
                ],
            }
        ],
    )
    report = validate_manifest(manifest)
    assert not report.ok
    assert {issue.code for issue in report.issues} >= {
        "missing_formal_config",
        "missing_formal_checker_command",
    }

    (root / "formal" / "demo").mkdir(parents=True)
    (root / "formal" / "demo" / "Demo.tla").write_text(
        "---- MODULE Demo ----\n====\n",
        encoding="utf-8",
    )
    (root / "formal" / "demo" / "Demo.cfg").write_text(
        "SPECIFICATION Spec\n",
        encoding="utf-8",
    )
    (root / "tests" / "pact").mkdir(parents=True)
    (root / "tests" / "pact" / "test_v3_formal_flow.py").write_text(
        "def test_formal_binding_requires_checker_refinement_and_freshness():\n"
        "    pass\n",
        encoding="utf-8",
    )
    manifest.formal[0].config = "formal/demo/Demo.cfg"
    manifest.formal[0].checker_command = (
        "tlc2.TLC -config formal/demo/Demo.cfg formal/demo/Demo.tla"
    )
    save_manifest(root, manifest)

    artifact = tmp_path / "tlc.stdout.log"
    artifact.write_text("TLC finished without invariant violations.\n", encoding="utf-8")
    formal_report = tmp_path / "formal-report.yaml"
    refinement_ref = (
        "tests/pact/test_v3_formal_flow.py::"
        "test_formal_binding_requires_checker_refinement_and_freshness"
    )
    formal_report.write_text(
        "results:\n"
        "  - property_id: DemoInvariant\n"
        "    kind: formal_check\n"
        "    status: pass\n"
        "    fresh: true\n"
        f"    checker_command: {manifest.formal[0].checker_command}\n"
        "    command: java -cp .tools/tla2tools.jar tlc2.TLC -config formal/demo/Demo.cfg formal/demo/Demo.tla\n"
        f"    artifact: {artifact}\n"
        "    refinement_tests:\n"
        f"      - {refinement_ref}\n"
        f"    module_digest: {file_digest(root / 'formal' / 'demo' / 'Demo.tla')}\n"
        f"    config_digest: {file_digest(root / 'formal' / 'demo' / 'Demo.cfg')}\n",
        encoding="utf-8",
    )
    # ingest defers on the queued ledger write; the binding itself landed
    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--formal-report", str(formal_report), "--json",
    ])
    assert rc == 3
    assert _json_out(capsys)["verdict"] == "deferred"
    rc = main([
        "pact", "--root", str(root), "evidence", "status", "demo", "--json",
    ])
    status = _json_out(capsys)
    assert rc == 0
    assert status["verdict"] == "recorded"
    requirements = {row["id"]: row for row in status["requirements"]}
    acceptance = {row["id"]: row for row in status["acceptance"]}

    assert requirements["req.demo"]["status"] == "satisfied"
    assert acceptance["acc.demo"]["status"] == "satisfied"

    updated = load_manifest(root, "demo")
    formal_entry = next(evidence for evidence in updated.evidence if evidence.kind == "formal_check")
    assert formal_entry.data["checker_command"] == (
        "tlc2.TLC -config formal/demo/Demo.cfg formal/demo/Demo.tla"
    )
    assert formal_entry.data["artifact_digest"].startswith("sha256:")
    assert formal_entry.data["executed_command"].startswith("java -cp")
    formal_evidence = formal_entry.id
    updated.tasks.append(
        Task(
            id="task.demo",
            title="Demo",
            status="done",
            verification="run TLC",
            requirements=["req.demo"],
            evidence=[formal_evidence],
        )
    )
    assert validate_manifest(updated).ok


def test_formal_report_rejects_sparse_self_reported_pass(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _seed_formal_manifest(root)
    report_path = tmp_path / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({
            "results": [
                {
                    "property_id": "DemoInvariant",
                    "status": "pass",
                    "command": "tlc2.TLC formal/demo/Demo.tla",
                }
            ]
        }),
        encoding="utf-8",
    )

    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--formal-report", str(report_path), "--json",
    ])
    payload = _json_out(capsys)
    codes = {error["code"] for error in payload["error"]["data"]["errors"]}

    assert rc == 1
    assert codes >= {
        "formal_report_missing_checker_command",
        "formal_report_missing_artifact",
        "formal_report_missing_freshness",
        "formal_report_missing_refinement_tests",
    }
    assert load_manifest(root, "demo").evidence == []


def test_formal_report_rejects_stale_missing_artifact_and_digest_mismatch(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    checker_command, refinement_ref = _seed_formal_manifest(root)
    result = _formal_report_result(
        root,
        tmp_path,
        checker_command=checker_command,
        refinement_ref=refinement_ref,
    )
    result["fresh"] = False
    result["artifact"] = str(tmp_path / "missing-tlc.stdout.log")
    result["module_digest"] = "sha256:not-the-module"
    result["config_digest"] = "sha256:not-the-config"
    report_path = tmp_path / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": [result]}),
        encoding="utf-8",
    )

    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--formal-report", str(report_path), "--json",
    ])
    payload = _json_out(capsys)
    codes = {error["code"] for error in payload["error"]["data"]["errors"]}

    assert rc == 1
    assert codes >= {
        "formal_report_stale",
        "formal_report_missing_artifact_file",
        "formal_report_module_digest_mismatch",
        "formal_report_config_digest_mismatch",
    }
    assert load_manifest(root, "demo").evidence == []


@pytest.mark.parametrize(
    ("missing_field", "expected_code"),
    [
        ("module_digest", "formal_report_missing_module_digest"),
        ("config_digest", "formal_report_missing_config_digest"),
    ],
)
def test_formal_report_rejects_missing_module_or_config_digest(
    tmp_path: Path,
    capsys,
    missing_field: str,
    expected_code: str,
) -> None:
    root = _seed_root(tmp_path)
    checker_command, refinement_ref = _seed_formal_manifest(root)
    result = _formal_report_result(
        root,
        tmp_path,
        checker_command=checker_command,
        refinement_ref=refinement_ref,
    )
    result.pop(missing_field)
    report_path = tmp_path / f"formal-report-missing-{missing_field}.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": [result]}),
        encoding="utf-8",
    )

    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--formal-report", str(report_path), "--json",
    ])
    payload = _json_out(capsys)
    codes = {error["code"] for error in payload["error"]["data"]["errors"]}

    assert rc == 1
    assert expected_code in codes
    assert load_manifest(root, "demo").evidence == []


def test_formal_report_rejects_artifact_outside_report_dir(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    checker_command, refinement_ref = _seed_formal_manifest(root)
    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    artifact_dir = tmp_path / "elsewhere"
    artifact_dir.mkdir()
    result = _formal_report_result(
        root,
        artifact_dir,
        checker_command=checker_command,
        refinement_ref=refinement_ref,
    )
    report_path = report_dir / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": [result]}),
        encoding="utf-8",
    )

    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--formal-report", str(report_path), "--json",
    ])
    payload = _json_out(capsys)
    codes = {error["code"] for error in payload["error"]["data"]["errors"]}

    assert rc == 1
    assert "formal_report_artifact_outside_report_dir" in codes
    assert load_manifest(root, "demo").evidence == []


@pytest.mark.parametrize(
    "mutation",
    ["module", "config", "artifact", "artifact_deleted", "checker_script"],
)
def test_stale_formal_evidence_no_longer_satisfies_requirements_tasks_or_gates(
    tmp_path: Path,
    capsys,
    mutation: str,
) -> None:
    root = _seed_root(tmp_path)
    (root / "scripts").mkdir()
    checker_script = root / "scripts" / "check_formal_demo.py"
    checker_script.write_text("raise SystemExit(0)\n", encoding="utf-8")
    checker_command = "uv run python scripts/check_formal_demo.py"
    checker_command, refinement_ref = _seed_formal_manifest(
        root,
        checker_command=checker_command,
    )
    result = _formal_report_result(
        root,
        tmp_path,
        checker_command=checker_command,
        refinement_ref=refinement_ref,
    )
    evidence_id = _ingest_formal_result(
        root,
        tmp_path / "formal-report.yaml",
        result,
        capsys,
    )
    manifest = _add_completed_formal_task(root, evidence_id)

    fresh = validate_manifest(manifest, repo_root=root, lifecycle_gate="verification")
    assert fresh.alignment["requirements"]["req.demo"]["satisfaction_status"] == "satisfied"
    assert fresh.alignment["acceptance"]["acc.demo"]["satisfaction_status"] == "satisfied"
    assert fresh.alignment["tasks"]["task.demo"]["status"] == "complete"
    assert "verification_gate_missing_formal_evidence" not in {
        issue.code for issue in fresh.issues
    }

    artifact_path = root / manifest.evidence[0].data["artifact_path"]
    if mutation == "module":
        (root / "formal" / "demo" / "Demo.tla").write_text(
            "---- MODULE Demo ----\nVARIABLE changed\n====\n",
            encoding="utf-8",
        )
    elif mutation == "config":
        (root / "formal" / "demo" / "Demo.cfg").write_text(
            "SPECIFICATION Changed\n",
            encoding="utf-8",
        )
    elif mutation == "artifact":
        artifact_path.write_text("different TLC output\n", encoding="utf-8")
    elif mutation == "artifact_deleted":
        artifact_path.unlink()
    elif mutation == "checker_script":
        checker_script.write_text("raise SystemExit(1)\n", encoding="utf-8")
    stale = validate_manifest(manifest, repo_root=root, lifecycle_gate="verification")
    codes = {issue.code for issue in stale.issues}

    assert "stale_formal_evidence" in codes
    assert "task_evidence_stale_formal" in codes
    assert "verification_gate_missing_formal_evidence" in codes
    assert stale.alignment["requirements"]["req.demo"]["satisfaction_status"] == "unsatisfied"
    assert stale.alignment["acceptance"]["acc.demo"]["satisfaction_status"] == "unsatisfied"
    assert stale.alignment["tasks"]["task.demo"]["status"] == "missing_evidence"


def test_archive_profile_ignores_only_machine_local_formal_artifact(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    (root / "scripts").mkdir()
    checker_script = root / "scripts" / "check_formal_demo.py"
    checker_script.write_text("raise SystemExit(0)\n", encoding="utf-8")
    checker_command, refinement_ref = _seed_formal_manifest(
        root,
        checker_command="uv run python scripts/check_formal_demo.py",
    )
    scratch = root / ".scratch" / "formal" / "demo"
    scratch.mkdir(parents=True)
    result = _formal_report_result(
        root,
        scratch,
        checker_command=checker_command,
        refinement_ref=refinement_ref,
    )
    evidence_id = _ingest_formal_result(
        root,
        scratch / "formal-report.yaml",
        result,
        capsys,
    )
    manifest = _add_completed_formal_task(root, evidence_id)
    artifact = root / manifest.evidence[-1].data["artifact_path"]
    artifact.write_text("different machine-local TLC output\n", encoding="utf-8")

    strict = validate_manifest(manifest, repo_root=root)
    assert "stale_formal_evidence" in {issue.code for issue in strict.issues}

    archive_context = PactEvaluationContext(
        ignore_machine_local_formal_artifact_digest=True
    )
    archived = validate_manifest(
        manifest,
        repo_root=root,
        context=archive_context,
    )
    assert archived.alignment["requirements"]["req.demo"][
        "satisfaction_status"
    ] == "satisfied"
    assert archived.alignment["tasks"]["task.demo"]["status"] == "complete"
    assert "stale_formal_evidence" not in {
        issue.code for issue in archived.issues
    }

    (root / "formal" / "demo" / "Demo.tla").write_text(
        "---- MODULE Demo ----\nVARIABLE changed\n====\n",
        encoding="utf-8",
    )
    tracked_stale = validate_manifest(
        manifest,
        repo_root=root,
        context=archive_context,
    )
    assert "stale_formal_evidence" in {
        issue.code for issue in tracked_stale.issues
    }
    assert tracked_stale.alignment["tasks"]["task.demo"]["status"] == (
        "missing_evidence"
    )


def test_stale_formal_evidence_no_longer_satisfies_direct_or_required_any_proofs(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    checker_command, refinement_ref = _seed_formal_manifest(root)
    manifest = load_manifest(root, "demo")
    req = manifest.requirements[0]
    req.evidence_policy.required = []
    req.evidence_policy.required_any = ["formal_check"]
    req.acceptance = [
        AcceptanceCriterion(
            id="acc.generic-formal",
            description="generic formal evidence is accepted only while fresh",
            evidence=["formal"],
        ),
        AcceptanceCriterion(
            id="acc.generic-formal-check",
            description="generic formal_check evidence is accepted only while fresh",
            evidence=["formal_check"],
        ),
    ]
    save_manifest(root, manifest)
    result = _formal_report_result(
        root,
        tmp_path,
        checker_command=checker_command,
        refinement_ref=refinement_ref,
    )
    evidence_id = _ingest_formal_result(
        root,
        tmp_path / "formal-report.yaml",
        result,
        capsys,
    )
    manifest = load_manifest(root, "demo")
    manifest.requirements[0].acceptance.append(
        AcceptanceCriterion(
            id="acc.direct-evidence",
            description="direct formal evidence id is accepted only while fresh",
            evidence=[evidence_id],
        )
    )
    save_manifest(root, manifest)
    manifest = load_manifest(root, "demo")

    fresh = validate_manifest(manifest, repo_root=root)
    assert fresh.alignment["requirements"]["req.demo"]["satisfaction_status"] == "satisfied"
    assert {
        key: row["satisfaction_status"]
        for key, row in fresh.alignment["acceptance"].items()
    } == {
        "acc.generic-formal": "satisfied",
        "acc.generic-formal-check": "satisfied",
        "acc.direct-evidence": "satisfied",
    }

    (root / manifest.evidence[0].data["artifact_path"]).write_text(
        "different TLC output\n",
        encoding="utf-8",
    )
    stale = validate_manifest(manifest, repo_root=root)

    assert stale.alignment["requirements"]["req.demo"]["satisfaction_status"] == "unsatisfied"
    assert "any:formal_check" in stale.alignment["requirements"]["req.demo"]["satisfaction_missing"]
    assert {
        key: row["satisfaction_status"]
        for key, row in stale.alignment["acceptance"].items()
    } == {
        "acc.generic-formal": "unsatisfied",
        "acc.generic-formal-check": "unsatisfied",
        "acc.direct-evidence": "unsatisfied",
    }


def test_formal_report_rejects_unknown_and_omitted_bound_properties(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    checker_command, refinement_ref = _seed_formal_manifest(
        root,
        property_ids=("DemoInvariant", "OtherInvariant"),
    )
    valid_result = _formal_report_result(
        root,
        tmp_path,
        checker_command=checker_command,
        refinement_ref=refinement_ref,
    )
    unknown_result = dict(valid_result)
    unknown_result["property_id"] = "UnknownInvariant"
    report_path = tmp_path / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": [valid_result, unknown_result]}),
        encoding="utf-8",
    )

    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--formal-report", str(report_path), "--json",
    ])
    payload = _json_out(capsys)
    codes = {error["code"] for error in payload["error"]["data"]["errors"]}

    assert rc == 1
    assert codes >= {
        "formal_report_unknown_property",
        "formal_report_missing_bound_property",
    }
    assert load_manifest(root, "demo").evidence == []


def test_formal_report_rejects_archived_refinement_tests(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    checker_command, refinement_ref = _seed_formal_manifest(root)
    result = _formal_report_result(
        root,
        tmp_path,
        checker_command=checker_command,
        refinement_ref=refinement_ref,
    )
    result["refinement_tests"] = [
        refinement_ref,
        (
            "./archive/ui/legacy-viz-2026-06-17/tests/console/"
            "test_offline_replay_formal_mapping.py::"
            "test_online_head_progress_is_mapped_to_runtime_replay_transitions"
        ),
    ]
    report_path = tmp_path / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": [result]}),
        encoding="utf-8",
    )

    rc = main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--formal-report", str(report_path), "--json",
    ])
    payload = _json_out(capsys)
    codes = {error["code"] for error in payload["error"]["data"]["errors"]}

    assert rc == 1
    assert "formal_report_archived_refinement_test" in codes
    assert load_manifest(root, "demo").evidence == []
