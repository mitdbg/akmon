from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.validation import validate_manifest


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_tla_binding_keeps_module_standalone(tmp_path: Path, capsys) -> None:
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "formal" / "demo").mkdir(parents=True)
    (root / "formal" / "demo" / "Demo.tla").write_text(
        "---- MODULE Demo ----\nVARIABLE x\n====\n",
        encoding="utf-8",
    )
    (root / "formal" / "demo" / "Demo.cfg").write_text(
        "INVARIANT DemoInvariant\n",
        encoding="utf-8",
    )
    (root / "tests" / "pact").mkdir(parents=True)
    (root / "tests" / "pact" / "test_formal_binding.py").write_text(
        "def test_tla_binding_keeps_module_standalone():\n    pass\n",
        encoding="utf-8",
    )
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 4\n"
        "project: okg\n"
        "pact_defaults:\n"
        "  artifact_format_version: 4\n"
        "  target_pact_version: 4\n",
        encoding="utf-8",
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    assert main([
        "pact", "--root", str(root), "new-change", "demo",
        "--template", "protocol", "--json",
    ]) == 0
    pact_operations.add_requirement(
        root, "demo", req_id="req.demo", title="Demo",
        statement="Demo must validate.", evidence=["formal"],
    )

    rc = main([
        "pact", "--root", str(root), "evidence", "bind-tla", "demo",
        "--requirement", "req.demo",
        "--model", "formal/demo/Demo.tla",
        "--property-id", "DemoInvariant",
        "--config", "formal/demo/Demo.cfg",
        "--checker-command", (
            "tlc2.TLC -config formal/demo/Demo.cfg formal/demo/Demo.tla"
        ),
        "--refinement-test", "tests/pact/test_formal_binding.py::test_tla_binding_keeps_module_standalone",
        "--json",
    ])
    payload = _json_out(capsys)

    assert rc == 0
    assert payload["formal"] == 1
    assert (root / "formal" / "demo" / "Demo.tla").read_text().startswith("---- MODULE")


def test_formal_binding_requires_refinement_test() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo", tier=1),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Needs formal proof.",
                "evidence_policy": {"required": ["formal"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "formal",
                        "when": ["validation runs"],
                        "then": ["refinement is required"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "formal proof",
                        "evidence": ["formal"],
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
            }
        ],
    )

    report = validate_manifest(manifest)

    assert not report.ok
    assert any(
        issue.code == "missing_formal_refinement"
        for issue in report.issues
    )


def test_formal_binding_validates_artifacts_when_repo_root_is_available(
    tmp_path: Path,
) -> None:
    root = tmp_path
    (root / "formal" / "demo").mkdir(parents=True)
    (root / "formal" / "demo" / "Demo.tla").write_text(
        "---- MODULE Demo ----\n====\n",
        encoding="utf-8",
    )
    (root / "formal" / "demo" / "Demo.cfg").write_text(
        "SPECIFICATION Spec\n",
        encoding="utf-8",
    )
    (root / "scripts").mkdir()
    (root / "scripts" / "check_formal_demo.py").write_text(
        "raise SystemExit(0)\n",
        encoding="utf-8",
    )
    (root / "tests" / "pact").mkdir(parents=True)
    (root / "tests" / "pact" / "test_demo.py").write_text(
        "def test_demo_refinement():\n    pass\n",
        encoding="utf-8",
    )
    manifest = PactManifest(
        target_pact_version=4,
        change=Change(id="demo", title="Demo", tier=1),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Needs formal proof.",
                "evidence_policy": {"required": ["formal_check"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "formal",
                        "when": ["validation runs"],
                        "then": ["artifacts exist"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "formal proof",
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
                "config": "formal/demo/Demo.cfg",
                "checker_command": "uv run python scripts/check_formal_demo.py",
                "refinement_tests": [
                    "tests/pact/test_demo.py::test_demo_refinement"
                ],
            }
        ],
    )

    report = validate_manifest(manifest, repo_root=root)

    assert report.ok


def test_formal_binding_reports_missing_artifacts_when_repo_root_is_available(
    tmp_path: Path,
) -> None:
    root = tmp_path
    (root / "tests" / "pact").mkdir(parents=True)
    (root / "tests" / "pact" / "test_demo.py").write_text(
        "def test_other_refinement():\n    pass\n",
        encoding="utf-8",
    )
    manifest = PactManifest(
        target_pact_version=4,
        change=Change(id="demo", title="Demo", tier=1),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Needs formal proof.",
                "evidence_policy": {"required": ["formal_check"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "formal",
                        "when": ["validation runs"],
                        "then": ["missing artifacts fail"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "formal proof",
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
                "module": "formal/demo/Missing.tla",
                "config": "formal/demo/Missing.cfg",
                "checker_command": "uv run python scripts/check_missing.py",
                "refinement_tests": [
                    "tests/pact/test_demo.py::test_missing_refinement",
                    "tests/pact/test_absent.py::test_absent_refinement",
                ],
            }
        ],
    )

    report = validate_manifest(manifest, repo_root=root)
    codes = {issue.code for issue in report.issues}

    assert not report.ok
    assert codes >= {
        "missing_formal_module",
        "missing_formal_config_file",
        "missing_formal_checker_script",
        "missing_formal_refinement_target",
        "missing_formal_refinement_file",
    }
