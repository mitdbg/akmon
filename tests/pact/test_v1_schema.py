from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.validation import validate_manifest, version_info
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
                "statement": "Demo must validate.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "validates",
                        "when": ["validation runs"],
                        "then": ["validation passes"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "planned test exists",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_schema.py::test_define_pact_v1_validates_under_v0",
            }
        ],
    )


def test_define_pact_v1_validates_under_v0() -> None:
    report = validate_manifest(_manifest())

    assert report.ok
    assert report.alignment["versions"]["artifact_format_version"] == 0
    assert report.alignment["versions"]["target_language_version"] == 1


def test_artifact_format_and_target_version_are_distinct() -> None:
    info = version_info(_manifest())

    assert info["artifact_format_version"] == 0
    assert info["target_language_version"] == 1
    assert info["compatibility_mode"] is True
    assert info["migration_diagnostics"][0]["code"] == "compatibility_mode"


def test_v1_reader_accepts_v0_compatibility(tmp_path: Path, capsys) -> None:
    """Per `simplify-pact-surface` task 3, the `version` CLI verb was
    deleted (the negotiation block is exposed in `gate post-edit`
    output). The underlying helper `version_info` still works; this
    test exercises it directly.
    """
    root = _seed_root(tmp_path)
    manifest = _manifest()
    from okg.substrate.pact.store import load_manifest, save_manifest

    save_manifest(root, manifest)
    reloaded = load_manifest(root, "demo")
    info = version_info(reloaded)

    assert info["artifact_format_version"] == 0
    assert info["target_language_version"] == 1
    assert info["compatibility_mode"] is True
